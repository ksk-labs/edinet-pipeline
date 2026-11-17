"""pipeline.monitor のテスト (S2-T5 §5)。

合格条件6の故障ケースを含む: 取得の失敗を人為的に起こした状態
(実行サマリが無い、失敗件数がある) で、警報が日次報告と状態ファイルに
載り、連続失敗が明示されることを確認する。
"""
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import monitor, parse


def _jsonl_bytes(entries):
    return "".join(
        json.dumps(e, ensure_ascii=False) + "\n" for e in entries
    ).encode("utf-8")


def _json_bytes(data):
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


class _FakeApi:
    def __init__(self, store):
        self.store = store

    def list_repo_files(self, repo, repo_type="dataset"):
        return sorted(self.store)

    def upload_file(self, path_or_fileobj, path_in_repo, repo_id,
                    repo_type="dataset", commit_message=None):
        with open(path_or_fileobj, "rb") as f:
            self.store[path_in_repo] = f.read()


class MonitorTestBase(unittest.TestCase):
    def setUp(self):
        self.store = {}
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        api = _FakeApi(self.store)

        def fake_download(repo_id, filename, repo_type=None, local_dir=None,
                          token=None):
            path = os.path.join(self.tmp, "dl",
                                filename.replace("/", "_"))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(self.store[filename])
            return path

        for p in [
            mock.patch.object(monitor, "HfApi", lambda: api),
            mock.patch.object(parse, "hf_hub_download", fake_download),
        ]:
            p.start()
            self.addCleanup(p.stop)
        self.work_seq = 0

    def put(self, path, data):
        self.store[path] = data

    def run_monitor(self, run_date):
        self.work_seq += 1
        return monitor.run_monitor(
            run_date, os.path.join(self.tmp, f"work-{self.work_seq}"),
            repo="fake/repo")

    def saved_report(self, run_date):
        data = self.store[f"monitor/reports/date={run_date}/report.json"]
        return json.loads(data.decode("utf-8"))

    def saved_state(self):
        return json.loads(self.store["monitor/state.json"].decode("utf-8"))

    def put_acquisition_ok(self, target_date, failed=0):
        month = target_date[:7]
        stamp = target_date.replace("-", "") + "T000000Z"
        self.put(
            f"acquisition/month={month}/run_summary-{target_date}-{stamp}.json",
            _json_bytes({
                "date": target_date, "list_count": 100,
                "downloaded": 200, "failed": failed,
                "started_at": f"{target_date}T18:30:00+00:00",
                "finished_at": f"{target_date}T19:00:00+00:00",
            }))

    def put_codelist_ok(self, run_date, row_count=11468):
        self.put(f"codelist/date={run_date}/snapshot.json", _json_bytes({
            "date": run_date, "row_count": row_count}))
        self.put("codelist/ledger.jsonl", _jsonl_bytes([
            {"date": run_date, "run_date": run_date, "status": "ok",
             "diff_status": "generated", "row_count": row_count},
        ]))


class HealthyDayTest(MonitorTestBase):
    def test_no_alerts_and_state_saved(self):
        self.put_acquisition_ok("2026-11-16")
        self.put_codelist_ok("2026-11-17")
        report = self.run_monitor("2026-11-17")
        self.assertEqual(report["alerts"], [])
        self.assertEqual(report["acquisition"]["status"], "ok")
        self.assertEqual(report["acquisition"]["consecutive_failures"], 0)
        self.assertEqual(report["signals"]["codelist_row_count"], 11468)
        self.assertEqual(report["signals"]["document_list_count"], 100)
        saved = self.saved_report("2026-11-17")
        self.assertEqual(saved["alerts"], [])
        state = self.saved_state()
        self.assertEqual(state["acquisition"]["consecutive_failures"], 0)
        self.assertEqual(state["acquisition"]["last_ok_date"], "2026-11-16")
        self.assertEqual(state["open_alerts"], [])


class AcquisitionFailureTest(MonitorTestBase):
    """合格条件6: 取得の失敗で警報が載り、連続失敗が明示される。"""

    def test_missing_run_alerts_and_counts_consecutive_days(self):
        self.put_codelist_ok("2026-11-17")
        report = self.run_monitor("2026-11-17")
        kinds = [a["kind"] for a in report["alerts"]]
        self.assertIn("acquisition_missing", kinds)
        self.assertEqual(report["acquisition"]["consecutive_failures"], 1)
        self.assertEqual(self.saved_state()["acquisition"]
                         ["consecutive_failures"], 1)

        self.put_codelist_ok("2026-11-18")
        report = self.run_monitor("2026-11-18")
        self.assertEqual(report["acquisition"]["consecutive_failures"], 2)
        alert = next(a for a in report["alerts"]
                     if a["kind"] == "acquisition_missing")
        self.assertEqual(alert["consecutive_failures"], 2)
        self.assertEqual(self.saved_report("2026-11-18")
                         ["acquisition"]["consecutive_failures"], 2)

    def test_failed_documents_count_as_failure_then_recovery(self):
        self.put_codelist_ok("2026-11-17")
        self.put_acquisition_ok("2026-11-16", failed=3)
        report = self.run_monitor("2026-11-17")
        kinds = [a["kind"] for a in report["alerts"]]
        self.assertIn("acquisition_failures", kinds)
        self.assertEqual(report["acquisition"]["consecutive_failures"], 1)

        self.put_codelist_ok("2026-11-18")
        self.put_acquisition_ok("2026-11-17")
        report = self.run_monitor("2026-11-18")
        self.assertEqual(report["acquisition"]["status"], "ok")
        self.assertEqual(report["acquisition"]["consecutive_failures"], 0)

    def test_same_target_rerun_does_not_double_count(self):
        self.put_codelist_ok("2026-11-17")
        self.run_monitor("2026-11-17")
        report = self.run_monitor("2026-11-17")
        self.assertEqual(report["acquisition"]["consecutive_failures"], 1)


class CodelistMissingTest(MonitorTestBase):
    def test_missing_snapshot_alerts(self):
        self.put_acquisition_ok("2026-11-16")
        report = self.run_monitor("2026-11-17")
        kinds = [a["kind"] for a in report["alerts"]]
        self.assertIn("codelist_missing", kinds)
        self.assertEqual(report["codelist"]["status"], "missing")
        self.assertIsNone(report["signals"]["codelist_row_count"])

    def test_conflict_and_pending_diff_alert(self):
        self.put_acquisition_ok("2026-11-16")
        self.put("codelist/date=2026-11-17/snapshot.json",
                 _json_bytes({"date": "2026-11-17", "row_count": 11468}))
        self.put("codelist/ledger.jsonl", _jsonl_bytes([
            {"date": "2026-11-17", "run_date": "2026-11-17",
             "status": "conflict", "diff_status": "pending"},
        ]))
        report = self.run_monitor("2026-11-17")
        kinds = [a["kind"] for a in report["alerts"]]
        self.assertIn("codelist_conflict", kinds)
        self.assertIn("codelist_diff_pending", kinds)


class ParseIssuesTest(MonitorTestBase):
    def setUp(self):
        super().setUp()
        self.put_acquisition_ok("2026-11-16")
        self.put_codelist_ok("2026-11-17")

    def test_quarantine_backlog_and_retired_alert(self):
        self.put("acquisition/month=2026-11/ledger.jsonl", _jsonl_bytes([
            {"doc_id": "DONE1", "kind": "zip", "status": "ok",
             "sha256": "s1", "fetched_at": "2026-11-16T10:00:00+00:00"},
            {"doc_id": "STUCK1", "kind": "zip", "status": "ok",
             "sha256": "s2", "fetched_at": "2026-11-16T10:00:00+00:00"},
            {"doc_id": "FRESH1", "kind": "zip", "status": "ok",
             "sha256": "s3", "fetched_at": "2026-11-17T10:00:00+00:00"},
            {"doc_id": "BAD1", "kind": "zip", "status": "ok",
             "sha256": "s4", "fetched_at": "2026-11-10T10:00:00+00:00"},
        ]))
        entries = [
            {"doc_id": "DONE1", "status": "ok", "rows": 10,
             "parsed_at": "2026-11-16T10:00:00+00:00", "month": "2026-11"},
            {"doc_id": "Q1", "status": "quarantined", "reason": "bad_zip",
             "parsed_at": "2026-11-16T21:30:00+00:00", "month": "2026-11"},
        ]
        entries += [
            {"doc_id": "BAD1", "status": "quarantined",
             "reason": "zero_facts",
             "parsed_at": "2026-11-11T21:00:00+00:00", "month": "2026-11"},
        ] * parse.MAX_PARSE_ATTEMPTS
        self.put("parse/month=2026-11/ledger.jsonl", _jsonl_bytes(entries))
        report = self.run_monitor("2026-11-17")
        kinds = [a["kind"] for a in report["alerts"]]
        self.assertIn("parse_quarantined", kinds)
        self.assertIn("parse_backlog", kinds)
        self.assertIn("parse_retired", kinds)
        self.assertEqual(report["parse"]["bad_reasons"], {"bad_zip": 1})
        backlog_alert = next(a for a in report["alerts"]
                             if a["kind"] == "parse_backlog")
        self.assertEqual(backlog_alert["doc_ids"], ["BAD1", "STUCK1"])
        retired_alert = next(a for a in report["alerts"]
                             if a["kind"] == "parse_retired")
        self.assertEqual(retired_alert["doc_ids"], ["BAD1"])
        self.assertEqual(report["signals"]["parse_ok_today"], 0)


if __name__ == "__main__":
    unittest.main()
