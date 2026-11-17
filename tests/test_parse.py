"""pipeline.parse の系譜判定・実行サマリ・再試行部品の回帰テスト。

S-1: merge の superseded_for は、解析目録で doc_type が分かる現在の値に
ついて repair-lineage と同じ判定 (130 のときだけ候補) をすること。
解析目録に無い (バックフィル期の) 値は、現在の値に頼る従来の扱いを保つ。
S-2: 実行サマリは実行単位のパス (run_summary-<対象日>-<実行開始時刻>)
に残り、同月の後続の実行で上書きされないこと。
T-2: 再試行部品は pipeline.hf_sync の _with_retry を共用すること。
"""
import io
import json
import os
import re
import shutil
import tempfile
import time
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import hf_sync, parse
from pipeline.batch import SILVER_SCHEMA


def _row(doc_id, submit_at, superseded_by=None, is_correction=False,
         parent_doc_id=None):
    return {
        "doc_id": doc_id, "element": "xbrli:Item", "value": 1.0,
        "unit": "xbrli:pure", "context_start": "2026-01-01",
        "context_end": "2026-01-01", "dimension": "",
        "submit_at": submit_at, "is_correction": is_correction,
        "parent_doc_id": parent_doc_id, "superseded_by": superseded_by,
        "point_in_time": "final" if is_correction else "as_filed",
    }


def _parquet_bytes(rows):
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=SILVER_SCHEMA), buf)
    return buf.getvalue()


def _jsonl_bytes(entries):
    return "".join(
        json.dumps(e, ensure_ascii=False) + "\n" for e in entries
    ).encode("utf-8")


class _FakeApi:
    def __init__(self, store):
        self.store = store

    def list_repo_files(self, repo, repo_type="dataset"):
        return sorted(self.store)

    def upload_file(self, path_or_fileobj, path_in_repo, repo_id,
                    repo_type="dataset", commit_message=None):
        with open(path_or_fileobj, "rb") as f:
            self.store[path_in_repo] = f.read()


class MergeTestBase(unittest.TestCase):
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

        self._patches = [
            mock.patch.object(parse, "HfApi", lambda: api),
            mock.patch.object(parse, "hf_hub_download", fake_download),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    def put(self, path, data):
        self.store[path] = data

    def silver_rows(self, month):
        data = self.store[f"silver/month={month}/silver.parquet"]
        return pq.read_table(io.BytesIO(data)).to_pylist()

    def write_shards(self, results):
        shards_dir = os.path.join(self.tmp, "shards")
        sdir = os.path.join(shards_dir, "shard-0")
        os.makedirs(os.path.join(sdir, "rows"), exist_ok=True)
        with open(os.path.join(sdir, "results.jsonl"), "w",
                  encoding="utf-8") as f:
            for e in results:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        open(os.path.join(sdir, "labels.jsonl"), "w").close()
        for e in results:
            if e["status"] != "ok":
                continue
            path = os.path.join(sdir, "rows", e["doc_id"] + ".parquet")
            with open(path, "wb") as f:
                f.write(_parquet_bytes([_row(
                    e["doc_id"], e["submit_at"],
                    is_correction=e.get("doc_type") == "130",
                    parent_doc_id=e.get("parent_doc_id"))]))
        return shards_dir

    def run_merge(self, results, tag):
        shards_dir = self.write_shards(results)
        plan = {"date": "2026-11-16",
                "docs": [{"doc_id": e["doc_id"]} for e in results]}
        plan_path = os.path.join(self.tmp, f"plan-{tag}.json")
        with open(plan_path, "w", encoding="utf-8") as f:
            json.dump(plan, f, ensure_ascii=False)
        return parse.run_merge(plan_path, shards_dir,
                               os.path.join(self.tmp, f"work-{tag}"),
                               repo="fake/repo")


class LineageAlignmentTest(MergeTestBase):
    """S-1: 現在の値の扱いを repair-lineage と揃える。"""

    def setUp(self):
        super().setUp()
        self.put("silver/month=2026-10/silver.parquet", _parquet_bytes([
            _row("P", "2026-06-01 10:00:00", superseded_by="XBAD"),
            _row("Q", "2026-06-01 10:00:00", superseded_by="ZOLD"),
            _row("ZOLD", "2026-09-01 09:00:00"),
            _row("R", "2026-06-01 10:00:00", superseded_by="Y130"),
        ]))
        self.put("silver/month=2026-10/ledger.jsonl", _jsonl_bytes(
            [{"doc_id": d} for d in ("P", "Q", "ZOLD", "R")]))
        self.put("parse/month=2026-11/ledger.jsonl", _jsonl_bytes([
            {"doc_id": "XBAD", "status": "ok", "doc_type": "040",
             "parent_doc_id": "P", "submit_at": "2026-11-12 10:00:00",
             "month": "2026-11"},
            {"doc_id": "Y130", "status": "ok", "doc_type": "130",
             "parent_doc_id": "OTHER", "submit_at": "2026-10-05 10:00:00",
             "month": "2026-11"},
        ]))
        self.results = [
            {"doc_id": "CNEW", "status": "ok", "doc_type": "130",
             "parent_doc_id": "P", "submit_at": "2026-11-10 10:00:00",
             "month": "2026-11"},
            {"doc_id": "COLD", "status": "ok", "doc_type": "130",
             "parent_doc_id": "Q", "submit_at": "2026-08-01 10:00:00",
             "month": "2026-11"},
            {"doc_id": "CMID", "status": "ok", "doc_type": "130",
             "parent_doc_id": "R", "submit_at": "2026-10-01 10:00:00",
             "month": "2026-11"},
        ]

    def superseded(self, rows, doc_id):
        values = {r["superseded_by"] for r in rows if r["doc_id"] == doc_id}
        self.assertEqual(len(values), 1)
        return values.pop()

    def test_known_non130_current_is_not_a_candidate(self):
        self.run_merge(self.results, "a")
        rows = self.silver_rows("2026-10")
        self.assertEqual(self.superseded(rows, "P"), "CNEW")

    def test_backfill_current_value_is_kept(self):
        self.run_merge(self.results, "a")
        rows = self.silver_rows("2026-10")
        self.assertEqual(self.superseded(rows, "Q"), "ZOLD")

    def test_known_130_current_stays_a_candidate(self):
        self.run_merge(self.results, "a")
        rows = self.silver_rows("2026-10")
        self.assertEqual(self.superseded(rows, "R"), "Y130")


class RunSummaryPathTest(MergeTestBase):
    """S-2: 実行サマリが実行単位で残る。"""

    def setUp(self):
        super().setUp()
        self.results = [
            {"doc_id": "N1", "status": "ok", "doc_type": "120",
             "parent_doc_id": None, "submit_at": "2026-11-16 10:00:00",
             "month": "2026-11"},
        ]

    def summary_keys(self):
        return sorted(k for k in self.store
                      if k.startswith("parse/month=2026-11/run_summary"))

    def test_summary_path_is_per_run(self):
        self.run_merge(self.results, "a")
        keys = self.summary_keys()
        self.assertEqual(len(keys), 1)
        self.assertRegex(
            keys[0],
            r"^parse/month=2026-11/run_summary-2026-11-16-\d{8}T\d{6}Z\.json$")
        time.sleep(1.1)
        self.run_merge(self.results, "b")
        keys = self.summary_keys()
        self.assertEqual(len(keys), 2)
        self.assertNotIn("parse/month=2026-11/run_summary.json", self.store)


class WithRetrySharingTest(unittest.TestCase):
    """T-2: _with_retry は hf_sync のものを共用する。"""

    def test_with_retry_is_hf_syncs(self):
        self.assertIs(parse._with_retry, hf_sync._with_retry)

    def test_status_code_only_exception_is_retried(self):
        class RateLimited(Exception):
            status_code = 429

        calls = []

        def fn():
            calls.append(1)
            if len(calls) < 3:
                raise RateLimited("slow down")
            return "ok"

        with mock.patch("time.sleep", lambda s: None):
            self.assertEqual(parse._with_retry("test", fn), "ok")
        self.assertEqual(len(calls), 3)


class PlanTestBase(unittest.TestCase):
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
            mock.patch.object(parse, "HfApi", lambda: api),
            mock.patch.object(parse, "hf_hub_download", fake_download),
        ]:
            p.start()
            self.addCleanup(p.stop)

    def put(self, path, data):
        self.store[path] = data

    def setup_doc(self, doc_id, parse_entries, sha="sha-current"):
        self.put("acquisition/month=2026-11/ledger.jsonl", _jsonl_bytes([
            {"doc_id": doc_id, "kind": "zip", "status": "ok",
             "path": f"raw/edinet/2026/11/16/zip/{doc_id}.zip",
             "sha256": sha, "fetched_at": "2026-11-17T00:00:00+00:00"},
        ]))
        self.put("catalog/month=2026-11/ledger.jsonl", _jsonl_bytes([
            {"docID": doc_id, "submitDateTime": "2026-11-16 10:00",
             "docTypeCode": "120", "parentDocID": None},
        ]))
        if parse_entries is not None:
            self.put("parse/month=2026-11/ledger.jsonl",
                     _jsonl_bytes(parse_entries))

    def run_plan(self, date=None, doc_ids=None):
        return parse.run_plan(date, doc_ids, None,
                              os.path.join(self.tmp, "plan"),
                              repo="fake/repo")


def _parse_entry(doc_id, status, **kw):
    entry = {"doc_id": doc_id, "status": status,
             "month": "2026-11", "submit_at": "2026-11-16 10:00",
             "doc_type": "120", "parent_doc_id": None,
             "zip_sha256": "sha-current"}
    entry.update(kw)
    return entry


class PlanRetirementTest(PlanTestBase):
    """再試行の上限: 連続失敗が上限に達した書類は計画から外れる。"""

    def test_retired_after_max_consecutive_failures(self):
        entries = [_parse_entry("D1", "quarantined", reason="bad_zip")
                   for _ in range(parse.MAX_PARSE_ATTEMPTS)]
        self.setup_doc("D1", entries)
        plan = self.run_plan()
        self.assertEqual(plan["counts"]["targets"], 0)
        self.assertEqual(plan["counts"]["skipped"]["retired"], 1)
        self.assertEqual(plan["retired"][0]["doc_id"], "D1")
        self.assertEqual(plan["retired"][0]["attempts"],
                         parse.MAX_PARSE_ATTEMPTS)
        self.assertEqual(plan["retired"][0]["last_reason"], "bad_zip")

    def test_targeted_below_max_failures(self):
        entries = [_parse_entry("D1", "failed", error_kind="OSError")
                   for _ in range(parse.MAX_PARSE_ATTEMPTS - 1)]
        self.setup_doc("D1", entries)
        plan = self.run_plan()
        self.assertEqual(plan["counts"]["targets"], 1)
        self.assertEqual(plan["retired"], [])

    def test_ok_resets_the_streak(self):
        entries = (
            [_parse_entry("D1", "quarantined", reason="bad_zip")
             for _ in range(parse.MAX_PARSE_ATTEMPTS)]
            + [_parse_entry("D1", "ok", zip_sha256="sha-old")]
            + [_parse_entry("D1", "quarantined", reason="zero_facts")])
        self.setup_doc("D1", entries)
        plan = self.run_plan()
        self.assertEqual(plan["counts"]["targets"], 1)
        self.assertEqual(plan["retired"], [])

    def test_explicit_doc_ids_bypass_retirement(self):
        entries = [_parse_entry("D1", "quarantined", reason="bad_zip")
                   for _ in range(parse.MAX_PARSE_ATTEMPTS + 2)]
        self.setup_doc("D1", entries)
        plan = self.run_plan(doc_ids="D1")
        self.assertEqual(plan["counts"]["targets"], 1)
        self.assertEqual(plan["retired"], [])


class FailureStreaksTest(unittest.TestCase):
    def test_counts_only_consecutive_non_ok(self):
        ledgers = {"2026-11": [
            {"doc_id": "A", "status": "quarantined"},
            {"doc_id": "A", "status": "failed"},
            {"doc_id": "B", "status": "failed"},
            {"doc_id": "A", "status": "ok"},
            {"doc_id": "A", "status": "quarantined"},
            {"doc_id": "B", "status": "ok"},
        ]}
        streaks = parse.failure_streaks(ledgers)
        self.assertEqual(streaks, {"A": 1, "B": 0})


if __name__ == "__main__":
    unittest.main()
