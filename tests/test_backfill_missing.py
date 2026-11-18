"""pipeline.backfill_missing と照合の分類 (S2-T6) のテスト。

実行: リポジトリのルートで `python -m unittest discover -s tests`
"""
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub.errors import EntryNotFoundError

from pipeline import backfill_missing
from pipeline.discovery import EdinetError

ZIP_BODY = b"PK\x03\x04 fake zip"

_INDEX_COLUMNS = {
    "doc_id": pa.string(), "submit_at": pa.string(),
    "doc_type": pa.string(), "parent_doc_id": pa.string(),
    "edinet_code": pa.string(), "code": pa.string(),
    "company_name": pa.string(),
    "xbrl_flag": pa.bool_(), "pdf_flag": pa.bool_(),
    "csv_flag": pa.bool_(), "attachment_flag": pa.bool_(),
    "english_flag": pa.bool_(), "raw_zip_path": pa.string(),
}


def index_row(doc_id, submit_at, xbrl=True, raw_zip_path=None,
              doc_type="120"):
    return {"doc_id": doc_id, "submit_at": submit_at,
            "doc_type": doc_type, "parent_doc_id": None,
            "edinet_code": "E00001", "code": "1234",
            "company_name": "テスト提出者",
            "xbrl_flag": xbrl, "pdf_flag": True, "csv_flag": True,
            "attachment_flag": False, "english_flag": False,
            "raw_zip_path": raw_zip_path}


def write_index(path, rows):
    schema = pa.schema(list(_INDEX_COLUMNS.items()))
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)


class FakeClient:
    """run_acquire 用の偽 EDINET クライアント。"""

    def __init__(self, list_docs=None, bodies=None, errors=None,
                 list_errors=None):
        self._list_docs = list_docs or {}
        self._bodies = bodies or {}
        self._errors = errors or {}
        self._list_errors = list_errors or {}
        self.request_count = 0
        self.doc_calls = []
        self.list_calls = []

    def fetch_list(self, date):
        self.list_calls.append(date)
        self.request_count += 1
        if date in self._list_errors:
            raise self._list_errors[date]
        return {"metadata": {"status": "200"},
                "results": self._list_docs.get(date, [])}

    def fetch_document(self, doc_id, type_):
        self.doc_calls.append((doc_id, type_))
        self.request_count += 1
        if doc_id in self._errors:
            raise self._errors[doc_id]
        return self._bodies[doc_id]

    def _sanitize(self, text):
        return str(text)


class FakeHfApi:
    def __init__(self, remote=None):
        self.remote = remote if remote is not None else {}

    def list_repo_files(self, repo, repo_type="dataset"):
        return sorted(self.remote)

    def upload_file(self, path_or_fileobj, path_in_repo, repo_id,
                    repo_type="dataset", commit_message=None):
        with open(path_or_fileobj, "rb") as f:
            self.remote[path_in_repo] = f.read()

    def upload_folder(self, folder_path, repo_id, repo_type="dataset",
                      path_in_repo=None, commit_message=None):
        for root, _, files in os.walk(folder_path):
            for name in files:
                full = os.path.join(root, name)
                rel = os.path.relpath(full, folder_path).replace(os.sep, "/")
                with open(full, "rb") as f:
                    self.remote[f"{path_in_repo}/{rel}"] = f.read()


class NoUploadHfApi(FakeHfApi):
    """upload_folder が何も上げない偽 HF (upload_unconfirmed の負系用)。"""

    def upload_folder(self, folder_path, repo_id, repo_type="dataset",
                      path_in_repo=None, commit_message=None):
        return None


def make_hf_download(remote, cache_dir):
    def fake_hf_hub_download(repo_id, filename, repo_type="dataset",
                             local_dir=None, token=None):
        if filename not in remote:
            raise EntryNotFoundError(filename)
        os.makedirs(cache_dir, exist_ok=True)
        path = os.path.join(cache_dir, filename.replace("/", "_"))
        with open(path, "wb") as f:
            f.write(remote[filename])
        return path
    return fake_hf_hub_download


def read_ledger(remote, path):
    return [json.loads(line)
            for line in remote[path].decode("utf-8").splitlines()
            if line.strip()]


class SelectTargetsTest(unittest.TestCase):
    def test_selects_only_no_zip_xbrl_in_period(self):
        rows = [
            index_row("A", "2026-03-13 09:00"),
            index_row("B", "2026-03-13 09:00",
                      raw_zip_path="raw/edinet/year=2026/month=03/day=13/zip/B.zip"),
            index_row("C", "2026-03-13 09:00", xbrl=False),
            index_row("D", "2020-01-01 09:00"),
            index_row("E", "2024-11-15 09:05"),
        ]
        targets = backfill_missing.select_targets(rows)
        self.assertEqual([r["doc_id"] for r in targets], ["E", "A"])

    def test_catalog_doc_from_index_fills_seconds(self):
        doc = backfill_missing.catalog_doc_from_index(
            index_row("A", "2024-11-15 09:05"))
        self.assertEqual(doc["submitDateTime"], "2024-11-15 09:05:00")
        self.assertEqual(doc["docID"], "A")
        self.assertEqual(doc["xbrlFlag"], "1")
        self.assertEqual(doc["englishDocFlag"], "0")


class RunAcquireTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.remote = {}
        self.api = FakeHfApi(self.remote)
        download = make_hf_download(
            self.remote, os.path.join(self.tmp.name, "cache"))
        patchers = [
            mock.patch.object(backfill_missing, "HfApi", lambda: self.api),
            mock.patch.object(backfill_missing, "hf_hub_download", download),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        self.index_path = os.path.join(self.tmp.name, "index.parquet")
        write_index(self.index_path, [
            index_row("OK1", "2026-03-13 09:00"),
            index_row("NG1", "2026-03-13 10:00"),
            index_row("DONE1", "2024-11-15 09:05",
                      raw_zip_path="raw/edinet/x.zip"),
        ])
        self.work_dir = os.path.join(self.tmp.name, "work")

    def test_acquire_ok_and_failed_are_recorded(self):
        listed = {"docID": "OK1",
                  "submitDateTime": "2026-03-13 09:00:00",
                  "docTypeCode": "120", "parentDocID": None,
                  "xbrlFlag": "1"}
        client = FakeClient(
            list_docs={"2026-03-13": [listed]},
            bodies={"OK1": ZIP_BODY},
            errors={"NG1": EdinetError("取得できませんでした")})
        summary = backfill_missing.run_acquire(
            self.index_path, self.work_dir, repo="fake/repo",
            client=client)
        self.assertEqual(summary["targets"], 2)
        self.assertEqual(summary["downloaded"], 1)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["acquired_doc_ids"], ["OK1"])
        self.assertEqual(summary["failed_doc_ids"], ["NG1"])
        # 原形が discovery と同じ保存先に置かれる
        self.assertIn("raw/edinet/2026/03/13/zip/OK1.zip", self.remote)
        # 取得目録: ok と failed (理由つき) の両方が残る
        acq = read_ledger(self.remote,
                          "acquisition/month=2026-03/ledger.jsonl")
        latest = {e["doc_id"]: e for e in acq}
        self.assertEqual(latest["OK1"]["status"], "ok")
        self.assertEqual(latest["NG1"]["status"], "failed")
        self.assertEqual(latest["NG1"]["error_kind"], "EdinetError")
        # 書類目録: 一覧で観測できた OK1 は edinet_list_api、
        # 観測できなかった NG1 は v1_index_fallback と明記される
        cat = read_ledger(self.remote, "catalog/month=2026-03/ledger.jsonl")
        cat_latest = {e["docID"]: e for e in cat}
        self.assertEqual(cat_latest["OK1"]["catalog_source"],
                         "edinet_list_api")
        self.assertEqual(cat_latest["NG1"]["catalog_source"],
                         "v1_index_fallback")
        # zip (type=1) だけを取得する
        self.assertEqual({t for _, t in client.doc_calls}, {1})

    def test_rerun_skips_done(self):
        client = FakeClient(bodies={"OK1": ZIP_BODY, "NG1": ZIP_BODY})
        backfill_missing.run_acquire(self.index_path, self.work_dir,
                                     repo="fake/repo", client=client)
        calls_after_first = len(client.doc_calls)
        summary = backfill_missing.run_acquire(
            self.index_path, self.work_dir, repo="fake/repo",
            client=client)
        self.assertEqual(summary["skipped_done"], 2)
        self.assertEqual(len(client.doc_calls), calls_after_first)

    def test_upload_unconfirmed_is_failed_not_ok(self):
        index_path = os.path.join(self.tmp.name, "index_up.parquet")
        write_index(index_path, [index_row("UP1", "2026-03-13 09:00")])
        api = NoUploadHfApi(self.remote)
        client = FakeClient(bodies={"UP1": ZIP_BODY})
        with mock.patch.object(backfill_missing, "HfApi", lambda: api):
            summary = backfill_missing.run_acquire(
                index_path, os.path.join(self.tmp.name, "work_up"),
                repo="fake/repo", client=client)
        self.assertEqual(summary["acquired_doc_ids"], [])
        self.assertEqual(summary["failed_doc_ids"], ["UP1"])
        self.assertEqual(summary["failed"], 1)
        acq = read_ledger(self.remote,
                          "acquisition/month=2026-03/ledger.jsonl")
        self.assertTrue(acq)
        self.assertTrue(all(e["status"] != "ok" for e in acq))
        latest = {e["doc_id"]: e for e in acq}
        self.assertEqual(latest["UP1"]["status"], "failed")
        self.assertEqual(latest["UP1"]["error_kind"], "upload_unconfirmed")

    def test_failed_doc_is_retried_on_rerun(self):
        client = FakeClient(
            bodies={"OK1": ZIP_BODY},
            errors={"NG1": EdinetError("取得できませんでした")})
        first = backfill_missing.run_acquire(
            self.index_path, self.work_dir, repo="fake/repo",
            client=client)
        self.assertEqual(first["failed_doc_ids"], ["NG1"])
        client2 = FakeClient(bodies={"OK1": ZIP_BODY, "NG1": ZIP_BODY})
        second = backfill_missing.run_acquire(
            self.index_path, self.work_dir, repo="fake/repo",
            client=client2)
        self.assertEqual(second["skipped_done"], 1)
        self.assertEqual(second["failed"], 0)
        self.assertEqual(second["acquired_doc_ids"], ["NG1", "OK1"])
        self.assertIn(("NG1", 1), client2.doc_calls)
        acq = read_ledger(self.remote,
                          "acquisition/month=2026-03/ledger.jsonl")
        latest = {e["doc_id"]: e for e in acq}
        self.assertEqual(latest["NG1"]["status"], "ok")

    def test_max_docs_stops_early(self):
        client = FakeClient(bodies={"OK1": ZIP_BODY, "NG1": ZIP_BODY})
        summary = backfill_missing.run_acquire(
            self.index_path, self.work_dir, repo="fake/repo",
            client=client, max_docs=1)
        self.assertEqual(summary["attempted"], 1)
        self.assertTrue(summary["stopped_early"])
        self.assertEqual(summary["downloaded"], 1)
        self.assertEqual(summary["acquired_doc_ids"], ["OK1"])

    def test_list_failure_falls_back_to_index(self):
        client = FakeClient(
            bodies={"OK1": ZIP_BODY, "NG1": ZIP_BODY},
            list_errors={"2026-03-13": EdinetError("一覧が取れません")})
        summary = backfill_missing.run_acquire(
            self.index_path, self.work_dir, repo="fake/repo",
            client=client)
        self.assertEqual(summary["downloaded"], 2)
        self.assertEqual(len(summary["list_failures"]), 1)
        cat = read_ledger(self.remote, "catalog/month=2026-03/ledger.jsonl")
        self.assertTrue(all(e["catalog_source"] == "v1_index_fallback"
                            for e in cat))


def _load_reconcile():
    path = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))),
        "verification", "reconcile_backfill.py")
    spec = importlib.util.spec_from_file_location("reconcile_backfill",
                                                  path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ClassifyMissingTest(unittest.TestCase):
    def setUp(self):
        self.reconcile = _load_reconcile()

    def test_classification(self):
        no_zip = [{"doc_id": d, "submit_at": "2026-03-13 09:00"}
                  for d in ("REC", "UNO", "QUA", "UNE")]
        parse_latest = {
            "REC": {"status": "ok", "rows": 10},
            "QUA": {"status": "quarantined", "reason": "zero_facts"},
        }
        acq_latest = {
            "REC": {"status": "ok"},
            "UNO": {"status": "failed", "error_kind": "EdinetError",
                    "error": "取得できませんでした"},
            "QUA": {"status": "ok"},
        }
        doc_rows = {"REC": 10}
        result = self.reconcile.classify_missing(
            no_zip, parse_latest, acq_latest, doc_rows)
        self.assertEqual(result["recovered"], ["REC"])
        self.assertEqual([e["doc_id"] for e in result["unobtainable"]],
                         ["UNO"])
        self.assertEqual([e["doc_id"] for e in result["quarantined"]],
                         ["QUA"])
        self.assertEqual(result["unexplained"], ["UNE"])

    def _classify_one(self, acq_entry):
        no_zip = [{"doc_id": "X", "submit_at": "2026-03-13 09:00"}]
        acq_latest = {} if acq_entry is None else {"X": acq_entry}
        return self.reconcile.classify_missing(no_zip, {}, acq_latest, {})

    def test_temporary_failures_are_unexplained_not_unobtainable(self):
        cases = {
            "upload_unconfirmed": {
                "status": "failed", "error_kind": "upload_unconfirmed",
                "error": "アップロード後に遠隔で実在を確認できませんでした"},
            "retryable_http": {
                "status": "failed", "error_kind": "_RetryableHttpError",
                "error": "HTTP 429"},
            "os_error": {
                "status": "failed", "error_kind": "OSError",
                "error": "Connection reset by peer"},
            "no_reason": {
                "status": "failed", "error_kind": None, "error": None},
            "missing_fields": {"status": "failed"},
            "edinet_error_empty_reason": {
                "status": "failed", "error_kind": "EdinetError",
                "error": ""},
            "edinet_error_no_reason": {
                "status": "failed", "error_kind": "EdinetError",
                "error": None},
            "edinet_error_blank_reason": {
                "status": "failed", "error_kind": "EdinetError",
                "error": "  "},
        }
        for name, entry in cases.items():
            with self.subTest(case=name):
                result = self._classify_one(entry)
                self.assertEqual(result["unobtainable"], [])
                self.assertEqual(result["unexplained"], ["X"])

    def test_edinet_error_with_reason_is_unobtainable(self):
        result = self._classify_one({
            "status": "failed", "error_kind": "EdinetError",
            "error": "取得できませんでした"})
        self.assertEqual([e["doc_id"] for e in result["unobtainable"]],
                         ["X"])
        self.assertEqual(result["unexplained"], [])

    def test_row_count_mismatch_is_unexplained(self):
        no_zip = [{"doc_id": "REC", "submit_at": "2026-03-13 09:00"}]
        result = self.reconcile.classify_missing(
            no_zip, {"REC": {"status": "ok", "rows": 10}},
            {"REC": {"status": "ok"}}, {"REC": 9})
        self.assertEqual(result["unexplained"], ["REC"])


if __name__ == "__main__":
    unittest.main()
