"""pipeline.discovery のテスト (S2-T5 §1)。

実行: リポジトリのルートで `python -m unittest discover -s tests`
"""
import json
import os
import sys
import tempfile
import unittest
import urllib.request
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from huggingface_hub.errors import EntryNotFoundError

from pipeline import discovery
from pipeline.discovery import EdinetClient, EdinetError, run_acquisition

ZIP_BODY = b"PK\x03\x04 fake zip"
PDF_BODY = b"%PDF-1.7 fake pdf"
ERR_429 = b'{"StatusCode": 429, "message": "Too Many Requests"}'
ERR_403 = b'{"StatusCode": 403, "message": "Forbidden"}'


def make_doc(doc_id, submit, xbrl=1, pdf=1, attach=0, english=0):
    return {
        "docID": doc_id, "submitDateTime": submit,
        "xbrlFlag": xbrl, "pdfFlag": pdf,
        "attachDocFlag": attach, "englishDocFlag": english,
        "filerName": "テスト提出者", "docTypeCode": "120",
    }


class FakeClient:
    """run_acquisition 用の偽 EDINET クライアント。"""

    def __init__(self, docs, bodies, errors=None):
        self._docs = docs
        self._bodies = bodies
        self._errors = errors or {}
        self.request_count = 0
        self.doc_calls = []

    def fetch_list(self, date):
        return {"metadata": {"status": "200"}, "results": self._docs}

    def fetch_document(self, doc_id, type_):
        self.doc_calls.append((doc_id, type_))
        self.request_count += 1
        if (doc_id, type_) in self._errors:
            raise self._errors[(doc_id, type_)]
        return self._bodies[(doc_id, type_)]

    def _sanitize(self, text):
        return str(text)


class FakeHfApi:
    """remote を dict で保持する偽 HfApi。"""

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


def make_hf_download(remote, cache_dir):
    def fake_hf_hub_download(repo_id, filename, repo_type="dataset",
                             local_dir=None, token=None):
        if filename not in remote:
            raise EntryNotFoundError(filename)
        os.makedirs(cache_dir, exist_ok=True)
        path = os.path.join(
            cache_dir, filename.replace("/", "_"))
        with open(path, "wb") as f:
            f.write(remote[filename])
        return path
    return fake_hf_hub_download


def read_ledger(remote, path):
    rows = []
    for line in remote[path].decode("utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def latest_by_key(rows, key_of):
    latest = {}
    for row in rows:
        latest[key_of(row)] = row
    return latest


class RunAcquisitionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.remote = {}
        self.api = FakeHfApi(self.remote)
        download = make_hf_download(
            self.remote, os.path.join(self.tmp.name, "cache"))
        patchers = [
            mock.patch.object(discovery, "HfApi", lambda: self.api),
            mock.patch.object(discovery, "hf_hub_download", download),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

    def work_dir(self, name):
        return os.path.join(self.tmp.name, name)

    def acq_latest(self, month="2026-11"):
        rows = read_ledger(
            self.remote, f"acquisition/month={month}/ledger.jsonl")
        return latest_by_key(rows, lambda e: f"{e['doc_id']}:{e['kind']}")

    def test_multiple_submit_dates_all_uploaded(self):
        docs = [
            make_doc("S100AAAA", "2026-11-16 10:00"),
            make_doc("S100BBBB", "2026-11-13 15:30"),
        ]
        bodies = {
            ("S100AAAA", 1): ZIP_BODY, ("S100AAAA", 2): PDF_BODY,
            ("S100BBBB", 1): ZIP_BODY, ("S100BBBB", 2): PDF_BODY,
        }
        client = FakeClient(docs, bodies)
        summary = run_acquisition("2026-11-16", self.work_dir("w1"),
                                  client=client)
        self.assertEqual(summary["failed"], 0)
        self.assertEqual(summary["downloaded"], 4)
        raw_paths = {p for p in self.remote if p.startswith("raw/")}
        self.assertEqual(raw_paths, {
            "raw/edinet/2026/11/16/zip/S100AAAA.zip",
            "raw/edinet/2026/11/16/pdf/S100AAAA.pdf",
            "raw/edinet/2026/11/13/zip/S100BBBB.zip",
            "raw/edinet/2026/11/13/pdf/S100BBBB.pdf",
        })
        latest = self.acq_latest()
        ok_paths = {e["path"] for e in latest.values()
                    if e["status"] == "ok"}
        self.assertEqual(ok_paths, raw_paths)

    def test_idempotent_second_run(self):
        docs = [make_doc("S100AAAA", "2026-11-16 10:00")]
        bodies = {("S100AAAA", 1): ZIP_BODY, ("S100AAAA", 2): PDF_BODY}
        client = FakeClient(docs, bodies)
        run_acquisition("2026-11-16", self.work_dir("w1"), client=client)
        calls_after_first = len(client.doc_calls)
        summary = run_acquisition("2026-11-16", self.work_dir("w2"),
                                  client=client)
        self.assertEqual(len(client.doc_calls), calls_after_first)
        self.assertEqual(summary["skipped_done"], 2)
        rows = read_ledger(
            self.remote, "acquisition/month=2026-11/ledger.jsonl")
        ok_rows = [r for r in rows if r["status"] == "ok"]
        self.assertEqual(len(ok_rows), 2)

    def test_failure_isolated_per_document(self):
        docs = [
            make_doc("S100AAAA", "2026-11-16 10:00"),
            make_doc("S100CCCC", "2026-11-16 11:00"),
        ]
        bodies = {
            ("S100AAAA", 1): ZIP_BODY, ("S100AAAA", 2): PDF_BODY,
            ("S100CCCC", 1): ZIP_BODY,
        }
        errors = {("S100CCCC", 2): EdinetError("boom")}
        client = FakeClient(docs, bodies, errors)
        summary = run_acquisition("2026-11-16", self.work_dir("w1"),
                                  client=client)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["downloaded"], 3)
        latest = self.acq_latest()
        self.assertEqual(latest["S100AAAA:pdf"]["status"], "ok")
        self.assertEqual(latest["S100CCCC:zip"]["status"], "ok")
        self.assertEqual(latest["S100CCCC:pdf"]["status"], "failed")

    def test_recovers_when_ledger_ok_but_remote_missing(self):
        doc = make_doc("S100AAAA", "2026-11-16 10:00")
        stale = [
            {"doc_id": "S100AAAA", "kind": "zip", "status": "ok",
             "path": "raw/edinet/2026/11/16/zip/S100AAAA.zip",
             "sha256": "x", "size": 1, "updated_at": "2026-11-16T00:00:00"},
            {"doc_id": "S100AAAA", "kind": "pdf", "status": "ok",
             "path": "raw/edinet/2026/11/16/pdf/S100AAAA.pdf",
             "sha256": "x", "size": 1, "updated_at": "2026-11-16T00:00:00"},
        ]
        self.remote["acquisition/month=2026-11/ledger.jsonl"] = (
            "".join(json.dumps(r) + "\n" for r in stale)).encode("utf-8")
        bodies = {("S100AAAA", 1): ZIP_BODY, ("S100AAAA", 2): PDF_BODY}
        client = FakeClient([doc], bodies)
        summary = run_acquisition("2026-11-16", self.work_dir("w1"),
                                  client=client)
        self.assertEqual(summary["downloaded"], 2)
        self.assertEqual(len(client.doc_calls), 2)
        self.assertIn("raw/edinet/2026/11/16/zip/S100AAAA.zip", self.remote)
        self.assertIn("raw/edinet/2026/11/16/pdf/S100AAAA.pdf", self.remote)

    def test_run_summaries_not_overwritten(self):
        docs = [make_doc("S100AAAA", "2026-11-16 10:00")]
        bodies = {("S100AAAA", 1): ZIP_BODY, ("S100AAAA", 2): PDF_BODY}
        client = FakeClient(docs, bodies)
        run_acquisition("2026-11-16", self.work_dir("w1"), client=client)
        summaries = {p: b for p, b in self.remote.items()
                     if "run_summary" in p}
        self.assertEqual(len(summaries), 1)
        first_path, first_bytes = next(iter(summaries.items()))
        self.assertTrue(first_path.startswith(
            "acquisition/month=2026-11/run_summary-2026-11-16-"))
        empty_client = FakeClient([], {})
        run_acquisition("2026-11-17", self.work_dir("w2"),
                        client=empty_client)
        summaries_after = {p for p in self.remote if "run_summary" in p}
        self.assertEqual(len(summaries_after), 2)
        self.assertEqual(self.remote[first_path], first_bytes)


class FakeResponse:
    def __init__(self, status, ctype, body):
        self.status = status
        self.headers = {"Content-Type": ctype}
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return self._body


class EdinetClientTest(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.queue = []
        patcher = mock.patch("time.sleep", lambda s: None)
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher2 = mock.patch.object(
            urllib.request, "urlopen", self._urlopen)
        patcher2.start()
        self.addCleanup(patcher2.stop)
        self.client = EdinetClient("TESTKEY", interval=0)

    def _urlopen(self, req, timeout=None):
        self.calls.append(req.full_url)
        return self.queue.pop(0)

    def respond(self, status, ctype, body):
        self.queue.append(FakeResponse(status, ctype, body))

    def test_body_429_retried_then_success(self):
        self.respond(200, "application/json", ERR_429)
        self.respond(200, "application/zip", ZIP_BODY)
        body = self.client.fetch_document("S100AAAA", 1)
        self.assertEqual(body, ZIP_BODY)
        self.assertEqual(len(self.calls), 2)

    def test_body_429_with_non_json_content_type_retried(self):
        self.respond(200, "text/plain", ERR_429)
        self.respond(200, "application/zip", ZIP_BODY)
        body = self.client.fetch_document("S100AAAA", 1)
        self.assertEqual(body, ZIP_BODY)
        self.assertEqual(len(self.calls), 2)

    def test_persistent_body_429_is_retryable_failure(self):
        for _ in range(10):
            self.respond(200, "application/json", ERR_429)
        with self.assertRaises(discovery._RetryableHttpError):
            self.client.fetch_document("S100AAAA", 1)
        self.assertEqual(len(self.calls), 6)

    def test_body_403_is_permanent_error(self):
        self.respond(200, "application/json", ERR_403)
        with self.assertRaises(EdinetError):
            self.client.fetch_document("S100AAAA", 1)
        self.assertEqual(len(self.calls), 1)

    def test_body_error_with_non_json_content_type_detected(self):
        self.respond(200, "text/plain", ERR_403)
        with self.assertRaises(EdinetError):
            self.client.fetch_document("S100AAAA", 1)
        self.assertEqual(len(self.calls), 1)

    def test_http_500_retried(self):
        self.respond(500, "text/html", b"server error")
        self.respond(200, "application/zip", ZIP_BODY)
        body = self.client.fetch_document("S100AAAA", 1)
        self.assertEqual(body, ZIP_BODY)
        self.assertEqual(len(self.calls), 2)

    def test_format_validation_rejects_non_zip(self):
        self.respond(200, "application/octet-stream", b"hello")
        with self.assertRaises(EdinetError):
            self.client.fetch_document("S100AAAA", 1)

    def test_format_validation_rejects_non_pdf(self):
        self.respond(200, "application/octet-stream", ZIP_BODY)
        with self.assertRaises(EdinetError):
            self.client.fetch_document("S100AAAA", 2)

    def test_format_validation_accepts_pdf(self):
        self.respond(200, "application/pdf", PDF_BODY)
        body = self.client.fetch_document("S100AAAA", 2)
        self.assertEqual(body, PDF_BODY)

    def test_fetch_list_success(self):
        payload = {"metadata": {"status": "200", "message": "OK"},
                   "results": [{"docID": "S100AAAA"}]}
        self.respond(200, "application/json",
                     json.dumps(payload).encode("utf-8"))
        data = self.client.fetch_list("2026-11-16")
        self.assertEqual(data["results"][0]["docID"], "S100AAAA")

    def test_fetch_list_body_429_retried(self):
        self.respond(200, "application/json", ERR_429)
        payload = {"metadata": {"status": "200", "message": "OK"},
                   "results": []}
        self.respond(200, "application/json",
                     json.dumps(payload).encode("utf-8"))
        data = self.client.fetch_list("2026-11-16")
        self.assertEqual(data["results"], [])
        self.assertEqual(len(self.calls), 2)


class RunWithRealClient429Test(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.remote = {}
        self.api = FakeHfApi(self.remote)
        download = make_hf_download(
            self.remote, os.path.join(self.tmp.name, "cache"))
        self.doc_requests = 0
        for p in [
            mock.patch.object(discovery, "HfApi", lambda: self.api),
            mock.patch.object(discovery, "hf_hub_download", download),
            mock.patch("time.sleep", lambda s: None),
            mock.patch.object(urllib.request, "urlopen", self._urlopen),
        ]:
            p.start()
            self.addCleanup(p.stop)

    def _urlopen(self, req, timeout=None):
        url = req.full_url
        if "documents.json" in url:
            payload = {"metadata": {"status": "200", "message": "OK"},
                       "results": [make_doc("S100AAAA",
                                            "2026-11-16 10:00",
                                            xbrl=1, pdf=0)]}
            return FakeResponse(200, "application/json",
                                json.dumps(payload).encode("utf-8"))
        self.doc_requests += 1
        return FakeResponse(200, "application/json", ERR_429)

    def test_429_documents_fail_retryable_and_nothing_saved(self):
        client = EdinetClient("TESTKEY", interval=0)
        summary = run_acquisition(
            "2026-11-16", os.path.join(self.tmp.name, "w1"), client=client)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["downloaded"], 0)
        self.assertEqual(self.doc_requests, 6)
        self.assertFalse(any(p.startswith("raw/") for p in self.remote))
        rows = read_ledger(
            self.remote, "acquisition/month=2026-11/ledger.jsonl")
        latest = latest_by_key(rows, lambda e: f"{e['doc_id']}:{e['kind']}")
        entry = latest["S100AAAA:zip"]
        self.assertEqual(entry["status"], "failed")
        self.assertEqual(entry["error_kind"], "_RetryableHttpError")


class RecordingHfApi(FakeHfApi):
    """取得目録の push のたびに、内容と遠隔の状態を記録する偽 HfApi。"""

    def __init__(self, remote=None):
        super().__init__(remote)
        self.acq_pushes = []

    def upload_file(self, path_or_fileobj, path_in_repo, repo_id,
                    repo_type="dataset", commit_message=None):
        if (path_in_repo.startswith("acquisition/")
                and path_in_repo.endswith("ledger.jsonl")):
            with open(path_or_fileobj, "rb") as f:
                content = f.read()
            ok_paths = set()
            for line in content.decode("utf-8").splitlines():
                if line.strip():
                    entry = json.loads(line)
                    if entry.get("status") == "ok":
                        ok_paths.add(entry["path"])
            self.acq_pushes.append((ok_paths, set(self.remote)))
        return super().upload_file(
            path_or_fileobj, path_in_repo, repo_id,
            repo_type=repo_type, commit_message=commit_message)


class DroppingHfApi(FakeHfApi):
    """upload_folder が何も上げない偽 HfApi (実在を確認できない状況)。"""

    def upload_folder(self, folder_path, repo_id, repo_type="dataset",
                      path_in_repo=None, commit_message=None):
        return None


class InterimPushTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.remote = {}
        self.api = RecordingHfApi(self.remote)
        download = make_hf_download(
            self.remote, os.path.join(self.tmp.name, "cache"))
        patchers = [
            mock.patch.object(discovery, "HfApi", lambda: self.api),
            mock.patch.object(discovery, "hf_hub_download", download),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

    def work_dir(self, name):
        return os.path.join(self.tmp.name, name)

    def test_ledger_pushed_incrementally_and_ok_always_exists(self):
        docs = [
            make_doc("S100AAAA", "2026-11-16 10:00"),
            make_doc("S100BBBB", "2026-11-13 15:30"),
        ]
        bodies = {
            ("S100AAAA", 1): ZIP_BODY, ("S100AAAA", 2): PDF_BODY,
            ("S100BBBB", 1): ZIP_BODY, ("S100BBBB", 2): PDF_BODY,
        }
        client = FakeClient(docs, bodies)
        summary = run_acquisition("2026-11-16", self.work_dir("w1"),
                                  client=client)
        self.assertEqual(summary["failed"], 0)
        self.assertGreaterEqual(len(self.api.acq_pushes), 2)
        for ok_paths, remote_paths in self.api.acq_pushes:
            self.assertLessEqual(ok_paths, remote_paths)

    def test_catalog_survives_crash_during_document_fetch(self):
        class CrashingClient(FakeClient):
            def fetch_document(self, doc_id, type_):
                raise KeyboardInterrupt

        docs = [make_doc("S100AAAA", "2026-11-16 10:00")]
        client = CrashingClient(docs, {})
        with self.assertRaises(KeyboardInterrupt):
            run_acquisition("2026-11-16", self.work_dir("w1"),
                            client=client)
        rows = read_ledger(
            self.remote, "catalog/month=2026-11/ledger.jsonl")
        self.assertEqual([r["docID"] for r in rows], ["S100AAAA"])
        self.assertFalse(any(p.startswith("raw/") for p in self.remote))


class UploadUnconfirmedTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.remote = {}
        self.api = DroppingHfApi(self.remote)
        download = make_hf_download(
            self.remote, os.path.join(self.tmp.name, "cache"))
        patchers = [
            mock.patch.object(discovery, "HfApi", lambda: self.api),
            mock.patch.object(discovery, "hf_hub_download", download),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

    def test_unconfirmed_uploads_become_failed_rows(self):
        docs = [make_doc("S100AAAA", "2026-11-16 10:00")]
        bodies = {("S100AAAA", 1): ZIP_BODY, ("S100AAAA", 2): PDF_BODY}
        client = FakeClient(docs, bodies)
        summary = run_acquisition(
            "2026-11-16", os.path.join(self.tmp.name, "w1"), client=client)
        self.assertEqual(summary["downloaded"], 2)
        self.assertEqual(summary["failed"], 2)
        rows = read_ledger(
            self.remote, "acquisition/month=2026-11/ledger.jsonl")
        latest = latest_by_key(rows, lambda e: f"{e['doc_id']}:{e['kind']}")
        for kind in ("zip", "pdf"):
            entry = latest[f"S100AAAA:{kind}"]
            self.assertEqual(entry["status"], "failed")
            self.assertEqual(entry["error_kind"], "upload_unconfirmed")


if __name__ == "__main__":
    unittest.main()
