"""pipeline.codelist のテスト (S2-T5 §4)。

実行: リポジトリのルートで `python -m unittest discover -s tests`
"""
import io
import json
import os
import sys
import tempfile
import unittest
import zipfile
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from huggingface_hub.errors import EntryNotFoundError

from pipeline import codelist
from pipeline.codelist import (
    CodelistError, compute_diff, parse_codelist_zip, run_snapshot,
)

COLUMNS = codelist.EXPECTED_COLUMNS


def make_row(code, name="テスト株式会社", capital="1000", industry="建設業"):
    values = {
        "ＥＤＩＮＥＴコード": code,
        "提出者種別": "内国法人・組合",
        "上場区分": "上場",
        "連結の有無": "有",
        "資本金": capital,
        "決算日": "3月31日",
        "提出者名": name,
        "提出者名（英字）": "TEST CO., LTD.",
        "提出者名（ヨミ）": "テストカブシキガイシャ",
        "所在地": "東京都千代田区",
        "提出者業種": industry,
        "証券コード": "12340",
        "提出者法人番号": "1234567890123",
    }
    return [values[c] for c in COLUMNS]


def make_csv_bytes(date_jp, rows, declared_count=None, header=None):
    count = len(rows) if declared_count is None else declared_count
    lines = [f"ダウンロード実行日,{date_jp}現在,件数,{count}件",
             ",".join(header if header is not None else COLUMNS)]
    for row in rows:
        lines.append(",".join(f'"{v}"' for v in row))
    return ("\r\n".join(lines) + "\r\n").encode("cp932")


def make_zip(csv_bytes, inner_name="EdinetcodeDlInfo.csv"):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(inner_name, csv_bytes)
    return buf.getvalue()


def sample_zip(date_jp="2026年11月17日", rows=None):
    if rows is None:
        rows = [make_row("E00001"), make_row("E00002", name="第二株式会社")]
    return make_zip(make_csv_bytes(date_jp, rows))


class ParseTest(unittest.TestCase):
    def test_parse_valid(self):
        snap = parse_codelist_zip(sample_zip())
        self.assertEqual(snap["declared_date"], "2026-11-17")
        self.assertEqual(snap["declared_count"], 2)
        self.assertEqual(snap["inner_name"], "EdinetcodeDlInfo.csv")
        self.assertEqual(len(snap["rows"]), 2)
        row = snap["by_code"]["E00001"]
        self.assertEqual(row["提出者名"], "テスト株式会社")
        self.assertEqual(row["提出者業種"], "建設業")
        self.assertEqual(list(row), COLUMNS)

    def test_parse_count_mismatch_fails(self):
        csv_bytes = make_csv_bytes("2026年11月17日", [make_row("E00001")],
                                   declared_count=5)
        with self.assertRaises(CodelistError):
            parse_codelist_zip(make_zip(csv_bytes))

    def test_parse_header_mismatch_fails(self):
        header = list(COLUMNS)
        header[10] = "業種"
        csv_bytes = make_csv_bytes("2026年11月17日", [make_row("E00001")],
                                   header=header)
        with self.assertRaises(CodelistError):
            parse_codelist_zip(make_zip(csv_bytes))

    def test_parse_duplicate_code_fails(self):
        rows = [make_row("E00001"), make_row("E00001", name="重複株式会社")]
        with self.assertRaises(CodelistError):
            parse_codelist_zip(sample_zip(rows=rows))

    def test_parse_no_csv_member_fails(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "hello")
        with self.assertRaises(CodelistError):
            parse_codelist_zip(buf.getvalue())

    def test_parse_not_zip_fails(self):
        with self.assertRaises(CodelistError):
            parse_codelist_zip(b"this is not a zip")


class DiffTest(unittest.TestCase):
    def _by_code(self, rows):
        return {r[0]: dict(zip(COLUMNS, r)) for r in rows}

    def test_added_removed_changed(self):
        old = self._by_code([
            make_row("E00001"),
            make_row("E00002", name="第二株式会社"),
            make_row("E00003", name="第三株式会社"),
        ])
        new = self._by_code([
            make_row("E00001", capital="2000"),
            make_row("E00003", name="第三株式会社"),
            make_row("E00004", name="第四株式会社"),
        ])
        added, removed, changed = compute_diff(old, new)
        self.assertEqual(list(added), ["E00004"])
        self.assertEqual(added["E00004"]["提出者名"], "第四株式会社")
        self.assertEqual(list(removed), ["E00002"])
        self.assertEqual(list(changed), ["E00001"])
        self.assertEqual(changed["E00001"], {"資本金": ["1000", "2000"]})

    def test_identical_is_empty(self):
        rows = self._by_code([make_row("E00001")])
        added, removed, changed = compute_diff(rows, dict(rows))
        self.assertEqual((added, removed, changed), ({}, {}, {}))

    def test_initial_all_added(self):
        new = self._by_code([make_row("E00001"), make_row("E00002")])
        added, removed, changed = compute_diff({}, new)
        self.assertEqual(sorted(added), ["E00001", "E00002"])
        self.assertEqual((removed, changed), ({}, {}))


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


def read_ledger(remote):
    rows = []
    for line in remote["codelist/ledger.jsonl"].decode("utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


class RunSnapshotTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.remote = {}
        self.api = FakeHfApi(self.remote)
        download = make_hf_download(
            self.remote, os.path.join(self.tmp.name, "cache"))
        for p in [
            mock.patch.object(codelist, "HfApi", lambda: self.api),
            mock.patch.object(codelist, "hf_hub_download", download),
        ]:
            p.start()
            self.addCleanup(p.stop)

    def work_dir(self, name):
        return os.path.join(self.tmp.name, name)

    def test_first_run_saves_everything(self):
        summary = run_snapshot(self.work_dir("w1"),
                               fetcher=lambda: sample_zip())
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["snapshot_date"], "2026-11-17")
        self.assertEqual(summary["row_count"], 2)
        base = "codelist/date=2026-11-17"
        for path in (f"{base}/EdinetcodeDlInfo.csv",
                     f"{base}/snapshot.json",
                     f"{base}/diff.json",
                     f"{base}/changes.jsonl",
                     "codelist/archive/Edinetcode-2026-11-17.zip"):
            self.assertIn(path, self.remote)
        diff = json.loads(self.remote[f"{base}/diff.json"].decode("utf-8"))
        self.assertIsNone(diff["base_date"])
        self.assertEqual(diff["added"], 2)
        self.assertEqual(diff["current_count"], 2)
        changes = [json.loads(line) for line in
                   self.remote[f"{base}/changes.jsonl"]
                   .decode("utf-8").splitlines()]
        self.assertEqual([c["change"] for c in changes], ["added", "added"])
        meta = json.loads(self.remote[f"{base}/snapshot.json"].decode("utf-8"))
        self.assertEqual(meta["declared_count"], 2)
        self.assertEqual(meta["inner_file"], "EdinetcodeDlInfo.csv")
        ledger = read_ledger(self.remote)
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger[0]["status"], "ok")
        self.assertEqual(ledger[0]["date"], "2026-11-17")

    def test_second_run_idempotent(self):
        zip_bytes = sample_zip()
        run_snapshot(self.work_dir("w1"), fetcher=lambda: zip_bytes)
        saved = dict(self.remote)
        summary = run_snapshot(self.work_dir("w2"), fetcher=lambda: zip_bytes)
        self.assertEqual(summary["status"], "already_present")
        self.assertEqual(summary["uploaded"], [])
        for path, data in saved.items():
            if path == "codelist/ledger.jsonl" or "/runs/" in path:
                continue
            self.assertEqual(self.remote[path], data)
        ledger = read_ledger(self.remote)
        self.assertEqual([r["status"] for r in ledger],
                         ["ok", "already_present"])

    def test_next_day_diff(self):
        run_snapshot(self.work_dir("w1"), fetcher=lambda: sample_zip())
        rows = [
            make_row("E00001", capital="2000"),
            make_row("E00003", name="第三株式会社"),
        ]
        zip2 = sample_zip("2026年11月18日", rows)
        summary = run_snapshot(self.work_dir("w2"), fetcher=lambda: zip2)
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["diff_base_date"], "2026-11-17")
        self.assertEqual(
            (summary["added"], summary["removed"], summary["changed"]),
            (1, 1, 1))
        changes = [json.loads(line) for line in
                   self.remote["codelist/date=2026-11-18/changes.jsonl"]
                   .decode("utf-8").splitlines()]
        by_code = {c["edinet_code"]: c for c in changes}
        self.assertEqual(by_code["E00001"]["change"], "changed")
        self.assertEqual(by_code["E00001"]["fields"],
                         {"資本金": ["1000", "2000"]})
        self.assertEqual(by_code["E00002"]["change"], "removed")
        self.assertEqual(by_code["E00003"]["change"], "added")

    def test_conflict_keeps_first(self):
        run_snapshot(self.work_dir("w1"), fetcher=lambda: sample_zip())
        first_csv = self.remote["codelist/date=2026-11-17/EdinetcodeDlInfo.csv"]
        other = sample_zip(rows=[make_row("E00009", name="別株式会社")])
        summary = run_snapshot(self.work_dir("w2"), fetcher=lambda: other)
        self.assertEqual(summary["status"], "conflict")
        self.assertEqual(
            self.remote["codelist/date=2026-11-17/EdinetcodeDlInfo.csv"],
            first_csv)
        ledger = read_ledger(self.remote)
        self.assertEqual(ledger[-1]["status"], "conflict")
        self.assertIn("existing_sha256_csv", ledger[-1])

    def test_fetch_failure_recorded(self):
        def boom():
            raise CodelistError("network down")
        summary = run_snapshot(self.work_dir("w1"), fetcher=boom)
        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["error_kind"], "CodelistError")
        self.assertFalse(
            any(p.startswith("codelist/date=") for p in self.remote))
        ledger = read_ledger(self.remote)
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger[0]["status"], "failed")
        self.assertIsNone(ledger[0]["date"])

    def test_validation_failure_recorded(self):
        bad = make_zip(make_csv_bytes("2026年11月17日",
                                      [make_row("E00001")],
                                      declared_count=99))
        summary = run_snapshot(self.work_dir("w1"), fetcher=lambda: bad)
        self.assertEqual(summary["status"], "failed")
        self.assertFalse(
            any(p.startswith("codelist/date=") for p in self.remote))
        ledger = read_ledger(self.remote)
        self.assertEqual(ledger[0]["status"], "failed")

    def test_resume_completes_missing_products(self):
        zip_bytes = sample_zip()
        snap = parse_codelist_zip(zip_bytes)
        base = "codelist/date=2026-11-17"
        self.remote[f"{base}/EdinetcodeDlInfo.csv"] = snap["csv_bytes"]
        summary = run_snapshot(self.work_dir("w1"),
                               fetcher=lambda: zip_bytes)
        self.assertEqual(summary["status"], "already_present")
        self.assertIn(f"{base}/snapshot.json", self.remote)
        self.assertIn(f"{base}/diff.json", self.remote)
        self.assertIn("codelist/archive/Edinetcode-2026-11-17.zip",
                      self.remote)
        self.assertEqual(
            self.remote[f"{base}/EdinetcodeDlInfo.csv"], snap["csv_bytes"])


if __name__ == "__main__":
    unittest.main()
