"""pipeline.ledger.JsonlLedger と pipeline.batch.Ledger の統合の回帰テスト。

T-3 (S2-T5 塊1 レビュー) で重複を解消し、両者は共通土台
pipeline.ledger.JsonlLedger の特殊形になった。ここでは統合前の両実装が
持っていた振る舞いが土台と特殊形で保たれることを固定する:
- 追記と最新行の状態、再読み込みでの復元
- 壊れた行の無視と corrupt_lines の数え上げ
- キーが取れない行を状態に数えないこと
- batch.Ledger の attempts の数え上げと is_done の判定
- バックフィル経路の回帰: run_batch の処理済み判定と combine が、
  統合後の Ledger でも従来どおり動くこと (小さな合成ジョブで確認)
"""
import hashlib
import io
import json
import os
import tarfile
import tempfile
import unittest
import zipfile

import pyarrow.parquet as pq

from pipeline.batch import Ledger, combine, run_batch
from pipeline.discovery import JsonlLedger as DiscoveryLedger
from pipeline.ledger import JsonlLedger


def _write(path, lines):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


class JsonlLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "sub", "ledger.jsonl")

    def test_append_and_reload_latest(self):
        ledger = JsonlLedger(self.path, lambda e: e.get("docID"))
        ledger.append({"docID": "A", "status": "failed"})
        ledger.append({"docID": "A", "status": "ok"})
        ledger.append({"docID": "B", "status": "ok"})
        self.assertEqual(ledger.latest["A"]["status"], "ok")
        self.assertEqual(ledger.attempts["A"], 2)
        reloaded = JsonlLedger(self.path, lambda e: e.get("docID"))
        self.assertEqual(reloaded.latest, ledger.latest)
        self.assertEqual(reloaded.attempts["A"], 2)
        self.assertEqual(reloaded.corrupt_lines, 0)

    def test_corrupt_and_keyless_lines_are_ignored(self):
        _write(self.path, [
            json.dumps({"docID": "A", "status": "ok"}),
            '{"docID": "B", "status": ',  # 中断で切れた行
            json.dumps({"status": "ok"}),  # キーが取れない行
            "",
        ])
        ledger = JsonlLedger(self.path, lambda e: e.get("docID"))
        self.assertEqual(list(ledger.latest), ["A"])
        self.assertEqual(ledger.corrupt_lines, 1)
        self.assertEqual(sum(ledger.attempts.values()), 1)

    def test_discovery_reexports_the_shared_class(self):
        self.assertIs(DiscoveryLedger, JsonlLedger)


class BatchLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "ledger.jsonl")
        self.shard = os.path.join(self.tmp.name, "S1.parquet")

    def test_attempts_and_latest_match_legacy_behavior(self):
        ledger = Ledger(self.path)
        ledger.append({"doc_id": "S1", "status": "failed",
                       "error_kind": "convert_error"})
        ledger.append({"doc_id": "S1", "status": "ok", "rows": 3,
                       "zip_sha256": "abc"})
        self.assertEqual(ledger.attempts["S1"], 2)
        reloaded = Ledger(self.path)
        self.assertEqual(reloaded.attempts["S1"], 2)
        self.assertEqual(reloaded.latest["S1"]["status"], "ok")

    def test_is_done_requires_ok_hash_and_shard(self):
        ledger = Ledger(self.path)
        ledger.append({"doc_id": "S1", "status": "ok", "zip_sha256": "abc"})
        self.assertFalse(ledger.is_done("S1", "abc", self.shard))
        with open(self.shard, "wb") as f:
            f.write(b"x")
        self.assertTrue(ledger.is_done("S1", "abc", self.shard))
        self.assertFalse(ledger.is_done("S1", "different", self.shard))
        self.assertFalse(ledger.is_done("S2", "abc", self.shard))
        ledger.append({"doc_id": "S1", "status": "failed"})
        self.assertFalse(ledger.is_done("S1", "abc", self.shard))

    def test_batch_and_discovery_views_agree_on_same_file(self):
        ledger = Ledger(self.path)
        ledger.append({"doc_id": "S1", "status": "ok", "docID": "S1"})
        shared = JsonlLedger(self.path, lambda e: e.get("doc_id"))
        self.assertEqual(shared.latest["S1"], ledger.latest["S1"])


def _make_tar(tars_dir, day, doc_id, zip_bytes):
    os.makedirs(tars_dir, exist_ok=True)
    tar_path = os.path.join(tars_dir, f"{day}.tar")
    info = tarfile.TarInfo(f"raw/edinet/year={day[:4]}/month={day[5:7]}/"
                           f"day={day[8:10]}/zip/{doc_id}.zip")
    info.size = len(zip_bytes)
    with tarfile.open(tar_path, "w") as tf:
        tf.addfile(info, io.BytesIO(zip_bytes))
    return tar_path


def _make_doc_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("XBRL/PublicDoc/dummy.txt", "not xbrl")
    return buf.getvalue()


def _make_valid_doc_zip():
    xbrl = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<xbrl xmlns="http://www.xbrl.org/2003/instance" '
        'xmlns:jppfs="http://disclosure.edinet-fsa.go.jp/taxonomy/jppfs/'
        '2024-11-01/jppfs_cor" '
        'xmlns:iso4217="http://www.xbrl.org/2003/iso4217">'
        '<context id="CurrentYearDuration"><entity>'
        '<identifier scheme="http://disclosure.edinet-fsa.go.jp">E00001'
        '</identifier></entity><period>'
        '<startDate>2015-04-01</startDate><endDate>2016-03-31</endDate>'
        '</period></context>'
        '<unit id="JPY"><measure>iso4217:JPY</measure></unit>'
        '<jppfs:NetSales contextRef="CurrentYearDuration" unitRef="JPY" '
        'decimals="0" scale="6">123</jppfs:NetSales>'
        '<jppfs:OperatingIncome contextRef="CurrentYearDuration" '
        'unitRef="JPY" decimals="0" scale="6">45</jppfs:OperatingIncome>'
        '</xbrl>'
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("XBRL/PublicDoc/test.xbrl", xbrl)
    return buf.getvalue()


class BackfillRegressionTest(unittest.TestCase):
    """run_batch + combine の小さな合成回帰 (変換対象のない zip は失敗として
    隔離され目録の再読み込みでも状態が保たれることと、有効な XBRL を含む
    zip は ok として処理され再実行で飛ばされることを確認する)。"""

    def test_run_batch_records_failure_and_survives_reload(self):
        with tempfile.TemporaryDirectory() as tmp:
            tars_dir = os.path.join(tmp, "tars")
            state_dir = os.path.join(tmp, "state")
            out_dir = os.path.join(tmp, "out")
            zip_bytes = _make_doc_zip()
            _make_tar(tars_dir, "2016-03-22", "S100TEST", zip_bytes)
            jobs = [{"doc_id": "S100TEST", "doc_type": "120",
                     "parent_doc_id": None, "submit_at": "2016-03-22 10:00",
                     "raw_zip_path": "raw/edinet/year=2016/month=03/day=22/"
                                     "zip/S100TEST.zip"}]
            summary = run_batch(jobs, {}, {}, tars_dir, state_dir, out_dir)
            # XBRL を含まない zip は変換結果が空か失敗のどちらかになるが、
            # 全体は止まらず、目録に最新状態が残る。
            self.assertEqual(summary["jobs"], 1)
            ledger = Ledger(os.path.join(state_dir, "ledger.jsonl"))
            self.assertIn("S100TEST", ledger.latest)
            out_path, rows, _, missing = combine(jobs, state_dir, out_dir)
            self.assertTrue(os.path.exists(out_path))
            self.assertEqual(missing, [])

    def test_run_batch_success_path_records_ok_and_skips_on_rerun(self):
        with tempfile.TemporaryDirectory() as tmp:
            tars_dir = os.path.join(tmp, "tars")
            state_dir = os.path.join(tmp, "state")
            out_dir = os.path.join(tmp, "out")
            zip_bytes = _make_valid_doc_zip()
            _make_tar(tars_dir, "2016-03-22", "S100OK01", zip_bytes)
            jobs = [{"doc_id": "S100OK01", "doc_type": "120",
                     "parent_doc_id": None, "submit_at": "2016-03-22 10:00",
                     "raw_zip_path": "raw/edinet/year=2016/month=03/day=22/"
                                     "zip/S100OK01.zip"}]
            summary = run_batch(jobs, {}, {}, tars_dir, state_dir, out_dir)
            self.assertEqual(summary["jobs"], 1)
            self.assertEqual(summary["processed_ok"], 1)
            self.assertEqual(summary["failed"], 0)
            self.assertEqual(summary["rows_total"], 2)
            ledger_path = os.path.join(state_dir, "ledger.jsonl")
            ledger = Ledger(ledger_path)
            entry = ledger.latest["S100OK01"]
            self.assertEqual(entry["status"], "ok")
            self.assertEqual(entry["rows"], 2)
            self.assertEqual(entry["zip_sha256"],
                             hashlib.sha256(zip_bytes).hexdigest())
            self.assertEqual(ledger.attempts["S100OK01"], 1)
            shard_path = os.path.join(out_dir, "silver", "S100OK01.parquet")
            self.assertTrue(os.path.exists(shard_path))
            self.assertTrue(ledger.is_done(
                "S100OK01", entry["zip_sha256"], shard_path))
            out_path, rows, _, missing = combine(jobs, state_dir, out_dir)
            self.assertEqual(rows, 2)
            self.assertEqual(missing, [])
            combined = pq.read_table(out_path).to_pylist()
            self.assertEqual(
                {(r["element"], r["value"]) for r in combined},
                {("jppfs_cor:NetSales", 123000000.0),
                 ("jppfs_cor:OperatingIncome", 45000000.0)})
            rerun = run_batch(jobs, {}, {}, tars_dir, state_dir, out_dir)
            self.assertEqual(rerun["processed_ok"], 0)
            self.assertEqual(rerun["skipped_done"], 1)
            self.assertEqual(rerun["failed"], 0)
            reloaded = Ledger(ledger_path)
            self.assertEqual(reloaded.attempts["S100OK01"], 1)
            self.assertEqual(reloaded.latest["S100OK01"]["status"], "ok")


if __name__ == "__main__":
    unittest.main()
