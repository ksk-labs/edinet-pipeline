"""pipeline.extension_backfill の回帰テスト (Issue #25 残件)。

- 出所の合成行は v1 索引の列を catalog 形式へ写し、証券コードの
  "JP:" 接頭辞を外す。索引に無い項目は None のまま残す。
- 対象の算出は、標準要素と既存ラベルでカバー済みの組を除く。
- 結果の分類は resolved_text / no_label_text (resolved_empty) /
  in_facts_standard / not_in_facts の4つに分かれ、未解決は理由つきで
  台帳に残る。
- ラベルの統合は既存行優先 (parse.run_merge と同じ規則) で冪等。
  同じ作業ディレクトリで再実行しても追加行は増えない。
- 隔離 (Quarantine) の書類は完了として台帳に残り、再実行で飛ばす。
- 書類ごとの書き込みはラベル (shard) が先で台帳が後 (SF-1)。shard の
  書き込みに失敗した書類は台帳に完了と記録されず再実行で処理し直され、
  ラベルだけが残った状態も再実行で通常実行と同じ結果に収束する。
"""
import io
import json
import os
import shutil
import tarfile
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub.errors import EntryNotFoundError

from pipeline import extension_backfill as eb
from pipeline import parse

NS = ("http://disclosure.edinet-fsa.go.jp/jpcrp030000/asr/001/"
      "E00001-000/2026-03-31/01/2026-11-16")
MONTH = "2020-01"


def _index_row(doc_id, day="02"):
    return {
        "doc_id": doc_id, "edinet_code": "E00001", "code": "JP:12340",
        "company_name": "提出者株式会社",
        "submit_at": f"2020-01-{day} 09:00", "doc_type": "120",
        "fund_code": None, "parent_doc_id": None,
        "raw_zip_path": f"raw/edinet/year=2020/month=01/day={day}"
                        f"/zip/{doc_id}.zip",
        "ope_date_time": None,
    }


def _label(element, ja="表示名", en="Label", ns=NS):
    return {"element": element, "element_ns": ns,
            "label_ja": ja, "label_en": en}


class CatalogRowTest(unittest.TestCase):
    def test_maps_index_columns_and_strips_jp_prefix(self):
        row = eb.catalog_row_from_index(_index_row("D1"))
        self.assertEqual(row["docID"], "D1")
        self.assertEqual(row["edinetCode"], "E00001")
        self.assertEqual(row["filerName"], "提出者株式会社")
        self.assertEqual(row["secCode"], "12340")
        self.assertIsNone(row["fundCode"])
        self.assertEqual(row["docTypeCode"], "120")
        self.assertEqual(row["submitDateTime"], "2020-01-02 09:00")
        self.assertEqual(row["source"], "v1_index")
        self.assertEqual(row["source_id"], "D1")
        self.assertIsNone(row["fetched_at"])
        self.assertEqual(row["redistribution"], "allowed")

    def test_missing_code_stays_none(self):
        src = _index_row("D2")
        src["code"] = None
        self.assertIsNone(eb.catalog_row_from_index(src)["secCode"])


class TarPathTest(unittest.TestCase):
    def test_from_logical_path(self):
        self.assertEqual(
            eb.tar_path_of("raw/edinet/year=2016/month=03/day=22"
                           "/zip/S10074JN.zip"),
            "raw/edinet/2016/03/22.tar")

    def test_unparseable_returns_none(self):
        self.assertIsNone(eb.tar_path_of(None))
        self.assertIsNone(eb.tar_path_of("raw/other/path.zip"))


class ClassifyTest(unittest.TestCase):
    def test_four_outcomes(self):
        labels = [_label("E1:Foo", ja="フー"),
                  _label("E1:Bar", ja=None, en=None)]
        counts, unresolved = eb.classify_targets(
            ["E1:Foo", "E1:Bar", "ifrs-full:X", "E1:Ghost"],
            {"E1:Foo", "E1:Bar", "ifrs-full:X"}, labels)
        self.assertEqual(counts, {"resolved_text": 1, "resolved_empty": 1,
                                  "in_facts_standard": 1, "not_in_facts": 1})
        reasons = {u["element"]: u["reason"] for u in unresolved}
        self.assertEqual(reasons, {"E1:Bar": "no_label_text",
                                   "ifrs-full:X": "in_facts_standard",
                                   "E1:Ghost": "not_in_facts"})


class MergeLabelsTest(unittest.TestCase):
    def test_existing_wins_and_result_is_deterministic(self):
        existing = [dict(_label("E1:Foo", ja="既存"), doc_id="D1")]
        new = [dict(_label("E1:Foo", ja="新規"), doc_id="D1"),
               dict(_label("E1:Bar"), doc_id="D2"),
               dict(_label("E1:Aaa"), doc_id="D2")]
        merged, added = eb.merge_label_rows(existing, new)
        self.assertEqual(added, 2)
        self.assertEqual(merged[0]["label_ja"], "既存")
        self.assertEqual([r["element"] for r in merged[1:]],
                         ["E1:Aaa", "E1:Bar"])
        again, added2 = eb.merge_label_rows(merged, new)
        self.assertEqual(added2, 0)
        self.assertEqual(again, merged)


class StandardKeysCacheTest(unittest.TestCase):
    def test_cache_is_used_without_fetching(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        cache = os.path.join(tmp, "keys.json")
        with open(cache, "w", encoding="utf-8") as f:
            json.dump(["jppfs_cor:Cash"], f)

        def boom(url):
            raise AssertionError("fetch しないはず")

        self.assertEqual(eb.load_standard_keys(cache, fetcher=boom),
                         {"jppfs_cor:Cash"})


class RunMonthTest(unittest.TestCase):
    """tar・索引・抽出を偽装した run_month の通しテスト。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.work = os.path.join(self.tmp, "work")
        # Silver: D1 (拡張2 + 標準1)、D2 (拡張1 + ifrs 1)、D3 (隔離)
        silver = os.path.join(self.tmp, "silver.parquet")
        pq.write_table(pa.table({
            "doc_id": ["D1", "D1", "D1", "D2", "D2", "D3"],
            "element": ["E00001-000:Foo", "E00001-000:Bar",
                        "jppfs_cor:Cash", "E00002-000:Baz",
                        "ifrs-full:Something", "E00003-000:Q"],
        }), silver)
        self.silver = silver
        # 日次 tar (2020-01-02) に D1・D2・D3 の zip を入れる
        tar_path = os.path.join(self.tmp, "day.tar")
        with tarfile.open(tar_path, "w") as tf:
            for doc_id in ("D1", "D2", "D3"):
                data = f"dummy-{doc_id}".encode()
                info = tarfile.TarInfo(f"zip/{doc_id}.zip")
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
        self.tar = tar_path
        self.index = {d: _index_row(d) for d in ("D1", "D2", "D3")}

        def fake_download(repo_id, filename, repo_type=None, revision=None,
                          local_dir=None, token=None):
            if filename.endswith("silver.parquet"):
                return self.silver
            if filename.endswith(".tar"):
                dest = os.path.join(local_dir, filename)
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                shutil.copyfile(self.tar, dest)
                return dest
            raise EntryNotFoundError(filename)

        def fake_extract(cntlr, zip_path):
            doc_id = os.path.basename(zip_path)[:-len(".zip")]
            if doc_id == "D3":
                raise parse.Quarantine("bad_zip", "開けません")
            if doc_id == "D1":
                return {"facts": [{"element": "E00001-000:Foo"},
                                  {"element": "E00001-000:Bar"}],
                        "stats": {},
                        "labels": {
                            (NS, "Foo"): _label("E00001-000:Foo", ja="フー"),
                            (NS, "Bar"): _label("E00001-000:Bar",
                                                ja=None, en=None)},
                        "xbrl_files": []}
            return {"facts": [{"element": "ifrs-full:Something"}],
                    "stats": {}, "labels": {}, "xbrl_files": []}

        patches = [
            mock.patch.object(eb, "hf_hub_download",
                              side_effect=fake_download),
            mock.patch.object(parse, "_new_cntlr",
                              return_value=object()),
            mock.patch.object(parse, "extract_facts",
                              side_effect=fake_extract),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def _run(self):
        return eb.run_month(None, MONTH, index=self.index,
                            standard_keys={"jppfs_cor:Cash"},
                            work_dir=self.work, upload=False,
                            log=lambda *a: None)

    def test_run_classifies_and_writes_outputs(self):
        summary = self._run()
        self.assertEqual(summary["target_docs"], 3)
        self.assertEqual(summary["target_pairs"], 5)
        self.assertEqual(summary["done_docs"], 3)
        self.assertEqual(summary["pending_docs"], 0)
        self.assertEqual(summary["ok_docs"], 2)
        self.assertEqual(summary["quarantined_docs"], 1)
        self.assertEqual(summary["resolved_text"], 1)
        self.assertEqual(summary["resolved_empty"], 1)
        self.assertEqual(summary["in_facts_standard"], 1)
        self.assertEqual(summary["not_in_facts"], 1)
        self.assertEqual(summary["unresolved_pairs"], 3)
        self.assertEqual(summary["label_rows_added"], 2)
        self.assertEqual(summary["label_rows_total"], 2)
        labels_out = os.path.join(
            self.work, "out", "parse", f"month={MONTH}",
            "extension_labels.jsonl")
        with open(labels_out, encoding="utf-8") as f:
            rows = [json.loads(line) for line in f if line.strip()]
        self.assertEqual({r["element"] for r in rows},
                         {"E00001-000:Foo", "E00001-000:Bar"})
        prov_out = os.path.join(
            self.work, "out", "dictionary", "backfill", f"month={MONTH}",
            "catalog.jsonl")
        with open(prov_out, encoding="utf-8") as f:
            prov = [json.loads(line) for line in f if line.strip()]
        self.assertEqual([r["docID"] for r in prov], ["D1"])
        self.assertEqual(prov[0]["source"], "v1_index")
        self.assertEqual(prov[0]["secCode"], "12340")

    def test_rerun_is_idempotent(self):
        first = self._run()
        labels_out = os.path.join(
            self.work, "out", "parse", f"month={MONTH}",
            "extension_labels.jsonl")
        with open(labels_out, "rb") as f:
            before = f.read()
        second = self._run()
        # 台帳で全書類が完了済みになるため再抽出は走らず、統合結果の
        # バイト列は初回と一致する (追加行数は現行リモートに対する
        # 差分の数え方であり、upload しないローカル実行では変わらない)。
        self.assertEqual(second["label_rows_added"],
                         first["label_rows_added"])
        self.assertEqual(second["label_rows_total"],
                         first["label_rows_total"])
        self.assertEqual(second["done_docs"], first["done_docs"])
        with open(labels_out, "rb") as f:
            self.assertEqual(f.read(), before)

    def test_shard_write_failure_keeps_doc_out_of_ledger(self):
        # SF-1 の回帰: shard のパスをディレクトリにして初回の書き込みを
        # 失敗させる。ラベルが書けなかった書類は台帳に完了 (ok) と
        # 記録されず、障害を取り除いた再実行で処理し直されて通常実行と
        # 同じ結果になる。台帳が先行する実装では、初回で ok が記録され、
        # 再実行で書類が飛ばされてラベルが欠けたままになる。
        shard = os.path.join(self.work, "labels", f"month={MONTH}.jsonl")
        os.makedirs(shard)
        with self.assertRaises(IsADirectoryError):
            self._run()
        ledger_path = os.path.join(self.work, "ledger",
                                   f"month={MONTH}.jsonl")
        entries = []
        if os.path.exists(ledger_path):
            with open(ledger_path, encoding="utf-8") as f:
                entries = [json.loads(line) for line in f if line.strip()]
        self.assertEqual(
            [e for e in entries if e.get("status") == "ok"], [])
        os.rmdir(shard)
        summary = self._run()
        self.assertEqual(summary["done_docs"], 3)
        self.assertEqual(summary["resolved_text"], 1)
        self.assertEqual(summary["label_rows_total"], 2)
        self.assertEqual(summary["label_rows_added"], 2)

    def test_labels_without_ledger_entry_converge_on_rerun(self):
        # 逆向きの不整合: 前回実行がラベル行を書いた直後に落ち、台帳に
        # 記録が無い状態を作る。再実行では再抽出で shard に重複行が
        # 増えるが、統合時の重複除去で通常実行と同じ出力に収束する。
        shard_dir = os.path.join(self.work, "labels")
        os.makedirs(shard_dir)
        rows = [dict(_label("E00001-000:Foo", ja="フー"), doc_id="D1"),
                dict(_label("E00001-000:Bar", ja=None, en=None),
                     doc_id="D1")]
        with open(os.path.join(shard_dir, f"month={MONTH}.jsonl"),
                  "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        summary = self._run()
        self.assertEqual(summary["resolved_text"], 1)
        self.assertEqual(summary["label_rows_total"], 2)
        self.assertEqual(summary["label_rows_added"], 2)
        labels_out = os.path.join(
            self.work, "out", "parse", f"month={MONTH}",
            "extension_labels.jsonl")
        with open(labels_out, encoding="utf-8") as f:
            out_rows = [json.loads(line) for line in f if line.strip()]
        self.assertEqual(len(out_rows), 2)
        self.assertEqual({r["element"] for r in out_rows},
                         {"E00001-000:Foo", "E00001-000:Bar"})


if __name__ == "__main__":
    unittest.main()
