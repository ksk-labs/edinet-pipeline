"""pipeline.extension_dict の構築規則の回帰テスト (P2-T10)。

- 辞書は (doc_id, element, element_ns) の観測を保持し、重複は除く。
- 出所は書類目録から併記し、目録に無い書類は行を落とさずサマリに数える。
- 目録の最新行は fetched_at で決まり、入力順に依存しない。
- 表示名の食い違いは黙って潰さず、サマリに記録する。同一キーの競合では
  表示名のある組が、表示名の無い組より優先される。
- sync の部分月 + upload は、明示的な許可が無い限り拒否される。
- 出力はソート済みで、parquet の往復で内容が変わらない。
"""
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from pipeline import extension_dict as ed


def _label(doc_id, element, ns, ja="表示名", en="Label"):
    return {"doc_id": doc_id, "element": element, "element_ns": ns,
            "label_ja": ja, "label_en": en}


def _catalog(doc_id, code="E00001", name="提出者株式会社"):
    return {"docID": doc_id, "edinetCode": code, "filerName": name,
            "secCode": "12340", "fundCode": None,
            "docTypeCode": "120", "submitDateTime": "2026-11-16 09:00",
            "source": "edinet_api", "source_id": doc_id,
            "fetched_at": "2026-11-17T00:00:00+00:00",
            "redistribution": "allowed"}


NS_A = "http://disclosure.edinet-fsa.go.jp/jpcrp030000/asr/001/E00001-000/2026-03-31/01/2026-11-16"
NS_B = "http://disclosure.edinet-fsa.go.jp/jpcrp030000/asr/001/E00001-000/2026-03-31/01/2026-11-17"


class BuildDictionaryTest(unittest.TestCase):
    def test_provenance_is_attached_from_catalog(self):
        labels = [_label("D1", "E00001-000:Foo", NS_A)]
        catalog, _ = ed.catalog_index([_catalog("D1")])
        rows, summary = ed.build_dictionary(labels, catalog)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["element_local"], "Foo")
        self.assertEqual(row["edinet_code"], "E00001")
        self.assertEqual(row["filer_name"], "提出者株式会社")
        self.assertEqual(row["source"], "edinet_api")
        self.assertEqual(row["redistribution"], "allowed")
        self.assertEqual(summary["output_rows"], 1)
        self.assertEqual(summary["docs_without_catalog"], [])

    def test_missing_catalog_keeps_row_and_is_counted(self):
        labels = [_label("D9", "E00009-000:Bar", NS_A)]
        rows, summary = ed.build_dictionary(labels, {})
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["filer_name"])
        self.assertEqual(summary["docs_without_catalog"], ["D9"])

    def test_catalog_index_latest_row_wins(self):
        older = _catalog("D1")
        newer = dict(_catalog("D1"), fetched_at="2026-11-17T01:00:00+00:00")
        index, duplicates = ed.catalog_index([older, newer])
        self.assertEqual(duplicates, 1)
        self.assertEqual(index["D1"]["fetched_at"],
                         "2026-11-17T01:00:00+00:00")

    def test_catalog_index_latest_row_wins_when_reversed(self):
        older = _catalog("D1")
        newer = dict(_catalog("D1"),
                     fetched_at="2026-11-17T01:00:00+00:00",
                     filerName="新しい提出者")
        index, duplicates = ed.catalog_index([newer, older])
        self.assertEqual(duplicates, 1)
        self.assertEqual(index["D1"]["fetched_at"],
                         "2026-11-17T01:00:00+00:00")
        self.assertEqual(index["D1"]["filerName"], "新しい提出者")

    def test_catalog_index_tie_on_fetched_at_is_order_independent(self):
        first = dict(_catalog("D1"), filerName="あいうえお株式会社")
        second = dict(_catalog("D1"), filerName="かきくけこ株式会社")
        index_ab, _ = ed.catalog_index([first, second])
        index_ba, _ = ed.catalog_index([second, first])
        self.assertEqual(index_ab["D1"], index_ba["D1"])

    def test_exact_duplicates_are_removed(self):
        labels = [_label("D1", "E00001-000:Foo", NS_A),
                  _label("D1", "E00001-000:Foo", NS_A)]
        rows, summary = ed.build_dictionary(labels, {})
        self.assertEqual(len(rows), 1)
        self.assertEqual(summary["duplicate_rows"], 1)
        self.assertEqual(summary["key_conflicts"], [])

    def test_same_key_different_labels_is_a_key_conflict(self):
        labels = [_label("D1", "E00001-000:Foo", NS_A, ja="あ"),
                  _label("D1", "E00001-000:Foo", NS_A, ja="い")]
        rows, summary = ed.build_dictionary(labels, {})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["label_ja"], "あ")
        self.assertEqual(len(summary["key_conflicts"]), 1)

    def test_key_conflict_prefers_existing_label_over_none(self):
        for labels in (
                [_label("D1", "E00001-000:Foo", NS_A, ja=None, en=None),
                 _label("D1", "E00001-000:Foo", NS_A)],
                [_label("D1", "E00001-000:Foo", NS_A),
                 _label("D1", "E00001-000:Foo", NS_A, ja=None, en=None)]):
            rows, summary = ed.build_dictionary(labels, {})
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["label_ja"], "表示名")
            self.assertEqual(rows[0]["label_en"], "Label")
            self.assertEqual(len(summary["key_conflicts"]), 1)

    def test_key_conflict_partial_label_beats_no_label(self):
        labels = [_label("D1", "E00001-000:Foo", NS_A, ja=None, en=None),
                  _label("D1", "E00001-000:Foo", NS_A, ja=None, en="Label")]
        rows, _ = ed.build_dictionary(labels, {})
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["label_ja"])
        self.assertEqual(rows[0]["label_en"], "Label")

    def test_key_conflict_labelled_variants_keep_dictionary_order(self):
        labels = [_label("D1", "E00001-000:Foo", NS_A, ja=None, en=None),
                  _label("D1", "E00001-000:Foo", NS_A, ja="い", en="I"),
                  _label("D1", "E00001-000:Foo", NS_A, ja="あ", en="A")]
        rows, _ = ed.build_dictionary(labels, {})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["label_ja"], "あ")
        self.assertEqual(rows[0]["label_en"], "A")

    def test_same_element_across_docs_with_different_labels(self):
        labels = [_label("D1", "E00001-000:Foo", NS_A, ja="あ"),
                  _label("D2", "E00001-000:Foo", NS_B, ja="い")]
        rows, summary = ed.build_dictionary(labels, {})
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(summary["label_conflicts"]), 1)
        self.assertEqual(summary["label_conflicts"][0]["doc_ids"],
                         ["D1", "D2"])

    def test_invalid_rows_are_counted_not_dropped_silently(self):
        labels = [_label("D1", "E00001-000:Foo", NS_A),
                  {"doc_id": "D2", "element": "E00001-000:Bar"},
                  {"element": "E00001-000:Baz", "element_ns": NS_A}]
        rows, summary = ed.build_dictionary(labels, {})
        self.assertEqual(len(rows), 1)
        self.assertEqual(summary["input_rows"], 3)
        self.assertEqual(summary["invalid_rows"], 2)

    def test_missing_labels_are_counted(self):
        labels = [_label("D1", "E00001-000:Foo", NS_A, ja=None, en=None)]
        rows, summary = ed.build_dictionary(labels, {})
        self.assertEqual(summary["label_ja_missing"], 1)
        self.assertEqual(summary["label_en_missing"], 1)

    def test_rows_are_sorted_by_doc_and_element(self):
        labels = [_label("D2", "E00001-000:B", NS_A),
                  _label("D1", "E00001-000:B", NS_A),
                  _label("D1", "E00001-000:A", NS_A)]
        rows, _ = ed.build_dictionary(labels, {})
        keys = [(r["doc_id"], r["element"]) for r in rows]
        self.assertEqual(keys, sorted(keys))


class BuildFromFilesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def _write_jsonl(self, name, rows, extra=""):
        path = os.path.join(self.tmp, name)
        with open(path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
            f.write(extra)
        return path

    def test_parquet_round_trip_and_corrupt_lines_reported(self):
        labels_path = self._write_jsonl(
            "labels.jsonl",
            [_label("D1", "E00001-000:Foo", NS_A)],
            extra="{壊れた行\n")
        catalog_path = self._write_jsonl("catalog.jsonl", [_catalog("D1")])
        out = os.path.join(self.tmp, "dict.parquet")
        summary_path = os.path.join(self.tmp, "summary.json")
        summary = ed.build_from_files([labels_path], [catalog_path],
                                      out, summary_path)
        self.assertEqual(summary["output_rows"], 1)
        self.assertEqual(list(summary["corrupt_lines"].values()), [1])
        rows = ed.read_dictionary(out)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["label_ja"], "表示名")
        with open(summary_path, encoding="utf-8") as f:
            saved = json.load(f)
        self.assertEqual(saved["output_rows"], 1)


class SyncPartialUploadTest(unittest.TestCase):
    REMOTE = [
        "parse/month=2026-10/extension_labels.jsonl",
        "parse/month=2026-11/extension_labels.jsonl",
        "catalog/month=2026-10/ledger.jsonl",
        "catalog/month=2026-11/ledger.jsonl",
    ]

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        api = mock.MagicMock()
        api.list_repo_files.return_value = list(self.REMOTE)
        self.uploads = []
        patches = [
            mock.patch.object(ed, "HfApi", return_value=api),
            mock.patch.object(ed, "_download", return_value=True),
            mock.patch.object(
                ed, "build_from_files",
                return_value={"output_rows": 1, "months": []}),
            mock.patch.object(
                ed, "_upload",
                side_effect=lambda *a, **k: self.uploads.append(a)),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_partial_months_upload_is_refused(self):
        with self.assertRaises(SystemExit):
            ed.sync("repo", ["2026-11"], self.tmp, upload=True)
        self.assertEqual(self.uploads, [])

    def test_partial_months_upload_allowed_with_explicit_flag(self):
        summary = ed.sync("repo", ["2026-11"], self.tmp, upload=True,
                          allow_partial_upload=True)
        self.assertEqual(len(self.uploads), 2)
        self.assertEqual(summary["months"], ["2026-11"])

    def test_all_months_upload_is_allowed(self):
        summary = ed.sync("repo", ["2026-10", "2026-11"], self.tmp,
                          upload=True)
        self.assertEqual(len(self.uploads), 2)
        self.assertEqual(summary["months"], ["2026-10", "2026-11"])

    def test_no_months_upload_is_allowed(self):
        summary = ed.sync("repo", None, self.tmp, upload=True)
        self.assertEqual(len(self.uploads), 2)
        self.assertEqual(summary["months"], ["2026-10", "2026-11"])

    def test_partial_months_without_upload_is_allowed(self):
        ed.sync("repo", ["2026-11"], self.tmp, upload=False)
        self.assertEqual(self.uploads, [])


class SyncBackfillCatalogTest(unittest.TestCase):
    """sync は過去分の出所 (dictionary/backfill/) も併せて読む。"""

    REMOTE = [
        "parse/month=2026-11/extension_labels.jsonl",
        "catalog/month=2026-11/ledger.jsonl",
        "dictionary/backfill/month=2026-11/catalog.jsonl",
    ]

    def test_backfill_catalog_is_passed_to_build(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        api = mock.MagicMock()
        api.list_repo_files.return_value = list(self.REMOTE)
        captured = {}

        def fake_build(label_paths, catalog_paths, out_path, summary_path):
            captured["labels"] = list(label_paths)
            captured["catalogs"] = list(catalog_paths)
            return {"output_rows": 0}

        with mock.patch.object(ed, "HfApi", return_value=api), \
                mock.patch.object(ed, "_download", return_value=True), \
                mock.patch.object(ed, "build_from_files",
                                  side_effect=fake_build):
            ed.sync("repo", ["2026-11"], tmp, upload=False)
        self.assertEqual(len(captured["labels"]), 1)
        self.assertEqual(len(captured["catalogs"]), 2)
        self.assertTrue(any("dictionary_backfill" in p
                            for p in captured["catalogs"]))


if __name__ == "__main__":
    unittest.main()
