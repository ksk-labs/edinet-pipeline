"""pipeline.extension_dict の構築規則の回帰テスト (P2-T10)。

- 辞書は (doc_id, element, element_ns) の観測を保持し、重複は除く。
- 出所は書類目録から併記し、目録に無い書類は行を落とさずサマリに数える。
- 表示名の食い違いは黙って潰さず、サマリに記録する。
- 出力はソート済みで、parquet の往復で内容が変わらない。
"""
import json
import os
import shutil
import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main()
