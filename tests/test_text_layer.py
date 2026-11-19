"""テキスト層の器 (P2-T7) のテスト。

原文が加工されず保持されること、要素からカテゴリへの初期対応、
重複の畳み込み、代表者の観測の分解と未確定の扱い、会社マスタの
属性履歴の形への変換 (ATTRIBUTE_SCHEMA との一致) を固定する。
標本の値は実書類 (S100ZI7X、株式会社ビックカメラの有価証券
報告書、2026-11-17 提出) で実測した文字事実に基づく。
"""
import json
import os
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from unittest import mock

from pipeline.company_master import ATTRIBUTE_SCHEMA
from pipeline.text_layer import (
    CATEGORIES,
    build_items,
    build_observations,
    classify_element,
    main,
    run_build,
    run_download,
    split_representative,
    to_attribute_rows,
)

GENERATED_AT = "2026-11-19T00:00:00+00:00"
CODE_TO_ID = {"E03481": 101, "E00002": 2}
ZIP_SHA = "a" * 64

BUSINESS_TEXT = (
    '\n<h4>【事業の内容】</h4>\n'
    '<p style="text-align: left">当社グループは、家電専門店を'
    '運営しております。</p>\n'
)


def fact(element, raw_text, doc_id="S100ZI7X", code="E03481",
         submit_at="2026-11-17 10:59", **kw):
    base = {
        "source_kind": "edinet_api",
        "source_id": doc_id,
        "source_doc_id": doc_id,
        "edinet_code": code,
        "submit_at": submit_at,
        "element": element,
        "element_ns": "http://disclosure.edinet-fsa.go.jp/taxonomy/jpcrp/2025-11-01/jpcrp_cor",
        "raw_text": raw_text,
        "context_start": "2025-09-01",
        "context_end": "2026-08-31",
        "dimension": "",
        "certainty": "confirmed",
        "source_sha256": ZIP_SHA,
        "redistribution": "allowed",
    }
    base.update(kw)
    return base


def representative_fact(raw="代表取締役社長　秋保　徹", **kw):
    return fact("jpcrp_cor:TitleAndNameOfRepresentativeCoverPage", raw,
                context_start="2026-11-17", context_end="2026-11-17", **kw)


class ClassifyElementTest(unittest.TestCase):
    def test_measured_elements(self):
        cases = {
            "jpcrp_cor:TitleAndNameOfRepresentativeCoverPage":
                "representative",
            "jpcrp_cor:ClassesOfSharesEtcTextBlock": "class_shares",
            "jpcrp_cor:DescriptionOfBusinessTextBlock":
                "business_and_risks",
            "jpcrp_cor:BusinessRisksTextBlock": "business_and_risks",
            "jpcrp_cor:CompanyHistoryTextBlock": "history",
            "jpcrp_cor:DescriptionOfReportableSegmentsTextBlock":
                "segments",
            "jpcrp_cor:OverviewOfAffiliatedEntitiesTextBlock":
                "affiliated_entities",
        }
        for element, category in cases.items():
            self.assertEqual(classify_element(element), category)

    def test_unknown_element_is_unclassified(self):
        self.assertIsNone(
            classify_element("jpcrp_cor:SomeOtherTextBlock"))

    def test_all_categories_have_labels(self):
        self.assertEqual(set(CATEGORIES), {
            "representative", "class_shares", "business_and_risks",
            "history", "segments", "shareholder_benefits",
            "affiliated_entities"})


class BuildItemsTest(unittest.TestCase):
    def test_raw_text_preserved_verbatim(self):
        items, _ = build_items(
            [fact("jpcrp_cor:DescriptionOfBusinessTextBlock",
                  BUSINESS_TEXT)], CODE_TO_ID, GENERATED_AT)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["raw_text"], BUSINESS_TEXT)
        self.assertEqual(items[0]["category"], "business_and_risks")
        self.assertEqual(items[0]["category_label"], "事業の内容・リスク")
        self.assertEqual(items[0]["internal_id"], 101)
        self.assertEqual(items[0]["normalized_summary"], None)

    def test_unclassified_element_still_stored(self):
        items, stats = build_items(
            [fact("jpcrp_cor:SomeOtherTextBlock", "本文")],
            CODE_TO_ID, GENERATED_AT)
        self.assertEqual(len(items), 1)
        self.assertIsNone(items[0]["category"])
        self.assertEqual(stats["by_category"]["unclassified"], 1)

    def test_duplicates_folded(self):
        f = fact("jpcrp_cor:CompanyHistoryTextBlock", "沿革の本文")
        items, stats = build_items([f, dict(f)], CODE_TO_ID,
                                   GENERATED_AT)
        self.assertEqual(len(items), 1)
        self.assertEqual(stats["duplicates"], 1)

    def test_same_element_different_text_kept(self):
        f1 = fact("jpcrp_cor:CompanyHistoryTextBlock", "沿革その1")
        f2 = fact("jpcrp_cor:CompanyHistoryTextBlock", "沿革その2")
        items, _ = build_items([f1, f2], CODE_TO_ID, GENERATED_AT)
        self.assertEqual(len(items), 2)
        self.assertNotEqual(items[0]["text_id"], items[1]["text_id"])

    def test_unconfirmed_and_summary_carried(self):
        f = fact("jpcrp_cor:CompanyHistoryTextBlock", "沿革の本文",
                 certainty="unconfirmed", normalized_summary="要約")
        items, _ = build_items([f], CODE_TO_ID, GENERATED_AT)
        self.assertEqual(items[0]["certainty"], "unconfirmed")
        self.assertEqual(items[0]["normalized_summary"], "要約")

    def test_missing_certainty_defaults_to_unconfirmed(self):
        f = fact("jpcrp_cor:CompanyHistoryTextBlock", "沿革の本文")
        del f["certainty"]
        items, _ = build_items([f], CODE_TO_ID, GENERATED_AT)
        self.assertEqual(items[0]["certainty"], "unconfirmed")

    def test_invalid_certainty_defaults_to_unconfirmed(self):
        f = fact("jpcrp_cor:CompanyHistoryTextBlock", "沿革の本文",
                 certainty="unknown")
        items, _ = build_items([f], CODE_TO_ID, GENERATED_AT)
        self.assertEqual(items[0]["certainty"], "unconfirmed")

    def test_explicit_confirmed_kept(self):
        f = fact("jpcrp_cor:CompanyHistoryTextBlock", "沿革の本文",
                 certainty="confirmed")
        items, _ = build_items([f], CODE_TO_ID, GENERATED_AT)
        self.assertEqual(items[0]["certainty"], "confirmed")

    def test_explicit_category_wins_over_element_map(self):
        f = fact("jpcrp_cor:SomeOtherTextBlock", "優待の記述",
                 category="shareholder_benefits")
        items, _ = build_items([f], CODE_TO_ID, GENERATED_AT)
        self.assertEqual(items[0]["category"], "shareholder_benefits")
        self.assertEqual(items[0]["category_label"], "株主優待の記述")

    def test_unresolved_code_keeps_row_without_internal_id(self):
        f = fact("jpcrp_cor:CompanyHistoryTextBlock", "沿革",
                 code="E99999")
        items, stats = build_items([f], CODE_TO_ID, GENERATED_AT)
        self.assertIsNone(items[0]["internal_id"])
        self.assertEqual(stats["unresolved_codes"], 1)

    def test_missing_redistribution_defaults_to_unknown(self):
        f = fact("jpcrp_cor:CompanyHistoryTextBlock", "沿革")
        del f["redistribution"]
        items, _ = build_items([f], CODE_TO_ID, GENERATED_AT)
        self.assertEqual(items[0]["redistribution"], "unknown")

    def test_none_text_skipped(self):
        f = fact("jpcrp_cor:CompanyHistoryTextBlock", "沿革")
        f["raw_text"] = None
        items, stats = build_items([f], CODE_TO_ID, GENERATED_AT)
        self.assertEqual(items, [])
        self.assertEqual(stats["empty_text"], 1)


class SplitRepresentativeTest(unittest.TestCase):
    def test_fullwidth_space(self):
        self.assertEqual(split_representative("代表取締役社長　秋保　徹"),
                         ("代表取締役社長", "秋保　徹"))

    def test_halfwidth_space(self):
        self.assertEqual(split_representative("代表取締役 山田 太郎"),
                         ("代表取締役", "山田 太郎"))

    def test_no_space_cannot_split(self):
        self.assertEqual(split_representative("代表取締役社長秋保徹"),
                         (None, None))

    def test_empty(self):
        self.assertEqual(split_representative(""), (None, None))
        self.assertEqual(split_representative(None), (None, None))


class ObservationsTest(unittest.TestCase):
    def _observations(self, facts):
        items, _ = build_items(facts, CODE_TO_ID, GENERATED_AT)
        return build_observations(items, GENERATED_AT)

    def test_representative_observation(self):
        obs = self._observations([representative_fact()])
        self.assertEqual(len(obs), 1)
        self.assertEqual(obs[0]["attribute"], "representative")
        self.assertEqual(obs[0]["role"], "代表取締役社長")
        self.assertEqual(obs[0]["name"], "秋保　徹")
        self.assertEqual(obs[0]["value"], "代表取締役社長 秋保　徹")
        self.assertEqual(obs[0]["certainty"], "confirmed")
        self.assertEqual(obs[0]["observed_date"], "2026-11-17")
        self.assertEqual(obs[0]["internal_id"], 101)

    def test_unsplittable_is_unconfirmed(self):
        obs = self._observations(
            [representative_fact("代表取締役社長秋保徹")])
        self.assertEqual(len(obs), 1)
        self.assertIsNone(obs[0]["role"])
        self.assertIsNone(obs[0]["name"])
        self.assertEqual(obs[0]["value"], "代表取締役社長秋保徹")
        self.assertEqual(obs[0]["certainty"], "unconfirmed")

    def test_non_representative_facts_produce_no_observation(self):
        obs = self._observations(
            [fact("jpcrp_cor:DescriptionOfBusinessTextBlock",
                  BUSINESS_TEXT)])
        self.assertEqual(obs, [])


class ToAttributeRowsTest(unittest.TestCase):
    def _rows(self, facts):
        items, _ = build_items(facts, CODE_TO_ID, GENERATED_AT)
        obs = build_observations(items, GENERATED_AT)
        return to_attribute_rows(obs)

    def test_rows_fit_attribute_schema(self):
        rows = self._rows([representative_fact()])
        table = pa.Table.from_pylist(rows, schema=ATTRIBUTE_SCHEMA)
        self.assertEqual(table.num_rows, 1)
        self.assertEqual(table.column("attribute")[0].as_py(),
                         "representative")
        self.assertEqual(table.column("value")[0].as_py(),
                         "代表取締役社長 秋保　徹")

    def test_same_value_merges_with_count(self):
        f1 = representative_fact(doc_id="S100AAAA",
                                 submit_at="2025-11-20 10:00")
        f2 = representative_fact(doc_id="S100BBBB",
                                 submit_at="2026-11-17 10:59")
        rows = self._rows([f2, f1])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["first_observed_date"], "2025-11-20")
        self.assertEqual(rows[0]["last_observed_date"], "2026-11-17")
        self.assertEqual(rows[0]["observation_count"], 2)

    def test_value_change_makes_two_rows(self):
        f1 = representative_fact(doc_id="S100AAAA",
                                 submit_at="2025-11-20 10:00")
        f2 = representative_fact("代表取締役社長　溝口　貴治",
                                 doc_id="S100BBBB",
                                 submit_at="2026-11-17 10:59")
        rows = self._rows([f1, f2])
        self.assertEqual(len(rows), 2)
        self.assertEqual({r["value"] for r in rows},
                         {"代表取締役社長 秋保　徹",
                          "代表取締役社長 溝口　貴治"})

    def test_unconfirmed_not_handed_over(self):
        rows = self._rows([representative_fact("代表取締役社長秋保徹")])
        self.assertEqual(rows, [])


class RunBuildTest(unittest.TestCase):
    def test_build_writes_parquet_outputs(self):
        with tempfile.TemporaryDirectory() as td:
            master_dir = os.path.join(td, "master")
            os.makedirs(master_dir)
            codes = pa.table({
                "internal_id": pa.array([101], type=pa.int64()),
                "edinet_code": pa.array(["E03481"]),
            })
            pq.write_table(codes, os.path.join(
                master_dir, "edinet_codes.parquet"))
            facts_path = os.path.join(td, "facts.jsonl")
            facts = [
                representative_fact(),
                fact("jpcrp_cor:DescriptionOfBusinessTextBlock",
                     BUSINESS_TEXT),
            ]
            with open(facts_path, "w", encoding="utf-8") as f:
                for row in facts:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            result = run_build(td, facts_paths=[facts_path],
                               master_dir=master_dir)
            out_dir = result["out_dir"]
            items = pq.read_table(
                os.path.join(out_dir, "items.parquet")).to_pylist()
            self.assertEqual(len(items), 2)
            by_element = {i["element"]: i for i in items}
            business = by_element[
                "jpcrp_cor:DescriptionOfBusinessTextBlock"]
            self.assertEqual(business["raw_text"], BUSINESS_TEXT)
            obs = pq.read_table(os.path.join(
                out_dir, "representative_observations.parquet"))
            self.assertEqual(obs.num_rows, 1)
            summary = result["summary"]
            self.assertEqual(summary["items_total"], 2)
            self.assertEqual(summary["observations_total"], 1)
            self.assertEqual(
                summary["items_by_category"]["representative"], 1)
            with open(os.path.join(out_dir, "summary.json"),
                      encoding="utf-8") as f:
                saved = json.load(f)
            self.assertEqual(saved["items_total"], 2)


class RunDownloadTest(unittest.TestCase):
    """download の配置が HF キャッシュのリンクに依存しないことを固定する。

    hf_hub_download はキャッシュ内の相対シンボリックリンクを返す。
    配置先へ rename で移すとリンク先が解決できず開けなくなる
    (Issue #57) ため、配置は実体のコピーでなければならない。
    """

    PAYLOADS = {
        "raw/edinet/2026/11/17/zip/S100ZI7X.zip": b"zip-bytes",
        "master/edinet_codes.parquet": b"parquet-bytes",
    }

    def _fake_hf_hub_download(self, cache_root, requested):
        def fake(repo_id, filename, repo_type, local_dir=None):
            requested.append(filename)
            blob = os.path.join(cache_root, "blobs",
                                filename.replace("/", "_"))
            os.makedirs(os.path.dirname(blob), exist_ok=True)
            with open(blob, "wb") as f:
                f.write(self.PAYLOADS[filename])
            snap = os.path.join(cache_root, "snapshots", filename)
            os.makedirs(os.path.dirname(snap), exist_ok=True)
            os.symlink(os.path.relpath(blob, os.path.dirname(snap)), snap)
            return snap
        return fake

    def _run(self, argv_download):
        with tempfile.TemporaryDirectory() as tmp:
            requested = []
            fake = self._fake_hf_hub_download(
                os.path.join(tmp, "cache"), requested)
            zip_out = os.path.join(tmp, "work", "filing.zip")
            master_dir = os.path.join(tmp, "work", "master")
            with mock.patch("pipeline.text_layer.hf_hub_download", fake):
                argv_download(zip_out, master_dir)
            self.assertEqual(requested, list(self.PAYLOADS))
            for dest, payload in (
                    (zip_out, self.PAYLOADS[
                        "raw/edinet/2026/11/17/zip/S100ZI7X.zip"]),
                    (os.path.join(master_dir, "edinet_codes.parquet"),
                     self.PAYLOADS["master/edinet_codes.parquet"])):
                self.assertFalse(os.path.islink(dest))
                with open(dest, "rb") as f:
                    self.assertEqual(f.read(), payload)

    def test_run_download_places_real_files(self):
        def call(zip_out, master_dir):
            result = run_download(
                "Yoshi-Dai/financial-lakehouse-v2", "S100ZI7X",
                "2026-11-17", zip_out, master_dir)
            self.assertEqual(result, {"zip": zip_out, "master": os.path.join(
                master_dir, "edinet_codes.parquet")})
        self._run(call)

    def test_main_download_places_real_files(self):
        def call(zip_out, master_dir):
            rc = main(["download", "--doc-id", "S100ZI7X",
                       "--submit-date", "2026-11-17",
                       "--zip-out", zip_out, "--master-dir", master_dir])
            self.assertEqual(rc, 0)
        self._run(call)


if __name__ == "__main__":
    unittest.main()
