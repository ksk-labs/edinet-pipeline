"""pipeline.gold の単体テスト。

勝者決定 (確定値)、要素の候補固定と合計候補、連結/単体の優先順、
フリーキャッシュフローの式、増配年数の系列計算 (分割調整を含む)、
決算期の同定と変則フラグ、配当の期末分の導出を固定する。
"""
import json
import os
import sys
import unittest
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from pipeline import gold  # noqa: E402

REGISTRY_YAML = """
version: 4
indicators:
  - id: net_sales
    name_ja: 売上高
    unit: 円
    kind: element
    formula: test
    elements:
      japan_gaap: [jpcrp_cor:NetSalesSummaryOfBusinessResults]
      ifrs: []
      us_gaap: []
  - id: operating_income
    name_ja: 営業利益
    unit: 円
    kind: element
    formula: test
    elements:
      japan_gaap: [jppfs_cor:OperatingIncome]
      ifrs: []
      us_gaap: []
  - id: operating_cf
    name_ja: 営業CF
    unit: 円
    kind: element
    formula: test
    elements:
      japan_gaap: [jpcrp_cor:NetCashProvidedByUsedInOperatingActivitiesSummaryOfBusinessResults]
      ifrs: []
      us_gaap: []
  - id: capex_acquisition
    name_ja: 取得支出
    unit: 円
    kind: element
    multiply: -1
    formula: test
    elements:
      japan_gaap:
        - [jppfs_cor:PurchaseOfPropertyPlantAndEquipmentAndIntangibleAssetsInvCF]
        - [jppfs_cor:PurchaseOfPropertyPlantAndEquipmentInvCF, jppfs_cor:PurchaseOfIntangibleAssetsInvCF]
      ifrs: []
      us_gaap: []
  - id: free_cf
    name_ja: FCF
    unit: 円
    kind: formula
    formula: operating_cf - capex_acquisition
  - id: dividend_per_share
    name_ja: 1株配当
    unit: 円/株
    kind: element
    dimension_priority: [non_consolidated, consolidated]
    formula: test
    elements:
      japan_gaap: [jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults]
      ifrs: [jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults]
      us_gaap: [jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults]
  - id: shares_outstanding
    name_ja: 発行済株式数
    unit: 株
    kind: element
    dimension_priority: [non_consolidated, consolidated]
    formula: test
    elements:
      japan_gaap: [jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults]
      ifrs: []
      us_gaap: []
  - id: dividend_increase_years
    name_ja: 増配年数
    unit: 年
    kind: series
    base_indicator: dividend_per_share
    formula: test
"""


def make_registry(tmp):
    path = os.path.join(tmp, "indicators.yaml")
    with open(path, "w", encoding="utf-8") as f:
        f.write(REGISTRY_YAML)
    return gold.load_registry(path)


def fact(doc_id, element, value, start, end, dimension="", unit="iso4217:JPY",
         submit_at="2026-06-01 10:00", pit="as_filed", seq=0):
    return {"doc_id": doc_id, "element": element, "value": value,
            "unit": unit, "context_start": start, "context_end": end,
            "dimension": dimension, "submit_at": submit_at,
            "point_in_time": pit, "seq": seq}


DOC_MAP = {
    "D1": {"edinet_code": "E00001", "doc_type": "120",
           "standard": "japan_gaap", "submit_at": "2026-06-01 10:00",
           "period_start": "2025-04-01", "period_end": "2026-03-31"},
    "D1C": {"edinet_code": "E00001", "doc_type": "130",
            "standard": "japan_gaap", "submit_at": "2026-07-01 10:00",
            "period_start": "2025-04-01", "period_end": "2026-03-31"},
}
CODE_TO_ID = {"E00001": 1}


def winners_for(facts):
    winners, stats = gold.resolve_winners(facts, DOC_MAP, CODE_TO_ID)
    return winners


class PeriodMonthsTest(unittest.TestCase):
    def test_full_year(self):
        self.assertEqual(gold.period_months("2025-04-01", "2026-03-31"), 12)

    def test_half_year(self):
        self.assertEqual(gold.period_months("2025-04-01", "2025-09-30"), 6)

    def test_irregular_15_months(self):
        self.assertEqual(gold.period_months("2025-01-01", "2026-03-31"), 15)

    def test_quarter(self):
        self.assertEqual(gold.period_months("2025-04-01", "2025-06-30"), 3)


class DimensionClassTest(unittest.TestCase):
    def test_empty_is_consolidated(self):
        self.assertEqual(gold.dimension_class(""), gold.CONSOLIDATED)

    def test_non_consolidated(self):
        self.assertEqual(
            gold.dimension_class("jppfs_cor:NonConsolidatedMember"),
            gold.NON_CONSOLIDATED)

    def test_segment_is_ignored(self):
        self.assertIsNone(
            gold.dimension_class("jpcrp_cor:ReportableSegmentsMember"))


class WinnerResolutionTest(unittest.TestCase):
    def test_correction_wins(self):
        facts = [
            fact("D1", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 100.0,
                 "2025-04-01", "2026-03-31", seq=0),
            fact("D1C", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 120.0,
                 "2025-04-01", "2026-03-31", pit="final",
                 submit_at="2026-07-01 10:00", seq=1),
        ]
        winners = winners_for(facts)
        key = (1, "jpcrp_cor:NetSalesSummaryOfBusinessResults",
               "2025-04-01", "2026-03-31", gold.CONSOLIDATED, "iso4217:JPY")
        self.assertEqual(winners[key]["value"], 120.0)

    def test_latest_correction_wins(self):
        facts = [
            fact("D1C", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 120.0,
                 "2025-04-01", "2026-03-31", pit="final",
                 submit_at="2026-07-01 10:00", seq=0),
            fact("D1C", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 130.0,
                 "2025-04-01", "2026-03-31", pit="final",
                 submit_at="2026-08-01 10:00", seq=1),
        ]
        winners = winners_for(facts)
        key = (1, "jpcrp_cor:NetSalesSummaryOfBusinessResults",
               "2025-04-01", "2026-03-31", gold.CONSOLIDATED, "iso4217:JPY")
        self.assertEqual(winners[key]["value"], 130.0)

    def test_unresolved_doc_counted(self):
        facts = [fact("UNKNOWN", "jpcrp_cor:NetSalesSummaryOfBusinessResults",
                      1.0, "2025-04-01", "2026-03-31", seq=0)]
        _, stats = gold.resolve_winners(facts, DOC_MAP, CODE_TO_ID)
        self.assertEqual(stats["unresolved_docs"], 1)

    def test_same_submit_at_correction_tie_goes_to_larger_doc_id(self):
        # S-1: 同一 submit_at の訂正2件では、系譜 (build_doc_lineage) と
        # 同じく doc_id の大きい方を勝者とする
        doc_map = dict(DOC_MAP)
        for doc_id in ("D1A", "D1B"):
            doc_map[doc_id] = {"edinet_code": "E00001", "doc_type": "130",
                               "standard": "japan_gaap",
                               "submit_at": "2026-07-01 10:00",
                               "period_start": "2025-04-01",
                               "period_end": "2026-03-31"}
        facts = [
            fact("D1A", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 120.0,
                 "2025-04-01", "2026-03-31", pit="final",
                 submit_at="2026-07-01 10:00", seq=0),
            fact("D1B", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 130.0,
                 "2025-04-01", "2026-03-31", pit="final",
                 submit_at="2026-07-01 10:00", seq=1),
        ]
        winners, _ = gold.resolve_winners(facts, doc_map, CODE_TO_ID)
        key = (1, "jpcrp_cor:NetSalesSummaryOfBusinessResults",
               "2025-04-01", "2026-03-31", gold.CONSOLIDATED, "iso4217:JPY")
        self.assertEqual(winners[key]["value"], 130.0)
        self.assertEqual(winners[key]["doc_id"], "D1B")


class BuildTest(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.mkdtemp()
        self.registry = make_registry(self.tmp)

    def build(self, facts):
        winners = winners_for(facts)
        tables, stats = gold.generate(
            self.registry, winners, DOC_MAP, CODE_TO_ID, {1: "上場"}, {},
            {}, "2026-11-17T00:00:00+00:00")
        return tables, stats

    def test_annual_row_and_free_cf(self):
        facts = [
            fact("D1", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 1000.0,
                 "2025-04-01", "2026-03-31", seq=0),
            fact("D1",
                 "jpcrp_cor:NetCashProvidedByUsedInOperatingActivitiesSummaryOfBusinessResults",
                 200.0, "2025-04-01", "2026-03-31", seq=1),
            fact("D1", "jppfs_cor:PurchaseOfPropertyPlantAndEquipmentInvCF",
                 -50.0, "2025-04-01", "2026-03-31", seq=2),
            fact("D1", "jppfs_cor:PurchaseOfIntangibleAssetsInvCF", -30.0,
                 "2025-04-01", "2026-03-31", seq=3),
        ]
        tables, stats = self.build(facts)
        rows = tables["annual"].to_pylist()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["net_sales"], 1000.0)
        self.assertEqual(row["capex_acquisition"], 80.0)  # 正の支出額
        self.assertEqual(row["free_cf"], 120.0)
        self.assertEqual(row["period_months"], 12)
        self.assertFalse(row["is_irregular"])
        self.assertEqual(row["market"], "JP")
        self.assertEqual(row["listing_status"], "上場")

    def test_combined_capex_candidate_preferred(self):
        facts = [
            fact("D1", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 1000.0,
                 "2025-04-01", "2026-03-31", seq=0),
            fact("D1",
                 "jppfs_cor:PurchaseOfPropertyPlantAndEquipmentAndIntangibleAssetsInvCF",
                 -70.0, "2025-04-01", "2026-03-31", seq=1),
        ]
        tables, _ = self.build(facts)
        row = tables["annual"].to_pylist()[0]
        self.assertEqual(row["capex_acquisition"], 70.0)

    def test_nonconsolidated_fallback_with_basis(self):
        facts = [
            fact("D1", "jppfs_cor:OperatingIncome", -5.0,
                 "2025-04-01", "2026-03-31",
                 dimension="jppfs_cor:NonConsolidatedMember", seq=0),
            fact("D1", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 100.0,
                 "2025-04-01", "2026-03-31", seq=1),
        ]
        tables, _ = self.build(facts)
        row = tables["annual"].to_pylist()[0]
        self.assertEqual(row["operating_income"], -5.0)
        basis = {(b["indicator_id"], b["basis"])
                 for b in tables["basis"].to_pylist()}
        self.assertIn(("operating_income", "non_consolidated"), basis)
        self.assertIn(("net_sales", "consolidated"), basis)

    def test_irregular_period_flag(self):
        facts = [
            fact("D1", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 100.0,
                 "2025-01-01", "2026-03-31", seq=0),
        ]
        tables, _ = self.build(facts)
        row = tables["annual"].to_pylist()[0]
        self.assertEqual(row["period_months"], 15)
        self.assertTrue(row["is_irregular"])

    def test_source_doc_is_latest_submission(self):
        # S-2: 代表書類はその期間の書類のうち提出日時が最大のもの
        doc_map = {
            "D1": {"edinet_code": "E00001", "doc_type": "120",
                   "standard": "japan_gaap", "submit_at": "2026-06-01 10:00",
                   "period_start": "2025-04-01",
                   "period_end": "2026-03-31"},
            "D2": {"edinet_code": "E00001", "doc_type": "120",
                   "standard": "japan_gaap", "submit_at": "2026-07-01 10:00",
                   "period_start": "2025-04-01",
                   "period_end": "2026-03-31"},
        }
        facts = [
            fact("D1", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 1000.0,
                 "2025-04-01", "2026-03-31", seq=0),
        ]
        winners = winners_for(facts)
        tables, _ = gold.generate(
            self.registry, winners, doc_map, CODE_TO_ID, {1: "上場"}, {},
            {}, "2026-11-17T00:00:00+00:00")
        row = tables["annual"].to_pylist()[0]
        self.assertEqual(row["source_doc_id"], "D2")


class DividendSeriesTest(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.mkdtemp()
        self.registry = make_registry(self.tmp)

    def _facts_for_years(self, dps_list, shares_list=None):
        facts = []
        seq = 0
        for i, dps in enumerate(dps_list):
            year = 2022 + i
            start, end = f"{year}-04-01", f"{year + 1}-03-31"
            facts.append(fact(
                "D1", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 100.0,
                start, end, seq=seq))
            seq += 1
            if dps is not None:
                facts.append(fact(
                    "D1",
                    "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults",
                    dps, start, end,
                    dimension="jppfs_cor:NonConsolidatedMember",
                    unit="iso4217:JPY per xbrli:shares", seq=seq))
                seq += 1
            if shares_list is not None:
                facts.append(fact(
                    "D1",
                    "jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults",
                    float(shares_list[i]), end, end,
                    dimension="jppfs_cor:NonConsolidatedMember",
                    unit="xbrli:shares", seq=seq))
                seq += 1
        return facts

    def test_increase_years(self):
        winners = winners_for(self._facts_for_years([10.0, 12.0, 12.0, 15.0]))
        tables, _ = gold.generate(
            self.registry, winners, DOC_MAP, CODE_TO_ID, {}, {}, {},
            "2026-11-17T00:00:00+00:00")
        rows = tables["annual"].to_pylist()
        self.assertEqual([r["dividend_increase_years"] for r in rows],
                         [0.0, 1.0, 0.0, 1.0])

    def test_split_adjustment(self):
        # 3年目に 1:2 の株式分割 (株式数が倍増) があり、配当は据え置きの
        # 10 -> 5 に見えるが、調整後は同額なので増配には数えない
        winners = winners_for(self._facts_for_years(
            [10.0, 10.0, 5.0], shares_list=[100, 100, 200]))
        tables, _ = gold.generate(
            self.registry, winners, DOC_MAP, CODE_TO_ID, {}, {}, {},
            "2026-11-17T00:00:00+00:00")
        rows = tables["annual"].to_pylist()
        self.assertEqual([r["dividend_increase_years"] for r in rows],
                         [0.0, 0.0, 0.0])

    def test_missing_breaks_streak(self):
        winners = winners_for(self._facts_for_years([10.0, None, 12.0]))
        tables, _ = gold.generate(
            self.registry, winners, DOC_MAP, CODE_TO_ID, {}, {}, {},
            "2026-11-17T00:00:00+00:00")
        rows = tables["annual"].to_pylist()
        self.assertEqual([r["dividend_increase_years"] for r in rows],
                         [0.0, None, 0.0])


class DividendTableTest(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.mkdtemp()
        self.registry = make_registry(self.tmp)

    def test_final_derived_from_annual_minus_interim(self):
        facts = [
            fact("D1", gold.DIVIDEND_ANNUAL_ELEMENT, 30.0,
                 "2025-04-01", "2026-03-31",
                 dimension="jppfs_cor:NonConsolidatedMember",
                 unit="iso4217:JPY per xbrli:shares", seq=0),
            fact("D1", gold.DIVIDEND_INTERIM_ELEMENT, 10.0,
                 "2025-04-01", "2025-09-30",
                 dimension="jppfs_cor:NonConsolidatedMember",
                 unit="iso4217:JPY per xbrli:shares", seq=1),
            fact("D1", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 100.0,
                 "2025-04-01", "2026-03-31", seq=2),
        ]
        winners = winners_for(facts)
        tables, _ = gold.generate(
            self.registry, winners, DOC_MAP, CODE_TO_ID, {},
            {1: ("12340", "普通株式")}, {}, "2026-11-17T00:00:00+00:00")
        rows = {(r["term_kind"]): r
                for r in tables["dividends"].to_pylist()}
        self.assertEqual(rows["annual"]["amount_per_share"], 30.0)
        self.assertEqual(rows["interim"]["amount_per_share"], 10.0)
        self.assertEqual(rows["final"]["amount_per_share"], 20.0)
        self.assertTrue(rows["final"]["is_derived"])
        self.assertEqual(rows["annual"]["breakdown"], "unspecified")
        self.assertIsNone(rows["annual"]["record_date"])
        self.assertEqual(rows["annual"]["securities_code"], "12340")


class LineageTest(unittest.TestCase):
    def test_latest_correction(self):
        index_rows = [
            {"doc_id": "C1", "doc_type": "130", "parent_doc_id": "P",
             "submit_at": "2026-07-01 10:00"},
            {"doc_id": "C2", "doc_type": "130", "parent_doc_id": "P",
             "submit_at": "2026-08-01 10:00"},
            {"doc_id": "A", "doc_type": "120", "parent_doc_id": None,
             "submit_at": "2026-06-01 10:00"},
        ]
        lineage = gold.build_doc_lineage(index_rows, [])
        self.assertEqual(lineage["P"], "C2")


class ForecastsVesselTest(unittest.TestCase):
    def test_forecasts_table_is_empty_with_schema(self):
        import tempfile
        registry = make_registry(tempfile.mkdtemp())
        tables, _ = gold.generate(
            registry, {}, DOC_MAP, CODE_TO_ID, {}, {}, {},
            "2026-11-17T00:00:00+00:00")
        self.assertEqual(tables["forecasts"].num_rows, 0)
        self.assertIn("revision_of", tables["forecasts"].column_names)


class QuarterlyExclusionTest(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.mkdtemp()
        self.registry = make_registry(self.tmp)

    def test_full_year_period_in_semiannual_doc_is_excluded(self):
        doc_map = dict(DOC_MAP)
        doc_map["Q1"] = {"edinet_code": "E00001", "doc_type": "160",
                         "standard": "japan_gaap",
                         "submit_at": "2026-11-01 10:00",
                         "period_start": "2026-04-01",
                         "period_end": "2027-03-31"}
        facts = [
            # 半期累計 (6か月) は四半期表に入る
            fact("Q1", "jppfs_cor:OperatingIncome", 10.0,
                 "2026-04-01", "2026-09-30", seq=0),
            # 同じ書類のサマリに再掲された通年 (12か月) は入らない
            fact("Q1", "jpcrp_cor:NetSalesSummaryOfBusinessResults", 999.0,
                 "2025-04-01", "2026-03-31", seq=1),
        ]
        winners, _ = gold.resolve_winners(facts, doc_map, CODE_TO_ID)
        tables, _ = gold.generate(
            self.registry, winners, doc_map, CODE_TO_ID, {}, {}, {},
            "2026-11-17T00:00:00+00:00")
        qrows = tables["quarterly"].to_pylist()
        self.assertEqual(len(qrows), 1)
        self.assertEqual(qrows[0]["period_end"], "2026-09-30")
        self.assertEqual(qrows[0]["period_months"], 6)


class UpdateWatermarkTest(unittest.TestCase):
    """MF-1: 透かしの内容ハッシュによる値だけの書き換えの検出。"""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.mkdtemp()
        self.registry_path = os.path.join(self.tmp, "indicators.yaml")
        with open(self.registry_path, "w", encoding="utf-8") as f:
            f.write(REGISTRY_YAML)
        self.silver_dir = os.path.join(self.tmp, "silver")
        self.master_dir = os.path.join(self.tmp, "master")
        self.catalog_dir = os.path.join(self.tmp, "catalog")
        self.work_dir = os.path.join(self.tmp, "work")
        os.makedirs(os.path.join(self.silver_dir, "month=2026-06"))
        os.makedirs(self.master_dir)
        os.makedirs(os.path.join(self.catalog_dir, "month=2026-06"))
        pq.write_table(
            pa.table({"edinet_code": ["E00001"], "internal_id": [1]}),
            os.path.join(self.master_dir, "edinet_codes.parquet"))
        with open(os.path.join(self.catalog_dir, "month=2026-06",
                               "ledger.jsonl"), "w", encoding="utf-8") as f:
            f.write(json.dumps(
                {"docID": "D1", "edinetCode": "E00001", "docTypeCode": "120",
                 "submitDateTime": "2026-06-01 10:00",
                 "periodStart": "2025-04-01",
                 "periodEnd": "2026-03-31"}, ensure_ascii=False) + "\n")

    def _write_silver(self, value):
        table = pa.table({
            "doc_id": ["D1"],
            "element": ["jpcrp_cor:NetSalesSummaryOfBusinessResults"],
            "value": [float(value)],
            "unit": ["iso4217:JPY"],
            "context_start": ["2025-04-01"],
            "context_end": ["2026-03-31"],
            "dimension": [""],
            "submit_at": ["2026-06-01 10:00"],
            "point_in_time": ["as_filed"],
        })
        pq.write_table(
            table,
            os.path.join(self.silver_dir, "month=2026-06", "silver.parquet"))

    def _args(self, command):
        return SimpleNamespace(
            command=command, work_dir=self.work_dir, upload=False,
            since_month="2021-11", silver_dir=self.silver_dir,
            master_dir=self.master_dir, catalog_dir=self.catalog_dir,
            index=None, registry=self.registry_path)

    def test_value_only_rewrite_is_detected(self):
        # build (net_sales 1000) した後、Silver を同行数・同最大提出日時
        # のまま 2000 に書き換えて update すると、その月が変化として
        # 検出され、annual の値が 2000 に更新される
        self._write_silver(1000.0)
        gold.cmd_build(self._args("build"))
        annual = pq.read_table(
            os.path.join(self.work_dir, "gold", "annual.parquet"))
        self.assertEqual(annual.to_pylist()[0]["net_sales"], 1000.0)
        self._write_silver(2000.0)
        result = gold.cmd_update(self._args("update"))
        self.assertEqual(result["changed_months"], ["2026-06"])
        annual = pq.read_table(
            os.path.join(self.work_dir, "gold", "annual.parquet"))
        self.assertEqual(annual.to_pylist()[0]["net_sales"], 2000.0)

    def test_unchanged_silver_is_not_changed(self):
        self._write_silver(1000.0)
        gold.cmd_build(self._args("build"))
        result = gold.cmd_update(self._args("update"))
        self.assertEqual(result["changed_months"], [])
        self.assertEqual(result["note"], "no changes")


class ResolveUpdateSinceMonthTest(unittest.TestCase):
    """Issue #55: update の対象開始月の解決規則。"""

    def test_unspecified_inherits_oldest_state_month(self):
        old = {"2016-03": {}, "2026-06": {}}
        self.assertEqual(gold.resolve_update_since_month(None, old),
                         "2016-03")

    def test_no_state_falls_back_to_default(self):
        self.assertEqual(gold.resolve_update_since_month(None, {}),
                         gold.DEFAULT_SINCE_MONTH)
        self.assertEqual(gold.resolve_update_since_month("2016-03", {}),
                         "2016-03")

    def test_narrower_than_state_is_rejected(self):
        with self.assertRaises(ValueError):
            gold.resolve_update_since_month("2021-11", {"2016-03": {}})

    def test_equal_or_wider_is_accepted(self):
        old = {"2016-03": {}}
        self.assertEqual(gold.resolve_update_since_month("2016-03", old),
                         "2016-03")
        self.assertEqual(gold.resolve_update_since_month("2015-01", old),
                         "2015-01")


class UpdateSinceGuardTest(unittest.TestCase):
    """Issue #55: 手動実行相当の update で全期間の状態が縮まないこと。

    Silver は 2021-11 の前後 (2020-05 と 2026-06) に置き、全期間
    (since_month=2016-03) で build した state を対象にする。
    """

    def setUp(self):
        import tempfile
        self.tmp = tempfile.mkdtemp()
        self.registry_path = os.path.join(self.tmp, "indicators.yaml")
        with open(self.registry_path, "w", encoding="utf-8") as f:
            f.write(REGISTRY_YAML)
        self.silver_dir = os.path.join(self.tmp, "silver")
        self.master_dir = os.path.join(self.tmp, "master")
        self.catalog_dir = os.path.join(self.tmp, "catalog")
        self.work_dir = os.path.join(self.tmp, "work")
        os.makedirs(self.master_dir)
        pq.write_table(
            pa.table({"edinet_code": ["E00001"], "internal_id": [1]}),
            os.path.join(self.master_dir, "edinet_codes.parquet"))
        docs = {
            "2020-05": ("D0", "2020-05-01 10:00",
                        "2019-04-01", "2020-03-31"),
            "2026-06": ("D1", "2026-06-01 10:00",
                        "2025-04-01", "2026-03-31"),
        }
        for month, (doc_id, submit_at, start, end) in docs.items():
            os.makedirs(os.path.join(self.silver_dir, f"month={month}"))
            os.makedirs(os.path.join(self.catalog_dir, f"month={month}"))
            with open(os.path.join(self.catalog_dir, f"month={month}",
                                   "ledger.jsonl"),
                      "w", encoding="utf-8") as f:
                f.write(json.dumps(
                    {"docID": doc_id, "edinetCode": "E00001",
                     "docTypeCode": "120", "submitDateTime": submit_at,
                     "periodStart": start, "periodEnd": end},
                    ensure_ascii=False) + "\n")
            self._write_silver(month, doc_id, 1000.0, submit_at,
                               start, end)

    def _write_silver(self, month, doc_id, value, submit_at, start, end):
        table = pa.table({
            "doc_id": [doc_id],
            "element": ["jpcrp_cor:NetSalesSummaryOfBusinessResults"],
            "value": [float(value)],
            "unit": ["iso4217:JPY"],
            "context_start": [start],
            "context_end": [end],
            "dimension": [""],
            "submit_at": [submit_at],
            "point_in_time": ["as_filed"],
        })
        pq.write_table(
            table, os.path.join(self.silver_dir, f"month={month}",
                                "silver.parquet"))

    def _args(self, command, since_month):
        return SimpleNamespace(
            command=command, work_dir=self.work_dir, upload=False,
            since_month=since_month, silver_dir=self.silver_dir,
            master_dir=self.master_dir, catalog_dir=self.catalog_dir,
            index=None, registry=self.registry_path)

    def _state_months(self):
        with open(os.path.join(self.work_dir, "gold", "state.json"),
                  encoding="utf-8") as f:
            return set(json.load(f)["months"])

    def _build_full(self):
        gold.cmd_build(self._args("build", "2016-03"))
        self.assertEqual(self._state_months(), {"2020-05", "2026-06"})

    def test_unspecified_update_does_not_shrink_state(self):
        # 全期間で build した後、新しい月だけを書き換えて、手動
        # dispatch 相当の since_month 未指定で update する。変更は
        # 取り込まれ、state と summary の対象月は全期間のまま縮まない
        self._build_full()
        self._write_silver("2026-06", "D1", 2000.0, "2026-06-01 10:00",
                           "2025-04-01", "2026-03-31")
        result = gold.cmd_update(self._args("update", None))
        self.assertEqual(result["changed_months"], ["2026-06"])
        self.assertEqual(self._state_months(), {"2020-05", "2026-06"})
        self.assertEqual(result["summary"]["source_months"],
                         ["2020-05", "2026-06"])
        annual = pq.read_table(
            os.path.join(self.work_dir, "gold", "annual.parquet"))
        rows = {r["period_end"]: r for r in annual.to_pylist()}
        self.assertEqual(rows["2026-03-31"]["net_sales"], 2000.0)
        self.assertEqual(rows["2020-03-31"]["net_sales"], 1000.0)

    def test_narrower_update_is_rejected_and_state_untouched(self):
        # state の範囲より狭い指定 (既定値だった 2021-11) は明示的に
        # エラーで止まり、state は書き換わらない
        self._build_full()
        with self.assertRaises(ValueError):
            gold.cmd_update(self._args("update", "2021-11"))
        self.assertEqual(self._state_months(), {"2020-05", "2026-06"})


if __name__ == "__main__":
    unittest.main()
