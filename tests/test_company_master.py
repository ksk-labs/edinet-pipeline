"""pipeline.company_master のテスト (P2-T3)。

実行: リポジトリのルートで `python -m unittest discover -s tests`
"""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.company_master import (
    apply_new_snapshots, empty_state, evaluate_jcn_link, majority_value,
    security_kind, state_from_history,
)
from pipeline.codelist import EXPECTED_COLUMNS

GEN = "2026-11-17T00:00:00+00:00"
JCN_ALPHA = "1000000000001"
JCN_BETA = "1000000000002"
JCN_DELTA = "1000000000003"

PROV = {
    "source_kind": "v1_index", "source_dataset": "Yoshi-Dai/financial-lakehouse",
    "source_file": "catalog/documents_index.parquet",
    "source_file_sha256": "x" * 64, "generated_at": GEN,
    "redistributable": True,
}


def hist_row(code, kind, value, count, first="2020-01-05", last="2020-02-01",
             normalized=None, key=None, placeholder=False):
    return {
        "edinet_code": code, "kind": kind, "value": value,
        "source_value": value, "value_normalized": normalized,
        "change_key": key, "is_placeholder": placeholder,
        "first_observed_date": first, "last_observed_date": last,
        "observation_count": count, "first_doc_id": "S1000001",
        "last_doc_id": "S1000002", **PROV,
    }


def build_history_dir(path):
    os.makedirs(path, exist_ok=True)
    history = [
        hist_row("E00001", "company_name", "株式会社アルファ", 5,
                 normalized="株式会社アルファ", key="アルファ"),
        hist_row("E00001", "corporate_number", JCN_ALPHA, 5),
        hist_row("E00001", "securities_code", "11110", 5),
        hist_row("E00002", "company_name", "アルファ株式会社", 1,
                 normalized="アルファ株式会社", key="アルファ"),
        hist_row("E00002", "corporate_number", JCN_ALPHA, 1),
        hist_row("E00003", "company_name", "株式会社ベータ", 3,
                 normalized="株式会社ベータ", key="ベータ"),
        hist_row("E00003", "corporate_number", JCN_BETA, 3),
        hist_row("E00003", "securities_code", "22220", 3),
        hist_row("E00004", "company_name", "株式会社ガンマ", 2,
                 normalized="株式会社ガンマ", key="ガンマ"),
    ]
    roster = [{
        "edinet_code": c, "first_observed_date": "2020-01-05",
        "last_observed_date": "2020-02-01", "document_count": n,
        "distinct_name_count": 1, "distinct_jcn_count": 1,
        "distinct_securities_code_count": 1, **PROV,
    } for c, n in (("E00001", 5), ("E00002", 1), ("E00003", 3), ("E00004", 2))]
    connections = [{
        "edinet_code_a": "E00001", "edinet_code_b": "E00002",
        "basis": "corporate_number", "shared_value": JCN_ALPHA,
        "rows_a": 5, "rows_b": 1, "strength": "corroborated",
        "name_agreement": True, "note": "fixture", **PROV,
    }]
    review = [{
        "item_type": "name_only_candidate", "edinet_code_a": "E00003",
        "edinet_code_b": "E00004", "shared_value": "ベータ",
        "detail": "fixture", **PROV,
    }]
    for name, rows in (("identifier_history", history),
                       ("code_roster", roster),
                       ("code_connections", connections),
                       ("review_items", review)):
        pq.write_table(pa.Table.from_pylist(rows),
                       os.path.join(path, f"{name}.parquet"))


def cl_row(code, name, jcn="", sec="", address="", industry="機械",
           filer_type="内国法人・組合", listing="上場", consolidated="有",
           capital="100", fy_end="3月31日", name_en="", name_kana=""):
    row = {c: "" for c in EXPECTED_COLUMNS}
    row.update({
        "ＥＤＩＮＥＴコード": code, "提出者種別": filer_type,
        "上場区分": listing, "連結の有無": consolidated, "資本金": capital,
        "決算日": fy_end, "提出者名": name, "提出者名（英字）": name_en,
        "提出者名（ヨミ）": name_kana, "所在地": address,
        "提出者業種": industry, "証券コード": sec, "提出者法人番号": jcn,
    })
    return row


SNAP1_DATE = "2026-11-17"
SNAP1 = [
    cl_row("E00001", "株式会社アルファ", JCN_ALPHA, "11110", "東京都千代田区1"),
    cl_row("E00002", "アルファ株式会社", JCN_ALPHA, "", "東京都千代田区1"),
    cl_row("E00003", "株式会社ベータ", JCN_BETA, "22220", "大阪府大阪市2"),
    cl_row("E00005", "株式会社デルタ", JCN_DELTA, "33335", "愛知県名古屋市3"),
]
SNAP2_DATE = "2026-11-18"
SNAP2 = [
    cl_row("E00001", "株式会社アルファ", JCN_ALPHA, "11110", "東京都千代田区9"),
    cl_row("E00005", "株式会社デルタ", JCN_DELTA, "33335", "愛知県名古屋市3",
           industry="サービス業"),
    cl_row("E00006", "株式会社ベータ", JCN_BETA, "", "大阪府大阪市2"),
]


class SecurityKindTest(unittest.TestCase):
    def test_common_stock_digit_zero(self):
        self.assertEqual(security_kind("72030"), ("普通株式", "0"))

    def test_alphanumeric_code(self):
        self.assertEqual(security_kind("619A0"), ("普通株式", "0"))

    def test_preferred_stock_digits(self):
        self.assertEqual(security_kind("12345"), ("優先株式", "5"))
        self.assertEqual(security_kind("12346"), ("優先株式", "6"))

    def test_warrant_digit(self):
        self.assertEqual(security_kind("12349"), ("新株予約権証券", "9"))

    def test_unknown_for_short_code(self):
        self.assertEqual(security_kind("1234"), ("不明", None))
        self.assertEqual(security_kind(""), ("不明", None))


class MajorityValueTest(unittest.TestCase):
    def test_unique_majority(self):
        self.assertEqual(majority_value({"a": 3, "b": 2}), "a")

    def test_tie_has_no_majority(self):
        self.assertIsNone(majority_value({"a": 2, "b": 2}))

    def test_empty(self):
        self.assertIsNone(majority_value({}))


class EvaluateJcnLinkTest(unittest.TestCase):
    def test_strong(self):
        self.assertEqual(
            evaluate_jcn_link({"J": 3}, {"J": 2}, "J", False, False, False),
            "strong")

    def test_corroborated_by_name(self):
        self.assertEqual(
            evaluate_jcn_link({"J": 1}, {"J": 5}, "J", True, False, False),
            "corroborated")

    def test_corroborated_by_address(self):
        self.assertEqual(
            evaluate_jcn_link({"J": 1}, {"J": 5}, "J", False, True, False),
            "corroborated")

    def test_sec_conflict_blocks_corroborated(self):
        self.assertIsNone(
            evaluate_jcn_link({"J": 1}, {"J": 5}, "J", True, True, True))

    def test_tie_majority_blocks_link(self):
        self.assertIsNone(
            evaluate_jcn_link({"J": 2, "K": 2}, {"J": 5}, "J",
                              True, True, False))

    def test_minority_blocks_link(self):
        self.assertIsNone(
            evaluate_jcn_link({"J": 1, "K": 9}, {"J": 5}, "J",
                              True, True, False))


class BuildAndUpdateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        history_dir = os.path.join(cls.tmp.name, "history")
        build_history_dir(history_dir)
        cls.state = state_from_history(history_dir, GEN)
        cls.applied = apply_new_snapshots(
            cls.state, [(SNAP1_DATE, SNAP1), (SNAP2_DATE, SNAP2)], GEN)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def code_row(self, code):
        for r in self.state["edinet_codes"]:
            if r["edinet_code"] == code:
                return r
        return None

    def test_connected_codes_share_internal_id(self):
        self.assertEqual(self.code_row("E00001")["internal_id"],
                         self.code_row("E00002")["internal_id"])
        self.assertEqual(self.code_row("E00002")["assignment_basis"],
                         "jcn_corroborated")
        self.assertEqual(self.code_row("E00002")["connected_from"], "E00001")

    def test_new_code_in_snapshot_gets_new_id(self):
        row = self.code_row("E00005")
        self.assertEqual(row["assignment_basis"], "new_in_codelist")
        # 会社は {E00001,E00002}・{E00003,E00006}・{E00004}・{E00005} の4社
        self.assertEqual(len(self.state["companies"]), 4)

    def test_new_code_with_matching_jcn_connects_on_update(self):
        # E00006 は SNAP2 で初出。法人番号が E00003 と一致し、名称と
        # 所在地も一致するため、新しい会社にはならず E00003 の会社へ
        # 接続される (会社数は SNAP1 時点から増えない)。
        row = self.code_row("E00006")
        self.assertEqual(row["internal_id"],
                         self.code_row("E00003")["internal_id"])
        self.assertEqual(row["assignment_basis"], "jcn_corroborated")
        self.assertEqual(row["connected_from"], "E00003")

    def test_attribute_change_opens_new_row(self):
        rows = [a for a in self.state["attribute_history"]
                if a["edinet_code"] == "E00001" and a["attribute"] == "address"]
        self.assertEqual(len(rows), 2)
        by_value = {r["value"]: r for r in rows}
        self.assertEqual(by_value["東京都千代田区1"]["last_observed_date"],
                         SNAP1_DATE)
        self.assertEqual(by_value["東京都千代田区9"]["first_observed_date"],
                         SNAP2_DATE)

    def test_absent_code_not_deleted(self):
        row = self.code_row("E00002")
        self.assertFalse(row["in_latest_snapshot"])
        company = [c for c in self.state["companies"]
                   if c["internal_id"] == row["internal_id"]][0]
        self.assertEqual(company["status"], "active")  # E00001 が現行
        beta = [c for c in self.state["companies"]
                if c["internal_id"] == self.code_row("E00004")["internal_id"]][0]
        self.assertEqual(beta["status"], "absent")

    def test_classification_first_seen(self):
        rows = {(c["taxonomy"], c["value"]): c
                for c in self.state["classifications"]}
        self.assertEqual(rows[("industry", "機械")]["first_seen_date"],
                         SNAP1_DATE)
        self.assertEqual(rows[("industry", "サービス業")]["first_seen_date"],
                         SNAP2_DATE)
        self.assertEqual(rows[("industry", "サービス業")]["snapshot_count"], 1)
        self.assertEqual(rows[("industry", "機械")]["snapshot_count"], 2)

    def test_primary_security_from_latest_snapshot(self):
        iid = self.code_row("E00001")["internal_id"]
        secs = [s for s in self.state["securities"]
                if s["internal_id"] == iid]
        self.assertEqual(len(secs), 1)
        self.assertTrue(secs[0]["is_primary"])
        self.assertEqual(secs[0]["primary_basis"], "latest_snapshot")
        delta = [s for s in self.state["securities"]
                 if s["securities_code"] == "33335"]
        self.assertEqual(delta[0]["security_kind"], "優先株式")

    def test_review_items_carried_over(self):
        items = [r for r in self.state["review_items"]
                 if r["item_type"] == "name_only_candidate"]
        self.assertEqual(len(items), 1)
        self.assertIsNotNone(items[0]["internal_id_a"])

    def test_reapplying_snapshots_is_noop(self):
        before = (len(self.state["companies"]),
                  len(self.state["identifier_history"]))
        applied = apply_new_snapshots(
            self.state, [(SNAP1_DATE, SNAP1), (SNAP2_DATE, SNAP2)], GEN)
        self.assertEqual(applied, [])
        after = (len(self.state["companies"]),
                 len(self.state["identifier_history"]))
        self.assertEqual(before, after)


class PlaceholderAndReviewTest(unittest.TestCase):
    def test_placeholder_not_a_security(self):
        state = empty_state(GEN)
        rows = [cl_row("E00009", "株式会社プレース", "1000000000009", "00000",
                       "東京都")]
        apply_new_snapshots(state, [("2026-11-17", rows)], GEN)
        self.assertEqual(state["securities"], [])
        ident = [h for h in state["identifier_history"]
                 if h["kind"] == "securities_code"]
        self.assertEqual(len(ident), 1)
        self.assertTrue(ident[0]["is_placeholder"])

    def test_unmatched_jcn_goes_to_review(self):
        state = empty_state(GEN)
        day1 = [cl_row("E00010", "株式会社ホンモノ", "1000000000010",
                       "11110", "東京都千代田区1")]
        day2 = [cl_row("E00010", "株式会社ホンモノ", "1000000000010",
                       "11110", "東京都千代田区1"),
                cl_row("E00011", "株式会社マチガイ", "1000000000010",
                       "99990", "大阪府大阪市9")]
        apply_new_snapshots(state, [("2026-11-17", day1),
                                    ("2026-11-18", day2)], GEN)
        codes = {r["edinet_code"]: r for r in state["edinet_codes"]}
        self.assertNotEqual(codes["E00010"]["internal_id"],
                            codes["E00011"]["internal_id"])
        items = [r for r in state["review_items"]
                 if r["item_type"] == "jcn_link_needs_review"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["edinet_code_a"], "E00011")


class StateRoundtripTest(unittest.TestCase):
    def test_write_and_read_state(self):
        from pipeline.company_master import read_state, write_state
        state = empty_state(GEN)
        rows = [cl_row("E00020", "株式会社ラウンド", "1000000000020",
                       "11110", "東京都")]
        apply_new_snapshots(state, [("2026-11-17", rows)], GEN)
        with tempfile.TemporaryDirectory() as tmp:
            write_state(state, tmp)
            back = read_state(tmp)
            with open(os.path.join(tmp, "state.json"), encoding="utf-8") as f:
                next_id = json.load(f)["next_internal_id"]
        self.assertEqual(back["meta"]["last_applied_snapshot_date"],
                         "2026-11-17")
        self.assertEqual(len(back["companies"]), 1)
        self.assertEqual(back["edinet_codes"][0]["edinet_code"], "E00020")
        self.assertEqual(next_id, 2)


if __name__ == "__main__":
    unittest.main()
