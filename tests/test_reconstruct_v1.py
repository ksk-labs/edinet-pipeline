"""pipeline.reconstruct_v1 のテスト (P2-T2)。

実行: リポジトリのルートで `python -m unittest discover -s tests`
"""
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.reconstruct_v1 import change_key, main, normalize_name

JCN_STRONG = "1000000000001"
JCN_CORROBORATED = "1000000000002"
JCN_MAJORITY = "1000000000003"
JCN_MINORITY = "1000000000004"
JCN_PLACEHOLDER = "1000000000005"


def build_index_rows():
    rows = []
    counter = [0]

    def add(edinet_code, name, jcn=None, code=None, times=1):
        for _ in range(times):
            counter[0] += 1
            rows.append({
                "doc_id": f"S100{counter[0]:04d}",
                "edinet_code": edinet_code,
                "code": code,
                "jcn": jcn,
                "company_name": name,
                "submit_at": "2024-01-05 09:00",
            })

    add("E10001", "株式会社ストロング甲", JCN_STRONG, "JP:11110", times=2)
    add("E10002", "株式会社ストロング乙", JCN_STRONG, "JP:22220", times=2)
    add("E10003", "株式会社コロボ", JCN_CORROBORATED, "JP:33330")
    add("E10004", "コロボ株式会社", JCN_CORROBORATED, "JP:33330", times=3)
    add("E10005", "株式会社ホンモノ", JCN_MAJORITY, "JP:55550", times=5)
    add("E10005", "株式会社マチガイ", JCN_MINORITY, "JP:99990")
    add("E10006", "株式会社ベツノ", JCN_MINORITY, "JP:66660", times=3)
    add("E10007", "株式会社プレース", JCN_PLACEHOLDER, "JP:00000")
    add("E10008", "株式会社プレース", JCN_PLACEHOLDER, "JP:77770", times=2)
    add("E10009", "株式会社ネーム", "1000000000006", "JP:11110", times=2)
    add("E10010", "ネーム株式会社", "1000000000007", "JP:22220", times=2)
    add("E10011", "株式会社アルファ", "1000000000008")
    add("E10011", "株式会社ベータ", "1000000000008")
    add("E10012", "アルファ株式会社", "1000000000009")
    add("E10012", "ベータ株式会社", "1000000000009")
    return rows


class NormalizeNameTest(unittest.TestCase):
    def test_nfkc_fullwidth_to_halfwidth(self):
        self.assertEqual(normalize_name("ＲｏｂｏｔＨｏｍｅ"), "RobotHome")
        self.assertEqual(normalize_name("ＫＳＫ"), "KSK")

    def test_whitespace_removed(self):
        self.assertEqual(normalize_name("株 式 会 社 テ ス ト"), "株式会社テスト")
        self.assertEqual(normalize_name("Ｒｏｂｏｔ Ｈｏｍｅ"), "RobotHome")

    def test_abbreviated_corp_form_expanded(self):
        self.assertEqual(normalize_name("(株)テスト"), "株式会社テスト")
        self.assertEqual(normalize_name("㈱テスト"), "株式会社テスト")
        self.assertEqual(normalize_name("テスト(有)"), "テスト有限会社")


class ChangeKeyTest(unittest.TestCase):
    def test_corp_form_removed_from_both_ends(self):
        self.assertEqual(change_key("株式会社テスト"), "テスト")
        self.assertEqual(change_key("テスト株式会社"), "テスト")
        self.assertEqual(change_key("株式会社テスト"), change_key("テスト株式会社"))

    def test_corp_form_kind_not_part_of_key(self):
        self.assertEqual(change_key("株式会社ＫＳＫ"), change_key("ＫＳＫ合同会社"))

    def test_casefold_robot_home(self):
        self.assertEqual(change_key("Ｒｏｂｏｔ Ｈｏｍｅ"), change_key("ｒｏｂｏｔ ｈｏｍｅ"))
        self.assertEqual(change_key("Robot Home"), change_key("robot home"))

    def test_distinct_names_have_distinct_keys(self):
        self.assertNotEqual(change_key("株式会社アルファ"), change_key("株式会社ベータ"))


class ReconstructionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        index_path = os.path.join(cls.tmp.name, "index.parquet")
        pq.write_table(pa.Table.from_pylist(build_index_rows()), index_path)
        cls.out_dir = os.path.join(cls.tmp.name, "out")
        with contextlib.redirect_stdout(io.StringIO()):
            main(["reconstruct_v1.py", index_path, cls.out_dir])
        cls.history = pq.read_table(
            os.path.join(cls.out_dir, "identifier_history.parquet")).to_pylist()
        cls.connections = pq.read_table(
            os.path.join(cls.out_dir, "code_connections.parquet")).to_pylist()
        cls.review_items = pq.read_table(
            os.path.join(cls.out_dir, "review_items.parquet")).to_pylist()
        with open(os.path.join(cls.out_dir, "summary.json"), encoding="utf-8") as f:
            cls.summary = json.load(f)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def connection(self, a, b):
        for c in self.connections:
            if (c["edinet_code_a"], c["edinet_code_b"]) == (a, b):
                return c
        return None

    def reviews(self, item_type, a=None, b=None):
        return [r for r in self.review_items
                if r["item_type"] == item_type
                and (a is None or r["edinet_code_a"] == a)
                and (b is None or r["edinet_code_b"] == b)]

    def test_strong_when_both_sides_multiple_rows_and_majority(self):
        conn = self.connection("E10001", "E10002")
        self.assertIsNotNone(conn)
        self.assertEqual(conn["strength"], "strong")
        self.assertEqual(conn["shared_value"], JCN_STRONG)
        self.assertFalse(conn["name_agreement"])

    def test_corroborated_when_single_row_but_majority_name_and_code_agree(self):
        conn = self.connection("E10003", "E10004")
        self.assertIsNotNone(conn)
        self.assertEqual(conn["strength"], "corroborated")
        self.assertEqual(conn["rows_a"], 1)
        self.assertTrue(conn["name_agreement"])

    def test_single_row_minority_observation_needs_review(self):
        self.assertIsNone(self.connection("E10005", "E10006"))
        items = self.reviews("jcn_link_needs_review", "E10005", "E10006")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["shared_value"], JCN_MINORITY)

    def test_conflicting_corporate_numbers_kept_side_by_side(self):
        values = {r["value"]: r["observation_count"] for r in self.history
                  if r["edinet_code"] == "E10005" and r["kind"] == "corporate_number"}
        self.assertEqual(values, {JCN_MAJORITY: 5, JCN_MINORITY: 1})
        self.assertEqual(
            len(self.reviews("jcn_within_code_conflict", "E10005")), 1)

    def test_placeholder_kept_in_history_with_flag(self):
        rows = [r for r in self.history
                if r["edinet_code"] == "E10007" and r["kind"] == "securities_code"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["value"], "00000")
        self.assertTrue(rows[0]["is_placeholder"])
        self.assertEqual(
            len(self.reviews("placeholder_securities_code", "E10007")), 1)

    def test_placeholder_not_used_for_securities_code_matching(self):
        conn = self.connection("E10007", "E10008")
        self.assertIsNotNone(conn)
        self.assertEqual(conn["strength"], "corroborated")

    def test_name_only_candidate_stays_unconfirmed(self):
        self.assertIsNone(self.connection("E10009", "E10010"))
        items = self.reviews("name_only_candidate", "E10009", "E10010")
        self.assertEqual(len(items), 1)

    def test_summary_pairs_count_distinct_pairs_not_rows(self):
        rows = self.reviews("name_only_candidate")
        self.assertEqual(len(rows), 3)
        self.assertEqual(self.summary["name_only_candidate_rows"], 3)
        self.assertEqual(self.summary["name_only_candidate_pairs"], 2)
        pair_rows = self.reviews("name_only_candidate", "E10011", "E10012")
        self.assertEqual(len(pair_rows), 2)


if __name__ == "__main__":
    unittest.main()
