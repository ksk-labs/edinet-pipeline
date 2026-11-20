"""pipeline.serving_sync のテスト (P2-T6)。

実行: リポジトリのルートで `python -m unittest discover -s tests`
Cloudflare へは接続せず、D1 は SQLite で模したフェイクで置き換える。
"""
import json
import math
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.serving_sync import (
    BudgetLedger, D1Client, batched_insert_sqls, build_company_rows,
    compute_diff, create_table_sql, delete_sql, derive_current_edinet_code,
    derive_current_name, derive_latest_annual, insert_sql, row_hash,
    run_sync, sqlite_type, update_sql,
)

GEN = "2026-11-20T00:00:00+00:00"
PROV = {"source_kind": "test", "source_id": "test",
        "generated_at": GEN, "redistribution": "test"}


class FakeD1:
    """D1 REST を SQLite で模したフェイク (call の署名は cf_api と同じ)。"""

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.queries = []

    def call(self, method, path, body=None):
        if method == "GET":
            return 200, {"success": True, "result": {"file_size": 4242}}
        sql = body["sql"]
        self.queries.append(sql)
        cur = self.conn.execute(sql)
        if cur.description:
            cols = [d[0] for d in cur.description]
            results = [dict(zip(cols, row)) for row in cur.fetchall()]
            written = 0
        else:
            results = []
            written = cur.rowcount if cur.rowcount > 0 else 0
        self.conn.commit()
        return 200, {"success": True, "result": [
            {"success": True, "results": results,
             "meta": {"rows_written": written}}]}

    def client(self):
        return D1Client(self.call, "acct", "dbid")

    def rows(self, table):
        cur = self.conn.execute(f'SELECT * FROM "{table}"')
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def write_parquet(cache_dir, rel, table):
    path = os.path.join(cache_dir, rel.replace("/", "_"))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path)


def build_fixture_cache(cache_dir):
    """同期の入力一式 (小さな標本) を cache_dir へ書く。"""
    os.makedirs(cache_dir, exist_ok=True)
    prov_cols = {
        "source_kind": pa.array(["test"] * 3),
        "source_id": pa.array(["test"] * 3),
        "generated_at": pa.array([GEN] * 3),
        "redistribution": pa.array(["test"] * 3),
    }
    write_parquet(cache_dir, "master/companies.parquet", pa.table({
        "internal_id": pa.array([1, 2, 3], pa.int64()),
        "status": pa.array(["active", "active", "absent"]),
        "disappearance_reason": pa.array([None, None, "merged"]),
        "successor_internal_id": pa.array([None, None, None], pa.int64()),
        "first_seen_date": pa.array(["2020-01-01"] * 3),
        "last_seen_date": pa.array(["2026-11-19"] * 3),
        **prov_cols,
    }))
    write_parquet(cache_dir, "master/edinet_codes.parquet", pa.table({
        "internal_id": pa.array([1, 2, 2], pa.int64()),
        "edinet_code": pa.array(["E00001", "E00002", "E00003"]),
        "assignment_basis": pa.array(
            ["new_in_codelist", "initial_v1", "jcn_strong"]),
        "connected_from": pa.array([None, None, None]),
        "filer_role": pa.array(["filer"] * 3),
        "first_seen_date": pa.array(["2020-01-01"] * 3),
        "last_seen_date": pa.array(
            ["2026-11-19", "2026-11-19", "2025-01-01"]),
        "in_latest_snapshot": pa.array([True, True, False]),
        **prov_cols,
    }))
    write_parquet(cache_dir, "master/identifier_history.parquet", pa.table({
        "internal_id": pa.array([1, 1, 2, 3, 1], pa.int64()),
        "edinet_code": pa.array(
            ["E00001", "E00001", "E00002", "E00009", "E00001"]),
        "kind": pa.array(["company_name", "company_name", "company_name",
                          "company_name", "corporate_number"]),
        "value": pa.array(["旧アルファ", "株式会社アルファ", "株式会社ベータ",
                           "株式会社ガンマ", "1000000000001"]),
        "source_value": pa.array([""] * 5),
        "value_normalized": pa.array([""] * 5),
        "change_key": pa.array([""] * 5),
        "is_placeholder": pa.array([False] * 5),
        "first_observed_date": pa.array(
            ["2020-01-01", "2023-05-01", "2020-01-01", "2020-01-01",
             "2020-01-01"]),
        "last_observed_date": pa.array(
            ["2023-04-30", "2026-11-19", "2026-11-19", "2026-11-19",
             "2026-11-19"]),
        "observation_count": pa.array([3, 9, 4, 2, 9], pa.int64()),
        **{k: pa.array([v[0]] * 5) for k, v in prov_cols.items()},
        "source_sha256": pa.array(["x" * 64] * 5),
    }))
    write_parquet(cache_dir, "master/securities.parquet", pa.table({
        "internal_id": pa.array([1, 2], pa.int64()),
        "edinet_code": pa.array(["E00001", "E00002"]),
        "securities_code": pa.array(["11110", "22220"]),
        "security_kind": pa.array(["普通株式", "普通株式"]),
        "reserve_digit": pa.array(["0", "0"]),
        "is_primary": pa.array([True, True]),
        "primary_basis": pa.array(["latest_snapshot"] * 2),
        "first_observed_date": pa.array(["2020-01-01"] * 2),
        "last_observed_date": pa.array(["2026-11-19"] * 2),
        "observation_count": pa.array([9, 4], pa.int64()),
        "in_latest_snapshot": pa.array([True, True]),
        **{k: pa.array([v[0]] * 2) for k, v in prov_cols.items()},
    }))
    write_parquet(cache_dir, "gold/annual.parquet", pa.table({
        "internal_id": pa.array([1, 1, 2], pa.int64()),
        "edinet_code": pa.array(["E00001", "E00001", "E00002"]),
        "market": pa.array(["JP"] * 3),
        "period_start": pa.array(
            ["2023-04-01", "2024-04-01", "2024-04-01"]),
        "period_end": pa.array(["2024-03-31", "2025-03-31", "2025-03-31"]),
        "period_months": pa.array([12, 12, 12], pa.int64()),
        "is_irregular": pa.array([False] * 3),
        "net_sales": pa.array([100.0, 120.0, float("nan")], pa.float64()),
        "source_doc_id": pa.array(["S1", "S2", "S3"]),
        **{k: pa.array([v[0]] * 3) for k, v in prov_cols.items()},
    }))
    write_parquet(cache_dir, "gold/quarterly.parquet", pa.table({
        "internal_id": pa.array([1], pa.int64()),
        "edinet_code": pa.array(["E00001"]),
        "period_start": pa.array(["2024-04-01"]),
        "period_end": pa.array(["2024-06-30"]),
        "doc_type": pa.array(["四半期報告書"]),
        "net_sales": pa.array([30.0], pa.float64()),
        **{k: pa.array([v[0]]) for k, v in prov_cols.items()},
    }))
    write_parquet(cache_dir, "gold/dividends.parquet", pa.table({
        "internal_id": pa.array([1], pa.int64()),
        "edinet_code": pa.array(["E00001"]),
        "securities_code": pa.array([None], pa.string()),
        "period_start": pa.array(["2024-04-01"]),
        "period_end": pa.array(["2025-03-31"]),
        "term_kind": pa.array(["annual"]),
        "amount_per_share": pa.array([50.0], pa.float64()),
        **{k: pa.array([v[0]]) for k, v in prov_cols.items()},
    }))
    write_parquet(cache_dir, "master/company_events.parquet", pa.table({
        "event_id": pa.array(["ev1", "ev2"]),
        "internal_id": pa.array([1, 2], pa.int64()),
        "edinet_code": pa.array(["E00001", "E00002"]),
        "event_kind": pa.array(["representative_change"] * 2),
        "disclosed_at": pa.array(
            ["2025-06-01T00:00:00+00:00", "2025-07-01T00:00:00+00:00"]),
        "related_internal_id": pa.array([None, None], pa.int64()),
        **{k: pa.array([v[0]] * 2) for k, v in prov_cols.items()},
        "source_sha256": pa.array(["x" * 64] * 2),
    }))
    write_parquet(cache_dir, "master/company_relationships.parquet",
                  pa.table({
        "parent_internal_id": pa.array([1], pa.int64()),
        "parent_edinet_code": pa.array(["E00001"]),
        "subsidiary_internal_id": pa.array([None], pa.int64()),
        "subsidiary_edinet_code": pa.array(["E99999"]),
        "relation_kind": pa.array(["large_holding"]),
        "first_observed_date": pa.array(["2025-01-01"]),
        "last_observed_date": pa.array(["2025-01-01"]),
        "observation_count": pa.array([1], pa.int64()),
        **{k: pa.array([v[0]]) for k, v in prov_cols.items()},
        "source_sha256": pa.array(["x" * 64]),
    }))
    with open(os.path.join(cache_dir, "gold_summary.json"), "w",
              encoding="utf-8") as f:
        json.dump({"snapshot_date": "2026-11-20", "generated_at": GEN}, f)


class TypeMappingTest(unittest.TestCase):
    def test_sqlite_type(self):
        self.assertEqual(sqlite_type(pa.int64()), "INTEGER")
        self.assertEqual(sqlite_type(pa.int32()), "INTEGER")
        self.assertEqual(sqlite_type(pa.float64()), "REAL")
        self.assertEqual(sqlite_type(pa.string()), "TEXT")
        self.assertEqual(sqlite_type(pa.bool_()), "INTEGER")
        self.assertEqual(sqlite_type(pa.date32()), "TEXT")

    def test_sqlite_type_unsupported(self):
        with self.assertRaises(ValueError):
            sqlite_type(pa.binary())


class CurrentViewTest(unittest.TestCase):
    def test_code_prefers_latest_snapshot_and_seen_date(self):
        rows = [
            {"internal_id": 1, "edinet_code": "E00002",
             "assignment_basis": "jcn_strong", "last_seen_date": "2025-01-01",
             "in_latest_snapshot": True},
            {"internal_id": 1, "edinet_code": "E00001",
             "assignment_basis": "initial_v1", "last_seen_date": "2026-01-01",
             "in_latest_snapshot": True},
            {"internal_id": 1, "edinet_code": "E00003",
             "assignment_basis": "jcn_strong", "last_seen_date": "2026-06-01",
             "in_latest_snapshot": False},
        ]
        self.assertEqual(derive_current_edinet_code(rows), {1: "E00001"})

    def test_code_tie_prefers_codelist_basis_then_smaller_code(self):
        rows = [
            {"internal_id": 1, "edinet_code": "E00009",
             "assignment_basis": "initial_v1", "last_seen_date": "2026-11-19",
             "in_latest_snapshot": True},
            {"internal_id": 1, "edinet_code": "E00008",
             "assignment_basis": "jcn_strong", "last_seen_date": "2026-11-19",
             "in_latest_snapshot": True},
        ]
        self.assertEqual(derive_current_edinet_code(rows), {1: "E00008"})

    def test_name_uses_company_name_kind_and_recency(self):
        rows = [
            {"internal_id": 1, "kind": "company_name", "value": "旧名称",
             "last_observed_date": "2020-01-01", "observation_count": 10},
            {"internal_id": 1, "kind": "company_name", "value": "現名称",
             "last_observed_date": "2026-01-01", "observation_count": 1},
            {"internal_id": 1, "kind": "corporate_number",
             "value": "1000000000001", "last_observed_date": "2026-06-01",
             "observation_count": 99},
            {"internal_id": 2, "kind": "company_name", "value": "乙",
             "last_observed_date": "2026-01-01", "observation_count": 5},
            {"internal_id": 2, "kind": "company_name", "value": "甲",
             "last_observed_date": "2026-01-01", "observation_count": 3},
        ]
        self.assertEqual(derive_current_name(rows),
                         {1: "現名称", 2: "乙"})

    def test_build_company_rows_missing_values(self):
        companies = [{"internal_id": 7, "status": "absent"}]
        rows = build_company_rows(companies, {}, {}, [])
        self.assertEqual(rows, [{
            "internal_id": 7, "edinet_code": None, "name": None,
            "status": "absent", "primary_securities_code": None,
            "security_kind": None}])

    def test_latest_annual_tie_breaks_by_period_start(self):
        rows = [
            {"internal_id": 1, "period_end": "2025-03-31",
             "period_start": "2024-04-01", "net_sales": 1.0},
            {"internal_id": 1, "period_end": "2025-03-31",
             "period_start": "2025-01-01", "net_sales": 2.0},
            {"internal_id": 1, "period_end": "2024-03-31",
             "period_start": "2023-04-01", "net_sales": 3.0},
        ]
        latest = derive_latest_annual(rows)
        self.assertEqual(len(latest), 1)
        self.assertEqual(latest[0]["net_sales"], 2.0)


SPEC = {
    "name": "t",
    "columns": [("internal_id", "INTEGER"), ("period_end", "TEXT"),
                ("value", "REAL")],
    "primary_key": ["internal_id", "period_end"],
    "indexes": [],
    "rows": [],
    "write_factor": 2,
}


class DiffAndSqlTest(unittest.TestCase):
    def test_compute_diff_classifies(self):
        rows = [
            {"internal_id": 1, "period_end": "2024-03-31", "value": 1.0},
            {"internal_id": 1, "period_end": "2025-03-31", "value": 2.0},
            {"internal_id": 2, "period_end": "2025-03-31", "value": 3.0},
        ]
        spec = dict(SPEC, rows=rows)
        columns = [c for c, _ in SPEC["columns"]]
        mirror = {
            (1, "2024-03-31"): row_hash(rows[0], columns),
            (1, "2025-03-31"): "stale-hash",
            (9, "2020-03-31"): "gone",
        }
        inserts, updates, deletes = compute_diff(mirror, spec)
        self.assertEqual([r["internal_id"] for r in inserts], [2])
        self.assertEqual([r["value"] for r in updates], [2.0])
        self.assertEqual(deletes, [(9, "2020-03-31")])

    def test_insert_sql_escapes_quotes(self):
        spec = {"name": "companies",
                "columns": [("internal_id", "INTEGER"), ("name", "TEXT")],
                "primary_key": ["internal_id"], "indexes": [],
                "rows": [], "write_factor": 2}
        sql = insert_sql(spec, [{"internal_id": 1, "name": "O'Brien"}])
        self.assertIn("'O''Brien'", sql)

    def test_update_and_delete_sql_with_null_key(self):
        spec = {"name": "gold_dividends",
                "columns": [("internal_id", "INTEGER"),
                            ("securities_code", "TEXT"),
                            ("amount", "REAL")],
                "primary_key": ["internal_id", "securities_code"],
                "indexes": [], "rows": [], "write_factor": 2}
        row = {"internal_id": 1, "securities_code": None, "amount": 5.0}
        self.assertIn('"securities_code" IS NULL', update_sql(spec, row))
        self.assertIn('"securities_code" IS NULL',
                      delete_sql(spec, row))

    def test_create_table_sql_single_int_pk(self):
        sql = create_table_sql(SPEC | {"primary_key": ["internal_id"]})
        self.assertIn('"internal_id" INTEGER PRIMARY KEY', sql)

    def test_batched_insert_sqls_respects_byte_limit(self):
        spec = {"name": "companies",
                "columns": [("internal_id", "INTEGER"), ("name", "TEXT")],
                "primary_key": ["internal_id"], "indexes": [],
                "rows": [], "write_factor": 2}
        rows = [{"internal_id": i, "name": "あ" * 4000}
                for i in range(30)]
        total = 0
        batches = list(batched_insert_sqls(spec, rows, byte_limit=30_000))
        self.assertGreater(len(batches), 1)
        for sql, count in batches:
            self.assertLessEqual(len(sql.encode("utf-8")), 30_000)
            total += count
        self.assertEqual(total, 30)


class BudgetLedgerTest(unittest.TestCase):
    def test_accumulates_and_persists(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "state.json")
            ledger = BudgetLedger(path, 100)
            self.assertEqual(ledger.remaining(), 100)
            ledger.add(30)
            self.assertEqual(ledger.remaining(), 70)
            reloaded = BudgetLedger(path, 100)
            self.assertEqual(reloaded.used_today(), 30)
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            self.assertEqual(reloaded.state["days"], {today: 30})


class SyncEndToEndTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.work_dir = self.tmp.name
        build_fixture_cache(os.path.join(self.work_dir, "cache"))
        patcher = mock.patch("pipeline.serving_sync.download_inputs",
                             lambda cache_dir: None)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.tmp.cleanup)
        self.fake = FakeD1()

    def test_full_sync_then_noop(self):
        report = run_sync(self.work_dir, client=self.fake.client())
        self.assertEqual(report["status"], "complete")
        self.assertTrue(report["complete"])
        self.assertEqual(report["count_mismatches"], {})
        self.assertEqual(report["d1_file_size"], 4242)
        companies = {r["internal_id"]: r for r in self.fake.rows("companies")}
        self.assertEqual(companies[1]["edinet_code"], "E00001")
        self.assertEqual(companies[1]["name"], "株式会社アルファ")
        self.assertEqual(companies[1]["primary_securities_code"], "11110")
        self.assertEqual(companies[2]["edinet_code"], "E00002")
        self.assertIsNone(companies[3]["edinet_code"])
        annual = self.fake.rows("gold_annual")
        self.assertEqual(len(annual), 3)
        nan_row = [r for r in annual if r["internal_id"] == 2][0]
        self.assertIsNone(nan_row["net_sales"])
        latest = self.fake.rows("gold_annual_latest")
        self.assertEqual(len(latest), 2)
        self.assertEqual(
            [r for r in latest if r["internal_id"] == 1][0]["period_end"],
            "2025-03-31")
        self.assertEqual(len(self.fake.rows("gold_dividends")), 1)
        self.assertEqual(len(self.fake.rows("relationships")), 1)
        meta = {r["key"]: r["value"]
                for r in self.fake.rows("serving_meta")}
        self.assertEqual(meta["snapshot_date"], "2026-11-20")
        counts = json.loads(meta["table_counts"])
        self.assertEqual(counts["gold_annual"], 3)

        second = run_sync(self.work_dir, client=self.fake.client())
        self.assertEqual(second["status"], "noop")
        self.assertEqual(second["planned_ops"], 0)

    def test_budget_stops_and_next_day_resumes(self):
        report = run_sync(self.work_dir, client=self.fake.client(),
                          daily_budget=2)
        self.assertEqual(report["status"], "partial")
        self.assertFalse(report["complete"])
        self.assertEqual(report["applied_ops"], 1)
        meta = {r["key"]: r["value"]
                for r in self.fake.rows("serving_meta")}
        self.assertNotIn("snapshot_date", meta)
        state_path = os.path.join(self.work_dir, "sync_state.json")
        with open(state_path, encoding="utf-8") as f:
            state = json.load(f)
        state["days"] = {"2000-01-01": state["days"][
            datetime.now(timezone.utc).strftime("%Y-%m-%d")]}
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state, f)
        resumed = run_sync(self.work_dir, client=self.fake.client())
        self.assertEqual(resumed["status"], "complete")
        meta = {r["key"]: r["value"]
                for r in self.fake.rows("serving_meta")}
        self.assertEqual(meta["snapshot_date"], "2026-11-20")

    def test_update_and_delete_propagate(self):
        run_sync(self.work_dir, client=self.fake.client())
        cache_dir = os.path.join(self.work_dir, "cache")
        annual = pq.read_table(
            os.path.join(cache_dir, "gold_annual.parquet")).to_pylist()
        annual[0]["net_sales"] = 999.0
        write_parquet(cache_dir, "gold/annual.parquet",
                      pa.Table.from_pylist(annual))
        events = pq.read_table(
            os.path.join(cache_dir, "master_company_events.parquet"))
        write_parquet(cache_dir, "master/company_events.parquet",
                      events.slice(0, 1))
        report = run_sync(self.work_dir, client=self.fake.client())
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["applied_ops"], 2)
        row = [r for r in self.fake.rows("gold_annual")
               if r["source_doc_id"] == "S1"][0]
        self.assertEqual(row["net_sales"], 999.0)
        self.assertEqual(len(self.fake.rows("events")), 1)


if __name__ == "__main__":
    unittest.main()
