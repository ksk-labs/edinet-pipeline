"""提供層 (Cloudflare D1) への同期 (P2-T6、仕様 #15 §9)。

v2 データセット (Yoshi-Dai/financial-lakehouse-v2) の現行ファイル
(Gold と会社マスタ) を目標状態とし、Cloudflare D1 との差分を、
無料枠の書き込み上限 (100,000 行/日) に対する予算の範囲で適用する。

方式 (設計の正本は Issue #21 の設計書 §4):

- 目標状態は入力 parquet から決定的に構築する。Gold 系テーブルは
  parquet のスキーマから列と型を導出し、会社マスタの現行ビュー
  (companies) は edinet_codes / identifier_history / securities
  から導出する (導出規則は build_company_rows と各 derive 関数の
  docstring が正本)。
- ローカルの SQLite ファイル (ミラー) が「D1 へ適用済みの状態」を
  保持する。差分は主キーと行ハッシュで取り、適用できた分だけを
  ミラーへも反映するため、途中で止まっても再開は差分の続きから
  始まる (冪等)。
- 適用は D1 REST の query エンドポイントへ SQL を送る。複数行
  INSERT は SQL 文の上限 (100 KB) の範囲で束ねる。各応答の
  meta.rows_written の実測値を UTC 日単位で状態ファイルへ累計し、
  1日の予算 (既定 90,000 行) を超える分は翌日へ回す。
- 資格情報は実行時に Cloudflare コネクタのヘルパー (cf_api.py) を
  import して使う。アカウント ID とデータベース UUID は引数で
  受け、コードには書かない。HTTP 層は差し替え可能で、単体テスト
  は Cloudflare へ接続しない。

使い方:
  python -m pipeline.serving_sync sync --work-dir DIR \
      --account-id ID --database-id UUID [--daily-budget 90000] \
      [--cf-api-path PATH] [--dry-run]
"""
import argparse
import hashlib
import importlib.util
import json
import math
import os
import shutil
import sqlite3
import sys
import time
import urllib.request
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

HF_BASE = ("https://huggingface.co/datasets/"
           "Yoshi-Dai/financial-lakehouse-v2/resolve/main")
INPUT_FILES = (
    "gold/annual.parquet",
    "gold/quarterly.parquet",
    "gold/dividends.parquet",
    "gold/summary.json",
    "master/companies.parquet",
    "master/securities.parquet",
    "master/edinet_codes.parquet",
    "master/identifier_history.parquet",
    "master/company_events.parquet",
    "master/company_relationships.parquet",
)
SQL_BYTE_LIMIT = 100_000
DEFAULT_DAILY_BUDGET = 90_000
NAME_KIND = "company_name"
DEFAULT_CF_API_PATH = ("~/workspace/custom-connectors/"
                       "custom.cloudflare/bin/cf_api.py")

GOLD_PRIMARY_KEYS = {
    "gold_annual": ("internal_id", "period_start", "period_end"),
    "gold_quarterly": ("internal_id", "period_start", "period_end", "doc_type"),
    "gold_dividends": ("internal_id", "period_start", "period_end",
                       "securities_code", "term_kind"),
}
TABLE_ORDER = (
    "companies", "securities", "gold_annual", "gold_annual_latest",
    "gold_quarterly", "gold_dividends", "relationships", "events",
)


def sqlite_type(field_type):
    """parquet (pyarrow) の型を SQLite の型へ対応させる。"""
    if pa.types.is_boolean(field_type):
        return "INTEGER"
    if pa.types.is_integer(field_type):
        return "INTEGER"
    if pa.types.is_floating(field_type):
        return "REAL"
    if pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
        return "TEXT"
    if pa.types.is_date(field_type) or pa.types.is_timestamp(field_type):
        return "TEXT"
    raise ValueError(f"unsupported parquet type: {field_type}")


def normalize_value(value):
    """parquet の値を SQLite へ載せられる形へそろえる。

    真偽値は 0/1、NaN と無限大は欠損 (NULL) として扱う。Gold の
    double 列は欠損を NaN で持つ場合があり、そのままでは行ハッシュ
    と SQL リテラルが安定しないため、ここで一律に NULL へ寄せる。
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def row_hash(row, columns):
    payload = json.dumps([row.get(c) for c in columns],
                         ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def _column_defs(spec, extra_columns=()):
    pk = spec["primary_key"]
    single_int_pk = (len(pk) == 1
                     and dict(spec["columns"])[pk[0]] == "INTEGER")
    parts = []
    for name, col_type in list(spec["columns"]) + list(extra_columns):
        if single_int_pk and name == pk[0]:
            parts.append(f'"{name}" INTEGER PRIMARY KEY')
        else:
            parts.append(f'"{name}" {col_type}')
    if not single_int_pk:
        pk_sql = ", ".join(f'"{c}"' for c in pk)
        parts.append(f"PRIMARY KEY ({pk_sql})")
    return ", ".join(parts)


def create_table_sql(spec):
    return (f'CREATE TABLE IF NOT EXISTS "{spec["name"]}" '
            f'({_column_defs(spec)})')


def create_index_sqls(spec):
    return [
        f'CREATE INDEX IF NOT EXISTS "{index_name}" '
        f'ON "{spec["name"]}" ({", ".join(chr(34) + c + chr(34) for c in cols)})'
        for index_name, cols in spec["indexes"]
    ]


def _pk_where(spec, values):
    parts = []
    for col in spec["primary_key"]:
        value = values[col]
        if value is None:
            parts.append(f'"{col}" IS NULL')
        else:
            parts.append(f'"{col}" = {sql_literal(value)}')
    return " AND ".join(parts)


def insert_sql(spec, rows):
    cols = [c for c, _ in spec["columns"]]
    col_sql = ", ".join(f'"{c}"' for c in cols)
    values = ", ".join(
        "(" + ", ".join(sql_literal(row.get(c)) for c in cols) + ")"
        for row in rows)
    return f'INSERT INTO "{spec["name"]}" ({col_sql}) VALUES {values}'


def update_sql(spec, row):
    pk = set(spec["primary_key"])
    sets = ", ".join(
        f'"{c}" = {sql_literal(row.get(c))}'
        for c, _ in spec["columns"] if c not in pk)
    return f'UPDATE "{spec["name"]}" SET {sets} WHERE {_pk_where(spec, row)}'


def delete_sql(spec, key_values):
    return f'DELETE FROM "{spec["name"]}" WHERE {_pk_where(spec, key_values)}'


def batched_insert_sqls(spec, rows, byte_limit=SQL_BYTE_LIMIT):
    """INSERT 文を byte_limit (UTF-8 バイト数) の範囲で束ねる。"""
    cols = [c for c, _ in spec["columns"]]
    col_sql = ", ".join(f'"{c}"' for c in cols)
    prefix = f'INSERT INTO "{spec["name"]}" ({col_sql}) VALUES '
    prefix_len = len(prefix.encode("utf-8"))
    batch = []
    batch_len = prefix_len
    for row in rows:
        fragment = "(" + ", ".join(
            sql_literal(row.get(c)) for c in cols) + ")"
        # 単一行だけで上限を超える場合は、D1 へ送っても必ず失敗し
        # その行を含むテーブル以降が毎回同じ場所で止まり続けるため、
        # 明示的に止める (N-3)。
        if prefix_len + len(fragment.encode("utf-8")) > byte_limit:
            raise ValueError(
                f"single row exceeds SQL byte limit "
                f"({byte_limit} bytes) for table {spec['name']}: "
                f"row is {prefix_len + len(fragment.encode('utf-8'))} "
                f"bytes")
        # 行どうしの区切り ", " は2バイト（カンマ＋空白）。
        frag_len = len(fragment.encode("utf-8")) + (2 if batch else 0)
        if batch and batch_len + frag_len > byte_limit:
            yield prefix + ", ".join(batch), len(batch)
            batch = []
            batch_len = prefix_len
            frag_len = len(fragment.encode("utf-8"))
        batch.append(fragment)
        batch_len += frag_len
    if batch:
        yield prefix + ", ".join(batch), len(batch)


def derive_current_edinet_code(code_rows):
    """会社の現行 EDINET コードを決める。

    最新スナップショットに残るコードのうち last_seen_date が最新の
    もの。同点の場合は、コードリスト由来の接続 (jcn_strong /
    jcn_corroborated 等) を v1 索引由来 (initial_v1) より優先し、
    それでも同点ならコードの辞書順で最小のものにする (決定的規則。
    実データでは5社が同点になり、この規則で一意に決まる)。
    """
    best = {}
    for row in code_rows:
        if not row["in_latest_snapshot"]:
            continue
        iid = row["internal_id"]
        rank = (row["last_seen_date"],
                0 if row["assignment_basis"] == "initial_v1" else 1)
        current = best.get(iid)
        if (current is None or rank > current[0]
                or (rank == current[0] and row["edinet_code"] < current[1])):
            best[iid] = (rank, row["edinet_code"])
    return {iid: code for iid, (_, code) in best.items()}


def derive_current_name(history_rows):
    """会社の現行名を決める。

    identifier_history の kind = company_name (実データの実値) の
    行から、last_observed_date が最新、同点なら observation_count
    が最大のもの。それでも同点なら値の辞書順で最小のものにする
    (実データでは未解決の同点は0件)。
    """
    best = {}
    for row in history_rows:
        if row["kind"] != NAME_KIND:
            continue
        iid = row["internal_id"]
        rank = (row["last_observed_date"], row["observation_count"])
        current = best.get(iid)
        if (current is None or rank > current[0]
                or (rank == current[0] and row["value"] < current[1])):
            best[iid] = (rank, row["value"])
    return {iid: value for iid, (_, value) in best.items()}


def build_company_rows(companies, codes, names, security_rows):
    """会社マスタの現行ビュー (companies テーブル) を構築する。"""
    primary = {}
    for row in security_rows:
        if row["is_primary"]:
            primary[row["internal_id"]] = row
    rows = []
    for company in companies:
        iid = company["internal_id"]
        main = primary.get(iid)
        rows.append({
            "internal_id": iid,
            "edinet_code": codes.get(iid),
            "name": names.get(iid),
            "status": company["status"],
            "primary_securities_code":
                main["securities_code"] if main else None,
            "security_kind": main["security_kind"] if main else None,
        })
    return rows


def derive_latest_annual(annual_rows):
    """会社ごとの最新 annual 行 (period_end 最大) を抜く。

    period_end が同点の場合は period_start が大きい方を最新とする
    (実データでは21社に同点があり、より後に始まった期間を直近の
    期間として扱う決定的規則)。
    """
    best = {}
    for row in annual_rows:
        iid = row["internal_id"]
        rank = (row["period_end"], row["period_start"])
        if iid not in best or rank > best[iid][0]:
            best[iid] = (rank, row)
    return [best[iid][1] for iid in sorted(best)]


def _read_parquet(cache_dir, rel):
    path = os.path.join(cache_dir, rel.replace("/", "_"))
    table = pq.read_table(path)
    columns = [field.name for field in table.schema]
    rows = [{c: normalize_value(row[c]) for c in columns}
            for row in table.to_pylist()]
    return table.schema, rows


def _parquet_spec(name, cache_dir, rel, primary_key, indexes, write_factor):
    schema, rows = _read_parquet(cache_dir, rel)
    columns = [(field.name, sqlite_type(field.type)) for field in schema]
    return {"name": name, "columns": columns,
            "primary_key": list(primary_key), "indexes": indexes,
            "rows": rows, "write_factor": write_factor}


def build_target(cache_dir):
    """入力ファイルから D1 の目標状態 (テーブル仕様と行) を作る。"""
    with open(os.path.join(cache_dir, "gold_summary.json"),
              encoding="utf-8") as f:
        summary = json.load(f)
    _, companies = _read_parquet(cache_dir, "master/companies.parquet")
    _, security_rows = _read_parquet(cache_dir, "master/securities.parquet")
    _, code_rows = _read_parquet(cache_dir, "master/edinet_codes.parquet")
    _, history_rows = _read_parquet(cache_dir,
                                    "master/identifier_history.parquet")
    codes = derive_current_edinet_code(code_rows)
    names = derive_current_name(history_rows)
    company_rows = build_company_rows(companies, codes, names, security_rows)

    specs = {}
    specs["companies"] = {
        "name": "companies",
        "columns": [("internal_id", "INTEGER"), ("edinet_code", "TEXT"),
                    ("name", "TEXT"), ("status", "TEXT"),
                    ("primary_securities_code", "TEXT"),
                    ("security_kind", "TEXT")],
        "primary_key": ["internal_id"],
        "indexes": [("idx_companies_edinet_code", ["edinet_code"])],
        "rows": company_rows, "write_factor": 2,
    }
    specs["securities"] = {
        "name": "securities",
        "columns": [("internal_id", "INTEGER"), ("securities_code", "TEXT"),
                    ("security_kind", "TEXT"), ("is_primary", "INTEGER")],
        "primary_key": ["internal_id", "securities_code"],
        "indexes": [("idx_securities_code", ["securities_code"])],
        "rows": [{"internal_id": r["internal_id"],
                  "securities_code": r["securities_code"],
                  "security_kind": r["security_kind"],
                  "is_primary": 1 if r["is_primary"] else 0}
                 for r in security_rows],
        "write_factor": 3,
    }
    for table, rel in (("gold_annual", "gold/annual.parquet"),
                       ("gold_quarterly", "gold/quarterly.parquet"),
                       ("gold_dividends", "gold/dividends.parquet")):
        specs[table] = _parquet_spec(table, cache_dir, rel,
                                     GOLD_PRIMARY_KEYS[table], [], 2)
    latest = {"name": "gold_annual_latest",
              "columns": specs["gold_annual"]["columns"],
              "primary_key": specs["gold_annual"]["primary_key"],
              "indexes": [],
              "rows": derive_latest_annual(specs["gold_annual"]["rows"]),
              "write_factor": 2}
    specs["gold_annual_latest"] = latest
    specs["events"] = _parquet_spec(
        "events", cache_dir, "master/company_events.parquet", ("event_id",),
        [("idx_events_company", ["internal_id", "disclosed_at"]),
         ("idx_events_related", ["related_internal_id"])], 4)
    specs["relationships"] = _parquet_spec(
        "relationships", cache_dir, "master/company_relationships.parquet",
        ("parent_edinet_code", "subsidiary_edinet_code", "relation_kind"),
        [("idx_relationships_parent", ["parent_internal_id"]),
         ("idx_relationships_subsidiary", ["subsidiary_internal_id"])], 4)
    meta = {"snapshot_date": summary["snapshot_date"],
            "gold_generated_at": summary["generated_at"]}
    return specs, meta


def compute_diff(mirror_hashes, spec):
    """ミラー (適用済み) と目標の差分を INSERT / UPDATE / DELETE に分ける。"""
    columns = [c for c, _ in spec["columns"]]
    target = {}
    for row in spec["rows"]:
        key = tuple(row.get(c) for c in spec["primary_key"])
        target[key] = row
    inserts, updates = [], []
    for key, row in target.items():
        if key not in mirror_hashes:
            inserts.append(row)
        elif mirror_hashes[key] != row_hash(row, columns):
            updates.append(row)
    deletes = [key for key in mirror_hashes if key not in target]
    return inserts, updates, deletes


class BudgetLedger:
    """UTC 日単位の書き込み実績 (rows_written) を状態ファイルに持つ。"""

    def __init__(self, path, daily_budget):
        self.path = path
        self.daily_budget = daily_budget
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                self.state = json.load(f)
        else:
            self.state = {"days": {}, "applied_snapshot_date": None}

    def _today(self):
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def used_today(self):
        return self.state["days"].get(self._today(), 0)

    def remaining(self):
        return self.daily_budget - self.used_today()

    def add(self, rows_written):
        today = self._today()
        self.state["days"][today] = self.state["days"].get(today, 0) \
            + rows_written
        self.save()

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=1)
        os.replace(tmp, self.path)


class D1Error(RuntimeError):
    pass


class D1Client:
    """D1 REST の query エンドポイントへの薄いラッパ。

    call は cf_api.call と同じ署名 (method, path, body) ->
    (status, dict) の関数で、テストでは差し替える。
    """

    def __init__(self, call, account_id, database_id):
        self._call = call
        self._base = f"/accounts/{account_id}/d1/database/{database_id}"

    def query(self, sql):
        status, resp = self._call("POST", self._base + "/query",
                                  {"sql": sql})
        if status != 200 or not resp.get("success"):
            raise D1Error(f"D1 query failed: status={status} resp={resp}")
        result = resp["result"][0]
        rows = result.get("results") or []
        rows_written = (result.get("meta") or {}).get("rows_written") or 0
        return rows, rows_written

    def database_info(self):
        status, resp = self._call("GET", self._base)
        if status != 200 or not resp.get("success"):
            raise D1Error(f"D1 database info failed: status={status}")
        return resp["result"]


def load_cf_module(cf_api_path=None):
    path = os.path.expanduser(
        cf_api_path or os.environ.get("CF_API_PATH") or DEFAULT_CF_API_PATH)
    spec = importlib.util.spec_from_file_location("cf_api", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def download_inputs(cache_dir):
    os.makedirs(cache_dir, exist_ok=True)
    for rel in INPUT_FILES:
        dest = os.path.join(cache_dir, rel.replace("/", "_"))
        url = f"{HF_BASE}/{rel}"
        last_error = None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=300) as resp, \
                        open(dest + ".tmp", "wb") as f:
                    shutil.copyfileobj(resp, f)
                os.replace(dest + ".tmp", dest)
                break
            except OSError as exc:
                last_error = exc
                time.sleep(2 ** attempt)
        else:
            raise last_error


META_SPEC = {
    "name": "serving_meta",
    "columns": [("key", "TEXT"), ("value", "TEXT")],
    "primary_key": ["key"],
    "indexes": [],
    "rows": [],
    "write_factor": 2,
}


def _mirror_connect(path):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _mirror_ensure_table(conn, spec):
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (spec["name"],)).fetchone()
    if not exists:
        defs = _column_defs(spec, extra_columns=[("_row_hash", "TEXT")])
        conn.execute(f'CREATE TABLE "{spec["name"]}" ({defs})')
        return
    existing = {r[1] for r in conn.execute(
        f'PRAGMA table_info("{spec["name"]}")')}
    for name, col_type in spec["columns"]:
        if name not in existing:
            conn.execute(
                f'ALTER TABLE "{spec["name"]}" ADD COLUMN '
                f'"{name}" {col_type}')
    if "_row_hash" not in existing:
        conn.execute(
            f'ALTER TABLE "{spec["name"]}" ADD COLUMN "_row_hash" TEXT')


def _mirror_hashes(conn, spec):
    pk = spec["primary_key"]
    cols = ", ".join(f'"{c}"' for c in pk)
    hashes = {}
    for row in conn.execute(
            f'SELECT {cols}, "_row_hash" FROM "{spec["name"]}"'):
        hashes[tuple(row[:-1])] = row[-1]
    return hashes


def _mirror_upsert(conn, spec, row):
    # 先に当該主キーの既存行を削除してから挿入する。SQLite の複合
    # 主キーは NULL を含む行の一意性を強制しないため、INSERT OR
    # REPLACE だけでは NULL キーの行が置換されずに旧行が残る
    # (M-1)。削除の条件は _mirror_delete と同じ NULL 安全な形。
    key = tuple(row.get(c) for c in spec["primary_key"])
    _mirror_delete(conn, spec, key)
    columns = [c for c, _ in spec["columns"]]
    values = [row.get(c) for c in columns]
    values.append(row_hash(row, columns))
    col_sql = ", ".join(f'"{c}"' for c in columns) + ', "_row_hash"'
    marks = ", ".join("?" for _ in values)
    conn.execute(
        f'INSERT OR REPLACE INTO "{spec["name"]}" ({col_sql}) '
        f'VALUES ({marks})', values)


def _mirror_delete(conn, spec, key):
    where = " AND ".join(
        f'"{c}" IS NULL' if v is None else f'"{c}" = ?'
        for c, v in zip(spec["primary_key"], key))
    params = [v for v in key if v is not None]
    conn.execute(f'DELETE FROM "{spec["name"]}" WHERE {where}', params)


def _d1_ensure_schema(client, ledger, specs):
    for spec in specs:
        _, written = client.query(create_table_sql(spec))
        ledger.add(written)
        for sql in create_index_sqls(spec):
            _, written = client.query(sql)
            ledger.add(written)
        rows, _ = client.query(f'PRAGMA table_info("{spec["name"]}")')
        existing = {r["name"] for r in rows}
        if not existing:
            continue
        for name, col_type in spec["columns"]:
            if name not in existing:
                _, written = client.query(
                    f'ALTER TABLE "{spec["name"]}" ADD COLUMN '
                    f'"{name}" {col_type}')
                ledger.add(written)


def _apply_table(client, ledger, conn, spec, diff):
    """1テーブル分の差分を予算の範囲で適用する。

    戻り値は (適用した演算数, 予算切れで止まったか)。
    """
    inserts, updates, deletes = diff
    applied = 0
    factor = spec["write_factor"]
    for key in deletes:
        if factor > ledger.remaining():
            return applied, True
        key_row = dict(zip(spec["primary_key"], key))
        _, written = client.query(delete_sql(spec, key_row))
        ledger.add(written)
        _mirror_delete(conn, spec, key)
        applied += 1
    for row in updates:
        if factor > ledger.remaining():
            return applied, True
        _, written = client.query(update_sql(spec, row))
        ledger.add(written)
        _mirror_upsert(conn, spec, row)
        applied += 1
    pending = list(inserts)
    while pending:
        capacity = ledger.remaining() // factor
        if capacity <= 0:
            return applied, True
        sql, count = next(iter(batched_insert_sqls(spec, pending)))
        taken = pending[:count]
        if count > capacity:
            taken = pending[:capacity]
            sql = insert_sql(spec, taken)
        _, written = client.query(sql)
        ledger.add(written)
        for row in taken:
            _mirror_upsert(conn, spec, row)
        applied += len(taken)
        pending = pending[len(taken):]
    return applied, False


def _upsert_meta(client, ledger, conn, values):
    for key, value in values.items():
        row = {"key": key, "value": value}
        sql = (f'INSERT OR REPLACE INTO "serving_meta" ("key", "value") '
               f'VALUES ({sql_literal(key)}, {sql_literal(value)})')
        _, written = client.query(sql)
        ledger.add(written)
        _mirror_upsert(conn, META_SPEC, row)
    conn.commit()


def run_sync(work_dir, client=None, daily_budget=DEFAULT_DAILY_BUDGET,
             dry_run=False):
    """同期を1回実行し、結果の要約 (dict) を返す。"""
    cache_dir = os.path.join(work_dir, "cache")
    if not dry_run or not os.path.exists(
            os.path.join(cache_dir, "gold_summary.json")):
        download_inputs(cache_dir)
    specs, meta = build_target(cache_dir)
    ledger = BudgetLedger(os.path.join(work_dir, "sync_state.json"),
                          daily_budget)
    conn = _mirror_connect(os.path.join(work_dir, "mirror.sqlite"))
    all_specs = [META_SPEC] + [specs[name] for name in TABLE_ORDER]
    for spec in all_specs:
        _mirror_ensure_table(conn, spec)
    conn.commit()

    diffs = {}
    total_ops = 0
    for name in TABLE_ORDER:
        diff = compute_diff(_mirror_hashes(conn, specs[name]), specs[name])
        diffs[name] = diff
        total_ops += sum(len(part) for part in diff)
    report = {
        "snapshot_date": meta["snapshot_date"],
        "gold_generated_at": meta["gold_generated_at"],
        "planned_ops": total_ops,
        "applied_ops": 0,
        "rows_written_today": ledger.used_today(),
        "complete": False,
        "dry_run": dry_run,
    }
    if total_ops == 0 and \
            ledger.state.get("applied_snapshot_date") == meta["snapshot_date"]:
        report["complete"] = True
        report["status"] = "noop"
        return report
    if dry_run:
        report["estimated_rows_written"] = sum(
            sum(len(part) for part in diffs[name])
            * specs[name]["write_factor"] for name in TABLE_ORDER)
        report["status"] = "planned"
        return report

    _d1_ensure_schema(client, ledger, all_specs)
    stopped = False
    for name in TABLE_ORDER:
        applied, stopped = _apply_table(
            client, ledger, conn, specs[name], diffs[name])
        report["applied_ops"] += applied
        conn.commit()
        if stopped:
            break
    counts = {}
    mismatches = {}
    for name in TABLE_ORDER:
        rows, _ = client.query(
            f'SELECT COUNT(*) AS n FROM "{name}"')
        counts[name] = rows[0]["n"]
        mirror_count = conn.execute(
            f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        if counts[name] != mirror_count:
            mismatches[name] = {"d1": counts[name], "mirror": mirror_count}
    # 版情報は全量適用が済んだ場合のみ進める。行数に不一致がある
    # 状態は全量適用が実測で確認できていないため complete にしない
    # (S-1)。synced_at と table_counts の記録は残す。
    complete = (not stopped and report["applied_ops"] == total_ops
                and not mismatches)
    meta_values = {
        "synced_at": datetime.now(timezone.utc).isoformat(),
        "table_counts": json.dumps(counts, ensure_ascii=False),
    }
    if complete:
        meta_values["snapshot_date"] = meta["snapshot_date"]
        meta_values["gold_generated_at"] = meta["gold_generated_at"]
        ledger.state["applied_snapshot_date"] = meta["snapshot_date"]
        ledger.save()
    _upsert_meta(client, ledger, conn, meta_values)
    info = client.database_info()
    report.update({
        "status": "complete" if complete else "partial",
        "complete": complete,
        "rows_written_today": ledger.used_today(),
        "table_counts": counts,
        "count_mismatches": mismatches,
        "d1_file_size": info.get("file_size"),
    })
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(prog="pipeline.serving_sync")
    sub = parser.add_subparsers(dest="command", required=True)
    sync_parser = sub.add_parser("sync")
    sync_parser.add_argument("--work-dir", required=True)
    sync_parser.add_argument("--account-id")
    sync_parser.add_argument("--database-id")
    sync_parser.add_argument("--daily-budget", type=int,
                             default=DEFAULT_DAILY_BUDGET)
    sync_parser.add_argument("--cf-api-path")
    sync_parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    client = None
    if not args.dry_run:
        if not args.account_id or not args.database_id:
            parser.error("sync には --account-id と --database-id が必要")
        module = load_cf_module(args.cf_api_path)
        client = D1Client(module.call, args.account_id, args.database_id)
    report = run_sync(args.work_dir, client=client,
                      daily_budget=args.daily_budget, dry_run=args.dry_run)
    print(json.dumps(report, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
