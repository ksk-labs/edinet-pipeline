"""Silver 変換のバッチ実行管理。

要件 (Issue #9 工程1 / Issue #10):
- 処理済みを目録 (ledger.jsonl) で管理し、再実行しても重複しない (冪等)。
  冪等性の鍵は入力の同一性で、目録には書類 zip の SHA-256 を記録し、
  再実行時は「目録が ok で、入力 zip のハッシュが同じで、出力シャードが
  実在する」場合だけ処理済みとして飛ばす。入力が変われば再変換する。
- 途中から再開できる。出力は書類ごとのシャード (silver/<doc_id>.parquet) に
  書き、シャードを実体ファイルへ原子的に置いてから目録へ1行追記する。
  途中で止まっても、目録にある分は次回そのまま使える。目録の最終行が
  中断で壊れていても読み込み時に無視する (追記は末尾にしか起きないため)。
- 1書類の失敗で全体を止めない。失敗は目録へ status=failed と理由つきで
  記録して次へ進む。失敗した書類は次回の実行で再試行する。

入力は v1 索引 (documents_index.parquet) から作るジョブ列、または
ジョブ CSV (doc_id, doc_type, parent_doc_id, submit_at, raw_zip_path)。
書類 zip の実体は日別 tar (raw/edinet/YYYY/MM/DD.tar) の中の
zip/<doc_id>.zip にあるため、tar 単位でまとめて開いて取り出す。
"""
import csv
import hashlib
import io
import os
import re
import tarfile
import traceback
import zipfile
from collections import Counter
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from .convert_core import convert_document, new_stats
from .ledger import JsonlLedger

SILVER_SCHEMA = pa.schema([
    ("doc_id", pa.string()),
    ("element", pa.string()),
    ("value", pa.float64()),
    ("unit", pa.string()),
    ("context_start", pa.string()),
    ("context_end", pa.string()),
    ("dimension", pa.string()),
    ("submit_at", pa.string()),
    ("is_correction", pa.bool_()),
    ("parent_doc_id", pa.string()),
    ("superseded_by", pa.string()),
    ("point_in_time", pa.string()),
])

_JOB_COLUMNS = ["doc_id", "doc_type", "parent_doc_id", "submit_at", "raw_zip_path"]


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- ジョブと系譜

def load_index_rows(index_path):
    """v1 索引 parquet を dict のリストで返す。"""
    table = pq.read_table(index_path)
    return table.to_pylist()


def build_lineage(index_rows):
    """訂正の系譜を索引全体から作る。

    戻り値: (parent_map, superseded_map)。
    parent_map: 訂正書類 doc_id -> 親 doc_id。
    superseded_map: 親 doc_id -> その親を訂正する最新の訂正書類 doc_id。
    同一の親に複数の訂正がある場合は、提出日時が最も新しいものを採る
    (同点なら doc_id の大きい方)。確定値は最新の訂正で決まるため。
    """
    parent_map = {}
    best = {}
    for r in index_rows:
        if r.get("doc_type") == "130" and r.get("parent_doc_id"):
            parent_map[r["doc_id"]] = r["parent_doc_id"]
            key = (r.get("submit_at") or "", r["doc_id"])
            cur = best.get(r["parent_doc_id"])
            if cur is None or key > cur[0]:
                best[r["parent_doc_id"]] = (key, r["doc_id"])
    superseded_map = {p: v[1] for p, v in best.items()}
    return parent_map, superseded_map


def jobs_from_index(index_rows, since=None, until=None, doc_types=None,
                    xbrl_only=True):
    """索引からジョブ列を作る。日付は submit_at の先頭10文字で判定する。"""
    jobs = []
    for r in index_rows:
        if xbrl_only and not r.get("xbrl_flag"):
            continue
        if not r.get("raw_zip_path"):
            continue
        day = (r.get("submit_at") or "")[:10]
        if since and day < since:
            continue
        if until and day > until:
            continue
        if doc_types and r.get("doc_type") not in doc_types:
            continue
        jobs.append({c: r.get(c) for c in _JOB_COLUMNS})
    return jobs


def jobs_from_csv(csv_path, index_by_id=None):
    """ジョブ CSV からジョブ列を作る。

    CSV に doc_id だけがある場合は、--index 側の索引で他の列を補う。
    索引がある場合は doc_type / parent_doc_id / submit_at / raw_zip_path は
    索引を正として上書きする (試作の標本マニフェストのように cell 列しか
    持たない CSV でも動くようにするため)。
    """
    jobs = []
    with open(csv_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            doc_id = row.get("doc_id")
            if not doc_id:
                continue
            job = {c: (row.get(c) or None) for c in _JOB_COLUMNS}
            job["doc_id"] = doc_id
            if index_by_id and doc_id in index_by_id:
                src = index_by_id[doc_id]
                for c in ("doc_type", "parent_doc_id", "submit_at", "raw_zip_path"):
                    job[c] = src.get(c)
            jobs.append(job)
    return jobs


_TAR_NAME = re.compile(r"raw/edinet/year=(\d+)/month=(\d+)/day=(\d+)/zip/")


def day_tar_name(raw_zip_path):
    """raw_zip_path から日別 tar のファイル名 (YYYY-MM-DD.tar) を作る。"""
    if not raw_zip_path:
        return None
    m = _TAR_NAME.match(raw_zip_path)
    if not m:
        return None
    y, mo, d = m.groups()
    return f"{y}-{mo}-{d}.tar"


# ---------------------------------------------------------------- 目録 (ledger)

class Ledger(JsonlLedger):
    """処理済み目録。キーは doc_id で、土台は pipeline.ledger.JsonlLedger。

    土台との違いは is_done だけ: 処理済みとみなすのは、目録の最新行が
    ok で、入力 zip のハッシュが記録と一致し、出力シャードが実在する
    場合に限る。
    """

    def __init__(self, path):
        super().__init__(path, lambda entry: entry.get("doc_id"))

    def is_done(self, doc_id, zip_sha256, shard_path):
        """入力が同一で出力が実在する場合だけ処理済みとみなす。"""
        e = self.latest.get(doc_id)
        return (e is not None and e.get("status") == "ok"
                and e.get("zip_sha256") == zip_sha256
                and os.path.exists(shard_path))


# ---------------------------------------------------------------- バッチ実行

def _write_shard(rows, shard_path):
    """書類ごとのシャードを原子的に書く (一時ファイル -> 置き換え)。"""
    table = pa.Table.from_pylist(rows, schema=SILVER_SCHEMA)
    tmp = shard_path + ".tmp"
    pq.write_table(table, tmp)
    os.replace(tmp, shard_path)


def _record_failure(ledger, job, kind, detail):
    ledger.append({
        "doc_id": job["doc_id"], "status": "failed",
        "error_kind": kind, "error": detail,
        "attempts": ledger.attempts[job["doc_id"]] + 1,
        "updated_at": _now(),
    })


def run_batch(jobs, parent_map, superseded_map, tars_dir, state_dir, out_dir,
              max_docs=None):
    """ジョブ列を処理する。戻り値は実行サマリ (dict)。

    max_docs は今回の実行で「試行する」書類数の上限 (処理済みとして飛ばした
    分は数えない)。長時間のバックフィルを区切って回すためのもので、
    途中終了しても目録とシャードは整合した状態で残る。
    """
    os.makedirs(state_dir, exist_ok=True)
    silver_dir = os.path.join(out_dir, "silver")
    os.makedirs(silver_dir, exist_ok=True)
    ledger = Ledger(os.path.join(state_dir, "ledger.jsonl"))
    summary = {
        "started_at": _now(),
        "jobs": len(jobs),
        "processed_ok": 0, "skipped_done": 0, "failed": 0,
        "stopped_early": False,
        "rows_total": 0,
        "stats_total": new_stats(),
        "stats_by_source": {"xbrl": new_stats(), "ixbrl": new_stats()},
        "out_of_scope_counts": {},
        "ixbrl_new_rows": 0,
        "failures": [],
        "ledger_corrupt_lines": ledger.corrupt_lines,
    }
    oos_counters = {}
    # tar 単位にまとめる (tar は1回だけ開く)。ジョブの順序は保持する。
    groups = {}
    order = []
    for i, job in enumerate(jobs):
        name = day_tar_name(job.get("raw_zip_path"))
        if name not in groups:
            groups[name] = []
            order.append(name)
        groups[name].append((i, job))
    attempted = 0
    stop = False
    for tar_name in order:
        if stop:
            break
        members = groups[tar_name]
        tar_path = os.path.join(tars_dir, tar_name) if tar_name else None
        if tar_path is None or not os.path.exists(tar_path):
            for _, job in members:
                _record_failure(ledger, job, "tar_missing",
                                f"tar not found: {tar_name}")
                summary["failed"] += 1
                summary["failures"].append(
                    {"doc_id": job["doc_id"], "error_kind": "tar_missing"})
            continue
        try:
            tf = tarfile.open(tar_path, "r")
        except (tarfile.TarError, OSError) as e:
            for _, job in members:
                _record_failure(ledger, job, "tar_unreadable",
                                f"{type(e).__name__}: {e}")
                summary["failed"] += 1
                summary["failures"].append(
                    {"doc_id": job["doc_id"], "error_kind": "tar_unreadable"})
            continue
        with tf:
            member_by_doc = {}
            try:
                for m in tf.getmembers():
                    base = os.path.basename(m.name)
                    if base.endswith(".zip") and m.isfile():
                        member_by_doc[base[:-4]] = m
            except (tarfile.TarError, OSError) as e:
                for _, job in members:
                    _record_failure(ledger, job, "tar_unreadable",
                                    f"{type(e).__name__}: {e}")
                    summary["failed"] += 1
                    summary["failures"].append(
                        {"doc_id": job["doc_id"], "error_kind": "tar_unreadable"})
                continue
            for _, job in members:
                if max_docs is not None and attempted >= max_docs:
                    summary["stopped_early"] = True
                    stop = True
                    break
                doc_id = job["doc_id"]
                shard_path = os.path.join(silver_dir, f"{doc_id}.parquet")
                member = member_by_doc.get(doc_id)
                if member is None:
                    attempted += 1
                    _record_failure(ledger, job, "zip_not_in_tar",
                                    f"zip/{doc_id}.zip not in {tar_name}")
                    summary["failed"] += 1
                    summary["failures"].append(
                        {"doc_id": doc_id, "error_kind": "zip_not_in_tar"})
                    continue
                try:
                    fobj = tf.extractfile(member)
                    if fobj is None:
                        raise tarfile.TarError(f"cannot extract {member.name}")
                    zip_bytes = fobj.read()
                except (tarfile.TarError, OSError) as e:
                    attempted += 1
                    _record_failure(ledger, job, "zip_unreadable",
                                    f"{type(e).__name__}: {e}")
                    summary["failed"] += 1
                    summary["failures"].append(
                        {"doc_id": doc_id, "error_kind": "zip_unreadable"})
                    continue
                zip_hash = sha256_bytes(zip_bytes)
                if ledger.is_done(doc_id, zip_hash, shard_path):
                    summary["skipped_done"] += 1
                    continue
                attempted += 1
                meta = {
                    "doc_id": doc_id,
                    "submit_at": job.get("submit_at"),
                    "is_correction": job.get("doc_type") == "130",
                    "parent_doc_id": parent_map.get(doc_id) or job.get("parent_doc_id"),
                    "superseded_by": superseded_map.get(doc_id),
                }
                try:
                    rows, stats, by_src, oos, ix_new = convert_document(
                        zip_bytes, meta)
                except zipfile.BadZipFile as e:
                    _record_failure(ledger, job, "bad_zip", f"{e}")
                    summary["failed"] += 1
                    summary["failures"].append(
                        {"doc_id": doc_id, "error_kind": "bad_zip"})
                    continue
                except Exception as e:  # 1書類の失敗で全体を止めない
                    _record_failure(ledger, job, "convert_error",
                                    f"{type(e).__name__}: {e}\n"
                                    + traceback.format_exc(limit=3))
                    summary["failed"] += 1
                    summary["failures"].append(
                        {"doc_id": doc_id, "error_kind": "convert_error"})
                    continue
                _write_shard(rows, shard_path)
                ledger.append({
                    "doc_id": doc_id, "status": "ok",
                    "rows": len(rows), "zip_sha256": zip_hash,
                    "shard": f"silver/{doc_id}.parquet",
                    "attempts": ledger.attempts[doc_id] + 1,
                    "updated_at": _now(),
                })
                summary["processed_ok"] += 1
                summary["rows_total"] += len(rows)
                summary["ixbrl_new_rows"] += ix_new
                for k, v in stats.items():
                    if isinstance(v, int):
                        summary["stats_total"][k] = \
                            summary["stats_total"].get(k, 0) + v
                for src in ("xbrl", "ixbrl"):
                    for k, v in by_src[src].items():
                        summary["stats_by_source"][src][k] += v
                for kind, items in oos.items():
                    cnt = oos_counters.setdefault(kind, Counter())
                    for item in items:
                        cnt[item[1]] += 1
    summary["out_of_scope_counts"] = {
        kind: {"total": sum(c.values()),
               "top_elements": c.most_common(50)}
        for kind, c in oos_counters.items()
    }
    summary["finished_at"] = _now()
    return summary


def combine(jobs, state_dir, out_dir):
    """目録で ok のシャードをジョブ順に結合し、silver.parquet を作る。

    戻り値: (path, rows, sha256, missing_shards)。missing_shards は目録では
    ok なのにシャードが無い書類 (通常は起きない)。結合は決定的で、
    同じシャード集合からは常に同じファイルになる。
    """
    ledger = Ledger(os.path.join(state_dir, "ledger.jsonl"))
    silver_dir = os.path.join(out_dir, "silver")
    tables = []
    total = 0
    missing_shards = []
    for job in jobs:
        doc_id = job["doc_id"]
        entry = ledger.latest.get(doc_id)
        if entry is None or entry.get("status") != "ok":
            continue
        shard_path = os.path.join(silver_dir, f"{doc_id}.parquet")
        if not os.path.exists(shard_path):
            missing_shards.append(doc_id)
            continue
        t = pq.read_table(shard_path)
        tables.append(t)
        total += t.num_rows
    if tables:
        combined = pa.concat_tables(tables)
    else:
        combined = pa.Table.from_pylist([], schema=SILVER_SCHEMA)
    out_path = os.path.join(out_dir, "silver.parquet")
    tmp = out_path + ".tmp"
    pq.write_table(combined, tmp)
    os.replace(tmp, out_path)
    return out_path, total, sha256_file(out_path), missing_shards
