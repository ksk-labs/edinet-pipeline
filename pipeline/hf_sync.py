"""v2 データセット (Hugging Face) との月次状態の同期 (S2-T2)。

バックフィルでは月ごとに次の3ファイルを v2 の silver/month=YYYY-MM/ に置く。

- ledger.jsonl: その月の処理済み目録 (pipeline.batch の Ledger と同じ実体)
- silver.parquet: その月の結合済み Silver (pipeline.cli の --combine-only が生成)
- run_summary.json: 直近の CLI 実行サマリ

fetch は3ファイルを落として目録を state-dir へ、結合を out-dir へ置き、
結合を書類ごとのシャードへ割って out-dir/silver/ に戻す。これにより
pipeline.cli の処理済み判定 (目録 ok + 入力 zip のハッシュ一致 +
シャード実在) が実行をまたいでも働く。目録では ok なのに結合に含まれて
いない書類はシャードが無いため自動で再変換される (自己回復)。

push は目録と結合とサマリを一括で上げる。push-ledger は目録だけを
上げるもので、日次の変換のたびに呼んで前進を細かく残すために使う。

days は索引からその月にジョブがある日 (XBRL あり・zip パスあり) を
列挙する。ワークフローが日別 tar の取得対象を決めるために使う。

認証は環境変数 HF_TOKEN を huggingface_hub が自動で読む。読み取りは
公開データセットなのでトークンなしでも動く。
"""
import argparse
import json
import os
import shutil
import sys
import tempfile

import pyarrow.compute as pc
import pyarrow.parquet as pq
from huggingface_hub import HfApi, snapshot_download

from .batch import SILVER_SCHEMA, jobs_from_index, load_index_rows

DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"
FILES = ("ledger.jsonl", "silver.parquet", "run_summary.json")


def _prefix(month):
    return f"silver/month={month}"


def _split_shards(combined_path, silver_dir):
    """結合 parquet を書類ごとのシャードへ割る。戻り値はシャード数。"""
    table = pq.read_table(combined_path)
    os.makedirs(silver_dir, exist_ok=True)
    if table.num_rows == 0:
        return 0
    order = pc.sort_indices(table, sort_keys=[("doc_id", "ascending")])
    table = table.take(order)
    doc_ids = table.column("doc_id").to_pylist()
    count = 0
    start = 0
    for i in range(1, len(doc_ids) + 1):
        if i == len(doc_ids) or doc_ids[i] != doc_ids[start]:
            shard = table.slice(start, i - start)
            pq.write_table(shard, os.path.join(
                silver_dir, f"{doc_ids[start]}.parquet"))
            count += 1
            start = i
    return count


def fetch(repo, month, state_dir, out_dir):
    """v2 からその月の状態を復元する。未着手の月は fresh を返す。"""
    prefix = _prefix(month)
    api = HfApi()
    files = [f for f in api.list_repo_files(repo, repo_type="dataset")
             if f.startswith(prefix + "/")]
    if f"{prefix}/ledger.jsonl" not in files:
        return {"month": month, "status": "fresh"}
    with tempfile.TemporaryDirectory() as tmp:
        snapshot_download(repo_id=repo, repo_type="dataset",
                          allow_patterns=[prefix + "/*"], local_dir=tmp)
        src = os.path.join(tmp, prefix)
        os.makedirs(state_dir, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)
        shutil.copyfile(os.path.join(src, "ledger.jsonl"),
                        os.path.join(state_dir, "ledger.jsonl"))
        combined = os.path.join(src, "silver.parquet")
        rows = 0
        shards = 0
        if os.path.exists(combined):
            shutil.copyfile(combined, os.path.join(out_dir, "silver.parquet"))
            rows = pq.read_metadata(os.path.join(out_dir, "silver.parquet")).num_rows
            shards = _split_shards(os.path.join(out_dir, "silver.parquet"),
                                   os.path.join(out_dir, "silver"))
        summary = os.path.join(src, "run_summary.json")
        if os.path.exists(summary):
            shutil.copyfile(summary, os.path.join(out_dir, "run_summary.json"))
    with open(os.path.join(state_dir, "ledger.jsonl"), encoding="utf-8") as f:
        entries = sum(1 for line in f if line.strip())
    return {"month": month, "status": "resumed", "ledger_lines": entries,
            "combined_rows": rows, "shards_restored": shards}


def push(repo, month, state_dir, out_dir):
    """その月の目録・結合・サマリを v2 へ上げる。"""
    prefix = _prefix(month)
    api = HfApi()
    with tempfile.TemporaryDirectory() as stage:
        shutil.copyfile(os.path.join(state_dir, "ledger.jsonl"),
                        os.path.join(stage, "ledger.jsonl"))
        for name in ("silver.parquet", "run_summary.json"):
            src = os.path.join(out_dir, name)
            if os.path.exists(src):
                shutil.copyfile(src, os.path.join(stage, name))
        result = api.upload_folder(
            folder_path=stage, repo_id=repo, repo_type="dataset",
            path_in_repo=prefix,
            commit_message=f"backfill {month}: ledger and combined silver")
    return {"month": month, "status": "pushed",
            "commit_url": getattr(result, "commit_url", None)}


def push_ledger(repo, month, state_dir):
    """目録だけを v2 へ上げる (日次の前進記録)。"""
    prefix = _prefix(month)
    api = HfApi()
    result = api.upload_file(
        path_or_fileobj=os.path.join(state_dir, "ledger.jsonl"),
        path_in_repo=f"{prefix}/ledger.jsonl",
        repo_id=repo, repo_type="dataset",
        commit_message=f"backfill {month}: ledger progress")
    return {"month": month, "status": "ledger_pushed",
            "commit_url": getattr(result, "commit_url", None)}


def days(index_path, month):
    """索引からその月にジョブがある日を列挙する。"""
    rows = load_index_rows(index_path)
    jobs = jobs_from_index(rows, since=f"{month}-01", until=f"{month}-31")
    return sorted({(j.get("submit_at") or "")[:10] for j in jobs})


def main(argv=None):
    ap = argparse.ArgumentParser(description="v2 データセットとの月次状態の同期")
    ap.add_argument("command", choices=["fetch", "push", "push-ledger", "days"])
    ap.add_argument("--repo", default=DEFAULT_REPO)
    ap.add_argument("--month", required=True, help="対象月 YYYY-MM")
    ap.add_argument("--index", help="v1 索引 parquet (days のとき必須)")
    ap.add_argument("--state-dir", help="目録の置き場所 (days 以外は必須)")
    ap.add_argument("--out-dir", help="シャードと結合の出力先 (fetch/push で必須)")
    args = ap.parse_args(argv)

    if args.command == "days":
        if not args.index:
            ap.error("days には --index が必要です")
        for d in days(args.index, args.month):
            print(d)
        return 0
    if not args.state_dir:
        ap.error("--state-dir が必要です")
    if args.command == "fetch":
        if not args.out_dir:
            ap.error("fetch には --out-dir が必要です")
        result = fetch(args.repo, args.month, args.state_dir, args.out_dir)
    elif args.command == "push":
        if not args.out_dir:
            ap.error("push には --out-dir が必要です")
        result = push(args.repo, args.month, args.state_dir, args.out_dir)
    else:
        result = push_ledger(args.repo, args.month, args.state_dir)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
