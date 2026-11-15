"""Silver 変換バッチの CLI。

使い方の例 (標本29書類):
    python -m pipeline.cli \
        --index /path/to/documents_index.parquet \
        --jobs-csv verification/sample_jobs.csv \
        --tars-dir /path/to/tars \
        --state-dir /path/to/state --out-dir /path/to/out

使い方の例 (索引から日付範囲でジョブを作る):
    python -m pipeline.cli \
        --index /path/to/documents_index.parquet \
        --since 2026-10-28 --until 2026-10-28 \
        --tars-dir /path/to/tars \
        --state-dir /path/to/state --out-dir /path/to/out

処理済みは state-dir の ledger.jsonl で管理され、再実行では入力 zip の
ハッシュが同じ書類を飛ばす。--max-docs は今回の実行で試行する書類数の
上限で、長時間のバックフィルを区切って回すために使う。
終了コード: 正常終了は 0。--strict のときに失敗が1件でもあれば 2。
"""
import argparse
import json
import os
import sys

from .batch import (build_lineage, combine, jobs_from_csv, jobs_from_index,
                    load_index_rows, run_batch)


def main(argv=None):
    ap = argparse.ArgumentParser(description="EDINET XBRL -> Silver 変換バッチ")
    ap.add_argument("--index", help="v1 索引 parquet (documents_index.parquet)")
    ap.add_argument("--jobs-csv", help="ジョブ CSV (doc_id 列が必須)")
    ap.add_argument("--since", help="対象の開始日 YYYY-MM-DD (索引モード)")
    ap.add_argument("--until", help="対象の終了日 YYYY-MM-DD (索引モード)")
    ap.add_argument("--doc-types", help="対象の doc_type をカンマ区切りで (例: 120,130)")
    ap.add_argument("--all-documents", action="store_true",
                    help="索引モードで xbrl_flag のない書類も対象にする (既定は XBRL のみ)")
    ap.add_argument("--tars-dir", required=True, help="日別 tar の置き場所")
    ap.add_argument("--state-dir", required=True, help="目録 (ledger.jsonl) の置き場所")
    ap.add_argument("--out-dir", required=True, help="シャードと結合結果の出力先")
    ap.add_argument("--max-docs", type=int, default=None,
                    help="今回の実行で試行する書類数の上限")
    ap.add_argument("--combine-only", action="store_true",
                    help="変換せず、目録で ok のシャードの結合だけを行う")
    ap.add_argument("--strict", action="store_true",
                    help="失敗が1件でもあれば終了コード 2 で終わる")
    args = ap.parse_args(argv)

    index_rows = None
    index_by_id = None
    parent_map, superseded_map = {}, {}
    if args.index:
        index_rows = load_index_rows(args.index)
        index_by_id = {r["doc_id"]: r for r in index_rows}
        parent_map, superseded_map = build_lineage(index_rows)

    if args.jobs_csv:
        jobs = jobs_from_csv(args.jobs_csv, index_by_id)
        if not index_rows:
            # 索引がない場合は CSV の行同士から系譜を作る
            pseudo = [{"doc_id": j["doc_id"], "doc_type": j.get("doc_type"),
                       "parent_doc_id": j.get("parent_doc_id"),
                       "submit_at": j.get("submit_at")} for j in jobs]
            parent_map, superseded_map = build_lineage(pseudo)
    elif index_rows is not None:
        doc_types = (set(args.doc_types.split(",")) if args.doc_types else None)
        jobs = jobs_from_index(index_rows, since=args.since, until=args.until,
                               doc_types=doc_types,
                               xbrl_only=not args.all_documents)
    else:
        ap.error("--index か --jobs-csv のどちらかが必要です")

    os.makedirs(args.out_dir, exist_ok=True)
    summary = None
    if not args.combine_only:
        summary = run_batch(jobs, parent_map, superseded_map, args.tars_dir,
                            args.state_dir, args.out_dir,
                            max_docs=args.max_docs)
    path, rows, sha, missing = combine(jobs, args.state_dir, args.out_dir)
    result = {
        "run": summary,
        "combined": {"path": path, "rows": rows, "sha256": sha,
                     "missing_shards": missing},
    }
    with open(os.path.join(args.out_dir, "run_summary.json"), "w",
              encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(json.dumps(result["combined"], ensure_ascii=False))
    if summary:
        print(f"ok={summary['processed_ok']} skipped={summary['skipped_done']} "
              f"failed={summary['failed']} stopped_early={summary['stopped_early']}")
    if args.strict and summary and summary["failed"]:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
