"""バックフィルの全数照合 (S2-T2)。

母数は固定リビジョンの v1 索引で xbrl_flag=true かつ提出日が対象期間に
入る書類。変換側は v2 データセットの silver/month=YYYY-MM/ にある
目録 (ledger.jsonl) と結合 (silver.parquet)。

照合項目:
1. 母数のうちジョブになった書類 (raw_zip_path あり) がすべて、
   その月の目録で最新状態 ok になっている (漏れがない)。
2. 目録で ok の書類がすべて母数に含まれる (余分がない)。
3. 書類ごとの行数が、目録の記録と結合 parquet の実数で一致する。
4. failed のまま残る書類は error_kind ごとに集計して列挙する
   (未説明の差分として扱い、ゼロでなければ終了コード 1)。
5. raw_zip_path が空でジョブにならなかった書類は別枠で数え、
   doc_type などの内訳を示す (索引側の事情として説明対象)。
6. S2-T6 (Issue #26) の補完取得の照合: 5 の書類はすべて、次の
   どれかに分類されなければならない (classify_missing)。
   - recovered: 取得目録が ok で、解析目録が ok で、結合 parquet の
     行数が解析目録の記録と一致する (Silver に追加済み)。
   - unobtainable: 取得目録の最新行が failed で、error_kind が
     EdinetError (EDINET 側の恒常的な取得不能) かつ error に理由が
     記録されているものだけ。一時的な失敗 (upload_unconfirmed、
     429/5xx の再試行枯渇、OSError 等) と理由の無い failed は
     終端にせず unexplained に倒す。
   - quarantined: 取得できたが解析で隔離された (解析目録の最新行が
     quarantined。Silver には入っていないため未説明として扱う)。
   - unexplained: 上のどれでもない (未説明。終了コード 1)。

v2 は公開データセットなので、取得はトークンなしの HTTPS で行う。
結果は JSON で標準出力に出す。
"""
import argparse
import json
import os
import sys
import urllib.request
from collections import Counter

import pyarrow.parquet as pq

V2_BASE = "https://huggingface.co/datasets/Yoshi-Dai/financial-lakehouse-v2/resolve/main"


def _download(url, dest):
    if os.path.exists(dest):
        return
    req = urllib.request.Request(url, headers={"User-Agent": "reconcile-backfill"})
    with urllib.request.urlopen(req) as r, open(dest, "wb") as f:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)


UNOBTAINABLE_ERROR_KIND = "EdinetError"


def _is_unobtainable(acquired):
    """取得目録の failed 行が終端の「取得不能」に当たるかを判定する。

    終端にできるのは、EDINET 側の恒常的な取得不能を示す
    EdinetError で、理由 (error) が記録されている場合だけである。
    upload_unconfirmed (EDINET からは取得できており再実行で回復
    する)、_RetryableHttpError (429/5xx の再試行枯渇)、OSError 等の
    一時的な失敗と、理由が空の failed は終端にしない。
    """
    if acquired is None or acquired.get("status") != "failed":
        return False
    if acquired.get("error_kind") != UNOBTAINABLE_ERROR_KIND:
        return False
    error = acquired.get("error")
    return isinstance(error, str) and bool(error.strip())


def classify_missing(no_zip, parse_latest, acq_latest, doc_rows):
    """未収穫だった書類を recovered / unobtainable / quarantined /
    unexplained に分類する (照合項目 6)。

    no_zip: 索引行のリスト (doc_id と submit_at を持つ)。
    parse_latest: doc_id -> 解析目録の最新行。
    acq_latest: doc_id -> 取得目録の最新行 (kind=zip のもの)。
    doc_rows: doc_id -> 結合 parquet での実行数 (無い書類は欠落)。
    """
    recovered = []
    unobtainable = []
    quarantined = []
    unexplained = []
    for r in no_zip:
        doc_id = r["doc_id"]
        month = (r["submit_at"] or "")[:7]
        parsed = parse_latest.get(doc_id)
        acquired = acq_latest.get(doc_id)
        if (parsed is not None and parsed.get("status") == "ok"
                and acquired is not None
                and acquired.get("status") == "ok"
                and doc_rows.get(doc_id) == parsed.get("rows")):
            recovered.append(doc_id)
        elif parsed is not None and parsed.get("status") == "quarantined":
            quarantined.append({"doc_id": doc_id, "month": month,
                                "reason": parsed.get("reason")})
        elif _is_unobtainable(acquired):
            unobtainable.append({"doc_id": doc_id, "month": month,
                                 "error_kind": acquired.get("error_kind"),
                                 "error": acquired.get("error")})
        else:
            unexplained.append(doc_id)
    return {"recovered": sorted(recovered),
            "unobtainable": sorted(unobtainable,
                                   key=lambda x: x["doc_id"]),
            "quarantined": sorted(quarantined,
                                  key=lambda x: x["doc_id"]),
            "unexplained": sorted(unexplained)}


def _load_latest_jsonl(path, key_of):
    """JSONL の最新行をキーごとの辞書で返す。ファイルが無ければ空。"""
    latest = {}
    if not os.path.exists(path):
        return latest
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                e = json.loads(line)
                key = key_of(e)
                if key:
                    latest[key] = e
    return latest


def main(argv=None):
    ap = argparse.ArgumentParser(description="バックフィルの全数照合")
    ap.add_argument("--index", required=True, help="固定リビジョンの v1 索引 parquet")
    ap.add_argument("--since", default="2021-11-01", help="対象の開始日 YYYY-MM-DD")
    ap.add_argument("--until", default=None,
                    help="対象の終了日 YYYY-MM-DD (省略時は上限なし)")
    ap.add_argument("--cache-dir", required=True, help="v2 の月次ファイルの置き場所")
    args = ap.parse_args(argv)

    table = pq.read_table(args.index, columns=[
        "doc_id", "submit_at", "xbrl_flag", "raw_zip_path", "doc_type"])
    rows = table.to_pylist()
    population = [r for r in rows
                  if r["xbrl_flag"] and (r["submit_at"] or "")[:10] >= args.since
                  and (args.until is None
                       or (r["submit_at"] or "")[:10] <= args.until)]
    no_zip = [r for r in population if not r["raw_zip_path"]]
    jobs = {}
    for r in population:
        if r["raw_zip_path"]:
            jobs[r["doc_id"]] = (r["submit_at"] or "")[:7]
    months = sorted(set(jobs.values()))
    print(f"population={len(population)} jobs={len(jobs)} "
          f"no_zip={len(no_zip)} months={len(months)}", file=sys.stderr)

    os.makedirs(args.cache_dir, exist_ok=True)
    ok_docs = {}       # doc_id -> (month, rows)
    failed_docs = {}   # doc_id -> (month, error_kind)
    row_mismatch = []
    missing_month_files = []
    total_rows_parquet = 0
    doc_rows = {}      # doc_id -> 結合 parquet での実行数 (全月)
    for month in months:
        prefix = f"silver/month={month}"
        ledger_path = os.path.join(args.cache_dir, f"{month}-ledger.jsonl")
        parquet_path = os.path.join(args.cache_dir, f"{month}-silver.parquet")
        try:
            _download(f"{V2_BASE}/{prefix}/ledger.jsonl", ledger_path)
            _download(f"{V2_BASE}/{prefix}/silver.parquet", parquet_path)
        except Exception as e:  # 月の成果物が無い場合は差分として記録する
            missing_month_files.append({"month": month, "error": str(e)})
            continue
        latest = {}
        with open(ledger_path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    e = json.loads(line)
                    latest[e["doc_id"]] = e
        counts = Counter(pq.read_table(parquet_path, columns=["doc_id"])
                         .column("doc_id").to_pylist())
        total_rows_parquet += sum(counts.values())
        doc_rows.update(counts)
        for doc_id, e in latest.items():
            if e["status"] == "ok":
                ok_docs[doc_id] = (month, e["rows"])
                if counts.get(doc_id) != e["rows"]:
                    row_mismatch.append({"doc_id": doc_id, "month": month,
                                         "ledger_rows": e["rows"],
                                         "parquet_rows": counts.get(doc_id)})
            else:
                failed_docs[doc_id] = (month, e.get("error_kind"))

    # 照合項目 6: 未収穫だった書類の分類 (S2-T6)。
    # 取得目録と解析目録は、対象書類の提出月についてだけ落とす。
    parse_latest = {}
    acq_latest = {}
    for month in sorted({(r["submit_at"] or "")[:7] for r in no_zip}):
        parse_path = os.path.join(args.cache_dir, f"{month}-parse.jsonl")
        acq_path = os.path.join(args.cache_dir,
                                f"{month}-acquisition.jsonl")
        for url, path in (
                (f"{V2_BASE}/parse/month={month}/ledger.jsonl", parse_path),
                (f"{V2_BASE}/acquisition/month={month}/ledger.jsonl",
                 acq_path)):
            try:
                _download(url, path)
            except Exception:
                # 目録が無い月は「記録なし」として分類側で扱う
                pass
        parse_latest.update(_load_latest_jsonl(
            parse_path, lambda e: e.get("doc_id")))
        acq_latest.update(_load_latest_jsonl(
            acq_path,
            lambda e: e.get("doc_id") if e.get("kind") == "zip" else None))
    missing_classification = classify_missing(
        no_zip, parse_latest, acq_latest, doc_rows)

    job_ids = set(jobs)
    ok_ids = set(ok_docs)
    result = {
        "population": len(population),
        "jobs": len(jobs),
        "no_zip_path": {
            "count": len(no_zip),
            "doc_type_counts": Counter(r["doc_type"] for r in no_zip),
            "months": Counter((r["submit_at"] or "")[:7] for r in no_zip),
            "doc_ids": sorted(r["doc_id"] for r in no_zip),
        },
        "ok": len(ok_docs),
        "failed_remaining": {
            "count": len(failed_docs),
            "by_kind": Counter(k for _, k in failed_docs.values()),
            "docs": sorted(
                ({"doc_id": d, "month": m, "error_kind": k}
                 for d, (m, k) in failed_docs.items()),
                key=lambda x: x["doc_id"]),
        },
        "missing_classification": {
            "recovered": missing_classification["recovered"],
            "recovered_count": len(missing_classification["recovered"]),
            "unobtainable": missing_classification["unobtainable"],
            "unobtainable_count":
                len(missing_classification["unobtainable"]),
            "quarantined": missing_classification["quarantined"],
            "quarantined_count":
                len(missing_classification["quarantined"]),
            "unexplained": missing_classification["unexplained"],
            "unexplained_count":
                len(missing_classification["unexplained"]),
        },
        "missing_from_ledger": sorted(job_ids - ok_ids - set(failed_docs)),
        "extra_in_ledger": sorted(ok_ids - job_ids),
        "row_mismatch": row_mismatch,
        "missing_month_files": missing_month_files,
        "total_rows_ledger": sum(r for _, r in ok_docs.values()),
        "total_rows_parquet": total_rows_parquet,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    unexplained = (result["missing_from_ledger"] or result["extra_in_ledger"]
                   or result["row_mismatch"] or result["missing_month_files"]
                   or result["failed_remaining"]["count"]
                   or result["missing_classification"]["unexplained"]
                   or result["missing_classification"]["quarantined"])
    return 1 if unexplained else 0


if __name__ == "__main__":
    sys.exit(main())
