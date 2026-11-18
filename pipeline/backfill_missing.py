"""v1 未収穫書類の補完取得 (S2-T6、Issue #26)。

第1期バックフィル (S2-T2) の全数照合で、xbrl_flag=true なのに v1 に
実ファイル (raw_zip_path) が無く未収穫だった書類が 250 件あることが
確定した (母数 305,662 件。固定リビジョンの v1 索引で再現できる)。
本モジュールはその書類を EDINET API から補完取得し、日次経路
(pipeline.discovery の取得・pipeline.parse の解析) と同じ目録の
仕組みに載せる。

- 対象の確定: 固定リビジョンの v1 索引から、xbrl_flag=true かつ
  raw_zip_path が空で、提出日が対象期間に入る書類を選ぶ (母数の
  定義は verification/reconcile_backfill.py と同じ)。
- 書類目録 (catalog/month=YYYY-MM/ledger.jsonl): 提出日の書類一覧を
  EDINET API で取得し、対象書類の行を観測として追記する。一覧の
  取得に失敗した日は v1 索引の行から組み立てた行で代用し、
  catalog_source=v1_index_fallback と明記する (索引自体が EDINET
  一覧の蓄積であるため、提出日・種別・親書類の確定には足りる)。
  出所フィールドの読み方: source (catalog_row が付ける
  SOURCE_KIND=edinet_api) は源泉種別であり、行ごとの観測経路は
  表さない。行が一覧 API の直接観測か索引由来の代用かは
  catalog_source が正であり (edinet_list_api=直接観測、
  v1_index_fallback=代用)、出所の判定は catalog_source で行う
  こと。代用行も catalog_row を通るため source と fetched_at は
  観測行と同型になるが、それをもって直接観測とみなしては
  ならない。
- 原形 (raw/edinet/YYYY/MM/DD/zip/<docID>.zip): 書類取得 API の
  type=1 を取得し、discovery と同じ保存先・同じ形式検証で保存する。
  取得は zip のみ (Silver への追加が目的であり、PDF 等は v1 側に
  既にある。discovery の ARTIFACTS のうち zip だけを対象にする)。
- 取得目録 (acquisition/month=YYYY-MM/ledger.jsonl): discovery と
  同じ形で、ok はアップロード後に遠隔で実在を確認できた分だけ書く
  (冪等: ok で実在する原形は再取得しない)。取得できない書類
  (取下げ・不開示・閲覧期間外など) は failed として理由つきで記録し、
  黙って落とさない。失敗した書類は次回の実行で再試行される。
- 解析は本モジュールの仕事ではない。取得が済んだ書類は、日次解析
  (pipeline.parse plan --doc-ids ...) が取得目録と書類目録から
  そのまま拾える (ワークフロー backfill_missing.yml が連結する)。

リクエスト間隔と再試行は discovery.EdinetClient の作法 (1.1 秒、
429 は指数バックオフ) をそのまま使う。認証は環境変数
EDINET_API_KEY と HF_TOKEN を使う (discovery と同じ)。
"""
import argparse
import json
import os
import shutil
import sys
import traceback
from datetime import datetime, timezone

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError

from .batch import load_index_rows
from .discovery import (EdinetClient, EdinetError, JsonlLedger,
                        _acq_key, catalog_row, raw_path, sha256_bytes)
from .hf_sync import _with_retry

DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"
SOURCE_KIND = "edinet_api"
REDISTRIBUTION = "allowed"


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def select_targets(index_rows, since="2021-11-01"):
    """v1 索引から補完取得の対象書類を選ぶ。

    母数の定義 (xbrl_flag=true、提出日が対象期間内) のうち、
    raw_zip_path が空でバックフィルのジョブにならなかった書類。
    戻り値は提出日時・doc_id の順に並んだ索引行のリスト。
    """
    targets = []
    for r in index_rows:
        if not r.get("xbrl_flag"):
            continue
        if r.get("raw_zip_path"):
            continue
        if (r.get("submit_at") or "")[:10] < since:
            continue
        targets.append(r)
    targets.sort(key=lambda r: (r.get("submit_at") or "", r["doc_id"]))
    return targets


def _flag(value):
    return "1" if value else "0"


def catalog_doc_from_index(row):
    """v1 索引の行を、書類目録に載せる API 形式の dict にする。

    一覧 API で観測できなかった日の代用であり、parse が使う項目
    (docID・submitDateTime・docTypeCode・parentDocID) を索引から
    確定させる。submit_at は「YYYY-MM-DD HH:MM」形式のため、
    秒を補って API の形式 (HH:MM:SS) にそろえる。
    """
    submit_at = row.get("submit_at") or ""
    if len(submit_at) == 16:
        submit_at += ":00"
    return {
        "docID": row["doc_id"],
        "submitDateTime": submit_at,
        "docTypeCode": row.get("doc_type"),
        "parentDocID": row.get("parent_doc_id"),
        "edinetCode": row.get("edinet_code"),
        "secCode": row.get("code"),
        "filerName": row.get("company_name"),
        "xbrlFlag": _flag(row.get("xbrl_flag")),
        "pdfFlag": _flag(row.get("pdf_flag")),
        "csvFlag": _flag(row.get("csv_flag")),
        "attachDocFlag": _flag(row.get("attachment_flag")),
        "englishDocFlag": _flag(row.get("english_flag")),
    }


def _download_if_exists(api, repo, path_in_repo, local_path):
    """HF 上のファイルを落とす。無ければ False (parse と同じ型)。"""
    try:
        got = _with_retry("hf_hub_download", lambda: hf_hub_download(
            repo_id=repo, filename=path_in_repo, repo_type="dataset",
            local_dir=None, token=None))
    except EntryNotFoundError:
        return False
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    shutil.copyfile(got, local_path)
    return True


def _remote_files(api, repo):
    return set(_with_retry(
        "list_repo_files",
        lambda: api.list_repo_files(repo, repo_type="dataset")))


def _upload_file(api, repo, local_path, path_in_repo, message):
    return _with_retry("upload_file", lambda: api.upload_file(
        path_or_fileobj=local_path, path_in_repo=path_in_repo,
        repo_id=repo, repo_type="dataset", commit_message=message))


def _upload_folder(api, repo, local_dir, path_in_repo, message):
    return _with_retry("upload_folder", lambda: api.upload_folder(
        folder_path=local_dir, repo_id=repo, repo_type="dataset",
        path_in_repo=path_in_repo, commit_message=message))


def run_acquire(index_path, work_dir, repo=DEFAULT_REPO, client=None,
                since="2021-11-01", max_docs=None, doc_ids=None):
    """対象書類の補完取得を行い、実行サマリ (dict) を返す。

    work_dir の配下に state/ (月別の目録)、stage/ (アップロード前の
    原形)、out/ (実行サマリと取得済み ID 一覧) を作る。月ごとに
    目録を HF から復元して追記し、原形のアップロードが保存先
    ディレクトリ単位で済むたびに取得目録を push する (discovery
    と同じ規律: 目録が ok を主張して実体が無い状態にはならない)。
    """
    index_rows = load_index_rows(index_path)
    targets = select_targets(index_rows, since=since)
    if doc_ids:
        wanted = {d.strip() for d in doc_ids.split(",") if d.strip()}
        targets = [r for r in targets if r["doc_id"] in wanted]
    os.makedirs(work_dir, exist_ok=True)
    state_dir = os.path.join(work_dir, "state")
    stage_dir = os.path.join(work_dir, "stage")
    out_dir = os.path.join(work_dir, "out")
    for d in (state_dir, stage_dir, out_dir):
        os.makedirs(d, exist_ok=True)
    api = HfApi()
    summary = {
        "started_at": _now(), "since": since,
        "targets": len(targets), "attempted": 0, "downloaded": 0,
        "skipped_done": 0, "failed": 0, "failures": [],
        "list_failures": [], "stopped_early": False,
        "acquired_doc_ids": [], "failed_doc_ids": [],
    }
    remote = _remote_files(api, repo)

    # 提出日ごとに一覧を取得し、書類目録の行の源泉にする。
    # 失敗した日は索引由来の行で代用する (理由はサマリに残す)。
    by_date = {}
    for row in targets:
        by_date.setdefault((row.get("submit_at") or "")[:10], []).append(row)
    list_docs = {}
    for date in sorted(by_date):
        try:
            data = client.fetch_list(date)
        except Exception as e:  # 一覧が取れない日は索引で代用して進める
            summary["list_failures"].append(
                {"date": date, "error_kind": type(e).__name__,
                 "error": client._sanitize(f"{e}")[:500]})
            continue
        for doc in data.get("results") or []:
            if doc.get("docID"):
                list_docs[doc["docID"]] = doc

    # 月ごとに処理する (目録が月別パーティションのため)。
    by_month = {}
    for row in targets:
        by_month.setdefault((row.get("submit_at") or "")[:7], []).append(row)
    for month in sorted(by_month):
        rows = by_month[month]
        cat_local = os.path.join(state_dir, f"catalog-{month}.jsonl")
        acq_local = os.path.join(state_dir, f"acquisition-{month}.jsonl")
        _download_if_exists(api, repo, f"catalog/month={month}/ledger.jsonl",
                            cat_local)
        _download_if_exists(api, repo,
                            f"acquisition/month={month}/ledger.jsonl",
                            acq_local)
        catalog = JsonlLedger(cat_local, lambda e: e.get("docID"))
        acq = JsonlLedger(acq_local, _acq_key)

        def push_acquisition_ledger():
            _upload_file(api, repo, acq_local,
                         f"acquisition/month={month}/ledger.jsonl",
                         f"backfill-missing {month}: acquisition ledger")

        # 書類目録への追記 (一覧で観測できた行は観測の記録、
        # できなかった行は索引由来の代用行。原形の取得成否に
        # 依存しない)。区別は catalog_source が担う。
        fetched_at = _now()
        for row in rows:
            doc_id = row["doc_id"]
            listed = list_docs.get(doc_id)
            if listed is not None:
                entry = catalog_row(listed, fetched_at)
                entry["catalog_source"] = "edinet_list_api"
            else:
                entry = catalog_row(catalog_doc_from_index(row), fetched_at)
                entry["catalog_source"] = "v1_index_fallback"
            catalog.append(entry)
        _upload_file(api, repo, cat_local,
                     f"catalog/month={month}/ledger.jsonl",
                     f"backfill-missing {month}: catalog ledger")

        # 原形の取得 (書類単位で失敗を隔離する)。
        staged_dirs = {}
        ok_entries = []
        failed_entries = []
        for row in rows:
            doc_id = row["doc_id"]
            doc = dict(catalog_doc_from_index(row), docID=doc_id)
            path = raw_path(doc, "zip")
            entry = acq.latest.get(f"{doc_id}:zip")
            if (entry is not None and entry.get("status") == "ok"
                    and path in remote):
                summary["skipped_done"] += 1
                summary["acquired_doc_ids"].append(doc_id)
                continue
            if max_docs is not None and summary["attempted"] >= max_docs:
                summary["stopped_early"] = True
                break
            summary["attempted"] += 1
            try:
                body = client.fetch_document(doc_id, 1)
            except Exception as e:  # 1書類の失敗で全体を止めない
                failed_entries.append({
                    "doc_id": doc_id, "kind": "zip", "status": "failed",
                    "error_kind": type(e).__name__,
                    "error": client._sanitize(f"{e}")[:500],
                    "path": path, "updated_at": _now(),
                })
                summary["failed"] += 1
                summary["failed_doc_ids"].append(doc_id)
                summary["failures"].append(
                    {"doc_id": doc_id, "error_kind": type(e).__name__})
                continue
            local = os.path.join(stage_dir, path)
            os.makedirs(os.path.dirname(local), exist_ok=True)
            with open(local, "wb") as f:
                f.write(body)
            staged_dirs[os.path.dirname(path)] = os.path.dirname(local)
            ok_entries.append({
                "doc_id": doc_id, "kind": "zip", "status": "ok",
                "sha256": sha256_bytes(body), "size": len(body),
                "path": path,
                "source": SOURCE_KIND, "source_id": doc_id,
                "fetched_at": _now(), "redistribution": REDISTRIBUTION,
                "updated_at": _now(),
            })
            summary["downloaded"] += 1
            summary["acquired_doc_ids"].append(doc_id)

        # 原形のアップロード (保存先ディレクトリごと)。discovery と
        # 同じく、遠隔で実在を確認できた分だけ ok を目録に書く。
        pending_ok = list(ok_entries)
        for rel_dir, local_dir in sorted(staged_dirs.items()):
            _upload_folder(api, repo, local_dir, rel_dir,
                           "backfill-missing: raw zip")
            remote_now = _remote_files(api, repo)
            confirmed = [e for e in pending_ok
                         if os.path.dirname(e["path"]) == rel_dir
                         and e["path"] in remote_now]
            for entry in confirmed:
                acq.append(entry)
                pending_ok.remove(entry)
            if confirmed:
                push_acquisition_ledger()
        if pending_ok:
            remote_after = _remote_files(api, repo)
            for entry in pending_ok:
                if entry["path"] in remote_after:
                    acq.append(entry)
                    continue
                acq.append({
                    "doc_id": entry["doc_id"], "kind": "zip",
                    "status": "failed", "error_kind": "upload_unconfirmed",
                    "error": "アップロード後に遠隔で実在を確認できませんでした",
                    "path": entry["path"], "updated_at": _now(),
                })
                summary["failed"] += 1
                summary["failures"].append(
                    {"doc_id": entry["doc_id"],
                     "error_kind": "upload_unconfirmed"})
                summary["acquired_doc_ids"].remove(entry["doc_id"])
                summary["failed_doc_ids"].append(entry["doc_id"])
        for entry in failed_entries:
            acq.append(entry)
        # 取得目録への追記が0件で遠隔目録も無い月は、ローカルの
        # state ファイル自体が存在しない。上げるものが無いので
        # push を行わない（max_docs で上限に達し次の月に到達した
        # 場合に当たる。JsonlLedger は初回追記時にだけファイルを作る）。
        if os.path.exists(acq_local):
            push_acquisition_ledger()
        if summary["stopped_early"]:
            break

    summary["finished_at"] = _now()
    summary["requests"] = client.request_count
    summary["acquired_doc_ids"].sort()
    summary["failed_doc_ids"].sort()
    with open(os.path.join(out_dir, "run_summary.json"), "w",
              encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "acquired_doc_ids.txt"), "w",
              encoding="utf-8") as f:
        f.write(",".join(summary["acquired_doc_ids"]))
    return summary


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="v1 未収穫書類の補完取得 (S2-T6)")
    sub = ap.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="対象書類の補完取得を行う")
    run.add_argument("--index", required=True,
                     help="固定リビジョンの v1 索引 parquet")
    run.add_argument("--work-dir", required=True, help="作業ディレクトリ")
    run.add_argument("--repo", default=DEFAULT_REPO)
    run.add_argument("--since", default="2021-11-01",
                     help="対象の開始日 YYYY-MM-DD (母数の定義)")
    run.add_argument("--max-docs", type=int, default=None,
                     help="今回の実行で試行する書類数の上限 (動作確認用)")
    run.add_argument("--doc-ids", default=None,
                     help="対象の書類 ID (カンマ区切り。対象の絞り込み用)")
    run.add_argument("--interval", type=float, default=1.1,
                     help="EDINET API のリクエスト間隔 (秒)")
    args = ap.parse_args(argv)

    if not os.environ.get("EDINET_API_KEY"):
        print(json.dumps({"status": "failed",
                          "error": "EDINET_API_KEY が設定されていません"},
                         ensure_ascii=False), file=sys.stderr)
        return 1
    client = EdinetClient(os.environ["EDINET_API_KEY"],
                          interval=args.interval)
    try:
        summary = run_acquire(args.index, args.work_dir, repo=args.repo,
                              client=client, since=args.since,
                              max_docs=args.max_docs, doc_ids=args.doc_ids)
    except EdinetError as e:
        print(json.dumps({"status": "failed",
                          "error": client._sanitize(f"{e}")},
                         ensure_ascii=False), file=sys.stderr)
        return 1
    except Exception:
        traceback.print_exc()
        return 1
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
