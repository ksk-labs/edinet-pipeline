"""EDINET 日次パイプラインの監視と日次報告 (S2-T5 §5)。

その日の取得・解析・コードリストの状態を、v2 データセット上の成果物
(目録と実行サマリ) から点検し、日次報告と状態ファイルを v2 へ保存する。

- 日次報告 (monitor/reports/date=YYYY-MM-DD/report.json): その日の
  点検結果。取得・コードリスト・解析の状態、件数の健全性信号
  (提出者数・書類数など)、警報の一覧を持つ。同日の再実行では最新の
  内容で上書きする。
- 状態ファイル (monitor/state.json): 監視をまたぐ状態。取得の連続
  失敗日数と最終成功日、未解消の警報を持つ。取得が連続して失敗した
  場合は、日次報告と状態ファイルの両方でその旨を明示する。

点検は成果物ベースで行うため、ワークフローが起動しなかった・途中で
落ちた場合は「実行サマリが無い」として検知できる。監視自体が実行
されなかった日は報告が残らないため、前回の点検対象日 (状態ファイル
の last_checked_target) との連続性も照合し、点検の抜けた対象日が
あれば次回以降の報告で警報にする。警報があっても
監視自体は成功として終わる (終了コードは監視自体の障害でのみ 1)。
警報の届け先は状態ファイルと日次報告までが仕様 §5 の定めであり、
それ以外の通知手段は持たない。

監視の対象 (仕様 §5): 取得の失敗、コードリストのスナップショットの
欠落、検証エラーの隔離件数、処理の滞留。あわせて件数を健全性の
監視信号として日々記録する。会社マスタ (Issue #41) は、会社数と
最終提出日の行数を件数信号に載せ、適用済みスナップショットが
最新のスナップショットに追いついていない場合に警報にする。
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from huggingface_hub import HfApi

from . import parse as parse_mod

DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"
JST = ZoneInfo("Asia/Tokyo")

STATE_PATH = "monitor/state.json"


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _today_jst():
    return datetime.now(JST).date().isoformat()


def _jst_date(iso_text):
    """ISO 日時の文字列を Asia/Tokyo の日付 (YYYY-MM-DD) にする。"""
    if not iso_text:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_text).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(JST).date().isoformat()


def _load_json(api, repo, remote, path_in_repo, work_dir, name):
    """HF 上の JSON ファイルを読む。無ければ None。"""
    if path_in_repo not in remote:
        return None
    local = os.path.join(work_dir, "dl", name)
    if not parse_mod._download_if_exists(api, repo, path_in_repo, local):
        return None
    try:
        with open(local, encoding="utf-8") as f:
            return json.load(f)
    except (ValueError, OSError):
        return None


def _upload_json(api, repo, work_dir, name, data, path_in_repo, message):
    local = os.path.join(work_dir, "out", name)
    os.makedirs(os.path.dirname(local), exist_ok=True)
    with open(local, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    parse_mod._upload_file(api, repo, local, path_in_repo, message)


def _acquisition_summaries(api, repo, remote, work_dir, target_date):
    """取得対象日の実行サマリを、実行順 (ファイル名の辞書順) で返す。"""
    month = target_date[:7]
    prefix = f"acquisition/month={month}/run_summary-{target_date}-"
    paths = sorted(p for p in remote if p.startswith(prefix)
                   and p.endswith(".json"))
    summaries = []
    for i, path in enumerate(paths):
        data = _load_json(api, repo, remote, path, work_dir,
                          f"acq-summary-{i}.json")
        if data is not None:
            summaries.append(data)
    return summaries


def _judge_acquisition(summaries, prev_acq, target_date):
    """取得の状態を判定する。戻り値は (報告用の dict, 状態用の dict)。"""
    if not summaries:
        status = "missing"
        latest = None
    else:
        latest = summaries[-1]
        status = "failed" if (latest.get("failed") or 0) > 0 else "ok"
    consecutive = prev_acq.get("consecutive_failures") or 0
    last_ok = prev_acq.get("last_ok_date")
    if status == "ok":
        consecutive = 0
        last_ok = target_date
    elif prev_acq.get("last_checked_target") != target_date:
        consecutive += 1
    report = {
        "target_date": target_date,
        "status": status,
        "runs": len(summaries),
        "list_count": latest.get("list_count") if latest else None,
        "downloaded": latest.get("downloaded") if latest else None,
        "failed": latest.get("failed") if latest else None,
        "consecutive_failures": consecutive,
    }
    state = {
        "consecutive_failures": consecutive,
        "last_ok_date": last_ok,
        "last_checked_target": target_date,
        "last_status": status,
    }
    return report, state


def _unchecked_targets(prev_last_checked, target_date):
    """前回の点検対象日の翌日から今回の対象日の前日までのうち、点検
    されていない対象日の一覧を返す。前回の記録が無い場合は空。"""
    if not prev_last_checked:
        return []
    try:
        day = datetime.fromisoformat(prev_last_checked).date()
        target = datetime.fromisoformat(target_date).date()
    except ValueError:
        return []
    unchecked = []
    day += timedelta(days=1)
    while day < target:
        unchecked.append(day.isoformat())
        day += timedelta(days=1)
    return unchecked


def _judge_codelist(api, repo, remote, work_dir, ledger_rows, run_date):
    """コードリストの状態を判定する。戻り値は報告用の dict。"""
    snapshot = _load_json(
        api, repo, remote, f"codelist/date={run_date}/snapshot.json",
        work_dir, "codelist-snapshot.json")
    today_rows = [r for r in ledger_rows if r.get("run_date") == run_date]
    latest = today_rows[-1] if today_rows else None
    return {
        "status": "ok" if snapshot is not None else "missing",
        "row_count": snapshot.get("row_count") if snapshot else None,
        "ledger_status": latest.get("status") if latest else None,
        "diff_status": latest.get("diff_status") if latest else None,
        "conflict": any(r.get("status") == "conflict" for r in today_rows),
    }


def _latest_snapshot_date(remote):
    """v2 上のコードリストのスナップショットの最新日を返す。"""
    dates = []
    for path in remote:
        if path.startswith("codelist/date=") and \
                path.endswith("/snapshot.json"):
            dates.append(path.split("=", 1)[1].split("/", 1)[0])
    return max(dates) if dates else None


def _judge_company_master(summary, latest_snapshot_date):
    """会社マスタの状態を判定する。戻り値は報告用の dict。

    master/summary.json が無い場合は missing とし、警報にはしない
    (警報の対象は仕様 §5 の取得・コードリスト・解析であり、会社
    マスタの有無は本節と件数信号で可視化する)。summary があり、
    適用済みスナップショットが最新のスナップショットに追いついて
    いない場合は stale とし、日次配線の停止を警報にする。
    """
    if summary is None:
        return {"status": "missing", "companies": None,
                "last_applied_snapshot_date": None,
                "latest_snapshot_date": latest_snapshot_date,
                "last_submissions_rows": None}
    applied = summary.get("last_applied_snapshot_date")
    stale = (latest_snapshot_date is not None
             and (applied is None or applied < latest_snapshot_date))
    return {
        "status": "stale" if stale else "ok",
        "companies": summary.get("companies"),
        "last_applied_snapshot_date": applied,
        "latest_snapshot_date": latest_snapshot_date,
        "last_submissions_rows": summary.get("last_submissions_rows"),
    }


def _judge_parse(acq_ledgers, parse_ledgers, run_date, target_date):
    """解析の状態 (当日の隔離、滞留、退避) を判定する。報告用の dict。"""
    parse_latest = {}
    parsed_today = []
    for entries in parse_ledgers.values():
        for e in entries:
            if e.get("doc_id"):
                parse_latest[e["doc_id"]] = e
            if _jst_date(e.get("parsed_at")) == run_date:
                parsed_today.append(e)
    ok_today = [e for e in parsed_today if e.get("status") == "ok"]
    bad_today = [e for e in parsed_today
                 if e.get("status") in ("quarantined", "failed")]
    reasons = {}
    for e in bad_today:
        key = e.get("reason") or e.get("error_kind") or "unknown"
        reasons[key] = reasons.get(key, 0) + 1

    streaks = parse_mod.failure_streaks(parse_ledgers)
    retired = sorted(
        doc_id for doc_id, n in streaks.items()
        if n >= parse_mod.MAX_PARSE_ATTEMPTS)

    acq_latest = {}
    for entries in acq_ledgers.values():
        for e in entries:
            if e.get("kind") == "zip" and e.get("doc_id"):
                acq_latest[e["doc_id"]] = e
    backlog = []
    for doc_id, e in sorted(acq_latest.items()):
        if e.get("status") != "ok":
            continue
        done = parse_latest.get(doc_id)
        if done is not None and done.get("status") == "ok":
            continue
        fetched_date = _jst_date(e.get("fetched_at"))
        if fetched_date is not None and fetched_date <= target_date:
            backlog.append(doc_id)

    return {
        "ok_today": len(ok_today),
        "rows_today": sum(e.get("rows") or 0 for e in ok_today),
        "bad_today": len(bad_today),
        "bad_reasons": reasons,
        "backlog": backlog,
        "retired": retired,
    }


def run_monitor(run_date, work_dir, repo=DEFAULT_REPO):
    """run_date (JST) の状態を点検し、日次報告と状態ファイルを保存する。

    取得の点検対象は前日分 (日次取得の対象日)。コードリストは当日分。
    戻り値は日次報告 (dict)。
    """
    os.makedirs(work_dir, exist_ok=True)
    target_date = (datetime.fromisoformat(run_date).date()
                   - timedelta(days=1)).isoformat()
    api = HfApi()
    remote = set(parse_mod._remote_files(api, repo))
    prev_state = _load_json(api, repo, remote, STATE_PATH, work_dir,
                            "state-prev.json") or {}
    prev_acq = prev_state.get("acquisition") or {}

    acq_ledgers = parse_mod._fetch_ledgers(api, repo, remote,
                                           "acquisition", work_dir)
    parse_ledgers = parse_mod._fetch_ledgers(api, repo, remote,
                                             "parse", work_dir)
    summaries = _acquisition_summaries(api, repo, remote, work_dir,
                                       target_date)
    acq_report, acq_state = _judge_acquisition(summaries, prev_acq,
                                              target_date)

    codelist_rows = []
    local = os.path.join(work_dir, "dl", "codelist-ledger.jsonl")
    if "codelist/ledger.jsonl" in remote and parse_mod._download_if_exists(
            api, repo, "codelist/ledger.jsonl", local):
        codelist_rows = parse_mod._load_jsonl(local)
    codelist_report = _judge_codelist(api, repo, remote, work_dir,
                                      codelist_rows, run_date)

    parse_report = _judge_parse(acq_ledgers, parse_ledgers, run_date,
                                target_date)

    master_summary = _load_json(api, repo, remote, "master/summary.json",
                                 work_dir, "master-summary.json")
    master_report = _judge_company_master(master_summary,
                                          _latest_snapshot_date(remote))

    alerts = []
    unchecked = _unchecked_targets(prev_acq.get("last_checked_target"),
                                   target_date)
    if unchecked:
        alerts.append({
            "kind": "monitor_gap",
            "detail": f"監視が実行されなかったため、対象日 "
                      f"{', '.join(unchecked)} の取得は点検されて"
                      "いません (前回の点検対象日は "
                      f"{prev_acq.get('last_checked_target')} です)",
            "unchecked_targets": unchecked,
        })
    if acq_report["status"] == "missing":
        alerts.append({
            "kind": "acquisition_missing",
            "detail": f"取得対象日 {target_date} の実行サマリがありません",
            "consecutive_failures": acq_report["consecutive_failures"],
        })
    elif acq_report["status"] == "failed":
        alerts.append({
            "kind": "acquisition_failures",
            "detail": f"取得対象日 {target_date} の取得に "
                      f"{acq_report['failed']} 件の失敗があります",
            "consecutive_failures": acq_report["consecutive_failures"],
        })
    if codelist_report["status"] == "missing":
        alerts.append({
            "kind": "codelist_missing",
            "detail": f"{run_date} のコードリストのスナップショットが"
                      "ありません。欠落したスナップショットは EDINET 側に"
                      "過去分が無いため埋められません",
        })
    if codelist_report["conflict"]:
        alerts.append({
            "kind": "codelist_conflict",
            "detail": f"{run_date} のコードリストに同日・異内容の競合が"
                      "記録されています (先行分を保持)",
        })
    if codelist_report["diff_status"] == "pending":
        alerts.append({
            "kind": "codelist_diff_pending",
            "detail": f"{run_date} のコードリストの差分が未生成です"
                      " (次回の実行で補完されます)",
        })
    if parse_report["bad_today"]:
        alerts.append({
            "kind": "parse_quarantined",
            "detail": f"本日の解析で {parse_report['bad_today']} 件が"
                      "隔離・失敗しました",
            "reasons": parse_report["bad_reasons"],
        })
    if parse_report["backlog"]:
        alerts.append({
            "kind": "parse_backlog",
            "detail": f"前日以前に取得された未解析の書類が "
                      f"{len(parse_report['backlog'])} 件残っています",
            "doc_ids": parse_report["backlog"],
        })
    if parse_report["retired"]:
        alerts.append({
            "kind": "parse_retired",
            "detail": f"連続失敗が上限 ({parse_mod.MAX_PARSE_ATTEMPTS} 回) "
                      f"に達し、再試行を退避した書類が "
                      f"{len(parse_report['retired'])} 件あります",
            "doc_ids": parse_report["retired"],
        })
    if master_report["status"] == "stale":
        alerts.append({
            "kind": "company_master_stale",
            "detail": f"会社マスタの適用済みスナップショット "
                      f"({master_report['last_applied_snapshot_date']}) が"
                      f"最新のスナップショット "
                      f"({master_report['latest_snapshot_date']}) に"
                      "追いついていません",
            "last_applied_snapshot_date":
                master_report["last_applied_snapshot_date"],
            "latest_snapshot_date": master_report["latest_snapshot_date"],
        })

    report = {
        "date": run_date,
        "generated_at": _now(),
        "acquisition": acq_report,
        "codelist": codelist_report,
        "parse": {
            "ok_today": parse_report["ok_today"],
            "rows_today": parse_report["rows_today"],
            "bad_today": parse_report["bad_today"],
            "bad_reasons": parse_report["bad_reasons"],
            "backlog_count": len(parse_report["backlog"]),
            "retired_count": len(parse_report["retired"]),
        },
        "company_master": master_report,
        "signals": {
            "codelist_row_count": codelist_report["row_count"],
            "document_list_count": acq_report["list_count"],
            "parse_ok_today": parse_report["ok_today"],
            "parse_rows_today": parse_report["rows_today"],
            "master_companies": master_report["companies"],
            "master_last_submissions_rows":
                master_report["last_submissions_rows"],
        },
        "alerts": alerts,
    }
    state = {
        "updated_at": _now(),
        "last_run_date": run_date,
        "acquisition": acq_state,
        "open_alerts": [a["kind"] for a in alerts],
    }
    _upload_json(api, repo, work_dir, "report.json", report,
                 f"monitor/reports/date={run_date}/report.json",
                 f"monitor {run_date}: daily report")
    _upload_json(api, repo, work_dir, "state.json", state, STATE_PATH,
                 f"monitor {run_date}: state")
    return report


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="EDINET 日次パイプラインの監視 (S2-T5 §5)")
    sub = ap.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="その日の状態を点検して報告を保存する")
    run.add_argument("--date", default=None,
                     help="監視の実行日 YYYY-MM-DD (JST。既定は当日)")
    run.add_argument("--work-dir", required=True, help="作業ディレクトリ")
    run.add_argument("--repo", default=DEFAULT_REPO)
    args = ap.parse_args(argv)

    run_date = args.date or _today_jst()
    report = run_monitor(run_date, args.work_dir, repo=args.repo)
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
