"""EDINET コードリストの日次スナップショットと差分 (S2-T5 §4)。

金融庁のダウンロード (固定 URL の Edinetcode.zip) から EDINET コード
リストを取得し、v2 データセットへ日次スナップショットとして保存する。

- スナップショット (codelist/date=YYYY-MM-DD/EdinetcodeDlInfo.csv):
  zip から取り出した CSV をバイト列のまま不変に保存する。日付は CSV
  の1行目にある「ダウンロード実行日」を使う (リスト自身の日付であり、
  取得を実行した日ではない)。同じ日付のスナップショットが既にあれば
  上書きしない (冪等)。同じ日付で内容が異なるものが既にあれば競合と
  して記録し、先に保存されたものを正とする。
- 原形の二重化 (codelist/archive/Edinetcode-YYYY-MM-DD.zip): 取得した
  zip そのものも別のパスへ保存する (保存先の二重化)。抽出に問題が
  あっても原形から再抽出でき、スナップショットとは別のコミットで
  上がるため、一方の保存が壊れても他方が残る。
- 差分 (codelist/date=YYYY-MM-DD/diff.json と changes.jsonl): 前回
  成功したスナップショット (保存済みのうち日付が直近のもの) と
  EDINET コード単位で比較し、追加・削除・変更 (項目ごとの旧値と
  新値) を記録する。値の正規化はしない (会社マスタ側の仕事。P2-T3)。
- 目録 (codelist/ledger.jsonl): 実行ごとの結果 (ok・already_present・
  conflict・failed) を追記する。スナップショットの成果物を上げて
  から目録を上げるので、目録が ok を主張して実体が無い状態には
  ならない。取得や検証の失敗は全体を止めず、理由つきで目録へ
  記録して次回の試行で再取得する。

CSV の形式 (一次情報で確定。files/2026-11-17-primary-source-checks.md):
カンマ区切り、文字コードは Shift_JIS 系、1行目はダウンロード実行日と
件数、2行目が見出し (13項目)、3行目以降が明細。検証として、見出しの
一致、宣言された件数と明細行数の一致、EDINET コードの一意性を必須と
する。復号は cp932 で行う (公式表記は Shift_JIS だが、実ファイルには
厳密な Shift_JIS では復号できないバイトが含まれる。2026-11-17 実測)。

認証は HF への書き込みに環境変数 HF_TOKEN を使う (huggingface_hub が
自動で読む)。コードリストの取得自体に API キーは不要である。
"""
import argparse
import csv
import hashlib
import io
import json
import os
import re
import shutil
import sys
import traceback
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError

from .hf_sync import _with_retry

CODELIST_URL = ("https://disclosure2dl.edinet-fsa.go.jp"
                "/searchdocument/codelist/Edinetcode.zip")
DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"

# 見出し (CSV の2行目) の13項目。公式ガイドの項目一覧と実ファイルで確認。
EXPECTED_COLUMNS = [
    "ＥＤＩＮＥＴコード", "提出者種別", "上場区分", "連結の有無",
    "資本金", "決算日", "提出者名", "提出者名（英字）", "提出者名（ヨミ）",
    "所在地", "提出者業種", "証券コード", "提出者法人番号",
]
CODE_COLUMN = EXPECTED_COLUMNS[0]

# 出所 (成果物に持たせる。仕様 §3)。
SOURCE_KIND = "edinet_codelist_download"
SOURCE_ID = CODELIST_URL
REDISTRIBUTION = "allowed"

_DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")
_COUNT_RE = re.compile(r"(\d+)\s*件")
_SNAPSHOT_PATH_RE = re.compile(
    r"^codelist/date=(\d{4}-\d{2}-\d{2})/EdinetcodeDlInfo\.csv$")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _today_jst():
    return datetime.now(ZoneInfo("Asia/Tokyo")).date().isoformat()


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


class CodelistError(Exception):
    """コードリストの取得・検証の恒常的な失敗。"""


class _RetryableHttpError(Exception):
    """429 / 5xx の応答。再試行の対象であることを示す内部例外。

    hf_sync._retryable がステータスで判定できるよう status_code を持つ。
    """

    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.status_code = status


def _retryable_status(status):
    return status == 429 or status >= 500


def fetch_codelist_zip(url=CODELIST_URL, timeout=120):
    """固定 URL から Edinetcode.zip を取得してバイト列を返す。"""
    def _once():
        req = urllib.request.Request(
            url, headers={"User-Agent": "edinet-pipeline codelist"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                body = res.read()
                status = res.status
        except urllib.error.HTTPError as e:
            status = e.code
            if _retryable_status(status):
                raise _RetryableHttpError(status) from e
            raise CodelistError(
                f"コードリストの取得に失敗しました: HTTP {status}") from e
        if _retryable_status(status):
            raise _RetryableHttpError(status)
        if status != 200:
            raise CodelistError(
                f"コードリストの取得に失敗しました: HTTP {status}")
        return body

    body = _with_retry("codelist", _once)
    if not body.startswith(b"PK"):
        raise CodelistError("コードリストの応答が zip ではありません")
    return body


def parse_codelist_zip(zip_bytes):
    """Edinetcode.zip を検証してスナップショット (dict) を返す。

    戻り値: inner_name (zip 内の CSV 名)、csv_bytes (CSV のバイト列)、
    declared_date (ダウンロード実行日。YYYY-MM-DD)、declared_date_raw
    (1行目の日付表記そのもの)、declared_count (1行目の件数)、
    columns (見出し13項目)、rows (明細の dict の列順どおりの並び)、
    by_code (EDINET コード -> 明細 dict)。
    形式が想定と異なる場合は CodelistError にする (形式の変更を
    静かに通さないため)。
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as e:
        raise CodelistError(f"zip を開けません: {e}") from e
    csv_names = [n for n in zf.namelist()
                 if n.lower().endswith(".csv") and not n.endswith("/")]
    if len(csv_names) != 1:
        raise CodelistError(
            f"zip 内の CSV が1件ではありません: {zf.namelist()}")
    inner_name = csv_names[0]
    csv_bytes = zf.read(inner_name)
    try:
        text = csv_bytes.decode("cp932")
    except UnicodeDecodeError as e:
        raise CodelistError(f"CSV を cp932 で復号できません: {e}") from e
    records = list(csv.reader(io.StringIO(text)))
    if len(records) < 2:
        raise CodelistError("CSV の行数が不足しています")
    first_line = ",".join(records[0])
    date_match = _DATE_RE.search(first_line)
    count_match = _COUNT_RE.search(first_line)
    if not date_match or not count_match:
        raise CodelistError(
            f"CSV の1行目から実行日と件数を読めません: {first_line!r}")
    declared_date = (f"{int(date_match.group(1)):04d}-"
                     f"{int(date_match.group(2)):02d}-"
                     f"{int(date_match.group(3)):02d}")
    declared_count = int(count_match.group(1))
    columns = records[1]
    if columns != EXPECTED_COLUMNS:
        raise CodelistError(f"CSV の見出しが想定と異なります: {columns}")
    rows = []
    by_code = {}
    for lineno, rec in enumerate(records[2:], start=3):
        if not rec or all(not cell.strip() for cell in rec):
            continue
        if len(rec) != len(EXPECTED_COLUMNS):
            raise CodelistError(
                f"CSV の{lineno}行目の項目数が13ではありません: {rec}")
        row = dict(zip(EXPECTED_COLUMNS, rec))
        code = row[CODE_COLUMN]
        if not code:
            raise CodelistError(f"CSV の{lineno}行目に EDINET コードがありません")
        if code in by_code:
            raise CodelistError(f"EDINET コードが重複しています: {code}")
        rows.append(row)
        by_code[code] = row
    if len(rows) != declared_count:
        raise CodelistError(
            f"宣言された件数 {declared_count} と明細 {len(rows)} 行が一致しません")
    return {
        "inner_name": inner_name,
        "csv_bytes": csv_bytes,
        "declared_date": declared_date,
        "declared_date_raw": date_match.group(0),
        "declared_count": declared_count,
        "columns": columns,
        "rows": rows,
        "by_code": by_code,
    }


def compute_diff(old_by_code, new_by_code):
    """2つのスナップショットの差分を EDINET コード単位で計算する。

    戻り値: (added, removed, changed)。added / removed はコード -> 明細
    dict、changed はコード -> {項目名: [旧値, 新値]}。いずれもコードの
    昇順に整列した dict で返す。
    """
    added = {}
    removed = {}
    changed = {}
    for code in sorted(new_by_code):
        if code not in old_by_code:
            added[code] = new_by_code[code]
    for code in sorted(old_by_code):
        if code not in new_by_code:
            removed[code] = old_by_code[code]
    for code in sorted(new_by_code):
        if code not in old_by_code:
            continue
        fields = {}
        for column in EXPECTED_COLUMNS:
            old_value = old_by_code[code][column]
            new_value = new_by_code[code][column]
            if old_value != new_value:
                fields[column] = [old_value, new_value]
        if fields:
            changed[code] = fields
    return added, removed, changed


def changes_rows(added, removed, changed):
    """差分を changes.jsonl の行 (dict) の並びにする。コードの昇順。"""
    rows = []
    for code in sorted(set(added) | set(removed) | set(changed)):
        if code in added:
            rows.append({"edinet_code": code, "change": "added",
                         "row": added[code]})
        elif code in removed:
            rows.append({"edinet_code": code, "change": "removed",
                         "row": removed[code]})
        else:
            rows.append({"edinet_code": code, "change": "changed",
                         "fields": changed[code]})
    return rows


def snapshot_paths(date):
    """スナップショット一式の v2 データセット内のパス。"""
    base = f"codelist/date={date}"
    return {
        "csv": f"{base}/EdinetcodeDlInfo.csv",
        "meta": f"{base}/snapshot.json",
        "diff": f"{base}/diff.json",
        "changes": f"{base}/changes.jsonl",
        "archive": f"codelist/archive/Edinetcode-{date}.zip",
    }


def _remote_files(api, repo):
    return set(_with_retry(
        "list_repo_files",
        lambda: api.list_repo_files(repo, repo_type="dataset")))


def _download_if_exists(api, repo, path_in_repo, local_path):
    """HF 上のファイルを落とす。無ければ False。"""
    try:
        got = _with_retry("hf_hub_download", lambda: hf_hub_download(
            repo_id=repo, filename=path_in_repo, repo_type="dataset",
            local_dir=None, token=None))
    except EntryNotFoundError:
        return False
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    shutil.copyfile(got, local_path)
    return True


def _upload_file(api, repo, local_path, path_in_repo, message):
    return _with_retry("upload_file", lambda: api.upload_file(
        path_or_fileobj=local_path, path_in_repo=path_in_repo,
        repo_id=repo, repo_type="dataset", commit_message=message))


def _write_local(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "wb" if isinstance(data, bytes) else "w"
    kwargs = {} if isinstance(data, bytes) else {"encoding": "utf-8"}
    with open(path, mode, **kwargs) as f:
        f.write(data)


def _append_ledger(api, repo, work_dir, row):
    """目録 (codelist/ledger.jsonl) へ1行追記して上げる。"""
    local = os.path.join(work_dir, "state", "ledger.jsonl")
    _download_if_exists(api, repo, "codelist/ledger.jsonl", local)
    os.makedirs(os.path.dirname(local), exist_ok=True)
    with open(local, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    _upload_file(api, repo, local, "codelist/ledger.jsonl",
                 f"codelist {row.get('run_date')}: ledger {row.get('status')}")


def _previous_snapshot(api, repo, remote, date, work_dir):
    """差分の基準 (date より前の直近の保存済みスナップショット) を読む。

    戻り値: (基準日, by_code)。無ければ (None, {})。
    """
    dates = sorted(
        m.group(1) for path in remote
        if (m := _SNAPSHOT_PATH_RE.match(path)) and m.group(1) < date)
    if not dates:
        return None, {}
    base_date = dates[-1]
    local = os.path.join(work_dir, "state", f"previous-{base_date}.csv")
    path = snapshot_paths(base_date)["csv"]
    if not _download_if_exists(api, repo, path, local):
        raise CodelistError(f"前回のスナップショットを取得できません: {path}")
    with open(local, "rb") as f:
        csv_bytes = f.read()
    text = csv_bytes.decode("cp932")
    records = list(csv.reader(io.StringIO(text)))
    by_code = {}
    for rec in records[2:]:
        if not rec or all(not cell.strip() for cell in rec):
            continue
        row = dict(zip(EXPECTED_COLUMNS, rec))
        by_code[row[CODE_COLUMN]] = row
    return base_date, by_code


def _finish_run(api, repo, work_dir, summary, row):
    """目録へ結果を追記し、実行サマリを保存して返す (run_snapshot の締め)。

    目録や サマリの保存自体が失敗した場合は、その旨をサマリに残す
    (記録の失敗で実行結果そのものを上書きしないため)。
    """
    summary["finished_at"] = _now()
    try:
        _append_ledger(api, repo, work_dir, row)
    except Exception as e:
        summary["ledger_error"] = f"{type(e).__name__}: {e}"[:500]
    summary_path = os.path.join(work_dir, "out", "run_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    day = summary["snapshot_date"] or summary["run_date"]
    try:
        _upload_file(api, repo, summary_path,
                     f"codelist/runs/run_summary-{day}-{stamp}.json",
                     f"codelist {day}: run summary")
    except Exception as e:
        summary["summary_upload_error"] = f"{type(e).__name__}: {e}"[:500]
    return summary

def run_snapshot(work_dir, repo=DEFAULT_REPO, fetcher=None):
    """コードリストを1回取得し、スナップショットと差分を保存する。

    実行サマリ (dict) を返す。取得・検証の失敗は例外にせず、status が
    failed のサマリとして返し、目録へも記録する (HF への記録自体が
    失敗した場合はその旨をサマリに残す)。
    """
    fetcher = fetcher or fetch_codelist_zip
    state_dir = os.path.join(work_dir, "state")
    stage_dir = os.path.join(work_dir, "stage")
    out_dir = os.path.join(work_dir, "out")
    for d in (state_dir, stage_dir, out_dir):
        os.makedirs(d, exist_ok=True)
    api = HfApi()
    run_date = _today_jst()
    fetched_at = _now()
    summary = {
        "run_date": run_date, "fetched_at": fetched_at,
        "status": None, "snapshot_date": None, "row_count": 0,
        "uploaded": [],
    }

    try:
        zip_bytes = fetcher()
        snap = parse_codelist_zip(zip_bytes)
    except Exception as e:
        summary["status"] = "failed"
        summary["error_kind"] = type(e).__name__
        summary["error"] = f"{e}"[:500]
        return _finish_run(api, repo, work_dir, summary, {
            "date": None, "run_date": run_date, "status": "failed",
            "fetched_at": fetched_at,
            "error_kind": summary["error_kind"],
            "error": summary["error"],
            "source": SOURCE_KIND, "source_id": SOURCE_ID,
            "redistribution": REDISTRIBUTION,
        })

    date = snap["declared_date"]
    summary["snapshot_date"] = date
    summary["row_count"] = len(snap["rows"])
    summary["declared_count"] = snap["declared_count"]
    summary["sha256_csv"] = sha256_bytes(snap["csv_bytes"])
    summary["sha256_zip"] = sha256_bytes(zip_bytes)
    paths = snapshot_paths(date)
    try:
        return _save_snapshot(api, repo, work_dir, snap, zip_bytes,
                              summary, paths, fetched_at, run_date)
    except Exception as e:
        summary["status"] = "failed"
        summary["error_kind"] = type(e).__name__
        summary["error"] = f"{e}"[:500]
        return _finish_run(api, repo, work_dir, summary, {
            "date": date, "run_date": run_date, "status": "failed",
            "fetched_at": fetched_at,
            "error_kind": summary["error_kind"],
            "error": summary["error"],
            "source": SOURCE_KIND, "source_id": SOURCE_ID,
            "redistribution": REDISTRIBUTION,
        })


def _save_snapshot(api, repo, work_dir, snap, zip_bytes, summary, paths,
                   fetched_at, run_date):
    """検証済みスナップショットの保存と差分の生成 (run_snapshot の後段)。"""
    date = snap["declared_date"]
    stage_dir = os.path.join(work_dir, "stage")
    state_dir = os.path.join(work_dir, "state")
    remote = _remote_files(api, repo)

    if paths["csv"] in remote:
        local_existing = os.path.join(state_dir, f"existing-{date}.csv")
        _download_if_exists(api, repo, paths["csv"], local_existing)
        with open(local_existing, "rb") as f:
            existing_sha = sha256_bytes(f.read())
        if existing_sha != summary["sha256_csv"]:
            summary["status"] = "conflict"
            summary["existing_sha256_csv"] = existing_sha
            return _finish_run(api, repo, work_dir, summary, {
                "date": date, "run_date": run_date, "status": "conflict",
                "fetched_at": fetched_at,
                "sha256_csv": summary["sha256_csv"],
                "existing_sha256_csv": existing_sha,
                "row_count": summary["row_count"],
                "source": SOURCE_KIND, "source_id": SOURCE_ID,
                "redistribution": REDISTRIBUTION,
            })
        status = "already_present"
    else:
        status = "ok"

    base_date, base_by_code = _previous_snapshot(
        api, repo, remote, date, work_dir)
    added, removed, changed = compute_diff(base_by_code, snap["by_code"])
    summary["diff_base_date"] = base_date
    summary["added"] = len(added)
    summary["removed"] = len(removed)
    summary["changed"] = len(changed)

    meta = {
        "date": date,
        "declared_download_date": snap["declared_date_raw"],
        "declared_count": snap["declared_count"],
        "row_count": len(snap["rows"]),
        "columns": snap["columns"],
        "inner_file": snap["inner_name"],
        "sha256_csv": summary["sha256_csv"],
        "sha256_zip": summary["sha256_zip"],
        "source": SOURCE_KIND, "source_id": SOURCE_ID,
        "fetched_at": fetched_at, "redistribution": REDISTRIBUTION,
    }
    diff = {
        "date": date, "base_date": base_date, "generated_at": _now(),
        "previous_count": len(base_by_code) if base_date else None,
        "current_count": len(snap["rows"]),
        "added": len(added), "removed": len(removed),
        "changed": len(changed),
        "unchanged": len(snap["rows"]) - len(added) - len(changed),
        "source": SOURCE_KIND, "source_id": SOURCE_ID,
        "fetched_at": fetched_at, "redistribution": REDISTRIBUTION,
    }
    changes_text = "".join(
        json.dumps(row, ensure_ascii=False) + "\n"
        for row in changes_rows(added, removed, changed))
    products = [
        (paths["archive"], zip_bytes,
         f"codelist {date}: original zip (archive)"),
        (paths["csv"], snap["csv_bytes"],
         f"codelist {date}: snapshot"),
        (paths["meta"], json.dumps(meta, ensure_ascii=False, indent=2),
         f"codelist {date}: snapshot meta"),
        (paths["diff"], json.dumps(diff, ensure_ascii=False, indent=2),
         f"codelist {date}: diff summary"),
        (paths["changes"], changes_text,
         f"codelist {date}: diff changes"),
    ]
    completed = []
    for repo_path, data, message in products:
        if repo_path in remote:
            continue
        local = os.path.join(stage_dir, repo_path)
        _write_local(local, data)
        _upload_file(api, repo, local, repo_path, message)
        summary["uploaded"].append(repo_path)
        completed.append(repo_path)
    if status == "already_present" and completed:
        summary["completed_missing"] = completed
    summary["status"] = status
    return _finish_run(api, repo, work_dir, summary, {
        "date": date, "run_date": run_date, "status": status,
        "fetched_at": fetched_at,
        "sha256_csv": summary["sha256_csv"],
        "row_count": summary["row_count"],
        "diff_base_date": base_date,
        "added": len(added), "removed": len(removed),
        "changed": len(changed),
        "source": SOURCE_KIND, "source_id": SOURCE_ID,
        "redistribution": REDISTRIBUTION,
    })


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="EDINET コードリストの日次スナップショット (S2-T5 §4)")
    sub = ap.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="コードリストを1回取得して保存する")
    run.add_argument("--work-dir", required=True, help="作業ディレクトリ")
    run.add_argument("--repo", default=DEFAULT_REPO)
    args = ap.parse_args(argv)

    try:
        summary = run_snapshot(args.work_dir, repo=args.repo)
    except Exception:
        traceback.print_exc()
        return 1
    print(json.dumps(summary, ensure_ascii=False))
    return 1 if summary["status"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
