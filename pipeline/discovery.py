"""EDINET からの日次取得 (S2-T5 §1: 取得・原形保存・書類目録)。

EDINET API で対象日の書類一覧を取得し、次を v2 データセットへ保存する。

- 書類目録 (catalog/month=YYYY-MM/ledger.jsonl): 一覧の各行を観測として
  追記する。同じ書類を別の実行で観測すれば行が増え、取下げ等で EDINET
  側の項目が null に変わる変化も履歴として残る。各行は API の応答の
  全項目に、出所 (源泉の種別・源泉側の識別子・取得日時・再配布可否) を
  付けたものである。
- 一覧の応答そのもの (catalog/responses/date=YYYY-MM-DD/fetched_at=
  <取得日時>.json): API が返した JSON をそのまま保存する (追記のみ。
  同じ取得日時のファイルが既にあれば上書きしない)。
- 書類の原形 (raw/edinet/YYYY/MM/DD/{zip,pdf,attach,english}/<docID>):
  zip (書類取得 API の type=1)・PDF (type=2)・添付文書 (type=3)・
  英語開示ファイル (type=4) を原形のまま保存する。CSV (type=5) は
  取得しない (XBRL からの機械生成の複製のため。2026-11-16 の決定)。
  取下げられた書類は EDINET から二度と取得できないため、この保存が
  開示当時の姿の保持を兼ねる。

取得状態は取得目録 (acquisition/month=YYYY-MM/ledger.jsonl) で管理し、
「目録が ok で、保存先が HF 上に実在する」原形は再取得しない (冪等)。
原形のアップロードが済んでから目録を push するので、目録が ok を
主張して実体が無い状態にはならない。1書類の失敗で全体を止めず、
失敗は目録へ理由つきで記録して次回の実行で再試行する。

認証は環境変数 EDINET_API_KEY (EDINET) と HF_TOKEN (HF への書き込み、
huggingface_hub が自動で読む) を使う。API キーはログと目録に出さない
(エラー文字列からは除去する)。
"""
import argparse
import hashlib
import json
import os
import shutil
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError

from .hf_sync import _with_retry

API_BASE = "https://api.edinet-fsa.go.jp/api/v2"
DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"

# 書類取得 API の type と保存先の対応 (種別名, type, 対応するフラグ, 拡張子)。
# type=5 (CSV) は XBRL からの機械生成の複製のため取得しない。
ARTIFACTS = (
    ("zip", 1, "xbrlFlag", ".zip"),
    ("pdf", 2, "pdfFlag", ".pdf"),
    ("attach", 3, "attachDocFlag", ".zip"),
    ("english", 4, "englishDocFlag", ".zip"),
)
_KIND_BY_NAME = {a[0]: a for a in ARTIFACTS}

# 書類取得 API の type ごとの応答の先頭バイト (保存前の形式検証用)。
_FORMAT_HEADS = {1: b"PK", 2: b"%PDF", 3: b"PK", 4: b"PK"}

# 出所 (成果物に持たせる。仕様 §3)。
SOURCE_KIND = "edinet_api"
REDISTRIBUTION = "allowed"


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def _retryable_status(status):
    return status == 429 or status >= 500


def _body_status_code(body):
    """HTTP 200 で返る JSON エラー応答の StatusCode を返す。

    書類取得 API のエラーは HTTP 200 のまま JSON ボディで返る
    (EDINET API 仕様書 (Version 2) 3-3)。成功応答 (バイナリ、または
    トップレベルに StatusCode を持たない一覧の JSON) では None を返す。
    """
    head = body[:512].lstrip()
    if not head.startswith(b"{") or b"StatusCode" not in head:
        return None
    try:
        data = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict) or "StatusCode" not in data:
        return None
    try:
        return int(data["StatusCode"])
    except (TypeError, ValueError):
        return None


class _RetryableHttpError(Exception):
    """429 / 5xx の応答。再試行の対象であることを示す内部例外。

    hf_sync._retryable がステータスで判定できるよう status_code を持つ。
    """

    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.status_code = status


class EdinetError(Exception):
    """EDINET API の恒常的な失敗 (4xx、JSON のエラー応答など)。"""


class EdinetClient:
    """EDINET API のクライアント。リクエスト間隔と再試行を持つ。

    interval は連続リクエストの最小間隔 (秒)。v1 (ARIA-3) の運用実績
    (1秒に1回の制限に対し 1.1 秒間隔) を既定とする。
    """

    def __init__(self, api_key, interval=1.1, timeout=120):
        if not api_key:
            raise EdinetError("EDINET_API_KEY が設定されていません")
        self._api_key = api_key
        self.interval = interval
        self.timeout = timeout
        self._last_at = 0.0
        self.request_count = 0

    def _sanitize(self, text):
        return str(text).replace(self._api_key, "***")

    def _throttle(self):
        wait = self._last_at + self.interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)

    def _get_once(self, url, params):
        self._throttle()
        req = urllib.request.Request(
            url + "?" + urllib.parse.urlencode(params),
            headers={"User-Agent": "edinet-pipeline discovery"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as res:
                body = res.read()
                status = res.status
                ctype = res.headers.get("Content-Type", "")
        except urllib.error.HTTPError as e:
            body = e.read()
            status = e.code
            ctype = e.headers.get("Content-Type", "") if e.headers else ""
        finally:
            self._last_at = time.monotonic()
            self.request_count += 1
        if _retryable_status(status):
            raise _RetryableHttpError(status)
        if status == 200:
            code = _body_status_code(body)
            if code is not None and code != 200:
                if _retryable_status(code):
                    raise _RetryableHttpError(code)
                raise EdinetError(
                    "EDINET API のエラー応答です: "
                    f"StatusCode {code}: "
                    + self._sanitize(body[:200]))
        return status, ctype, body

    def _get(self, url, params):
        return _with_retry("edinet", lambda: self._get_once(url, params))

    def fetch_list(self, date):
        """対象日の書類一覧 (type=2) を取得し、応答 JSON をそのまま返す。"""
        status, _, body = self._get(
            f"{API_BASE}/documents.json",
            {"date": date, "type": 2, "Subscription-Key": self._api_key})
        if status != 200:
            raise EdinetError(
                f"書類一覧の取得に失敗しました: HTTP {status}: "
                + self._sanitize(body[:200]))
        try:
            data = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            raise EdinetError(f"書類一覧の応答が JSON ではありません: {e}")
        meta = data.get("metadata") or {}
        if str(meta.get("status", "200")) != "200":
            raise EdinetError(
                "書類一覧の応答がエラーです: "
                + self._sanitize(meta.get("message", meta)))
        return data

    def fetch_document(self, doc_id, type_):
        """書類の原形 (type=1..4) を取得してバイト列を返す。

        エラー応答 (HTTP 200 + JSON ボディの StatusCode) は _get_once で
        判定する。ここでは Content-Type が JSON の応答と、種別の形式
        (先頭バイト) に合わない応答を EdinetError にする。
        """
        status, ctype, body = self._get(
            f"{API_BASE}/documents/{doc_id}",
            {"type": type_, "Subscription-Key": self._api_key})
        if status != 200:
            raise EdinetError(
                f"書類の取得に失敗しました: HTTP {status}: "
                + self._sanitize(body[:200]))
        if "application/json" in ctype:
            raise EdinetError(
                "書類の取得が JSON のエラー応答でした: "
                + self._sanitize(body[:200]))
        head = _FORMAT_HEADS.get(type_)
        if head is not None and not body.startswith(head):
            raise EdinetError(
                f"書類の取得応答が想定した形式ではありません (type={type_}): "
                + self._sanitize(body[:200]))
        return body


class JsonlLedger:
    """追記型の JSONL 目録。キーの最新行が状態 (pipeline.batch と同じ型)。

    壊れた行 (中断で最終行が切れた場合など) は読み込み時に無視する。
    """

    def __init__(self, path, key_of):
        self.path = path
        self.key_of = key_of
        self.latest = {}
        self.corrupt_lines = 0
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except ValueError:
                        self.corrupt_lines += 1
                        continue
                    key = key_of(entry)
                    if key:
                        self.latest[key] = entry

    def append(self, entry):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        key = self.key_of(entry)
        if key:
            self.latest[key] = entry


def _acq_key(entry):
    doc_id = entry.get("doc_id")
    kind = entry.get("kind")
    return f"{doc_id}:{kind}" if doc_id and kind else None


def _month_of(date):
    return date[:7]


def raw_path(doc, kind):
    """原形の保存先パス。提出日の年月日で階層化する (v1 の raw と同じ型)。"""
    _, _, _, ext = _KIND_BY_NAME[kind]
    day = (doc.get("submitDateTime") or "")[:10]
    if len(day) != 10:
        day = doc["_target_date"]
    y, m, d = day[:4], day[5:7], day[8:10]
    return f"raw/edinet/{y}/{m}/{d}/{kind}/{doc['docID']}{ext}"


def catalog_row(doc, fetched_at):
    """書類目録の1行。API の応答の全項目 + 出所。"""
    row = dict(doc)
    row["fetched_at"] = fetched_at
    row["source"] = SOURCE_KIND
    row["source_id"] = doc.get("docID")
    row["redistribution"] = REDISTRIBUTION
    return row


def _download_if_exists(api, repo, path_in_repo, local_path):
    """HF 上のファイルを落とす。無ければ False。"""
    try:
        got = hf_hub_download(repo_id=repo, filename=path_in_repo,
                              repo_type="dataset", local_dir=None,
                              token=None)
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


def run_acquisition(date, work_dir, repo=DEFAULT_REPO, client=None,
                    max_docs=None, kinds=None):
    """対象日1日分の取得を行い、実行サマリ (dict) を返す。

    work_dir の配下に state/ (目録)、stage/ (アップロード前の原形)、
    out/ (応答 JSON と実行サマリ) を作る。目録は HF から復元して追記し、
    原形のアップロードが済んでから push する。
    """
    month = _month_of(date)
    state_dir = os.path.join(work_dir, "state")
    stage_dir = os.path.join(work_dir, "stage")
    out_dir = os.path.join(work_dir, "out")
    for d in (state_dir, stage_dir, out_dir):
        os.makedirs(d, exist_ok=True)
    api = HfApi()
    summary = {
        "date": date, "started_at": _now(),
        "list_count": 0, "catalog_rows_appended": 0,
        "attempted_docs": 0, "downloaded": 0, "skipped_done": 0,
        "no_artifacts": 0, "failed": 0, "failures": [],
        "stopped_early": False,
    }

    # 目録の復元 (無ければ新規)
    cat_path_local = os.path.join(state_dir, "catalog-ledger.jsonl")
    acq_path_local = os.path.join(state_dir, "acquisition-ledger.jsonl")
    _download_if_exists(api, repo, f"catalog/month={month}/ledger.jsonl",
                        cat_path_local)
    _download_if_exists(api, repo, f"acquisition/month={month}/ledger.jsonl",
                        acq_path_local)
    catalog = JsonlLedger(cat_path_local, lambda e: e.get("docID"))
    acq = JsonlLedger(acq_path_local, _acq_key)
    remote = _remote_files(api, repo)

    # 一覧の取得と応答そのものの保存
    fetched_at = _now()
    data = client.fetch_list(date)
    results = data.get("results") or []
    summary["list_count"] = len(results)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    response_repo_path = (
        f"catalog/responses/date={date}/fetched_at={stamp}.json")
    response_local = os.path.join(out_dir, "list_response.json")
    with open(response_local, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    if response_repo_path not in remote:
        _upload_file(api, repo, response_local, response_repo_path,
                     f"discovery {date}: document list response")

    # 書類目録への追記 (一覧の全行が対象。原形の取得上限とは独立)
    for doc in results:
        catalog.append(catalog_row(doc, fetched_at))
        summary["catalog_rows_appended"] += 1

    # 原形の取得 (書類単位で失敗を隔離する)
    wanted = [k for k in _KIND_BY_NAME if kinds is None or k in kinds]
    staged_dirs = {}  # 保存先ディレクトリ (repo 側) -> ローカルのディレクトリ
    ok_entries = []
    failed_entries = []
    for doc in results:
        doc_id = doc.get("docID")
        if not doc_id:
            continue
        doc = dict(doc, _target_date=date)
        targets = []
        for kind in wanted:
            _, type_, flag, _ = _KIND_BY_NAME[kind]
            if str(doc.get(flag)) != "1":
                continue
            path = raw_path(doc, kind)
            entry = acq.latest.get(f"{doc_id}:{kind}")
            if (entry is not None and entry.get("status") == "ok"
                    and path in remote):
                summary["skipped_done"] += 1
                continue
            targets.append((kind, type_, path))
        if not targets:
            if not any(str(doc.get(_KIND_BY_NAME[k][2])) == "1"
                       for k in wanted):
                summary["no_artifacts"] += 1
            continue
        if max_docs is not None and summary["attempted_docs"] >= max_docs:
            summary["stopped_early"] = True
            break
        summary["attempted_docs"] += 1
        for kind, type_, path in targets:
            try:
                body = client.fetch_document(doc_id, type_)
            except Exception as e:  # 1書類の失敗で全体を止めない
                failed_entries.append({
                    "doc_id": doc_id, "kind": kind, "status": "failed",
                    "error_kind": type(e).__name__,
                    "error": client._sanitize(f"{e}")[:500],
                    "path": path, "updated_at": _now(),
                })
                summary["failed"] += 1
                summary["failures"].append(
                    {"doc_id": doc_id, "kind": kind,
                     "error_kind": type(e).__name__})
                continue
            local = os.path.join(stage_dir, path)
            os.makedirs(os.path.dirname(local), exist_ok=True)
            with open(local, "wb") as f:
                f.write(body)
            staged_dirs[os.path.dirname(path)] = os.path.dirname(local)
            ok_entries.append({
                "doc_id": doc_id, "kind": kind, "status": "ok",
                "sha256": sha256_bytes(body), "size": len(body),
                "path": path,
                "source": SOURCE_KIND, "source_id": doc_id,
                "fetched_at": _now(), "redistribution": REDISTRIBUTION,
                "updated_at": _now(),
            })
            summary["downloaded"] += 1

    # 原形のアップロード (保存先ディレクトリごと。済んでから目録を push する)
    for rel_dir, local_dir in sorted(staged_dirs.items()):
        kind = rel_dir.rsplit("/", 1)[-1]
        _upload_folder(api, repo, local_dir, rel_dir,
                       f"discovery {date}: raw {kind}")

    # 目録の ok は、アップロード後に遠隔で実在を確認できた分だけ書く
    if ok_entries:
        remote_after = _remote_files(api, repo)
        for entry in ok_entries:
            if entry["path"] in remote_after:
                acq.append(entry)
                continue
            acq.append({
                "doc_id": entry["doc_id"], "kind": entry["kind"],
                "status": "failed", "error_kind": "upload_unconfirmed",
                "error": "アップロード後に遠隔で実在を確認できませんでした",
                "path": entry["path"], "updated_at": _now(),
            })
            summary["failed"] += 1
            summary["failures"].append(
                {"doc_id": entry["doc_id"], "kind": entry["kind"],
                 "error_kind": "upload_unconfirmed"})
    for entry in failed_entries:
        acq.append(entry)

    # 目録と実行サマリの push
    summary["finished_at"] = _now()
    summary["requests"] = client.request_count
    summary_path = os.path.join(out_dir, "run_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    _upload_file(api, repo, cat_path_local,
                 f"catalog/month={month}/ledger.jsonl",
                 f"discovery {date}: catalog ledger")
    _upload_file(api, repo, acq_path_local,
                 f"acquisition/month={month}/ledger.jsonl",
                 f"discovery {date}: acquisition ledger")
    _upload_file(api, repo, summary_path,
                 f"acquisition/month={month}/run_summary-{date}-{stamp}.json",
                 f"discovery {date}: run summary")
    return summary


def _default_date():
    """前日 (Asia/Tokyo)。cron 化したときの既定の対象日。"""
    today = datetime.now(ZoneInfo("Asia/Tokyo")).date()
    return (today - timedelta(days=1)).isoformat()


def main(argv=None):
    ap = argparse.ArgumentParser(description="EDINET 日次取得 (S2-T5 §1)")
    sub = ap.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="対象日1日分の取得を行う")
    run.add_argument("--date", default=None,
                     help="対象日 YYYY-MM-DD (既定は前日 JST)")
    run.add_argument("--work-dir", required=True, help="作業ディレクトリ")
    run.add_argument("--repo", default=DEFAULT_REPO)
    run.add_argument("--max-docs", type=int, default=None,
                     help="今回の実行で試行する書類数の上限 (動作確認用)")
    run.add_argument("--kinds", default=None,
                     help="対象の種別 (カンマ区切り。既定は zip,pdf,attach,english)")
    run.add_argument("--interval", type=float, default=1.1,
                     help="EDINET API のリクエスト間隔 (秒)")
    args = ap.parse_args(argv)

    date = args.date or _default_date()
    kinds = None
    if args.kinds:
        kinds = [k.strip() for k in args.kinds.split(",") if k.strip()]
        unknown = [k for k in kinds if k not in _KIND_BY_NAME]
        if unknown:
            ap.error(f"未知の種別です: {unknown}")
    if not os.environ.get("EDINET_API_KEY"):
        print(json.dumps({"date": date, "status": "failed",
                          "error": "EDINET_API_KEY が設定されていません"},
                         ensure_ascii=False), file=sys.stderr)
        return 1
    client = EdinetClient(os.environ["EDINET_API_KEY"],
                          interval=args.interval)
    try:
        summary = run_acquisition(date, args.work_dir, repo=args.repo,
                                  client=client, max_docs=args.max_docs,
                                  kinds=kinds)
    except EdinetError as e:
        print(json.dumps({"date": date, "status": "failed",
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
