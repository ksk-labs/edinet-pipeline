"""EDINET 日次解析 (S2-T5 §2 解析と検証、§3 Silver への追記)。

塊1 (pipeline.discovery) が取得した書類の原形 (zip) を arelle で解析し、
EDINET 専用プラグインの検証を行い、結果を v2 データセット
(Yoshi-Dai/financial-lakehouse-v2) の Silver へ追記する。構成は3段階で、
GitHub Actions のジョブ分割 (plan -> parse マトリクス -> merge) に対応する。

- plan: 取得目録 (acquisition/) と書類目録 (catalog/) を読み、未解析の
  zip を対象として計画 (plan.json) を作る。解析済みの判定は解析目録
  (parse/month=YYYY-MM/ledger.jsonl) の最新行が ok で、入力 zip の
  SHA-256 が取得目録の記録と一致する場合だけとする (冪等)。
- shard: 計画の書類を担当分だけ解析する。書類ごとに Silver の行
  (parquet)、解析目録の行 (results.jsonl)、提出者拡張タクソノミの
  表示名 (labels.jsonl) を作業ディレクトリへ書く。HF へは書かない。
- merge: 全 shard の結果を集め、月別の silver.parquet へ反映してから
  解析目録・表示名・実行サマリを push する (原形のアップロードが
  済んでから目録を push する塊1と同じ規律。途中で落ちても、次回の
  実行が書類単位の置き換えで冪等にやり直せる)。

解析の規則 (仕様 §2):
- 抽出は arelle (版は ARELLE_VERSION に固定) で .xbrl インスタンスのみを
  対象とする。Inline XBRL のみの事実は S2-T3 で全標本 0 件を確認済みで、
  EDINET のダウンロード zip には iXBRL から実体化した .xbrl が常に入る
  ため、.xbrl の抽出で既存 Silver (自前変換器) と等価になる (標本45書類・
  14,875事実で完全一致を実測済み)。
- 固定事項2点: 期間の終端は endDate を使う (endDatetime は排他的な終端を
  返す)。プログラムから Cntlr を使うときは FormulaOptions を設定する
  (未設定だと検証の参照で AttributeError になる)。
- EDINET 専用プラグインの検証は、書類の zip を対象とする公式の呼び出し方
  (CntlrCmdLine に --plugins validate/EDINET --disclosureSystem <制度>
  --validate) で別プロセスで実行する。開示制度は書類のタクソノミ年度に
  合うものを選ぶ (2025-11-01 版は EDINET、2024-11-01 版は EDINET-2025)。
- 取り込みを止める (隔離する) のは次の場合だけとする (2026-11-17 の決定):
  (a) arelle がインスタンスを読めない (zip が壊れている、.xbrl が無い、
  読込に失敗する)、(b) 抽出できた数値事実が 0 件の場合。本工程が扱う
  書類はすべて xbrlFlag=1 であり、バックフィル済みの全書類種別・
  311,786 件で Silver の行数が 0 の書類は 0 件だった (実測) ため、
  0 件は「その書類種別では事実が存在するはず」に当たる。
  止めた書類は書類単位で隔離し、理由を解析目録に記録する (全体は止めない)。
- 検証の指摘はすべて記録のみで、取り込みは止めない。構造系のコード
  (EC01xx) は提出パッケージ用の規則であり、ダウンロードパッケージでは
  書類の所見として扱わず、検証の実行記録 (コード別件数) としてのみ残す。
  表紙系のコード (EC1001E・EC1057E) は、タクソノミ年度に合う開示制度で
  実行した場合に限り所見として記録し、年度が合わない実行ではノイズとして
  扱う (files/2026-11-17-edinet-validation-error-classification.md)。

Silver への追記 (仕様 §3):
- 行のスキーマは既存 Silver の実物 (silver/month=YYYY-MM/silver.parquet、
  pipeline.batch.SILVER_SCHEMA) と同一。実物に表示名の列は無いため、
  提出者拡張タクソノミの表示名は Silver へは入れず、P2-T10 の要素辞書への
  受け渡しとして parse/month=YYYY-MM/extension_labels.jsonl に分ける。
- 訂正の系譜 (superseded_by) は工程1の確定規則と同一 (同一親への訂正が
  複数ある場合は、提出日時が最も新しい訂正1件。同点なら doc_id の
  大きい方)。訂正として扱うのは doc_type=130 のみであり、040
  (訂正届出書) や 350 (大量保有の変更報告) など、目録上 parent_doc_id を
  持つ他の書類種別は系譜の対象ではない (convert_core の確定規則)。
  merge が、今回取り込む訂正と既存 Silver の状態から
  勝者を決め直し、親書類の行の superseded_by も必要なら書き換える。
- 成果物には出所 (源泉の種別・源泉側の識別子・取得日時・再配布可否) を
  解析目録の各行に持たせる。

認証は環境変数 HF_TOKEN (HF への書き込み、huggingface_hub が自動で読む)
を使う。読み取りは公開データセットなのでトークンなしでも動く。
"""
import argparse
import hashlib
import importlib.metadata
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import zipfile
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError

from .batch import SILVER_SCHEMA
from .convert_core import normalize_prefix

ARELLE_VERSION = "2.47.5"
DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"

# 出所 (成果物に持たせる。仕様 §3。塊1と同じ値)。
SOURCE_KIND = "edinet_api"
REDISTRIBUTION = "allowed"

# タクソノミの版 (名前空間 URI に現れる日付) -> 開示制度の対応。
# プラグイン (arelle 2.47.5 の plugin/validate/EDINET) が持つ制度は
# EDINET-2026 (2025-11-01 版、別名 EDINET) と EDINET-2025
# (2024-11-01 版) の2つだけである。
DISCLOSURE_SYSTEMS = {
    "2025-11-01": "EDINET",
    "2024-11-01": "EDINET-2025",
}
DEFAULT_DISCLOSURE_SYSTEM = "EDINET"

# 表紙系のコード。タクソノミ年度に合う開示制度で実行したときだけ
# 所見として意味を持つ (年度がずれると QName が一致せず全件欠落に見える)。
COVER_CODES = {"EDINET.EC1001E", "EDINET.EC1057E"}
# 構造系のコード (提出パッケージのフォルダ・ファイル名・拡張子の規則)。
_STRUCTURAL_RE = re.compile(r"^EDINET\.EC01\d{2}[A-Z]$")

_TAXONOMY_DATE_RE = re.compile(
    rb"disclosure\.edinet-fsa\.go\.jp/taxonomy/[A-Za-z0-9_]+/(\d{4}-\d{2}-\d{2})")

# 検証の別プロセスの上限。実測は約40秒/書類 (初回のタクソノミ取得を含む)
# だが、キャッシュが無い初回や大きな書類に余裕を持たせる。
VALIDATION_TIMEOUT_SEC = 900


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def _retryable(exc):
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status is not None:
        return status == 429 or status >= 500
    return isinstance(exc, OSError)


def _with_retry(what, fn):
    """HF API 呼び出しを指数バックオフで再試行する (pipeline.hf_sync と同じ方針)。"""
    delays = (5, 15, 30, 60, 120)
    for attempt in range(len(delays) + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt == len(delays) or not _retryable(exc):
                raise
            print(f"{what}: retry {attempt + 1} after error: {exc!r}",
                  file=sys.stderr)
            time.sleep(delays[attempt])


def _remote_files(api, repo):
    return _with_retry(
        "list_repo_files",
        lambda: api.list_repo_files(repo, repo_type="dataset"))


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


def _load_jsonl(path):
    rows = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    continue
    return rows


def _fetch_ledgers(api, repo, remote, prefix, work_dir):
    """prefix/month=*/ledger.jsonl をすべて落とし、行のリストで返す。

    戻り値: {month: [entry, ...]}。対象が無ければ空の辞書。
    """
    out = {}
    paths = sorted(p for p in remote
                   if p.startswith(prefix + "/month=")
                   and p.endswith("/ledger.jsonl"))
    for path in paths:
        month = path.split("/")[1].split("=", 1)[1]
        local = os.path.join(work_dir, "ledgers",
                             path.replace("/", "_"))
        if _download_if_exists(api, repo, path, local):
            out[month] = _load_jsonl(local)
    return out


# ---------------------------------------------------------------- 抽出

class Quarantine(Exception):
    """仕様 §2 の判定規則で取り込みを止める (隔離する) 場合の内部例外。"""

    def __init__(self, reason, detail):
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def _new_cntlr(webcache_dir):
    """抽出用の Cntlr を作る。arelle の import はここに閉じる
    (plan / merge のジョブは arelle をインストールしないため)。"""
    from arelle import Cntlr
    from arelle.ModelFormulaObject import FormulaOptions
    cntlr = Cntlr.Cntlr(logFileName="logToBuffer")
    cntlr.webCache.cacheDir = webcache_dir
    cntlr.webCache.enabled = True
    # 開示制度による読込の遮断は検証側 (別プロセス) の仕事であり、
    # 抽出側では無効にする (有効一覧に無い版の書類で事実が欠落するため)。
    cntlr.modelManager.validateDisclosureSystem = False
    # Validate.validate は modelManager.formulaOptions を参照するが、
    # プログラムから Cntlr を使う場合は未設定で AttributeError になる。
    cntlr.modelManager.formulaOptions = FormulaOptions()
    return cntlr


def _nsmap_of_file(path):
    """インスタンス文書の xmlns 宣言 (URI -> 接頭辞) を返す。

    次元の explicitMember の表記をインスタンス原文に寄せるために使う。
    不正な XML (実例: 命名実体参照のセミコロン欠落) では失敗する。
    その場合は空を返し、arelle 側の表記に任せる。
    """
    import xml.etree.ElementTree as ET
    uri2prefix = {}
    try:
        with open(path, "rb") as fh:
            for _event, (prefix, uri) in ET.iterparse(
                    fh, events=("start-ns",)):
                uri2prefix.setdefault(uri, prefix)
    except ET.ParseError:
        return {}
    return uri2prefix


def _dimension_string(ctx, uri2prefix):
    """context の次元を Silver の dimension 列の文字列にする。

    pipeline.convert_core の _dimension_string と同じ意味になるよう、
    explicitMember はその値 (QName 文字列) を、typedMember はテキスト
    内容を使い、";" で連結する。QName の接頭辞はインスタンス文書自身の
    xmlns 宣言から引く (原文の表記に寄せるため)。
    """
    parts = []
    for _dim_qname, dim_value in ctx.qnameDims.items():
        if dim_value.isExplicit:
            mq = dim_value.memberQname
            if mq is None:
                continue
            prefix = uri2prefix.get(mq.namespaceURI)
            parts.append(f"{prefix}:{mq.localName}" if prefix else str(mq))
        else:
            tm = dim_value.typedMember
            v = ""
            if tm is not None:
                if hasattr(tm, "itertext"):
                    v = "".join(tm.itertext()).strip()
                else:
                    v = (tm.textValue or "").strip()
                if not v:
                    children = list(tm)
                    if children:
                        v = children[0].tag.split("}")[-1]
            if v:
                parts.append(v)
    return ";".join(parts)


def _unit_string(fact):
    """単位を Silver の unit 列の文字列にする (convert_core の parse_units と同じ形)。"""
    if fact.unit is None:
        return ""
    nums, dens = fact.unit.measures
    s = "/".join(str(q) for q in nums)
    if dens:
        s += " per " + "/".join(str(q) for q in dens)
    return s


def _datestr(v):
    """arelle の日時を原文の日付文字列 (YYYY-MM-DD) にそろえる。"""
    if v is None:
        return None
    if hasattr(v, "date") and callable(getattr(v, "date")):
        try:
            return v.date().isoformat()
        except Exception:
            pass
    return v.isoformat() if hasattr(v, "isoformat") else str(v)


def _period_of(ctx):
    """context の期間を (開始日, 終了日) で返す。

    終端は endDate を使う。arelle の endDatetime は排他的な終端
    (書かれた endDate の翌日 00:00) を返すため、原文の日付と1日ずれる。
    """
    if ctx.isInstantPeriod:
        d = _datestr(ctx.instantDate)
        return d, d
    if ctx.isStartEndPeriod:
        return _datestr(ctx.startDatetime), _datestr(ctx.endDate)
    # forever 期間は既存 Silver と同じく開始・終了とも None で保持する
    return None, None


def _is_extension_ns(ns_uri):
    """提出者拡張タクソノミの名前空間かどうか。

    標準タクソノミの URI は /taxonomy/ を含む
    (例: .../taxonomy/jppfs/2025-11-01/jppfs_cor)。提出者拡張は
    .../jpcrp030000/asr/001/E00988-000/... の形で /taxonomy/ を含まない。
    """
    return "/taxonomy/" not in (ns_uri or "")


def extract_facts(cntlr, zip_path):
    """1書類の zip から arelle で数値事実を抽出する。

    戻り値: dict (facts, stats, labels, xbrl_files)。
    facts は重複除去済みの事実のリストで、各要素は Silver の行のうち
    書類単位の列 (doc_id 等) を除いた dict。
    labels は提出者拡張タクソノミの要素の表示名で、
    {(名前空間 URI, 要素名): {element, element_ns, label_ja, label_en}}。
    読めない場合は Quarantine を送出する (判定規則 (a))。
    """
    try:
        zf = zipfile.ZipFile(zip_path)
    except zipfile.BadZipFile as e:
        raise Quarantine("bad_zip", f"zip を開けません: {e}")
    with zf:
        names = sorted(n for n in zf.namelist()
                       if n.lower().endswith(".xbrl"))
        if not names:
            raise Quarantine("no_xbrl_instance",
                             "zip 内に .xbrl インスタンスがありません")
        with tempfile.TemporaryDirectory() as td:
            zf.extractall(td)
            paths = [os.path.join(td, n) for n in names]
            return _extract_from_files(cntlr, paths, names)


def _extract_from_files(cntlr, paths, names):
    facts = []
    seen = set()
    labels = {}
    stats = {"numeric": 0, "non_numeric": 0, "nil": 0, "no_context": 0,
             "unparseable": 0, "duplicates": 0}
    for path, name in zip(paths, names):
        uri2prefix = _nsmap_of_file(path)
        try:
            model = cntlr.modelManager.load(path)
        except Exception as e:
            raise Quarantine(
                "instance_load_failed",
                f"{name}: 読込に失敗しました: {type(e).__name__}: {e}")
        if model is None:
            raise Quarantine("instance_load_failed",
                             f"{name}: 読込に失敗しました (model が None)")
        try:
            for fact in model.facts:
                concept = fact.concept
                if concept is not None and _is_extension_ns(
                        concept.qname.namespaceURI):
                    key = (concept.qname.namespaceURI,
                           concept.qname.localName)
                    if key not in labels:
                        labels[key] = _concept_labels(concept)
                if fact.isNil:
                    stats["nil"] += 1
                    continue
                if not fact.isNumeric:
                    stats["non_numeric"] += 1
                    continue
                ctx = fact.context
                if ctx is None:
                    stats["no_context"] += 1
                    continue
                try:
                    val = Decimal(str(fact.value))
                except (InvalidOperation, ValueError):
                    stats["unparseable"] += 1
                    continue
                if val == 0:
                    # 符号付きゼロ (-0) を 0 に正規化する (convert_core と同じ)
                    val = Decimal(0)
                start, end = _period_of(ctx)
                rec = {
                    "element": (f"{normalize_prefix(concept.qname.namespaceURI)}"
                                f":{concept.qname.localName}"),
                    "value": float(val),
                    "value_dec": format(val, "f"),
                    "unit": _unit_string(fact),
                    "context_start": start,
                    "context_end": end,
                    "dimension": _dimension_string(ctx, uri2prefix),
                }
                key = (rec["element"], rec["context_start"],
                       rec["context_end"], rec["dimension"], rec["unit"],
                       rec["value_dec"])
                if key in seen:
                    stats["duplicates"] += 1
                    continue
                seen.add(key)
                facts.append(rec)
                stats["numeric"] += 1
        finally:
            cntlr.modelManager.close(model)
    return {"facts": facts, "stats": stats, "labels": labels,
            "xbrl_files": [os.path.basename(n) for n in names]}


def _concept_labels(concept):
    """拡張要素の表示名 (日英) を DTS から取る。無い言語は None。

    label() の第1引数は preferredLabel であり、言語は lang= で渡す。
    モデルを閉じた後に呼ぶと AssertionError になるため、呼出は
    モデルが開いている間に行う (extract 側の責務)。
    """
    out = {"element": (f"{normalize_prefix(concept.qname.namespaceURI)}"
                       f":{concept.qname.localName}"),
           "element_ns": concept.qname.namespaceURI,
           "label_ja": None, "label_en": None}
    for lang, key in (("ja", "label_ja"), ("en", "label_en")):
        try:
            text = concept.label(lang=lang)
        except Exception:
            text = None
        if text:
            out[key] = str(text)
    return out


# ---------------------------------------------------------------- 検証

def detect_taxonomy_date(zip_path):
    """zip 内の .xbrl から、参照しているタクソノミの版の日付を推定する。

    名前空間 URI と schemaRef に現れる /taxonomy/<体系>/<日付>/ の日付を
    集め、最頻のものを返す。見つからなければ None。
    """
    counts = Counter()
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".xbrl"):
                    continue
                with zf.open(name) as fh:
                    head = fh.read(1 << 18)
                for m in _TAXONOMY_DATE_RE.finditer(head):
                    counts[m.group(1).decode("ascii")] += 1
    except zipfile.BadZipFile:
        return None
    if not counts:
        return None
    return counts.most_common(1)[0][0]


_LOG_ENTRY_RE = re.compile(r'<entry code="([^"]+)" level="([^"]+)"')


def run_validation(zip_path, webcache_dir, work_dir, doc_id):
    """EDINET 専用プラグインの検証を公式の呼び出し方で実行する。

    戻り値: 検証の記録 (dict)。検証は記録のみが目的であり、失敗しても
    例外は送出しない (取り込みは止めない)。
    counts は検証の実行記録 (全コード。構造系 EC01xx を含む)。
    findings は書類の所見 (構造系を除く。表紙系は年度が合う場合のみ)。
    """
    taxonomy_date = detect_taxonomy_date(zip_path)
    system = DISCLOSURE_SYSTEMS.get(taxonomy_date,
                                    DEFAULT_DISCLOSURE_SYSTEM)
    year_match = taxonomy_date in DISCLOSURE_SYSTEMS
    result = {
        "ran": False, "system": system, "taxonomy_date": taxonomy_date,
        "year_match": year_match, "partial": False, "returncode": None,
        "counts": [], "findings": [], "error": None,
    }
    log_path = os.path.join(work_dir, f"validation-{doc_id}.xml")
    cmd = [sys.executable, "-m", "arelle.CntlrCmdLine",
           "--plugins", "validate/EDINET",
           "--disclosureSystem", system,
           "--validate",
           "--cacheDirectory", webcache_dir,
           "--logFile", log_path,
           "--file", zip_path]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=VALIDATION_TIMEOUT_SEC)
        result["returncode"] = proc.returncode
    except subprocess.TimeoutExpired:
        result["error"] = f"検証が {VALIDATION_TIMEOUT_SEC} 秒で終わりませんでした"
        return result
    except Exception as e:
        result["error"] = f"検証を実行できませんでした: {type(e).__name__}: {e}"
        return result
    if not os.path.exists(log_path):
        result["error"] = "検証のログファイルが作られませんでした"
        return result
    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError as e:
        result["error"] = f"検証のログを読めませんでした: {e}"
        return result
    counter = Counter(_LOG_ENTRY_RE.findall(text))
    counts = [{"code": c, "level": lv, "count": n}
              for (c, lv), n in sorted(counter.items())]
    result["counts"] = counts
    result["ran"] = True
    # 終了コードが 0 でない場合は、検証の後段で落ちた可能性がある
    # (実例: arelle 2.47.5 の rule_EC8021W の AttributeError)。
    # ログに残った分は実行記録として有効なため、部分実行として記録する。
    if proc.returncode != 0:
        result["partial"] = True
    for item in counts:
        code = item["code"]
        if item["level"] not in ("error", "warning"):
            continue
        if _STRUCTURAL_RE.match(code):
            continue
        if code in COVER_CODES and not year_match:
            continue
        result["findings"].append(item)
    return result


# ---------------------------------------------------------------- plan

def run_plan(date, doc_ids, max_docs, work_dir, repo=DEFAULT_REPO):
    """未解析の zip を対象として計画を作り、plan.json を書く。

    対象の特定は提出日 (書類目録の submitDateTime) で行う。取得目録と
    書類目録は月をまたいで全件読むため、月末に提出されて翌月の
    一覧で取得された書類も、その提出日で計画に載る。
    """
    os.makedirs(work_dir, exist_ok=True)
    api = HfApi()
    remote = set(_remote_files(api, repo))
    acq = _fetch_ledgers(api, repo, remote, "acquisition", work_dir)
    cat = _fetch_ledgers(api, repo, remote, "catalog", work_dir)
    parse = _fetch_ledgers(api, repo, remote, "parse", work_dir)

    acq_latest = {}
    for entries in acq.values():
        for e in entries:
            if e.get("kind") == "zip" and e.get("doc_id"):
                acq_latest[e["doc_id"]] = e
    cat_latest = {}
    for entries in cat.values():
        for e in entries:
            if e.get("docID"):
                cat_latest[e["docID"]] = e
    parse_latest = {}
    for entries in parse.values():
        for e in entries:
            if e.get("doc_id"):
                parse_latest[e["doc_id"]] = e

    wanted_ids = None
    if doc_ids:
        wanted_ids = {d.strip() for d in doc_ids.split(",") if d.strip()}
    targets = []
    skipped = {"done": 0, "no_catalog": 0, "filtered": 0}
    for doc_id, entry in sorted(acq_latest.items()):
        if entry.get("status") != "ok":
            continue
        meta = cat_latest.get(doc_id)
        if meta is None:
            skipped["no_catalog"] += 1
            continue
        submit_at = meta.get("submitDateTime") or ""
        if wanted_ids is not None and doc_id not in wanted_ids:
            skipped["filtered"] += 1
            continue
        if wanted_ids is None and date and submit_at[:10] != date:
            skipped["filtered"] += 1
            continue
        done = parse_latest.get(doc_id)
        if (done is not None and done.get("status") == "ok"
                and done.get("zip_sha256") == entry.get("sha256")):
            skipped["done"] += 1
            continue
        targets.append({
            "doc_id": doc_id,
            "zip_path": entry.get("path"),
            "zip_sha256": entry.get("sha256"),
            "submit_at": submit_at,
            "month": submit_at[:7],
            "doc_type": meta.get("docTypeCode"),
            "parent_doc_id": meta.get("parentDocID"),
            "fetched_at": entry.get("fetched_at"),
        })
    targets.sort(key=lambda d: (d["submit_at"] or "", d["doc_id"]))
    if max_docs is not None:
        targets = targets[:max_docs]
    plan = {
        "generated_at": _now(), "date": date, "doc_ids": doc_ids,
        "max_docs": max_docs, "docs": targets,
        "counts": {"targets": len(targets), "skipped": skipped,
                   "acquisition_zip_docs": len(acq_latest)},
    }
    plan_path = os.path.join(work_dir, "plan.json")
    with open(plan_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, ensure_ascii=False, indent=2)
    return plan


# ---------------------------------------------------------------- shard

def _silver_rows(doc, facts):
    """抽出事実を Silver の行 (batch.SILVER_SCHEMA の dict) にする。

    superseded_by は merge が全体の状態から決めるため、ここでは None。
    """
    is_correction = doc.get("doc_type") == "130"
    rows = []
    for f in facts:
        rows.append({
            "doc_id": doc["doc_id"],
            "element": f["element"],
            "value": f["value"],
            "unit": f["unit"],
            "context_start": f["context_start"],
            "context_end": f["context_end"],
            "dimension": f["dimension"],
            "submit_at": doc["submit_at"],
            "is_correction": is_correction,
            "parent_doc_id": doc["parent_doc_id"] if is_correction else None,
            "superseded_by": None,
            "point_in_time": "final" if is_correction else "as_filed",
        })
    return rows


def process_document(cntlr, doc, work_dir, webcache_dir, repo=DEFAULT_REPO):
    """1書類を解析し、(結果エントリ, Silver の行, 表示名の行) を返す。

    隔離する場合は Quarantine を送出する。その他の例外は呼び出し側で
    失敗として隔離する。
    """
    api = HfApi()
    local_zip = _with_retry("hf_hub_download", lambda: hf_hub_download(
        repo_id=repo, filename=doc["zip_path"], repo_type="dataset",
        local_dir=None, token=None))
    with open(local_zip, "rb") as f:
        data = f.read()
    zip_hash = sha256_bytes(data)
    if doc.get("zip_sha256") and zip_hash != doc["zip_sha256"]:
        raise Quarantine(
            "zip_hash_mismatch",
            "取得目録の SHA-256 と実物の zip が一致しません")
    extracted = extract_facts(cntlr, local_zip)
    facts = extracted["facts"]
    if not facts:
        # 判定規則 (b)。本工程の対象はすべて xbrlFlag=1 であり、
        # 事実が存在するはずの書類種別だけがここに来る (根拠は冒頭)。
        raise Quarantine(
            "zero_facts",
            "数値事実が 0 件でした (stats="
            + json.dumps(extracted["stats"], ensure_ascii=False) + ")")
    validation = run_validation(local_zip, webcache_dir, work_dir,
                                doc["doc_id"])
    rows = _silver_rows(doc, facts)
    labels = []
    for item in extracted["labels"].values():
        labels.append(dict(item, doc_id=doc["doc_id"]))
    entry = {
        "doc_id": doc["doc_id"], "status": "ok",
        "zip_sha256": zip_hash, "rows": len(rows),
        "month": doc["month"], "submit_at": doc["submit_at"],
        "doc_type": doc["doc_type"],
        "parent_doc_id": doc["parent_doc_id"],
        "stats": extracted["stats"],
        "xbrl_files": extracted["xbrl_files"],
        "validation": validation,
        "labels": len(labels),
        "arelle_version": _arelle_version(),
        "source": SOURCE_KIND, "source_id": doc["doc_id"],
        "fetched_at": doc.get("fetched_at"),
        "redistribution": REDISTRIBUTION,
        "parsed_at": _now(),
    }
    return entry, rows, labels


def _arelle_version():
    try:
        return importlib.metadata.version("arelle-release")
    except importlib.metadata.PackageNotFoundError:
        return None


def run_shard(plan_path, shard, shards, work_dir, webcache_dir,
              repo=DEFAULT_REPO):
    """計画のうち担当分 (index % shards == shard) を解析する。

    出力は work_dir/shard-<番号>/ の rows/<doc_id>.parquet、
    results.jsonl、labels.jsonl。HF へは書かない (merge の仕事)。
    """
    with open(plan_path, encoding="utf-8") as f:
        plan = json.load(f)
    docs = [d for i, d in enumerate(plan["docs"]) if i % shards == shard]
    out_dir = os.path.join(work_dir, f"shard-{shard}")
    rows_dir = os.path.join(out_dir, "rows")
    os.makedirs(rows_dir, exist_ok=True)
    os.makedirs(webcache_dir, exist_ok=True)
    results_path = os.path.join(out_dir, "results.jsonl")
    labels_path = os.path.join(out_dir, "labels.jsonl")
    summary = {"shard": shard, "docs": len(docs), "ok": 0,
               "quarantined": 0, "failed": 0, "rows": 0,
               "started_at": _now()}
    cntlr = _new_cntlr(webcache_dir)
    for doc in docs:
        doc_id = doc["doc_id"]
        try:
            entry, rows, labels = process_document(
                cntlr, doc, work_dir, webcache_dir, repo=repo)
        except Quarantine as q:
            entry = {
                "doc_id": doc_id, "status": "quarantined",
                "reason": q.reason, "error": q.detail,
                "zip_sha256": doc.get("zip_sha256"),
                "month": doc["month"], "submit_at": doc["submit_at"],
                "doc_type": doc["doc_type"],
                "parent_doc_id": doc["parent_doc_id"],
                "arelle_version": _arelle_version(),
                "source": SOURCE_KIND, "source_id": doc_id,
                "fetched_at": doc.get("fetched_at"),
                "redistribution": REDISTRIBUTION,
                "parsed_at": _now(),
            }
            summary["quarantined"] += 1
            _append_jsonl(results_path, entry)
            continue
        except Exception as e:  # 1書類の失敗で全体を止めない
            entry = {
                "doc_id": doc_id, "status": "failed",
                "error_kind": type(e).__name__,
                "error": f"{e}"[:500],
                "traceback": traceback.format_exc(limit=3),
                "zip_sha256": doc.get("zip_sha256"),
                "month": doc["month"], "submit_at": doc["submit_at"],
                "doc_type": doc["doc_type"],
                "parent_doc_id": doc["parent_doc_id"],
                "arelle_version": _arelle_version(),
                "source": SOURCE_KIND, "source_id": doc_id,
                "fetched_at": doc.get("fetched_at"),
                "redistribution": REDISTRIBUTION,
                "parsed_at": _now(),
            }
            summary["failed"] += 1
            _append_jsonl(results_path, entry)
            # Cntlr の状態が壊れている可能性があるため作り直す
            cntlr = _new_cntlr(webcache_dir)
            continue
        table = pa.Table.from_pylist(rows, schema=SILVER_SCHEMA)
        tmp = os.path.join(rows_dir, f"{doc_id}.parquet.tmp")
        pq.write_table(table, tmp)
        os.replace(tmp, os.path.join(rows_dir, f"{doc_id}.parquet"))
        _append_jsonl(results_path, entry)
        for label in labels:
            _append_jsonl(labels_path, label)
        summary["ok"] += 1
        summary["rows"] += len(rows)
    summary["finished_at"] = _now()
    with open(os.path.join(out_dir, "shard_summary.json"), "w",
              encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


def _append_jsonl(path, entry):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


# ---------------------------------------------------------------- merge

def _winner(candidates):
    """訂正の勝者を決める。candidates は {doc_id: submit_at}。

    工程1の確定規則: 提出日時が最も新しい訂正1件 (同点なら doc_id の
    大きい方)。batch.build_lineage と同じ比較である。
    """
    if not candidates:
        return None
    return max(candidates.items(), key=lambda kv: (kv[1] or "", kv[0]))[0]


def run_merge(plan_path, shards_dir, work_dir, repo=DEFAULT_REPO):
    """全 shard の結果を Silver と解析目録へ反映する。

    手順: (1) shard の結果と行を読む。(2) 影響する月ごとに
    silver.parquet を読み、取り込む書類の行を置き換え、訂正の勝者の
    変化に合わせて親書類の行の superseded_by を書き換える。
    (3) silver.parquet をすべて上げてから、解析目録・表示名・
    実行サマリを push する。
    """
    os.makedirs(work_dir, exist_ok=True)
    with open(plan_path, encoding="utf-8") as f:
        plan = json.load(f)
    api = HfApi()
    remote = set(_remote_files(api, repo))
    summary = {"started_at": _now(), "plan_docs": len(plan["docs"]),
               "ok": 0, "quarantined": 0, "failed": 0, "rows_added": 0,
               "months": {}, "lineage_updates": [], "notes": []}

    # shard の結果 (書類ごとの最新行が状態)
    results = {}
    shard_rows = {}  # doc_id -> parquet のパス
    label_rows = []
    shard_dirs = sorted(
        d for d in os.listdir(shards_dir)
        if d.startswith("shard-")
        and os.path.isdir(os.path.join(shards_dir, d)))
    for name in shard_dirs:
        sdir = os.path.join(shards_dir, name)
        for entry in _load_jsonl(os.path.join(sdir, "results.jsonl")):
            results[entry["doc_id"]] = entry
        label_rows.extend(_load_jsonl(os.path.join(sdir, "labels.jsonl")))
        rows_sub = os.path.join(sdir, "rows")
        if os.path.isdir(rows_sub):
            for fn in os.listdir(rows_sub):
                if fn.endswith(".parquet"):
                    shard_rows[fn[:-len(".parquet")]] = os.path.join(
                        rows_sub, fn)
    ok_docs = {d: e for d, e in results.items() if e.get("status") == "ok"}
    summary["ok"] = len(ok_docs)
    summary["quarantined"] = sum(
        1 for e in results.values() if e.get("status") == "quarantined")
    summary["failed"] = sum(
        1 for e in results.values() if e.get("status") == "failed")

    # 既存の解析目録 (全月) — 日次化以降の訂正の候補と、書類の所在の把握
    parse_ledgers = _fetch_ledgers(api, repo, remote, "parse", work_dir)
    parse_latest = {}
    for entries in parse_ledgers.values():
        for e in entries:
            if e.get("doc_id"):
                parse_latest[e["doc_id"]] = e
    # 既に Silver にある訂正 (解析目録で ok のもの): 親 -> {訂正: 提出日時}。
    # 訂正として扱うのは doc_type=130 のみ (工程1の確定規則)。
    # 040 (訂正届出書) や 350 (大量保有の変更報告) なども目録上は
    # parent_doc_id を持つが、系譜の対象ではない。
    known_corrections = {}
    for doc_id, e in parse_latest.items():
        if (e.get("status") == "ok" and e.get("doc_type") == "130"
                and e.get("parent_doc_id")):
            known_corrections.setdefault(e["parent_doc_id"], {})[doc_id] = \
                e.get("submit_at")
    for doc_id, e in ok_docs.items():
        if e.get("doc_type") == "130" and e.get("parent_doc_id"):
            known_corrections.setdefault(e["parent_doc_id"], {})[doc_id] = \
                e.get("submit_at")

    # Silver 側の書類の所在 (月) を目録から引くための索引。
    # バックフィル分の所在は silver/month=*/ledger.jsonl が持つ。
    silver_ledgers = _fetch_ledgers(api, repo, remote, "silver", work_dir)
    doc_month = {}
    for month, entries in silver_ledgers.items():
        for e in entries:
            if e.get("doc_id"):
                doc_month[e["doc_id"]] = month
    for doc_id, e in parse_latest.items():
        if e.get("month"):
            doc_month[doc_id] = e["month"]
    for doc_id, e in ok_docs.items():
        doc_month[doc_id] = e["month"]

    # 影響する月の集合: 取り込む書類の月 + 勝者が変わりうる親の月
    affected_parents = set()
    for e in ok_docs.values():
        if e.get("doc_type") == "130" and e.get("parent_doc_id"):
            affected_parents.add(e["parent_doc_id"])
    months = {e["month"] for e in ok_docs.values()}
    for parent in affected_parents:
        m = doc_month.get(parent)
        if m:
            months.add(m)
        else:
            summary["notes"].append(
                f"親書類 {parent} の所在が分からず系譜を更新できません")

    # 月ごとに parquet を読み、更新して stage へ書く
    stage_dir = os.path.join(work_dir, "stage")
    os.makedirs(stage_dir, exist_ok=True)
    submit_at_cache = {}

    def submit_at_of(doc_id, month_hint=None):
        """書類の提出日時を Silver の行から引く (勝者の比較用)。"""
        if doc_id in ok_docs:
            return ok_docs[doc_id].get("submit_at")
        if doc_id in parse_latest:
            return parse_latest[doc_id].get("submit_at")
        if doc_id in submit_at_cache:
            return submit_at_cache[doc_id]
        month = month_hint or doc_month.get(doc_id)
        value = None
        if month:
            path = os.path.join(stage_dir, month, "silver.parquet")
            if not os.path.exists(path):
                _download_month_silver(api, repo, remote, month, stage_dir)
            if os.path.exists(path):
                t = pq.read_table(path, columns=["doc_id", "submit_at"])
                for r in t.to_pylist():
                    if r["doc_id"] == doc_id:
                        value = r["submit_at"]
                        break
        submit_at_cache[doc_id] = value
        return value

    new_silver = {}  # month -> 書き換え後の parquet のパス
    for month in sorted(months):
        local = _download_month_silver(api, repo, remote, month, stage_dir)
        table = (pq.read_table(local) if local
                 else pa.Table.from_pylist([], schema=SILVER_SCHEMA))
        rows = table.to_pylist()
        by_doc = {}
        for r in rows:
            by_doc.setdefault(r["doc_id"], []).append(r)
        changed = False

        def superseded_for(doc_id):
            cands = dict(known_corrections.get(doc_id) or {})
            current = None
            if by_doc.get(doc_id):
                current = by_doc[doc_id][0].get("superseded_by")
            if current and current not in cands:
                sat = submit_at_of(current)
                if sat:
                    cands[current] = sat
            return _winner(cands)

        # 取り込む書類: 既存の行を置き換える
        for doc_id, e in ok_docs.items():
            if e["month"] != month:
                continue
            new_rows = pq.read_table(shard_rows[doc_id]).to_pylist()
            winner = superseded_for(doc_id)
            for r in new_rows:
                r["superseded_by"] = winner
            by_doc[doc_id] = new_rows
            changed = True
            summary["rows_added"] += len(new_rows)
        # 親書類: 勝者が変わっていたら superseded_by だけ書き換える
        for parent in sorted(affected_parents):
            if doc_month.get(parent) != month or parent in ok_docs:
                continue
            if parent not in by_doc:
                summary["notes"].append(
                    f"親書類 {parent} が Silver ({month}) に無く"
                    "系譜を更新できません")
                continue
            winner = superseded_for(parent)
            current = by_doc[parent][0].get("superseded_by")
            if winner != current:
                for r in by_doc[parent]:
                    r["superseded_by"] = winner
                changed = True
                summary["lineage_updates"].append(
                    {"doc_id": parent, "month": month,
                     "from": current, "to": winner})
        if not changed:
            continue
        out_rows = []
        for doc_id in by_doc:
            out_rows.extend(by_doc[doc_id])
        out_table = pa.Table.from_pylist(out_rows, schema=SILVER_SCHEMA)
        out_path = os.path.join(stage_dir, month, "silver.parquet")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        tmp = out_path + ".tmp"
        pq.write_table(out_table, tmp)
        os.replace(tmp, out_path)
        new_silver[month] = out_path
        summary["months"][month] = {
            "rows": out_table.num_rows, "docs": len(by_doc)}

    # (3) push: silver をすべて上げてから目録類を上げる
    for month, path in sorted(new_silver.items()):
        _upload_file(api, repo, path, f"silver/month={month}/silver.parquet",
                     f"parse {plan.get('date') or ''}: silver {month}")
    # 解析目録: 月ごとに既存へ追記する
    entries_by_month = {}
    for e in results.values():
        entries_by_month.setdefault(e["month"], []).append(e)
    for month, entries in sorted(entries_by_month.items()):
        local = os.path.join(work_dir, "parse-ledgers", month, "ledger.jsonl")
        os.makedirs(os.path.dirname(local), exist_ok=True)
        existing = parse_ledgers.get(month) or []
        with open(local, "w", encoding="utf-8") as f:
            for e in existing:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
            for e in sorted(entries, key=lambda x: x["doc_id"]):
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        _upload_file(api, repo, local, f"parse/month={month}/ledger.jsonl",
                     f"parse {plan.get('date') or ''}: parse ledger {month}")
    # 表示名: (doc_id, element) で重複を除いて追記する
    labels_by_month = {}
    for label in label_rows:
        doc_id = label.get("doc_id")
        month = (ok_docs.get(doc_id) or {}).get("month")
        if month:
            labels_by_month.setdefault(month, []).append(label)
    for month, labels in sorted(labels_by_month.items()):
        local = os.path.join(work_dir, "labels", month,
                             "extension_labels.jsonl")
        os.makedirs(os.path.dirname(local), exist_ok=True)
        seen_pairs = set()
        merged = []
        remote_path = f"parse/month={month}/extension_labels.jsonl"
        if remote_path in remote:
            tmp_local = local + ".remote"
            if _download_if_exists(api, repo, remote_path, tmp_local):
                for e in _load_jsonl(tmp_local):
                    seen_pairs.add((e.get("doc_id"), e.get("element")))
                    merged.append(e)
        for label in labels:
            pair = (label.get("doc_id"), label.get("element"))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            merged.append(label)
        with open(local, "w", encoding="utf-8") as f:
            for e in merged:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        _upload_file(api, repo, local, remote_path,
                     f"parse {plan.get('date') or ''}: extension labels {month}")
    summary["finished_at"] = _now()
    for month in sorted(set(entries_by_month) | set(new_silver)):
        local = os.path.join(work_dir, "summaries", month, "run_summary.json")
        os.makedirs(os.path.dirname(local), exist_ok=True)
        with open(local, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        _upload_file(api, repo, local, f"parse/month={month}/run_summary.json",
                     f"parse {plan.get('date') or ''}: run summary {month}")
    return summary


def _download_month_silver(api, repo, remote, month, stage_dir):
    """その月の silver.parquet を stage へ落とす。無ければ None。"""
    local = os.path.join(stage_dir, month, "silver.parquet")
    if os.path.exists(local):
        return local
    path = f"silver/month={month}/silver.parquet"
    if path not in remote:
        return None
    if _download_if_exists(api, repo, path, local):
        return local
    return None


def run_repair_lineage(doc_ids, work_dir, repo=DEFAULT_REPO):
    """指定した書類の superseded_by を、系譜の規則で決め直す。

    候補は、解析目録で ok の doc_type=130 の訂正だけとする。現在の値が
    解析目録にある書類を指す場合は、その種別が 130 のときだけ候補に
    含める (130 でないと分かっている値は、規則に合わない値として
    外す対象になる)。現在の値が解析目録に無い (バックフィル期の) 書類を
    指し、候補が1件も無い場合は、判断材料が無いため触らない。
    規則の解釈を直したときに対象書類の状態を修復するための運用コマンドで、
    変更があった月だけ parquet を書き換えて上げる。
    """
    os.makedirs(work_dir, exist_ok=True)
    api = HfApi()
    remote = set(_remote_files(api, repo))
    parse_ledgers = _fetch_ledgers(api, repo, remote, "parse", work_dir)
    silver_ledgers = _fetch_ledgers(api, repo, remote, "silver", work_dir)
    parse_latest = {}
    for entries in parse_ledgers.values():
        for e in entries:
            if e.get("doc_id"):
                parse_latest[e["doc_id"]] = e
    doc_month = {}
    for month, entries in silver_ledgers.items():
        for e in entries:
            if e.get("doc_id"):
                doc_month[e["doc_id"]] = month
    for doc_id, e in parse_latest.items():
        if e.get("month"):
            doc_month[doc_id] = e["month"]
    known_corrections = {}
    for doc_id, e in parse_latest.items():
        if (e.get("status") == "ok" and e.get("doc_type") == "130"
                and e.get("parent_doc_id")):
            known_corrections.setdefault(e["parent_doc_id"], {})[doc_id] = \
                e.get("submit_at")

    stage_dir = os.path.join(work_dir, "stage")
    os.makedirs(stage_dir, exist_ok=True)
    tables = {}

    def rows_of(doc_id):
        month = doc_month.get(doc_id)
        if not month:
            return None, None
        if month not in tables:
            local = _download_month_silver(api, repo, remote, month,
                                           stage_dir)
            if not local:
                return None, None
            tables[month] = pq.read_table(local).to_pylist()
        rows = [r for r in tables[month] if r["doc_id"] == doc_id]
        return month, rows

    summary = {"doc_ids": doc_ids, "repairs": [], "unchanged": [],
               "not_found": []}
    dirty_months = set()
    for doc_id in doc_ids:
        month, rows = rows_of(doc_id)
        if not rows:
            summary["not_found"].append(doc_id)
            continue
        current = rows[0].get("superseded_by")
        cands = dict(known_corrections.get(doc_id) or {})
        cur_entry = parse_latest.get(current) if current else None
        if cur_entry is not None and cur_entry.get("doc_type") == "130":
            cands.setdefault(current, cur_entry.get("submit_at"))
        if cands:
            winner = _winner(cands)
        elif current and cur_entry is not None:
            # 現在の値が 130 でないと分かっており、候補も無い
            winner = None
        else:
            # 判断材料が無い (バックフィル期の値) ため触らない
            winner = current
        if winner == current:
            summary["unchanged"].append(doc_id)
            continue
        for r in tables[month]:
            if r["doc_id"] == doc_id:
                r["superseded_by"] = winner
        dirty_months.add(month)
        summary["repairs"].append(
            {"doc_id": doc_id, "month": month, "from": current, "to": winner})
    for month in sorted(dirty_months):
        out_table = pa.Table.from_pylist(tables[month], schema=SILVER_SCHEMA)
        out_path = os.path.join(stage_dir, month, "silver.parquet")
        tmp = out_path + ".tmp"
        pq.write_table(out_table, tmp)
        os.replace(tmp, out_path)
        _upload_file(api, repo, out_path,
                     f"silver/month={month}/silver.parquet",
                     f"parse repair-lineage: silver {month}")
    summary["finished_at"] = _now()
    return summary


# ---------------------------------------------------------------- CLI

def main(argv=None):
    ap = argparse.ArgumentParser(description="EDINET 日次解析 (S2-T5 §2・§3)")
    sub = ap.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="未解析の書類から計画を作る")
    plan.add_argument("--date", default=None,
                      help="対象の提出日 YYYY-MM-DD (--doc-ids と併用しない)")
    plan.add_argument("--doc-ids", default=None,
                      help="対象の書類 ID (カンマ区切り)")
    plan.add_argument("--max-docs", type=int, default=None,
                      help="今回の計画に載せる書類数の上限 (動作確認用)")
    plan.add_argument("--work-dir", required=True)
    plan.add_argument("--repo", default=DEFAULT_REPO)

    shard = sub.add_parser("shard", help="計画の担当分を解析する")
    shard.add_argument("--plan", required=True, help="plan.json のパス")
    shard.add_argument("--shard", type=int, required=True)
    shard.add_argument("--shards", type=int, required=True)
    shard.add_argument("--work-dir", required=True)
    shard.add_argument("--webcache-dir", default=None,
                       help="arelle のタクソノミキャッシュ (既定は work-dir/webcache)")
    shard.add_argument("--repo", default=DEFAULT_REPO)

    merge = sub.add_parser("merge", help="shard の結果を Silver へ反映する")
    merge.add_argument("--plan", required=True, help="plan.json のパス")
    merge.add_argument("--shards-dir", required=True,
                       help="shard-* ディレクトリの親")
    merge.add_argument("--work-dir", required=True)
    merge.add_argument("--repo", default=DEFAULT_REPO)

    repair = sub.add_parser(
        "repair-lineage", help="指定した書類の superseded_by を決め直す")
    repair.add_argument("--doc-ids", required=True,
                        help="対象の書類 ID (カンマ区切り)")
    repair.add_argument("--work-dir", required=True)
    repair.add_argument("--repo", default=DEFAULT_REPO)

    extract = sub.add_parser(
        "extract-one", help="1書類の zip を解析する (照合・動作確認用)")
    extract.add_argument("--zip", required=True)
    extract.add_argument("--out", required=True, help="結果 JSON の出力先")
    extract.add_argument("--webcache-dir", required=True)
    extract.add_argument("--validate", action="store_true",
                         help="EDINET 検証も実行する")
    args = ap.parse_args(argv)

    if args.command == "plan":
        result = run_plan(args.date, args.doc_ids, args.max_docs,
                          args.work_dir, repo=args.repo)
        print(json.dumps(result["counts"], ensure_ascii=False))
        return 0
    if args.command == "shard":
        webcache = args.webcache_dir or os.path.join(args.work_dir, "webcache")
        result = run_shard(args.plan, args.shard, args.shards,
                           args.work_dir, webcache, repo=args.repo)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    if args.command == "merge":
        result = run_merge(args.plan, args.shards_dir, args.work_dir,
                           repo=args.repo)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    if args.command == "repair-lineage":
        doc_ids = [d.strip() for d in args.doc_ids.split(",") if d.strip()]
        result = run_repair_lineage(doc_ids, args.work_dir, repo=args.repo)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    if args.command == "extract-one":
        cntlr = _new_cntlr(args.webcache_dir)
        try:
            extracted = extract_facts(cntlr, args.zip)
            out = {"facts": [{k: v for k, v in f.items()}
                             for f in extracted["facts"]],
                   "stats": extracted["stats"],
                   "xbrl_files": extracted["xbrl_files"],
                   "labels": list(extracted["labels"].values())}
        except Quarantine as q:
            out = {"quarantined": q.reason, "error": q.detail}
        if args.validate:
            out["validation"] = run_validation(
                args.zip, args.webcache_dir,
                os.path.dirname(os.path.abspath(args.out)), "extract-one")
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False)
        print(json.dumps({k: out[k] for k in out if k != "facts"},
                         ensure_ascii=False))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
