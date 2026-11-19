"""テキスト層の器 (P2-T7、仕様 #15 §6)。

EDINET 書類の文字事実 (非数値事実) を、源泉非依存の形で原文のまま
保持する格納層。有価証券報告書の表紙・本文のテキストブロック等が
対象で、数値事実だけを持つ Silver に対する文字側の受け皿である。
本モジュールは器 (格納の形、取り込み口、代表者の観測の受け皿) まで
であり、要素の分類の網羅や文章の構造化抽出はテキスト層の本格実装
(別工程) の範囲とする。

成果物は v2 データセット (Yoshi-Dai/financial-lakehouse-v2) の
text/ 配下に置く (本モジュールが正本):
- items.parquet: 文字事実。1行が1事実で、原文 (raw_text) を
  加工せず保持する。正規化した要約は normalized_summary に
  併記できる (取り込み時点では null。後続工程が埋める)。
  抽出の確度が低いものは certainty=unconfirmed と明示する。
  文脈 (context_start / context_end / dimension) は Silver と
  同じ文字列形で保持し、事実の同一性を再現できる形にする。
  同一書類内の完全な重複 (要素・文脈・次元・原文がすべて同一)
  は1行に畳む (parse.py の数値事実の重複除去と同じ規則)。
  text_id は (源泉種別, 源泉書類 ID, 要素, 文脈, 次元, 原文の
  sha256) から決定的に生成する。
- representative_observations.parquet: 代表者の観測の受け皿。
  有報の表紙の文字事実 (代表者の役職氏名) を取り込み、会社
  マスタの属性履歴 (company_master.attribute_history) へ渡せる
  列構成 (internal_id, edinet_code, attribute, value と観測日)
  を持つ。役職・氏名への分解は「最初の空白までを役職、残りを
  氏名」とする決定的な規則で行い、空白が無く分解できないものは
  role / name を null にして certainty=unconfirmed (未確定)
  として残す。未確定の観測は属性へ渡さない。
- summary.json: 構築のサマリと照合結果。

対象の7項目 (カテゴリの語彙。本モジュールが正本):
representative (代表者)、class_shares (種類株式の内容)、
business_and_risks (事業の内容・リスク)、history (沿革・変遷)、
segments (セグメントの説明)、shareholder_benefits (株主優待の
記述)、affiliated_entities (関係会社の状況)。
EDINET 有報タクソノミ (jpcrp_cor) の要素からカテゴリへの対応は、
実書類 (S100ZI7X、株式会社ビックカメラの有価証券報告書、
2026-11-17 提出) で実測した要素名に基づく初期対応であり、
対応表に無い要素は category=null (未分類) のまま原文を格納する。
株主優待は XBRL の定型要素が無い (文章・表が源泉であり、主たる
源泉は TDnet) ため EDINET 側の対応は空であり、カテゴリの語彙と
受け皿だけを用意する。株主優待のイベントの器は会社イベント
(company_events) 側に種別を予約済みであり、本モジュールでは
作らない。

取り込み口は源泉非依存とする: build の入力は文字事実レコード
(JSONL) で、各レコードが source_kind と source_doc_id で書類を
特定する。EDINET の XBRL、TDnet の XBRL、PDF からの抽出の
いずれも同じ形で入る。EDINET 用の抽出 (extract) は arelle で
提出 zip からこのレコード列を作るアダプタとして分離し、抽出の
固定事項 (終端は endDate、FormulaOptions の設定、開示制度による
読込遮断の無効化) は parse.py の実装を再利用する。

すべての行に出所 (源泉の種別、源泉側の識別子、取得日時、
再配布可否) を持たせる。再配布可否はレコード側の値を保持し、
欠けた場合は unknown とする (可否が無い源泉を公開層へ出さない
原則に合わせ、許可側へは倒さない)。EDINET 抽出では allowed を
明示する。

使い方:
  python -m pipeline.text_layer download --doc-id ID \
      --submit-date YYYY-MM-DD --zip-out ZIP --master-dir DIR
  python -m pipeline.text_layer extract --zip ZIP --doc-id ID \
      --edinet-code CODE --submit-at TS --out facts.jsonl
  python -m pipeline.text_layer build --work-dir DIR [--upload]
build は --facts (文字事実 JSONL、複数指定可) と --master-dir
(edinet_codes.parquet を含むローカルディレクトリ) を受け付け、
HF を介さない検証実行ができる。
認証は環境変数 HF_TOKEN (書き込み。huggingface_hub が自動で読む)。
読み取りは公開データセットなのでトークンなしでも動く。
"""
import argparse
import collections
import hashlib
import json
import os
import re
import sys
import tempfile
import zipfile
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download

from .hf_sync import _with_retry

DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"
TEXT_PREFIX = "text"
MASTER_PREFIX = "master"
ITEMS_NAME = "items"
OBSERVATIONS_NAME = "representative_observations"
SUMMARY_NAME = "summary.json"

EDINET_SOURCE_KIND = "edinet_api"
REDISTRIBUTION_ALLOWED = "allowed"
REDISTRIBUTION_UNKNOWN = "unknown"

CERTAINTY_CONFIRMED = "confirmed"
CERTAINTY_UNCONFIRMED = "unconfirmed"
CERTAINTIES = (CERTAINTY_CONFIRMED, CERTAINTY_UNCONFIRMED)

CATEGORIES = {
    "representative": "代表者",
    "class_shares": "種類株式の内容",
    "business_and_risks": "事業の内容・リスク",
    "history": "沿革・変遷",
    "segments": "セグメントの説明",
    "shareholder_benefits": "株主優待の記述",
    "affiliated_entities": "関係会社の状況",
}

ELEMENT_CATEGORY = {
    "jpcrp_cor:TitleAndNameOfRepresentativeCoverPage": "representative",
    "jpcrp_cor:ClassesOfSharesEtcTextBlock": "class_shares",
    "jpcrp_cor:DescriptionOfBusinessTextBlock": "business_and_risks",
    "jpcrp_cor:BusinessRisksTextBlock": "business_and_risks",
    "jpcrp_cor:CompanyHistoryTextBlock": "history",
    "jpcrp_cor:DescriptionOfReportableSegmentsTextBlock": "segments",
    "jpcrp_cor:NotesSegmentInformationEtcConsolidatedFinancialStatementsTextBlock":
        "segments",
    "jpcrp_cor:OverviewOfAffiliatedEntitiesTextBlock": "affiliated_entities",
}

REPRESENTATIVE_ATTRIBUTE = "representative"

ITEMS_SCHEMA = pa.schema([
    ("text_id", pa.string()),
    ("internal_id", pa.int64()),
    ("edinet_code", pa.string()),
    ("category", pa.string()),
    ("category_label", pa.string()),
    ("element", pa.string()),
    ("element_ns", pa.string()),
    ("raw_text", pa.string()),
    ("normalized_summary", pa.string()),
    ("certainty", pa.string()),
    ("context_start", pa.string()),
    ("context_end", pa.string()),
    ("dimension", pa.string()),
    ("submit_at", pa.string()),
    ("source_doc_id", pa.string()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("source_sha256", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

OBSERVATIONS_SCHEMA = pa.schema([
    ("internal_id", pa.int64()),
    ("edinet_code", pa.string()),
    ("attribute", pa.string()),
    ("role", pa.string()),
    ("name", pa.string()),
    ("value", pa.string()),
    ("raw_text", pa.string()),
    ("certainty", pa.string()),
    ("observed_date", pa.string()),
    ("submit_at", pa.string()),
    ("source_doc_id", pa.string()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

_WHITESPACE_RUN = re.compile(r"\s+")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def classify_element(element):
    """要素名からカテゴリを返す。対応表に無ければ None (未分類)。"""
    return ELEMENT_CATEGORY.get(element)


def split_representative(raw_text):
    """代表者の役職氏名を (役職, 氏名) へ分解する。

    規則は「最初の空白 (全角・半角を含む) までを役職、残りを
    氏名」とする決定的なもの。氏名内部の空白は保持する。
    分解できなければ (None, None) を返す。
    """
    text = (raw_text or "").strip()
    if not text:
        return None, None
    parts = _WHITESPACE_RUN.split(text, maxsplit=1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None, None
    return parts[0], parts[1]


def _text_id(fact, raw_text):
    raw_sha = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
    key = "\x1f".join([
        fact.get("source_kind") or "",
        fact.get("source_doc_id") or "",
        fact.get("element") or "",
        fact.get("context_start") or "",
        fact.get("context_end") or "",
        fact.get("dimension") or "",
        raw_sha,
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def build_items(facts, code_to_id, generated_at):
    """文字事実レコードの列から items の行を作る。

    facts の各要素は source_kind / source_doc_id / edinet_code /
    submit_at / element / raw_text を持ち、element_ns /
    context_start / context_end / dimension / certainty /
    normalized_summary / category / source_id / source_sha256 /
    redistribution は任意とする。カテゴリはレコードの明示が
    あればそれを優先し、無ければ要素の対応表で決める。
    戻り値は (行のリスト, 統計 dict)。
    """
    items = []
    seen = set()
    stats = {
        "facts_in": len(facts),
        "duplicates": 0,
        "empty_text": 0,
        "unresolved_codes": 0,
        "by_category": collections.Counter(),
    }
    for fact in facts:
        raw_text = fact.get("raw_text")
        if raw_text is None:
            stats["empty_text"] += 1
            continue
        text_id = _text_id(fact, raw_text)
        if text_id in seen:
            stats["duplicates"] += 1
            continue
        seen.add(text_id)
        category = fact.get("category")
        if category not in CATEGORIES:
            category = classify_element(fact.get("element"))
        certainty = fact.get("certainty")
        if certainty not in CERTAINTIES:
            certainty = CERTAINTY_UNCONFIRMED
        code = fact.get("edinet_code")
        internal_id = code_to_id.get(code) if code else None
        if code and internal_id is None:
            stats["unresolved_codes"] += 1
        items.append({
            "text_id": text_id,
            "internal_id": internal_id,
            "edinet_code": code,
            "category": category,
            "category_label": CATEGORIES.get(category),
            "element": fact.get("element"),
            "element_ns": fact.get("element_ns"),
            "raw_text": raw_text,
            "normalized_summary": fact.get("normalized_summary"),
            "certainty": certainty,
            "context_start": fact.get("context_start"),
            "context_end": fact.get("context_end"),
            "dimension": fact.get("dimension") or "",
            "submit_at": fact.get("submit_at"),
            "source_doc_id": fact.get("source_doc_id"),
            "source_kind": fact.get("source_kind"),
            "source_id": fact.get("source_id") or fact.get("source_doc_id"),
            "source_sha256": fact.get("source_sha256"),
            "generated_at": generated_at,
            "redistribution": fact.get("redistribution")
                or REDISTRIBUTION_UNKNOWN,
        })
        stats["by_category"][category or "unclassified"] += 1
    stats["items_total"] = len(items)
    return items, stats


def build_observations(items, generated_at):
    """items から代表者の観測を作る。

    対象はカテゴリが representative の行。観測日は提出日
    (submit_at の先頭10文字) とする。
    """
    observations = []
    for item in items:
        if item["category"] != REPRESENTATIVE_ATTRIBUTE:
            continue
        role, name = split_representative(item["raw_text"])
        if role is None:
            value = item["raw_text"].strip()
            certainty = CERTAINTY_UNCONFIRMED
        else:
            value = f"{role} {name}"
            certainty = item["certainty"]
        observations.append({
            "internal_id": item["internal_id"],
            "edinet_code": item["edinet_code"],
            "attribute": REPRESENTATIVE_ATTRIBUTE,
            "role": role,
            "name": name,
            "value": value,
            "raw_text": item["raw_text"],
            "certainty": certainty,
            "observed_date": (item["submit_at"] or "")[:10] or None,
            "submit_at": item["submit_at"],
            "source_doc_id": item["source_doc_id"],
            "source_kind": item["source_kind"],
            "source_id": item["source_id"],
            "generated_at": generated_at,
            "redistribution": item["redistribution"],
        })
    return observations


def to_attribute_rows(observations):
    """代表者の観測を会社マスタの属性履歴の形の行へ変換する。

    戻り値の各行は company_master.ATTRIBUTE_SCHEMA と同じキー
    構成を持ち、そのまま attribute_history へ取り込める。
    変換の規則は会社マスタの開閉の規則に合わせる: 同じ会社・
    同じ値の観測が続けば1行にまとめ (first/last_observed_date と
    observation_count を伸ばし)、値が変われば別の行にする。
    未確定 (certainty=unconfirmed) の観測は渡さない。
    """
    confirmed = [o for o in observations
                 if o["certainty"] == CERTAINTY_CONFIRMED
                 and o["internal_id"] is not None]
    confirmed.sort(key=lambda o: (o["internal_id"], o["edinet_code"] or "",
                                  o["observed_date"] or "",
                                  o["submit_at"] or "",
                                  o["source_doc_id"] or ""))
    rows = []
    open_rows = {}
    for obs in confirmed:
        key = (obs["internal_id"], obs["edinet_code"], obs["value"])
        row = open_rows.get(key)
        if row is not None and (row["last_observed_date"] or "") <= \
                (obs["observed_date"] or ""):
            row["last_observed_date"] = obs["observed_date"]
            row["observation_count"] += 1
            continue
        row = {
            "internal_id": obs["internal_id"],
            "edinet_code": obs["edinet_code"],
            "attribute": REPRESENTATIVE_ATTRIBUTE,
            "value": obs["value"],
            "first_observed_date": obs["observed_date"],
            "last_observed_date": obs["observed_date"],
            "observation_count": 1,
            "source_kind": obs["source_kind"],
            "source_id": obs["source_id"],
            "generated_at": obs["generated_at"],
            "redistribution": obs["redistribution"],
        }
        open_rows[key] = row
        rows.append(row)
    rows.sort(key=lambda r: (r["internal_id"], r["first_observed_date"] or "",
                             r["value"] or ""))
    return rows


def load_facts(paths):
    """文字事実レコード (JSONL) をすべて読む。"""
    facts = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    facts.append(json.loads(line))
    return facts


def load_code_to_id(master_dir):
    """master/edinet_codes.parquet からコード -> 内部 ID を作る。"""
    table = pq.read_table(os.path.join(master_dir, "edinet_codes.parquet"))
    rows = table.to_pylist()
    return {r["edinet_code"]: r["internal_id"] for r in rows}


def extract_text_facts(zip_path, doc_id, edinet_code, submit_at,
                       webcache_dir):
    """提出 zip から文字事実レコードを作る (EDINET 用アダプタ)。

    parse.py の抽出と同じ経路で .xbrl インスタンスを arelle で
    読み、非数値・非 nil の事実を原文のまま返す。数値事実は
    Silver の範囲であり、ここでは数えない。
    戻り値は (レコードのリスト, 統計 dict, zip の sha256)。
    """
    from .convert_core import normalize_prefix
    from .parse import (_dimension_string, _new_cntlr, _nsmap_of_file,
                        _period_of)
    with open(zip_path, "rb") as f:
        zip_sha256 = hashlib.sha256(f.read()).hexdigest()
    cntlr = _new_cntlr(webcache_dir)
    facts = []
    stats = {"xbrl_files": 0, "numeric": 0, "nil": 0, "non_numeric": 0,
             "no_value": 0}
    with zipfile.ZipFile(zip_path) as zf:
        names = sorted(n for n in zf.namelist()
                       if n.lower().endswith(".xbrl"))
        stats["xbrl_files"] = len(names)
        with tempfile.TemporaryDirectory() as td:
            zf.extractall(td)
            for name in names:
                path = os.path.join(td, name)
                uri2prefix = _nsmap_of_file(path)
                model = cntlr.modelManager.load(path)
                if model is None:
                    raise ValueError(f"{name}: 読込に失敗しました")
                try:
                    for fact in model.facts:
                        if fact.isNil:
                            stats["nil"] += 1
                            continue
                        if fact.isNumeric:
                            stats["numeric"] += 1
                            continue
                        concept = fact.concept
                        if concept is None or fact.value is None:
                            stats["no_value"] += 1
                            continue
                        start, end = (None, None)
                        dimension = ""
                        if fact.context is not None:
                            start, end = _period_of(fact.context)
                            dimension = _dimension_string(fact.context,
                                                          uri2prefix)
                        facts.append({
                            "source_kind": EDINET_SOURCE_KIND,
                            "source_id": doc_id,
                            "source_doc_id": doc_id,
                            "edinet_code": edinet_code,
                            "submit_at": submit_at,
                            "element": (f"{normalize_prefix(concept.qname.namespaceURI)}"
                                        f":{concept.qname.localName}"),
                            "element_ns": concept.qname.namespaceURI,
                            "raw_text": fact.value,
                            "context_start": start,
                            "context_end": end,
                            "dimension": dimension,
                            "certainty": CERTAINTY_CONFIRMED,
                            "source_sha256": zip_sha256,
                            "redistribution": REDISTRIBUTION_ALLOWED,
                        })
                        stats["non_numeric"] += 1
                finally:
                    cntlr.modelManager.close(model)
    return facts, stats, zip_sha256


def _download_one(repo_id, filename, dest):
    import shutil
    got = _with_retry(
        "hf_hub_download",
        lambda: hf_hub_download(repo_id=repo_id, filename=filename,
                                repo_type="dataset", local_dir=None))
    shutil.copyfile(got, dest)
    return dest


def download_master(repo, work_dir):
    """master/edinet_codes.parquet を HF から取得する。"""
    master_dir = os.path.join(work_dir, "master_in")
    os.makedirs(master_dir, exist_ok=True)
    _download_one(repo, f"{MASTER_PREFIX}/edinet_codes.parquet",
                  os.path.join(master_dir, "edinet_codes.parquet"))
    return master_dir


def run_download(repo, doc_id, submit_date, zip_out, master_dir):
    """提出 zip と会社マスタを取得し、実体ファイルとして配置する。"""
    year, month, day = submit_date.split("-")
    zip_dir = os.path.dirname(zip_out)
    if zip_dir:
        os.makedirs(zip_dir, exist_ok=True)
    os.makedirs(master_dir, exist_ok=True)
    _download_one(repo,
                  f"raw/edinet/{year}/{month}/{day}/zip/{doc_id}.zip",
                  zip_out)
    master_path = os.path.join(master_dir, "edinet_codes.parquet")
    _download_one(repo, f"{MASTER_PREFIX}/edinet_codes.parquet",
                  master_path)
    return {"zip": zip_out, "master": master_path}


def write_outputs(items, observations, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(items, schema=ITEMS_SCHEMA),
                   os.path.join(out_dir, f"{ITEMS_NAME}.parquet"))
    pq.write_table(
        pa.Table.from_pylist(observations, schema=OBSERVATIONS_SCHEMA),
        os.path.join(out_dir, f"{OBSERVATIONS_NAME}.parquet"))


def upload_outputs(out_dir, repo):
    api = HfApi()
    uploaded = []
    for name in (f"{ITEMS_NAME}.parquet", f"{OBSERVATIONS_NAME}.parquet",
                 SUMMARY_NAME):
        local = os.path.join(out_dir, name)
        _with_retry("upload_file", lambda local=local, name=name:
                    api.upload_file(
                        path_or_fileobj=local,
                        path_in_repo=f"{TEXT_PREFIX}/{name}",
                        repo_id=repo, repo_type="dataset",
                        commit_message=f"text: update text layer ({name})"))
        uploaded.append(f"{TEXT_PREFIX}/{name}")
    return uploaded


def run_build(work_dir, repo=DEFAULT_REPO, facts_paths=None, master_dir=None,
              upload=False):
    """文字事実レコードから text/ の成果物を構築する。"""
    generated_at = _now()
    if master_dir is None:
        master_dir = download_master(repo, work_dir)
    facts = load_facts(facts_paths or [])
    code_to_id = load_code_to_id(master_dir)
    items, stats = build_items(facts, code_to_id, generated_at)
    observations = build_observations(items, generated_at)
    out_dir = os.path.join(work_dir, "text_out")
    write_outputs(items, observations, out_dir)
    summary = {
        "generated_at": generated_at,
        "facts_in": stats["facts_in"],
        "duplicates": stats["duplicates"],
        "empty_text": stats["empty_text"],
        "unresolved_codes": stats["unresolved_codes"],
        "items_total": stats["items_total"],
        "items_by_category": dict(stats["by_category"]),
        "observations_total": len(observations),
        "observations_unconfirmed": sum(
            1 for o in observations
            if o["certainty"] == CERTAINTY_UNCONFIRMED),
        "documents": len({i["source_doc_id"] for i in items}),
    }
    with open(os.path.join(out_dir, SUMMARY_NAME), "w",
              encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    uploaded = upload_outputs(out_dir, repo) if upload else []
    return {"out_dir": out_dir, "uploaded": uploaded, "summary": summary}


def run_extract(zip_path, doc_id, edinet_code, submit_at, out_path,
                webcache_dir):
    """1書類の zip から文字事実レコード (JSONL) を作る。"""
    facts, stats, zip_sha256 = extract_text_facts(
        zip_path, doc_id, edinet_code, submit_at, webcache_dir)
    with open(out_path, "w", encoding="utf-8") as f:
        for fact in facts:
            f.write(json.dumps(fact, ensure_ascii=False) + "\n")
    return {"out": out_path, "zip_sha256": zip_sha256, "stats": stats,
            "facts": len(facts)}


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="テキスト層の器 (P2-T7、仕様 #15 §6)")
    sub = ap.add_subparsers(dest="command", required=True)

    ex = sub.add_parser("extract", help="提出 zip から文字事実を作る")
    ex.add_argument("--zip", required=True, help="提出 zip のパス")
    ex.add_argument("--doc-id", required=True, help="源泉側の書類 ID")
    ex.add_argument("--edinet-code", help="提出者の EDINET コード")
    ex.add_argument("--submit-at", help="提出日時")
    ex.add_argument("--out", required=True, help="出力 JSONL のパス")
    ex.add_argument("--webcache-dir",
                    default=os.path.join(tempfile.gettempdir(),
                                         "text-layer-webcache"),
                    help="arelle のタクソノミキャッシュの置き場所")

    dl = sub.add_parser("download", help="提出 zip と会社マスタを取得する")
    dl.add_argument("--repo", default=DEFAULT_REPO)
    dl.add_argument("--doc-id", required=True, help="源泉側の書類 ID")
    dl.add_argument("--submit-date", required=True,
                    help="提出日 (raw/ の日付パス用、YYYY-MM-DD)")
    dl.add_argument("--zip-out", required=True, help="提出 zip の配置先")
    dl.add_argument("--master-dir", required=True,
                    help="edinet_codes.parquet の配置先ディレクトリ")

    bu = sub.add_parser("build", help="文字事実から成果物を構築する")
    bu.add_argument("--work-dir", required=True, help="作業ディレクトリ")
    bu.add_argument("--repo", default=DEFAULT_REPO)
    bu.add_argument("--facts", action="append", default=[],
                    help="文字事実レコード (JSONL)。複数指定可")
    bu.add_argument("--master-dir",
                    help="edinet_codes.parquet を含むローカルディレクトリ")
    bu.add_argument("--upload", action="store_true",
                    help="成果物を v2 データセットへ上げる")

    args = ap.parse_args(argv)
    if args.command == "extract":
        result = run_extract(args.zip, args.doc_id, args.edinet_code,
                             args.submit_at, args.out, args.webcache_dir)
        json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    elif args.command == "download":
        result = run_download(args.repo, args.doc_id, args.submit_date,
                              args.zip_out, args.master_dir)
        json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    else:
        result = run_build(args.work_dir, repo=args.repo,
                           facts_paths=args.facts,
                           master_dir=args.master_dir, upload=args.upload)
        json.dump(result["summary"], sys.stdout, ensure_ascii=False,
                  indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
