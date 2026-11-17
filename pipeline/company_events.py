"""会社イベントと親子関係 (P2-T4、仕様 #15 §5)。

臨時報告書の提出事由 (府令の条項) をイベント種別へ対応づけて
会社イベントとして取り込み、親会社等状況報告書のメタデータから
親子関係を時点つきで記録する。成果物は v2 データセット
(Yoshi-Dai/financial-lakehouse-v2) の master/ 配下に置く。

master/ への追加分 (本モジュールが正本):
- company_events.parquet: 会社イベント。行は (源泉書類, 提出事由の
  原子) 単位。種別・開示日時・関係先・源泉書類を持ち、全行に出所
  (源泉の種別・源泉側の識別子・生成日時・再配布可否) を持たせる。
- company_relationships.parquet: 親子関係。行は (親会社, 子会社)
  単位で、初回・最終の観測日と観測回数を持つ (時点つきの記録)。
- company_events_summary.json: 構築のサマリと照合結果。

イベント種別の決め方 (実測に基づく切り分け):
- 取れる信号: 書類メタデータの current_report_reason (提出事由)。
  臨時報告書の提出事由は、企業内容等の開示に関する内閣府令
  第19条第2項 (内国会社) と特定有価証券の内容等の開示に関する
  内閣府令第29条第2項 (特定有価証券) の各号として EDINET が
  構造化して返す。本モジュールの対応表は e-Gov で公表されている
  両府令の現行条文 (2026-10-05 施行の改正まで反映された版) の
  各号の文面から作った。観測された原子のうち、この対応表で
  分類できないのは「第19条の2」(外国会社等に係る別条) のみで、
  未分類 (unclassified) として原子の原値を保持する。
- 取れない信号: イベントの相手先の同定、実際の異動・効力発生日、
  株式併合の割合は、書類本文 (テキストブロック) の自由文の中に
  あり、様式は決まっていても記載の形式が一定しないため、
  決定的には抽出できない。器には related_*、event_date、
  split_ratio、effective_date の列を用意し、本チケットでは
  null のまま残す。構造化抽出はテキスト層の本格実装 (別工程)
  の範囲とする。株式分割には府令に独立の号が無く (株主総会の
  決議事項として第9号の2の本文に埋もれる)、EDINET の範囲では
  系統的な源泉が無いため、種別 stock_split は予約のみとする。
  株主優待の新設・変更・廃止も同様に予約のみ (主たる源泉は
  TDnet で、取り込みは TDnet フェーズ)。
- イベントの日付は開示日 (提出日時) とする。本文中の実際の
  発生日は上記のとおり決定的に取れないため、開示日を「日付」と
  し、event_date (本文由来の発生日) と列を分けて区別する。
- 訂正臨時報告書 (doc_type=190) は新たなイベントとして数えず、
  原書類のイベントに一本化する (訂正の存在はサマリに数える)。
  親会社等状況報告書の訂正 (doc_type=210) も同様で、実測では
  全件が原書類と同じ子会社コードを指していたため、観測として
  二重に数えない。

親子関係の源泉と範囲:
- 親会社等状況報告書 (v1 索引の doc_type=200): 提出者が親会社等、
  subsidiary_edinet_code が上場子会社を指す。841件 (訂正含む)
  の観測があり、本チケットではこの源泉だけを関係として取り込む。
- 書類メタデータの issuer / subject コードは、実測では大量保有
  報告書系 (issuer) と公開買付け関係書類系 (subject) にしか
  現れない。これらの書類の取り込みは後続チケットの範囲であり、
  関係の器への投入もそのチケットに合わせる。有報の関係会社の
  状況はテキストであり、テキスト層 (P2-T7) の範囲。
- 臨時報告書の第3号 (親会社・特定子会社の異動) はイベントとして
  取り込むが、相手先は本文の名称のみで EDINET コードが無いため、
  関係の辺としては記録しない。

doc_type についての注意: 入力の v1 索引 (ARIA-3 の
documents_index) の doc_type は API の docTypeCode と一部で
異なる独自番号であり、タイトルで実測して対応を確認した
(180=臨時報告書は API と同じ。親会社等状況報告書は v1 で 200、
API では 300)。本モジュールは v1 索引を入力とするため v1 の
番号を使う。

対象範囲は第1期 Silver の範囲 (提出日 2021-11-01 以降) とする
(仕様 §7 の Gold の初期対象期間と同じ線引き)。

使い方:
  python -m pipeline.company_events build --work-dir DIR [--upload]
build は --index (v1 索引 parquet) と --master-dir
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
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download

from .hf_sync import _with_retry

DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"
V1_REPO = "Yoshi-Dai/financial-lakehouse"
V1_INDEX_PATH = "catalog/documents_index.parquet"
MASTER_PREFIX = "master"
EVENTS_NAME = "company_events"
RELATIONSHIPS_NAME = "company_relationships"
SUMMARY_NAME = "company_events_summary.json"

V1_SOURCE_KIND = "v1_index"
REDISTRIBUTION_ALLOWED = "allowed"

DEFAULT_SINCE = "2021-11-01"

# v1 索引の doc_type (タイトルで実測した対応。モジュールの docstring 参照)
DOC_TYPE_EXTRAORDINARY = "180"
DOC_TYPE_EXTRAORDINARY_AMENDMENT = "190"
DOC_TYPE_PARENT_REPORT = "200"
DOC_TYPE_PARENT_REPORT_AMENDMENT = "210"

ORDINANCE_CORPORATE = "企業内容等の開示に関する内閣府令"
ORDINANCE_SPECIFIED = "特定有価証券の内容等の開示に関する内閣府令"

# 提出事由の原子 -> (イベント種別, 日本語ラベル)。
# 根拠は両府令の現行条文の各号の文面 (e-Gov、2026-11-17 取得)。
# キーは (条, 号) で、号は「9の2」の形に正規化したもの。
EVENT_KIND_BY_ITEM = {
    # 企業内容等の開示に関する内閣府令 第19条第2項
    ("19", "1"): ("securities_offering", "有価証券の募集・売出し"),
    ("19", "2"): ("securities_issuance_without_offering",
                  "募集によらない有価証券の発行"),
    ("19", "2の2"): ("share_solicitation", "株券等の取得勧誘"),
    ("19", "3"): ("parent_or_subsidiary_change", "親会社・特定子会社の異動"),
    ("19", "4"): ("major_shareholder_change", "主要株主の異動"),
    ("19", "4の2"): ("share_surrender_demand", "株式等売渡請求"),
    ("19", "4の3"): ("acquisition_of_callable_class_shares",
                     "全部取得条項付種類株式の取得"),
    ("19", "4の4"): ("share_consolidation", "株式の併合"),
    ("19", "5"): ("material_disaster", "重要な災害"),
    ("19", "6"): ("litigation", "訴訟の提起・解決"),
    ("19", "6の2"): ("share_exchange", "株式交換"),
    ("19", "6の3"): ("share_transfer", "株式移転"),
    ("19", "7"): ("absorption_type_split", "吸収分割"),
    ("19", "7の2"): ("incorporation_type_split", "新設分割"),
    ("19", "7の3"): ("absorption_type_merger", "吸収合併"),
    ("19", "7の4"): ("incorporation_type_merger", "新設合併"),
    ("19", "8"): ("business_transfer", "事業の譲渡・譲受け"),
    ("19", "8の2"): ("subsidiary_acquisition", "子会社取得"),
    ("19", "9"): ("representative_change", "代表者の異動"),
    ("19", "9の2"): ("shareholders_meeting_resolution", "株主総会の決議"),
    ("19", "9の3"): ("resolution_amended_or_rejected",
                     "決議事項の修正・否決"),
    ("19", "9の4"): ("auditor_change", "監査公認会計士等の異動"),
    ("19", "10"): ("insolvency_proceedings",
                   "破産・再生・更生手続開始の申立て等"),
    ("19", "11"): ("debtor_default_risk", "債務者等の不渡り・破産等"),
    ("19", "12"): ("material_subsequent_event",
                   "財政状態等に著しい影響を与える事象"),
    ("19", "12の2"): ("shareholder_agreement_governance",
                      "株主との合意 (役員の選任等)"),
    ("19", "12の3"): ("shareholder_agreement_transfer",
                      "株主との合意 (株式の譲渡制限等)"),
    ("19", "12の4"): ("financial_covenant_execution",
                      "財務上の特約つき借入・社債"),
    ("19", "12の5"): ("financial_covenant_change_or_trigger",
                      "財務上の特約の変更・事由の発生"),
    ("19", "13"): ("subsidiary_material_disaster", "連結子会社の重要な災害"),
    ("19", "14"): ("subsidiary_litigation", "連結子会社への訴訟"),
    ("19", "14の2"): ("subsidiary_share_exchange", "連結子会社の株式交換"),
    ("19", "14の3"): ("subsidiary_share_transfer", "連結子会社の株式移転"),
    ("19", "15"): ("subsidiary_absorption_type_split",
                   "連結子会社の吸収分割"),
    ("19", "15の2"): ("subsidiary_incorporation_type_split",
                      "連結子会社の新設分割"),
    ("19", "15の3"): ("subsidiary_absorption_type_merger",
                      "連結子会社の吸収合併"),
    ("19", "15の4"): ("subsidiary_incorporation_type_merger",
                      "連結子会社の新設合併"),
    ("19", "16"): ("subsidiary_business_transfer",
                   "連結子会社の事業の譲渡・譲受け"),
    ("19", "16の2"): ("grandchild_acquisition", "連結子会社による子会社取得"),
    ("19", "17"): ("subsidiary_insolvency_proceedings",
                   "連結子会社の破産・再生・更生手続開始の申立て等"),
    ("19", "18"): ("subsidiary_debtor_default_risk",
                   "連結子会社の債務者等の不渡り・破産等"),
    ("19", "19"): ("material_subsequent_event_consolidated",
                   "連結の財政状態等に著しい影響を与える事象"),
    ("19", "20"): ("subsidiary_financial_covenant_execution",
                   "連結子会社の財務上の特約つき借入・社債"),
    ("19", "21"): ("subsidiary_financial_covenant_change_or_trigger",
                   "連結子会社の財務上の特約の変更・事由の発生"),
    # 特定有価証券の内容等の開示に関する内閣府令 第29条第2項
    ("29", "1"): ("specified_securities_offering",
                  "特定有価証券の募集・売出し"),
    ("29", "2"): ("fund_key_party_change", "ファンド等の主要な関係法人の異動"),
    ("29", "3"): ("fund_policy_change", "ファンドの運用方針等の変更"),
    ("29", "4"): ("fund_calculation_period_end", "特定期間の満了"),
    ("29", "5"): ("fund_material_disaster", "ファンド等の重要な災害"),
    ("29", "6"): ("fund_litigation", "ファンド等への訴訟"),
    ("29", "7"): ("investment_corporation_absorption_merger",
                  "投資法人の吸収合併"),
    ("29", "8"): ("investment_corporation_incorporation_merger",
                  "投資法人の新設合併"),
    ("29", "9"): ("fund_merger", "ファンドの併合"),
    ("29", "10"): ("fund_insolvency_proceedings",
                   "発行者等の破産・再生・更生手続開始の申立て等"),
    ("29", "11"): ("fund_debtor_default_risk", "債務者等の不渡り・破産等"),
    ("29", "12"): ("fund_material_subsequent_event",
                   "ファンド等の財政状態等に著しい影響を与える事象"),
    ("29", "13"): ("fund_administrative_disposition", "行政処分等"),
    ("29", "14"): ("fund_dissolution", "解散・信託の終了"),
}

# 予約のみの種別 (本チケットでは生成しない)。
# - stock_split: 株式分割には府令に独立の号が無く、EDINET の範囲では
#   系統的な源泉が無い (仕様 §5 の「比率と効力発生日」の構造化は
#   テキスト層の本格実装で扱う)。
# - shareholder_benefit_*: 株主優待の主たる源泉は TDnet で、
#   取り込みは TDnet フェーズ (仕様 §5)。
RESERVED_EVENT_KINDS = {
    "stock_split": "株式分割",
    "shareholder_benefit_established": "株主優待の新設",
    "shareholder_benefit_changed": "株主優待の変更",
    "shareholder_benefit_abolished": "株主優待の廃止",
}

UNCLASSIFIED_KIND = "unclassified"
UNCLASSIFIED_LABEL = "未分類"

RELATION_KIND_PARENT_SUBSIDIARY = "parent_subsidiary"

EVENT_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("internal_id", pa.int64()),
    ("edinet_code", pa.string()),
    ("event_kind", pa.string()),
    ("event_kind_label", pa.string()),
    ("ordinance", pa.string()),
    ("reason_atom", pa.string()),
    ("reason_raw", pa.string()),
    ("disclosed_at", pa.string()),
    ("disclosure_date", pa.string()),
    ("event_date", pa.string()),
    ("related_edinet_code", pa.string()),
    ("related_internal_id", pa.int64()),
    ("split_ratio", pa.string()),
    ("effective_date", pa.string()),
    ("source_doc_id", pa.string()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("source_sha256", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

RELATIONSHIP_SCHEMA = pa.schema([
    ("parent_internal_id", pa.int64()),
    ("parent_edinet_code", pa.string()),
    ("subsidiary_internal_id", pa.int64()),
    ("subsidiary_edinet_code", pa.string()),
    ("relation_kind", pa.string()),
    ("first_observed_date", pa.string()),
    ("last_observed_date", pa.string()),
    ("observation_count", pa.int64()),
    ("first_source_doc_id", pa.string()),
    ("last_source_doc_id", pa.string()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("source_sha256", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

_ATOM_RE = re.compile(r"^第(\d+)条第2項第(\d+)号(?:の(\d+))?$")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _provenance(generated_at, source_sha256):
    return {
        "source_kind": V1_SOURCE_KIND,
        "source_id": f"{V1_REPO}:{V1_INDEX_PATH}",
        "source_sha256": source_sha256,
        "generated_at": generated_at,
        "redistribution": REDISTRIBUTION_ALLOWED,
    }


def parse_reason_atoms(reason_raw):
    """提出事由の文字列を原子 (各号) の列へ分解する。"""
    if not reason_raw:
        return []
    return [a.strip() for a in reason_raw.split(",") if a.strip()]


def classify_atom(atom):
    """原子を (府令名, 条, 号, 種別, ラベル) へ対応づける。

    対応表に無い原子 (条文の新設・別条の事由) は None を返す。
    呼び出し側は未分類として原子の原値を保持する。
    """
    m = _ATOM_RE.match(atom or "")
    if not m:
        return None
    article = m.group(1)
    item = m.group(2) + (f"の{m.group(3)}" if m.group(3) else "")
    found = EVENT_KIND_BY_ITEM.get((article, item))
    if found is None:
        return None
    ordinance = (ORDINANCE_CORPORATE if article == "19"
                 else ORDINANCE_SPECIFIED if article == "29" else None)
    if ordinance is None:
        return None
    kind, label = found
    return {"ordinance": ordinance, "article": article, "item": item,
            "event_kind": kind, "event_kind_label": label}


def build_events(rows, code_to_id, since, generated_at, source_sha256):
    """v1 索引の行から会社イベントを作る。

    rows は doc_type / submit_at / edinet_code / doc_id /
    current_report_reason を持つ辞書の列。対象は臨時報告書
    (doc_type=180) で提出日が since 以降のもの。
    """
    prov = _provenance(generated_at, source_sha256)
    events = []
    stats = {
        "extraordinary_filings": 0,
        "amendment_filings_excluded": 0,
        "filings_without_reason": 0,
        "unresolved_filer_codes": 0,
        "unmapped_atoms": collections.Counter(),
        "events_by_kind": collections.Counter(),
    }
    for row in rows:
        submit_at = row.get("submit_at") or ""
        if submit_at[:10] < since:
            continue
        if row.get("doc_type") == DOC_TYPE_EXTRAORDINARY_AMENDMENT:
            stats["amendment_filings_excluded"] += 1
            continue
        if row.get("doc_type") != DOC_TYPE_EXTRAORDINARY:
            continue
        stats["extraordinary_filings"] += 1
        code = row.get("edinet_code")
        internal_id = code_to_id.get(code)
        if internal_id is None:
            stats["unresolved_filer_codes"] += 1
        reason_raw = row.get("current_report_reason") or None
        atoms = parse_reason_atoms(reason_raw)
        if not atoms:
            stats["filings_without_reason"] += 1
            atoms = [None]
        for seq, atom in enumerate(atoms, start=1):
            classified = classify_atom(atom) if atom else None
            if atom and classified is None:
                stats["unmapped_atoms"][atom] += 1
            kind = (classified["event_kind"] if classified
                    else UNCLASSIFIED_KIND)
            stats["events_by_kind"][kind] += 1
            events.append({
                "event_id": f"{row['doc_id']}#{seq}",
                "internal_id": internal_id,
                "edinet_code": code,
                "event_kind": kind,
                "event_kind_label": (classified["event_kind_label"]
                                     if classified else UNCLASSIFIED_LABEL),
                "ordinance": (classified["ordinance"] if classified
                              else None),
                "reason_atom": atom,
                "reason_raw": reason_raw,
                "disclosed_at": submit_at,
                "disclosure_date": submit_at[:10],
                "event_date": None,
                "related_edinet_code": None,
                "related_internal_id": None,
                "split_ratio": None,
                "effective_date": None,
                "source_doc_id": row["doc_id"],
                **prov,
            })
    events.sort(key=lambda e: (e["disclosure_date"], e["disclosed_at"],
                               e["event_id"]))
    stats["events_total"] = len(events)
    return events, stats


def build_relationships(rows, code_to_id, since, generated_at,
                        source_sha256):
    """親会社等状況報告書の観測から親子関係を集約する。

    対象は doc_type=200 で提出日が since 以降のもの。訂正
    (doc_type=210) は観測として数えず、件数だけをサマリに残す。
    """
    prov = _provenance(generated_at, source_sha256)
    pairs = {}
    stats = {
        "parent_report_filings": 0,
        "parent_report_amendments_excluded": 0,
        "self_pairs": 0,
        "unresolved_codes": 0,
    }
    ordered = sorted(
        (r for r in rows if (r.get("submit_at") or "")[:10] >= since),
        key=lambda r: (r.get("submit_at") or "", r.get("doc_id") or ""))
    for row in ordered:
        if row.get("doc_type") == DOC_TYPE_PARENT_REPORT_AMENDMENT:
            stats["parent_report_amendments_excluded"] += 1
            continue
        if row.get("doc_type") != DOC_TYPE_PARENT_REPORT:
            continue
        subsidiary = row.get("subsidiary_edinet_code")
        if not subsidiary:
            continue
        stats["parent_report_filings"] += 1
        parent = row.get("edinet_code")
        date = (row.get("submit_at") or "")[:10]
        key = (parent, subsidiary)
        entry = pairs.get(key)
        if entry is None:
            entry = {
                "parent_internal_id": code_to_id.get(parent),
                "parent_edinet_code": parent,
                "subsidiary_internal_id": code_to_id.get(subsidiary),
                "subsidiary_edinet_code": subsidiary,
                "relation_kind": RELATION_KIND_PARENT_SUBSIDIARY,
                "first_observed_date": date,
                "last_observed_date": date,
                "observation_count": 0,
                "first_source_doc_id": row["doc_id"],
                "last_source_doc_id": row["doc_id"],
                **prov,
            }
            pairs[key] = entry
            if parent == subsidiary:
                stats["self_pairs"] += 1
            if (entry["parent_internal_id"] is None
                    or entry["subsidiary_internal_id"] is None):
                stats["unresolved_codes"] += 1
        entry["last_observed_date"] = date
        entry["last_source_doc_id"] = row["doc_id"]
        entry["observation_count"] += 1
    relationships = sorted(
        pairs.values(),
        key=lambda e: (e["parent_edinet_code"], e["subsidiary_edinet_code"]))
    stats["relationship_pairs"] = len(relationships)
    return relationships, stats


def load_index_rows(index_path):
    """v1 索引から必要な列だけを読む。"""
    columns = ["doc_id", "edinet_code", "submit_at", "doc_type",
               "current_report_reason", "subsidiary_edinet_code",
               "parent_doc_id"]
    table = pq.read_table(index_path, columns=columns)
    return table.to_pylist()


def load_code_to_id(master_dir):
    """master/edinet_codes.parquet からコード -> 内部 ID を作る。"""
    table = pq.read_table(os.path.join(master_dir, "edinet_codes.parquet"))
    rows = table.to_pylist()
    return {r["edinet_code"]: r["internal_id"] for r in rows}


def _download_one(repo_id, filename, dest):
    import shutil
    got = _with_retry(
        "hf_hub_download",
        lambda: hf_hub_download(repo_id=repo_id, filename=filename,
                                repo_type="dataset", local_dir=None,
                                token=None))
    shutil.copyfile(got, dest)
    return dest


def download_inputs(repo, work_dir):
    """v1 索引と master/edinet_codes.parquet を HF から取得する。"""
    index_dir = os.path.join(work_dir, "v1")
    os.makedirs(index_dir, exist_ok=True)
    index_path = _download_one(
        V1_REPO, V1_INDEX_PATH,
        os.path.join(index_dir, "documents_index.parquet"))
    master_dir = os.path.join(work_dir, "master_in")
    os.makedirs(master_dir, exist_ok=True)
    _download_one(repo, f"{MASTER_PREFIX}/edinet_codes.parquet",
                  os.path.join(master_dir, "edinet_codes.parquet"))
    return index_path, master_dir


def write_outputs(events, relationships, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(events, schema=EVENT_SCHEMA),
                   os.path.join(out_dir, f"{EVENTS_NAME}.parquet"))
    pq.write_table(
        pa.Table.from_pylist(relationships, schema=RELATIONSHIP_SCHEMA),
        os.path.join(out_dir, f"{RELATIONSHIPS_NAME}.parquet"))


def upload_outputs(out_dir, repo):
    api = HfApi()
    uploaded = []
    for name in (f"{EVENTS_NAME}.parquet", f"{RELATIONSHIPS_NAME}.parquet",
                 SUMMARY_NAME):
        local = os.path.join(out_dir, name)
        _with_retry("upload_file", lambda local=local, name=name:
                    api.upload_file(
                        path_or_fileobj=local,
                        path_in_repo=f"{MASTER_PREFIX}/{name}",
                        repo_id=repo, repo_type="dataset",
                        commit_message=f"master: update company events ({name})"))
        uploaded.append(f"{MASTER_PREFIX}/{name}")
    return uploaded


def run_build(work_dir, repo=DEFAULT_REPO, index_path=None, master_dir=None,
              since=DEFAULT_SINCE, upload=False):
    """初期構築: v1 索引から会社イベントと親子関係を作る。"""
    generated_at = _now()
    if index_path is None or master_dir is None:
        dl_index, dl_master = download_inputs(repo, work_dir)
        index_path = index_path or dl_index
        master_dir = master_dir or dl_master
    with open(index_path, "rb") as f:
        source_sha256 = hashlib.sha256(f.read()).hexdigest()
    rows = load_index_rows(index_path)
    code_to_id = load_code_to_id(master_dir)
    events, event_stats = build_events(rows, code_to_id, since,
                                       generated_at, source_sha256)
    relationships, rel_stats = build_relationships(
        rows, code_to_id, since, generated_at, source_sha256)
    out_dir = os.path.join(work_dir, "master_out")
    write_outputs(events, relationships, out_dir)
    summary = {
        "generated_at": generated_at,
        "window_since": since,
        "index_rows_total": len(rows),
        "index_sha256": source_sha256,
        "events": {
            "extraordinary_filings":
                event_stats["extraordinary_filings"],
            "amendment_filings_excluded":
                event_stats["amendment_filings_excluded"],
            "filings_without_reason":
                event_stats["filings_without_reason"],
            "unresolved_filer_codes":
                event_stats["unresolved_filer_codes"],
            "unmapped_atoms": dict(event_stats["unmapped_atoms"]),
            "events_total": event_stats["events_total"],
            "events_by_kind": dict(event_stats["events_by_kind"]),
        },
        "relationships": rel_stats,
        "reserved_event_kinds": RESERVED_EVENT_KINDS,
    }
    with open(os.path.join(out_dir, SUMMARY_NAME), "w",
              encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    uploaded = upload_outputs(out_dir, repo) if upload else []
    return {"out_dir": out_dir, "uploaded": uploaded, "summary": summary}


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="会社イベントと親子関係 (P2-T4、仕様 #15 §5)")
    ap.add_argument("command", choices=["build"])
    ap.add_argument("--work-dir", required=True, help="作業ディレクトリ")
    ap.add_argument("--repo", default=DEFAULT_REPO)
    ap.add_argument("--index", help="v1 索引 parquet")
    ap.add_argument("--master-dir",
                    help="edinet_codes.parquet を含むローカルディレクトリ")
    ap.add_argument("--since", default=DEFAULT_SINCE,
                    help="対象の開始日 (提出日、YYYY-MM-DD)")
    ap.add_argument("--upload", action="store_true",
                    help="成果物を v2 データセットへ上げる")
    args = ap.parse_args(argv)
    result = run_build(args.work_dir, repo=args.repo,
                       index_path=args.index, master_dir=args.master_dir,
                       since=args.since, upload=args.upload)
    json.dump(result["summary"], sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
