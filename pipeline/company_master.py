"""会社マスタと銘柄レイヤー (P2-T3、仕様 #15 §1・§4)。

v1 からの再構成 (P2-T2、pipeline.reconstruct_v1) の初期履歴と、EDINET
コードリストの日次スナップショット (S2-T5、pipeline.codelist) を入力に、
v2 データセット (Yoshi-Dai/financial-lakehouse-v2) の master/ 配下へ
会社マスタを構築し、日次スナップショットで更新する。

master/ の構成 (本モジュールが正本):
- history_from_v1/: P2-T2 の再構成成果 (identifier_history、
  code_roster、code_connections、review_items、summary.json) に、
  派生集計 last_submissions.parquet (コード×書類種別ごとの最終提出日)
  を加えたもの。place-history が配置する。
- companies.parquet: 会社 (内部 ID 単位) の台帳。status は現行
  スナップショットにコードが残る会社が active、残らない会社が absent
  (削除はしない)。disappearance_reason と successor_internal_id は
  会社イベント (P2-T4) で裏取りする器であり、未確定は null のまま残す。
- edinet_codes.parquet: EDINET コードと内部 ID の対応 (内部 ID の
  発行台帳を兼ねる)。assignment_basis は発行・接続の根拠
  (initial_v1 / jcn_strong / jcn_corroborated / new_in_codelist /
  initial_codelist)。filer_role は提出の立場 (コードリストの
  提出者種別の現行値)。
- identifier_history.parquet: 外部識別子と名称の有効期間つき履歴。
  v1 再構成の行 (source_kind=v1_index) とコードリストの観測行
  (source_kind=edinet_codelist_download) を源泉を区別したまま併記する。
  値が変われば前の行を閉じて新しい行を開き、同じ値が続けば
  last_observed_date と observation_count を伸ばす (変更日は検知日)。
- attribute_history.parquet: 属性 (所在地・業種・上場区分・資本金・
  決算日・連結の有無・提出者種別) の有効期間つき履歴。識別子と同じ
  開閉の規則で持つ。空文字は「記載なし」であり観測として記録しない。
- securities.parquet: 銘柄レイヤー。会社と銘柄は 1:N で、行は
  (内部 ID, 証券コード) 単位。種類は証券コードの予備コード (5文字目)
  で識別する (証券コード協議会の取扱い要領。0=普通株式、1=新株式、
  2=第二新株式、5・6=優先株式、3・4・7・8=その他の種類株式、
  9=新株予約権証券)。プレースホルダ 00000 は銘柄として数えず、
  識別子履歴にだけ観測として残す。先行ゼロの5桁コードはそのまま
  では種類を判定しない: 回転形 (先行ゼロを末尾へ移した形) が同一
  会社で観測されていれば表記ゆれとして回転形の銘柄へ観測を統合し、
  観測されていなければ種類を「未確認」として review_items に残す
  (security_kind_unverified)。is_primary は主銘柄 (現行
  スナップショットの証券コード。スナップショットに無い会社は
  最終観測日の最も新しいコード) で、primary_basis に根拠を持つ。
  コードリストの証券コードは提出者あたり1つだけなので、種類株の
  同時列挙は EDINET 書類側が第一源泉であり、本モジュールの範囲は
  器と主銘柄 (と履歴で観測されたコード) の登録までである。
- classifications.parquet: 分類体系 (業種・上場区分・提出者種別・
  連結の有無) の版つき参照データ。値ごとに初出日 (first_seen_date)
  と最終観測日を持ち、ある日付時点の版は、その日付を
  [first_seen_date, last_seen_date] に含む値の集合として再現できる。
  新しい値はスナップショットの適用時に差分で検知して登録する。
- review_items.parquet: 未確定のまま残す項目 (法人番号の接続が
  条件を満たさない組、名称のみの候補、コード内の法人番号の矛盾、
  プレースホルダ)。v1 再構成の項目を引き継ぎ、更新時にも追加する。
- last_submissions.parquet: 派生属性 (源泉の事実ではない) として、
  内部 ID × 書類種別ごとの最終提出日。初期値は history_from_v1 の
  集計から作り、日次の更新 (update) では書類目録 (S2-T5 の取得経路が
  v2 に積む catalog/) の集計で最終提出日と件数を更新する。目録は
  取得経路が既に保存したものを読むだけであり、二重取得はしない。
- state.json: 適用済みスナップショットの最終日と次の内部 ID。
  update の再開判定に使う。
- summary.json: 構築・更新のサマリと照合結果。

内部 ID の発行規則: 初期構築では、法人番号の接続でまとめた組と
単独コードを、代表コード (組の最小の EDINET コード) の昇順に
1 から連番で発行する。更新では新しいコードに既存の最大値+1 から
昇順に発行し、既存の対応は変えない。意味は持たせない。

法人番号による接続の規則 (仕様 §1 の精緻化。reconstruct_v1 と同旨
だが、再構成側は v1 索引に所在地の列が無いため名称のみで判定する):
共有する法人番号が両側で複数行観測され、かつ両側の多数派であれば
strong。多数派同士で、名称 (変更キー) または所在地が一致し、
証券コードに矛盾が無ければ corroborated。それ以外は接続せず
要確認に回す。多数派は「観測回数が最大の値が1つだけ」の場合に
限る (同数が複数ある場合は多数派なしとして扱い、接続しない。
reconstruct_v1 の従来実装は挿入順依存だったため、本チケットで
決定的な規則として定めた)。

使い方:
  python -m pipeline.company_master place-history --work-dir DIR [--upload]
  python -m pipeline.company_master build --work-dir DIR [--upload]
  python -m pipeline.company_master update --work-dir DIR [--upload]
build / update は --snapshots-dir (EdinetcodeDlInfo.csv を含む
ローカルディレクトリ) と --history-dir / --state-dir (ローカルの
入力ディレクトリ) を受け付け、HF を介さない検証実行ができる。
update は --catalog-dir (書類目録 JSONL のローカルディレクトリ) も
受け付ける。
認証は環境変数 HF_TOKEN (書き込み。huggingface_hub が自動で読む)。
読み取りは公開データセットなのでトークンなしでも動く。
"""
import argparse
import collections
import csv
import hashlib
import io
import json
import os
import re
import sys
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError

from .codelist import CODELIST_URL, EXPECTED_COLUMNS
from .hf_sync import _with_retry
from .reconstruct_v1 import change_key, normalize_name

DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"
V1_REPO = "Yoshi-Dai/financial-lakehouse"
V1_INDEX_PATH = "catalog/documents_index.parquet"
MASTER_PREFIX = "master"
HISTORY_PREFIX = "master/history_from_v1"

V1_SOURCE_KIND = "v1_index"
CODELIST_SOURCE_KIND = "edinet_codelist_download"
CODELIST_SOURCE_ID = CODELIST_URL
CATALOG_SOURCE_KIND = "edinet_api"
CATALOG_SOURCE_ID = f"{DEFAULT_REPO}:catalog/month=*/ledger.jsonl"
CATALOG_PREFIX = "catalog"
REDISTRIBUTION_ALLOWED = "allowed"

CODE_COLUMN = EXPECTED_COLUMNS[0]
NAME_COLUMN = "提出者名"
JCN_COLUMN = "提出者法人番号"
SEC_COLUMN = "証券コード"
ADDRESS_COLUMN = "所在地"

# コードリストの列 -> 識別子の種別 (値が変われば履歴の行を開閉する)
IDENTIFIER_COLUMNS = {
    "提出者名": "company_name",
    "提出者名（英字）": "company_name_en",
    "提出者名（ヨミ）": "company_name_kana",
    "証券コード": "securities_code",
    "提出者法人番号": "corporate_number",
}
NAME_KINDS = {"company_name", "company_name_en", "company_name_kana"}

# コードリストの列 -> 属性名
ATTRIBUTE_COLUMNS = {
    "提出者種別": "filer_type",
    "上場区分": "listing_status",
    "連結の有無": "has_consolidated",
    "資本金": "capital",
    "決算日": "fiscal_year_end",
    "所在地": "address",
    "提出者業種": "industry",
}

# コードリストの列 -> 分類体系名
CLASSIFICATION_COLUMNS = {
    "提出者業種": "industry",
    "上場区分": "listing_status",
    "提出者種別": "filer_type",
    "連結の有無": "has_consolidated",
}

# 証券コードの予備コード (5文字目) -> 種類 (証券コード協議会の取扱い要領)
SECURITY_KINDS = {
    "0": "普通株式", "1": "新株式", "2": "第二新株式",
    "3": "その他の種類株式", "4": "その他の種類株式",
    "5": "優先株式", "6": "優先株式",
    "7": "その他の種類株式", "8": "その他の種類株式",
    "9": "新株予約権証券",
}
PLACEHOLDER_SEC_CODE = "00000"
UNVERIFIED_KIND = "未確認"

COMPANIES_SCHEMA = pa.schema([
    ("internal_id", pa.int64()),
    ("status", pa.string()),
    ("disappearance_reason", pa.string()),
    ("successor_internal_id", pa.int64()),
    ("first_seen_date", pa.string()),
    ("last_seen_date", pa.string()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

EDINET_CODES_SCHEMA = pa.schema([
    ("internal_id", pa.int64()),
    ("edinet_code", pa.string()),
    ("assignment_basis", pa.string()),
    ("connected_from", pa.string()),
    ("filer_role", pa.string()),
    ("first_seen_date", pa.string()),
    ("last_seen_date", pa.string()),
    ("in_latest_snapshot", pa.bool_()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

IDENTIFIER_SCHEMA = pa.schema([
    ("internal_id", pa.int64()),
    ("edinet_code", pa.string()),
    ("kind", pa.string()),
    ("value", pa.string()),
    ("source_value", pa.string()),
    ("value_normalized", pa.string()),
    ("change_key", pa.string()),
    ("is_placeholder", pa.bool_()),
    ("first_observed_date", pa.string()),
    ("last_observed_date", pa.string()),
    ("observation_count", pa.int64()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("source_sha256", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

ATTRIBUTE_SCHEMA = pa.schema([
    ("internal_id", pa.int64()),
    ("edinet_code", pa.string()),
    ("attribute", pa.string()),
    ("value", pa.string()),
    ("first_observed_date", pa.string()),
    ("last_observed_date", pa.string()),
    ("observation_count", pa.int64()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

SECURITIES_SCHEMA = pa.schema([
    ("internal_id", pa.int64()),
    ("edinet_code", pa.string()),
    ("securities_code", pa.string()),
    ("security_kind", pa.string()),
    ("reserve_digit", pa.string()),
    ("is_primary", pa.bool_()),
    ("primary_basis", pa.string()),
    ("first_observed_date", pa.string()),
    ("last_observed_date", pa.string()),
    ("observation_count", pa.int64()),
    ("in_latest_snapshot", pa.bool_()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

CLASSIFICATIONS_SCHEMA = pa.schema([
    ("taxonomy", pa.string()),
    ("value", pa.string()),
    ("first_seen_date", pa.string()),
    ("last_seen_date", pa.string()),
    ("snapshot_count", pa.int64()),
    ("in_latest_snapshot", pa.bool_()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

REVIEW_SCHEMA = pa.schema([
    ("item_type", pa.string()),
    ("internal_id_a", pa.int64()),
    ("internal_id_b", pa.int64()),
    ("edinet_code_a", pa.string()),
    ("edinet_code_b", pa.string()),
    ("shared_value", pa.string()),
    ("detail", pa.string()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

LAST_SUBMISSIONS_SCHEMA = pa.schema([
    ("internal_id", pa.int64()),
    ("edinet_code", pa.string()),
    ("doc_type", pa.string()),
    ("last_submit_date", pa.string()),
    ("document_count", pa.int64()),
    ("is_derived", pa.bool_()),
    ("source_kind", pa.string()),
    ("source_id", pa.string()),
    ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])

TABLE_SCHEMAS = {
    "companies": COMPANIES_SCHEMA,
    "edinet_codes": EDINET_CODES_SCHEMA,
    "identifier_history": IDENTIFIER_SCHEMA,
    "attribute_history": ATTRIBUTE_SCHEMA,
    "securities": SECURITIES_SCHEMA,
    "classifications": CLASSIFICATIONS_SCHEMA,
    "review_items": REVIEW_SCHEMA,
    "last_submissions": LAST_SUBMISSIONS_SCHEMA,
}

_DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def security_kind(code):
    """証券コードから (種類, 予備コード) を返す。判定できなければ (不明, None)。"""
    if code and len(code) == 5 and code[4] in SECURITY_KINDS:
        return SECURITY_KINDS[code[4]], code[4]
    return "不明", None


def rotated_form(code):
    """先行ゼロの5桁コードの回転形 (先行ゼロを末尾へ移した形) を返す。

    対象はプレースホルダを除く5桁で先行ゼロを持つコードだけで、
    それ以外は None を返す。回転形が実在するかは呼び出し側が
    同一会社の観測集合で確認する。
    """
    if (code and len(code) == 5 and code[0] == "0"
            and code != PLACEHOLDER_SEC_CODE):
        return code[1:] + code[0]
    return None


def majority_value(counts):
    """観測回数が最大の値を返す。同数が複数あれば None (多数派なし)。

    多数派の判定を決定的にするための規則 (挿入順に依存しない)。
    同数の場合は「多数の観測を優先する」前提自体が成立しないため、
    多数派なしとして扱い、接続の根拠にはしない。
    """
    if not counts:
        return None
    top = max(counts.values())
    winners = [v for v, c in counts.items() if c == top]
    return winners[0] if len(winners) == 1 else None


def evaluate_jcn_link(counts_a, counts_b, shared_jcn, name_agree,
                      address_agree, sec_conflict):
    """法人番号の共有による接続の強さを返す (strong / corroborated / None)。

    counts_a / counts_b はコードごとの法人番号 -> 観測回数。
    規則はモジュールの docstring のとおり (仕様 §1 の精緻化)。
    """
    rows_a = counts_a.get(shared_jcn, 0)
    rows_b = counts_b.get(shared_jcn, 0)
    majority_a = majority_value(counts_a) == shared_jcn
    majority_b = majority_value(counts_b) == shared_jcn
    if rows_a >= 2 and rows_b >= 2 and majority_a and majority_b:
        return "strong"
    if (majority_a and majority_b and (name_agree or address_agree)
            and not sec_conflict):
        return "corroborated"
    return None


def parse_codelist_csv(csv_bytes):
    """スナップショットの CSV バイト列を (日付, 行のリスト) にする。

    日付は CSV の1行目にあるダウンロード実行日 (リスト自身の日付)。
    形式の検証 (見出しの一致) は pipeline.codelist が保存時に済ませて
    いるため、ここでは見出しだけを確認する。
    """
    try:
        text = csv_bytes.decode("cp932")
    except UnicodeDecodeError as e:
        raise ValueError(f"コードリスト CSV を cp932 で復号できません: {e}") from e
    records = list(csv.reader(io.StringIO(text)))
    if len(records) < 2:
        raise ValueError("コードリスト CSV の行数が不足しています")
    date_match = _DATE_RE.search(",".join(records[0]))
    if not date_match:
        raise ValueError("コードリスト CSV の1行目から実行日を読めません")
    date = (f"{int(date_match.group(1)):04d}-{int(date_match.group(2)):02d}-"
            f"{int(date_match.group(3)):02d}")
    if records[1] != EXPECTED_COLUMNS:
        raise ValueError(f"コードリスト CSV の見出しが想定と異なります: {records[1]}")
    rows = []
    for rec in records[2:]:
        if not rec or all(not cell.strip() for cell in rec):
            continue
        rows.append(dict(zip(EXPECTED_COLUMNS, rec)))
    return date, rows


def load_local_snapshots(snapshots_dir):
    """ローカルディレクトリの CSV 群を (日付, 行) の日付昇順リストにする。"""
    found = {}
    for name in sorted(os.listdir(snapshots_dir)):
        if not name.lower().endswith(".csv"):
            continue
        with open(os.path.join(snapshots_dir, name), "rb") as f:
            date, rows = parse_codelist_csv(f.read())
        if date in found:
            raise ValueError(f"同じ日付のスナップショットが複数あります: {date}")
        found[date] = rows
    return sorted(found.items())


def empty_state(generated_at):
    return {
        "meta": {"last_applied_snapshot_date": None, "next_internal_id": 1},
        "companies": [], "edinet_codes": [], "identifier_history": [],
        "attribute_history": [], "securities": [], "classifications": [],
        "review_items": [], "last_submissions": [],
        "generated_at": generated_at,
    }


def _provenance(source_kind, source_id, generated_at, sha256=None):
    return {
        "source_kind": source_kind, "source_id": source_id,
        "source_sha256": sha256, "generated_at": generated_at,
        "redistribution": REDISTRIBUTION_ALLOWED,
    }


def _v1_source_id(row):
    return f"{row['source_dataset']}:{row['source_file']}"


def state_from_history(history_dir, generated_at):
    """P2-T2 の再構成成果から初期状態を作る (スナップショット適用前)。

    内部 ID は、法人番号の接続 (code_connections) でまとめた組と
    単独コードに、代表コードの昇順で連番を発行する。
    """
    history = pq.read_table(
        os.path.join(history_dir, "identifier_history.parquet")).to_pylist()
    roster = pq.read_table(
        os.path.join(history_dir, "code_roster.parquet")).to_pylist()
    connections = pq.read_table(
        os.path.join(history_dir, "code_connections.parquet")).to_pylist()
    review = pq.read_table(
        os.path.join(history_dir, "review_items.parquet")).to_pylist()

    # 法人番号の接続による組 (union-find)
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for c in connections:
        ra, rb = find(c["edinet_code_a"]), find(c["edinet_code_b"])
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    groups = collections.defaultdict(list)
    for r in roster:
        groups[find(r["edinet_code"])].append(r["edinet_code"])
    representatives = sorted(min(codes) for codes in groups.values())
    group_of = {}
    for codes in groups.values():
        rep = min(codes)
        for code in codes:
            group_of[code] = rep
    internal_id_of = {rep: i + 1 for i, rep in enumerate(representatives)}

    conn_by_pair = {(c["edinet_code_a"], c["edinet_code_b"]): c
                    for c in connections}
    roster_by_code = {r["edinet_code"]: r for r in roster}

    state = empty_state(generated_at)
    state["meta"]["next_internal_id"] = len(representatives) + 1
    for rep in representatives:
        iid = internal_id_of[rep]
        members = sorted(c for c, g in group_of.items() if g == rep)
        state["companies"].append({
            "internal_id": iid, "status": "absent",
            "disappearance_reason": None, "successor_internal_id": None,
            "first_seen_date": min(roster_by_code[c]["first_observed_date"]
                                   for c in members),
            "last_seen_date": max(roster_by_code[c]["last_observed_date"]
                                  for c in members),
            **_provenance(V1_SOURCE_KIND, f"{V1_REPO}:{V1_INDEX_PATH}",
                          generated_at),
        })
        for code in members:
            basis, connected_from = "initial_v1", None
            if code != rep:
                linked = []
                for m in members:
                    if m == code:
                        continue
                    conn = conn_by_pair.get((code, m)) or \
                        conn_by_pair.get((m, code))
                    if conn is not None:
                        linked.append(conn)
                best = sorted(linked,
                              key=lambda c: 0 if c["strength"] == "strong" else 1)[0]
                basis = f"jcn_{best['strength']}"
                connected_from = (best["edinet_code_b"]
                                  if best["edinet_code_a"] == code
                                  else best["edinet_code_a"])
            state["edinet_codes"].append({
                "internal_id": iid, "edinet_code": code,
                "assignment_basis": basis, "connected_from": connected_from,
                "filer_role": None,
                "first_seen_date": roster_by_code[code]["first_observed_date"],
                "last_seen_date": roster_by_code[code]["last_observed_date"],
                "in_latest_snapshot": False,
                **_provenance(V1_SOURCE_KIND,
                              f"{V1_REPO}:{V1_INDEX_PATH}",
                              generated_at),
            })
    for h in history:
        state["identifier_history"].append({
            "internal_id": internal_id_of[group_of[h["edinet_code"]]],
            "edinet_code": h["edinet_code"], "kind": h["kind"],
            "value": h["value"], "source_value": h["source_value"],
            "value_normalized": h["value_normalized"],
            "change_key": h["change_key"],
            "is_placeholder": bool(h["is_placeholder"]),
            "first_observed_date": h["first_observed_date"],
            "last_observed_date": h["last_observed_date"],
            "observation_count": int(h["observation_count"]),
            **_provenance(V1_SOURCE_KIND, _v1_source_id(h), generated_at,
                          sha256=h["source_file_sha256"]),
        })
    observed_secs = collections.defaultdict(set)
    for h in history:
        if h["kind"] == "securities_code" and not h["is_placeholder"]:
            observed_secs[internal_id_of[group_of[h["edinet_code"]]]].add(
                h["value"])
    merged_secs = {}
    unverified_secs = []
    for h in history:
        if h["kind"] != "securities_code" or h["is_placeholder"]:
            continue
        iid = internal_id_of[group_of[h["edinet_code"]]]
        rot = rotated_form(h["value"])
        if rot is not None and rot in observed_secs[iid]:
            effective = rot
        else:
            effective = h["value"]
        row = merged_secs.get((iid, effective))
        if row is None:
            if rot is not None and effective == h["value"]:
                kind_label, digit = UNVERIFIED_KIND, None
                unverified_secs.append((iid, h))
            else:
                kind_label, digit = security_kind(effective)
            merged_secs[(iid, effective)] = {
                "internal_id": iid, "edinet_code": h["edinet_code"],
                "securities_code": effective, "security_kind": kind_label,
                "reserve_digit": digit, "is_primary": False,
                "primary_basis": None,
                "first_observed_date": h["first_observed_date"],
                "last_observed_date": h["last_observed_date"],
                "observation_count": int(h["observation_count"]),
                "in_latest_snapshot": False,
                **_provenance(V1_SOURCE_KIND, _v1_source_id(h), generated_at),
            }
        else:
            row["observation_count"] += int(h["observation_count"])
            row["first_observed_date"] = min(row["first_observed_date"],
                                             h["first_observed_date"])
            row["last_observed_date"] = max(row["last_observed_date"],
                                            h["last_observed_date"])
            if h["value"] == effective:
                row["edinet_code"] = h["edinet_code"]
    state["securities"].extend(merged_secs.values())
    for r in review:
        a, b = r["edinet_code_a"], r["edinet_code_b"]
        state["review_items"].append({
            "item_type": r["item_type"],
            "internal_id_a": internal_id_of[group_of[a]] if a else None,
            "internal_id_b": internal_id_of[group_of[b]] if b else None,
            "edinet_code_a": a, "edinet_code_b": b,
            "shared_value": r["shared_value"], "detail": r["detail"],
            **_provenance(V1_SOURCE_KIND, _v1_source_id(r), generated_at),
        })
    for iid, h in unverified_secs:
        state["review_items"].append({
            "item_type": "security_kind_unverified",
            "internal_id_a": iid, "internal_id_b": None,
            "edinet_code_a": h["edinet_code"], "edinet_code_b": None,
            "shared_value": h["value"],
            "detail": (f"証券コード {h['value']} は先行ゼロの5桁コードであり、"
                       f"回転形 {rotated_form(h['value'])} が同一会社で"
                       "観測されないため、種類を断定せず未確認として記録する。"
                       "裏取り (銘柄名・有報の種類株式の内容) は書類側の"
                       "取り込み後に行う。"),
            **_provenance(V1_SOURCE_KIND, _v1_source_id(h), generated_at),
        })
    last_sub_path = os.path.join(history_dir, "last_submissions.parquet")
    if os.path.exists(last_sub_path):
        for r in pq.read_table(last_sub_path).to_pylist():
            state["last_submissions"].append({
                "internal_id": internal_id_of[group_of[r["edinet_code"]]],
                "edinet_code": r["edinet_code"], "doc_type": r["doc_type"],
                "last_submit_date": r["last_submit_date"],
                "document_count": int(r["document_count"]),
                "is_derived": True,
                **_provenance(V1_SOURCE_KIND, _v1_source_id(r), generated_at),
            })
    return state


def _code_index(state):
    return {r["edinet_code"]: r for r in state["edinet_codes"]}


class _Indexes:
    """スナップショット適用中の索引 (表の線形走査を避けるため)。

    ident_open / attr_open は、キーごとの「現在開いている行」
    (最終観測日が最大の行) を指す。観測は日付順に適用されるため、
    開いている行は常に最後に触れた行でよい。
    """

    def __init__(self, state):
        self.code = _code_index(state)
        self.ident_open = {}
        for h in state["identifier_history"]:
            if h["source_kind"] != CODELIST_SOURCE_KIND:
                continue
            key = (h["internal_id"], h["edinet_code"], h["kind"])
            cur = self.ident_open.get(key)
            if cur is None or h["last_observed_date"] >= cur["last_observed_date"]:
                self.ident_open[key] = h
        self.attr_open = {}
        for a in state["attribute_history"]:
            key = (a["internal_id"], a["edinet_code"], a["attribute"])
            cur = self.attr_open.get(key)
            if cur is None or a["last_observed_date"] >= cur["last_observed_date"]:
                self.attr_open[key] = a
        self.sec = {(s["internal_id"], s["securities_code"]): s
                    for s in state["securities"]}
        self.jcn_counts = collections.defaultdict(collections.Counter)
        for h in state["identifier_history"]:
            if h["kind"] == "corporate_number":
                self.jcn_counts[h["edinet_code"]][h["value"]] += \
                    h["observation_count"]


def _latest_values(state, internal_id, kind):
    """会社の識別子種別ごとの最新値の集合 (最終観測日が最大の行の値)。"""
    rows = [h for h in state["identifier_history"]
            if h["internal_id"] == internal_id and h["kind"] == kind
            and not h["is_placeholder"]]
    if not rows:
        return set()
    latest = max(h["last_observed_date"] for h in rows)
    return {h["value"] for h in rows if h["last_observed_date"] == latest}


def _name_keys(state, internal_id):
    return {h["change_key"] for h in state["identifier_history"]
            if h["internal_id"] == internal_id and h["kind"] == "company_name"
            and h["change_key"]}


def _latest_address(state, internal_id):
    rows = [a for a in state["attribute_history"]
            if a["internal_id"] == internal_id and a["attribute"] == "address"]
    if not rows:
        return None
    latest = max(a["last_observed_date"] for a in rows)
    values = [a["value"] for a in rows if a["last_observed_date"] == latest]
    return values[0] if values else None


def _add_review(state, indexes, item_type, code_a, code_b, shared_value,
                detail, generated_at):
    idx = indexes.code if indexes is not None else _code_index(state)
    state["review_items"].append({
        "item_type": item_type,
        "internal_id_a": idx[code_a]["internal_id"] if code_a in idx else None,
        "internal_id_b": idx[code_b]["internal_id"] if code_b in idx else None,
        "edinet_code_a": code_a, "edinet_code_b": code_b,
        "shared_value": shared_value, "detail": detail,
        **_provenance(CODELIST_SOURCE_KIND, CODELIST_SOURCE_ID, generated_at),
    })


def _register_code(state, indexes, row_dict):
    state["edinet_codes"].append(row_dict)
    if indexes is not None:
        indexes.code[row_dict["edinet_code"]] = row_dict


def _assign_new_code(state, indexes, code, row, date, generated_at, basis):
    iid = state["meta"]["next_internal_id"]
    state["meta"]["next_internal_id"] += 1
    state["companies"].append({
        "internal_id": iid, "status": "active", "disappearance_reason": None,
        "successor_internal_id": None, "first_seen_date": date,
        "last_seen_date": date,
        **_provenance(CODELIST_SOURCE_KIND, CODELIST_SOURCE_ID, generated_at),
    })
    _register_code(state, indexes, {
        "internal_id": iid, "edinet_code": code, "assignment_basis": basis,
        "connected_from": None, "filer_role": row["提出者種別"] or None,
        "first_seen_date": date, "last_seen_date": date,
        "in_latest_snapshot": True,
        **_provenance(CODELIST_SOURCE_KIND, CODELIST_SOURCE_ID, generated_at),
    })
    return iid


def _resolve_new_code(state, indexes, code, row, date, generated_at):
    """スナップショットで初めて見るコードの内部 ID を決める。

    法人番号が既存の会社と共有され、接続の規則 (evaluate_jcn_link) を
    満たせばその会社へ接続する。満たさなければ新しい内部 ID を発行し、
    法人番号の共有自体は要確認として残す。法人番号で結べなくても
    名称 (変更キー) が一致する会社があれば候補として残す。
    """
    jcn = (row[JCN_COLUMN] or "").strip()
    if jcn:
        counts_by_code = {c: dict(cnt) for c, cnt in
                          indexes.jcn_counts.items()}
        candidates = sorted(c for c, counts in counts_by_code.items()
                            if jcn in counts and c != code)
        new_counts = {jcn: 1}
        new_name_keys = ({change_key(row[NAME_COLUMN])}
                         if row[NAME_COLUMN] else set())
        sec_value = (row[SEC_COLUMN] or "").strip()
        new_secs = ({sec_value} if sec_value
                    and sec_value != PLACEHOLDER_SEC_CODE else set())
        new_address = normalize_name(row[ADDRESS_COLUMN]) \
            if row[ADDRESS_COLUMN] else None
        for other in candidates:
            other_iid = indexes.code[other]["internal_id"]
            name_agree = bool(new_name_keys & _name_keys(state, other_iid))
            other_address = _latest_address(state, other_iid)
            address_agree = bool(
                new_address and other_address
                and new_address == normalize_name(other_address))
            other_secs = _latest_values(state, other_iid, "securities_code")
            sec_conflict = bool(new_secs and other_secs
                                and not (new_secs & other_secs))
            strength = evaluate_jcn_link(
                new_counts, counts_by_code[other], jcn,
                name_agree, address_agree, sec_conflict)
            if strength:
                _register_code(state, indexes, {
                    "internal_id": other_iid, "edinet_code": code,
                    "assignment_basis": f"jcn_{strength}",
                    "connected_from": other,
                    "filer_role": row["提出者種別"] or None,
                    "first_seen_date": date, "last_seen_date": date,
                    "in_latest_snapshot": True,
                    **_provenance(CODELIST_SOURCE_KIND, CODELIST_SOURCE_ID,
                                  generated_at),
                })
                return other_iid
        if candidates:
            _add_review(
                state, indexes, "jcn_link_needs_review", code,
                candidates[0], jcn,
                f"法人番号 {jcn} を既存コード {candidates[0]} と共有するが、"
                "自動接続の条件 (多数派・名称または所在地の一致・証券コードの"
                "矛盾なし) を満たさないため未確定として残す。", generated_at)
            return _assign_new_code(state, indexes, code, row, date,
                                    generated_at, "new_in_codelist")
    iid = _assign_new_code(state, indexes, code, row, date, generated_at,
                           "new_in_codelist")
    if row[NAME_COLUMN]:
        key = change_key(row[NAME_COLUMN])
        matched = sorted({h["edinet_code"] for h in state["identifier_history"]
                          if h["kind"] == "company_name"
                          and h["change_key"] == key
                          and h["edinet_code"] != code})
        if matched:
            _add_review(
                state, indexes, "name_only_candidate", code, matched[0], key,
                "正規化した名称が一致するが、法人番号では結べない。"
                "未確定の候補として残す (仕様 §1)。", generated_at)
    return iid


def _observe(open_map, rows, key, new_row):
    """有効期間つきの行へ観測を1回分適用する。

    open_map はキーごとの「現在開いている行」を指す。開いている行と
    値が同じなら最終観測日と観測回数を伸ばし、違う値なら新しい行を
    開く (前の行は閉じたまま残る)。変更日は検知日として記録される。
    """
    current = open_map.get(key)
    if current is not None and current["value"] == new_row["value"]:
        current["last_observed_date"] = new_row["last_observed_date"]
        current["observation_count"] += new_row["observation_count"]
        return current
    rows.append(new_row)
    open_map[key] = new_row
    return new_row


def _apply_code_observation(state, indexes, code, row, date, generated_at):
    """1コード分のスナップショット行を識別子・属性・銘柄へ反映する。"""
    iid = indexes.code[code]["internal_id"]
    prov = _provenance(CODELIST_SOURCE_KIND, CODELIST_SOURCE_ID, generated_at)
    for column, kind in IDENTIFIER_COLUMNS.items():
        value = row[column]
        if value is None or not str(value).strip():
            continue
        value = str(value)
        is_name = kind in NAME_KINDS
        _observe(indexes.ident_open, state["identifier_history"],
                 (iid, code, kind),
                 {
                     "internal_id": iid, "edinet_code": code, "kind": kind,
                     "value": value, "source_value": value,
                     "value_normalized": normalize_name(value) if is_name else None,
                     "change_key": change_key(value) if is_name else None,
                     "is_placeholder": kind == "securities_code"
                     and value == PLACEHOLDER_SEC_CODE,
                     "first_observed_date": date, "last_observed_date": date,
                     "observation_count": 1, **prov,
                 })
        if kind == "corporate_number":
            indexes.jcn_counts[code][value] += 1
    for column, attribute in ATTRIBUTE_COLUMNS.items():
        value = row[column]
        if value is None or not str(value).strip():
            continue
        _observe(indexes.attr_open, state["attribute_history"],
                 (iid, code, attribute),
                 {
                     "internal_id": iid, "edinet_code": code,
                     "attribute": attribute, "value": str(value),
                     "first_observed_date": date, "last_observed_date": date,
                     "observation_count": 1, **prov,
                 })
    sec = (row[SEC_COLUMN] or "").strip()
    if sec and sec != PLACEHOLDER_SEC_CODE:
        effective = sec
        unverified = False
        rot = rotated_form(sec)
        if rot is not None:
            if (iid, rot) in indexes.sec:
                effective = rot
            else:
                unverified = True
        target = indexes.sec.get((iid, effective))
        if target is not None:
            target["last_observed_date"] = date
            target["observation_count"] += 1
            target["in_latest_snapshot"] = True
            if target["edinet_code"] != code:
                # 同じ銘柄が別のコードからも観測された場合は、
                # 観測元のコードを最新のものへ寄せる (行は会社単位で1つ)。
                target["edinet_code"] = code
        else:
            kind_label, digit = (UNVERIFIED_KIND, None) if unverified \
                else security_kind(effective)
            new_row = {
                "internal_id": iid, "edinet_code": code,
                "securities_code": effective, "security_kind": kind_label,
                "reserve_digit": digit, "is_primary": False,
                "primary_basis": None, "first_observed_date": date,
                "last_observed_date": date, "observation_count": 1,
                "in_latest_snapshot": True, **prov,
            }
            state["securities"].append(new_row)
            indexes.sec[(iid, effective)] = new_row
            if unverified:
                _add_review(
                    state, indexes, "security_kind_unverified", code, None,
                    sec,
                    f"証券コード {sec} は先行ゼロの5桁コードであり、回転形 "
                    f"{rot} が同一会社で観測されないため、種類を断定せず"
                    "未確認として記録する。裏取り (銘柄名・有報の種類株式の"
                    "内容) は書類側の取り込み後に行う。", generated_at)


def apply_snapshot(state, rows, date, generated_at):
    """コードリストのスナップショット1回分を状態へ適用する。

    コードの昇順に処理し、新しいコードは内部 ID の解決 (法人番号の
    接続判定を含む) をしてから観測を反映する (同じ回に現れた
    新しいコード同士でも、前のコードの観測が後の判定に見える)。
    戻り値は適用の内訳 (新規コード数・接続数など)。
    """
    by_code = {r[CODE_COLUMN]: r for r in rows}
    indexes = _Indexes(state)
    stats = {"new_codes": 0, "connected": 0, "rows": len(rows)}
    for code in sorted(by_code):
        if code not in indexes.code:
            iid = _resolve_new_code(state, indexes, code, by_code[code],
                                    date, generated_at)
            stats["new_codes"] += 1
            if indexes.code[code]["assignment_basis"].startswith("jcn_"):
                stats["connected"] += 1
        _apply_code_observation(state, indexes, code, by_code[code], date,
                                generated_at)
    for r in state["edinet_codes"]:
        present = r["edinet_code"] in by_code
        r["in_latest_snapshot"] = present
        if present:
            r["last_seen_date"] = date
            r["filer_role"] = by_code[r["edinet_code"]]["提出者種別"] or None
    for s in state["securities"]:
        if s["last_observed_date"] != date:
            s["in_latest_snapshot"] = False
    present_values = collections.defaultdict(set)
    for r in rows:
        for column, taxonomy in CLASSIFICATION_COLUMNS.items():
            value = r[column]
            if value is not None and str(value).strip():
                present_values[taxonomy].add(str(value))
    known = {(c["taxonomy"], c["value"]) for c in state["classifications"]}
    for taxonomy, values in present_values.items():
        for value in sorted(values):
            if (taxonomy, value) not in known:
                state["classifications"].append({
                    "taxonomy": taxonomy, "value": value,
                    "first_seen_date": date, "last_seen_date": date,
                    "snapshot_count": 0, "in_latest_snapshot": False,
                    **_provenance(CODELIST_SOURCE_KIND, CODELIST_SOURCE_ID,
                                  generated_at),
                })
                known.add((taxonomy, value))
    for c in state["classifications"]:
        present = c["value"] in present_values.get(c["taxonomy"], set())
        c["in_latest_snapshot"] = present
        if present:
            c["last_seen_date"] = date
            c["snapshot_count"] += 1
    state["meta"]["last_applied_snapshot_date"] = date
    finalize(state)
    return stats


def finalize(state):
    """会社台帳の状態と主銘柄を、現在の表群から決定的に再計算する。"""
    codes_by_iid = collections.defaultdict(list)
    for r in state["edinet_codes"]:
        codes_by_iid[r["internal_id"]].append(r)
    for c in state["companies"]:
        members = codes_by_iid[c["internal_id"]]
        c["first_seen_date"] = min(m["first_seen_date"] for m in members)
        c["last_seen_date"] = max(m["last_seen_date"] for m in members)
        c["status"] = ("active" if any(m["in_latest_snapshot"] for m in members)
                       else "absent")
    secs_by_iid = collections.defaultdict(list)
    for s in state["securities"]:
        secs_by_iid[s["internal_id"]].append(s)
    for iid, secs in secs_by_iid.items():
        def rank(s):
            return (0 if s["in_latest_snapshot"] else 1,
                    _neg_date(s["last_observed_date"]),
                    -s["observation_count"], s["securities_code"])
        primary = sorted(secs, key=rank)[0]
        for s in secs:
            s["is_primary"] = s is primary
            s["primary_basis"] = (
                "latest_snapshot" if primary["in_latest_snapshot"]
                else "latest_observation") if s is primary else None


def _neg_date(date):
    """日付の降順ソート用キー (YYYY-MM-DD の各桁を反転する)。"""
    return tuple(-int(part) for part in date.split("-"))


def apply_new_snapshots(state, snapshots, generated_at):
    """未適用のスナップショットだけを日付順に適用する (冪等)。

    snapshots は (日付, 行のリスト) のリスト。適用済みの日付
    (state の meta にある最終適用日) 以前のものは読み飛ばす。
    """
    last = state["meta"].get("last_applied_snapshot_date")
    applied = []
    for date, rows in sorted(snapshots):
        if last and date <= last:
            continue
        stats = apply_snapshot(state, rows, date, generated_at)
        applied.append({"date": date, **stats})
        last = date
    return applied


def write_state(state, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    for name, schema in TABLE_SCHEMAS.items():
        fields = [f.name for f in schema]
        rows = [{f: r.get(f) for f in fields} for r in state[name]]
        pq.write_table(pa.Table.from_pylist(rows, schema=schema),
                       os.path.join(out_dir, f"{name}.parquet"))
    with open(os.path.join(out_dir, "state.json"), "w", encoding="utf-8") as f:
        json.dump(state["meta"], f, ensure_ascii=False, indent=2)


def read_state(in_dir):
    state = empty_state(_now())
    for name, schema in TABLE_SCHEMAS.items():
        path = os.path.join(in_dir, f"{name}.parquet")
        if os.path.exists(path):
            state[name] = pq.read_table(path).to_pylist()
    with open(os.path.join(in_dir, "state.json"), encoding="utf-8") as f:
        state["meta"] = json.load(f)
    return state


def build_summary(state, applied, extra=None):
    summary = {
        "generated_at": state["generated_at"],
        "last_applied_snapshot_date": state["meta"].get(
            "last_applied_snapshot_date"),
        "next_internal_id": state["meta"]["next_internal_id"],
        "companies": len(state["companies"]),
        "companies_active": sum(1 for c in state["companies"]
                                if c["status"] == "active"),
        "edinet_codes": len(state["edinet_codes"]),
        "identifier_history_rows": len(state["identifier_history"]),
        "attribute_history_rows": len(state["attribute_history"]),
        "securities": len(state["securities"]),
        "securities_primary": sum(1 for s in state["securities"]
                                  if s["is_primary"]),
        "securities_by_kind": dict(collections.Counter(
            s["security_kind"] for s in state["securities"])),
        "companies_with_multiple_securities": sum(
            1 for _, secs in _group_by(state["securities"], "internal_id")
            if len(secs) > 1),
        "classifications": len(state["classifications"]),
        "review_items": len(state["review_items"]),
        "last_submissions_rows": len(state["last_submissions"]),
        "review_items_by_type": dict(collections.Counter(
            r["item_type"] for r in state["review_items"])),
        "applied_snapshots": applied,
    }
    if extra:
        summary.update(extra)
    return summary


def _group_by(rows, field):
    groups = collections.defaultdict(list)
    for r in rows:
        groups[r[field]].append(r)
    return groups.items()


def compute_last_submissions(index_path, source_sha, generated_at):
    """v1 索引から (EDINET コード, 書類種別) ごとの最終提出日を集計する。

    会社マスタの派生属性 (最新の提出日) の初期値。源泉の事実では
    ないため is_derived を立てる形で history 側に置く。
    """
    table = pq.read_table(index_path,
                          columns=["edinet_code", "doc_type", "submit_at"])
    d = table.to_pydict()
    agg = {}
    for ec, doc_type, submit_at in zip(d["edinet_code"], d["doc_type"],
                                       d["submit_at"]):
        key = (ec, doc_type or "")
        date = submit_at[:10]
        a = agg.get(key)
        if a is None:
            agg[key] = {"last": date, "count": 1}
        else:
            a["count"] += 1
            a["last"] = max(a["last"], date)
    return [{
        "edinet_code": ec, "doc_type": doc_type,
        "last_submit_date": a["last"], "document_count": a["count"],
        "source_kind": V1_SOURCE_KIND, "source_dataset": V1_REPO,
        "source_file": V1_INDEX_PATH, "source_file_sha256": source_sha,
        "generated_at": generated_at, "redistributable": True,
    } for (ec, doc_type), a in sorted(agg.items())]


def aggregate_catalog_submissions(rows):
    """書類目録 (catalog/) の行を (EDINET コード, 書類種別) ごとに集計する。

    目録は観測の追記であり、同じ書類 (docID) が複数回現れるため、
    書類単位に一意化してから数える。一意化で提出日が食い違う場合は
    新しい方を採る。docID と EDINET コードの無い行は数えない。
    戻り値は {(コード, 種別): {"last": 最終提出日,
    "by_date": Counter(提出日 -> 件数)}}。
    """
    docs = {}
    for r in rows:
        doc_id = r.get("docID")
        code = r.get("edinetCode")
        if not doc_id or not code:
            continue
        date = (r.get("submitDateTime") or "")[:10]
        if not date:
            continue
        prev = docs.get(doc_id)
        if prev is None or date > prev[2]:
            docs[doc_id] = (code, r.get("docTypeCode") or "", date)
    agg = {}
    for code, doc_type, date in docs.values():
        a = agg.setdefault((code, doc_type),
                           {"last": date, "by_date": collections.Counter()})
        a["last"] = max(a["last"], date)
        a["by_date"][date] += 1
    return agg


def refresh_last_submissions(state, agg, generated_at):
    """last_submissions を書類目録の集計で更新する (冪等)。

    最終提出日は最大値で更新する。件数は、現在の最終提出日より後の
    提出日の書類だけを加算する。初期値 (v1 索引の集計) には提出日
    以下の書類が既に含まれているため、それ以前の書類を加算すると
    二重数えになる。代わりに、基準日と同日の別書類は数え落とす
    ことがある (派生属性の近似として許容し、ここに明記する)。
    同じ集計で再実行しても結果は変わらない。マスタに無いコードの
    書類は取り込まず、件数だけを統計として返す。
    """
    idx = _code_index(state)
    rows_by_key = {(r["edinet_code"], r["doc_type"]): r
                   for r in state["last_submissions"]}
    stats = {"updated_rows": 0, "new_rows": 0, "added_documents": 0,
             "unknown_codes": 0, "unknown_documents": 0}
    for (code, doc_type), a in sorted(agg.items()):
        code_row = idx.get(code)
        if code_row is None:
            stats["unknown_codes"] += 1
            stats["unknown_documents"] += sum(a["by_date"].values())
            continue
        row = rows_by_key.get((code, doc_type))
        if row is None:
            count = sum(a["by_date"].values())
            row = {"internal_id": code_row["internal_id"],
                   "edinet_code": code, "doc_type": doc_type,
                   "last_submit_date": a["last"], "document_count": count,
                   "is_derived": True,
                   **_provenance(CATALOG_SOURCE_KIND, CATALOG_SOURCE_ID,
                                 generated_at)}
            state["last_submissions"].append(row)
            rows_by_key[(code, doc_type)] = row
            stats["new_rows"] += 1
            stats["added_documents"] += count
            continue
        baseline = row["last_submit_date"]
        added = sum(n for d, n in a["by_date"].items() if d > baseline)
        if added or a["last"] > baseline:
            row["last_submit_date"] = max(baseline, a["last"])
            row["document_count"] += added
            row.update(_provenance(CATALOG_SOURCE_KIND, CATALOG_SOURCE_ID,
                                   generated_at))
            stats["updated_rows"] += 1
            stats["added_documents"] += added
    return stats


_CATALOG_LEDGER_RE = re.compile(r"^catalog/month=\d{4}-\d{2}/ledger\.jsonl$")


def download_catalog_rows(repo, work_dir):
    """v2 の catalog/ から全月の書類目録を読み、行のリストで返す。"""
    api = HfApi()
    files = _with_retry("list_repo_files", lambda: api.list_repo_files(
        repo, repo_type="dataset"))
    rows = []
    for path in sorted(f for f in files if _CATALOG_LEDGER_RE.match(f)):
        local = _with_retry("hf_hub_download", lambda path=path:
                            hf_hub_download(
                                repo_id=repo, filename=path,
                                repo_type="dataset",
                                local_dir=os.path.join(work_dir, "catalog")))
        with open(local, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
    return rows


def load_catalog_rows(catalog_dir):
    """ローカルディレクトリの JSONL (月別の書類目録) を行で返す。"""
    rows = []
    for name in sorted(os.listdir(catalog_dir)):
        if not name.endswith(".jsonl"):
            continue
        with open(os.path.join(catalog_dir, name), encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
    return rows


def place_history(work_dir, repo=DEFAULT_REPO, index_path=None, upload=False):
    """再構成を実行し、成果を master/history_from_v1/ の形に揃える。"""
    from . import reconstruct_v1

    os.makedirs(work_dir, exist_ok=True)
    if index_path is None:
        index_path = _with_retry("hf_hub_download", lambda: hf_hub_download(
            repo_id=V1_REPO, filename=V1_INDEX_PATH, repo_type="dataset",
            local_dir=os.path.join(work_dir, "v1")))
    out_dir = os.path.join(work_dir, "history_from_v1")
    reconstruct_v1.main(["reconstruct_v1.py", index_path, out_dir])
    with open(index_path, "rb") as f:
        source_sha = hashlib.sha256(f.read()).hexdigest()
    generated_at = _now()
    rows = compute_last_submissions(index_path, source_sha, generated_at)
    schema = pa.schema([
        ("edinet_code", pa.string()), ("doc_type", pa.string()),
        ("last_submit_date", pa.string()), ("document_count", pa.int64()),
        ("source_kind", pa.string()), ("source_dataset", pa.string()),
        ("source_file", pa.string()), ("source_file_sha256", pa.string()),
        ("generated_at", pa.string()), ("redistributable", pa.bool_()),
    ])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema),
                   os.path.join(out_dir, "last_submissions.parquet"))
    uploaded = []
    if upload:
        api = HfApi()
        for name in ("identifier_history.parquet", "code_roster.parquet",
                     "code_connections.parquet", "review_items.parquet",
                     "summary.json", "last_submissions.parquet"):
            local = os.path.join(out_dir, name)
            _with_retry("upload_file", lambda local=local, name=name:
                        api.upload_file(
                            path_or_fileobj=local,
                            path_in_repo=f"{HISTORY_PREFIX}/{name}",
                            repo_id=repo, repo_type="dataset",
                            commit_message=f"master: place v1 reconstruction "
                                           f"history ({name})"))
            uploaded.append(f"{HISTORY_PREFIX}/{name}")
    with open(os.path.join(out_dir, "summary.json"), encoding="utf-8") as f:
        recon_summary = json.load(f)
    return {"history_dir": out_dir, "uploaded": uploaded,
            "reconstruction": recon_summary,
            "last_submissions_rows": len(rows)}


_SNAPSHOT_PATH_RE = re.compile(
    r"^codelist/date=(\d{4}-\d{2}-\d{2})/EdinetcodeDlInfo\.csv$")


def download_snapshots(repo, work_dir):
    """v2 の codelist/ から全スナップショットを (日付, 行) で取得する。"""
    api = HfApi()
    files = _with_retry("list_repo_files", lambda: api.list_repo_files(
        repo, repo_type="dataset"))
    dates = sorted(m.group(1) for f in files
                   if (m := _SNAPSHOT_PATH_RE.match(f)))
    snapshots = []
    for date in dates:
        local = _with_retry("hf_hub_download", lambda date=date:
                            hf_hub_download(
                                repo_id=repo,
                                filename=f"codelist/date={date}/"
                                         f"EdinetcodeDlInfo.csv",
                                repo_type="dataset",
                                local_dir=os.path.join(work_dir, "codelist")))
        with open(local, "rb") as f:
            parsed_date, rows = parse_codelist_csv(f.read())
        if parsed_date != date:
            raise ValueError(
                f"スナップショットのパス日付 {date} と CSV の日付 "
                f"{parsed_date} が一致しません")
        snapshots.append((date, rows))
    return snapshots


def download_master(repo, work_dir):
    """v2 の master/ から現在の状態一式を落として読み込む。"""
    api = HfApi()
    state_dir = os.path.join(work_dir, "state_in")
    os.makedirs(state_dir, exist_ok=True)
    names = [f"{name}.parquet" for name in TABLE_SCHEMAS] + ["state.json"]
    for name in names:
        try:
            got = _with_retry("hf_hub_download", lambda name=name:
                              hf_hub_download(
                                  repo_id=repo,
                                  filename=f"{MASTER_PREFIX}/{name}",
                                  repo_type="dataset", local_dir=None,
                                  token=None))
        except EntryNotFoundError:
            raise ValueError(
                f"master/{name} が v2 データセットにありません。"
                "初期構築 (build) が先です") from None
        import shutil
        shutil.copyfile(got, os.path.join(state_dir, name))
    return read_state(state_dir)


def download_history(repo, work_dir):
    """v2 の master/history_from_v1/ を落としてディレクトリを返す。"""
    api = HfApi()
    history_dir = os.path.join(work_dir, "history_from_v1")
    os.makedirs(history_dir, exist_ok=True)
    for name in ("identifier_history.parquet", "code_roster.parquet",
                 "code_connections.parquet", "review_items.parquet",
                 "last_submissions.parquet"):
        got = _with_retry("hf_hub_download", lambda name=name:
                          hf_hub_download(
                              repo_id=repo,
                              filename=f"{HISTORY_PREFIX}/{name}",
                              repo_type="dataset", local_dir=None, token=None))
        import shutil
        shutil.copyfile(got, os.path.join(history_dir, name))
    return history_dir


def upload_master(state, out_dir, repo):
    """状態一式とサマリを master/ へ上げる。"""
    api = HfApi()
    uploaded = []
    names = [f"{name}.parquet" for name in TABLE_SCHEMAS] + ["state.json",
                                                             "summary.json"]
    for name in names:
        local = os.path.join(out_dir, name)
        _with_retry("upload_file", lambda local=local, name=name:
                    api.upload_file(
                        path_or_fileobj=local,
                        path_in_repo=f"{MASTER_PREFIX}/{name}",
                        repo_id=repo, repo_type="dataset",
                        commit_message=f"master: update company master ({name})"))
        uploaded.append(f"{MASTER_PREFIX}/{name}")
    return uploaded


def corroboration_checks(connections, latest_rows):
    """接続候補を現行スナップショットで裏取りした結果を返す。

    両コードが現行スナップショットにある組は、法人番号の一致と
    所在地 (正規化後) の一致を確認する。片方しかない組は、現行側の
    実在だけを確認し、矛盾が無いことを記録する。
    """
    by_code = {r[CODE_COLUMN]: r for r in latest_rows} if latest_rows else {}
    results = []
    for c in connections:
        a, b = c["edinet_code_a"], c["edinet_code_b"]
        ra, rb = by_code.get(a), by_code.get(b)
        entry = {"edinet_code_a": a, "edinet_code_b": b,
                 "strength": c["strength"]}
        if ra and rb:
            entry["both_present"] = True
            entry["jcn_match"] = (
                bool(ra[JCN_COLUMN]) and ra[JCN_COLUMN] == rb[JCN_COLUMN])
            entry["address_match"] = (
                normalize_name(ra[ADDRESS_COLUMN] or "")
                == normalize_name(rb[ADDRESS_COLUMN] or ""))
            entry["securities_code_match"] = (
                (ra[SEC_COLUMN] or "") == (rb[SEC_COLUMN] or ""))
            entry["contradiction"] = bool(
                ra[JCN_COLUMN] and rb[JCN_COLUMN]
                and ra[JCN_COLUMN] != rb[JCN_COLUMN])
        elif ra or rb:
            entry["both_present"] = False
            entry["present_code"] = a if ra else b
            entry["contradiction"] = False
        else:
            entry["both_present"] = False
            entry["present_code"] = None
            entry["contradiction"] = False
        results.append(entry)
    return results


def run_build(work_dir, repo=DEFAULT_REPO, history_dir=None,
              snapshots=None, snapshots_dir=None, upload=False):
    """初期構築: v1 初期履歴へ全スナップショットを日付順に適用する。"""
    generated_at = _now()
    if history_dir is None:
        history_dir = download_history(repo, work_dir)
    if snapshots is None:
        snapshots = (load_local_snapshots(snapshots_dir) if snapshots_dir
                     else download_snapshots(repo, work_dir))
    if not snapshots:
        raise ValueError("コードリストのスナップショットがありません")
    state = state_from_history(history_dir, generated_at)
    connections = pq.read_table(
        os.path.join(history_dir, "code_connections.parquet")).to_pylist()
    idx = _code_index(state)
    matched = sum(1 for c in connections
                  if idx[c["edinet_code_a"]]["internal_id"]
                  == idx[c["edinet_code_b"]]["internal_id"])
    applied = apply_new_snapshots(state, snapshots, generated_at)
    latest_rows = snapshots[-1][1]
    extra = {
        "history_connection_check": {
            "expected_pairs": len(connections), "matched_pairs": matched},
        "codelist_corroboration": corroboration_checks(connections,
                                                       latest_rows),
    }
    out_dir = os.path.join(work_dir, "master_out")
    write_state(state, out_dir)
    summary = build_summary(state, applied, extra)
    with open(os.path.join(out_dir, "summary.json"), "w",
              encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    uploaded = upload_master(state, out_dir, repo) if upload else []
    return {"out_dir": out_dir, "uploaded": uploaded, "summary": summary}


def run_update(work_dir, repo=DEFAULT_REPO, state_dir=None,
               snapshots=None, snapshots_dir=None, upload=False,
               catalog_rows=None):
    """日次更新: 未適用のスナップショットを適用し、最終提出日を更新する。

    スナップショットの適用に加え、書類目録 (catalog/) の集計で
    last_submissions を更新する。目録は取得経路が既に v2 へ保存した
    ものを読むだけであり、EDINET への再取得はしない。
    """
    generated_at = _now()
    state = read_state(state_dir) if state_dir else download_master(
        repo, work_dir)
    state["generated_at"] = generated_at
    if snapshots is None:
        snapshots = (load_local_snapshots(snapshots_dir) if snapshots_dir
                     else download_snapshots(repo, work_dir))
    applied = apply_new_snapshots(state, snapshots, generated_at)
    if catalog_rows is None:
        catalog_rows = download_catalog_rows(repo, work_dir)
    last_sub_stats = refresh_last_submissions(
        state, aggregate_catalog_submissions(catalog_rows), generated_at)
    out_dir = os.path.join(work_dir, "master_out")
    write_state(state, out_dir)
    summary = build_summary(
        state, applied, {"last_submissions_refresh": last_sub_stats})
    with open(os.path.join(out_dir, "summary.json"), "w",
              encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    uploaded = upload_master(state, out_dir, repo) if upload else []
    return {"out_dir": out_dir, "uploaded": uploaded, "summary": summary,
            "applied": applied, "last_submissions_refresh": last_sub_stats}


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="会社マスタと銘柄レイヤー (P2-T3、仕様 #15 §1・§4)")
    ap.add_argument("command",
                    choices=["place-history", "build", "update"])
    ap.add_argument("--work-dir", required=True, help="作業ディレクトリ")
    ap.add_argument("--repo", default=DEFAULT_REPO)
    ap.add_argument("--index", help="v1 索引 parquet (place-history 用)")
    ap.add_argument("--history-dir",
                    help="history_from_v1 のローカルディレクトリ (build 用)")
    ap.add_argument("--state-dir",
                    help="現在の master 状態のローカルディレクトリ (update 用)")
    ap.add_argument("--snapshots-dir",
                    help="コードリスト CSV のローカルディレクトリ")
    ap.add_argument("--catalog-dir",
                    help="書類目録 JSONL のローカルディレクトリ (update 用)")
    ap.add_argument("--upload", action="store_true",
                    help="成果を v2 データセットへ上げる")
    args = ap.parse_args(argv)

    if args.command == "place-history":
        result = place_history(args.work_dir, repo=args.repo,
                               index_path=args.index, upload=args.upload)
    elif args.command == "build":
        result = run_build(args.work_dir, repo=args.repo,
                           history_dir=args.history_dir,
                           snapshots_dir=args.snapshots_dir,
                           upload=args.upload)
    else:
        catalog_rows = (load_catalog_rows(args.catalog_dir)
                        if args.catalog_dir else None)
        result = run_update(args.work_dir, repo=args.repo,
                            state_dir=args.state_dir,
                            snapshots_dir=args.snapshots_dir,
                            upload=args.upload, catalog_rows=catalog_rows)
    print(json.dumps(result, ensure_ascii=False, default=str)[:4000])
    return 0


if __name__ == "__main__":
    sys.exit(main())
