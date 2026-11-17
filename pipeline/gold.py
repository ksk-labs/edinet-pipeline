"""Gold 生成 (P2-T5、仕様 #15 §7・§8)。

第1期 Silver (提出日 2021-11-01 以降) から Gold (会社×決算期) を生成する。
成果物は v2 データセット (Yoshi-Dai/financial-lakehouse-v2) の gold/ 配下に置く。

gold/ への追加分 (本モジュールが正本):
- annual.parquet: 会社×決算期の横持ち。指標登録簿 (edinet-definitions の
  indicators.yaml) の指標列と基礎値、期間月数と変則フラグ、訂正の系譜への
  参照 (代表書類・訂正の有無・最新訂正) を持つ。全提出者を保持し、
  上場区分は属性として列に持つ。market 列は将来の米国 (Step 3) に備えて
  最初から持ち、EDINET 由来は 'JP' とする。
- annual_basis.parquet: 指標ごとの採用区分 (連結 consolidated /
  単体 non_consolidated) と採用要素。横持ちに区分の列を指標ごとに
  持つと列が倍になるため、縦持ちの別表で「単体であることを明示」する。
- quarterly.parquet: 四半期・半期の累計期間の表 (TTM 合成の材料)。
  年次行とは別に持つ (仕様 §7)。
- dividends.parquet: 配当の事実。銘柄 (種類) 次元つきで、期種別
  (annual / interim / final) を分ける。内訳 (通常・特別・記念) と
  権利確定日・支払日は Silver の数値事実からは決定的に取れないため、
  列だけを用意して null とし、内訳は unspecified と記録する (下記の
  切り分け参照)。
- forecasts.parquet: 会社予想の事実型の器 (仕様 §8)。主たる源泉は
  決算短信 (TDnet) のため、本チケットではスキーマだけを作り行は作らない。
- summary.json: 生成統計 (行数、指標ごとの充足、未解決の書類数、
  登録簿の版と sha256、スナップショット日)。
- state.json: 日次差分 (update) のための透かし (月ごとの行数と
  最大提出日時と内容ハッシュ、登録簿の sha256)。内容ハッシュは
  行数と最大提出日時が変わらない値だけの書き換え (既存書類の
  再解析) も検出するために持つ。
- snapshots/date=YYYY-MM-DD/: 日次スナップショットとしての版固定の
  複写 (仕様 §9、台帳 D14)。build / update のたびに当日の一式を置く。

確定値と勝者決定:
- Silver の事実は (会社, 要素, 期間, 次元区分, 単位) ごとに複数あり得る
  (有報のサマリの推移、訂正、前期分の再掲)。確定値は試作 (Step 1 の
  prototype/build_gold.py) の規則を本番 Silver に合わせて引き継ぐ:
  (1) point_in_time='final' (訂正書類由来) を優先する、
  (2) 提出日時の新しいものを優先する、(3) Silver の格納順
  (月の昇順、ファイル内の行順) で先に現れたものを優先する。
  (1)(2) により、同一親への訂正が複数ある場合は最新の訂正1件が
  勝者になり、superseded_by の本番規則と整合する。
- 連結と単体の規則: 要素の事実は次元なし (連結) を優先し、無い場合は
  NonConsolidatedMember (単体) を採用して basis に明示する。指標ごとに
  登録簿の dimension_priority で優先順を変えられる (配当と発行済
  株式数は単体の様式でしか出ないため単体を優先する)。セグメント等の
  その他の次元の事実は採用せず、数えて summary に記録する。

決算期の同定:
- フロー系の事実の期間 (context_start, context_end) を決算期とする。
  ストック系の事実は instant (開始=終了) で決算日に結ぶ。値の取得では
  期間一致と instant 一致の両方を探し、存在する方を取る (サマリ要素は
  フロー系が期間、ストック系が instant で出るため)。
- 年次行は、期間月数が 10 以上、または有報 (doc_type=120) の対象期間
  である期間とする。変則決算 (決算期変更の移行期の短縮決算など) は
  有報の対象期間であることで年次行に入る。期間月数は暦月で計算し、
  12 でない行に変則フラグを立てる。
- 四半期行は四半期報告書 (140)・半期報告書 (160) とその訂正
  (150・170) の書類に由来する累計期間の行とする。

取れる信号と取れない信号の切り分け (実測は作業記録):
- 取れる: 初期指標15件のうち、期中平均株式数を除く全てと、基礎値の
  発行済株式数。配当の年間実績と中間実績 (銘柄は主銘柄=普通株式)。
- 取れない (器に留める): 期中平均株式数 (標準要素が存在しない)、
  配当の内訳 (通常・特別・記念) の分離、配当の権利確定日と支払日
  (日付は非数値のため Silver に入らない)、種類株の配当 (拡張要素で
  会社ごとに様式が異なり標準化できない)、会社予想の取り込み
  (源泉は TDnet)。

入力の差し替え:
- build / update は --silver-dir (month=YYYY-MM の並び)、--index
  (v1 索引 parquet)、--catalog-dir (v2 の catalog の月次目録)、
  --master-dir (edinet_codes 等を含むローカルディレクトリ)、
  --registry (indicators.yaml) を受け付け、HF を介さない検証実行が
  できる。引数が無い場合は HF から取得する。
- 登録簿の正本は edinet-definitions リポジトリ。実行は HF 上の写し
  (definitions/indicators.yaml) を使い、sha256 を summary と state に
  記録して版を固定する。写しの同期は sync-registry で行う
  (引数で渡したローカルの登録簿ファイルを写しとして置く。正本の
  取得は bot の経路がある環境で呼び出し側が行う)。

使い方:
  python -m pipeline.gold build --work-dir DIR [--upload] [差し替え引数]
  python -m pipeline.gold update --work-dir DIR [--upload] [差し替え引数]
  python -m pipeline.gold sync-registry --work-dir DIR [--upload]
認証は環境変数 HF_TOKEN (書き込み。huggingface_hub が自動で読む)。
読み取りは公開データセットなのでトークンなしでも動く。
"""
import argparse
import ast
import collections
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml
from huggingface_hub import HfApi, hf_hub_download

from .hf_sync import _with_retry

DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"
V1_REPO = "Yoshi-Dai/financial-lakehouse"
V1_INDEX_PATH = "catalog/documents_index.parquet"
GOLD_PREFIX = "gold"
REGISTRY_REPO_PATH = "definitions/indicators.yaml"
DEFINITIONS_REPO = "ksk-labs/edinet-definitions"

DEFAULT_SINCE_MONTH = "2021-11"
MARKET_JP = "JP"
REDISTRIBUTION_ALLOWED = "allowed"
SOURCE_KIND = "edinet_silver"

# 登録簿の単位から、採用する Silver の単位を決める
SOURCE_UNIT = {
    "円": "iso4217:JPY",
    "%": "xbrli:pure",
    "円/株": "iso4217:JPY per xbrli:shares",
    "株": "xbrli:shares",
    "年": None,  # series の結果であり要素の単位では引かない
}

CONSOLIDATED = "consolidated"
NON_CONSOLIDATED = "non_consolidated"
NON_CONSOLIDATED_MEMBER = "NonConsolidatedMember"

DOC_TYPE_ANNUAL = "120"
DOC_TYPE_QUARTERLY = {"140", "150"}
DOC_TYPE_SEMIANNUAL = {"160", "170"}
QUARTERLY_DOC_TYPES = DOC_TYPE_QUARTERLY | DOC_TYPE_SEMIANNUAL

# 四半期表に持つフロー指標 (登録簿の id)
QUARTERLY_INDICATORS = (
    "net_sales", "operating_income", "ordinary_income", "net_income",
    "operating_cf",
)

# 配当の源泉要素 (登録簿の dividend_per_share とは別に、期種別を
# 分けるために直接引く)
DIVIDEND_ANNUAL_ELEMENT = "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults"
DIVIDEND_INTERIM_ELEMENT = (
    "jpcrp_cor:InterimDividendPaidPerShareSummaryOfBusinessResults")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def period_months(start, end):
    """暦月数。start と end は 'YYYY-MM-DD'。期間の月数を返す。

    (end.year - start.year) * 12 + (end.month - start.month) に、
    終了日が開始日以上の日なら 1 を足す (2025-04-01..2026-03-31 は
    12 か月、2025-04-01..2025-09-30 は 6 か月)。
    """
    sy, sm, sd = (int(x) for x in start.split("-"))
    ey, em, ed = (int(x) for x in end.split("-"))
    months = (ey - sy) * 12 + (em - sm)
    if ed >= sd:
        months += 1
    return months


def dimension_class(dimension):
    """Silver の dimension 文字列を区分に変える。

    '' は次元なし (連結本表) で consolidated、NonConsolidatedMember を
    含むものは non_consolidated、それ以外 (セグメント等) は None
    (Gold の材料にしない)。
    """
    if not dimension:
        return CONSOLIDATED
    parts = [p.strip() for p in dimension.split(";")]
    if len(parts) == 1 and parts[0].endswith(NON_CONSOLIDATED_MEMBER):
        return NON_CONSOLIDATED
    if len(parts) == 1 and parts[0].endswith("ConsolidatedMember"):
        return CONSOLIDATED
    return None


# ---------------------------------------------------------------- 登録簿


def load_registry(path):
    """indicators.yaml を読み、検証して返す。"""
    if not path:
        raise ValueError(
            "registry not found: run sync-registry first, or pass "
            "--registry with a local indicators.yaml")
    with open(path, encoding="utf-8") as f:
        raw = f.read()
    registry = yaml.safe_load(raw)
    ids = [i["id"] for i in registry["indicators"]]
    if len(ids) != len(set(ids)):
        raise ValueError("registry has duplicate indicator ids")
    for ind in registry["indicators"]:
        if ind["kind"] not in ("element", "formula", "cagr", "series"):
            raise ValueError(f"unknown kind: {ind['id']}")
    registry["_raw"] = raw
    registry["_sha256"] = sha256_bytes(raw.encode("utf-8"))
    return registry


def registry_elements(registry):
    """登録簿が参照する Silver 要素名の集合 (合計候補は展開する)。"""
    elements = set()
    for ind in registry["indicators"]:
        for candidates in (ind.get("elements") or {}).values():
            for cand in candidates or []:
                if isinstance(cand, list):
                    elements.update(cand)
                else:
                    elements.add(cand)
    return elements


def candidate_elements(candidate):
    """候補 (文字列または要素名リスト) を要素名のリストにする。"""
    if isinstance(candidate, list):
        return list(candidate)
    return [candidate]


# ---------------------------------------------------------------- 書類対応


def build_doc_map(index_rows, catalog_rows):
    """doc_id -> {edinet_code, doc_type, standard, submit_at, period}。

    バックフィル分は v1 索引、日次分は v2 の書類目録 (catalog) から引く。
    両方にある場合は索引を優先する (会計基準を持つため)。
    """
    doc_map = {}
    for r in catalog_rows:
        doc_id = r.get("docID")
        if not doc_id:
            continue
        doc_map[doc_id] = {
            "edinet_code": r.get("edinetCode"),
            "doc_type": r.get("docTypeCode"),
            "standard": None,
            "submit_at": r.get("submitDateTime"),
            "period_start": r.get("periodStart"),
            "period_end": r.get("periodEnd"),
        }
    for r in index_rows:
        doc_id = r.get("doc_id")
        if not doc_id:
            continue
        doc_map[doc_id] = {
            "edinet_code": r.get("edinet_code"),
            "doc_type": r.get("doc_type"),
            "standard": _standard_key(r.get("accounting_standard")),
            "submit_at": r.get("submit_at"),
            "period_start": r.get("period_start"),
            "period_end": r.get("period_end"),
        }
    return doc_map


def _standard_key(accounting_standard):
    return {
        "Japan GAAP": "japan_gaap",
        "IFRS": "ifrs",
        "US GAAP": "us_gaap",
    }.get(accounting_standard)


def load_catalog_rows(catalog_dir):
    """catalog の月次目録 (ledger.jsonl) を全月分読む。"""
    rows = []
    if not catalog_dir or not os.path.isdir(catalog_dir):
        return rows
    for name in sorted(os.listdir(catalog_dir)):
        path = os.path.join(catalog_dir, name, "ledger.jsonl")
        if not os.path.exists(path):
            continue
        seen = set()
        with open(path, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                r = json.loads(line)
                key = r.get("docID")
                if key and key not in seen:
                    seen.add(key)
                    rows.append(r)
    return rows


# ---------------------------------------------------------------- 事実の収集と確定


def silver_months(silver_dir):
    """silver_dir 配下の month=YYYY-MM の一覧 (昇順)。"""
    months = []
    if not silver_dir or not os.path.isdir(silver_dir):
        return months
    for name in sorted(os.listdir(silver_dir)):
        if name.startswith("month=") and os.path.exists(
                os.path.join(silver_dir, name, "silver.parquet")):
            months.append(name[len("month="):])
    return months


def collect_facts(silver_dir, months, wanted_elements, doc_ids=None):
    """対象月の Silver から、必要な要素の事実だけを格納順に集める。

    doc_ids が与えられた場合はその書類の事実だけを集める (update 用)。
    戻り値は事実の dict のリストで、読み込み順 (月の昇順、ファイル内の
    行順) を保持する。seq は格納順の通し番号。
    """
    facts = []
    seq = 0
    for month in months:
        path = os.path.join(silver_dir, f"month={month}", "silver.parquet")
        table = pq.read_table(path, columns=[
            "doc_id", "element", "value", "unit", "context_start",
            "context_end", "dimension", "submit_at", "point_in_time"])
        mask = pc.is_in(table.column("element"),
                        value_set=pa.array(sorted(wanted_elements)))
        if doc_ids is not None:
            mask = pc.and_(mask, pc.is_in(
                table.column("doc_id"), value_set=pa.array(sorted(doc_ids))))
        table = table.filter(mask)
        for row in table.to_pylist():
            row["seq"] = seq
            seq += 1
            facts.append(row)
    return facts


def resolve_winners(facts, doc_map, code_to_id):
    """確定値の勝者を決める。

    キーは (internal_id, element, context_start, context_end, 区分, unit)。
    優先順は final (訂正由来) -> 提出日時の新しいもの -> doc_id の
    大きいもの -> 格納順の早いもの。doc_id の決め手は系譜
    (build_doc_lineage) と同じ順序 (提出日時、次に doc_id) に揃えて
    あり、同一分の訂正が複数ある場合に勝者と latest_correction_doc_id
    が食い違わないようにする。
    戻り値: (winners, stats)。winners の値は事実 dict に internal_id と
    dim_class を加えたもの。
    """
    by_key = {}
    stats = {"facts": 0, "unresolved_docs": 0, "ignored_dimensions": 0,
             "same_value_discarded": 0, "differing_value_discarded": 0,
             "correction_superseded": 0}
    unresolved = set()
    for f in facts:
        info = doc_map.get(f["doc_id"])
        if info is None or not info.get("edinet_code"):
            unresolved.add(f["doc_id"])
            continue
        internal_id = code_to_id.get(info["edinet_code"])
        if internal_id is None:
            unresolved.add(f["doc_id"])
            continue
        dim_class = dimension_class(f["dimension"])
        if dim_class is None:
            stats["ignored_dimensions"] += 1
            continue
        stats["facts"] += 1
        key = (internal_id, f["element"], f["context_start"],
               f["context_end"], dim_class, f["unit"])
        cand = dict(f)
        cand["internal_id"] = internal_id
        cand["dim_class"] = dim_class
        cand["edinet_code"] = info["edinet_code"]
        by_key.setdefault(key, []).append(cand)
    winners = {}
    for key, cands in by_key.items():
        ordered = sorted(cands, key=lambda c: c["seq"])
        ordered = sorted(
            ordered,
            key=lambda c: (c["submit_at"] or "", c["doc_id"]),
            reverse=True)
        ordered = sorted(
            ordered, key=lambda c: 0 if c["point_in_time"] == "final" else 1)
        winner = ordered[0]
        winners[key] = winner
        for other in ordered[1:]:
            if other["value"] == winner["value"]:
                stats["same_value_discarded"] += 1
            else:
                stats["differing_value_discarded"] += 1
                if (winner["point_in_time"] == "final"
                        and other["point_in_time"] != "final"):
                    stats["correction_superseded"] += 1
    stats["unresolved_docs"] = len(unresolved)
    stats["unresolved_doc_ids"] = sorted(unresolved)
    return winners, stats


def infer_standard(winner_facts):
    """事実の要素構成から会計基準を推定する (索引に無い会社用)。

    IFRS 版・米国版の経営指標サマリ要素があればその基準、無ければ
    日本基準とする。
    """
    for f in winner_facts:
        el = f["element"]
        if "IFRSSummaryOfBusinessResults" in el:
            return "ifrs"
        if "USGAAPSummaryOfBusinessResults" in el:
            return "us_gaap"
    return "japan_gaap"


# ---------------------------------------------------------------- 指標の計算


class Missing(Exception):
    """式の材料が欠損している、またはゼロ除算になった。"""


def eval_formula(expr, env):
    """四則演算と指標識別子だけの式を評価する (試作から継承)。"""
    def ev(node):
        if isinstance(node, ast.Expression):
            return ev(node.body)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return float(node.value)
            raise ValueError("unsupported constant")
        if isinstance(node, ast.Name):
            v = env.get(node.id)
            if v is None:
                raise Missing(node.id)
            return v
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            return -ev(node.operand)
        if isinstance(node, ast.BinOp):
            a, b = ev(node.left), ev(node.right)
            if isinstance(node.op, ast.Add):
                return a + b
            if isinstance(node.op, ast.Sub):
                return a - b
            if isinstance(node.op, ast.Mult):
                return a * b
            if isinstance(node.op, ast.Div):
                if b == 0:
                    raise Missing("division by zero")
                return a / b
        raise ValueError(f"unsupported expression: {ast.dump(node)}")
    return ev(ast.parse(expr, mode="eval"))


class GoldBuilder:
    """勝者の事実から会社×決算期の指標を組み立てる。"""

    def __init__(self, registry, winners, doc_map, code_to_id):
        self.registry = registry
        self.indicators = {i["id"]: i for i in registry["indicators"]}
        self.winners = winners
        self.doc_map = doc_map
        # (internal_id, element, start, end, dim_class, unit) -> winner
        self.by_fact = winners
        # 会社ごとの事実一覧 (基準推定と候補固定に使う)
        self.company_facts = collections.defaultdict(list)
        for w in winners.values():
            self.company_facts[w["internal_id"]].append(w)
        self.candidate_choice = {}  # (internal_id, standard, ind_id) -> candidate
        self.basis_rows = []
        # 書類の対象期間の逆引き: (internal_id, start, end) -> [doc_id]
        # 有報だけの逆引きも別に持つ (年次行と基準の判定に使う)
        self.period_docs = collections.defaultdict(list)
        self.annual_docs = collections.defaultdict(list)
        for doc_id, info in doc_map.items():
            if not info.get("edinet_code") or not info.get("period_start"):
                continue
            internal_id = code_to_id.get(info["edinet_code"])
            if internal_id is None:
                continue
            key = (internal_id, info["period_start"], info["period_end"])
            self.period_docs[key].append(doc_id)
            if info.get("doc_type") == DOC_TYPE_ANNUAL:
                self.annual_docs[key].append(doc_id)

    def standard_for(self, internal_id, period_start, period_end):
        """会社×決算期の会計基準。

        その期の有報の索引値を優先し、無ければその期間の事実の
        要素構成から推定する。
        """
        for doc_id in self.annual_docs.get(
                (internal_id, period_start, period_end), []):
            info = self.doc_map.get(doc_id) or {}
            if info.get("standard"):
                return info["standard"]
        facts = [f for f in self.company_facts.get(internal_id, [])
                 if f["context_end"] == period_end]
        return infer_standard(facts)

    def annual_periods(self, internal_id):
        """その会社の年次行の期間 (period_start, period_end) の集合。

        フロー系の勝者事実の期間のうち、月数が 10 以上、または
        有報の対象期間であるもの。
        """
        periods = set()
        for f in self.company_facts.get(internal_id, []):
            start, end = f["context_start"], f["context_end"]
            if start == end:
                continue
            if period_months(start, end) >= 10:
                periods.add((start, end))
            elif (internal_id, start, end) in self.annual_docs:
                periods.add((start, end))
        return sorted(periods)

    def fact_value(self, internal_id, element, period_start, period_end,
                   dim_class, unit):
        """期間一致と instant 一致の両方を探して勝者の値を返す。"""
        for start, end in ((period_start, period_end),
                           (period_end, period_end)):
            w = self.by_fact.get(
                (internal_id, element, start, end, dim_class, unit))
            if w is not None:
                return w
        return None

    def choose_candidate(self, internal_id, standard, ind):
        """会社×基準で採用する候補を固定する (系列の混在を避ける)。"""
        key = (internal_id, standard, ind["id"])
        if key in self.candidate_choice:
            return self.candidate_choice[key]
        candidates = (ind.get("elements") or {}).get(standard) or []
        chosen = None
        company_elements = {f["element"]
                            for f in self.company_facts.get(internal_id, [])}
        for cand in candidates:
            if any(el in company_elements
                   for el in candidate_elements(cand)):
                chosen = cand
                break
        self.candidate_choice[key] = chosen
        return chosen

    def element_value(self, internal_id, standard, ind, period_start,
                      period_end):
        """element 指標の値を取る。戻り値は (値, basis, 要素名, doc_id)。

        取れない場合は (None, None, None, None)。
        """
        unit = SOURCE_UNIT.get(ind["unit"])
        if unit is None:
            return None, None, None, None
        cand = self.choose_candidate(internal_id, standard, ind)
        if cand is None:
            return None, None, None, None
        elements = candidate_elements(cand)
        priority = ind.get("dimension_priority") or [
            "consolidated", "non_consolidated"]
        for dim_name in priority:
            dim_class = (CONSOLIDATED if dim_name == "consolidated"
                         else NON_CONSOLIDATED)
            total = 0.0
            docs = []
            ok = True
            for el in elements:
                w = self.fact_value(internal_id, el, period_start,
                                    period_end, dim_class, unit)
                if w is None:
                    ok = False
                    break
                total += w["value"]
                docs.append(w["doc_id"])
            if ok:
                value = total * (ind.get("multiply") or 1)
                return value, dim_class, "+".join(elements), docs[0]
        return None, None, None, None


    def compute_rows(self, internal_id, periods):
        """会社の年次行を組み立てる。戻り値は行 dict のリスト。

        element 指標を先に取り、formula / cagr / series は登録順に
        系列が揃ってから計算する。basis は self.basis_rows に積む。
        """
        rows = {}
        for (start, end) in periods:
            row = {"internal_id": internal_id, "period_start": start,
                   "period_end": end}
            standard = self.standard_for(internal_id, start, end)
            row["_standard"] = standard
            filled = False
            for ind in self.registry["indicators"]:
                if ind["kind"] != "element":
                    continue
                value, basis, element_name, doc_id = self.element_value(
                    internal_id, standard, ind, start, end)
                row[ind["id"]] = value
                if value is not None:
                    filled = True
                    self.basis_rows.append({
                        "internal_id": internal_id, "scope": "annual",
                        "period_start": start, "period_end": end,
                        "indicator_id": ind["id"], "basis": basis,
                        "element": element_name, "source_doc_id": doc_id})
            if filled:
                rows[(start, end)] = row
        # formula と cagr と series は系列で計算する
        ordered = [rows[k] for k in sorted(rows)]
        for ind in self.registry["indicators"]:
            iid = ind["id"]
            if ind["kind"] == "formula":
                for row in ordered:
                    try:
                        row[iid] = eval_formula(ind["formula"], row)
                    except (Missing, ValueError):
                        row[iid] = None
            elif ind["kind"] == "cagr":
                self._apply_cagr(ind, ordered)
            elif ind["kind"] == "series":
                self._apply_series(ind, ordered)
        return ordered

    def _apply_cagr(self, ind, ordered):
        """cagr の規則は試作の確定規則を引き継ぐ。"""
        base = ind["base_indicator"]
        window = ind.get("years", 5)
        series = [(r["period_end"], r[base]) for r in ordered
                  if r.get(base) is not None]
        for row in ordered:
            iid = ind["id"]
            if row.get(base) is None:
                row[iid] = None
                continue
            pts = [(p, v) for p, v in series
                   if p <= row["period_end"]][-window:]
            if len(pts) < 3:
                row[iid] = None
                continue
            (p0, v0), (p1, v1) = pts[0], pts[-1]
            if v0 <= 0 or v1 <= 0:
                row[iid] = None
                continue
            span = (date.fromisoformat(p1)
                    - date.fromisoformat(p0)).days / 365.25
            if span < 1.5:
                row[iid] = None
                continue
            row[iid] = ((v1 / v0) ** (1.0 / span) - 1.0) * 100.0

    def _apply_series(self, ind, ordered):
        """series の計算。現在は増配年数 (dividend_increase_years) のみ。

        隣接期の比較で、発行済株式数が両期に揃う場合は当期の配当を
        前期の株式数ベースに換算してから比較し、株式分割・併合による
        機械的な変動を除く (登録簿の adjustment の定義)。
        """
        iid = ind["id"]
        if iid != "dividend_increase_years":
            for row in ordered:
                row[iid] = None
            return
        base = ind["base_indicator"]
        prev = None  # (dps, shares) の前期分
        streak = 0
        for row in ordered:
            dps = row.get(base)
            shares = row.get("shares_outstanding")
            if dps is None:
                row[iid] = None
                streak = 0
                prev = None
                continue
            if prev is None:
                row[iid] = 0
                streak = 0
            else:
                prev_dps, prev_shares = prev
                comparable = dps
                if shares and prev_shares:
                    comparable = dps * shares / prev_shares
                if comparable > prev_dps:
                    streak += 1
                else:
                    streak = 0
                row[iid] = streak
            prev = (dps, shares)


    def quarterly_periods(self, internal_id):
        """四半期・半期書類に由来する累計期間と代表書類を返す。

        戻り値は {(period_start, period_end): (doc_type, doc_id)}。
        """
        found = {}
        for f in self.company_facts.get(internal_id, []):
            info = self.doc_map.get(f["doc_id"]) or {}
            if info.get("doc_type") not in QUARTERLY_DOC_TYPES:
                continue
            start, end = f["context_start"], f["context_end"]
            if start == end:
                continue
            # 四半期・半期書類のサマリには前年通年の値も再掲される。
            # 四半期表は累計期間 (12 か月未満) だけを対象とし、
            # 通年の期間は年次行の材料としてこちらには入れない
            if period_months(start, end) >= 12:
                continue
            key = (start, end)
            cur = found.get(key)
            if cur is None or (f["submit_at"] or "") > (cur[2] or ""):
                found[key] = (info["doc_type"], f["doc_id"],
                              f["submit_at"])
        return {k: (v[0], v[1]) for k, v in found.items()}

    def compute_quarterly_rows(self, internal_id):
        """四半期・半期の累計期間の行を組み立てる。"""
        rows = []
        periods = self.quarterly_periods(internal_id)
        inds = [self.indicators[i] for i in QUARTERLY_INDICATORS
                if i in self.indicators]
        for (start, end), (doc_type, doc_id) in sorted(periods.items()):
            standard = self.standard_for(internal_id, start, end)
            row = {"internal_id": internal_id, "period_start": start,
                   "period_end": end, "doc_type": doc_type,
                   "source_doc_id": doc_id}
            filled = False
            for ind in inds:
                value, basis, element_name, vdoc = self.element_value(
                    internal_id, standard, ind, start, end)
                row[ind["id"]] = value
                if value is not None:
                    filled = True
                    self.basis_rows.append({
                        "internal_id": internal_id, "scope": "quarterly",
                        "period_start": start, "period_end": end,
                        "indicator_id": ind["id"], "basis": basis,
                        "element": element_name, "source_doc_id": vdoc})
            if not filled:
                continue
            margin = self.indicators.get("operating_margin")
            if margin is not None:
                try:
                    row["operating_margin"] = eval_formula(
                        margin["formula"], row)
                except (Missing, ValueError):
                    row["operating_margin"] = None
            rows.append(row)
        return rows


def build_doc_lineage(index_rows, catalog_rows):
    """索引と目録から親 -> 最新の訂正書類の対応を作る。"""
    best = {}

    def consider(doc_id, parent, submit_at):
        if not parent:
            return
        key = (submit_at or "", doc_id)
        cur = best.get(parent)
        if cur is None or key > cur[0]:
            best[parent] = (key, doc_id)

    for r in index_rows:
        if r.get("doc_type") == "130":
            consider(r.get("doc_id"), r.get("parent_doc_id"),
                     r.get("submit_at"))
    for r in catalog_rows:
        if r.get("docTypeCode") == "130":
            consider(r.get("docID"), r.get("parentDocID"),
                     r.get("submitDateTime"))
    return {p: v[1] for p, v in best.items()}


# ---------------------------------------------------------------- 出力スキーマ


def annual_schema(registry):
    fields = [
        ("internal_id", pa.int64()), ("edinet_code", pa.string()),
        ("market", pa.string()),
        ("period_start", pa.string()), ("period_end", pa.string()),
        ("period_months", pa.int64()), ("is_irregular", pa.bool_()),
        ("listing_status", pa.string()),
        ("accounting_standard", pa.string()),
    ]
    fields += [(i["id"], pa.float64()) for i in registry["indicators"]]
    fields += [
        ("source_doc_id", pa.string()), ("has_correction", pa.bool_()),
        ("latest_correction_doc_id", pa.string()),
        ("source_kind", pa.string()), ("source_id", pa.string()),
        ("generated_at", pa.string()), ("redistribution", pa.string()),
    ]
    return pa.schema(fields)


BASIS_SCHEMA = pa.schema([
    ("internal_id", pa.int64()), ("scope", pa.string()),
    ("period_start", pa.string()), ("period_end", pa.string()),
    ("indicator_id", pa.string()), ("basis", pa.string()),
    ("element", pa.string()), ("source_doc_id", pa.string()),
    ("generated_at", pa.string()),
])

QUARTERLY_SCHEMA_HEAD = [
    ("internal_id", pa.int64()), ("edinet_code", pa.string()),
    ("market", pa.string()),
    ("period_start", pa.string()), ("period_end", pa.string()),
    ("period_months", pa.int64()), ("doc_type", pa.string()),
]
QUARTERLY_SCHEMA_TAIL = [
    ("source_doc_id", pa.string()), ("has_correction", pa.bool_()),
    ("source_kind", pa.string()), ("source_id", pa.string()),
    ("generated_at", pa.string()), ("redistribution", pa.string()),
]


def quarterly_schema():
    fields = list(QUARTERLY_SCHEMA_HEAD)
    fields += [(iid, pa.float64()) for iid in QUARTERLY_INDICATORS]
    fields += [("operating_margin", pa.float64())]
    fields += list(QUARTERLY_SCHEMA_TAIL)
    return pa.schema(fields)


DIVIDENDS_SCHEMA = pa.schema([
    ("internal_id", pa.int64()), ("edinet_code", pa.string()),
    ("market", pa.string()),
    ("securities_code", pa.string()), ("security_kind", pa.string()),
    ("period_start", pa.string()), ("period_end", pa.string()),
    ("term_kind", pa.string()), ("amount_per_share", pa.float64()),
    ("breakdown", pa.string()), ("is_derived", pa.bool_()),
    ("record_date", pa.string()), ("payment_date_planned", pa.string()),
    ("payment_date_actual", pa.string()),
    ("source_doc_id", pa.string()),
    ("source_kind", pa.string()), ("source_id", pa.string()),
    ("generated_at", pa.string()), ("redistribution", pa.string()),
])

FORECASTS_SCHEMA = pa.schema([
    ("forecast_id", pa.string()), ("internal_id", pa.int64()),
    ("edinet_code", pa.string()), ("market", pa.string()),
    ("source_kind", pa.string()), ("announced_at", pa.string()),
    ("period_start", pa.string()), ("period_end", pa.string()),
    ("item", pa.string()), ("value", pa.float64()),
    ("unit", pa.string()), ("revision_of", pa.string()),
    ("source_id", pa.string()), ("generated_at", pa.string()),
    ("redistribution", pa.string()),
])


def build_dividends(builder, primary_security):
    """配当の事実を組み立てる。

    源泉は経営指標サマリの年間配当実績と中間配当実績 (勝者の事実)。
    銘柄は会社マスタの主銘柄 (普通株式) に結ぶ。期末分は年間から
    中間を引いた導出として is_derived を立てて作る。
    """
    rows = []
    by_company = collections.defaultdict(list)
    for w in builder.winners.values():
        if w["element"] in (DIVIDEND_ANNUAL_ELEMENT,
                            DIVIDEND_INTERIM_ELEMENT):
            by_company[w["internal_id"]].append(w)
    for internal_id, facts in by_company.items():
        sec = primary_security.get(internal_id) or (None, None)
        # 勝者は (要素, 期間, 区分) で一意。区分は単体を優先する
        picked = {}
        for w in facts:
            key = (w["element"], w["context_start"], w["context_end"])
            cur = picked.get(key)
            rank = 0 if w["dim_class"] == NON_CONSOLIDATED else 1
            if cur is None or rank < cur[0]:
                picked[key] = (rank, w)
        annual = {}
        interim = {}
        for (element, start, end), (_, w) in picked.items():
            row = {
                "internal_id": internal_id,
                "edinet_code": w["edinet_code"],
                "market": MARKET_JP,
                "securities_code": sec[0], "security_kind": sec[1],
                "period_start": start, "period_end": end,
                "amount_per_share": w["value"],
                "breakdown": "unspecified", "is_derived": False,
                "record_date": None, "payment_date_planned": None,
                "payment_date_actual": None,
                "source_doc_id": w["doc_id"],
            }
            if element == DIVIDEND_ANNUAL_ELEMENT:
                row["term_kind"] = "annual"
                annual[(start, end)] = row
            else:
                row["term_kind"] = "interim"
                interim[(start, end)] = row
            rows.append(row)
        for (astart, aend), arow in annual.items():
            for (istart, iend), irow in interim.items():
                if istart == astart and iend < aend:
                    derived = dict(arow)
                    derived["term_kind"] = "final"
                    derived["amount_per_share"] = (
                        arow["amount_per_share"]
                        - irow["amount_per_share"])
                    derived["is_derived"] = True
                    rows.append(derived)
    return rows


# ---------------------------------------------------------------- マスタ


def load_master(master_dir):
    """会社マスタの読み込み。

    戻り値: (code_to_id, listing_status, primary_security)。
    listing_status は internal_id -> 最新の上場区分の値、
    primary_security は internal_id -> (securities_code, security_kind)。
    """
    code_to_id = {}
    table = pq.read_table(os.path.join(master_dir, "edinet_codes.parquet"))
    for r in table.to_pylist():
        code_to_id[r["edinet_code"]] = r["internal_id"]
    listing_status = {}
    path = os.path.join(master_dir, "attribute_history.parquet")
    if os.path.exists(path):
        table = pq.read_table(path)
        best = {}
        for r in table.to_pylist():
            if r["attribute"] != "listing_status":
                continue
            key = r["internal_id"]
            cur = best.get(key)
            if cur is None or (r["last_observed_date"] or "") > cur[0]:
                best[key] = (r["last_observed_date"], r["value"])
        listing_status = {k: v[1] for k, v in best.items()}
    primary_security = {}
    path = os.path.join(master_dir, "securities.parquet")
    if os.path.exists(path):
        table = pq.read_table(path)
        for r in table.to_pylist():
            if r.get("is_primary"):
                primary_security[r["internal_id"]] = (
                    r["securities_code"], r["security_kind"])
    return code_to_id, listing_status, primary_security


# ---------------------------------------------------------------- 生成の統括


def generate(registry, winners, doc_map, code_to_id, listing_status,
             primary_security, lineage, generated_at, internal_ids=None):
    """Gold の各表を生成する。戻り値は (tables, stats)。

    tables は annual / basis / quarterly / dividends / forecasts の
    pyarrow Table。internal_ids が与えられた場合はその会社の行だけを
    生成する (update 用)。
    """
    builder = GoldBuilder(registry, winners, doc_map, code_to_id)
    if internal_ids is None:
        internal_ids = sorted({w["internal_id"]
                               for w in winners.values()})
    annual_rows = []
    quarterly_rows = []
    coverage = collections.Counter()
    for internal_id in internal_ids:
        periods = builder.annual_periods(internal_id)
        rows = builder.compute_rows(internal_id, periods)
        facts = builder.company_facts.get(internal_id, [])
        edinet_code = facts[0]["edinet_code"] if facts else None
        for row in rows:
            row["edinet_code"] = edinet_code
            row["market"] = MARKET_JP
            months = period_months(row["period_start"], row["period_end"])
            row["period_months"] = months
            row["is_irregular"] = months != 12
            row["listing_status"] = listing_status.get(internal_id)
            row["accounting_standard"] = row.pop("_standard")
            docs = builder.annual_docs.get(
                (internal_id, row["period_start"], row["period_end"]), [])
            source_doc = None
            if docs:
                # 代表書類はその期間の書類のうち提出日時が最大のもの
                # (挿入順の docs[0] では恣意的な選択になるため)
                source_doc = max(
                    docs,
                    key=lambda d: (doc_map.get(d) or {}).get("submit_at")
                    or "")
            if source_doc is None:
                for ind in registry["indicators"]:
                    for b in builder.basis_rows:
                        if (b["internal_id"] == internal_id
                                and b["scope"] == "annual"
                                and b["period_end"] == row["period_end"]):
                            source_doc = b["source_doc_id"]
                            break
                    if source_doc:
                        break
            row["source_doc_id"] = source_doc
            correction = lineage.get(source_doc) if source_doc else None
            row["has_correction"] = correction is not None
            row["latest_correction_doc_id"] = correction
            for ind in registry["indicators"]:
                if row.get(ind["id"]) is not None:
                    coverage[ind["id"]] += 1
            annual_rows.append(row)
        for qrow in builder.compute_quarterly_rows(internal_id):
            qrow["edinet_code"] = edinet_code
            qrow["market"] = MARKET_JP
            qrow["period_months"] = period_months(
                qrow["period_start"], qrow["period_end"])
            qrow["has_correction"] = (
                lineage.get(qrow["source_doc_id"]) is not None)
            quarterly_rows.append(qrow)
    dividend_rows = build_dividends(builder, primary_security)
    if internal_ids is not None:
        keep = set(internal_ids)
        dividend_rows = [r for r in dividend_rows
                         if r["internal_id"] in keep]

    for row in annual_rows + quarterly_rows + dividend_rows:
        row["source_kind"] = SOURCE_KIND
        row["source_id"] = DEFAULT_REPO
        row["generated_at"] = generated_at
        row["redistribution"] = REDISTRIBUTION_ALLOWED
    annual_rows.sort(key=lambda r: (r["internal_id"], r["period_end"]))
    quarterly_rows.sort(key=lambda r: (r["internal_id"], r["period_end"]))
    dividend_rows.sort(key=lambda r: (r["internal_id"], r["period_end"],
                                      r["term_kind"]))
    basis_rows = [dict(b, generated_at=generated_at)
                  for b in builder.basis_rows]
    basis_rows.sort(key=lambda r: (r["internal_id"], r["scope"],
                                   r["period_end"], r["indicator_id"]))

    tables = {
        "annual": _to_table(annual_rows, annual_schema(registry)),
        "basis": _to_table(basis_rows, BASIS_SCHEMA),
        "quarterly": _to_table(quarterly_rows, quarterly_schema()),
        "dividends": _to_table(dividend_rows, DIVIDENDS_SCHEMA),
        "forecasts": _to_table([], FORECASTS_SCHEMA),
    }
    stats = {
        "annual_rows": len(annual_rows),
        "quarterly_rows": len(quarterly_rows),
        "dividend_rows": len(dividend_rows),
        "basis_rows": len(basis_rows),
        "companies": len({r["internal_id"] for r in annual_rows}),
        "indicator_coverage": {
            ind["id"]: coverage.get(ind["id"], 0)
            for ind in registry["indicators"]},
    }
    return tables, stats


def _to_table(rows, schema):
    data = {name: [r.get(name) for r in rows] for name in schema.names}
    return pa.Table.from_pydict(data, schema=schema)


# ---------------------------------------------------------------- 入出力


def month_watermark(silver_dir, month):
    """月の透かし (行数・最大提出日時・内容ハッシュ)。update の変化検出に使う。

    内容ハッシュ (sha256) は、行数と最大提出日時が変わらない
    値だけの書き換え (既存書類の再解析) も検出するために持つ。
    ファイルサイズだけでは不十分なため、バイト列のハッシュとする。
    """
    path = os.path.join(silver_dir, f"month={month}", "silver.parquet")
    rows = pq.read_metadata(path).num_rows
    table = pq.read_table(path, columns=["submit_at"])
    values = table.column("submit_at").to_pylist()
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"rows": rows, "max_submit_at": max(values) if values else None,
            "sha256": digest.hexdigest()}


def fetch_inputs(work_dir, months):
    """HF から入力一式を work_dir/inputs へ取得し、パスの辞書を返す。"""
    inputs = os.path.join(work_dir, "inputs")
    silver_dir = os.path.join(inputs, "silver")
    master_dir = os.path.join(inputs, "master")
    catalog_dir = os.path.join(inputs, "catalog")
    for d in (silver_dir, master_dir, catalog_dir):
        os.makedirs(d, exist_ok=True)
    for month in months:
        dst = os.path.join(silver_dir, f"month={month}")
        os.makedirs(dst, exist_ok=True)
        src = _with_retry("download silver", lambda m=month: hf_hub_download(
            DEFAULT_REPO, f"silver/month={m}/silver.parquet",
            repo_type="dataset"))
        _copy(src, os.path.join(dst, "silver.parquet"))
    api = HfApi()
    files = _with_retry("list files", lambda: api.list_repo_files(
        DEFAULT_REPO, repo_type="dataset"))
    for name in files:
        if name.startswith("master/") and (
                name.endswith(".parquet") or name.endswith(".json")):
            src = _with_retry("download master", lambda n=name:
                              hf_hub_download(DEFAULT_REPO, n,
                                              repo_type="dataset"))
            _copy(src, os.path.join(master_dir, os.path.basename(name)))
        elif name.startswith("catalog/month=") and name.endswith(
                "ledger.jsonl"):
            month_dir = name.split("/")[1]
            dst = os.path.join(catalog_dir, month_dir)
            os.makedirs(dst, exist_ok=True)
            src = _with_retry("download catalog", lambda n=name:
                              hf_hub_download(DEFAULT_REPO, n,
                                              repo_type="dataset"))
            _copy(src, os.path.join(dst, "ledger.jsonl"))
    index_src = _with_retry("download v1 index", lambda: hf_hub_download(
        V1_REPO, V1_INDEX_PATH, repo_type="dataset"))
    index_path = os.path.join(inputs, "v1_index.parquet")
    _copy(index_src, index_path)
    registry_path = os.path.join(inputs, "indicators.yaml")
    try:
        src = _with_retry("download registry", lambda: hf_hub_download(
            DEFAULT_REPO, REGISTRY_REPO_PATH, repo_type="dataset"))
        _copy(src, registry_path)
    except Exception:
        registry_path = None
    return {"silver_dir": silver_dir, "master_dir": master_dir,
            "catalog_dir": catalog_dir, "index": index_path,
            "registry": registry_path}


def _copy(src, dst):
    import shutil
    shutil.copyfile(src, dst)


def write_outputs(work_dir, tables, summary, state, snapshot_date):
    """gold/ へ書き、snapshots/date=<日>/ にも複写する。"""
    gold_dir = os.path.join(work_dir, "gold")
    os.makedirs(gold_dir, exist_ok=True)
    names = {"annual": "annual.parquet", "basis": "annual_basis.parquet",
             "quarterly": "quarterly.parquet",
             "dividends": "dividends.parquet",
             "forecasts": "forecasts.parquet"}
    written = []
    for key, filename in names.items():
        path = os.path.join(gold_dir, filename)
        pq.write_table(tables[key], path)
        written.append(path)
    for name, obj in (("summary.json", summary), ("state.json", state)):
        path = os.path.join(gold_dir, name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        written.append(path)
    snap_dir = os.path.join(gold_dir, "snapshots", f"date={snapshot_date}")
    os.makedirs(snap_dir, exist_ok=True)
    for path in written:
        _copy(path, os.path.join(snap_dir, os.path.basename(path)))
    return written


def upload_outputs(work_dir, snapshot_date):
    """gold/ の成果物を v2 データセットへ上げる。"""
    api = HfApi()
    gold_dir = os.path.join(work_dir, "gold")
    targets = []
    for name in os.listdir(gold_dir):
        path = os.path.join(gold_dir, name)
        if os.path.isfile(path):
            targets.append((path, f"{GOLD_PREFIX}/{name}"))
    snap_dir = os.path.join(gold_dir, "snapshots", f"date={snapshot_date}")
    for name in os.listdir(snap_dir):
        targets.append((os.path.join(snap_dir, name),
                        f"{GOLD_PREFIX}/snapshots/date={snapshot_date}/{name}"))
    for local, remote in targets:
        _with_retry("upload", lambda l=local, r=remote: api.upload_file(
            path_or_fileobj=l, path_in_repo=r, repo_id=DEFAULT_REPO,
            repo_type="dataset"))
    return [r for _, r in targets]


def load_existing_gold(work_dir):
    """既存の Gold (HF または work_dir/gold) を読み、表の辞書を返す。"""
    gold_dir = os.path.join(work_dir, "gold")
    names = {"annual": "annual.parquet", "basis": "annual_basis.parquet",
             "quarterly": "quarterly.parquet",
             "dividends": "dividends.parquet",
             "forecasts": "forecasts.parquet"}
    tables = {}
    for key, filename in names.items():
        path = os.path.join(gold_dir, filename)
        if not os.path.exists(path):
            try:
                src = _with_retry("download gold", lambda f=filename:
                                  hf_hub_download(
                                      DEFAULT_REPO, f"{GOLD_PREFIX}/{f}",
                                      repo_type="dataset"))
                os.makedirs(gold_dir, exist_ok=True)
                _copy(src, path)
            except Exception:
                tables[key] = None
                continue
        tables[key] = pq.read_table(path)
    state = None
    state_path = os.path.join(gold_dir, "state.json")
    if not os.path.exists(state_path):
        try:
            src = _with_retry("download state", lambda: hf_hub_download(
                DEFAULT_REPO, f"{GOLD_PREFIX}/state.json",
                repo_type="dataset"))
            os.makedirs(gold_dir, exist_ok=True)
            _copy(src, state_path)
        except Exception:
            state_path = None
    if state_path and os.path.exists(state_path):
        with open(state_path, encoding="utf-8") as f:
            state = json.load(f)
    return tables, state


def _read_doc_context(paths):
    """索引と目録を読み、doc_map と系譜を作る。"""
    index_rows = []
    if paths.get("index"):
        index_rows = pq.read_table(paths["index"]).to_pylist()
    catalog_rows = load_catalog_rows(paths.get("catalog_dir"))
    doc_map = build_doc_map(index_rows, catalog_rows)
    lineage = build_doc_lineage(index_rows, catalog_rows)
    return doc_map, lineage


def _prepare(paths, registry_path):
    registry = load_registry(registry_path)
    code_to_id, listing_status, primary_security = load_master(
        paths["master_dir"])
    doc_map, lineage = _read_doc_context(paths)
    return registry, code_to_id, listing_status, primary_security, \
        doc_map, lineage


# ---------------------------------------------------------------- コマンド


def cmd_build(args):
    generated_at = _now()
    snapshot_date = generated_at[:10]
    paths = _resolve_paths(args)
    months = [m for m in silver_months(paths["silver_dir"])
              if m >= args.since_month]
    registry, code_to_id, listing_status, primary_security, doc_map, \
        lineage = _prepare(paths, args.registry or paths.get("registry"))
    wanted = registry_elements(registry) | {
        DIVIDEND_ANNUAL_ELEMENT, DIVIDEND_INTERIM_ELEMENT}
    facts = collect_facts(paths["silver_dir"], months, wanted)
    winners, fact_stats = resolve_winners(facts, doc_map, code_to_id)
    tables, stats = generate(registry, winners, doc_map, code_to_id,
                             listing_status, primary_security, lineage,
                             generated_at)
    state = {"generated_at": generated_at,
             "registry_sha256": registry["_sha256"],
             "registry_version": registry.get("version"),
             "months": {m: month_watermark(paths["silver_dir"], m)
                        for m in months}}
    summary = _make_summary(registry, stats, fact_stats, generated_at,
                            snapshot_date, months)
    written = write_outputs(args.work_dir, tables, summary, state,
                            snapshot_date)
    uploaded = upload_outputs(args.work_dir, snapshot_date) \
        if args.upload else []
    return {"mode": "build", "summary": summary,
            "written": [os.path.basename(p) for p in written],
            "uploaded": uploaded}


def cmd_update(args):
    generated_at = _now()
    snapshot_date = generated_at[:10]
    paths = _resolve_paths(args)
    months = [m for m in silver_months(paths["silver_dir"])
              if m >= args.since_month]
    registry, code_to_id, listing_status, primary_security, doc_map, \
        lineage = _prepare(paths, args.registry or paths.get("registry"))
    existing, state = load_existing_gold(args.work_dir)
    old_months = (state or {}).get("months", {})
    watermarks = {m: month_watermark(paths["silver_dir"], m)
                  for m in months}
    changed = [m for m in months
               if old_months.get(m) != watermarks[m]]
    if not changed:
        return {"mode": "update", "changed_months": [],
                "note": "no changes"}
    # 変化した月に現れた書類の会社を影響会社とする
    changed_docs = set()
    for month in changed:
        table = pq.read_table(
            os.path.join(paths["silver_dir"], f"month={month}",
                         "silver.parquet"), columns=["doc_id"])
        changed_docs.update(table.column("doc_id").to_pylist())
    affected = set()
    for doc_id in changed_docs:
        info = doc_map.get(doc_id)
        if info and info.get("edinet_code") in code_to_id:
            affected.add(code_to_id[info["edinet_code"]])
    if not affected:
        return {"mode": "update", "changed_months": changed,
                "affected_companies": 0, "note": "no affected companies"}
    # 影響会社の書類がある月だけを読み直す
    doc_months = set()
    affected_docs = set()
    for doc_id, info in doc_map.items():
        if info.get("edinet_code") in code_to_id and \
                code_to_id[info["edinet_code"]] in affected:
            affected_docs.add(doc_id)
            if info.get("submit_at"):
                doc_months.add(info["submit_at"][:7])
    target_months = [m for m in months if m in doc_months]
    wanted = registry_elements(registry) | {
        DIVIDEND_ANNUAL_ELEMENT, DIVIDEND_INTERIM_ELEMENT}
    facts = collect_facts(paths["silver_dir"], target_months, wanted,
                          doc_ids=affected_docs)
    winners, fact_stats = resolve_winners(facts, doc_map, code_to_id)
    new_tables, stats = generate(
        registry, winners, doc_map, code_to_id, listing_status,
        primary_security, lineage, generated_at,
        internal_ids=sorted(affected))
    tables = {}
    for key in ("annual", "basis", "quarterly", "dividends"):
        old = existing.get(key)
        if old is None or old.num_rows == 0:
            tables[key] = new_tables[key]
            continue
        keep_mask = pc.invert(pc.is_in(
            old.column("internal_id"),
            value_set=pa.array(sorted(affected), type=pa.int64())))
        tables[key] = pa.concat_tables(
            [old.filter(keep_mask), new_tables[key]])
    tables["forecasts"] = existing.get("forecasts") \
        if existing.get("forecasts") is not None \
        else new_tables["forecasts"]
    state = {"generated_at": generated_at,
             "registry_sha256": registry["_sha256"],
             "registry_version": registry.get("version"),
             "months": watermarks}
    # summary の件数は再生成した影響会社分ではなく、結合後の表全体で
    # 数え直す (部分集合の統計を全体の統計と誤表示しないため)
    summary = _make_summary(registry, stats, fact_stats, generated_at,
                            snapshot_date, months)
    summary["annual_rows"] = tables["annual"].num_rows
    summary["quarterly_rows"] = tables["quarterly"].num_rows
    summary["dividend_rows"] = tables["dividends"].num_rows
    summary["basis_rows"] = tables["basis"].num_rows
    annual = tables["annual"]
    summary["companies"] = len(set(
        annual.column("internal_id").to_pylist()))
    summary["indicator_coverage"] = {
        ind["id"]: sum(1 for v in annual.column(ind["id"]).to_pylist()
                       if v is not None)
        for ind in registry["indicators"]}
    summary["update"] = {"changed_months": changed,
                         "affected_companies": len(affected)}
    written = write_outputs(args.work_dir, tables, summary, state,
                            snapshot_date)
    uploaded = upload_outputs(args.work_dir, snapshot_date) \
        if args.upload else []
    return {"mode": "update", "changed_months": changed,
            "affected_companies": len(affected), "summary": summary,
            "written": [os.path.basename(p) for p in written],
            "uploaded": uploaded}


def _make_summary(registry, stats, fact_stats, generated_at,
                  snapshot_date, months):
    return {
        "generated_at": generated_at,
        "snapshot_date": snapshot_date,
        "registry_version": registry.get("version"),
        "registry_sha256": registry["_sha256"],
        "source_months": months,
        "annual_rows": stats["annual_rows"],
        "quarterly_rows": stats["quarterly_rows"],
        "dividend_rows": stats["dividend_rows"],
        "basis_rows": stats["basis_rows"],
        "companies": stats["companies"],
        "indicator_coverage": stats["indicator_coverage"],
        "facts": fact_stats["facts"],
        "ignored_dimensions": fact_stats["ignored_dimensions"],
        "unresolved_docs": fact_stats["unresolved_docs"],
        "unresolved_doc_ids": fact_stats["unresolved_doc_ids"][:100],
        "same_value_discarded": fact_stats["same_value_discarded"],
        "differing_value_discarded":
            fact_stats["differing_value_discarded"],
        "correction_superseded": fact_stats["correction_superseded"],
    }


def cmd_sync_registry(args):
    """ローカルの登録簿ファイルを HF の写しへ同期する。"""
    with open(args.registry_file, "rb") as f:
        data = f.read()
    digest = sha256_bytes(data)
    if args.upload:
        api = HfApi()
        import io
        _with_retry("upload registry", lambda: api.upload_file(
            path_or_fileobj=io.BytesIO(data), path_in_repo=REGISTRY_REPO_PATH,
            repo_id=DEFAULT_REPO, repo_type="dataset"))
    return {"mode": "sync-registry", "registry_sha256": digest,
            "uploaded": bool(args.upload)}


def _resolve_paths(args):
    if args.silver_dir:
        return {"silver_dir": args.silver_dir,
                "master_dir": args.master_dir,
                "catalog_dir": args.catalog_dir,
                "index": args.index,
                "registry": args.registry}
    months = _remote_months(args.since_month)
    return fetch_inputs(args.work_dir, months)


def _remote_months(since_month):
    api = HfApi()
    files = _with_retry("list files", lambda: api.list_repo_files(
        DEFAULT_REPO, repo_type="dataset"))
    months = set()
    for name in files:
        if name.startswith("silver/month=") and name.endswith(
                "silver.parquet"):
            month = name.split("/")[1][len("month="):]
            if month >= since_month:
                months.add(month)
    return sorted(months)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="pipeline.gold")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("build", "update"):
        p = sub.add_parser(name)
        p.add_argument("--work-dir", required=True)
        p.add_argument("--upload", action="store_true")
        p.add_argument("--since-month", default=DEFAULT_SINCE_MONTH)
        p.add_argument("--silver-dir")
        p.add_argument("--master-dir")
        p.add_argument("--catalog-dir")
        p.add_argument("--index")
        p.add_argument("--registry")
    p = sub.add_parser("sync-registry")
    p.add_argument("--work-dir")
    p.add_argument("--upload", action="store_true")
    p.add_argument("--registry-file", required=True)
    args = parser.parse_args(argv)
    if args.command == "build":
        result = cmd_build(args)
    elif args.command == "update":
        result = cmd_update(args)
    else:
        result = cmd_sync_registry(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
