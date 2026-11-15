#!/usr/bin/env python3
"""T5: Silver -> Gold（会社×年度の横持ち + 指標）の生成スクリプト。Step 1 試作。

入力: Silver（型つき事実）、標本の目録（会社情報）、指標登録簿（edinet-definitions の
indicators.yaml と同じ形式）。出力: Gold の parquet と、生成統計の JSON。

処理の規則:
- 事実の確定: 同一の (会社, 要素, 期間, 単位) に複数の事実がある場合は、
  (1) 訂正報告書の事実（point_in_time='final'）、(2) 提出日時の新しいもの、
  (3) Silver の格納順で先に現れたもの、の優先順で1つに決める。
  格納順の先頭は、原文インスタンスで次元を持たない連結本表の値が最初に現れる
  ことを実物で確認済み（T5 作業記録）。セグメント値や単体の値はこの規則で落ちる。
- 要素の対応: 指標ごとに、会社の会計基準に対応する候補要素のうち、その会社の
  事実が存在する最初の候補を採用する（会社単位で決め、系列の混在を避ける）。
- 通貨の指標は単位 iso4217:JPY の事実だけを、比率の指標（ROE）は xbrli:pure の
  事実だけを使う。他の単位の事実は数えて報告し、採用しない。
- 年度は決算日（context_end）で表し、fiscal_year は決算日の年とする。
- 登録簿の kind:
    element: 要素から直接取る（multiply があれば掛ける）。
    formula: 他の指標への四則演算の式を評価する。材料が欠損した年度は欠損。
    cagr:    base_indicator の系列から年平均成長率を計算する。
- 出力は (edinet_code, period_end) で整列し、列順は登録簿の登録順に固定する
  （再実行で同一の成果物になるため）。
"""
import argparse
import ast
import csv
import hashlib
import json
import os
import sys
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

STANDARD_KEYS = {"Japan GAAP": "japan_gaap", "IFRS": "ifrs", "US GAAP": "us_gaap"}
# 指標の単位から、採用する Silver の単位を決める（試作の規則。本表は docstring 参照）
SOURCE_UNIT = {"円": "iso4217:JPY", "%": "xbrli:pure"}


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest(path):
    docs = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            docs[row["doc_id"]] = row
    return docs


def resolve_facts(silver_path, docs, wanted_elements):
    """Silver を読み、(会社, 要素, 開始, 終了, 単位) ごとに勝者の事実を決める。

    戻り値: (winners, stats)
      winners: dict key=(edinet_code, element, context_start, context_end, unit)
               -> {value, doc_id, submit_at, point_in_time}
    """
    table = pq.read_table(silver_path)
    rows = table.to_pylist()  # ファイルの格納順を保持する
    by_key = {}
    total = 0
    for idx, r in enumerate(rows):
        if r["element"] not in wanted_elements:
            continue
        doc = docs.get(r["doc_id"])
        if doc is None:
            continue
        total += 1
        key = (doc["edinet_code"], r["element"], r["context_start"],
               r["context_end"], r["unit"])
        cand = {
            "value": r["value"], "doc_id": r["doc_id"],
            "submit_at": r["submit_at"], "point_in_time": r["point_in_time"],
            "row_idx": idx,
        }
        by_key.setdefault(key, []).append(cand)
    winners = {}
    stats = {"facts_for_registry_elements": total, "winner_facts": 0,
             "discarded_same_value": 0, "discarded_differing_value": 0,
             "correction_superseded": 0}
    for key, cands in by_key.items():
        # 安定ソートを重ねて優先順を実現する: 格納順 -> 提出日時降順 -> final 優先
        ordered = sorted(cands, key=lambda c: c["row_idx"])
        ordered = sorted(ordered, key=lambda c: c["submit_at"], reverse=True)
        ordered = sorted(ordered,
                         key=lambda c: 0 if c["point_in_time"] == "final" else 1)
        winner = ordered[0]
        winners[key] = winner
        stats["winner_facts"] += 1
        for other in ordered[1:]:
            if other["value"] == winner["value"]:
                stats["discarded_same_value"] += 1
            else:
                stats["discarded_differing_value"] += 1
                if (winner["point_in_time"] == "final"
                        and other["point_in_time"] != "final"):
                    stats["correction_superseded"] += 1
    return winners, stats


class Missing(Exception):
    """式の材料が欠損している、またはゼロ除算になった。"""


def eval_formula(expr, env):
    """四則演算と指標識別子だけの式を評価する。欠損・ゼロ除算は Missing。"""
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


def main():
    ap = argparse.ArgumentParser()
    base = os.path.expanduser("~/workspace/goals/edinet")
    ap.add_argument("--silver", default=os.path.expanduser(
        "~/workspace/goals/edinet/hidden_files/t3/output/silver_sample.parquet"))
    ap.add_argument("--manifest", default=os.path.join(
        base, "files/2026-11-15-step1-sample-manifest.csv"))
    ap.add_argument("--registry", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--stats-out", default=None)
    args = ap.parse_args()

    with open(args.registry, encoding="utf-8") as f:
        registry = yaml.safe_load(f)
    indicators = registry["indicators"]
    docs = load_manifest(args.manifest)

    # 会社情報（提出が最新の書類から会社名・証券コード・会計基準を取る）
    companies = {}
    for doc in sorted(docs.values(), key=lambda d: d["submit_at"]):
        companies[doc["edinet_code"]] = {
            "company_name": doc["company_name"],
            "securities_code": doc["code"] or None,
            "accounting_standard": doc["accounting_standard"],
        }

    wanted = set()
    for ind in indicators:
        for cands in (ind.get("elements") or {}).values():
            wanted.update(cands)
    winners, fact_stats = resolve_facts(args.silver, docs, wanted)

    # 指標（element 種別）: 会社ごとに候補要素を決め、値の系列を作る
    values = {}          # (indicator_id) -> {(edinet_code, period_end): value}
    chosen_element = {}  # (indicator_id, edinet_code) -> element or None
    element_missing = {} # indicator_id -> {reason: count} (行単位で後で数える)
    for ind in indicators:
        if ind["kind"] != "element":
            continue
        iid = ind["id"]
        source_unit = SOURCE_UNIT.get(ind["unit"])
        multiply = ind.get("multiply", 1)
        series = {}
        for code, comp in companies.items():
            std_key = STANDARD_KEYS[comp["accounting_standard"]]
            candidates = (ind.get("elements") or {}).get(std_key, [])
            chosen = None
            for cand in candidates:
                # 単位の絞り込み: winners のキーは単位を含むため、ここで確認する
                usable = {}
                for (c2, el, start, end, unit), w in winners.items():
                    if c2 == code and el == cand and unit == source_unit:
                        usable[end] = w
                if usable:
                    chosen = cand
                    for end, w in usable.items():
                        series[(code, end)] = w["value"] * multiply
                    break
            chosen_element[(iid, code)] = chosen
        values[iid] = series

    # Gold の行: element 種別の指標が値を取れた (会社, 決算日) の和集合
    row_keys = set()
    for ind in indicators:
        if ind["kind"] == "element":
            row_keys.update(values[ind["id"]].keys())
    row_keys = sorted(row_keys)

    # 行ごとの土台
    gold = {}
    for code, period_end in row_keys:
        comp = companies[code]
        gold[(code, period_end)] = {
            "edinet_code": code,
            "company_name": comp["company_name"],
            "securities_code": comp["securities_code"],
            "accounting_standard": comp["accounting_standard"],
            "period_end": period_end,
            "fiscal_year": int(period_end[:4]),
            "source_doc_id": None,
        }
        for ind in indicators:
            if ind["kind"] == "element":
                gold[(code, period_end)][ind["id"]] = values[ind["id"]].get(
                    (code, period_end))
    # source_doc_id: その行に値を与えた最初の element 指標の勝者の書類
    for (code, period_end), row in gold.items():
        for ind in indicators:
            if ind["kind"] != "element":
                continue
            el = chosen_element.get((ind["id"], code))
            if el is None:
                continue
            for (c2, el2, start, end, unit), w in winners.items():
                if c2 == code and el2 == el and end == period_end \
                        and unit == SOURCE_UNIT.get(ind["unit"]):
                    row["source_doc_id"] = w["doc_id"]
                    break
            if row["source_doc_id"]:
                break

    # 指標（formula / cagr 種別）を登録順に計算する
    indicator_stats = {}
    for ind in indicators:
        iid = ind["id"]
        if ind["kind"] == "element":
            comp_rows = sum(1 for v in values[iid].values() if v is not None)
            missing = {"no_element_for_company": 0, "no_fact_for_period": 0}
            for (code, period_end), row in gold.items():
                if row[iid] is None:
                    if chosen_element.get((iid, code)) is None:
                        missing["no_element_for_company"] += 1
                    else:
                        missing["no_fact_for_period"] += 1
            indicator_stats[iid] = {"computable_rows": comp_rows,
                                    "missing": missing}
        elif ind["kind"] == "formula":
            missing = {"operand_missing": 0, "division_by_zero": 0}
            n = 0
            for row in gold.values():
                env = {k: row.get(k) for k in row}
                try:
                    row[iid] = eval_formula(ind["formula"], env)
                    n += 1
                except Missing as e:
                    row[iid] = None
                    if str(e) == "division by zero":
                        missing["division_by_zero"] += 1
                    else:
                        missing["operand_missing"] += 1
            indicator_stats[iid] = {"computable_rows": n, "missing": missing}
        elif ind["kind"] == "cagr":
            base = ind["base_indicator"]
            window = ind.get("years", 5)
            missing = {"base_value_missing": 0, "insufficient_series": 0,
                       "nonpositive_endpoint": 0}
            n = 0
            by_company = {}
            for (code, period_end), row in gold.items():
                by_company.setdefault(code, []).append((period_end, row))
            for code, rows_ in by_company.items():
                rows_.sort(key=lambda x: x[0])
                series = [(pe, r[base]) for pe, r in rows_
                          if r.get(base) is not None]
                for pe, row in rows_:
                    if row.get(base) is None:
                        row[iid] = None
                        missing["base_value_missing"] += 1
                        continue
                    pts = [(p, v) for p, v in series if p <= pe][-window:]
                    if len(pts) < 3:
                        row[iid] = None
                        missing["insufficient_series"] += 1
                        continue
                    (p0, v0), (p1, v1) = pts[0], pts[-1]
                    if v0 <= 0 or v1 <= 0:
                        row[iid] = None
                        missing["nonpositive_endpoint"] += 1
                        continue
                    span = (date.fromisoformat(p1) - date.fromisoformat(p0)).days / 365.25
                    if span < 1.5:
                        row[iid] = None
                        missing["insufficient_series"] += 1
                        continue
                    row[iid] = ((v1 / v0) ** (1.0 / span) - 1.0) * 100.0
                    n += 1
            indicator_stats[iid] = {"computable_rows": n, "missing": missing}

    # 出力（列順は登録順、行は整列済み）
    columns = ["edinet_code", "company_name", "securities_code",
               "accounting_standard", "period_end", "fiscal_year",
               "source_doc_id"] + [ind["id"] for ind in indicators]
    data = {c: [] for c in columns}
    for key in row_keys:
        row = gold[key]
        for c in columns:
            data[c].append(row.get(c))
    schema = pa.schema([
        ("edinet_code", pa.string()), ("company_name", pa.string()),
        ("securities_code", pa.string()), ("accounting_standard", pa.string()),
        ("period_end", pa.string()), ("fiscal_year", pa.int64()),
        ("source_doc_id", pa.string()),
    ] + [(ind["id"], pa.float64()) for ind in indicators])
    table = pa.Table.from_pydict(data, schema=schema)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    pq.write_table(table, args.out)

    chosen_summary = {}
    for ind in indicators:
        if ind["kind"] != "element":
            continue
        per_element = {}
        for (iid, code), el in chosen_element.items():
            if iid == ind["id"] and el:
                per_element[el] = per_element.get(el, 0) + 1
        chosen_summary[ind["id"]] = per_element

    stats = {
        "silver": args.silver,
        "silver_sha256": sha256_file(args.silver),
        "registry": args.registry,
        "registry_sha256": sha256_file(args.registry),
        "registry_version": registry.get("version"),
        "companies_in_sample": len(companies),
        "gold_rows": table.num_rows,
        "gold_columns": columns,
        "gold_sha256": sha256_file(args.out),
        "fact_resolution": fact_stats,
        "indicators": indicator_stats,
        "chosen_elements": chosen_summary,
    }
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    if args.stats_out:
        with open(args.stats_out, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
