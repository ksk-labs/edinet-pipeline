"""v1 索引から EDINET コード・証券コード・法人番号・会社名の履歴を再構成する (P2-T2)。

v1 索引 (catalog/documents_index.parquet) の各行を「提出日に、提出者について
識別子と名称が観測された」出来事として扱い、(EDINET コード, 種別, 値) 単位に
集約して会社マスタの初期履歴を作る。あわせて法人番号の共有によるコード間の
接続候補 (確定候補と要確認) を切り分ける。

設計の正本は作業記録 (docs/records/2026-11-17-p2-t2-reconstruction-work-record.md)。
使い方: reconstruct_v1.py <索引 parquet のパス> <出力ディレクトリ>
"""

import collections
import hashlib
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

SOURCE_KIND = "v1_index"
SOURCE_DATASET = "Yoshi-Dai/financial-lakehouse"
SOURCE_FILE = "catalog/documents_index.parquet"

CORP_FORMS = [
    "一般社団法人", "公益社団法人", "一般財団法人", "公益財団法人",
    "特定非営利活動法人", "株式会社", "有限会社", "合同会社", "合資会社",
    "合名会社", "医療法人", "学校法人", "社会福祉法人", "投資法人", "協同組合",
]
_ABBREV_FORMS = {
    "(株)": "株式会社", "(有)": "有限会社", "(名)": "合名会社",
    "(資)": "合資会社", "(同)": "合同会社",
}


def normalize_name(name):
    """名称の正規化 (仕様 §1: 全角・半角、空白、法人格の表記)。"""
    s = unicodedata.normalize("NFKC", name)
    s = re.sub(r"\s+", "", s)
    for abbr, full in _ABBREV_FORMS.items():
        s = s.replace(abbr, full)
    return s


def change_key(name):
    """変更判定用のキー。正規化し、英字を小文字化し、前後の法人格を除く。"""
    s = normalize_name(name).casefold()
    changed = True
    while changed:
        changed = False
        for form in CORP_FORMS:
            if s.startswith(form):
                s = s[len(form):]
                changed = True
            if s.endswith(form):
                s = s[:-len(form)]
                changed = True
    return s


def main(argv):
    index_path, out_dir = argv[1], argv[2]
    with open(index_path, "rb") as f:
        source_sha = hashlib.sha256(f.read()).hexdigest()
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    provenance = {
        "source_kind": SOURCE_KIND,
        "source_dataset": SOURCE_DATASET,
        "source_file": SOURCE_FILE,
        "source_file_sha256": source_sha,
        "generated_at": generated_at,
        "redistributable": True,
    }

    table = pq.read_table(
        index_path,
        columns=["doc_id", "edinet_code", "code", "jcn", "company_name", "submit_at"],
    )
    d = table.to_pydict()
    total_rows = table.num_rows

    # (edinet_code, kind, value) -> 集約
    agg = {}
    roster = {}
    for doc_id, ec, code, jcn, name, submit_at in zip(
        d["doc_id"], d["edinet_code"], d["code"], d["jcn"],
        d["company_name"], d["submit_at"],
    ):
        obs_date = submit_at[:10]
        r = roster.setdefault(ec, {
            "first": obs_date, "last": obs_date, "docs": 0,
            "names": set(), "jcns": set(), "secs": set(),
        })
        r["docs"] += 1
        r["first"] = min(r["first"], obs_date)
        r["last"] = max(r["last"], obs_date)

        observations = []
        if code:
            value = code.split(":", 1)[1] if ":" in code else code
            observations.append(("securities_code", value, code))
            r["secs"].add(value)
        if jcn:
            observations.append(("corporate_number", jcn, jcn))
            r["jcns"].add(jcn)
        if name:
            observations.append(("company_name", name, name))
            r["names"].add(name)
        for kind, value, source_value in observations:
            key = (ec, kind, value)
            a = agg.get(key)
            if a is None:
                agg[key] = {
                    "source_value": source_value, "first": obs_date,
                    "last": obs_date, "count": 1,
                    "first_doc": doc_id, "last_doc": doc_id,
                }
            else:
                a["count"] += 1
                if obs_date < a["first"]:
                    a["first"], a["first_doc"] = obs_date, doc_id
                if obs_date > a["last"]:
                    a["last"], a["last_doc"] = obs_date, doc_id

    history_rows = []
    for (ec, kind, value), a in sorted(agg.items()):
        is_name = kind == "company_name"
        history_rows.append({
            "edinet_code": ec,
            "kind": kind,
            "value": value,
            "source_value": a["source_value"],
            "value_normalized": normalize_name(value) if is_name else None,
            "change_key": change_key(value) if is_name else None,
            "is_placeholder": kind == "securities_code" and value == "00000",
            "first_observed_date": a["first"],
            "last_observed_date": a["last"],
            "observation_count": a["count"],
            "first_doc_id": a["first_doc"],
            "last_doc_id": a["last_doc"],
            **provenance,
        })

    roster_rows = [{
        "edinet_code": ec,
        "first_observed_date": r["first"],
        "last_observed_date": r["last"],
        "document_count": r["docs"],
        "distinct_name_count": len(r["names"]),
        "distinct_jcn_count": len(r["jcns"]),
        "distinct_securities_code_count": len(r["secs"]),
        **provenance,
    } for ec, r in sorted(roster.items())]

    # コードごとの法人番号の観測 (接続判定の材料)
    jcn_by_code = collections.defaultdict(dict)   # code -> {jcn: count}
    names_by_code = collections.defaultdict(set)  # code -> {change_key}
    secs_by_code = collections.defaultdict(set)   # code -> {value}
    for (ec, kind, value), a in agg.items():
        if kind == "corporate_number":
            jcn_by_code[ec][value] = a["count"]
        elif kind == "company_name":
            names_by_code[ec].add(change_key(value))
        elif kind == "securities_code" and value != "00000":
            secs_by_code[ec].add(value)

    codes_by_jcn = collections.defaultdict(set)
    for ec, jcns in jcn_by_code.items():
        for jcn in jcns:
            codes_by_jcn[jcn].add(ec)

    connections = []
    review_items = []

    def add_review(item_type, a, b, shared, detail):
        review_items.append({
            "item_type": item_type, "edinet_code_a": a, "edinet_code_b": b,
            "shared_value": shared, "detail": detail, **provenance,
        })

    linked_pairs = set()
    for jcn, codes in sorted(codes_by_jcn.items()):
        if len(codes) < 2:
            continue
        codes = sorted(codes)
        for i, ca in enumerate(codes):
            for cb in codes[i + 1:]:
                linked_pairs.add((ca, cb))
                rows_a, rows_b = jcn_by_code[ca][jcn], jcn_by_code[cb][jcn]
                majority_a = jcn == max(jcn_by_code[ca], key=jcn_by_code[ca].get)
                majority_b = jcn == max(jcn_by_code[cb], key=jcn_by_code[cb].get)
                name_agree = bool(names_by_code[ca] & names_by_code[cb])
                sec_conflict = bool(
                    secs_by_code[ca] and secs_by_code[cb]
                    and not (secs_by_code[ca] & secs_by_code[cb])
                )
                if rows_a >= 2 and rows_b >= 2 and majority_a and majority_b:
                    strength = "strong"
                elif (majority_a and majority_b and name_agree and not sec_conflict):
                    strength = "corroborated"
                else:
                    add_review(
                        "jcn_link_needs_review", ca, cb, jcn,
                        f"法人番号 {jcn} の観測は {ca} で {rows_a} 行、{cb} で {rows_b} 行。"
                        f"多数派一致: {ca}={majority_a}, {cb}={majority_b}。"
                        f"名称一致: {name_agree}、証券コード矛盾: {sec_conflict}。"
                        "自動接続の条件を満たさないため未確定として残す。",
                    )
                    continue
                connections.append({
                    "edinet_code_a": ca, "edinet_code_b": cb,
                    "basis": "corporate_number", "shared_value": jcn,
                    "rows_a": rows_a, "rows_b": rows_b, "strength": strength,
                    "name_agreement": name_agree,
                    "note": "法人番号の共有による接続候補 (内部 ID の統合は P2-T3)",
                    **provenance,
                })

    for ec, jcns in sorted(jcn_by_code.items()):
        if len(jcns) > 1:
            detail = "、".join(
                f"{j} ({c} 行)" for j, c in
                sorted(jcns.items(), key=lambda kv: -kv[1])
            )
            add_review(
                "jcn_within_code_conflict", ec, None, None,
                f"同一コードに複数の法人番号が観測された: {detail}。"
                "上書きせず併記し、多数の観測を優先する (仕様 §1)。",
            )

    codes_by_name_key = collections.defaultdict(set)
    for ec, keys in names_by_code.items():
        for k in keys:
            codes_by_name_key[k].add(ec)
    name_candidate_rows = 0
    name_candidate_pairs = set()
    for key, codes in sorted(codes_by_name_key.items()):
        if len(codes) < 2:
            continue
        codes = sorted(codes)
        for i, ca in enumerate(codes):
            for cb in codes[i + 1:]:
                if (ca, cb) in linked_pairs:
                    continue
                name_candidate_rows += 1
                name_candidate_pairs.add((ca, cb))
                add_review(
                    "name_only_candidate", ca, cb, key,
                    "正規化した名称が一致するが、法人番号では結べない。"
                    "未確定の候補として残す (仕様 §1)。",
                )

    for (ec, kind, value), a in sorted(agg.items()):
        if kind == "securities_code" and value == "00000":
            add_review(
                "placeholder_securities_code", ec, None, value,
                f"証券コードのプレースホルダとみられる値 00000 が {a['count']} 行で"
                "観測された。履歴には観測として保持し、照合には使わない。",
            )

    import os
    os.makedirs(out_dir, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(history_rows),
                   f"{out_dir}/identifier_history.parquet")
    pq.write_table(pa.Table.from_pylist(roster_rows),
                   f"{out_dir}/code_roster.parquet")
    pq.write_table(pa.Table.from_pylist(connections),
                   f"{out_dir}/code_connections.parquet")
    pq.write_table(pa.Table.from_pylist(review_items),
                   f"{out_dir}/review_items.parquet")

    summary = {
        "input_rows": total_rows,
        "source_file_sha256": source_sha,
        "generated_at": generated_at,
        "target_codes": len(roster_rows),
        "history_rows": len(history_rows),
        "history_rows_by_kind": dict(collections.Counter(
            r["kind"] for r in history_rows)),
        "codes_with_multiple_raw_names": sum(
            1 for r in roster_rows if r["distinct_name_count"] > 1),
        "codes_with_multiple_change_keys": sum(
            1 for ec, keys in names_by_code.items() if len(keys) > 1),
        "codes_without_jcn": sum(
            1 for r in roster_rows if r["distinct_jcn_count"] == 0),
        "connections": len(connections),
        "connections_by_strength": dict(collections.Counter(
            c["strength"] for c in connections)),
        "review_items": len(review_items),
        "review_items_by_type": dict(collections.Counter(
            r["item_type"] for r in review_items)),
        "name_only_candidate_pairs": len(name_candidate_pairs),
        "name_only_candidate_rows": name_candidate_rows,
    }
    with open(f"{out_dir}/summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main(sys.argv)
