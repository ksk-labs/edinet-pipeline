#!/usr/bin/env python3
"""T3: XBRL -> Silver (型つき事実) の変換スクリプト。Step 1 試作。

入力: 標本の目録 (sample_manifest.csv)。各書類の zip は HF 上の日別 tar
(raw/edinet/YYYY/MM/DD.tar) の中の zip/<書類ID>.zip にある。
出力: Silver の parquet と、対象外要素の分類報告・実行ログ。

変換の規則:
- 金額は数値型。XBRL の scale 属性を適用する (value = text * 10**scale)。
- 数値でない事実 (文字列・日付・真偽値) は Silver に入れず、対象外として分類する。
- nil の事実は値なしとして分類する。
- 時点値と確定値の分離: 120 (有価証券報告書) の事実は point_in_time='as_filed'。
  130 (訂正有価証券報告書) の事実は point_in_time='final' で、訂正の系譜として
  parent_doc_id を保持する。訂正のない 120 の事実は as_filed がそのまま確定値
  (訂正が存在しないため) であり、行の重複は作らない。
- 訂正の判定は doc_type=130 と parent_doc_id で行う。索引の is_amendment 列は
  全行 True の欠陥があり使えない。
"""
import csv
import hashlib
import io
import json
import os
import re
import sys
import tarfile
import zipfile
from decimal import Decimal, InvalidOperation

from lxml import etree

MANIFEST = os.environ.get("MANIFEST", os.path.expanduser(
    "~/workspace/goals/edinet/files/2026-11-15-step1-sample-manifest.csv"))
TARS_DIR = os.environ.get("TARS_DIR", os.path.expanduser(
    "~/workspace/goals/edinet/hidden_files/t3/tars"))
OUT_DIR = os.environ.get("OUT_DIR", os.path.expanduser(
    "~/workspace/goals/edinet/hidden_files/t3/output"))
INDEX_PARQUET = os.environ.get("INDEX_PARQUET", os.path.expanduser(
    "~/workspace/goals/edinet/hidden_files/t1/documents_index.parquet"))

XBRL_NS = "http://www.xbrl.org/2003/instance"
LINK_NS = "http://www.xbrl.org/2003/linkbase"
IX_NS = "http://www.xbrl.org/2008/inlineXBRL"


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_parent_map():
    """訂正報告書 (130) の parent_doc_id を索引から読む。"""
    import duckdb
    con = duckdb.connect()
    rows = con.execute(
        "SELECT doc_id, parent_doc_id FROM read_parquet(?) "
        "WHERE doc_type='130' AND parent_doc_id IS NOT NULL",
        [INDEX_PARQUET]).fetchall()
    return {d: p for d, p in rows}


def day_tar_path(raw_zip_path):
    m = re.match(r"raw/edinet/year=(\d+)/month=(\d+)/day=(\d+)/zip/", raw_zip_path)
    if not m:
        return None
    y, mo, d = m.groups()
    return os.path.join(TARS_DIR, f"{y}-{mo}-{d}.tar")


def extract_doc_zip(tar_path, doc_id):
    """日別 tar から zip/<docID>.zip を取り出し、バイト列で返す。

    tar が途中で切れている等で読めない場合は TarError を送出する。
    呼び出し側で捕捉し、その書類を「tar unreadable」として記録して次へ進む
    （1件の破損が全体を止めない）。
    """
    member = f"zip/{doc_id}.zip"
    with tarfile.open(tar_path, "r") as tf:
        try:
            f = tf.extractfile(member)
        except KeyError:
            return None
        if f is None:
            return None
        return f.read()


def parse_contexts(root):
    """context 要素を辞書化する。key: context id -> (start, end, dimension)。"""
    contexts = {}
    for ctx in root.iter(f"{{{XBRL_NS}}}context"):
        cid = ctx.get("id")
        period = ctx.find(f"{{{XBRL_NS}}}period")
        start = end = None
        if period is not None:
            inst = period.find(f"{{{XBRL_NS}}}instant")
            if inst is not None:
                start = end = (inst.text or "").strip()
            else:
                s = period.find(f"{{{XBRL_NS}}}startDate")
                e = period.find(f"{{{XBRL_NS}}}endDate")
                start = (s.text or "").strip() if s is not None else None
                end = (e.text or "").strip() if e is not None else None
        dims = []
        entity = ctx.find(f"{{{XBRL_NS}}}entity")
        if entity is not None:
            seg = entity.find(f"{{{XBRL_NS}}}segment")
            if seg is not None:
                for dm in seg.iter(f"{{{XBRL_NS}}}explicitMember"):
                    dims.append((dm.text or "").strip())
        contexts[cid] = (start, end, ";".join(dims))
    return contexts


def parse_units(root):
    units = {}
    for u in root.iter(f"{{{XBRL_NS}}}unit"):
        uid = u.get("id")
        measure = u.find(f"{{{XBRL_NS}}}measure")
        if measure is not None:
            units[uid] = (measure.text or "").strip()
        else:
            # divide 型 (例: JPY / shares) は measure を連結する
            div = u.find(f"{{{XBRL_NS}}}divide")
            if div is not None:
                nums = [m.text for m in div.iter(f"{{{XBRL_NS}}}unitNumerator")
                        for m in [m.find(f"{{{XBRL_NS}}}measure")] if m is not None]
                dens = [m.text for m in div.iter(f"{{{XBRL_NS}}}unitDenominator")
                        for m in [m.find(f"{{{XBRL_NS}}}measure")] if m is not None]
                units[uid] = "/".join(nums) + " per " + "/".join(dens) if dens else "/".join(nums)
            else:
                units[uid] = ""
    return units


def parse_xbrl(data, doc_id, out_of_scope):
    """1つの .xbrl ファイルを解析し、(facts, stats) を返す。"""
    root = etree.fromstring(data)
    contexts = parse_contexts(root)
    units = parse_units(root)
    facts = []
    stats = {"numeric": 0, "non_numeric": 0, "nil": 0, "unparseable": 0, "no_context": 0}
    for el in root:
        tag = el.tag
        if not isinstance(tag, str) or tag.startswith("{"):
            ns, _, local = tag[1:].partition("}") if tag.startswith("{") else ("", "", tag)
        else:
            ns, local = "", tag
        # xbrl インスタンス直下の context/unit/schemaRef 等は事実ではない
        if ns in (XBRL_NS, LINK_NS):
            continue
        if el.get(f"{{{XBRL_NS}}}nil") == "true" or el.get("nil") == "true":
            stats["nil"] += 1
            out_of_scope["nil"].append((doc_id, local))
            continue
        ctx_ref = el.get("contextRef")
        unit_ref = el.get("unitRef")
        if ctx_ref is None or ctx_ref not in contexts:
            stats["no_context"] += 1
            out_of_scope["no_context"].append((doc_id, local))
            continue
        text = (el.text or "").strip()
        if unit_ref is None:
            # 単位のない事実 (文字列・日付・真偽値など) は対象外
            stats["non_numeric"] += 1
            out_of_scope["non_numeric"].append((doc_id, local))
            continue
        try:
            num = Decimal(text.replace(",", ""))
        except InvalidOperation:
            stats["unparseable"] += 1
            out_of_scope["unparseable"].append((doc_id, local, text[:80]))
            continue
        scale = el.get("scale")
        if scale is not None:
            try:
                num = num * (Decimal(10) ** int(scale))
            except (ValueError, InvalidOperation):
                stats["unparseable"] += 1
                out_of_scope["unparseable"].append((doc_id, local, f"bad scale {scale}"))
                continue
        start, end, dims = contexts[ctx_ref]
        facts.append({
            "element": f"{ns.split('/')[-1] if ns else ''}:{local}" if ns else local,
            "element_ns": ns,
            "value": float(num),
            "value_raw": str(num),
            "unit": units.get(unit_ref, ""),
            "context_start": start,
            "context_end": end,
            "dimension": dims,
            "context_ref": ctx_ref,
        })
        stats["numeric"] += 1
    return facts, stats


def parse_inline_xbrl(data, doc_id, out_of_scope):
    """Inline XBRL (.htm) の ix:nonFraction を抽出する。"""
    root = etree.fromstring(data, parser=etree.HTMLParser())
    facts = []
    stats = {"numeric": 0, "non_numeric": 0, "nil": 0, "unparseable": 0, "no_context": 0}
    # HTML パーサでは名前空間が落ちるため、タグ名で探す
    for el in root.iter():
        if not isinstance(el.tag, str):
            continue
        local = el.tag.split("}")[-1].split(":")[-1]
        if local not in ("nonFraction", "nonNumeric"):
            continue
        name = el.get("name", "")
        if local == "nonNumeric":
            stats["non_numeric"] += 1
            out_of_scope["non_numeric"].append((doc_id, name))
            continue
        ctx_ref = el.get("contextRef")
        unit_ref = el.get("unitRef")
        text = (el.text or "").strip()
        try:
            num = Decimal(text.replace(",", ""))
        except InvalidOperation:
            stats["unparseable"] += 1
            out_of_scope["unparseable"].append((doc_id, name, text[:80]))
            continue
        scale = el.get("scale")
        if scale is not None:
            try:
                num = num * (Decimal(10) ** int(scale))
            except (ValueError, InvalidOperation):
                stats["unparseable"] += 1
                out_of_scope["unparseable"].append((doc_id, name, f"bad scale {scale}"))
                continue
        facts.append({
            "element": name,
            "element_ns": "",
            "value": float(num),
            "value_raw": str(num),
            "unit": unit_ref or "",
            "context_start": None,
            "context_end": None,
            "dimension": "",
            "context_ref": ctx_ref or "",
        })
        stats["numeric"] += 1
    return facts, stats


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    parent_map = load_parent_map()
    manifest = list(csv.DictReader(open(MANIFEST, encoding="utf-8")))
    out_of_scope = {"non_numeric": [], "nil": [], "unparseable": [], "no_context": []}
    rows = []
    log = {"docs": [], "missing_zip": [], "tar_hashes": {}, "stats_total": {}}
    # 訂正の系譜: 120 -> それを訂正する 130
    superseded_by = {}
    for m in manifest:
        if m["cell"] == "AMEND-130":
            parent = parent_map.get(m["doc_id"])
            if parent:
                superseded_by[parent] = m["doc_id"]
    for m in manifest:
        doc_id = m["doc_id"]
        cell = m["cell"]
        is_correction = cell == "AMEND-130"
        parent_doc_id = parent_map.get(doc_id) if is_correction else None
        tar_path = day_tar_path(m["raw_zip_path"])
        entry = {"doc_id": doc_id, "cell": cell, "tar": tar_path,
                 "submit_at": m["submit_at"], "status": ""}
        if tar_path is None or not os.path.exists(tar_path):
            entry["status"] = "tar missing"
            log["missing_zip"].append(doc_id)
            log["docs"].append(entry)
            continue
        if tar_path not in log["tar_hashes"]:
            log["tar_hashes"][tar_path] = sha256_file(tar_path)
        try:
            zip_bytes = extract_doc_zip(tar_path, doc_id)
        except (tarfile.TarError, OSError) as e:
            entry["status"] = f"tar unreadable: {type(e).__name__}"
            log["missing_zip"].append(doc_id)
            log["docs"].append(entry)
            continue
        if zip_bytes is None:
            entry["status"] = "zip not in tar"
            log["missing_zip"].append(doc_id)
            log["docs"].append(entry)
            continue
        doc_facts = []
        doc_stats = {}
        try:
            zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        except zipfile.BadZipFile:
            entry["status"] = "bad zip"
            log["missing_zip"].append(doc_id)
            log["docs"].append(entry)
            continue
        xbrl_names = [n for n in zf.namelist()
                      if n.lower().endswith(".xbrl")]
        htm_names = [n for n in zf.namelist()
                     if n.lower().endswith((".htm", ".html")) and "ixbrl" in n.lower()]
        # Inline XBRL は .htm の中に ix: 名前空間があるもので判定する
        for n in zf.namelist():
            if n.lower().endswith((".htm", ".html")) and n not in htm_names:
                head = zf.read(n, )[:4096].decode("utf-8", "ignore")
                if "inlineXBRL" in head or "ix:" in head:
                    htm_names.append(n)
        for n in xbrl_names:
            facts, stats = parse_xbrl(zf.read(n), doc_id, out_of_scope)
            doc_facts.extend(facts)
            for k, v in stats.items():
                doc_stats[k] = doc_stats.get(k, 0) + v
        for n in htm_names:
            facts, stats = parse_inline_xbrl(zf.read(n), doc_id, out_of_scope)
            doc_facts.extend(facts)
            for k, v in stats.items():
                doc_stats[k] = doc_stats.get(k, 0) + v
        # 重複の除去 (同一 element/context/unit/value)
        seen = {}
        for f in doc_facts:
            key = (f["element"], f["context_ref"], f["unit"], f["value_raw"])
            if key in seen:
                if seen[key] != f["value_raw"]:
                    out_of_scope.setdefault("conflict", []).append(
                        (doc_id, f["element"], f["context_ref"]))
                continue
            seen[key] = f["value_raw"]
        point = "final" if is_correction else "as_filed"
        for f in doc_facts:
            key = (f["element"], f["context_ref"], f["unit"], f["value_raw"])
            if seen.get(key) != f["value_raw"]:
                continue
            rows.append({
                "doc_id": doc_id,
                "cell": cell,
                "element": f["element"],
                "value": f["value"],
                "unit": f["unit"],
                "context_start": f["context_start"],
                "context_end": f["context_end"],
                "dimension": f["dimension"],
                "submit_at": m["submit_at"],
                "is_correction": is_correction,
                "parent_doc_id": parent_doc_id,
                "superseded_by": superseded_by.get(doc_id),
                "point_in_time": point,
            })
        entry["status"] = "ok"
        entry["facts"] = len(doc_facts)
        entry["stats"] = doc_stats
        entry["xbrl_files"] = len(xbrl_names) + len(htm_names)
        log["docs"].append(entry)
        for k, v in doc_stats.items():
            log["stats_total"][k] = log["stats_total"].get(k, 0) + v
    # parquet へ書き出す
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.Table.from_pylist(rows) if rows else pa.table({
        "doc_id": [], "cell": [], "element": [], "value": [], "unit": [],
        "context_start": [], "context_end": [], "dimension": [],
        "submit_at": [], "is_correction": [], "parent_doc_id": [],
        "superseded_by": [], "point_in_time": []})
    out_parquet = os.path.join(OUT_DIR, "silver_sample.parquet")
    pq.write_table(table, out_parquet)
    log["rows"] = len(rows)
    log["parquet_sha256"] = sha256_file(out_parquet)
    log["out_of_scope_counts"] = {k: len(v) for k, v in out_of_scope.items()}
    with open(os.path.join(OUT_DIR, "run_log.json"), "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    # 対象外の分類報告
    lines = ["# 対象外要素の分類報告（T3）\n"]
    for kind in ("non_numeric", "nil", "unparseable", "no_context", "conflict"):
        items = out_of_scope.get(kind, [])
        lines.append(f"## {kind}: {len(items)}件\n")
        from collections import Counter
        cnt = Counter(x[1] for x in items if len(x) > 1)
        for elem, n in cnt.most_common(30):
            lines.append(f"- {elem}: {n}件")
        lines.append("")
    with open(os.path.join(OUT_DIR, "out_of_scope_elements.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"docs ok: {sum(1 for d in log['docs'] if d['status']=='ok')}/{len(manifest)}")
    print(f"rows: {len(rows)}")
    print(f"stats: {log['stats_total']}")
    print(f"missing: {log['missing_zip']}")


if __name__ == "__main__":
    main()
