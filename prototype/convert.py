#!/usr/bin/env python3
"""T3: XBRL -> Silver (型つき事実) の変換スクリプト。Step 1 試作。

入力: 標本の目録 (sample_manifest.csv)。各書類の zip は HF 上の日別 tar
(raw/edinet/YYYY/MM/DD.tar) の中の zip/<書類ID>.zip にある。
出力: Silver の parquet と、対象外要素の分類報告・実行ログ。

変換の規則:
- 金額は数値型。XBRL の scale 属性を適用する (value = text * 10**scale)。
- 数値でない事実 (文字列・日付・真偽値) は Silver に入れず、対象外として分類する。
- nil の事実は値なしとして分類する。nil の判定は xsi:nil
  (http://www.w3.org/2001/XMLSchema-instance の属性 nil="true") で行う。
- 時点値と確定値の分離: 120 (有価証券報告書) の事実は point_in_time='as_filed'。
  130 (訂正有価証券報告書) の事実は point_in_time='final' で、訂正の系譜として
  parent_doc_id を保持する。訂正のない 120 の事実は as_filed がそのまま確定値
  (訂正が存在しないため) であり、行の重複は作らない。
- 訂正の判定は doc_type=130 と parent_doc_id で行う。索引の is_amendment 列は
  全行 True の欠陥があり使えない。
- .xbrl インスタンスと Inline XBRL (.htm) の両方から抽出し、重複は
  (要素・context・単位・値) で除去する。Inline XBRL の context/unit 定義は
  ix:header 内の ix:resources (ヘッダーファイル 0000000_header_*_ixbrl.htm 等)
  から読む。
- 次元情報は context の segment と scenario の両方から xbrldi の
  explicitMember / typedMember を読み、dimension 列に保持する。

Step 2 塊1 の修正 (判定報告の open_caveats への対応):
(a) nil 判定の名前空間を xsi:nil に修正 (従来は XBRL インスタンス名前空間で
    探していたため nil が unparseable に落ちていた)。
(b) Inline XBRL 抽出の実働化: .htm を XHTML として XML 解析し、名前空間つきで
    ix:nonFraction / ix:nonNumeric を拾う (従来は HTML パーサのタグ小文字化で
    一致せず寄与 0 件だった)。sign="-" は負値、format の日付変換は数値でない
    事実として分類し、ixt:numunitdecimal (例: 「87円56銭」-> 87.56) にも対応する。
(c) Silver の次元情報: segment と scenario の両方から次元メンバーを読む
    (従来は segment のみ、かつ explicitMember の名前空間も取り違えていて
    dimension は全行空だった)。
(d) 拡張要素名の正規化: 提出者拡張要素の名前空間の末尾には提出日等が入る
    (例: .../E00988-000/2026-03-31/01/2026-06-24) ため、同一要素が書類ごとに
    別物に見えていた。正規化の規則は normalize_prefix の docstring と
    実行ログ (run_log.md) に明記する。
(e) 重複除去の数値キー化 (Step 2 塊1 の独立照合フォロー): 従来は値の文字列形
    (value_raw) を重複除去のキーにしていたため、符号付きゼロ (-0.0 と 0.0) や
    小数表記の違い (802.0 に scale=6 を適用した値と 802000000) が別行として
    残っていた。事実の取り込み時点で符号付きゼロを 0 に正規化し、重複除去の
    キーは scale 適用後の Decimal の値そのもの (value_dec) に変更した。
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
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
XBRDI_NS = "http://xbrl.org/2006/xbrldi"

NORMALIZATION_RULE = (
    "要素名の正規化規則 (Step 2 塊1 (d)): 要素の接頭辞は名前空間 URI から作る。"
    "URI のパス末尾のセグメントが日付 (YYYY-MM-DD) または数字のみである間は"
    "それを取り除き続け、残った末尾のセグメントを接頭辞とする。"
    "例: http://disclosure.edinet-fsa.go.jp/jpcrp030000/asr/001/E00988-000"
    "/2026-03-31/01/2026-06-24 -> 接頭辞 E00988-000。"
    "標準タクソノミ (例: .../taxonomy/jppfs/2025-11-01/jppfs_cor) は末尾が"
    "日付でも数字でもないため変わらない (接頭辞 jppfs_cor)。"
    "Inline XBRL の name 属性は、文書の xmlns 宣言で名前空間 URI へ解決してから"
    "同じ規則を適用する。"
)

_DATE_SEG = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_NUM_SEG = re.compile(r"^\d+$")


def normalize_prefix(ns_uri):
    """名前空間 URI から要素の接頭辞を作る。規則は NORMALIZATION_RULE のとおり。"""
    if not ns_uri:
        return ""
    segs = [s for s in ns_uri.split("/") if s]
    while len(segs) > 1 and (_DATE_SEG.match(segs[-1]) or _NUM_SEG.match(segs[-1])):
        segs.pop()
    return segs[-1] if segs else ""


def is_nil(el):
    """xsi:nil="true" かどうか (修正 (a))。"""
    return el.get(f"{{{XSI_NS}}}nil") == "true"


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


def _dimension_string(ctx):
    """context の segment と scenario から次元メンバーの文字列を作る (修正 (c))。

    explicitMember はその値 (QName 文字列) を、typedMember はテキスト内容を
    使い、";" で連結する。連結/単体を区別するメンバー
    (ConsolidatedMember / NonConsolidatedMember 等) もそのまま含まれる。
    """
    parts = []
    for container_name in ("segment", "scenario"):
        for container in ctx.iter(f"{{{XBRL_NS}}}{container_name}"):
            for dm in container.iter():
                if dm.tag == f"{{{XBRDI_NS}}}explicitMember":
                    v = (dm.text or "").strip()
                    if v:
                        parts.append(v)
                elif dm.tag == f"{{{XBRDI_NS}}}typedMember":
                    v = "".join(dm.itertext()).strip()
                    if not v and len(dm):
                        v = dm[0].tag.split("}")[-1]
                    if v:
                        parts.append(v)
    return ";".join(parts)


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
        contexts[cid] = (start, end, _dimension_string(ctx))
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


def _new_stats():
    return {"numeric": 0, "non_numeric": 0, "nil": 0, "unparseable": 0, "no_context": 0}


def parse_xbrl(data, doc_id, out_of_scope):
    """1つの .xbrl ファイルを解析し、(facts, stats) を返す。"""
    root = etree.fromstring(data)
    contexts = parse_contexts(root)
    units = parse_units(root)
    facts = []
    stats = _new_stats()
    for el in root:
        tag = el.tag
        if not isinstance(tag, str):
            continue
        if tag.startswith("{"):
            ns, _, local = tag[1:].partition("}")
        else:
            ns, local = "", tag
        # xbrl インスタンス直下の context/unit/schemaRef 等は事実ではない
        if ns in (XBRL_NS, LINK_NS):
            continue
        if is_nil(el):
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
        if num == 0:
            # 符号付きゼロ (-0) を 0 に正規化する (修正 (e))
            num = Decimal(0)
        start, end, dims = contexts[ctx_ref]
        facts.append({
            "element": f"{normalize_prefix(ns)}:{local}" if ns else local,
            "element_ns": ns,
            "value": float(num),
            "value_raw": format(num, "f"),
            "value_dec": num,
            "unit": units.get(unit_ref, ""),
            "context_start": start,
            "context_end": end,
            "dimension": dims,
            "context_ref": ctx_ref,
            "source": "xbrl",
        })
        stats["numeric"] += 1
    return facts, stats


_YEN_SEN = re.compile(r"^(\d+)円(\d+)銭$")


def _parse_ix_number(text, fmt_local):
    """Inline XBRL の表示文字列を Decimal にする。失敗時は InvalidOperation。"""
    t = re.sub(r"[\s　]+", "", text)
    if fmt_local == "numunitdecimal":
        # ixt:numunitdecimal: 単位つき表記 (EDINET では「87円56銭」= 87.56)
        m = _YEN_SEN.match(t.replace(",", ""))
        if m:
            return Decimal(m.group(1)) + Decimal(m.group(2)) / Decimal(100)
        t = t.replace(",", "").replace("円", "")
        return Decimal(t)
    if fmt_local == "numcommadecimal":
        t = t.replace(".", "").replace(",", ".")
    else:
        t = t.replace(",", "")
    return Decimal(t)


def parse_inline_xbrl(root, doc_id, out_of_scope, contexts, units):
    """Inline XBRL (.htm を XML として解析した木) から事実を抽出する (修正 (b))。

    contexts / units は ix:header 内 ix:resources から集めたものを渡す。
    name 属性は文書の xmlns 宣言で名前空間 URI へ解決し、正規化した接頭辞で
    .xbrl 側と同じ要素名を作る (重複除去が効くようにするため)。
    """
    facts = []
    stats = _new_stats()
    continuations = {}
    for el in root.iter(f"{{{IX_NS}}}continuation"):
        if el.get("id"):
            continuations[el.get("id")] = "".join(el.itertext())
    for el in root.iter():
        if el.tag not in (f"{{{IX_NS}}}nonFraction", f"{{{IX_NS}}}nonNumeric"):
            continue
        name = el.get("name", "")
        prefix, _, local = name.partition(":")
        ns_uri = el.nsmap.get(prefix) if prefix else None
        element = f"{normalize_prefix(ns_uri)}:{local}" if ns_uri else name
        if is_nil(el):
            stats["nil"] += 1
            out_of_scope["nil"].append((doc_id, local or name))
            continue
        if el.tag == f"{{{IX_NS}}}nonNumeric":
            stats["non_numeric"] += 1
            out_of_scope["non_numeric"].append((doc_id, local or name))
            continue
        ctx_ref = el.get("contextRef")
        unit_ref = el.get("unitRef")
        if ctx_ref is None or ctx_ref not in contexts:
            stats["no_context"] += 1
            out_of_scope["no_context"].append((doc_id, local or name))
            continue
        if unit_ref is None:
            stats["non_numeric"] += 1
            out_of_scope["non_numeric"].append((doc_id, local or name))
            continue
        fmt = el.get("format") or ""
        fmt_local = fmt.split(":")[-1] if fmt else ""
        if fmt_local.startswith("date"):
            # 日付の変換規則を持つ事実は数値でない (期間の日付など)
            stats["non_numeric"] += 1
            out_of_scope["non_numeric"].append((doc_id, local or name))
            continue
        text = "".join(el.itertext())
        cont = el.get("continuedAt")
        if cont and cont in continuations:
            text += continuations[cont]
        try:
            num = _parse_ix_number(text, fmt_local)
        except InvalidOperation:
            stats["unparseable"] += 1
            out_of_scope["unparseable"].append((doc_id, local or name, text.strip()[:80]))
            continue
        if el.get("sign") == "-":
            num = -num
        scale = el.get("scale")
        if scale is not None:
            try:
                num = num * (Decimal(10) ** int(scale))
            except (ValueError, InvalidOperation):
                stats["unparseable"] += 1
                out_of_scope["unparseable"].append((doc_id, local or name, f"bad scale {scale}"))
                continue
        if num == 0:
            # sign="-" と本文 "0" の組み合わせ等で生じる符号付きゼロを
            # 0 に正規化する (修正 (e))
            num = Decimal(0)
        start, end, dims = contexts[ctx_ref]
        facts.append({
            "element": element,
            "element_ns": ns_uri or "",
            "value": float(num),
            "value_raw": format(num, "f"),
            "value_dec": num,
            "unit": units.get(unit_ref, ""),
            "context_start": start,
            "context_end": end,
            "dimension": dims,
            "context_ref": ctx_ref,
            "source": "ixbrl",
        })
        stats["numeric"] += 1
    return facts, stats


def _load_previous(out_dir):
    """上書き前の出力 (前回の実行結果) を修正前として読み取る。無ければ None。"""
    prev = {"rows": None, "unparseable": None, "nil": None,
            "unparseable_xbrl": None, "nil_xbrl": None,
            "dimension_nonempty_rows": None, "ixbrl_new_rows": 0,
            "parquet_sha256": None}
    log_path = os.path.join(out_dir, "run_log.json")
    pq_path = os.path.join(out_dir, "silver_sample.parquet")
    if os.path.exists(log_path):
        try:
            old = json.load(open(log_path, encoding="utf-8"))
            prev["rows"] = old.get("rows")
            prev["parquet_sha256"] = old.get("parquet_sha256")
            st = old.get("stats_total", {})
            prev["unparseable"] = st.get("unparseable")
            prev["nil"] = st.get("nil")
            # 由来別 (.xbrl のみ) の値と比較表の補助項目は、前回ログの
            # stats_by_source と ixbrl_new_rows から読む (全体値で代用しない)
            by_src = old.get("stats_by_source", {}).get("xbrl", {})
            prev["unparseable_xbrl"] = by_src.get("unparseable")
            prev["nil_xbrl"] = by_src.get("nil")
            if old.get("ixbrl_new_rows") is not None:
                prev["ixbrl_new_rows"] = old["ixbrl_new_rows"]
            if old.get("dimension_nonempty_rows") is not None:
                prev["dimension_nonempty_rows"] = old["dimension_nonempty_rows"]
        except (ValueError, OSError):
            pass
    if prev["dimension_nonempty_rows"] is None and os.path.exists(pq_path):
        try:
            import pyarrow.parquet as pq
            t = pq.read_table(pq_path, columns=["dimension"])
            dims = t.column("dimension").to_pylist()
            prev["dimension_nonempty_rows"] = sum(1 for d in dims if d)
        except Exception:
            pass
    if prev["rows"] is None:
        return None
    return prev


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    previous = _load_previous(OUT_DIR)
    parent_map = load_parent_map()
    manifest = list(csv.DictReader(open(MANIFEST, encoding="utf-8")))
    out_of_scope = {"non_numeric": [], "nil": [], "unparseable": [], "no_context": []}
    rows = []
    log = {"docs": [], "missing_zip": [], "tar_hashes": {}, "stats_total": {},
           "stats_by_source": {"xbrl": _new_stats(), "ixbrl": _new_stats()},
           "normalization_rule": NORMALIZATION_RULE}
    # 訂正の系譜: 120 -> それを訂正する 130
    superseded_by = {}
    for m in manifest:
        if m["cell"] == "AMEND-130":
            parent = parent_map.get(m["doc_id"])
            if parent:
                superseded_by[parent] = m["doc_id"]
    ix_parser = etree.XMLParser(no_network=True)
    total_ix_new = 0
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
        doc_stats = _new_stats()
        doc_stats_by_source = {"xbrl": _new_stats(), "ixbrl": _new_stats()}
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
                doc_stats_by_source["xbrl"][k] += v
        # Inline XBRL: ディレクトリ (PublicDoc / AuditDoc) ごとに、
        # ix:resources の context/unit 定義を集めてから事実を抽出する
        ix_parse_errors = []
        groups = {}
        for n in htm_names:
            groups.setdefault(os.path.dirname(n), []).append(n)
        for group_names in groups.values():
            roots = {}
            for n in group_names:
                try:
                    roots[n] = etree.fromstring(zf.read(n), parser=ix_parser)
                except etree.XMLSyntaxError:
                    ix_parse_errors.append(n)
            contexts_ix = {}
            units_ix = {}
            for root in roots.values():
                for res in root.iter(f"{{{IX_NS}}}resources"):
                    contexts_ix.update(parse_contexts(res))
                    units_ix.update(parse_units(res))
            for root in roots.values():
                facts, stats = parse_inline_xbrl(
                    root, doc_id, out_of_scope, contexts_ix, units_ix)
                doc_facts.extend(facts)
                for k, v in stats.items():
                    doc_stats[k] = doc_stats.get(k, 0) + v
                    doc_stats_by_source["ixbrl"][k] += v
        if ix_parse_errors:
            entry["ix_parse_errors"] = ix_parse_errors
        # 重複の除去 (同一 element/context/unit/数値)。先勝ち。
        # キーの値は文字列形ではなく scale 適用後の Decimal そのもの (修正 (e))。
        # .xbrl を先に処理するため、残った ixbrl 由来の行が「ixbrl のみ」になる。
        seen = set()
        point = "final" if is_correction else "as_filed"
        doc_rows = 0
        doc_ix_new = 0
        for f in doc_facts:
            key = (f["element"], f["context_ref"], f["unit"], f["value_dec"])
            if key in seen:
                continue
            seen.add(key)
            if f["source"] == "ixbrl":
                doc_ix_new += 1
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
            doc_rows += 1
        total_ix_new += doc_ix_new
        entry["status"] = "ok"
        entry["facts"] = doc_rows
        entry["extracted_facts"] = len(doc_facts)
        entry["ixbrl_new"] = doc_ix_new
        entry["stats"] = doc_stats
        entry["stats_by_source"] = doc_stats_by_source
        entry["xbrl_files"] = len(xbrl_names) + len(htm_names)
        log["docs"].append(entry)
        for k, v in doc_stats.items():
            log["stats_total"][k] = log["stats_total"].get(k, 0) + v
        for src in ("xbrl", "ixbrl"):
            for k, v in doc_stats_by_source[src].items():
                log["stats_by_source"][src][k] += v
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
    log["dimension_nonempty_rows"] = sum(1 for r in rows if r["dimension"])
    log["ixbrl_new_rows"] = total_ix_new
    # 修正前後の比較 (修正前は上書き前に読み取った前回の出力)
    after = {"rows": log["rows"],
             "unparseable": log["stats_total"].get("unparseable"),
             "unparseable_xbrl": log["stats_by_source"]["xbrl"].get("unparseable"),
             "nil": log["stats_total"].get("nil"),
             "nil_xbrl": log["stats_by_source"]["xbrl"].get("nil"),
             "dimension_nonempty_rows": log["dimension_nonempty_rows"],
             "ixbrl_new_rows": total_ix_new,
             "parquet_sha256": log["parquet_sha256"]}
    log["comparison"] = {"before": previous, "after": after}
    if previous:
        # 修正前後の行数の差 = この修正で落ちた重複行の件数
        log["comparison"]["duplicates_dropped"] = previous["rows"] - after["rows"]
    with open(os.path.join(OUT_DIR, "run_log.json"), "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    # 実行ログ (Markdown)
    md = ["# 実行ログ（T3 変換、Step 2 塊1 修正・重複除去修正後）\n"]
    md.append(f"- 処理した書類: {sum(1 for d in log['docs'] if d['status'] == 'ok')}/{len(manifest)}")
    md.append(f"- Silver の行数: {log['rows']}")
    md.append(f"- parquet の SHA-256: {log['parquet_sha256']}")
    md.append(f"- 欠落した書類: {log['missing_zip'] if log['missing_zip'] else 'なし'}")
    md.append(f"- 統計: {log['stats_total']}")
    md.append(f"- 統計 (由来別): {log['stats_by_source']}")
    md.append(f"- 対象外の内訳: {log['out_of_scope_counts']}")
    md.append(f"- dimension が空でない行数: {log['dimension_nonempty_rows']}")
    md.append(f"- ixbrl 由来で新たに加わった行数: {total_ix_new}")
    md.append("")
    md.append("## 要素名の正規化規則 (Step 2 塊1 (d))\n")
    md.append(NORMALIZATION_RULE)
    md.append("")
    md.append("## 修正前後の比較\n")
    if previous:
        md.append("| 項目 | 修正前 | 修正後 |")
        md.append("|---|---|---|")
        md.append(f"| Silver の行数 | {previous['rows']} | {after['rows']} |")
        md.append(f"| unparseable 件数 (全体) | {previous['unparseable']} | {after['unparseable']} |")
        md.append(f"| unparseable 件数 (.xbrl 由来のみ) | {previous['unparseable_xbrl']} | {after['unparseable_xbrl']} |")
        md.append(f"| nil 件数 (全体) | {previous['nil']} | {after['nil']} |")
        md.append(f"| nil 件数 (.xbrl 由来のみ) | {previous['nil_xbrl']} | {after['nil_xbrl']} |")
        md.append(f"| dimension が空でない行数 | {previous['dimension_nonempty_rows']} | {after['dimension_nonempty_rows']} |")
        md.append(f"| ixbrl 由来で新たに加わった行数 | {previous['ixbrl_new_rows']} | {after['ixbrl_new_rows']} |")
        md.append(f"| parquet の SHA-256 | {previous['parquet_sha256']} | {after['parquet_sha256']} |")
        md.append("")
        md.append(f"重複除去の数値キー化 (修正 (e)) により、修正前 {previous['rows']} 行から "
                  f"{log['comparison']['duplicates_dropped']} 件の重複行が落ち、"
                  f"修正後は {after['rows']} 行になった。")
        md.append("")
        md.append("注: 修正前は、上書き前に読み取った前回の実行結果 (run_log.json と "
                  "parquet) の値。nil と unparseable は全体と .xbrl 由来のみを併記する。")
    else:
        md.append("修正前の出力が無かったため比較なし。")
    md.append("")
    md.append("## tar のハッシュ（取得元）\n")
    for k, v in log["tar_hashes"].items():
        md.append(f"- {os.path.basename(k)}: {v}")
    md.append("")
    md.append("## 書類ごとの結果\n")
    for d in log["docs"]:
        line = f"- {d['doc_id']} ({d['cell']}): {d['status']} facts={d.get('facts', '-')}"
        if d["status"] == "ok":
            line += f" extracted={d.get('extracted_facts', '-')} ixbrl_new={d.get('ixbrl_new', '-')}"
        if d.get("ix_parse_errors"):
            line += f" ix_parse_errors={d['ix_parse_errors']}"
        md.append(line)
    with open(os.path.join(OUT_DIR, "run_log.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    # 対象外の分類報告
    lines = ["# 対象外要素の分類報告（T3、Step 2 塊1 修正・重複除去修正後）\n"]
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
    print(f"stats_by_source: {log['stats_by_source']}")
    print(f"dimension_nonempty_rows: {log['dimension_nonempty_rows']}")
    print(f"ixbrl_new_rows: {total_ix_new}")
    print(f"missing: {log['missing_zip']}")


if __name__ == "__main__":
    main()
