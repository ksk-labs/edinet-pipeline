"""XBRL -> Silver（型つき事実）変換の中核。

試作 prototype/convert.py（Step 1、標本29書類で独立照合済み）の変換規則を
そのまま移植したもの。変換の意味は試作の確定仕様どおり:

- 金額は数値型。XBRL の scale 属性を適用する (value = text * 10**scale)。
- 数値でない事実 (文字列・日付・真偽値) は Silver に入れず、対象外として分類する。
- nil の事実は値なしとして分類する。nil の判定は xsi:nil
  (http://www.w3.org/2001/XMLSchema-instance の属性 nil="true") で行う。
- 時点値と確定値の分離: doc_type=120 の事実は point_in_time='as_filed'。
  doc_type=130 (訂正) の事実は point_in_time='final' で、訂正の系譜として
  parent_doc_id を保持する。訂正のない 120 の事実は as_filed がそのまま
  確定値であり、行の重複は作らない。
- 訂正の判定は doc_type=130 と parent_doc_id で行う。索引の is_amendment 列は
  全行 True の欠陥があり使えない。
- .xbrl インスタンスと Inline XBRL (.htm) の両方から抽出し、重複は
  (要素・context・単位・数値) で除去する。キーの値は文字列形ではなく
  scale 適用後の Decimal そのものとし、符号付きゼロ (-0.0 と 0.0) や
  小数表記の違いで重複が残らないようにする。Inline XBRL の context/unit
  定義は ix:header 内の ix:resources から読む。
- 次元情報は context の segment と scenario の両方から xbrldi の
  explicitMember / typedMember を読み、dimension 列に保持する。
- 提出者拡張要素の名前空間末尾には提出日等が入るため、要素名は
  normalize_prefix の規則で正規化する (規則の正本は NORMALIZATION_RULE)。
"""
import io
import os
import re
import zipfile
from decimal import Decimal, InvalidOperation

from lxml import etree

XBRL_NS = "http://www.xbrl.org/2003/instance"
LINK_NS = "http://www.xbrl.org/2003/linkbase"
IX_NS = "http://www.xbrl.org/2008/inlineXBRL"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
XBRDI_NS = "http://xbrl.org/2006/xbrldi"

NORMALIZATION_RULE = (
    "要素名の正規化規則: 要素の接頭辞は名前空間 URI から作る。"
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
    """xsi:nil="true" かどうか。"""
    return el.get(f"{{{XSI_NS}}}nil") == "true"


def _dimension_string(ctx):
    """context の segment と scenario から次元メンバーの文字列を作る。

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


def new_stats():
    return {"numeric": 0, "non_numeric": 0, "nil": 0, "unparseable": 0, "no_context": 0}


def _new_out_of_scope():
    return {"non_numeric": [], "nil": [], "unparseable": [], "no_context": []}


def parse_xbrl(data, doc_id, out_of_scope):
    """1つの .xbrl ファイルを解析し、(facts, stats) を返す。"""
    root = etree.fromstring(data)
    contexts = parse_contexts(root)
    units = parse_units(root)
    facts = []
    stats = new_stats()
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
            # 符号付きゼロ (-0) を 0 に正規化する
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
    """Inline XBRL (.htm を XML として解析した木) から事実を抽出する。

    contexts / units は ix:header 内 ix:resources から集めたものを渡す。
    name 属性は文書の xmlns 宣言で名前空間 URI へ解決し、正規化した接頭辞で
    .xbrl 側と同じ要素名を作る (重複除去が効くようにするため)。
    """
    facts = []
    stats = new_stats()
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
            # sign="-" と本文 "0" の組み合わせ等で生じる符号付きゼロを 0 に正規化する
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


def convert_document(zip_bytes, meta):
    """1書類の zip バイト列を Silver の行へ変換する。

    meta は dict: doc_id, submit_at, is_correction (bool),
    parent_doc_id (str|None), superseded_by (str|None)。
    戻り値: (rows, stats, stats_by_source, out_of_scope, ixbrl_new_rows)。
    rows は Silver の1行 = dict (列は batch.SILVER_SCHEMA と同じ並び)。
    zip が壊れている場合は zipfile.BadZipFile を送出する (呼び出し側で隔離する)。
    """
    doc_id = meta["doc_id"]
    out_of_scope = _new_out_of_scope()
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    doc_facts = []
    doc_stats = new_stats()
    doc_stats_by_source = {"xbrl": new_stats(), "ixbrl": new_stats()}
    xbrl_names = [n for n in zf.namelist()
                  if n.lower().endswith(".xbrl")]
    htm_names = [n for n in zf.namelist()
                 if n.lower().endswith((".htm", ".html")) and "ixbrl" in n.lower()]
    # Inline XBRL は .htm の中に ix: 名前空間があるもので判定する
    for n in zf.namelist():
        if n.lower().endswith((".htm", ".html")) and n not in htm_names:
            head = zf.read(n)[:4096].decode("utf-8", "ignore")
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
    ix_parser = etree.XMLParser(no_network=True)
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
    # 重複の除去 (同一 element/context/unit/数値)。先勝ち。
    # キーの値は文字列形ではなく scale 適用後の Decimal そのもの。
    # .xbrl を先に処理するため、残った ixbrl 由来の行が「ixbrl のみ」になる。
    seen = set()
    is_correction = bool(meta["is_correction"])
    point = "final" if is_correction else "as_filed"
    rows = []
    ix_new = 0
    for f in doc_facts:
        key = (f["element"], f["context_ref"], f["unit"], f["value_dec"])
        if key in seen:
            continue
        seen.add(key)
        if f["source"] == "ixbrl":
            ix_new += 1
        rows.append({
            "doc_id": doc_id,
            "element": f["element"],
            "value": f["value"],
            "unit": f["unit"],
            "context_start": f["context_start"],
            "context_end": f["context_end"],
            "dimension": f["dimension"],
            "submit_at": meta["submit_at"],
            "is_correction": is_correction,
            "parent_doc_id": meta["parent_doc_id"] if is_correction else None,
            "superseded_by": meta["superseded_by"],
            "point_in_time": point,
        })
    if ix_parse_errors:
        doc_stats["ix_parse_errors"] = len(ix_parse_errors)
    return rows, doc_stats, doc_stats_by_source, out_of_scope, ix_new
