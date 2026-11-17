"""EDINET タクソノミの取得・年次更新の自動化と規約文書の継続確認 (P2-T9)。

仕様: Issue #15 §10 (タクソノミと規約文書の継続確認)。合格条件8
(未知要素の隔離と警報の故障ケース確認) に対応する。

取得元 (一次情報で確定。STATE.md「タクソノミ取得元の確定」):
- 金融庁「EDINETタクソノミ一覧」ページ (版と ZIP の所在の正本):
  https://www.fsa.go.jp/search/EDINET_Taxonomy_All.html
  各版の ZIP は /search/<公表日YYYYMMDD>/1c_Taxonomy.zip の形で置かれる
  (ARIA-3 の taxonomy_urls.json と実ダウンロードで確認済み)。
- 配布ポータル「EDINET TAXONOMY & CODE LIST DOWNLOAD」:
  https://disclosure2.edinet-fsa.go.jp/weee0020.aspx
  分類 (00 ALL、01 DEI、02 財務諸表本表 等) ごとに ALL_<タクソノミ日付>.zip
  等のファイル名で配布される。一覧ページの最新版とポータルの最新
  ALL 日付の突合を検出の冗長化に使う。

版の同一性: 金融庁の版年 (例: 2026年版)・公表日 (例: 2025-11-11)・
タクソノミ日付 (名前空間内の日付。例: 2025-11-01) は別物として保持する。
版年は一覧ページの表記を優先し、取れない場合は公表日から導く
(11月・12月公表の版は翌年版。2023年版=2022-11-08公表、2024年版=
2023-12-11公表、2025年版=2024-11-12公表、2026年版=2025-11-11公表の
実績に基づく)。

要素辞書: ZIP 内の taxonomy/ 配下のスキーマ (*.xsd) の要素宣言と、
ラベルリンクベース (*_lab.xml = 日本語、*_lab-en.xml = 英語) から構築する。
要素のキーは接頭辞:要素名 (接頭辞は名前空間の末尾セグメント。例:
jppfs_cor:CashAndDeposits) で、Silver の element 列と同じ形にする。
タクソノミ日付は要素ごとに別フィールドで保持するため、版をまたいだ
差分はキーでそのまま取れる。

保存先 (v2 データセット。codelist と同じ流儀の不変保存+目録):
- taxonomy/edition=<版年>/elements.jsonl: 要素辞書 (1行1要素)
- taxonomy/edition=<版年>/meta.json: 版・公表日・ZIP のハッシュ等の出所
- taxonomy/edition=<版年>/diff.json と changes.jsonl: 直前版との差分
- taxonomy/edition=<版年>/quarantine.jsonl: 未知要素の隔離記録 (生成時のみ)
- taxonomy/archive/1c_Taxonomy-<版年>.zip: 取得した ZIP の原形
- taxonomy/state.json: 継続確認の状態 (一覧ページ・ポータル・取扱い要領の
  ハッシュ、arelle の追随確認の結果、前回確認日時)
- taxonomy/ledger.jsonl: 実行ごとの結果の追記目録

年次更新は完全自動とする: 検出 (月次確認+11月の重点確認) → 取込 →
差分報告 → 未知要素の隔離と警報。未知要素の隔離は Silver の要素一覧
が渡された場合に行うもので、年次ワークフローへの配線
(--silver-elements) は Silver 側の接続点 (S2-T5) の確定待ちであり、
現行の年次ワークフローでは隔離は未実施である (到達点は作業記録に
明記)。ただし指標に効く要素の改名・分割・
削除・意味変更の判断はユーザーの判断事項であり、自動では行わない。
該当する差分は judgment_items として警報に上げ、確定までは旧対応で
運転を継続する (本モジュールは指標登録簿を変更しない)。

arelle の追随確認: arelle の EDINET 専用プラグインの有効一覧
(plugin/validate/EDINET/resources/edinet-taxonomies.xml) が、取り込んだ
タクソノミの名前空間日付をすべて含むかを確認する。旧版の日付が一覧に
無いと制度選択時に事実が欠落するため (2026-11-17 実測)、新しい日付が
一覧に無い場合は警報にする。確認対象はインストール済み arelle の
同梱ファイルを優先し、無ければ上流 (Arelle/Arelle の master) を使う
(先勝ちであり、両方の突合はしない)。

証券コード協議会の取扱い要領の継続確認: JPX の証券コード協議会
「関係資料等」ページから取扱い要領 PDF (codestock_<日付>.pdf) を
発見し、ページと PDF の両方のハッシュを state と比較する。変更が
あれば警報にする (種類識別 §4 への影響判断は人間が行う)。

認証は HF への書き込みに環境変数 HF_TOKEN を使う。取得自体に
API キーは不要である。
"""
import argparse
import hashlib
import io
import json
import os
import re
import shutil
import sys
import traceback
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError
from lxml import etree

from .hf_sync import _with_retry

LIST_PAGE_URL = "https://www.fsa.go.jp/search/EDINET_Taxonomy_All.html"
PORTAL_URL = "https://disclosure2.edinet-fsa.go.jp/weee0020.aspx"
COUNCIL_PAGE_URL = "https://www.jpx.co.jp/sicc/securities-code/01.html"
ARELLE_UPSTREAM_URL = (
    "https://raw.githubusercontent.com/Arelle/Arelle/master/"
    "arelle/plugin/validate/EDINET/resources/edinet-taxonomies.xml")
DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"

SOURCE_LIST = "fsa_taxonomy_list_page"
SOURCE_PORTAL = "edinet_taxonomy_portal"
SOURCE_COUNCIL = "securities_code_council_via_jpx"
REDISTRIBUTION = "allowed"

_XSD_NS = "http://www.w3.org/2001/XMLSchema"
_XBRLI_NS = "http://www.xbrl.org/2003/instance"
_LINK_NS = "http://www.xbrl.org/2003/linkbase"
_XLINK_NS = "http://www.w3.org/1999/xlink"
_XML_NS = "http://www.w3.org/XML/1998/namespace"
_PARSER = etree.XMLParser(no_network=True, huge_tree=True, recover=False)

_ZIP_URL_RE = re.compile(r"/search/(\d{8})/(1c_Taxonomy\.zip)")
_EDITION_LABEL_RE = re.compile(r"(20\d\d)\s*年版")
_PORTAL_FILE_RE = re.compile(r"onDownload\('([^']+\.zip)'\)")
_ALL_FILE_RE = re.compile(r"^ALL_(\d{8})\.zip$")
_NS_DATE_RE = re.compile(r"/(\d{4}-\d{2}-\d{2})/")
_COUNCIL_PDF_RE = re.compile(r'href="([^"]*codestock_\d+\.pdf)"')
_STANDARD_LABEL_ROLE = "http://www.xbrl.org/2003/role/label"
_DEPRECATED_LABEL_ROLE = "http://www.xbrl.org/2009/role/deprecatedLabel"


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _today_jst():
    return datetime.now(ZoneInfo("Asia/Tokyo")).date().isoformat()


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


class TaxonomyError(Exception):
    """タクソノミの取得・検証の恒常的な失敗。"""


class _RetryableHttpError(Exception):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.status_code = status


def _retryable_status(status):
    return status == 429 or status >= 500


def fetch_url(url, timeout=120):
    """URL を取得してバイト列を返す (再試行つき)。"""
    def _once():
        req = urllib.request.Request(
            url, headers={"User-Agent": "edinet-pipeline taxonomy"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                body = res.read()
                status = res.status
        except urllib.error.HTTPError as e:
            status = e.code
            if _retryable_status(status):
                raise _RetryableHttpError(status) from e
            raise TaxonomyError(f"取得に失敗しました: HTTP {status}: {url}") from e
        if _retryable_status(status):
            raise _RetryableHttpError(status)
        if status != 200:
            raise TaxonomyError(f"取得に失敗しました: HTTP {status}: {url}")
        return body

    return _with_retry("taxonomy fetch", _once)


# ---------------------------------------------------------------- 一覧ページ

def edition_year_for(published):
    """公表日 (YYYY-MM-DD) から版年を導く。

    11月・12月に公表される版は翌年版である (2022-11-08公表=2023年版、
    2023-12-11公表=2024年版、2024-11-12公表=2025年版、
    2025-11-11公表=2026年版の実績)。それ以外は同年版とする。
    """
    year, month, _day = (int(p) for p in published.split("-"))
    return year + 1 if month >= 11 else year


def parse_list_page(html_text):
    """金融庁の一覧ページから版の一覧を抽出する。

    戻り値: [{edition, published, zip_url}] (公表日の降順)。
    ZIP の URL パターン (/search/<公表日>/1c_Taxonomy.zip) を正とし、
    版年はリンク周辺の「YYYY年版」表記を優先し、無ければ
    edition_year_for で導く。同じ公表日の重複は1件にまとめる。
    """
    editions = {}
    matches = list(_ZIP_URL_RE.finditer(html_text))
    for i, m in enumerate(matches):
        digits = m.group(1)
        published = f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"
        url = ("https://www.fsa.go.jp/search/"
               f"{digits}/{m.group(2)}")
        # 版年表記はリンク文字列 (URL の直後) にあるのが実ページの形。
        # 次の ZIP リンクまでの範囲で最初の表記を取り、無ければ直前の
        # 範囲の最後の表記を取る (隣の版の表記を拾わないため)。
        label = None
        next_start = (matches[i + 1].start() if i + 1 < len(matches)
                      else len(html_text))
        after = html_text[m.end():min(next_start, m.end() + 1000)]
        labels = _EDITION_LABEL_RE.findall(after)
        if labels:
            label = int(labels[0])
        else:
            before = html_text[max(0, m.start() - 1000):m.start()]
            labels = _EDITION_LABEL_RE.findall(before)
            if labels:
                label = int(labels[-1])
        edition = label if label is not None else edition_year_for(published)
        editions[published] = {
            "edition": edition, "published": published, "zip_url": url,
            "edition_label_found": label is not None,
        }
    return sorted(editions.values(), key=lambda e: e["published"],
                  reverse=True)


def parse_portal_page(html_text):
    """配布ポータルから配布ファイル名の一覧と最新日付を抽出する。

    ポータルのダウンロードは onDownload('<ファイル名>') の形で埋め込ま
    れている。戻り値: {files, newest_all_date, newest_by_section}。
    newest_all_date は 00 ALL の最新タクソノミ日付 (YYYY-MM-DD)。
    newest_by_section は2桁の分類コード -> 最新日付。
    """
    files = sorted(set(_PORTAL_FILE_RE.findall(html_text)))
    all_dates = []
    by_section = {}
    for name in files:
        m = _ALL_FILE_RE.match(name)
        if m:
            d = m.group(1)
            all_dates.append(f"{d[0:4]}-{d[4:6]}-{d[6:8]}")
            continue
        sec = re.match(r"^(\d{2})_(\d{8})\.zip$", name)
        if sec:
            d = sec.group(2)
            date = f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
            if date > by_section.get(sec.group(1), ""):
                by_section[sec.group(1)] = date
    return {
        "files": files,
        "newest_all_date": max(all_dates) if all_dates else None,
        "newest_by_section": by_section,
    }


def parse_council_page(html_text):
    """証券コード協議会の関係資料ページから取扱い要領 PDF を発見する。

    戻り値: PDF の絶対 URL (無ければ None)。ファイル名は
    codestock_<日付>.pdf で、改定のたびに日付部分が変わるため、
    ページから動的に発見する (固定 URL を前提にしない)。
    """
    m = _COUNCIL_PDF_RE.search(html_text)
    if not m:
        return None
    href = m.group(1)
    if href.startswith("http"):
        return href
    return "https://www.jpx.co.jp" + href


# ---------------------------------------------------------------- 要素辞書

def _member_name(raw_name):
    """ZIP 内ファイル名を復号する (cp437 で読まれた名前を cp932 に直す)。"""
    try:
        return raw_name.encode("cp437").decode("cp932")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return raw_name


def _parse_labels(data):
    """ラベルリンクベース1ファイルを {要素ID: {言語: 標準ラベル}} にする。"""
    root = etree.fromstring(data, parser=_PARSER)
    locs = {}
    for loc in root.iter(f"{{{_LINK_NS}}}loc"):
        label = loc.get(f"{{{_XLINK_NS}}}label")
        href = loc.get(f"{{{_XLINK_NS}}}href") or ""
        if label and "#" in href:
            locs[label] = href.rsplit("#", 1)[1]
    labels = {}
    deprecated_labels = {}
    for lab in root.iter(f"{{{_LINK_NS}}}label"):
        role = lab.get(f"{{{_XLINK_NS}}}role")
        label_id = lab.get(f"{{{_XLINK_NS}}}label")
        lang = lab.get(f"{{{_XML_NS}}}lang")
        text = (lab.text or "").strip()
        if not (label_id and lang and text):
            continue
        if role in (None, _STANDARD_LABEL_ROLE):
            labels.setdefault(label_id, {})[lang] = text
        elif role == _DEPRECATED_LABEL_ROLE:
            # 廃止要素のラベルは deprecatedLabel 役割で置かれる。辞書の
            # 継続性のため、標準ラベルが無い場合の予備として保持する。
            deprecated_labels.setdefault(label_id, {})[lang] = text
    for label_id, by_lang in deprecated_labels.items():
        slot = labels.setdefault(label_id, {})
        for lang, text in by_lang.items():
            slot.setdefault(lang, text)
    arcs = {}
    for arc in root.iter(f"{{{_LINK_NS}}}labelArc"):
        frm = arc.get(f"{{{_XLINK_NS}}}from")
        to = arc.get(f"{{{_XLINK_NS}}}to")
        if frm and to:
            arcs.setdefault(frm, []).append(to)
    result = {}
    for loc_label, element_id in locs.items():
        for lab_label in arcs.get(loc_label, []):
            for lang, text in labels.get(lab_label, {}).items():
                result.setdefault(element_id, {})[lang] = text
    return result


def _parse_schema(data):
    """スキーマ1ファイルから要素宣言の一覧を返す。"""
    root = etree.fromstring(data, parser=_PARSER)
    ns = root.get("targetNamespace") or ""
    prefix = ns.rstrip("/").rsplit("/", 1)[-1] if ns else ""
    m = _NS_DATE_RE.search(ns)
    tax_date = m.group(1) if m else None
    elements = []
    for el in root.findall(f"{{{_XSD_NS}}}element"):
        name = el.get("name")
        if not name:
            continue
        elements.append({
            "key": f"{prefix}:{name}",
            "prefix": prefix,
            "name": name,
            "namespace": ns,
            "taxonomy_date": tax_date,
            "element_id": el.get("id"),
            "type": el.get("type"),
            "substitution_group": el.get("substitutionGroup"),
            "abstract": el.get("abstract") == "true",
            "period_type": el.get(f"{{{_XBRLI_NS}}}periodType"),
            "balance": el.get(f"{{{_XBRLI_NS}}}balance"),
        })
    return elements


def parse_taxonomy_zip(zip_bytes):
    """タクソノミ ZIP から要素辞書を構築する。

    対象は ZIP 内の taxonomy/ 配下の *.xsd (要素宣言) と *_lab.xml /
    *_lab-en.xml (ラベル)。deprecated/ 配下のスキーマで宣言された要素は
    deprecated=True とする。戻り値: {elements (キーの辞書), taxonomy_dates
    (名前空間日付の集合), member_count, schema_count}。
    要素が1件も取れない ZIP は形式の想定外として TaxonomyError にする。
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as e:
        raise TaxonomyError(f"zip を開けません: {e}") from e
    members = {_member_name(n): n for n in zf.namelist()}
    label_maps = {}
    schemas = []
    for name in sorted(members):
        if "/taxonomy/" not in name and not name.startswith("taxonomy/"):
            continue
        lower = name.lower()
        if lower.endswith(("_lab.xml", "_lab-en.xml")):
            label_maps[name] = _parse_labels(zf.read(members[name]))
        elif lower.endswith(".xsd"):
            schemas.append(name)
    # ラベルの統合: deprecated でないファイルを優先する (先勝ち)。
    merged_labels = {}
    for name in sorted(label_maps, key=lambda n: ("/deprecated/" in n, n)):
        for element_id, by_lang in label_maps[name].items():
            slot = merged_labels.setdefault(element_id, {})
            for lang, text in by_lang.items():
                slot.setdefault(lang, text)
    elements = {}
    for name in schemas:
        deprecated_schema = "/deprecated/" in name or "_dep_" in name
        for el in _parse_schema(zf.read(members[name])):
            el["deprecated"] = deprecated_schema
            by_lang = merged_labels.get(el["element_id"] or "", {})
            el["label_ja"] = by_lang.get("ja")
            el["label_en"] = by_lang.get("en")
            existing = elements.get(el["key"])
            if existing is None:
                elements[el["key"]] = el
            else:
                # 同一キーが複数スキーマにある場合はラベル等を補完する。
                for field in ("label_ja", "label_en", "type",
                              "period_type", "balance"):
                    if not existing.get(field) and el.get(field):
                        existing[field] = el[field]
                existing["deprecated"] = (existing["deprecated"]
                                          and el["deprecated"])
    if not elements:
        raise TaxonomyError("ZIP から要素を1件も抽出できませんでした")
    dates = sorted({el["taxonomy_date"] for el in elements.values()
                    if el["taxonomy_date"]})
    return {
        "elements": elements,
        "taxonomy_dates": dates,
        "member_count": len(members),
        "schema_count": len(schemas),
    }


# ---------------------------------------------------------------- 差分

# 意味変更の判定項目。namespace はタクソノミ日付を含むため、項目に
# 入れると日付のみの繰り上がり (date_only) と分離できなくなるので
# 対象外とする。substitution_group と element_id は日付を含まず、
# XBRL 上の意味・同一性に効くため判定項目に含める。
_SEMANTIC_FIELDS = ("label_ja", "label_en", "type", "period_type",
                    "balance", "abstract", "deprecated",
                    "substitution_group", "element_id")


def compute_diff(old, new, indicator_elements=()):
    """2つの要素辞書 (キー -> 要素) の差分を計算する。

    戻り値の主な内容:
    - added / removed: キーのみの増減
    - changed: 意味のある変更 (ラベル・型・期間・バランス・抽象・廃止)
      があった要素の {キー: {項目: [旧値, 新値]}}
    - date_only: タクソノミ日付だけが変わった要素数 (版の繰り上がり)
    - rename_candidates: 削除と追加の組で日英ラベルが完全一致するもの
      (改名の候補。確定はしない)
    - split_candidates: 削除要素の日本語ラベルと包含関係にある追加要素が
      2件以上あるもの (分割の候補。確定はしない)
    - judgment_items: 指標に効く要素に触れる差分 (削除・意味変更・改名
      候補・分割候補)。意味判断はユーザーの判断事項であり、本関数は
      候補の提示までで対応の変更は行わない。
    """
    indicators = set(indicator_elements)
    old_keys, new_keys = set(old), set(new)
    added = sorted(new_keys - old_keys)
    removed = sorted(old_keys - new_keys)
    changed = {}
    date_only = 0
    for key in sorted(old_keys & new_keys):
        diffs = {f: [old[key].get(f), new[key].get(f)]
                 for f in _SEMANTIC_FIELDS
                 if old[key].get(f) != new[key].get(f)}
        if diffs:
            changed[key] = diffs
        elif old[key].get("taxonomy_date") != new[key].get("taxonomy_date"):
            date_only += 1

    def _label_pair(el):
        return (el.get("label_ja"), el.get("label_en"))

    added_by_label = {}
    for key in added:
        pair = _label_pair(new[key])
        if pair[0] and pair[1]:
            added_by_label.setdefault(pair, []).append(key)
    rename_candidates = []
    renamed_old, renamed_new = set(), set()
    for key in removed:
        pair = _label_pair(old[key])
        candidates = [c for c in added_by_label.get(pair, [])
                      if c not in renamed_new
                      and new[c]["prefix"] == old[key]["prefix"]]
        if len(candidates) == 1:
            rename_candidates.append(
                {"from": key, "to": candidates[0],
                 "label_ja": pair[0], "label_en": pair[1]})
            renamed_old.add(key)
            renamed_new.add(candidates[0])

    split_candidates = []
    for key in removed:
        if key in renamed_old:
            continue
        label = old[key].get("label_ja")
        if not label:
            continue
        parts = [c for c in added
                 if c not in renamed_new
                 and new[c]["prefix"] == old[key]["prefix"]
                 and new[c].get("label_ja")
                 and (new[c]["label_ja"].startswith(label)
                      or label.startswith(new[c]["label_ja"]))]
        if len(parts) >= 2:
            split_candidates.append({"from": key, "to": parts,
                                     "label_ja": label})

    judgment_items = []
    for key in removed:
        if key in indicators:
            judgment_items.append({"element": key, "kind": "removed"})
    for key in changed:
        if key in indicators:
            judgment_items.append({"element": key, "kind": "changed",
                                   "fields": sorted(changed[key])})
    for cand in rename_candidates:
        if cand["from"] in indicators or cand["to"] in indicators:
            judgment_items.append({"element": cand["from"],
                                   "kind": "rename_candidate",
                                   "to": cand["to"]})
    for cand in split_candidates:
        if cand["from"] in indicators:
            judgment_items.append({"element": cand["from"],
                                   "kind": "split_candidate",
                                   "to": cand["to"]})

    return {
        "added": added, "removed": removed, "changed": changed,
        "date_only": date_only,
        "rename_candidates": rename_candidates,
        "split_candidates": split_candidates,
        "judgment_items": judgment_items,
        "counts": {
            "old": len(old), "new": len(new), "added": len(added),
            "removed": len(removed), "changed": len(changed),
            "date_only": date_only,
            "rename_candidates": len(rename_candidates),
            "split_candidates": len(split_candidates),
            "judgment_items": len(judgment_items),
        },
    }


def changes_rows(diff):
    """差分を changes.jsonl の行 (dict) の並びにする。"""
    rows = []
    for key in diff["added"]:
        rows.append({"change": "added", "element": key})
    for key in diff["removed"]:
        rows.append({"change": "removed", "element": key})
    for key, fields in diff["changed"].items():
        rows.append({"change": "changed", "element": key,
                     "fields": fields})
    for cand in diff["rename_candidates"]:
        rows.append({"change": "rename_candidate", **cand})
    for cand in diff["split_candidates"]:
        rows.append({"change": "split_candidate", **cand})
    return rows


def find_unknown(silver_elements, dictionary):
    """Silver の要素名のうち辞書に無いものを返す (未知要素の隔離)。

    判定の対象は、接頭辞が辞書に存在する標準タクソノミの要素だけとする。
    提出者拡張の要素 (接頭辞が辞書に無い) は P2-T10 の要素辞書の範囲で
    あり、ここでは未知として扱わない。
    """
    prefixes = {el["prefix"] for el in dictionary.values()}
    unknown = []
    for element in sorted(set(silver_elements)):
        if ":" not in element:
            unknown.append(element)
            continue
        prefix = element.split(":", 1)[0]
        if prefix in prefixes and element not in dictionary:
            unknown.append(element)
    return unknown


# ---------------------------------------------------------------- arelle

def parse_arelle_taxonomies(xml_bytes):
    """arelle の有効一覧 (edinet-taxonomies.xml) から FSA 日付を抽出する。"""
    dates = sorted(set(m.group(1).decode()
                       for m in re.finditer(
                           rb"taxonomy/[a-z0-9\-]+/(\d{4}-\d{2}-\d{2})/",
                           xml_bytes)))
    return {"dates": dates, "sha256": sha256_bytes(xml_bytes)}


def arelle_coverage(required_dates, arelle_info):
    """必要なタクソノミ日付が arelle の有効一覧にすべて含まれるか。"""
    covered = set(arelle_info["dates"])
    missing = [d for d in required_dates if d not in covered]
    return {"required": list(required_dates), "covered_dates": arelle_info["dates"],
            "missing": missing, "ok": not missing,
            "sha256": arelle_info["sha256"]}


def arelle_date_report(taxonomy_dates, arelle_info):
    """版の日付ごとの arelle 追随状況を作る。

    追随確認の必須対象は、その版の最新日付 (年次更新で新しくなる日付)
    だけとする。版に同梱される旧日付の体系 (例: 2021-11-01 の
    jpcrp-sbr / jpctl) は arelle の有効一覧に元々含まれないものがあり
    (2026-11-17 実測の範囲でも一覧は主要体系のみ)、必須にすると
    常時警報になるため、未収載は uncovered_legacy として情報に留める。
    """
    primary = max(taxonomy_dates) if taxonomy_dates else None
    coverage = arelle_coverage([primary] if primary else [], arelle_info)
    covered = set(arelle_info["dates"])
    coverage["primary_date"] = primary
    coverage["uncovered_legacy"] = [d for d in taxonomy_dates
                                    if d != primary and d not in covered]
    return coverage


def load_arelle_taxonomies(fetcher=None):
    """arelle の有効一覧を取得する (同梱ファイル優先、無ければ上流)。

    戻り値: (info, source)。同梱も上流も取れない場合は (None, None)。
    """
    try:
        import arelle
        path = os.path.join(
            os.path.dirname(arelle.__file__),
            "plugin", "validate", "EDINET", "resources",
            "edinet-taxonomies.xml")
        with open(path, "rb") as f:
            return parse_arelle_taxonomies(f.read()), f"installed:{path}"
    except Exception:
        pass
    fetcher = fetcher or fetch_url
    try:
        return parse_arelle_taxonomies(fetcher(ARELLE_UPSTREAM_URL)), \
            f"upstream:{ARELLE_UPSTREAM_URL}"
    except Exception:
        return None, None


def load_indicator_elements(path):
    """指標に効く要素の一覧ファイルを読む (1行1要素、# は注記)。"""
    elements = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                elements.append(line)
    return elements


# ---------------------------------------------------------------- HF 保存

def edition_paths(edition):
    base = f"taxonomy/edition={edition}"
    return {
        "elements": f"{base}/elements.jsonl",
        "meta": f"{base}/meta.json",
        "diff": f"{base}/diff.json",
        "changes": f"{base}/changes.jsonl",
        "quarantine": f"{base}/quarantine.jsonl",
        "archive": f"taxonomy/archive/1c_Taxonomy-{edition}.zip",
    }


_EDITION_PATH_RE = re.compile(r"^taxonomy/edition=(\d+)/elements\.jsonl$")
_EDITION_META_RE = re.compile(r"^taxonomy/edition=(\d+)/meta\.json$")


def _remote_files(api, repo):
    return set(_with_retry(
        "list_repo_files",
        lambda: api.list_repo_files(repo, repo_type="dataset")))


def _download_if_exists(api, repo, path_in_repo, local_path):
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


def _write_local(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "wb" if isinstance(data, bytes) else "w"
    kwargs = {} if isinstance(data, bytes) else {"encoding": "utf-8"}
    with open(path, mode, **kwargs) as f:
        f.write(data)


def _append_ledger(api, repo, work_dir, row):
    local = os.path.join(work_dir, "state", "taxonomy-ledger.jsonl")
    _download_if_exists(api, repo, "taxonomy/ledger.jsonl", local)
    os.makedirs(os.path.dirname(local), exist_ok=True)
    with open(local, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    _upload_file(api, repo, local, "taxonomy/ledger.jsonl",
                 f"taxonomy {row.get('run_date')}: ledger {row.get('status')}")


def _read_remote_json(api, repo, path_in_repo, work_dir, name):
    local = os.path.join(work_dir, "state", name)
    if not _download_if_exists(api, repo, path_in_repo, local):
        return None
    with open(local, encoding="utf-8") as f:
        return json.load(f)


def _read_remote_elements(api, repo, edition, work_dir):
    local = os.path.join(work_dir, "state", f"elements-{edition}.jsonl")
    path = edition_paths(edition)["elements"]
    if not _download_if_exists(api, repo, path, local):
        return None
    elements = {}
    with open(local, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                el = json.loads(line)
                elements[el["key"]] = el
    return elements


def _previous_edition(remote, edition):
    editions = sorted(
        (int(m.group(1)) for path in remote
         if (m := _EDITION_PATH_RE.match(path)) and int(m.group(1)) < edition))
    return editions[-1] if editions else None


def _remote_bytes(api, repo, path_in_repo, work_dir, name):
    local = os.path.join(work_dir, "state", name)
    if not _download_if_exists(api, repo, path_in_repo, local):
        return None
    with open(local, "rb") as f:
        return f.read()


def _complete_diff(api, repo, work_dir, summary, edition, elements,
                   remote, fetched_at, indicator_elements):
    """差分 (diff.json と changes.jsonl) を生成して保存する。

    codelist の _complete_diff と同じ流儀とする。両方そろっていれば
    present、不足していれば直前版から生成して補完し (already_present
    の再実行でも不足分はここで生成される)、生成できなければ例外に
    せず pending として返す。戻り値の dict は status (present /
    generated / pending / no_base) と base_edition を持ち、generated
    のときは counts と judgment_items、pending のときは error を
    持つ。保存したパスは summary["uploaded"] へ追記する。
    """
    paths = edition_paths(edition)
    result = {"status": "no_base",
              "base_edition": _previous_edition(remote, edition)}
    if result["base_edition"] is None:
        return result
    if paths["diff"] in remote and paths["changes"] in remote:
        result["status"] = "present"
        return result
    try:
        old = _read_remote_elements(api, repo, result["base_edition"],
                                    work_dir)
        if old is None:
            raise TaxonomyError("直前版の要素辞書を取得できませんでした")
        diff = compute_diff(old, elements, indicator_elements)
        diff_doc = {
            "edition": edition, "base_edition": result["base_edition"],
            "generated_at": _now(), "counts": diff["counts"],
            "rename_candidates": diff["rename_candidates"],
            "split_candidates": diff["split_candidates"],
            "judgment_items": diff["judgment_items"],
            "source": SOURCE_LIST, "fetched_at": fetched_at,
            "redistribution": REDISTRIBUTION,
        }
        changes_text = "".join(
            json.dumps(row, ensure_ascii=False) + "\n"
            for row in changes_rows(diff))
        for repo_path, data, message in [
                (paths["diff"],
                 json.dumps(diff_doc, ensure_ascii=False, indent=2),
                 f"taxonomy {edition}: diff summary"),
                (paths["changes"], changes_text,
                 f"taxonomy {edition}: diff changes")]:
            if repo_path in remote:
                continue
            local = os.path.join(work_dir, "stage", repo_path)
            _write_local(local, data)
            _upload_file(api, repo, local, repo_path, message)
            summary["uploaded"].append(repo_path)
            remote.add(repo_path)
    except Exception as e:
        result["status"] = "pending"
        result["error"] = f"{type(e).__name__}: {e}"[:500]
        return result
    result["status"] = "generated"
    result["counts"] = diff["counts"]
    result["judgment_items"] = diff["judgment_items"]
    return result


def _apply_diff_result(summary, edition, dres):
    """_complete_diff の結果を summary と警報へ反映する。"""
    summary["diff_status"] = dres["status"]
    summary["diff_base_edition"] = dres["base_edition"]
    if dres.get("counts"):
        summary["diff_counts"] = dres["counts"]
    if dres.get("error"):
        summary["diff_error"] = dres["error"]
    if dres.get("judgment_items"):
        summary["alerts"].append({
            "kind": "judgment_required",
            "items": dres["judgment_items"],
            "note": "指標に効く要素の差分。意味判断はユーザーの"
                    "判断事項であり、確定までは旧対応で継続する"})
    if dres["status"] == "pending":
        summary["alerts"].append({
            "kind": "diff_pending", "edition": edition,
            "error": dres.get("error"),
            "note": "差分が未生成です。次回の取込実行で補完されます"})


def _apply_arelle_check(summary, taxonomy_dates):
    """取込時の arelle 追随確認を summary と警報へ反映する。

    有効一覧の取得自体が失敗した場合も、無言で通さず警報にする
    (run_check 側の failed_sources と対称にする)。
    """
    arelle_info, arelle_source = load_arelle_taxonomies()
    if arelle_info is None:
        summary["arelle_error"] = "有効一覧を取得できませんでした"
        summary["alerts"].append({
            "kind": "arelle_unavailable",
            "note": "arelle の有効一覧を取得できず、追随確認が"
                    "できませんでした"})
        return
    coverage = arelle_date_report(taxonomy_dates, arelle_info)
    summary["arelle"] = {**coverage, "source": arelle_source}
    if not coverage["ok"]:
        summary["alerts"].append({
            "kind": "arelle_not_following",
            "missing_dates": coverage["missing"]})


# ---------------------------------------------------------------- 確認 (check)

def run_check(work_dir, repo=DEFAULT_REPO, fetcher=None, save_state=True):
    """継続確認を1回行い、結果 (dict) を返す。

    一覧ページ・ポータル・取扱い要領・arelle の4経路を確認し、前回の
    状態 (taxonomy/state.json) と比較する。取得に失敗した経路は
    failed_sources に記録し、他の経路の確認は続ける (部分失敗の隔離)。
    新版の検出は「一覧ページの最新版がまだ取り込まれていないこと」で
    判定する (取込済みの証拠は state の ingested_editions とリモートの
    版ごとの meta の有無)。state の latest_edition は最後に見た版の
    記録にすぎず検出の判定には使わないため、確認のみの実行や取込の
    失敗で検出が消費されることはない。ポータルの最新 ALL 日付との
    不整合も警報にする。
    """
    fetcher = fetcher or fetch_url
    os.makedirs(os.path.join(work_dir, "state"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "out"), exist_ok=True)
    api = HfApi()
    checked_at = _now()
    result = {
        "run_date": _today_jst(), "checked_at": checked_at,
        "alerts": [], "failed_sources": [],
        "list_page": None, "portal": None, "council": None,
        "arelle": None, "new_edition": None,
    }
    state = _read_remote_json(api, repo, "taxonomy/state.json",
                              work_dir, "taxonomy-state.json") or {}
    ingested = set(state.get("ingested_editions") or [])
    try:
        for path in _remote_files(api, repo):
            m = _EDITION_META_RE.match(path)
            if m:
                ingested.add(int(m.group(1)))
    except Exception:
        pass
    result["ingested_editions"] = sorted(ingested)

    try:
        html = fetcher(LIST_PAGE_URL).decode("utf-8", errors="replace")
        editions = parse_list_page(html)
        if not editions:
            raise TaxonomyError("一覧ページから版を抽出できませんでした")
        latest = editions[0]
        result["list_page"] = {
            "sha256": sha256_bytes(html.encode("utf-8")),
            "editions_found": len(editions),
            "latest_edition": latest["edition"],
            "latest_published": latest["published"],
            "latest_zip_url": latest["zip_url"],
            "source": SOURCE_LIST, "source_id": LIST_PAGE_URL,
            "fetched_at": checked_at, "redistribution": REDISTRIBUTION,
        }
        if latest["edition"] not in ingested:
            result["new_edition"] = latest
            result["alerts"].append({
                "kind": "new_edition", "edition": latest["edition"],
                "published": latest["published"],
                "zip_url": latest["zip_url"]})
    except Exception as e:
        result["failed_sources"].append(
            {"source": "list_page", "error": f"{type(e).__name__}: {e}"[:500]})

    try:
        html = fetcher(PORTAL_URL).decode("utf-8", errors="replace")
        portal = parse_portal_page(html)
        if not portal["files"] or not portal["newest_all_date"]:
            raise TaxonomyError(
                "ポータルから配布ファイルを抽出できませんでした")
        result["portal"] = {
            **portal, "sha256": sha256_bytes(html.encode("utf-8")),
            "source": SOURCE_PORTAL, "source_id": PORTAL_URL,
            "fetched_at": checked_at, "redistribution": REDISTRIBUTION,
        }
    except Exception as e:
        result["failed_sources"].append(
            {"source": "portal", "error": f"{type(e).__name__}: {e}"[:500]})

    # 一覧ページとポータルの突合: 最新版のタクソノミ日付 (版年の前年11月
    # 1日が実績) とポータルの最新 ALL 日付が食い違えば警報にする。
    if result["list_page"] and result["portal"]:
        expected = f"{result['list_page']['latest_edition'] - 1}-11-01"
        newest_all = result["portal"]["newest_all_date"]
        if newest_all and newest_all != expected:
            result["alerts"].append({
                "kind": "portal_mismatch", "expected_all_date": expected,
                "portal_newest_all_date": newest_all})

    try:
        page = fetcher(COUNCIL_PAGE_URL)
        pdf_url = parse_council_page(page.decode("utf-8", errors="replace"))
        council = {
            "page_url": COUNCIL_PAGE_URL,
            "page_sha256": sha256_bytes(page),
            "pdf_url": pdf_url, "pdf_sha256": None,
            "source": SOURCE_COUNCIL, "source_id": COUNCIL_PAGE_URL,
            "fetched_at": checked_at, "redistribution": REDISTRIBUTION,
        }
        if pdf_url:
            council["pdf_sha256"] = sha256_bytes(fetcher(pdf_url))
        previous = state.get("council") or {}
        changed = (
            previous and (previous.get("page_sha256") != council["page_sha256"]
                          or previous.get("pdf_sha256") != council["pdf_sha256"]
                          or previous.get("pdf_url") != council["pdf_url"]))
        council["changed"] = bool(changed)
        result["council"] = council
        if changed:
            result["alerts"].append({
                "kind": "council_document_changed",
                "pdf_url": pdf_url,
                "note": "取扱い要領の変更。種類識別 (§4) への影響判断が必要"})
    except Exception as e:
        result["failed_sources"].append(
            {"source": "council", "error": f"{type(e).__name__}: {e}"[:500]})

    arelle_info, arelle_source = load_arelle_taxonomies(fetcher)
    if arelle_info is None:
        result["failed_sources"].append(
            {"source": "arelle", "error": "有効一覧を取得できませんでした"})
    else:
        required = state.get("required_taxonomy_dates") or []
        coverage = arelle_coverage(required, arelle_info)
        result["arelle"] = {**coverage, "source": arelle_source}
        if required and not coverage["ok"]:
            result["alerts"].append({
                "kind": "arelle_not_following",
                "missing_dates": coverage["missing"]})

    if save_state:
        new_state = {
            "checked_at": checked_at,
            "latest_edition": (result["list_page"] or {}).get("latest_edition",
                                state.get("latest_edition")),
            "latest_published": (result["list_page"] or {}).get(
                "latest_published", state.get("latest_published")),
            "list_page_sha256": (result["list_page"] or {}).get(
                "sha256", state.get("list_page_sha256")),
            "portal_newest_all_date": (result["portal"] or {}).get(
                "newest_all_date", state.get("portal_newest_all_date")),
            "portal_sha256": (result["portal"] or {}).get(
                "sha256", state.get("portal_sha256")),
            "council": result["council"] or state.get("council"),
            "arelle": result["arelle"] or state.get("arelle"),
            "required_taxonomy_dates": state.get("required_taxonomy_dates"),
            "ingested_editions": sorted(ingested),
        }
        result["state"] = new_state
        local = os.path.join(work_dir, "state", "taxonomy-state-new.json")
        _write_local(local, json.dumps(new_state, ensure_ascii=False,
                                       indent=2))
        _upload_file(api, repo, local, "taxonomy/state.json",
                     f"taxonomy check {_today_jst()}: state")
    summary_path = os.path.join(work_dir, "out", "check_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result


# ---------------------------------------------------------------- 取込 (ingest)

def run_ingest(work_dir, edition, zip_bytes, published=None, zip_url=None,
               repo=DEFAULT_REPO, indicator_elements=(),
               silver_elements=None):
    """1版を取り込み、要素辞書と差分を保存する。実行サマリを返す。

    同じ版が同じ ZIP ハッシュで保存済みなら already_present (冪等)。
    同じ版で異なるハッシュなら conflict として記録し、先に保存された
    ものを正とする (codelist と同じ扱い)。競合の判定は meta の ZIP
    ハッシュだけでなく、保存済みの要素辞書自体のハッシュでも行う
    (elements だけが先行保存された部分状態で、別内容の辞書と新内容の
    meta が組み合わさる食い違いを防ぐため)。差分の生成は本体の保存に
    従属し、失敗しても本体の保存は成功として記録するが、未生成
    (pending) は警報と目録に残し、次回の実行が不足分を補完する。
    """
    api = HfApi()
    stage_dir = os.path.join(work_dir, "stage")
    state_dir = os.path.join(work_dir, "state")
    out_dir = os.path.join(work_dir, "out")
    for d in (stage_dir, state_dir, out_dir):
        os.makedirs(d, exist_ok=True)
    fetched_at = _now()
    summary = {
        "run_date": _today_jst(), "fetched_at": fetched_at,
        "edition": edition, "status": None, "uploaded": [],
        "alerts": [],
    }
    parsed = parse_taxonomy_zip(zip_bytes)
    elements = parsed["elements"]
    summary["element_count"] = len(elements)
    summary["taxonomy_dates"] = parsed["taxonomy_dates"]
    summary["sha256_zip"] = sha256_bytes(zip_bytes)
    paths = edition_paths(edition)
    remote = _remote_files(api, repo)
    elements_text = "".join(
        json.dumps(elements[key], ensure_ascii=False) + "\n"
        for key in sorted(elements))
    summary["sha256_elements"] = sha256_bytes(
        elements_text.encode("utf-8"))

    def _elements_conflict():
        """保存済みの要素辞書と今回の辞書のハッシュが食い違うか。"""
        if paths["elements"] not in remote:
            return False
        existing_bytes = _remote_bytes(api, repo, paths["elements"],
                                       work_dir, f"elements-{edition}.jsonl")
        if existing_bytes is None:
            return False
        existing_sha = sha256_bytes(existing_bytes)
        if existing_sha == summary["sha256_elements"]:
            return False
        summary["existing_sha256_elements"] = existing_sha
        return True

    if paths["meta"] in remote:
        existing = _read_remote_json(api, repo, paths["meta"], work_dir,
                                     f"meta-{edition}.json") or {}
        if existing.get("sha256_zip") != summary["sha256_zip"] \
                or _elements_conflict():
            summary["status"] = "conflict"
            summary["existing_sha256_zip"] = existing.get("sha256_zip")
            _finish_ingest(api, repo, work_dir, summary)
            return summary
        summary["status"] = "already_present"
        dres = _complete_diff(api, repo, work_dir, summary, edition,
                              elements, remote, fetched_at,
                              indicator_elements)
        _apply_diff_result(summary, edition, dres)
        _apply_arelle_check(summary, parsed["taxonomy_dates"])
        _finish_ingest(api, repo, work_dir, summary)
        return summary

    if _elements_conflict():
        # meta が無いのに要素辞書だけが別内容で保存されている部分状態。
        # meta だけを新内容で書くと辞書と出所が食い違うため競合にする。
        summary["status"] = "conflict"
        summary["conflict_note"] = "elements_only_partial_state"
        _finish_ingest(api, repo, work_dir, summary)
        return summary

    meta = {
        "edition": edition, "published": published,
        "zip_url": zip_url, "sha256_zip": summary["sha256_zip"],
        "sha256_elements": summary["sha256_elements"],
        "element_count": len(elements),
        "taxonomy_dates": parsed["taxonomy_dates"],
        "schema_count": parsed["schema_count"],
        "member_count": parsed["member_count"],
        "source": SOURCE_LIST, "source_id": zip_url or LIST_PAGE_URL,
        "fetched_at": fetched_at, "redistribution": REDISTRIBUTION,
    }
    products = [
        (paths["archive"], zip_bytes,
         f"taxonomy {edition}: original zip (archive)"),
        (paths["elements"], elements_text,
         f"taxonomy {edition}: elements dictionary"),
        (paths["meta"], json.dumps(meta, ensure_ascii=False, indent=2),
         f"taxonomy {edition}: meta"),
    ]
    for repo_path, data, message in products:
        if repo_path in remote:
            continue
        local = os.path.join(stage_dir, repo_path)
        _write_local(local, data)
        _upload_file(api, repo, local, repo_path, message)
        summary["uploaded"].append(repo_path)
        remote.add(repo_path)

    # 差分 (直前版との比較)。従属工程として失敗を隔離し、未生成は
    # 警報と目録に残して次回の実行で補完する。
    dres = _complete_diff(api, repo, work_dir, summary, edition,
                          elements, remote, fetched_at,
                          indicator_elements)
    _apply_diff_result(summary, edition, dres)

    # 未知要素の隔離 (Silver の要素一覧が与えられた場合)。
    if silver_elements is not None:
        unknown = find_unknown(silver_elements, elements)
        summary["unknown_elements"] = unknown
        if unknown:
            summary["alerts"].append({
                "kind": "unknown_elements", "count": len(unknown),
                "elements": unknown})
            if paths["quarantine"] not in remote:
                quarantine_text = "".join(
                    json.dumps({"element": el, "edition": edition,
                                "detected_at": fetched_at,
                                "source": "silver_elements"},
                               ensure_ascii=False) + "\n" for el in unknown)
                local = os.path.join(stage_dir, paths["quarantine"])
                _write_local(local, quarantine_text)
                _upload_file(api, repo, local, paths["quarantine"],
                             f"taxonomy {edition}: unknown elements quarantine")
                summary["uploaded"].append(paths["quarantine"])
                remote.add(paths["quarantine"])

    # arelle の追随確認 (取り込んだ版の日付で)。
    _apply_arelle_check(summary, parsed["taxonomy_dates"])
    summary["status"] = "ok"
    _finish_ingest(api, repo, work_dir, summary)
    return summary


def _finish_ingest(api, repo, work_dir, summary):
    summary["finished_at"] = _now()
    row = {
        "date": summary.get("edition"), "run_date": summary["run_date"],
        "status": summary["status"], "fetched_at": summary["fetched_at"],
        "sha256_zip": summary.get("sha256_zip"),
        "element_count": summary.get("element_count"),
        "diff_status": summary.get("diff_status"),
        "alerts": summary.get("alerts"),
        "source": SOURCE_LIST, "redistribution": REDISTRIBUTION,
    }
    if summary.get("diff_error"):
        row["diff_error"] = summary["diff_error"]
    try:
        _append_ledger(api, repo, work_dir, row)
    except Exception as e:
        summary["ledger_error"] = f"{type(e).__name__}: {e}"[:500]
    summary_path = os.path.join(work_dir, "out", "run_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


# ---------------------------------------------------------------- 一括 (run)

def run_all(work_dir, repo=DEFAULT_REPO, indicator_elements=(),
            silver_elements=None, fetcher=None):
    """確認→新版があれば取込までを一括で行う (ワークフローの入口)。

    取込が成功 (ok / already_present) した場合だけ state の
    required_taxonomy_dates と ingested_editions を更新する。ZIP の
    取得や取込が失敗した場合は例外として表面化し、取込済み状態は
    進まないため、次回の実行で検出・取込が再試行される。
    """
    fetcher = fetcher or fetch_url
    check = run_check(work_dir, repo=repo, fetcher=fetcher)
    summary = {"check": check, "ingest": None}
    new_edition = check.get("new_edition")
    if new_edition:
        zip_bytes = fetcher(new_edition["zip_url"])
        ingest = run_ingest(
            work_dir, new_edition["edition"], zip_bytes,
            published=new_edition["published"],
            zip_url=new_edition["zip_url"], repo=repo,
            indicator_elements=indicator_elements,
            silver_elements=silver_elements)
        summary["ingest"] = ingest
        if ingest.get("status") in ("ok", "already_present"):
            # state の更新 (取り込んだ版と必要な日付)。
            api = HfApi()
            state = _read_remote_json(api, repo, "taxonomy/state.json",
                                      work_dir, "taxonomy-state.json") or {}
            ingested = sorted(set((state.get("ingested_editions") or [])
                                  + [new_edition["edition"]]))
            state["ingested_editions"] = ingested
            dates = ingest.get("taxonomy_dates") or []
            state["required_taxonomy_dates"] = [max(dates)] if dates else []
            state["ingested_taxonomy_dates"] = dates
            local = os.path.join(work_dir, "state", "taxonomy-state-new.json")
            _write_local(local, json.dumps(state, ensure_ascii=False, indent=2))
            _upload_file(api, repo, local, "taxonomy/state.json",
                         f"taxonomy {new_edition['edition']}: state after ingest")
    summary_path = os.path.join(work_dir, "out", "run_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="EDINET タクソノミの取得・年次更新と継続確認 (P2-T9)")
    sub = ap.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check", help="継続確認を1回行う")
    check.add_argument("--work-dir", required=True)
    check.add_argument("--repo", default=DEFAULT_REPO)

    ingest = sub.add_parser("ingest", help="1版を取り込む")
    ingest.add_argument("--work-dir", required=True)
    ingest.add_argument("--repo", default=DEFAULT_REPO)
    ingest.add_argument("--edition", type=int, required=True)
    ingest.add_argument("--zip-file", help="取得済み ZIP のパス")
    ingest.add_argument("--zip-url", help="ZIP の URL (取得して取り込む)")
    ingest.add_argument("--published", help="公表日 YYYY-MM-DD")
    ingest.add_argument("--indicator-elements",
                        help="指標に効く要素の一覧ファイル")
    ingest.add_argument("--silver-elements",
                        help="未知要素を確認する Silver 要素の一覧ファイル"
                             " (1行1要素)")

    run = sub.add_parser("run", help="確認→新版の取込までを一括で行う")
    run.add_argument("--work-dir", required=True)
    run.add_argument("--repo", default=DEFAULT_REPO)
    run.add_argument("--indicator-elements",
                     help="指標に効く要素の一覧ファイル")
    run.add_argument("--silver-elements",
                     help="未知要素を確認する Silver 要素の一覧ファイル")

    diff_cmd = sub.add_parser("diff", help="要素辞書2ファイルの差分を表示する")
    diff_cmd.add_argument("--old", required=True, help="旧版の elements.jsonl")
    diff_cmd.add_argument("--new", required=True, help="新版の elements.jsonl")
    diff_cmd.add_argument("--indicator-elements",
                          help="指標に効く要素の一覧ファイル")
    diff_cmd.add_argument("--out", help="差分の JSON の出力先")

    quar = sub.add_parser("quarantine",
                          help="Silver 要素のうち辞書に無いものを表示する")
    quar.add_argument("--dictionary", required=True,
                      help="要素辞書の elements.jsonl")
    quar.add_argument("--silver-elements", required=True,
                      help="Silver 要素の一覧ファイル (1行1要素)")

    args = ap.parse_args(argv)
    try:
        if args.command == "check":
            result = run_check(args.work_dir, repo=args.repo)
            print(json.dumps({
                "new_edition": result["new_edition"],
                "alerts": result["alerts"],
                "failed_sources": result["failed_sources"],
            }, ensure_ascii=False))
            return 1 if result["failed_sources"] else 0
        if args.command == "ingest":
            if bool(args.zip_file) == bool(args.zip_url):
                ap.error("ingest には --zip-file か --zip-url のどちらか"
                         "一方を指定してください")
            if args.zip_file:
                with open(args.zip_file, "rb") as f:
                    zip_bytes = f.read()
                zip_url = args.zip_url
            else:
                zip_bytes = fetch_url(args.zip_url)
                zip_url = args.zip_url
            indicators = (load_indicator_elements(args.indicator_elements)
                          if args.indicator_elements else ())
            silver = (load_indicator_elements(args.silver_elements)
                      if args.silver_elements else None)
            summary = run_ingest(
                args.work_dir, args.edition, zip_bytes,
                published=args.published, zip_url=zip_url, repo=args.repo,
                indicator_elements=indicators, silver_elements=silver)
            print(json.dumps(summary, ensure_ascii=False))
            return 0 if summary["status"] in ("ok", "already_present") else 1
        if args.command == "run":
            indicators = (load_indicator_elements(args.indicator_elements)
                          if args.indicator_elements else ())
            silver = (load_indicator_elements(args.silver_elements)
                      if args.silver_elements else None)
            summary = run_all(args.work_dir, repo=args.repo,
                              indicator_elements=indicators,
                              silver_elements=silver)
            print(json.dumps({
                "new_edition": (summary["check"] or {}).get("new_edition"),
                "ingest_status": (summary["ingest"] or {}).get("status"),
                "check_alerts": (summary["check"] or {}).get("alerts"),
                "ingest_alerts": (summary["ingest"] or {}).get("alerts"),
            }, ensure_ascii=False))
            return 0
        if args.command == "diff":
            def _load(path):
                elements = {}
                with open(path, encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            el = json.loads(line)
                            elements[el["key"]] = el
                return elements
            indicators = (load_indicator_elements(args.indicator_elements)
                          if args.indicator_elements else ())
            diff = compute_diff(_load(args.old), _load(args.new), indicators)
            doc = {"counts": diff["counts"],
                   "rename_candidates": diff["rename_candidates"],
                   "split_candidates": diff["split_candidates"],
                   "judgment_items": diff["judgment_items"],
                   "changed": diff["changed"]}
            if args.out:
                with open(args.out, "w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)
            print(json.dumps(doc["counts"], ensure_ascii=False))
            return 0
        if args.command == "quarantine":
            dictionary = {}
            with open(args.dictionary, encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        el = json.loads(line)
                        dictionary[el["key"]] = el
            silver = load_indicator_elements(args.silver_elements)
            unknown = find_unknown(silver, dictionary)
            print(json.dumps({"unknown": unknown,
                              "count": len(unknown)}, ensure_ascii=False))
            return 1 if unknown else 0
    except Exception:
        traceback.print_exc()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
