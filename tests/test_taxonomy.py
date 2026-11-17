"""pipeline.taxonomy のテスト (P2-T9)。

実行: リポジトリのルートで `python -m unittest discover -s tests`
"""
import io
import json
import os
import sys
import unittest
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import taxonomy
from pipeline.taxonomy import (
    TaxonomyError, arelle_coverage, compute_diff, edition_year_for,
    find_unknown, parse_arelle_taxonomies, parse_council_page,
    parse_list_page, parse_portal_page, parse_taxonomy_zip,
)

LIST_PAGE_HTML = """
<html><body>
<a href="https://www.fsa.go.jp/search/20241112/1c_Taxonomy.zip">
2025年版EDINETタクソノミ [2024年11月12日]</a>
<a href="https://www.fsa.go.jp/search/20251111/1c_Taxonomy.zip">
2026年版EDINETタクソノミ [2025年11月11日]</a>
</body></html>
"""

PORTAL_HTML = """
<a onclick="onDownload('ALL_20241101.zip');return false;">2025</a>
<a onclick="onDownload('ALL_20251101.zip');return false;">2026</a>
<a onclick="onDownload('01_20251101.zip'); return false;">DEI</a>
<a onclick="onDownload('02_20241101.zip'); return false;">PFS old</a>
<a onclick="onDownload('02_20251101.zip'); return false;">PFS</a>
"""

COUNCIL_HTML = """
<a href="/sicc/securities-code/nlsgeu0000032d48-att/codestock_20250701.pdf">
株式及び公社債銘柄コードの設定、変更及び削除に関する取扱い要領</a>
"""

XSD_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema targetNamespace="http://disclosure.edinet-fsa.go.jp/taxonomy/{fam}/{date}/{fam}_cor"
 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
 xmlns:xbrli="http://www.xbrl.org/2003/instance">
{elements}
</xsd:schema>
"""

ELEMENT = ('<xsd:element name="{name}" id="{fam}_cor_{name}" '
           'type="xbrli:monetaryItemType" substitutionGroup="xbrli:item" '
           'abstract="false" xbrli:balance="debit" '
           'xbrli:periodType="instant"/>')

LABEL_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase"
 xmlns:xlink="http://www.w3.org/1999/xlink">
 <link:labelLink xlink:type="extended"
  xlink:role="http://www.xbrl.org/2003/role/link">
{body}
 </link:labelLink>
</link:linkbase>
"""


def label_file(fam, entries, lang):
    """entries: [(要素名, ラベル)] からラベルリンクベースを作る。"""
    parts = []
    for i, (name, text) in enumerate(entries):
        parts.append(
            f'<link:loc xlink:type="resource" xlink:label="loc{i}" '
            f'xlink:href="{fam}_cor.xsd#{fam}_cor_{name}"/>')
        parts.append(
            f'<link:labelArc xlink:type="arc" xlink:from="loc{i}" '
            f'xlink:to="lab{i}" '
            f'xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label"/>')
        parts.append(
            f'<link:label xlink:type="resource" xlink:label="lab{i}" '
            f'xlink:role="http://www.xbrl.org/2003/role/label" '
            f'xml:lang="{lang}">{text}</link:label>')
    return LABEL_TEMPLATE.format(body="\n".join(parts))


def make_taxonomy_zip(date="2024-11-01", extra_elements=(), drop=()):
    """jppfs の小さいタクソノミ ZIP を作る (要素3件+廃止1件)。"""
    elements = [
        ("CashAndDeposits", "現金及び預金", "Cash and deposits"),
        ("OperatingIncome", "営業利益", "Operating income"),
        ("NetSales", "売上高", "Net sales"),
    ]
    elements = [e for e in elements if e[0] not in drop] + list(extra_elements)
    fam = "jppfs"
    xsd = XSD_TEMPLATE.format(
        fam=fam, date=date,
        elements="\n".join(ELEMENT.format(name=n, fam=fam)
                           for n, _ja, _en in elements))
    dep_xsd = XSD_TEMPLATE.format(
        fam=fam, date=date,
        elements=ELEMENT.format(name="OldElement", fam=fam))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        base = f"taxonomy/{fam}/{date}"
        zf.writestr(f"{base}/{fam}_cor_{date}.xsd", xsd)
        zf.writestr(f"{base}/deprecated/{fam}_dep_{date}.xsd", dep_xsd)
        zf.writestr(f"{base}/label/{fam}_{date}_lab.xml",
                    label_file(fam, [(n, ja) for n, ja, _e in elements], "ja"))
        zf.writestr(f"{base}/label/{fam}_{date}_lab-en.xml",
                    label_file(fam, [(n, en) for n, _j, en in elements], "en"))
    return buf.getvalue()


class ListPageTest(unittest.TestCase):
    def test_parse_list_page(self):
        editions = parse_list_page(LIST_PAGE_HTML)
        self.assertEqual([e["edition"] for e in editions], [2026, 2025])
        self.assertEqual(editions[0]["published"], "2025-11-11")
        self.assertEqual(
            editions[0]["zip_url"],
            "https://www.fsa.go.jp/search/20251111/1c_Taxonomy.zip")

    def test_edition_year_rule(self):
        self.assertEqual(edition_year_for("2025-11-11"), 2026)
        self.assertEqual(edition_year_for("2024-11-12"), 2025)
        self.assertEqual(edition_year_for("2023-12-11"), 2024)
        self.assertEqual(edition_year_for("2022-11-08"), 2023)


class PortalPageTest(unittest.TestCase):
    def test_parse_portal(self):
        portal = parse_portal_page(PORTAL_HTML)
        self.assertEqual(portal["newest_all_date"], "2025-11-01")
        self.assertEqual(portal["newest_by_section"]["01"], "2025-11-01")
        self.assertEqual(portal["newest_by_section"]["02"], "2025-11-01")
        self.assertIn("ALL_20251101.zip", portal["files"])


class CouncilPageTest(unittest.TestCase):
    def test_parse_council_page(self):
        url = parse_council_page(COUNCIL_HTML)
        self.assertEqual(
            url, "https://www.jpx.co.jp/sicc/securities-code/"
                 "nlsgeu0000032d48-att/codestock_20250701.pdf")

    def test_parse_council_page_missing(self):
        self.assertIsNone(parse_council_page("<html></html>"))


class ParseZipTest(unittest.TestCase):
    def test_parse_elements_and_labels(self):
        parsed = parse_taxonomy_zip(make_taxonomy_zip())
        elements = parsed["elements"]
        self.assertEqual(parsed["taxonomy_dates"], ["2024-11-01"])
        cash = elements["jppfs_cor:CashAndDeposits"]
        self.assertEqual(cash["label_ja"], "現金及び預金")
        self.assertEqual(cash["label_en"], "Cash and deposits")
        self.assertEqual(cash["period_type"], "instant")
        self.assertEqual(cash["balance"], "debit")
        self.assertFalse(cash["deprecated"])
        self.assertTrue(elements["jppfs_cor:OldElement"]["deprecated"])

    def test_parse_empty_zip_fails(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "hello")
        with self.assertRaises(TaxonomyError):
            parse_taxonomy_zip(buf.getvalue())

    def test_parse_not_zip_fails(self):
        with self.assertRaises(TaxonomyError):
            parse_taxonomy_zip(b"this is not a zip")


class DiffTest(unittest.TestCase):
    def setUp(self):
        self.old = parse_taxonomy_zip(make_taxonomy_zip())["elements"]

    def test_identical_is_empty(self):
        diff = compute_diff(self.old, dict(self.old))
        self.assertEqual(diff["counts"]["added"], 0)
        self.assertEqual(diff["counts"]["removed"], 0)
        self.assertEqual(diff["counts"]["changed"], 0)

    def test_date_rollforward_is_date_only(self):
        new = parse_taxonomy_zip(make_taxonomy_zip(date="2025-11-01"))
        diff = compute_diff(self.old, new["elements"])
        self.assertEqual(diff["counts"]["changed"], 0)
        self.assertEqual(diff["counts"]["date_only"], 4)

    def test_rename_candidate_and_judgment(self):
        new = parse_taxonomy_zip(make_taxonomy_zip(
            drop=("NetSales",),
            extra_elements=[("Revenue", "売上高", "Net sales")]))["elements"]
        diff = compute_diff(self.old, new,
                            indicator_elements=["jppfs_cor:NetSales"])
        self.assertEqual(diff["counts"]["rename_candidates"], 1)
        cand = diff["rename_candidates"][0]
        self.assertEqual(cand["from"], "jppfs_cor:NetSales")
        self.assertEqual(cand["to"], "jppfs_cor:Revenue")
        kinds = {(j["element"], j["kind"]) for j in diff["judgment_items"]}
        self.assertIn(("jppfs_cor:NetSales", "removed"), kinds)
        self.assertIn(("jppfs_cor:NetSales", "rename_candidate"), kinds)

    def test_label_change_is_semantic_change(self):
        new = {k: dict(v) for k, v in self.old.items()}
        new["jppfs_cor:OperatingIncome"]["label_ja"] = "営業利益（改定）"
        diff = compute_diff(self.old, new,
                            indicator_elements=["jppfs_cor:OperatingIncome"])
        self.assertIn("jppfs_cor:OperatingIncome", diff["changed"])
        self.assertEqual(
            diff["changed"]["jppfs_cor:OperatingIncome"]["label_ja"],
            ["営業利益", "営業利益（改定）"])
        self.assertTrue(any(
            j["kind"] == "changed" for j in diff["judgment_items"]))


class UnknownTest(unittest.TestCase):
    """合格条件8の故障ケース: 未知要素が隔離され、拡張要素は対象外。"""

    def test_unknown_quarantined(self):
        dictionary = parse_taxonomy_zip(make_taxonomy_zip())["elements"]
        silver = [
            "jppfs_cor:CashAndDeposits",      # 既知
            "jppfs_cor:NoSuchElementXYZ",     # 未知 (標準の接頭辞)
            "E00988-000:FilerSpecificThing",  # 提出者拡張 (対象外)
        ]
        unknown = find_unknown(silver, dictionary)
        self.assertEqual(unknown, ["jppfs_cor:NoSuchElementXYZ"])

    def test_all_known_is_empty(self):
        dictionary = parse_taxonomy_zip(make_taxonomy_zip())["elements"]
        self.assertEqual(find_unknown(list(dictionary), dictionary), [])


class ArelleTest(unittest.TestCase):
    XML = (b'<Erxl><Loc><Href>http://disclosure.edinet-fsa.go.jp/taxonomy/'
           b'jppfs/2025-11-01/jppfs_cor_2025-11-01.xsd</Href></Loc>'
           b'<Loc><Href>http://disclosure.edinet-fsa.go.jp/taxonomy/'
           b'jpdei/2013-08-31/jpdei_cor_2013-08-31.xsd</Href></Loc></Erxl>')

    def test_coverage_ok(self):
        info = parse_arelle_taxonomies(self.XML)
        cov = arelle_coverage(["2013-08-31", "2025-11-01"], info)
        self.assertTrue(cov["ok"])
        self.assertEqual(cov["missing"], [])

    def test_coverage_missing_is_alert(self):
        info = parse_arelle_taxonomies(self.XML)
        cov = arelle_coverage(["2025-11-01", "2026-11-01"], info)
        self.assertFalse(cov["ok"])
        self.assertEqual(cov["missing"], ["2026-11-01"])

    def test_date_report_legacy_dates_are_not_alerts(self):
        # 版に同梱の旧日付 (2021-11-01 等) が一覧に無くても警報にしない。
        # 必須は版の最新日付だけ (実測に基づく。taxonomy.py の docstring)。
        info = parse_arelle_taxonomies(self.XML)
        report = taxonomy.arelle_date_report(
            ["2013-08-31", "2021-11-01", "2025-11-01"], info)
        self.assertTrue(report["ok"])
        self.assertEqual(report["primary_date"], "2025-11-01")
        self.assertEqual(report["uncovered_legacy"], ["2021-11-01"])

    def test_date_report_new_date_missing_is_alert(self):
        info = parse_arelle_taxonomies(self.XML)
        report = taxonomy.arelle_date_report(
            ["2013-08-31", "2026-11-01"], info)
        self.assertFalse(report["ok"])
        self.assertEqual(report["missing"], ["2026-11-01"])


if __name__ == "__main__":
    unittest.main()
