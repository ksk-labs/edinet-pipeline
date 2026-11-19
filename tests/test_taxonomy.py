"""pipeline.taxonomy のテスト (P2-T9)。

実行: リポジトリのルートで `python -m unittest discover -s tests`
"""
import io
import json
import os
import re
import sys
import tempfile
import unittest
import zipfile
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import taxonomy
from pipeline.taxonomy import (
    TaxonomyError, arelle_coverage, compute_diff, edition_year_for,
    find_unknown, parse_arelle_taxonomies, parse_council_page,
    parse_list_page, parse_portal_page, parse_taxonomy_zip,
)

LIST_PAGE_HTML = """
<html><body>
<a href="/search/20241112.html">2025年版EDINETタクソノミ</a> [2024年(令和６年)11月12日]
<a href="/search/20251111.html">2026年版EDINETタクソノミ</a> [2025年(令和７年)11月11日]
</body></html>
"""

DETAIL_PAGE_HTML = """
<html><body>
<a href="/search/{digits}/1c_Taxonomy.zip">EDINETタクソノミ</a>
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
        entries = parse_list_page(LIST_PAGE_HTML)
        self.assertEqual([e["edition"] for e in entries], [2026, 2025])
        self.assertEqual(entries[0]["published"], "2025-11-11")
        self.assertEqual(
            entries[0]["detail_url"],
            "https://www.fsa.go.jp/search/20251111.html")

    def test_parse_detail_page(self):
        html = DETAIL_PAGE_HTML.format(digits="20251111")
        self.assertEqual(
            taxonomy.parse_detail_page(html),
            "https://www.fsa.go.jp/search/20251111/1c_Taxonomy.zip")
        self.assertIsNone(taxonomy.parse_detail_page("<html></html>"))

    def test_fetch_editions_follows_detail_pages(self):
        # MF-1 の回帰: 実ページの構造 (一覧→日付別詳細ページ→ZIP) で
        # 版が解決できること。一覧ページ自体に ZIP リンクは無い。
        self.assertNotIn("1c_Taxonomy.zip", LIST_PAGE_HTML)
        editions = taxonomy.fetch_editions(make_fetcher(), LIST_PAGE_HTML)
        self.assertEqual([e["edition"] for e in editions], [2026, 2025])
        self.assertEqual(editions[0]["published"], "2025-11-11")
        self.assertEqual(
            editions[0]["zip_url"],
            "https://www.fsa.go.jp/search/20251111/1c_Taxonomy.zip")

    def test_fetch_editions_latest_without_zip_fails(self):
        def fetch(url):
            return b"<html>no zip link</html>"
        with self.assertRaises(TaxonomyError):
            taxonomy.fetch_editions(fetch, LIST_PAGE_HTML)

    def test_fetch_editions_drops_unresolvable_old_entry(self):
        # 初期の版は詳細ページの配布形式が違い ZIP リンクが無い。
        # 最新の版が解決できれば、古い版は結果から除いてよい。
        def fetch(url):
            if url.endswith("20251111.html"):
                return DETAIL_PAGE_HTML.format(digits="20251111").encode()
            return b"<html>old format</html>"
        editions = taxonomy.fetch_editions(fetch, LIST_PAGE_HTML)
        self.assertEqual([e["edition"] for e in editions], [2026])

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


class SplitCandidateTest(unittest.TestCase):
    """分割候補 (S1): 日本語ラベルの包含+同一接頭辞+追加2件以上。"""

    def setUp(self):
        self.old = parse_taxonomy_zip(make_taxonomy_zip())["elements"]

    def test_split_candidate_and_judgment(self):
        new = parse_taxonomy_zip(make_taxonomy_zip(
            drop=("CashAndDeposits",),
            extra_elements=[
                ("CashAndDepositsDomestic", "現金及び預金（国内）",
                 "Cash and deposits (domestic)"),
                ("CashAndDepositsOverseas", "現金及び預金（海外）",
                 "Cash and deposits (overseas)"),
            ]))["elements"]
        diff = compute_diff(
            self.old, new,
            indicator_elements=["jppfs_cor:CashAndDeposits"])
        self.assertEqual(diff["counts"]["split_candidates"], 1)
        cand = diff["split_candidates"][0]
        self.assertEqual(cand["from"], "jppfs_cor:CashAndDeposits")
        self.assertEqual(sorted(cand["to"]), [
            "jppfs_cor:CashAndDepositsDomestic",
            "jppfs_cor:CashAndDepositsOverseas"])
        kinds = {(j["element"], j["kind"]) for j in diff["judgment_items"]}
        self.assertIn(("jppfs_cor:CashAndDeposits", "split_candidate"),
                      kinds)

    def test_single_partial_match_is_not_split(self):
        new = parse_taxonomy_zip(make_taxonomy_zip(
            drop=("CashAndDeposits",),
            extra_elements=[
                ("CashAndDepositsDomestic", "現金及び預金（国内）",
                 "Cash and deposits (domestic)"),
            ]))["elements"]
        diff = compute_diff(self.old, new)
        self.assertEqual(diff["counts"]["split_candidates"], 0)


class SemanticFieldsTest(unittest.TestCase):
    def test_substitution_group_change_is_semantic(self):
        old = parse_taxonomy_zip(make_taxonomy_zip())["elements"]
        new = {k: dict(v) for k, v in old.items()}
        new["jppfs_cor:CashAndDeposits"]["substitution_group"] = \
            "xbrli:tuple"
        diff = compute_diff(old, new)
        self.assertIn("substitution_group",
                      diff["changed"]["jppfs_cor:CashAndDeposits"])


class FakeRemote:
    """HF データセットの代役。保存経路の関数を差し替えて使う。"""

    def __init__(self):
        self.files = {}
        self.uploads = []

    def list_files(self, api, repo):
        return set(self.files)

    def download(self, api, repo, path_in_repo, local_path):
        if path_in_repo not in self.files:
            return False
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(self.files[path_in_repo])
        return True

    def upload(self, api, repo, local_path, path_in_repo, message):
        with open(local_path, "rb") as f:
            self.files[path_in_repo] = f.read()
        self.uploads.append(path_in_repo)


def make_fetcher(portal_html=PORTAL_HTML, zip_bytes=None, fail_zip=False):
    def fetch(url):
        if url == taxonomy.LIST_PAGE_URL:
            return LIST_PAGE_HTML.encode()
        detail = re.search(r"/search/(\d{8})\.html$", url)
        if detail:
            return DETAIL_PAGE_HTML.format(
                digits=detail.group(1)).encode()
        if url == taxonomy.PORTAL_URL:
            return portal_html.encode()
        if url == taxonomy.COUNCIL_PAGE_URL:
            return COUNCIL_HTML.encode()
        if "codestock_" in url:
            return b"%PDF-fake"
        if url == taxonomy.ARELLE_UPSTREAM_URL:
            return ArelleTest.XML
        if url.endswith("1c_Taxonomy.zip"):
            if fail_zip:
                raise TaxonomyError("zip の取得に失敗しました (代役)")
            return zip_bytes
        raise AssertionError(f"想定外の URL: {url}")
    return fetch


class OrchestrationBase(unittest.TestCase):
    def setUp(self):
        self.remote = FakeRemote()
        self.work = tempfile.mkdtemp(prefix="taxonomy-test-")
        arelle_info = parse_arelle_taxonomies(ArelleTest.XML)
        self._patches = [
            mock.patch.object(taxonomy, "HfApi", lambda *a, **k: object()),
            mock.patch.object(taxonomy, "_remote_files",
                              self.remote.list_files),
            mock.patch.object(taxonomy, "_download_if_exists",
                              self.remote.download),
            mock.patch.object(taxonomy, "_upload_file", self.remote.upload),
            mock.patch.object(
                taxonomy, "load_arelle_taxonomies",
                lambda fetcher=None: (arelle_info, "test:fake")),
        ]
        for p in self._patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self._patches])


class CheckOrchestrationTest(OrchestrationBase):
    def test_check_does_not_consume_detection(self):
        # M1 の回帰: 確認のみを繰り返しても、取込済みでない最新版は
        # 検出され続ける (state の latest_edition が進んでも消費されない)。
        fetcher = make_fetcher()
        first = taxonomy.run_check(self.work, fetcher=fetcher)
        self.assertEqual(first["new_edition"]["edition"], 2026)
        state = json.loads(self.remote.files["taxonomy/state.json"])
        self.assertEqual(state["latest_edition"], 2026)
        self.assertEqual(state["ingested_editions"], [])
        second = taxonomy.run_check(self.work, fetcher=fetcher)
        self.assertEqual(second["new_edition"]["edition"], 2026)
        # リモートに版の meta があれば取込済みとして検出は止まる。
        self.remote.files["taxonomy/edition=2026/meta.json"] = b"{}"
        third = taxonomy.run_check(self.work, fetcher=fetcher)
        self.assertIsNone(third["new_edition"])

    def test_portal_empty_is_failed_source(self):
        # M2 の回帰: ポータルの抽出が0件なら failed_sources に残る。
        fetcher = make_fetcher(portal_html="<html>no downloads</html>")
        result = taxonomy.run_check(self.work, fetcher=fetcher)
        sources = {s["source"] for s in result["failed_sources"]}
        self.assertIn("portal", sources)
        self.assertIsNotNone(result["list_page"])
        self.assertEqual(result["new_edition"]["edition"], 2026)

    def test_failed_source_is_isolated(self):
        def fetch(url):
            if url == taxonomy.COUNCIL_PAGE_URL:
                raise RuntimeError("council down (fake)")
            return make_fetcher()(url)
        result = taxonomy.run_check(self.work, fetcher=fetch)
        self.assertEqual([s["source"] for s in result["failed_sources"]],
                         ["council"])
        self.assertIsNotNone(result["portal"])


class IngestOrchestrationTest(OrchestrationBase):
    def setUp(self):
        super().setUp()
        self.zip2026 = make_taxonomy_zip(date="2025-11-01")

    def test_ingest_ok_then_already_present(self):
        first = taxonomy.run_ingest(self.work, 2026, self.zip2026)
        self.assertEqual(first["status"], "ok")
        paths = taxonomy.edition_paths(2026)
        for key in ("archive", "elements", "meta"):
            self.assertIn(paths[key], self.remote.files)
        self.remote.uploads.clear()
        second = taxonomy.run_ingest(self.work, 2026, self.zip2026)
        self.assertEqual(second["status"], "already_present")
        # 版の成果物は再送されない (目録への追記だけが行われる)。
        self.assertEqual(self.remote.uploads, ["taxonomy/ledger.jsonl"])

    def test_conflict_on_different_zip(self):
        taxonomy.run_ingest(self.work, 2026, self.zip2026)
        other = make_taxonomy_zip(
            date="2025-11-01",
            extra_elements=[("ExtraThing", "追加要素", "Extra thing")])
        result = taxonomy.run_ingest(self.work, 2026, other)
        self.assertEqual(result["status"], "conflict")

    def test_partial_elements_mismatch_is_conflict(self):
        # S8 の回帰: elements だけが別内容で先行保存された部分状態では、
        # meta だけを新内容で書かず競合にする。
        paths = taxonomy.edition_paths(2026)
        self.remote.files[paths["elements"]] = b"unrelated\n"
        result = taxonomy.run_ingest(self.work, 2026, self.zip2026)
        self.assertEqual(result["status"], "conflict")
        self.assertNotIn(paths["meta"], self.remote.files)

    def test_partial_elements_match_completes(self):
        paths = taxonomy.edition_paths(2026)
        elements = parse_taxonomy_zip(self.zip2026)["elements"]
        text = "".join(json.dumps(elements[k], ensure_ascii=False) + "\n"
                       for k in sorted(elements))
        self.remote.files[paths["elements"]] = text.encode("utf-8")
        result = taxonomy.run_ingest(self.work, 2026, self.zip2026)
        self.assertEqual(result["status"], "ok")
        self.assertIn(paths["meta"], self.remote.files)
        self.assertNotIn(paths["elements"], self.remote.uploads)

    def test_diff_pending_then_completed_on_retry(self):
        # S2 の回帰: 差分の生成失敗は警報と目録に pending として残り、
        # 次回の取込 (already_present) で補完される。
        zip2025 = make_taxonomy_zip(date="2024-11-01")
        taxonomy.run_ingest(self.work, 2025, zip2025)

        def boom(old, new, indicator_elements=()):
            raise RuntimeError("diff boom (fake)")
        with mock.patch.object(taxonomy, "compute_diff", boom):
            failed = taxonomy.run_ingest(self.work, 2026, self.zip2026)
        self.assertEqual(failed["status"], "ok")
        self.assertEqual(failed["diff_status"], "pending")
        self.assertIn("diff_pending",
                      {a["kind"] for a in failed["alerts"]})
        ledger = self.remote.files["taxonomy/ledger.jsonl"].decode()
        self.assertIn('"diff_status": "pending"', ledger)
        paths = taxonomy.edition_paths(2026)
        self.assertNotIn(paths["diff"], self.remote.files)

        retried = taxonomy.run_ingest(self.work, 2026, self.zip2026)
        self.assertEqual(retried["status"], "already_present")
        self.assertEqual(retried["diff_status"], "generated")
        self.assertIn(paths["diff"], self.remote.files)
        self.assertIn(paths["changes"], self.remote.files)

    def test_arelle_unavailable_is_alert(self):
        # S3 の回帰: 取込時に arelle の有効一覧が取得できない場合は
        # 無言で通さず警報にする。
        with mock.patch.object(taxonomy, "load_arelle_taxonomies",
                               lambda fetcher=None: (None, None)):
            result = taxonomy.run_ingest(self.work, 2026, self.zip2026)
        self.assertEqual(result["status"], "ok")
        self.assertIn("arelle_unavailable",
                      {a["kind"] for a in result["alerts"]})


class RunAllOrchestrationTest(OrchestrationBase):
    def test_failed_zip_fetch_is_retried_next_run(self):
        # M1 の回帰 (統括経路): check のみの実行で検出は消費されず、
        # ZIP 取得が失敗した run の翌回に再検出・再取込される。
        zip2026 = make_taxonomy_zip(date="2025-11-01")
        failing = make_fetcher(zip_bytes=zip2026, fail_zip=True)
        checked = taxonomy.run_check(self.work, fetcher=failing)
        self.assertEqual(checked["new_edition"]["edition"], 2026)
        with self.assertRaises(TaxonomyError):
            taxonomy.run_all(self.work, fetcher=failing)
        working = make_fetcher(zip_bytes=zip2026)
        summary = taxonomy.run_all(self.work, fetcher=working)
        self.assertEqual(summary["ingest"]["status"], "ok")
        again = taxonomy.run_all(self.work, fetcher=working)
        self.assertIsNone(again["check"]["new_edition"])
        self.assertIsNone(again["ingest"])


if __name__ == "__main__":
    unittest.main()
