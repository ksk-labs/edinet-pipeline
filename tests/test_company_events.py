"""会社イベントと親子関係 (P2-T4) のテスト。

提出事由の原子の分類、複数事由の分解、訂正の除外、対象期間の
線引き、親子関係の集約 (初回・最終観測日と観測回数) を固定する。
"""
import unittest

from pipeline.company_events import (
    EVENT_KIND_BY_ITEM,
    RESERVED_EVENT_KINDS,
    build_events,
    build_relationships,
    classify_atom,
    parse_reason_atoms,
)

GENERATED_AT = "2026-11-17T00:00:00+00:00"
SHA = "0" * 64
SINCE = "2021-11-01"
CODE_TO_ID = {"E00001": 1, "E00002": 2, "E00003": 3}


def filing(doc_id, doc_type, submit_at, code="E00001", reason=None,
           subsidiary=None):
    return {
        "doc_id": doc_id,
        "doc_type": doc_type,
        "submit_at": submit_at,
        "edinet_code": code,
        "current_report_reason": reason,
        "subsidiary_edinet_code": subsidiary,
        "parent_doc_id": None,
    }


class ParseReasonAtomsTest(unittest.TestCase):
    def test_single(self):
        self.assertEqual(parse_reason_atoms("第19条第2項第3号"),
                         ["第19条第2項第3号"])

    def test_multiple_with_space(self):
        self.assertEqual(
            parse_reason_atoms("第19条第2項第12号, 第19条第2項第19号"),
            ["第19条第2項第12号", "第19条第2項第19号"])

    def test_empty(self):
        self.assertEqual(parse_reason_atoms(None), [])
        self.assertEqual(parse_reason_atoms(""), [])


class ClassifyAtomTest(unittest.TestCase):
    def test_corporate_parent_change(self):
        found = classify_atom("第19条第2項第3号")
        self.assertEqual(found["event_kind"], "parent_or_subsidiary_change")
        self.assertEqual(found["ordinance"],
                         "企業内容等の開示に関する内閣府令")

    def test_corporate_sub_item(self):
        found = classify_atom("第19条第2項第9号の2")
        self.assertEqual(found["event_kind"],
                         "shareholders_meeting_resolution")

    def test_share_consolidation(self):
        found = classify_atom("第19条第2項第4号の4")
        self.assertEqual(found["event_kind"], "share_consolidation")

    def test_specified_securities(self):
        found = classify_atom("第29条第2項第4号")
        self.assertEqual(found["event_kind"], "fund_calculation_period_end")
        self.assertEqual(found["ordinance"],
                         "特定有価証券の内容等の開示に関する内閣府令")

    def test_unknown_article(self):
        # 第19条の2 は別条であり、対応表の対象外 (未分類に倒す)
        self.assertIsNone(classify_atom("第19条の2"))

    def test_unknown_item(self):
        self.assertIsNone(classify_atom("第19条第2項第99号"))

    def test_reserved_kinds_are_not_produced(self):
        produced = {kind for kind, _label in EVENT_KIND_BY_ITEM.values()}
        for reserved in RESERVED_EVENT_KINDS:
            self.assertNotIn(reserved, produced)

    def test_all_items_have_labels(self):
        for key, (kind, label) in EVENT_KIND_BY_ITEM.items():
            self.assertTrue(kind, key)
            self.assertTrue(label, key)


class BuildEventsTest(unittest.TestCase):
    def test_multi_reason_filing_yields_one_event_per_atom(self):
        rows = [filing("S1", "180", "2024-05-01 10:00",
                       reason="第19条第2項第3号,第19条第2項第8号の2")]
        events, stats = build_events(rows, CODE_TO_ID, SINCE,
                                     GENERATED_AT, SHA)
        self.assertEqual(len(events), 2)
        self.assertEqual([e["event_id"] for e in events], ["S1#1", "S1#2"])
        self.assertEqual(events[0]["event_kind"],
                         "parent_or_subsidiary_change")
        self.assertEqual(events[1]["event_kind"], "subsidiary_acquisition")
        self.assertEqual(events[0]["internal_id"], 1)
        self.assertEqual(events[0]["disclosure_date"], "2024-05-01")
        self.assertEqual(stats["extraordinary_filings"], 1)

    def test_filing_without_reason_is_unclassified(self):
        rows = [filing("S1", "180", "2024-05-01 10:00", reason=None)]
        events, stats = build_events(rows, CODE_TO_ID, SINCE,
                                     GENERATED_AT, SHA)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_kind"], "unclassified")
        self.assertIsNone(events[0]["reason_atom"])
        self.assertEqual(stats["filings_without_reason"], 1)

    def test_unmapped_atom_is_unclassified_and_counted(self):
        rows = [filing("S1", "180", "2024-05-01 10:00", reason="第19条の2")]
        events, stats = build_events(rows, CODE_TO_ID, SINCE,
                                     GENERATED_AT, SHA)
        self.assertEqual(events[0]["event_kind"], "unclassified")
        self.assertEqual(events[0]["reason_atom"], "第19条の2")
        self.assertEqual(dict(stats["unmapped_atoms"]), {"第19条の2": 1})

    def test_amendment_is_excluded_and_counted(self):
        rows = [filing("S1", "180", "2024-05-01 10:00",
                       reason="第19条第2項第3号"),
                filing("S2", "190", "2024-05-02 10:00",
                       reason="第19条第2項第3号")]
        events, stats = build_events(rows, CODE_TO_ID, SINCE,
                                     GENERATED_AT, SHA)
        self.assertEqual(len(events), 1)
        self.assertEqual(stats["amendment_filings_excluded"], 1)

    def test_before_window_is_excluded(self):
        rows = [filing("S1", "180", "2021-10-31 10:00",
                       reason="第19条第2項第3号"),
                filing("S2", "180", "2021-11-01 00:00",
                       reason="第19条第2項第3号")]
        events, _stats = build_events(rows, CODE_TO_ID, SINCE,
                                      GENERATED_AT, SHA)
        self.assertEqual([e["source_doc_id"] for e in events], ["S2"])

    def test_unresolved_filer_keeps_row_with_null_id(self):
        rows = [filing("S1", "180", "2024-05-01 10:00", code="E99999",
                       reason="第19条第2項第3号")]
        events, stats = build_events(rows, CODE_TO_ID, SINCE,
                                     GENERATED_AT, SHA)
        self.assertIsNone(events[0]["internal_id"])
        self.assertEqual(events[0]["edinet_code"], "E99999")
        self.assertEqual(stats["unresolved_filer_codes"], 1)

    def test_provenance_on_every_row(self):
        rows = [filing("S1", "180", "2024-05-01 10:00",
                       reason="第19条第2項第3号")]
        events, _stats = build_events(rows, CODE_TO_ID, SINCE,
                                      GENERATED_AT, SHA)
        e = events[0]
        self.assertEqual(e["source_kind"], "v1_index")
        self.assertEqual(e["source_id"],
                         "Yoshi-Dai/financial-lakehouse:"
                         "catalog/documents_index.parquet")
        self.assertEqual(e["source_sha256"], SHA)
        self.assertEqual(e["generated_at"], GENERATED_AT)
        self.assertEqual(e["redistribution"], "allowed")

    def test_body_derived_fields_start_null(self):
        rows = [filing("S1", "180", "2024-05-01 10:00",
                       reason="第19条第2項第4号の4")]
        events, _stats = build_events(rows, CODE_TO_ID, SINCE,
                                      GENERATED_AT, SHA)
        e = events[0]
        self.assertEqual(e["event_kind"], "share_consolidation")
        self.assertIsNone(e["split_ratio"])
        self.assertIsNone(e["effective_date"])
        self.assertIsNone(e["event_date"])
        self.assertIsNone(e["related_edinet_code"])


class BuildRelationshipsTest(unittest.TestCase):
    def test_aggregation_first_last_count(self):
        rows = [
            filing("S2", "200", "2025-04-01 10:00", subsidiary="E00002"),
            filing("S1", "200", "2024-04-01 10:00", subsidiary="E00002"),
            filing("S3", "200", "2026-04-01 10:00", subsidiary="E00002"),
        ]
        rels, stats = build_relationships(rows, CODE_TO_ID, SINCE,
                                          GENERATED_AT, SHA)
        self.assertEqual(len(rels), 1)
        r = rels[0]
        self.assertEqual(r["parent_internal_id"], 1)
        self.assertEqual(r["subsidiary_internal_id"], 2)
        self.assertEqual(r["first_observed_date"], "2024-04-01")
        self.assertEqual(r["last_observed_date"], "2026-04-01")
        self.assertEqual(r["observation_count"], 3)
        self.assertEqual(r["first_source_doc_id"], "S1")
        self.assertEqual(r["last_source_doc_id"], "S3")
        self.assertEqual(stats["relationship_pairs"], 1)

    def test_amendment_is_not_an_observation(self):
        rows = [
            filing("S1", "200", "2024-04-01 10:00", subsidiary="E00002"),
            filing("S2", "210", "2024-05-01 10:00", subsidiary="E00002"),
        ]
        rels, stats = build_relationships(rows, CODE_TO_ID, SINCE,
                                          GENERATED_AT, SHA)
        self.assertEqual(rels[0]["observation_count"], 1)
        self.assertEqual(stats["parent_report_amendments_excluded"], 1)

    def test_missing_subsidiary_code_is_skipped(self):
        rows = [filing("S1", "200", "2024-04-01 10:00", subsidiary=None)]
        rels, stats = build_relationships(rows, CODE_TO_ID, SINCE,
                                          GENERATED_AT, SHA)
        self.assertEqual(rels, [])
        self.assertEqual(stats["parent_report_filings"], 0)

    def test_self_pair_is_kept_and_counted(self):
        rows = [filing("S1", "200", "2024-04-01 10:00", code="E00001",
                       subsidiary="E00001")]
        rels, stats = build_relationships(rows, CODE_TO_ID, SINCE,
                                          GENERATED_AT, SHA)
        self.assertEqual(len(rels), 1)
        self.assertEqual(stats["self_pairs"], 1)

    def test_unresolved_subsidiary_keeps_row(self):
        rows = [filing("S1", "200", "2024-04-01 10:00",
                       subsidiary="E99999")]
        rels, stats = build_relationships(rows, CODE_TO_ID, SINCE,
                                          GENERATED_AT, SHA)
        self.assertIsNone(rels[0]["subsidiary_internal_id"])
        self.assertEqual(rels[0]["subsidiary_edinet_code"], "E99999")
        self.assertEqual(stats["unresolved_codes"], 1)


if __name__ == "__main__":
    unittest.main()
