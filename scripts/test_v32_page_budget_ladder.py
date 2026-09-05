"""Independent no-page ladder evidence contracts."""

import copy
import json
import unittest

from scripts import test_run_v32_no_page_containment as baseline_tests


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def fixture():
    queries, truths, logical_pages, registry = [], [], {}, {}
    for ordinal in range(1024, 1056):
        current = json.loads(
            baseline_tests.V32NoPageContainmentTests.diagnostic(
                ordinal,
                codes_scanned=230_000,
                leaves_eligible=4096,
                leaves_scanned=768,
                global_scope=True,
                scan_budget=262144,
            )
        )
        for index, rank in [(8, 20), (9, 33)]:
            current["diagnostics"][index].update(
                page_ordinal=2_000_000 + ordinal * 64 + rank,
                candidate_rank=rank,
                first_unique_page_rank=rank,
                page_selected=False,
                reciprocal_rank_selected=False,
                stage="page-reducer",
            )
        baseline_tests.V32NoPageContainmentTests.refresh_page_selections(current)
        prefix = copy.deepcopy(current["page_selections"]["first_distinct"]["pages"])
        prefix.extend(
            {
                "ordinal": 2_000_000 + ordinal * 64 + rank,
                "encoded_bytes": 181250,
                "sha256": f"{2_000_000 + ordinal * 64 + rank:064x}",
            }
            for rank in range(16, 64)
        )
        for page in prefix:
            page.update(primary_rows=1, replica_rows=0)
            registry[page["ordinal"]] = copy.deepcopy(page)
        truths.append(tuple(t["logical"] for t in current["diagnostics"]))
        logical_pages.update(
            {t["logical"]: t["page_ordinal"] for t in current["diagnostics"]}
        )
        cells = []
        for cap, hits in [(16, 8), (32, 9), (64, 10)]:
            cells.append(
                dict(
                    requested_pages=cap,
                    selected_page_count=cap,
                    selected_pages=copy.deepcopy(prefix[:cap]),
                    selected_page_bytes=cap * 181250,
                    contained_truth_count=hits,
                    containment_ppm=hits * 100000,
                )
            )
        queries.append(
            dict(
                query_ordinal=ordinal,
                candidate_replay_sha256=f"{ordinal:064x}",
                current=current,
                cells=cells,
            )
        )
    return (
        dict(
            schema_version=11,
            query_start=1024,
            claim_eligible=False,
            page_body_reads=0,
            queries=queries,
            resources=dict(peak_rss_bytes=1000000, phase_wall_ns=900, phase_cpu_ns=800),
        ),
        tuple(truths),
        logical_pages,
        registry,
    )


class PageBudgetLadderTests(unittest.TestCase):
    def validate(self, value, truths, mapping, registry):
        from scripts.run_v32_no_page_containment import validate_page_budget_ladder

        return validate_page_budget_ladder(
            canonical(value),
            query_start=1024,
            truth_logicals=truths,
            logical_pages=mapping,
            registered_pages=registry,
            maximum_leaves_eligible=4096,
            root_beam=8,
        )

    def test_ladder_recomputes_exact_containment_and_bytes(self):
        # Break: treating widened page coverage as measured recall, or trusting
        # producer totals instead of the independently registered identities.
        value, truths, mapping, registry = fixture()
        result = self.validate(value, truths, mapping, registry)
        self.assertEqual(result["contained_truth_counts"], [256, 288, 320])
        self.assertEqual(
            result["selected_page_bytes"], [92_800_000, 185_600_000, 371_200_000]
        )
        self.assertEqual(result["minimum_containment_ppm"], [800000, 900000, 1000000])
        self.assertIs(result["claim_eligible"], False)

    def test_ladder_rejects_cross_language_evidence_mutations(self):
        # Break: coherent byte/hash changes evade external registry binding, or
        # malformed prefixes/targets/cohorts silently alter the experiment.
        baseline, truths, mapping, registry = fixture()
        self.validate(baseline, truths, mapping, registry)
        for mutation in range(12):
            value = copy.deepcopy(baseline)
            q = value["queries"][0]
            if mutation == 0:
                value["query_start"] = 64
            elif mutation == 1:
                value["page_body_reads"] = True
            elif mutation == 2:
                q["candidate_replay_sha256"] = "bad"
            elif mutation == 3:
                q["cells"][0]["contained_truth_count"] = 10
            elif mutation == 4:
                q["cells"][1]["selected_page_bytes"] += 1
            elif mutation == 5:
                q["cells"][2]["selected_pages"][33]["sha256"] = "f" * 64
            elif mutation == 6:
                q["cells"][1]["selected_pages"][16] = q["cells"][1]["selected_pages"][0]
            elif mutation == 7:
                q["cells"][2]["selected_pages"][16:18] = list(
                    reversed(q["cells"][2]["selected_pages"][16:18])
                )
            elif mutation == 8:
                q["current"]["diagnostics"][0]["page_ordinal"] += 1
            elif mutation == 9:
                value["queries"].pop()
            elif mutation == 10:
                q["cells"][0]["requested_pages"] = True
            else:
                value["resources"]["peak_rss_bytes"] = 0
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.validate(value, truths, mapping, registry)

    def test_ladder_rejects_exhausted_prefix_with_later_known_target_rank(self):
        # Break: an alleged exhausted 32-page population retains evidence of a
        # 34th distinct page. Totals are coherent; rank authority must reject it.
        value, truths, mapping, registry = fixture()
        cell = value["queries"][0]["cells"][2]
        cell.update(
            selected_pages=cell["selected_pages"][:32],
            selected_page_count=32,
            selected_page_bytes=5_800_000,
            contained_truth_count=9,
            containment_ppm=900000,
        )
        with self.assertRaises(ValueError):
            self.validate(value, truths, mapping, registry)

    def test_ladder_accepts_exhausted_prefix_after_all_known_ranks(self):
        # Break: conflating the requested cap with an actual page count rejects
        # legitimate short candidate populations or overstates GETs and bytes.
        value, truths, mapping, registry = fixture()
        for query in value["queries"]:
            cell = query["cells"][2]
            cell.update(
                selected_pages=cell["selected_pages"][:40],
                selected_page_count=40,
                selected_page_bytes=7_250_000,
            )
        result = self.validate(value, truths, mapping, registry)
        self.assertEqual(result["contained_truth_counts"], [256, 288, 320])
        self.assertEqual(result["selected_page_bytes"][2], 232_000_000)


if __name__ == "__main__":
    unittest.main()
