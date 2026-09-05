"""Independent no-page ladder evidence contracts."""

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

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
    def test_expanded_frontier_recomputes_coverage_and_rejects_old_scope(self):
        from scripts.run_v32_no_page_containment import validate_expanded_frontier

        value, truths, mapping, registry = fixture()
        value["schema_version"] = 12
        for query in value["queries"]:
            query["current"]["routing"].update(
                global_leaf_limit=1536,
                scan_budget=524288,
                leaves_scanned=1536,
                codes_scanned=360482,
            )

        def validate(payload):
            return validate_expanded_frontier(
                canonical(payload),
                query_start=1024,
                truth_logicals=truths,
                logical_pages=mapping,
                registered_pages=registry,
                maximum_leaves_eligible=4096,
                root_beam=8,
            )

        result = validate(value)
        self.assertEqual(result["contained_truth_counts"], [256, 288, 320])
        with self.assertRaises(ValueError):
            self.validate(value, truths, mapping, registry)
        for field, wrong in [
            ("global_leaf_limit", 768),
            ("scan_budget", 262144),
            ("codes_scanned", 524289),
            ("leaves_scanned", 1537),
        ]:
            bad = copy.deepcopy(value)
            bad["queries"][0]["current"]["routing"][field] = wrong
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate(bad)
        bad = copy.deepcopy(value)
        bad["queries"][0]["cells"][2]["contained_truth_count"] = 9
        with self.assertRaises(ValueError):
            validate(bad)

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


class PageRegistryTests(unittest.TestCase):
    def test_registry_authenticates_parquet_and_maps_only_requested_logicals(self):
        # Break: trusting producer target ownership, allocating a corpus-sized
        # mapping, or accepting gaps/overlaps/schema changes after hash checks.
        from scripts.run_v32_no_page_containment import (
            LocalArtifact,
            read_page_ladder_registry,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schema = pa.schema(
                [
                    pa.field("page_ordinal", pa.uint32(), nullable=False),
                    pa.field("logical_start", pa.uint64(), nullable=False),
                    pa.field("row_count", pa.uint16(), nullable=False),
                    pa.field("sha256", pa.string(), nullable=False),
                    pa.field("encoded_bytes", pa.uint64(), nullable=False),
                    pa.field("primary_rows", pa.uint16(), nullable=False),
                    pa.field("replica_rows", pa.uint16(), nullable=False),
                ]
            )
            baseline = [
                dict(
                    page_ordinal=i,
                    logical_start=i * 2,
                    row_count=2,
                    sha256=f"{i + 1:064x}",
                    encoded_bytes=1000 + i,
                    primary_rows=2,
                    replica_rows=0,
                )
                for i in range(64)
            ]

            def write(rows, *, stale=False, filename="pages.parquet"):
                path = root / "pages.parquet"
                pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
                payload = path.read_bytes()
                descriptor = dict(
                    file=filename,
                    role="v32-page-ranges-parquet",
                    sha256=hashlib.sha256(payload).hexdigest(),
                    encoded_bytes=len(payload),
                )
                manifest = canonical(
                    dict(layout=dict(source_rows=128, page_ranges=descriptor))
                )
                manifest_path = root / "manifest.json"
                manifest_path.write_bytes(manifest)
                if stale:
                    path.write_bytes(payload + b"x")
                return LocalArtifact(
                    manifest_path, hashlib.sha256(manifest).hexdigest(), len(manifest)
                )

            authority = write(baseline)
            mapping, pages = read_page_ladder_registry(
                authority, root, 128, (0, 1, 64, 127)
            )
            self.assertEqual(mapping, {0: 0, 1: 0, 64: 32, 127: 63})
            self.assertEqual(len(pages), 64)
            self.assertEqual(
                pages[63],
                dict(
                    ordinal=63,
                    sha256=f"{64:064x}",
                    encoded_bytes=1063,
                    primary_rows=2,
                    replica_rows=0,
                ),
            )
            for mutation in range(8):
                rows = copy.deepcopy(baseline)
                if mutation == 0:
                    rows[1]["logical_start"] = 1
                elif mutation == 1:
                    rows[1]["row_count"] = 481
                elif mutation == 2:
                    rows[1]["primary_rows"] = 1
                elif mutation == 3:
                    rows[1]["page_ordinal"] = 0
                elif mutation == 4:
                    rows[1]["sha256"] = "bad"
                elif mutation == 5:
                    rows[1]["replica_rows"] = 1
                authority = write(
                    rows,
                    stale=mutation == 6,
                    filename="../pages.parquet" if mutation == 7 else "pages.parquet",
                )
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    read_page_ladder_registry(authority, root, 128, (0, 127))


class PageLadderRunnerTests(unittest.TestCase):
    def test_runner_authenticates_before_one_no_page_invocation(self):
        # Break: unbound query/source/range inputs launch science, or a cohort
        # accidentally uses 32 captures/legacy geometry/page-serving commands.
        from dataclasses import replace

        from scripts.run_v32_no_page_containment import (
            LocalArtifact,
            run_expanded_frontier_replay,
            run_page_budget_ladder,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan, truth = baseline_tests.V32NoPageContainmentTests().fixture(root)
            plan.artifact_dir.mkdir()
            ranges = [
                dict(
                    page_ordinal=i,
                    logical_start=i * 480,
                    row_count=min(480, 1_000_000 - i * 480),
                    sha256=f"{i + 1:064x}",
                    encoded_bytes=196000,
                    primary_rows=min(480, 1_000_000 - i * 480),
                    replica_rows=0,
                )
                for i in range(2084)
            ]
            types = [
                pa.uint32(),
                pa.uint64(),
                pa.uint16(),
                pa.string(),
                pa.uint64(),
                pa.uint16(),
                pa.uint16(),
            ]
            schema = pa.schema(
                [
                    pa.field(k, t, nullable=False)
                    for k, t in zip(ranges[0], types, strict=True)
                ]
            )
            page_path = plan.artifact_dir / "pages.parquet"
            pq.write_table(pa.Table.from_pylist(ranges, schema=schema), page_path)
            page_bytes = page_path.read_bytes()
            manifest = json.loads(plan.manifest.path.read_bytes())
            manifest["layout"]["page_ranges"] = dict(
                file="pages.parquet",
                role="v32-page-ranges-parquet",
                encoded_bytes=len(page_bytes),
                sha256=hashlib.sha256(page_bytes).hexdigest(),
            )
            manifest["diagnostics"] = dict(
                logical_sources=dict(
                    file=plan.logical_sources.path.name,
                    role="v32-logical-sources-arrow",
                    sha256=plan.logical_sources.sha256,
                    encoded_bytes=plan.logical_sources.encoded_bytes,
                )
            )
            raw = canonical(manifest)
            plan.manifest.path.write_bytes(raw)
            plan = replace(
                plan,
                manifest=LocalArtifact(
                    plan.manifest.path, hashlib.sha256(raw).hexdigest(), len(raw)
                ),
                leaf_beam=256,
                global_leaf_limit=768,
            )
            logical_rows = [[q * 100 + r + 1 for r in range(10)] for q in range(64, 96)]
            truth_type = pa.list_(pa.field("element", pa.uint64(), nullable=False), 10)
            batch_schema = pa.schema(
                [
                    pa.field("query_ordinal", pa.uint64(), nullable=False),
                    pa.field("truth_logicals", truth_type, nullable=False),
                ]
            )
            batch_path = root / "batch.arrow"
            with (
                pa.OSFile(str(batch_path), "wb") as sink,
                pa.ipc.new_file(sink, batch_schema) as writer,
            ):
                writer.write_table(
                    pa.Table.from_arrays(
                        [
                            pa.array(range(64, 96), type=pa.uint64()),
                            pa.array(logical_rows, type=truth_type),
                        ],
                        schema=batch_schema,
                    )
                )
            batch = batch_path.read_bytes()
            plan = replace(
                plan,
                diagnostic_batch=LocalArtifact(
                    batch_path, hashlib.sha256(batch).hexdigest(), len(batch)
                ),
            )
            queries = []
            for q in range(64, 96):
                current = json.loads(
                    baseline_tests.V32NoPageContainmentTests.diagnostic(
                        q,
                        global_scope=True,
                        leaves_eligible=4096,
                        leaves_scanned=768,
                        scan_budget=262144,
                        codes_scanned=230000,
                    )
                )
                page_start = (q * 100 + 1) // 480
                for target in current["diagnostics"]:
                    target.update(page_ordinal=page_start, first_unique_page_rank=0)
                pages = [
                    dict(
                        ordinal=r["page_ordinal"],
                        **{
                            k: r[k]
                            for k in (
                                "sha256",
                                "encoded_bytes",
                                "primary_rows",
                                "replica_rows",
                            )
                        },
                    )
                    for r in ranges[page_start : page_start + 64]
                ]
                selection = dict(
                    pages=[
                        {k: p[k] for k in ("ordinal", "sha256", "encoded_bytes")}
                        for p in pages[:16]
                    ],
                    selected_page_bytes=3_136_000,
                )
                current["page_selections"] = dict(
                    first_distinct=selection, reciprocal_rank=selection
                )
                current["routing"]["selected_page_bytes"] = 3_136_000
                cells = [
                    dict(
                        requested_pages=cap,
                        selected_page_count=cap,
                        selected_pages=pages[:cap],
                        selected_page_bytes=cap * 196000,
                        contained_truth_count=10,
                        containment_ppm=1000000,
                    )
                    for cap in (16, 32, 64)
                ]
                queries.append(
                    dict(
                        query_ordinal=q,
                        candidate_replay_sha256=f"{q:064x}",
                        current=current,
                        cells=cells,
                    )
                )
            payload = canonical(
                dict(
                    schema_version=11,
                    query_start=64,
                    claim_eligible=False,
                    page_body_reads=0,
                    queries=queries,
                    resources=dict(
                        peak_rss_bytes=1000000, phase_cpu_ns=900, phase_wall_ns=1000
                    ),
                )
            )
            calls = []

            def invoke(command):
                calls.append(command)
                return payload

            result = json.loads(run_page_budget_ladder(plan, invoke=invoke))
            self.assertEqual(
                result["summary"]["contained_truth_counts"], [320, 320, 320]
            )
            self.assertEqual(len(calls), 1)
            self.assertIn("--page-budget-ladder", calls[0])
            self.assertNotIn("--virtual-geometric-pages", calls[0])
            self.assertNotIn("--serving-tier", calls[0])
            self.assertEqual(result["manifest_sha256"], plan.manifest.sha256)
            expanded = json.loads(payload)
            expanded["schema_version"] = 12
            for query in expanded["queries"]:
                query["current"]["routing"].update(
                    global_leaf_limit=1536,
                    scan_budget=524288,
                    leaves_scanned=1536,
                    codes_scanned=360482,
                )
            payload = canonical(expanded)
            calls.clear()
            result = json.loads(run_expanded_frontier_replay(plan, invoke=invoke))
            self.assertEqual(result["schema"], "borsuk-v32-expanded-frontier-v1")
            self.assertEqual(
                result["summary"]["contained_truth_counts"], [320, 320, 320]
            )
            self.assertEqual(len(calls), 1)
            self.assertIn("--expanded-frontier-replay", calls[0])
            self.assertNotIn("--page-budget-ladder", calls[0])
            self.assertEqual(
                calls[0][calls[0].index("--global-leaf-limit") + 1], "1536"
            )
            plan.query.path.write_bytes(b"wrong")
            calls.clear()
            with self.assertRaises(ValueError):
                run_page_budget_ladder(plan, invoke=invoke)
            self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
