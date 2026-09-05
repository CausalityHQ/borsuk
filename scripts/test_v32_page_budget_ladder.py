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
    @staticmethod
    def root64_payloads(*, tied=False):
        import numpy as np

        def arrow(table):
            stream = pa.BufferOutputStream()
            with pa.ipc.new_file(stream, table.schema) as writer:
                writer.write_table(table)
            return stream.getvalue().to_pybytes()

        half_type = pa.list_(pa.field("element", pa.float16(), nullable=False), 96)

        def centers(n):
            return pa.FixedSizeListArray.from_arrays(
                pa.array(np.full(n * 96, 0.1, dtype=np.float16)), type=half_type
            )

        root_values = np.zeros((128, 96), dtype=np.float16)
        root_values[np.arange(128), 0 if tied else np.arange(128) % 2] = 1
        root_vectors = pa.FixedSizeListArray.from_arrays(
            pa.array(root_values.reshape(-1)), type=half_type
        )
        roots = pa.Table.from_arrays(
            [root_vectors],
            schema=pa.schema([pa.field("centroid", half_type, nullable=False)]),
        )
        owners = np.arange(4096, dtype=np.uint16) // 32
        leaves = pa.Table.from_arrays(
            [pa.array(owners), centers(4096)],
            schema=pa.schema(
                [
                    pa.field("root_ordinal", pa.uint16(), nullable=False),
                    pa.field("centroid", half_type, nullable=False),
                ]
            ),
        )
        counts = np.array([245] * 576 + [244] * 3520, dtype=np.uint64)
        route_schema = pa.schema(
            [
                pa.field("routing_leaf_ordinal", pa.uint32(), nullable=False),
                pa.field("code_parent_leaf_ordinal", pa.uint32(), nullable=False),
                pa.field("routing_centroid", half_type, nullable=False),
                pa.field("logical_start", pa.uint64(), nullable=False),
                pa.field("row_count", pa.uint64(), nullable=False),
                pa.field("page_start", pa.uint32(), nullable=False),
                pa.field("page_count", pa.uint32(), nullable=False),
            ]
        )
        routes = pa.Table.from_arrays(
            [
                pa.array(np.arange(4096, dtype=np.uint32)),
                pa.array(np.arange(4096, dtype=np.uint32)),
                centers(4096),
                pa.array(np.cumsum(counts) - counts),
                pa.array(counts),
                pa.array(np.arange(4096, dtype=np.uint32)),
                pa.array(np.ones(4096, dtype=np.uint32)),
            ],
            schema=route_schema,
        )
        query_type = pa.list_(pa.field("element", pa.float32(), nullable=False), 96)
        query_values = np.zeros((10000, 96), dtype=np.float32)
        query_values[:, 1] = 1
        query_values[1024:1040, 1] = 0
        query_values[1024:1040, 0] = 1
        vectors = pa.FixedSizeListArray.from_arrays(
            pa.array(query_values.reshape(-1)), type=query_type
        )
        query = pa.Table.from_arrays(
            [vectors], schema=pa.schema([pa.field("emb", query_type, nullable=False)])
        )
        stream = pa.BufferOutputStream()
        pq.write_table(query, stream)
        args = (
            arrow(roots),
            arrow(leaves),
            arrow(routes),
            stream.getvalue().to_pybytes(),
        )
        return args, owners, leaves, routes, arrow

    def test_root64_metadata_reads_exact_arrow_query_shape_and_orders_roots(self):
        # Break: wrong code-parent ownership, centroid arithmetic, or query schema.
        from scripts.run_v32_no_page_containment import root64_metadata_from_bytes

        args, owners, leaves, routes, arrow = self.root64_payloads()
        route_schema = routes.schema
        result = root64_metadata_from_bytes(*args, query_start=1024)
        even_first = tuple(range(0, 128, 2)) + tuple(range(1, 128, 2))
        odd_first = tuple(range(1, 128, 2)) + tuple(range(0, 128, 2))
        self.assertEqual(result.root_orders, (even_first,) * 16 + (odd_first,) * 16)
        self.assertEqual(result.leaf_owners, tuple(int(n) for n in owners))
        self.assertEqual(result.leaf_rows, (245,) * 576 + (244,) * 3520)
        bad_parent = routes.set_column(
            1, route_schema.field(1), pa.array([4096] * 4096, type=pa.uint32())
        )
        with self.assertRaises(ValueError):
            root64_metadata_from_bytes(
                args[0], args[1], arrow(bad_parent), args[3], query_start=1024
            )
        bad_owner = leaves.set_column(
            0, leaves.schema.field(0), pa.array([128] * 4096, type=pa.uint16())
        )
        with self.assertRaises(ValueError):
            root64_metadata_from_bytes(
                args[0], arrow(bad_owner), args[2], args[3], query_start=1024
            )
        with self.assertRaises(ValueError):
            root64_metadata_from_bytes(*args, query_start=9970)

    def test_root64_recomputes_scope_and_rejects_coherent_output_drift(self):
        # Break: trust producer roots/counts instead of independently derived metadata.
        from scripts.run_v32_no_page_containment import (
            Root64Metadata,
            validate_root64_frontier,
        )

        value, truths, mapping, registry = fixture()
        value["schema_version"] = 13
        metadata = Root64Metadata(
            root_orders=tuple(tuple(range(128)) for _ in range(32)),
            leaf_owners=tuple(i // 32 for i in range(4096)),
            leaf_rows=(245,) * 576 + (244,) * 3520,
        )
        for query in value["queries"]:
            query["selected_root_ordinals"] = list(range(64))
            query["current"]["routing"].update(
                global_leaf_limit=None,
                scan_budget=524288,
                leaves_eligible=2048,
                leaves_scanned=2048,
                codes_scanned=500288,
                scope="root-gated",
                stop_reason="root-gated",
            )
            for target in query["current"]["diagnostics"]:
                # All fixture logicals are below141120: first576 leaves have245 rows.
                leaf = target["logical"] // 245
                target.update(
                    leaf_ordinal=leaf,
                    routing_leaf_rank=leaf + 1,
                    owner_root_ordinal=leaf // 32,
                    owner_root_rank=leaf // 32 + 1,
                )

        def validate(v):
            return validate_root64_frontier(
                canonical(v),
                query_start=1024,
                truth_logicals=truths,
                logical_pages=mapping,
                registered_pages=registry,
                metadata=metadata,
            )

        self.assertEqual(validate(value)["contained_truth_counts"], [256, 288, 320])
        for mutation in range(8):
            bad = copy.deepcopy(value)
            q = bad["queries"][0]
            if mutation == 0:
                q["selected_root_ordinals"][0], q["selected_root_ordinals"][1] = 1, 0
            elif mutation == 1:
                q["current"]["routing"]["codes_scanned"] += 1
            elif mutation == 2:
                q["current"]["routing"]["leaves_scanned"] -= 1
                q["current"]["routing"]["leaves_eligible"] -= 1
            elif mutation == 3:
                q["current"]["diagnostics"][0]["owner_root_rank"] += 1
            elif mutation == 4:
                q["current"]["diagnostics"][0]["leaf_ordinal"] += 1
            elif mutation == 5:
                q["selected_root_ordinals"][0] = False
            elif mutation == 6:
                q["current"]["routing"]["global_leaf_limit"] = 1536
            else:
                q["cells"][2]["contained_truth_count"] = 9
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate(bad)

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
            run_root64_replay,
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
            # Real metadata and query parsing; update only their dependent hashes.
            inputs, *_ = PageBudgetLadderTests.root64_payloads(tied=True)
            manifest_value = json.loads(plan.manifest.path.read_bytes())
            hierarchy = {}
            for name, role, raw in zip(
                ("roots.arrow", "leaves.arrow", "routing-ranges.arrow"),
                ("v27-roots-arrow", "v27-leaves-arrow", "v32-routing-ranges-arrow"),
                inputs[:3],
                strict=True,
            ):
                (plan.artifact_dir / name).write_bytes(raw)
                desc = dict(
                    file=name,
                    role=role,
                    sha256=hashlib.sha256(raw).hexdigest(),
                    encoded_bytes=len(raw),
                )
                if name == "routing-ranges.arrow":
                    manifest_value["layout"]["routing_ranges"] = desc
                else:
                    hierarchy[name.split(".")[0]] = desc
            manifest_value["hierarchy"] = hierarchy
            raw_manifest = canonical(manifest_value)
            plan.manifest.path.write_bytes(raw_manifest)
            plan.query.path.write_bytes(inputs[3])
            receipt = json.loads(plan.truth_receipt.path.read_bytes())
            receipt.update(
                query_sha256=hashlib.sha256(inputs[3]).hexdigest(),
                query_bytes=len(inputs[3]),
            )
            raw_receipt = canonical(receipt)
            plan.truth_receipt.path.write_bytes(raw_receipt)
            plan = replace(
                plan,
                manifest=LocalArtifact(
                    plan.manifest.path,
                    hashlib.sha256(raw_manifest).hexdigest(),
                    len(raw_manifest),
                ),
                query=LocalArtifact(
                    plan.query.path,
                    hashlib.sha256(inputs[3]).hexdigest(),
                    len(inputs[3]),
                ),
                truth_receipt=LocalArtifact(
                    plan.truth_receipt.path,
                    hashlib.sha256(raw_receipt).hexdigest(),
                    len(raw_receipt),
                ),
            )
            root_result = json.loads(payload)
            root_result["schema_version"] = 13
            for query in root_result["queries"]:
                query["selected_root_ordinals"] = list(range(64))
                query["current"]["routing"].update(
                    global_leaf_limit=None,
                    scope="root-gated",
                    stop_reason="root-gated",
                    leaves_eligible=2048,
                    leaves_scanned=2048,
                    codes_scanned=500288,
                )
                for target in query["current"]["diagnostics"]:
                    leaf = target["logical"] // 245
                    target.update(
                        leaf_ordinal=leaf,
                        routing_leaf_rank=leaf + 1,
                        owner_root_ordinal=leaf // 32,
                        owner_root_rank=leaf // 32 + 1,
                    )
            payload = canonical(root_result)
            calls.clear()
            result = json.loads(run_root64_replay(plan, invoke=invoke))
            self.assertEqual(result["schema"], "borsuk-v32-root64-replay-v1")
            self.assertEqual(
                result["summary"]["contained_truth_counts"], [320, 320, 320]
            )
            self.assertEqual(len(calls), 1)
            self.assertIn("--root64-replay", calls[0])
            self.assertNotIn("--global-leaf-limit", calls[0])
            self.assertNotIn("--serving-tier", calls[0])
            root_path = plan.artifact_dir / "roots.arrow"
            original_root = root_path.read_bytes()
            root_path.write_bytes(b"x" + original_root[1:])
            calls.clear()
            with self.assertRaises(ValueError):
                run_root64_replay(plan, invoke=invoke)
            self.assertEqual(calls, [])
            root_path.write_bytes(original_root)
            plan.query.path.write_bytes(b"wrong")
            calls.clear()
            with self.assertRaises(ValueError):
                run_page_budget_ladder(plan, invoke=invoke)
            self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
