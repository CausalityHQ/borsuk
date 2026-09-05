import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.run_v32_no_page_containment import (
    LocalArtifact,
    V32ContainmentPlan,
    build_v32_containment_commands,
    containment_exit_status,
    run_v32_no_page_containment,
)


class V32NoPageContainmentTests(unittest.TestCase):
    def test_v32_containment_exit_status_fails_closed_on_scientific_rejection(self) -> None:
        self.assertEqual(
            containment_exit_status(b'{"failed_gates":[],"status":"passed"}\n'),
            0,
        )
        self.assertEqual(
            containment_exit_status(
                b'{"failed_gates":["perfect-containment"],"status":"failed"}\n'
            ),
            2,
        )

    def test_v32_containment_direct_script_exposes_only_resident_diagnostics(self) -> None:
        # Break caught: the Spot boundary is not directly executable or grows a
        # page-source flag that permits scientific page reads in the fast gate.
        completed = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("run_v32_no_page_containment.py")),
                "--help",
            ],
            check=False,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr.decode())
        self.assertIn(b"--truth-parquet", completed.stdout)
        self.assertIn(b"--artifact-dir", completed.stdout)
        self.assertIn(b"--diagnostic-batch-arrow", completed.stdout)
        self.assertNotIn(b"--serving-tier", completed.stdout)
        self.assertNotIn(b"--page", completed.stdout)

    def fixture(
        self, directory: Path, *, source_rows: int = 1_000_000
    ) -> tuple[V32ContainmentPlan, bytes]:
        neighbors = pa.array(
            [
                [((64 + row) * 100 + rank) for rank in range(10)]
                for row in range(32)
            ],
            type=pa.list_(pa.field("item", pa.int64(), nullable=False), 10),
        )
        truth_path = directory / "neighbors.parquet"
        pq.write_table(
            pa.Table.from_arrays(
                [neighbors],
                schema=pa.schema(
                    [pa.field("neighbors_id", neighbors.type, nullable=False)]
                ),
            ),
            truth_path,
        )
        truth = truth_path.read_bytes()
        manifest = directory / "manifest.json"
        manifest_bytes = (
            json.dumps(
                {
                    "layout": {
                        "maximum_code_parent_rows": 32_000,
                        "maximum_routing_leaf_rows": 1_024,
                        "maximum_routing_leaves_per_root": 64,
                        "page_rows": 480,
                        "projected_resident_bytes": 30_000_000,
                        "source_rows": source_rows,
                    },
                    "routing": {
                        "algorithm": "hierarchical-routing-microleaf-pq-v1",
                        "arms": [
                            {"leaf_beam": 64, "maximum_scanned_codes": 65_536},
                            {"leaf_beam": 128, "maximum_scanned_codes": 131_072},
                            {"leaf_beam": 256, "maximum_scanned_codes": 262_144},
                        ],
                        "candidate_depth": 12_288,
                        "page_count": 16,
                        "root_beam": 8,
                    },
                    "source": {
                        "corpus_manifest_bytes": 1_938,
                        "corpus_manifest_sha256": "c" * 64,
                    },
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
            + b"\n"
        )
        manifest.write_bytes(manifest_bytes)
        truth_rows = pq.read_table(pa.BufferReader(truth))["neighbors_id"].to_pylist()
        truth_ids = b"".join(
            logical.to_bytes(8, "little", signed=True)
            for row in truth_rows
            for logical in row
        )
        truth_receipt_path = directory / "truth-receipt.json"
        truth_receipt = (
            json.dumps(
                {
                    "claim_eligible": False,
                    "corpus_manifest_bytes": 1_938,
                    "corpus_manifest_sha256": "c" * 64,
                    "corpus_normalization": "f64-l2-once-to-f32",
                    "corpus_shards": [{"role": "fixture"}],
                    "distance": "squared-l2-f64-fixed-dimension-order",
                    "query_bytes": 5,
                    "query_count": 32,
                    "query_normalization": "f64-l2-twice-to-f32",
                    "query_sha256": hashlib.sha256(b"query").hexdigest(),
                    "query_start": 64,
                    "rank_10_11_tie_queries": 0,
                    "schema": "borsuk-v32-prefix-truth-v2",
                    "shards_read": 1,
                    "source_rows": source_rows,
                    "status": "passed",
                    "tie_break": "source-ordinal-ascending",
                    "top_k": 10,
                    "truth_bytes": len(truth),
                    "truth_id_space": "source-ordinal",
                    "truth_ids_sha256": hashlib.sha256(truth_ids).hexdigest(),
                    "truth_row_semantics": "window-relative",
                    "truth_sha256": hashlib.sha256(truth).hexdigest(),
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
            + b"\n"
        )
        truth_receipt_path.write_bytes(truth_receipt)
        query = directory / "test.parquet"
        query.write_bytes(b"query")
        logical_sources_path = directory / "logical-sources.arrow"
        logical_sources = pa.array(
            [(logical - 1) % source_rows for logical in range(source_rows)],
            type=pa.uint64(),
        )
        logical_sources_table = pa.Table.from_arrays(
            [logical_sources],
            schema=pa.schema(
                [pa.field("source_ordinal", pa.uint64(), nullable=False)]
            ),
        )
        with pa.OSFile(str(logical_sources_path), "wb") as sink:
            with pa.ipc.new_file(sink, logical_sources_table.schema) as writer:
                writer.write_table(logical_sources_table)
        logical_sources_bytes = logical_sources_path.read_bytes()
        return (
            V32ContainmentPlan(
                qualifier=Path("/opt/borsuk/v30_s3_qualify"),
                manifest=LocalArtifact(
                    manifest,
                    hashlib.sha256(manifest_bytes).hexdigest(),
                    len(manifest_bytes),
                ),
                artifact_dir=directory / "resident",
                query=LocalArtifact(query, hashlib.sha256(b"query").hexdigest(), 5),
                logical_sources=LocalArtifact(
                    logical_sources_path,
                    hashlib.sha256(logical_sources_bytes).hexdigest(),
                    len(logical_sources_bytes),
                ),
                truth=LocalArtifact(
                    truth_path, hashlib.sha256(truth).hexdigest(), len(truth)
                ),
                truth_receipt=LocalArtifact(
                    truth_receipt_path,
                    hashlib.sha256(truth_receipt).hexdigest(),
                    len(truth_receipt),
                ),
                source_rows=source_rows,
                query_start=64,
                query_count=32,
                root_beam=8,
                leaf_beam=64,
            ),
            truth,
        )

    @staticmethod
    def refresh_page_selections(value: dict[str, object]) -> None:
        diagnostics = value["diagnostics"]
        assert isinstance(diagnostics, list)

        def selection(flag: str) -> dict[str, object]:
            ordinals = [
                item["page_ordinal"]
                for item in diagnostics
                if (
                    item["stage"] == "selected-page"
                    if flag == "first_distinct"
                    else item["reciprocal_rank_selected"]
                )
            ]
            filler = int(value["query_ordinal"]) * 1_000
            while len(ordinals) < 16:
                if filler not in ordinals:
                    ordinals.append(filler)
                filler += 1
            return {
                "pages": [
                    {
                        "encoded_bytes": 181_250,
                        "ordinal": ordinal,
                        "sha256": f"{ordinal:064x}",
                    }
                    for ordinal in ordinals
                ],
                "selected_page_bytes": 2_900_000,
            }

        value["page_selections"] = {
            "first_distinct": selection("first_distinct"),
            "reciprocal_rank": selection("reciprocal_rank"),
        }

    @staticmethod
    def diagnostic(
        query_ordinal: int,
        *,
        miss: bool = False,
        candidates_retained: int = 12_288,
        codes_scanned: int | None = None,
        leaves_eligible: int = 128,
        leaves_scanned: int = 128,
        global_scope: bool = False,
        stop_reason: str | None = None,
        next_leaf_rows: int | None = None,
        scan_budget: int = 65_536,
    ) -> bytes:
        diagnostics = []
        for rank in range(10):
            selected = not (miss and rank == 9)
            diagnostics.append(
                {
                    "candidate_rank": rank if selected else None,
                    "first_unique_page_rank": rank if selected else None,
                    "global_routing_leaf_rank": rank + 1,
                    "leaf_ordinal": query_ordinal,
                    "logical": (query_ordinal * 100 + rank + 1) % 1_000_000,
                    "owner_root_ordinal": query_ordinal % 128,
                    "owner_root_rank": query_ordinal % 8 + 1,
                    "page_in_retained_pool": selected,
                    "page_in_scanned_pool": True,
                    "page_ordinal": query_ordinal * 10 + rank,
                    "page_selected": selected,
                    "reciprocal_rank_selected": selected,
                    "routing_leaf_rank": (
                        rank + 1
                        if global_scope
                        else query_ordinal % leaves_scanned + 1
                    ),
                    "stage": "selected-page" if selected else "candidate-retention",
                }
            )
        value = {
            "claim_eligible": False,
            "diagnostics": diagnostics,
            "page_body_reads": 0,
            "query_ordinal": query_ordinal,
            "routing": {
                "candidates_retained": candidates_retained,
                "codes_scanned": (
                    40_000 + query_ordinal
                    if codes_scanned is None
                    else codes_scanned
                ),
                "global_leaf_limit": 768 if global_scope else None,
                "leaves_eligible": leaves_eligible,
                "leaves_scanned": leaves_scanned,
                "next_leaf_rows": next_leaf_rows,
                "pages_considered": 20,
                "peak_query_table_pairs_live": 1,
                "query_table_pairs_built": 1,
                "roots_scored": 128,
                "scan_budget": scan_budget,
                "scope": "global" if global_scope else "root-gated",
                "selected_page_bytes": 2_900_000,
                "selected_pages": 16,
                "stop_reason": stop_reason
                or ("leaf-limit" if global_scope else "root-gated"),
                "total_routing_leaves": 4_096,
            },
            "schema_version": 5,
            "truth_independent_selection": True,
        }
        V32NoPageContainmentTests.refresh_page_selections(value)
        return (
            json.dumps(
                value,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
            + b"\n"
        )

    def test_v32_containment_uses_ten_truth_rows_per_query_and_no_page_source(self) -> None:
        # Break caught: scale diagnosis reloads the resident index once per truth
        # row or accidentally issues page GETs before containment is known.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary))
            commands = build_v32_containment_commands(plan, truth)
        self.assertEqual(len(commands), 32)
        for offset, command in enumerate(commands):
            self.assertNotIn("--serving-tier", command)
            self.assertNotIn("--local-page-dir", command)
            self.assertNotIn("--s3-page-prefix", command)
            self.assertEqual(command[command.index("--query-count") + 1], "1")
            logicals = command[command.index("--diagnose-logicals") + 1]
            self.assertEqual(
                logicals,
                ",".join(
                    str(((64 + offset) * 100 + rank + 1) % 1_000_000)
                    for rank in range(10)
                ),
            )

    def test_v32_containment_commands_use_the_manifest_routing_ladder(self) -> None:
        # Break caught: the reducer accepts a wider scale rung but commands keep
        # sending the 1M-only leaf beam, so the qualifier rejects every query.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary))
            plan = replace(plan, leaf_beam=128)
            commands = build_v32_containment_commands(plan, truth)
            with self.assertRaisesRegex(ValueError, "scale geometry"):
                build_v32_containment_commands(replace(plan, leaf_beam=192), truth)
            wider_roots = build_v32_containment_commands(
                replace(plan, root_beam=16), truth
            )
        self.assertEqual(
            {command[command.index("--leaf-beam") + 1] for command in commands},
            {"128"},
        )
        self.assertEqual(
            {command[command.index("--root-beam") + 1] for command in wider_roots},
            {"16"},
        )

    def test_v32_containment_global_prefix_is_exact_and_truth_independent(self) -> None:
        # Break caught: the no-page falsifier claims root-independent coverage
        # while invoking the old root gate or silently widening after a miss.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary))
            plan = replace(plan, leaf_beam=256, global_leaf_limit=768)
            commands = build_v32_containment_commands(plan, truth)
            results = {
                query: self.diagnostic(
                    query,
                    codes_scanned=220_000,
                    leaves_eligible=4_096,
                    leaves_scanned=768,
                    global_scope=True,
                    scan_budget=262_144,
                )
                for query in range(64, 96)
            }
            payload = run_v32_no_page_containment(
                plan,
                truth,
                invoke=lambda command: results[
                    int(command[command.index("--query-start") + 1])
                ],
            )
        self.assertTrue(all("--global-leaf-limit" in command for command in commands))
        self.assertEqual(
            {command[command.index("--global-leaf-limit") + 1] for command in commands},
            {"768"},
        )
        value = json.loads(payload)
        self.assertEqual(value["routing_scope"], "global")
        self.assertEqual(value["global_leaf_limit"], 768)
        self.assertEqual(value["maximum_leaves_eligible"], 4_096)
        self.assertEqual(value["maximum_leaves_scanned"], 768)
        self.assertEqual(value["maximum_codes_scanned"], 220_000)
        self.assertEqual(value["selected_page_hits"], 320)


class V32VirtualGeometricPackingTests(V32NoPageContainmentTests):
    @staticmethod
    def virtual_diagnostic(
        query_ordinal: int, *, current_misses: int, reciprocal_misses: int
    ) -> bytes:
        value = json.loads(
            V32NoPageContainmentTests.diagnostic(
                query_ordinal,
                codes_scanned=230_000,
                leaves_eligible=4_096,
                leaves_scanned=768,
                global_scope=True,
                scan_budget=262_144,
            )
        )
        if current_misses:
            for target in value["diagnostics"][-current_misses:]:
                target["candidate_rank"] = 20 + target["logical"] % 10
                target["first_unique_page_rank"] = 20 + target["logical"] % 10
                target["page_in_retained_pool"] = True
                target["page_selected"] = False
                target["stage"] = "page-reducer"
        if reciprocal_misses:
            for target in value["diagnostics"][-reciprocal_misses:]:
                target["reciprocal_rank_selected"] = False
        V32NoPageContainmentTests.refresh_page_selections(value)
        value["schema_version"] = 6
        value["virtual_geometric"] = {
            "candidate_replay_sha256": f"{query_ordinal:064x}",
            "newly_lost_logicals": [],
            "page_body_reads": 0,
            "page_rows": 480,
            "projected_selected_bytes": 3_145_728,
            "projected_selected_bytes_at_eight": 1_572_864,
            "recovered_logicals": [
                target["logical"]
                for target in value["diagnostics"]
                if not target["page_selected"]
            ],
            "selected_pages": list(range(100, 116)),
            "selected_pages_at_eight": list(range(100, 108)),
            "targets": [
                {
                    "logical": item["logical"],
                    "page_ordinal": 100 + rank % 8,
                    "selected": True,
                    "selected_at_eight": True,
                }
                for rank, item in enumerate(value["diagnostics"])
            ],
            "truth_microleaf_count": 1,
            "truth_virtual_page_count": 8,
            "virtual_layout_sha256": "b" * 64,
        }
        return (
            json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
            + b"\n"
        )

    def test_v32_virtual_geometric_replay_reproduces_control_and_requires_perfect_treatment(
        self,
    ) -> None:
        # Break caught: the virtual-layout treatment runs without reproducing
        # the frozen 308/320 control or advances with any treatment miss.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary))
            source_to_logical = {
                source: logical
                for logical, source in enumerate(
                    pa.ipc.open_file(plan.logical_sources.path)
                    .read_all()
                    .column("source_ordinal")
                    .to_pylist()
                )
            }
            truth_rows = pq.read_table(pa.BufferReader(truth)).column(
                "neighbors_id"
            ).to_pylist()
            diagnostic_batch_path = Path(temporary) / "diagnostic-batch.arrow"
            truth_logicals = pa.array(
                [
                    [source_to_logical[source] for source in row]
                    for row in truth_rows
                ],
                type=pa.list_(pa.field("element", pa.uint64(), nullable=False), 10),
            )
            diagnostic_batch = pa.Table.from_arrays(
                [pa.array(range(64, 96), type=pa.uint64()), truth_logicals],
                schema=pa.schema(
                    [
                        pa.field("query_ordinal", pa.uint64(), nullable=False),
                        pa.field(
                            "truth_logicals", truth_logicals.type, nullable=False
                        ),
                    ]
                ),
            )
            with pa.OSFile(str(diagnostic_batch_path), "wb") as sink:
                with pa.ipc.new_file(sink, diagnostic_batch.schema) as writer:
                    writer.write_table(diagnostic_batch)
            diagnostic_batch_bytes = diagnostic_batch_path.read_bytes()
            plan = replace(
                plan,
                leaf_beam=256,
                global_leaf_limit=768,
                virtual_geometric_pages=True,
                diagnostic_batch=LocalArtifact(
                    diagnostic_batch_path,
                    hashlib.sha256(diagnostic_batch_bytes).hexdigest(),
                    len(diagnostic_batch_bytes),
                ),
            )
            commands = build_v32_containment_commands(plan, truth)
            results = {
                query: self.virtual_diagnostic(
                    query,
                    current_misses=(
                        3
                        if query == 64
                        else 2
                        if query == 65
                        else 1
                        if query < 73
                        else 0
                    ),
                    reciprocal_misses=(
                        3
                        if query == 64
                        else 2
                        if query < 72
                        else 1
                        if query < 77
                        else 0
                    ),
                )
                for query in range(64, 96)
            }
            batch_payload = (
                json.dumps(
                    {
                        "claim_eligible": False,
                        "page_body_reads": 0,
                        "queries": [json.loads(results[query]) for query in range(64, 96)],
                        "schema_version": 7,
                    },
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                + b"\n"
            )
            payload = run_v32_no_page_containment(
                plan,
                truth,
                invoke=lambda _command: batch_payload,
            )
        self.assertEqual(len(commands), 1)
        self.assertIn("--diagnostic-batch-arrow", commands[0])
        self.assertTrue(all("--virtual-geometric-pages" in command for command in commands))
        value = json.loads(payload)
        self.assertEqual(value["control"]["selected_page_hits"], 308)
        self.assertEqual(
            value["control"]["reciprocal_rank"]["selected_page_hits"], 298
        )
        self.assertEqual(value["virtual_geometric"]["selected_page_hits"], 320)
        self.assertEqual(value["virtual_geometric"]["minimum_containment_ppm"], 1_000_000)
        self.assertEqual(value["virtual_geometric"]["perfect_queries"], 32)
        self.assertEqual(value["virtual_geometric"]["failed_gates"], [])
        self.assertEqual(value["virtual_geometric"]["status"], "passed")
        self.assertEqual(len(value["virtual_geometric"]["queries"]), 32)
        self.assertEqual(
            value["virtual_geometric"]["queries"][0]["query_ordinal"], 64
        )
        self.assertEqual(
            value["virtual_geometric"]["queries"][0]["candidate_replay_sha256"],
            f"{64:064x}",
        )
        self.assertEqual(
            value["virtual_geometric"]["queries"][0]["virtual_layout_sha256"],
            "b" * 64,
        )
        self.assertEqual(value["failed_gates"], [])
        self.assertEqual(value["status"], "passed")
        self.assertEqual(containment_exit_status(payload), 0)

    def test_v32_containment_global_prefix_proof_and_rank_domains_are_strict(self) -> None:
        # Break caught: rooted eligible counts reject a valid global rank, or a
        # global run claims a short prefix without a leaf-limit/code-cap proof.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary))
            with self.assertRaisesRegex(ValueError, "root beam"):
                build_v32_containment_commands(
                    replace(plan, global_leaf_limit=768), truth
                )

            rooted = {
                query: self.diagnostic(query) for query in range(64, 96)
            }
            changed = json.loads(rooted[64])
            changed["diagnostics"][0]["global_routing_leaf_rank"] = 3_000
            rooted[64] = (
                json.dumps(changed, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            run_v32_no_page_containment(
                plan,
                truth,
                invoke=lambda command: rooted[
                    int(command[command.index("--query-start") + 1])
                ],
            )

            global_plan = replace(plan, leaf_beam=256, global_leaf_limit=768)
            valid_global = {
                query: self.diagnostic(
                    query,
                    codes_scanned=220_000,
                    leaves_eligible=4_096,
                    leaves_scanned=768,
                    global_scope=True,
                    scan_budget=262_144,
                )
                for query in range(64, 96)
            }
            for field, value in [
                ("routing_leaf_rank", 2),
                ("page_in_scanned_pool", False),
            ]:
                invalid = dict(valid_global)
                changed = json.loads(invalid[64])
                changed["diagnostics"][0][field] = value
                invalid[64] = (
                    json.dumps(changed, separators=(",", ":"), sort_keys=True).encode()
                    + b"\n"
                )
                with self.assertRaisesRegex(ValueError, "diagnostic value"):
                    run_v32_no_page_containment(
                        global_plan,
                        truth,
                        invoke=lambda command, values=invalid: values[
                            int(command[command.index("--query-start") + 1])
                        ],
                    )

            invalid_stop = dict(valid_global)
            changed = json.loads(invalid_stop[64])
            changed["routing"]["leaves_scanned"] = 700
            invalid_stop[64] = (
                json.dumps(changed, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "routing stop"):
                run_v32_no_page_containment(
                    global_plan,
                    truth,
                    invoke=lambda command: invalid_stop[
                        int(command[command.index("--query-start") + 1])
                    ],
                )

    def test_v32_containment_global_frontier_rank_matches_scanned_prefix(self) -> None:
        # Break caught: global diagnostics rank every routing leaf, but the
        # reducer rejects a legitimate truth page whose ranked leaf lies just
        # beyond the complete scanned prefix.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary))
            plan = replace(plan, leaf_beam=256, global_leaf_limit=768)
            for stop_reason, leaves_scanned, codes_scanned, next_leaf_rows, page_scanned in (
                ("leaf-limit", 768, 220_000, None, False),
                ("scan-budget", 700, 220_000, 50_000, True),
            ):
                with self.subTest(stop_reason=stop_reason):
                    results = {
                        query: self.diagnostic(
                            query,
                            codes_scanned=codes_scanned,
                            leaves_eligible=4_096,
                            leaves_scanned=leaves_scanned,
                            global_scope=True,
                            stop_reason=stop_reason,
                            next_leaf_rows=next_leaf_rows,
                            scan_budget=262_144,
                        )
                        for query in range(64, 96)
                    }
                    frontier = json.loads(results[64])
                    target = frontier["diagnostics"][-1]
                    target["candidate_rank"] = None
                    target["first_unique_page_rank"] = None
                    target["global_routing_leaf_rank"] = leaves_scanned + 1
                    target["page_in_retained_pool"] = False
                    target["page_in_scanned_pool"] = page_scanned
                    target["page_selected"] = False
                    target["reciprocal_rank_selected"] = False
                    target["routing_leaf_rank"] = leaves_scanned + 1
                    target["stage"] = "leaf-frontier"
                    self.refresh_page_selections(frontier)
                    results[64] = (
                        json.dumps(
                            frontier,
                            allow_nan=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        ).encode()
                        + b"\n"
                    )
                    payload = run_v32_no_page_containment(
                        plan,
                        truth,
                        invoke=lambda command, values=results: values[
                            int(command[command.index("--query-start") + 1])
                        ],
                    )
                    value = json.loads(payload)
                    self.assertEqual(value["losses_by_stage"], {"leaf-frontier": 1})
                    self.assertEqual(value["selected_page_hits"], 319)
                    self.assertEqual(value["queries"][0]["targets"][-1]["routing_leaf_rank"], leaves_scanned + 1)

                    forged = json.loads(results[64])
                    forged["diagnostics"][-1]["routing_leaf_rank"] = leaves_scanned
                    forged["diagnostics"][-1]["global_routing_leaf_rank"] = leaves_scanned
                    results[64] = (
                        json.dumps(
                            forged,
                            allow_nan=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        ).encode()
                        + b"\n"
                    )
                    with self.assertRaisesRegex(ValueError, "diagnostic value"):
                        run_v32_no_page_containment(
                            plan,
                            truth,
                            invoke=lambda command, values=results: values[
                                int(command[command.index("--query-start") + 1])
                            ],
                        )

    def test_v32_containment_clamps_leaf_beam_to_the_selected_root_frontier(self) -> None:
        # Break caught: the Rust router correctly clamps a wider beam to a
        # smaller selected-root frontier, but the independent reducer rejects
        # that bounded work before it can classify containment.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary))
            plan = replace(plan, leaf_beam=256)
            results = {
                query: self.diagnostic(
                    query,
                    leaves_eligible=73,
                    leaves_scanned=73,
                    scan_budget=262_144,
                )
                for query in range(64, 96)
            }
            payload = run_v32_no_page_containment(
                plan,
                truth,
                invoke=lambda command: results[
                    int(command[command.index("--query-start") + 1])
                ],
            )
        value = json.loads(payload)
        self.assertEqual(value["maximum_leaves_eligible"], 73)
        self.assertEqual(value["maximum_leaves_scanned"], 73)
        self.assertEqual(value["selected_page_hits"], 320)
        self.assertEqual(value["root_beam"], 8)
        self.assertEqual(value["leaf_beam"], 256)

    def test_v32_100k_rank_evidence_caps_candidates_at_scanned_population(self) -> None:
        # Break caught: the 100K rank-envelope leg is rejected because its root
        # frontier contains fewer than the serving maximum of 12,288 rows.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary), source_rows=100_000)
            results = {
                query: self.diagnostic(
                    query, candidates_retained=6_000, codes_scanned=6_000
                )
                for query in range(64, 96)
            }
            payload = run_v32_no_page_containment(
                plan,
                truth,
                invoke=lambda command: results[
                    int(command[command.index("--query-start") + 1])
                ],
            )
        value = json.loads(payload)
        self.assertEqual(value["source_rows"], 100_000)
        self.assertEqual(value["selected_page_hits"], 320)
        self.assertEqual(value["maximum_codes_scanned"], 6_000)
        self.assertEqual(value["status"], "passed")

    def test_v32_containment_recomputes_failure_stage_without_page_bodies(self) -> None:
        # Break caught: a candidate/page miss is hidden behind aggregate recall or
        # a no-page diagnostic is mislabeled as a serving measurement.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary))
            results = {
                query: self.diagnostic(query, miss=query == 75)
                for query in range(64, 96)
            }
            payload = run_v32_no_page_containment(
                plan, truth, invoke=lambda command: results[int(command[command.index("--query-start") + 1])]
            )
        value = json.loads(payload)
        self.assertEqual(value["source_rows"], 1_000_000)
        self.assertEqual(value["selected_page_hits"], 319)
        self.assertEqual(value["aggregate_containment_ppm"], 996_875)
        self.assertEqual(value["minimum_containment_ppm"], 900_000)
        self.assertEqual(value["perfect_queries"], 31)
        self.assertEqual(value["losses_by_stage"], {"candidate-retention": 1})
        self.assertEqual(value["page_body_reads"], 0)
        self.assertEqual(value["maximum_codes_scanned"], 40_095)
        self.assertEqual(value["maximum_leaves_eligible"], 128)
        self.assertEqual(value["maximum_leaves_scanned"], 128)
        self.assertEqual(value["maximum_truth_microleaf_rank"], 96)
        self.assertEqual(value["maximum_query_table_pairs_built"], 1)
        self.assertEqual(value["maximum_peak_query_table_pairs_live"], 1)
        self.assertEqual(value["maximum_routing_leaf_rows"], 1_024)
        self.assertEqual(value["maximum_selected_page_bytes"], 2_900_000)
        self.assertEqual(value["status"], "failed")
        self.assertEqual(value["failed_gates"], ["perfect-containment"])
        self.assertFalse(value["claim_eligible"])

    def test_v32_containment_preserves_paired_reducer_recoveries_and_losses(self) -> None:
        # Break caught: the runner collapses validated per-target evidence to a
        # hit count, so a reciprocal-rank recovery and a different eviction can
        # produce the same aggregate terminal and become indistinguishable.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary))
            results = {
                query: self.diagnostic(query, miss=query == 75)
                for query in range(64, 96)
            }
            recovered = json.loads(results[75])
            recovered["diagnostics"][-1]["reciprocal_rank_selected"] = True
            evicted = json.loads(results[76])
            evicted["diagnostics"][0]["reciprocal_rank_selected"] = False
            self.refresh_page_selections(recovered)
            self.refresh_page_selections(evicted)
            results[75] = (
                json.dumps(
                    recovered,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                + b"\n"
            )
            results[76] = (
                json.dumps(
                    evicted,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                + b"\n"
            )
            payload = run_v32_no_page_containment(
                plan,
                truth,
                invoke=lambda command: results[
                    int(command[command.index("--query-start") + 1])
                ],
            )

        value = json.loads(payload)
        self.assertEqual(value["schema_version"], 4)
        self.assertEqual(value["selected_page_hits"], 319)
        self.assertEqual(value["reciprocal_rank"]["selected_page_hits"], 319)
        self.assertEqual(value["reciprocal_rank"]["aggregate_containment_ppm"], 996_875)
        self.assertEqual(value["reciprocal_rank"]["minimum_containment_ppm"], 900_000)
        self.assertEqual(value["reciprocal_rank"]["perfect_queries"], 31)
        self.assertEqual(
            value["reciprocal_rank"]["maximum_selected_page_bytes"], 2_900_000
        )
        query_75 = value["queries"][11]
        self.assertEqual(query_75["query_ordinal"], 75)
        self.assertEqual(query_75["baseline_hits"], 9)
        self.assertEqual(query_75["reciprocal_rank_hits"], 10)
        self.assertEqual(query_75["recovered_logicals"], [7_510])
        self.assertEqual(query_75["lost_logicals"], [])
        self.assertEqual(query_75["targets"][-1]["source_ordinal"], 7_509)
        self.assertEqual(query_75["targets"][-1]["truth_position"], 9)
        query_76 = value["queries"][12]
        self.assertEqual(query_76["query_ordinal"], 76)
        self.assertEqual(query_76["baseline_hits"], 10)
        self.assertEqual(query_76["reciprocal_rank_hits"], 9)
        self.assertEqual(query_76["recovered_logicals"], [])
        self.assertEqual(query_76["lost_logicals"], [7_601])
        self.assertEqual(query_76["routing"]["selected_pages"], 16)
        self.assertEqual(
            query_76["page_selections"]["reciprocal_rank"]["pages"][0][
                "ordinal"
            ],
            761,
        )

    def test_v32_containment_counts_a_selected_page_after_candidate_pruning(self) -> None:
        # Break caught: the reducer reports a false miss when another retained
        # row selects the physical page containing a pruned truth row.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary))
            cases = ((76, False), (None, True), (None, False))
            for routing_leaf_rank, reciprocal_rank_selected in cases:
                with self.subTest(
                    routing_leaf_rank=routing_leaf_rank,
                    reciprocal_rank_selected=reciprocal_rank_selected,
                ):
                    results = {
                        query: self.diagnostic(query, miss=query == 75)
                        for query in range(64, 96)
                    }
                    recovered = json.loads(results[75])
                    recovered["diagnostics"][-1]["stage"] = "selected-page"
                    recovered["diagnostics"][-1]["first_unique_page_rank"] = 9
                    recovered["diagnostics"][-1]["page_in_retained_pool"] = True
                    recovered["diagnostics"][-1]["page_selected"] = True
                    recovered["diagnostics"][-1]["routing_leaf_rank"] = routing_leaf_rank
                    recovered["diagnostics"][-1]["reciprocal_rank_selected"] = (
                        reciprocal_rank_selected
                    )
                    self.refresh_page_selections(recovered)
                    results[75] = (
                        json.dumps(
                            recovered,
                            allow_nan=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        ).encode()
                        + b"\n"
                    )
                    payload = run_v32_no_page_containment(
                        plan,
                        truth,
                        invoke=lambda command, results=results: results[
                            int(command[command.index("--query-start") + 1])
                        ],
                    )
                    value = json.loads(payload)
                    self.assertEqual(value["selected_page_hits"], 320)
                    self.assertEqual(value["failed_gates"], [])
                    self.assertEqual(value["status"], "passed")

    def test_v32_containment_rejects_diagnostic_or_identity_drift(self) -> None:
        # Break caught: malformed/noncanonical router evidence or a different
        # truth identity is accepted as scale authority.
        with tempfile.TemporaryDirectory() as temporary:
            plan, truth = self.fixture(Path(temporary))
            results = tuple(self.diagnostic(query) for query in range(64, 96))
            malformed = json.loads(results[0])
            malformed["diagnostics"][0]["logical"] = 999_999
            changed = (
                json.dumps(malformed, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "truth binding"):
                run_v32_no_page_containment(
                    plan,
                    truth,
                    invoke=lambda command: changed
                    if command[command.index("--query-start") + 1] == "64"
                    else results[int(command[command.index("--query-start") + 1]) - 64],
                )

            work_drift = json.loads(results[0])
            work_drift["routing"]["leaves_eligible"] = 513
            changed_work = (
                json.dumps(work_drift, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "routing work"):
                run_v32_no_page_containment(
                    plan,
                    truth,
                    invoke=lambda command: changed_work
                    if command[command.index("--query-start") + 1] == "64"
                    else results[int(command[command.index("--query-start") + 1]) - 64],
                )

            rank_drift = json.loads(results[0])
            rank_drift["diagnostics"][0]["routing_leaf_rank"] = 129
            changed_rank = (
                json.dumps(rank_drift, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "diagnostic value"):
                run_v32_no_page_containment(
                    plan,
                    truth,
                    invoke=lambda command: changed_rank
                    if command[command.index("--query-start") + 1] == "64"
                    else results[int(command[command.index("--query-start") + 1]) - 64],
                )
            drift = V32ContainmentPlan(
                **{
                    **plan.__dict__,
                    "truth": LocalArtifact(
                        plan.truth.path, "0" * 64, plan.truth.encoded_bytes
                    ),
                }
            )
            with self.assertRaisesRegex(ValueError, "truth byte authority"):
                build_v32_containment_commands(drift, truth)

            receipt_value = json.loads(plan.truth_receipt.path.read_bytes())
            receipt_value["query_start"] = 0
            receipt_bytes = (
                json.dumps(receipt_value, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            plan.truth_receipt.path.write_bytes(receipt_bytes)
            receipt_drift = V32ContainmentPlan(
                **{
                    **plan.__dict__,
                    "truth_receipt": LocalArtifact(
                        plan.truth_receipt.path,
                        hashlib.sha256(receipt_bytes).hexdigest(),
                        len(receipt_bytes),
                    ),
                }
            )
            with self.assertRaisesRegex(ValueError, "truth receipt authority"):
                run_v32_no_page_containment(
                    receipt_drift,
                    truth,
                    invoke=lambda _command: self.fail(
                        "diagnostic ran after truth receipt drift"
                    ),
                )

            plan.truth_receipt.path.write_bytes(
                json.dumps(
                    {**receipt_value, "query_start": 64},
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                + b"\n"
            )

            manifest_value = json.loads(plan.manifest.path.read_bytes())
            manifest_value["layout"]["maximum_routing_leaf_rows"] = 1_025
            manifest_bytes = (
                json.dumps(manifest_value, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            plan.manifest.path.write_bytes(manifest_bytes)
            geometry_drift = V32ContainmentPlan(
                **{
                    **plan.__dict__,
                    "manifest": LocalArtifact(
                        plan.manifest.path,
                        hashlib.sha256(manifest_bytes).hexdigest(),
                        len(manifest_bytes),
                    ),
                }
            )
            with self.assertRaisesRegex(ValueError, "scale geometry"):
                run_v32_no_page_containment(
                    geometry_drift,
                    truth,
                    invoke=lambda _command: self.fail(
                        "diagnostic ran after routing-range overflow"
                    ),
                )

            plan.manifest.path.write_bytes(
                json.dumps(
                    {
                        **manifest_value,
                        "layout": {
                            **manifest_value["layout"],
                            "maximum_routing_leaf_rows": 1_024,
                        },
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                + b"\n"
            )


if __name__ == "__main__":
    unittest.main()
