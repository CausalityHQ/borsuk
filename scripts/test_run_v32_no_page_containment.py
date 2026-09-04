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
        self.assertNotIn(b"--serving-tier", completed.stdout)
        self.assertNotIn(b"--page", completed.stdout)

    def fixture(
        self, directory: Path, *, source_rows: int = 1_000_000
    ) -> tuple[V32ContainmentPlan, bytes]:
        neighbors = pa.array(
            [[row * 100 + rank for rank in range(10)] for row in range(128)],
            type=pa.list_(pa.field("item", pa.int32(), nullable=False), 10),
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
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
            + b"\n"
        )
        manifest.write_bytes(manifest_bytes)
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
                source_rows=source_rows,
                query_start=64,
                query_count=32,
                leaf_beam=64,
            ),
            truth,
        )

    @staticmethod
    def diagnostic(
        query_ordinal: int,
        *,
        miss: bool = False,
        candidates_retained: int = 12_288,
        codes_scanned: int | None = None,
        leaves_eligible: int = 128,
        leaves_scanned: int = 64,
    ) -> bytes:
        diagnostics = []
        for rank in range(10):
            selected = not (miss and rank == 9)
            diagnostics.append(
                {
                    "candidate_rank": rank if selected else None,
                    "first_unique_page_rank": rank if selected else None,
                    "leaf_ordinal": query_ordinal,
                    "logical": (query_ordinal * 100 + rank + 1) % 1_000_000,
                    "page_ordinal": query_ordinal * 10 + rank,
                    "reciprocal_rank_selected": selected,
                    "routing_leaf_rank": query_ordinal % leaves_scanned + 1,
                    "stage": "selected-page" if selected else "candidate-retention",
                }
            )
        return (
            json.dumps(
                {
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
                        "leaves_eligible": leaves_eligible,
                        "leaves_scanned": leaves_scanned,
                        "pages_considered": 20,
                        "peak_query_table_pairs_live": 1,
                        "query_table_pairs_built": 1,
                        "roots_scored": 128,
                        "selected_page_bytes": 2_900_000,
                        "selected_pages": 16,
                    },
                    "schema_version": 3,
                    "truth_independent_selection": True,
                },
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
        self.assertEqual(
            {command[command.index("--leaf-beam") + 1] for command in commands},
            {"128"},
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
        self.assertEqual(value["maximum_leaves_scanned"], 64)
        self.assertEqual(value["maximum_truth_microleaf_rank"], 32)
        self.assertEqual(value["maximum_query_table_pairs_built"], 1)
        self.assertEqual(value["maximum_peak_query_table_pairs_live"], 1)
        self.assertEqual(value["maximum_routing_leaf_rows"], 1_024)
        self.assertEqual(value["maximum_selected_page_bytes"], 2_900_000)
        self.assertEqual(value["status"], "failed")
        self.assertEqual(value["failed_gates"], ["perfect-containment"])
        self.assertFalse(value["claim_eligible"])

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
