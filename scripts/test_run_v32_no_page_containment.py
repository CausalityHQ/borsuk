import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.run_v32_no_page_containment import (
    LocalArtifact,
    V32ContainmentPlan,
    build_v32_containment_commands,
    run_v32_no_page_containment,
)


class V32NoPageContainmentTests(unittest.TestCase):
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

    def fixture(self, directory: Path) -> tuple[V32ContainmentPlan, bytes]:
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
        manifest.write_bytes(b"manifest")
        query = directory / "test.parquet"
        query.write_bytes(b"query")
        logical_sources_path = directory / "logical-sources.arrow"
        logical_sources = pa.array(
            [(logical - 1) % 1_000_000 for logical in range(1_000_000)],
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
                    manifest, hashlib.sha256(b"manifest").hexdigest(), 8
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
                source_rows=1_000_000,
                query_start=64,
                query_count=32,
            ),
            truth,
        )

    @staticmethod
    def diagnostic(query_ordinal: int, *, miss: bool = False) -> bytes:
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
                        "candidates_retained": 12_288,
                        "codes_scanned": 40_000 + query_ordinal,
                        "leaves_scored": 256,
                        "pages_considered": 20,
                        "roots_scored": 128,
                        "selected_page_bytes": 2_900_000,
                        "selected_pages": 16,
                    },
                    "schema_version": 3,
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


if __name__ == "__main__":
    unittest.main()
