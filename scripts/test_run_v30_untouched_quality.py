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

from scripts.run_v30_untouched_quality import (
    LocalArtifact,
    V30UntouchedPlan,
    build_qualifier_commands,
    run_v30_untouched_quality,
)


class V30UntouchedQualityTests(unittest.TestCase):
    def test_v30_untouched_direct_script_reaches_the_closed_cli(self) -> None:
        # Break caught: the Spot worker executes this file directly, but an
        # absolute package import fails before parsing any registered input.
        completed = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("run_v30_untouched_quality.py")),
                "--help",
            ],
            check=False,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr.decode())
        self.assertIn(b"--serving-tier", completed.stdout)
        self.assertIn(b"--page-count", completed.stdout)

    def fixture(self, directory: Path) -> tuple[V30UntouchedPlan, dict[int, bytes]]:
        neighbors = pa.array(
            [[row * 100 + rank for rank in range(10)] for row in range(128)],
            type=pa.list_(pa.field("item", pa.int32(), nullable=False), 10),
        )
        table = pa.Table.from_arrays(
            [neighbors],
            schema=pa.schema([pa.field("neighbors_id", neighbors.type, nullable=False)]),
        )
        truth_path = directory / "neighbors.parquet"
        pq.write_table(table, truth_path)
        truth = truth_path.read_bytes()
        query_path = directory / "test.parquet"
        query_path.write_bytes(b"query")
        manifest_path = directory / "manifest.json"
        manifest_path.write_bytes(b"manifest")
        plan = V30UntouchedPlan(
            qualifier=Path("/opt/borsuk/v30_s3_qualify"),
            manifest=LocalArtifact(
                manifest_path, hashlib.sha256(b"manifest").hexdigest(), 8
            ),
            artifact_dir=directory / "resident",
            query=LocalArtifact(
                query_path, hashlib.sha256(b"query").hexdigest(), 5
            ),
            truth=LocalArtifact(
                truth_path, hashlib.sha256(truth).hexdigest(), len(truth)
            ),
            serving_tier="standard",
            source_rows=100_000,
            query_start=64,
            query_count=32,
            leaf_beam=64,
            page_count=16,
        )
        results = {
            row: self.result(row, miss=row == 75, page_count=16) for row in range(64, 96)
        }
        return plan, results

    def result(self, query_row: int, *, miss: bool, page_count: int) -> bytes:
        sources = [query_row * 100 + rank for rank in range(10)]
        if miss:
            sources[-1] = 9_000_000
        value = {
            "claim_eligible": False,
            "matches": [
                {"source_ordinal": source, "squared_distance": float(rank)}
                for rank, source in enumerate(sources)
            ],
            "schema_version": 2,
            "timing": {
                "elapsed_ns": 8_000_000 + query_row,
                "exact_rerank_cpu_ns": 3_000_000 + query_row,
                "exact_rerank_elapsed_ns": 3_000_000 + query_row,
                "page_read_cpu_ns": 1_000_000 + query_row,
                "page_read_elapsed_ns": 2_000_000 + query_row,
                "peak_rss_bytes": 2_000_000_000 + query_row,
                "process_cpu_ns": 12_000_000 + query_row,
                "routing_cpu_ns": 4_000_000 + query_row,
                "routing_elapsed_ns": 2_500_000 + query_row,
            },
            "work": {
                "decoded_rows": 4_000,
                "encoded_bytes": 2_000_000,
                "get_count": page_count,
                "routing": {
                    "candidates_retained": 12_288,
                    "codes_scanned": 900_000,
                    "leaves_eligible": 64,
                    "leaves_scanned": 64,
                    "pages_considered": page_count,
                    "peak_query_table_pairs_live": 1,
                    "query_table_pairs_built": 4,
                    "roots_scored": 1_024,
                    "selected_pages": page_count,
                },
                "unique_rows": 4_000,
            },
        }
        return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"

    def test_v30_untouched_runner_uses_real_qualifier_for_only_the_sealed_rows(self) -> None:
        # Break caught: the campaign invokes nonexistent qualifier flags or
        # evaluates a burned/discovered query range instead of the sealed 32 rows.
        with tempfile.TemporaryDirectory() as temporary:
            plan, results = self.fixture(Path(temporary))
            commands = build_qualifier_commands(plan)
            self.assertEqual(len(commands), 1)
            self.assertEqual(int(commands[0][commands[0].index("--query-start") + 1]), 64)
            self.assertEqual(int(commands[0][commands[0].index("--query-count") + 1]), 32)
            self.assertEqual(int(commands[0][commands[0].index("--page-count") + 1]), 16)
            self.assertEqual(int(commands[0][commands[0].index("--root-beam") + 1]), 8)
            self.assertEqual(int(commands[0][commands[0].index("--leaf-beam") + 1]), 64)
            self.assertEqual(
                int(commands[0][commands[0].index("--candidate-depth") + 1]), 12_288
            )
            self.assertTrue(all("--serving-tier" in command for command in commands))
            self.assertTrue(all("--s3-page-prefix" not in command for command in commands))
            self.assertTrue(all("--construction-manifest-s3" not in command for command in commands))

            wider = build_qualifier_commands(replace(plan, leaf_beam=128))
            self.assertEqual(
                int(wider[0][wider[0].index("--leaf-beam") + 1]), 128
            )
            with self.assertRaisesRegex(ValueError, "untouched plan authority"):
                build_qualifier_commands(replace(plan, leaf_beam=512))
            full = build_qualifier_commands(
                replace(plan, source_rows=9_990_000, leaf_beam=512)
            )
            self.assertEqual(int(full[0][full[0].index("--leaf-beam") + 1]), 512)

            seen: list[tuple[str, ...]] = []

            def invoke(command: tuple[str, ...]) -> bytes:
                seen.append(command)
                return self.batch(results)

            payload = run_v30_untouched_quality(plan, invoke=invoke)
            value = json.loads(payload)
            self.assertEqual(seen, [commands[0]])
            self.assertEqual(value["aggregate_recall_ppm"], 996_875)
            self.assertEqual(value["floor_compliance_ppm"], 1_000_000)
            self.assertEqual(value["minimum_recall_ppm"], 900_000)
            self.assertEqual(value["perfect_queries"], 31)
            self.assertEqual(len(value["samples"]), 32)
            self.assertEqual(value["samples"][11]["query_ordinal"], 75)
            self.assertEqual(value["samples"][11]["hits"], 9)
            self.assertEqual(value["maximum_codes_scanned"], 900_000)
            self.assertEqual(value["maximum_encoded_bytes"], 2_000_000)
            self.assertEqual(value["maximum_get_count"], 16)
            self.assertEqual(value["measured_process_cpu_p99_ns"], 12_000_095)
            self.assertEqual(value["measured_cold_p99_ns"], 8_000_095)
            self.assertEqual(value["maximum_routing_cpu_ns"], 4_000_095)
            self.assertEqual(value["maximum_page_read_cpu_ns"], 1_000_095)
            self.assertEqual(value["maximum_exact_rerank_cpu_ns"], 3_000_095)
            self.assertEqual(value["maximum_routing_elapsed_ns"], 2_500_095)
            self.assertEqual(value["maximum_page_read_elapsed_ns"], 2_000_095)
            self.assertEqual(value["maximum_exact_rerank_elapsed_ns"], 3_000_095)
            self.assertEqual(value["maximum_peak_rss_bytes"], 2_000_000_095)
            self.assertEqual(value["status"], "passed")
            self.assertFalse(value["claim_eligible"])

            over_memory = dict(results)
            bad = json.loads(over_memory[64])
            bad["timing"]["peak_rss_bytes"] = 3 * 1024**3 + 1
            over_memory[64] = (
                json.dumps(bad, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            failed = json.loads(
                run_v30_untouched_quality(
                    plan,
                    invoke=lambda _command: self.batch(over_memory),
                )
            )
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(failed["failed_gates"], ["peak-rss"])
            self.assertFalse(failed["claim_eligible"])

            legacy_work = dict(results)
            legacy = json.loads(legacy_work[64])
            legacy["work"]["routing"]["codes_scanned"] = 0
            legacy_work[64] = (
                json.dumps(legacy, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "production routing work"):
                run_v30_untouched_quality(
                    plan,
                    invoke=lambda _command: self.batch(legacy_work),
                )

    @staticmethod
    def batch(results: dict[int, bytes]) -> bytes:
        value = {
            "claim_eligible": False,
            "results": [json.loads(results[row]) for row in sorted(results)],
            "schema_version": 2,
        }
        return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


if __name__ == "__main__":
    unittest.main()
