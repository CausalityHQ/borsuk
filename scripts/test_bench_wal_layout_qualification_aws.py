import csv
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "bench_wal_layout_qualification_aws.sh"


class BenchWalLayoutQualificationAwsTests(unittest.TestCase):
    def test_shell_is_valid(self) -> None:
        subprocess.run(["bash", "-n", str(SCRIPT)], check=True)

    def test_dry_run_freezes_complete_counterbalanced_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            environment = os.environ.copy()
            environment.update(
                {
                    "BORSUK_WAL_LAYOUT_ROOT": str(root),
                    "BORSUK_WAL_LAYOUT_EXECUTE": "0",
                }
            )
            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=REPO,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("220 cases", completed.stdout)
            with (root / "schedule.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 220)
            self.assertEqual(len({row["case_id"] for row in rows}), 220)
            self.assertEqual(
                {row["dataset"] for row in rows if row["dataset"]},
                {"fashion-mnist-784", "glove-100"},
            )

            grouped: dict[tuple[str, str, str], list[dict[str, str]]] = {}
            for row in rows:
                key = (
                    row["repetition_id"],
                    row["workload"],
                    row["backend"],
                )
                grouped.setdefault(key, []).append(row)
            self.assertEqual(len(grouped), 110)
            for cases in grouped.values():
                self.assertEqual(
                    {case["arm"] for case in cases},
                    {"fixed-parquet", "adaptive-candidate"},
                )
                self.assertEqual({case["arm_position"] for case in cases}, {"0", "1"})
            first_positions = {
                cases[0]["arm"]
                for cases in grouped.values()
                if cases[0]["arm_position"] == "0"
            }
            self.assertEqual(first_positions, {"fixed-parquet", "adaptive-candidate"})

    def test_protocol_freezes_record_only_schema_and_runtime_rule(self) -> None:
        protocol = json.loads(
            (
                REPO / "docs" / "research" / "wal-layout-qualification-protocol.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            protocol["campaign_id"],
            "wal-layout-qualification-20260728-v5",
        )
        self.assertEqual(
            protocol["predecessor_run"]["campaign_id"],
            "wal-layout-qualification-20260727-v4",
        )
        self.assertEqual(
            protocol["predecessor_run"]["status"],
            "invalidated-before-execution",
        )
        self.assertEqual(protocol["predecessor_run"]["reused_cases"], 0)
        self.assertEqual(
            protocol["last_completed_run"]["campaign_id"],
            "wal-layout-qualification-20260727-v3",
        )
        self.assertEqual(protocol["wal_schema_contract"]["table_format_version"], 16)
        self.assertEqual(
            protocol["wal_schema_contract"]["required_columns"],
            [
                "record_id",
                "metadata",
                "vector",
                "wal_record_extras",
                "wal_vector_element_type",
                "wal_vector_dimensions",
            ],
        )
        self.assertEqual(
            protocol["wal_schema_contract"]["omitted_segment_columns"],
            ["segment_header", "routing_code", "pq_code"],
        )
        self.assertEqual(
            protocol["candidate_contract"]["decision_cardinality"],
            "actual-wal-object-rows-at-write-time",
        )

    def test_dry_run_rejects_hand_labeled_format_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            protocol = json.loads(
                (
                    REPO
                    / "docs"
                    / "research"
                    / "wal-layout-qualification-protocol.json"
                ).read_text(encoding="utf-8")
            )
            protocol["wal_schema_contract"] = {
                "table_format_version": 16,
                "required_columns": [
                    "record_id",
                    "metadata",
                    "vector",
                    "wal_record_extras",
                    "wal_vector_element_type",
                    "wal_vector_dimensions",
                ],
                "omitted_segment_columns": [
                    "segment_header",
                    "routing_code",
                    "pq_code",
                ],
            }
            protocol["candidate_contract"]["decision_cardinality"] = (
                "actual-wal-object-rows-at-write-time"
            )
            protocol["workloads"][0]["expected_candidate_format"] = "vortex"
            protocol_path = root / "protocol.json"
            protocol_path.write_text(json.dumps(protocol), encoding="utf-8")
            environment = os.environ.copy()
            environment.update(
                {
                    "BORSUK_WAL_LAYOUT_PROTOCOL": str(protocol_path),
                    "BORSUK_WAL_LAYOUT_ROOT": str(root / "campaign"),
                    "BORSUK_WAL_LAYOUT_EXECUTE": "0",
                }
            )
            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=REPO,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("expected_candidate_format", completed.stderr)

    def test_dry_run_rejects_launcher_campaign_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            environment = os.environ.copy()
            environment.update(
                {
                    "BORSUK_WAL_LAYOUT_ROOT": str(Path(directory) / "campaign"),
                    "BORSUK_WAL_LAYOUT_EXECUTE": "0",
                    "BORSUK_FORMAT_RUN_ID": "wrong-campaign",
                }
            )
            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=REPO,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("does not match protocol campaign", completed.stderr)


if __name__ == "__main__":
    unittest.main()
