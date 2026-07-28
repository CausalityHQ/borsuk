import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from validate_wal_layout_qualification import (
    REQUIRED_RESULT_FIELDS,
    assemble,
    read_single_case,
    validate_case,
)


def result_row(policy: str, physical_format: str) -> dict[str, str]:
    row = {field: "0" for field in REQUIRED_RESULT_FIELDS}
    row.update(
        {
            "repetition": "r01",
            "policy": policy,
            "element_type": "float32",
            "metric": "euclidean",
            "rows": "5000",
            "dimensions": "96",
            "batch_rows": "500",
            "batches": "10",
            "wal_objects": "10",
            "wal_bytes": "1000",
            "parquet_objects": "10" if physical_format == "parquet" else "0",
            "parquet_bytes": "1000" if physical_format == "parquet" else "0",
            "vortex_objects": "10" if physical_format == "vortex" else "0",
            "vortex_bytes": "1000" if physical_format == "vortex" else "0",
            "ingest_ms": "100",
            "batch_p95_ms": "11",
            "ingest_bytes_written": "2000",
            "ingest_puts": "10",
            "open_ms": "1",
            "first_query_ms": "8",
            "warm_query_p95_ms": "3",
            "warm_query_p99_ms": "4",
            "flush_ms": "20",
            "status": "complete",
        }
    )
    return row


def write_csv(path: Path, row: dict[str, str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=sorted(REQUIRED_RESULT_FIELDS))
        writer.writeheader()
        writer.writerow(row)


class ValidateWalLayoutQualificationTests(unittest.TestCase):
    def test_case_validation_requires_the_selected_physical_format(self) -> None:
        validate_case(result_row("parquet", "parquet"), "fixed-parquet", "vortex")
        validate_case(result_row("adaptive", "vortex"), "adaptive-candidate", "vortex")
        with self.assertRaisesRegex(ValueError, "only Vortex"):
            validate_case(
                result_row("adaptive", "parquet"),
                "adaptive-candidate",
                "vortex",
            )

    def test_single_case_rejects_multiple_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "case.csv"
            row = result_row("parquet", "parquet")
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle, fieldnames=sorted(REQUIRED_RESULT_FIELDS)
                )
                writer.writeheader()
                writer.writerow(row)
                writer.writerow(row)
            with self.assertRaisesRegex(ValueError, "exactly one"):
                read_single_case(path)

    def test_root_assembly_checks_schedule_protocol_and_markers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            protocol = {
                "campaign_id": "fixture",
                "repetitions": 1,
                "queries_per_case": 100,
                "baseline_arm": "fixed-parquet",
                "candidate_arm": "adaptive-candidate",
                "backends": ["local-disk"],
                "workloads": [
                    {
                        "name": "boundary",
                        "rows": 5000,
                        "dimensions": 96,
                        "batch_rows": 500,
                        "element_type": "float32",
                        "metric": "euclidean",
                        "dataset": "",
                        "expected_candidate_format": "vortex",
                    }
                ],
                "promotion_gates": {"required_complete_cases": 2},
            }
            protocol_path = root / "qualification-protocol.json"
            protocol_path.write_text(json.dumps(protocol), encoding="utf-8")
            identity_bytes = b"{}\n"
            (root / "dataset-identities.json").write_bytes(identity_bytes)
            dataset_identity_sha256 = hashlib.sha256(identity_bytes).hexdigest()
            (root / "environment.txt").write_text(
                "source_sha256=" + "a" * 64 + "\n"
                f"dataset_identity_sha256={dataset_identity_sha256}\n",
                encoding="utf-8",
            )
            fields = [
                "repetition_id",
                "workload",
                "backend",
                "arm",
                "arm_position",
                "rows",
                "dimensions",
                "batch_rows",
                "element_type",
                "metric",
                "dataset",
                "expected_candidate_format",
                "case_id",
            ]
            schedule = []
            for arm, policy, physical_format, position in (
                ("fixed-parquet", "parquet", "parquet", "0"),
                ("adaptive-candidate", "adaptive", "vortex", "1"),
            ):
                case_id = f"r01/boundary/local-disk/{arm}"
                scheduled = {
                    "repetition_id": "r01",
                    "workload": "boundary",
                    "backend": "local-disk",
                    "arm": arm,
                    "arm_position": position,
                    "rows": "5000",
                    "dimensions": "96",
                    "batch_rows": "500",
                    "element_type": "float32",
                    "metric": "euclidean",
                    "dataset": "",
                    "expected_candidate_format": "vortex",
                    "case_id": case_id,
                }
                schedule.append(scheduled)
                case_root = root / case_id
                case_root.mkdir(parents=True)
                (case_root / "CASE_COMPLETE").write_text("complete\n", encoding="utf-8")
                protocol_values = {
                    key: value for key, value in scheduled.items() if key != "case_id"
                }
                protocol_values["source_sha256"] = "a" * 64
                protocol_values["dataset_identity_sha256"] = dataset_identity_sha256
                protocol_values["queries_per_case"] = "100"
                protocol_values["index_uri"] = "fixture"
                (case_root / "protocol.txt").write_text(
                    "".join(
                        f"{key}={value}\n" for key, value in protocol_values.items()
                    ),
                    encoding="utf-8",
                )
                (case_root / "resources.csv").write_text(
                    "elapsed_ms,cpu_percent,rss_bytes\n0,0,1024\n100,50,2048\n",
                    encoding="utf-8",
                )
                write_csv(case_root / "result.csv", result_row(policy, physical_format))
            with (root / "schedule.csv").open(
                "w", newline="", encoding="utf-8"
            ) as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerows(schedule)

            output = root / "assembled.csv"
            self.assertEqual(assemble(root, protocol_path, output), 2)
            with output.open(newline="", encoding="utf-8") as handle:
                assembled = list(csv.DictReader(handle))
                self.assertEqual(len(assembled), 2)
                self.assertEqual(assembled[0]["peak_rss_bytes"], "2048")
                self.assertGreater(float(assembled[0]["cpu_core_ms"]), 0)

            schedule[0]["workload"] = "unscheduled"
            with (root / "schedule.csv").open(
                "w", newline="", encoding="utf-8"
            ) as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerows(schedule)
            second_output = root / "drifted.csv"
            with self.assertRaisesRegex(ValueError, "frozen protocol"):
                assemble(root, protocol_path, second_output)

    def test_assembly_requires_valid_resource_samples(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            case_root = root / "r01/wide/local-disk/fixed-parquet"
            case_root.mkdir(parents=True)
            (case_root / "resources.csv").write_text(
                "elapsed_ms,cpu_percent,rss_bytes\n0,0,1024\n100,50,2048\n",
                encoding="utf-8",
            )
            from validate_wal_layout_qualification import read_resource_summary

            summary = read_resource_summary(case_root / "resources.csv")
            self.assertEqual(summary["peak_rss_bytes"], 2048)
            self.assertAlmostEqual(summary["cpu_core_ms"], 50.0)

    def test_resource_summary_prefers_exact_reaped_child_cpu(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "resources.csv"
            path.write_text(
                "elapsed_ms,cpu_percent,rss_bytes,child_cpu_seconds,child_max_rss_bytes\n"
                "0,0,1024,,\n"
                "100,50,2048,,\n"
                "120,0,2048,0.2,4096\n",
                encoding="utf-8",
            )
            from validate_wal_layout_qualification import read_resource_summary

            summary = read_resource_summary(path)
            self.assertEqual(summary["peak_rss_bytes"], 4096)
            self.assertAlmostEqual(summary["cpu_core_ms"], 200.0)


if __name__ == "__main__":
    unittest.main()
