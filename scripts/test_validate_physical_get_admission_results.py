import csv
import json
import tempfile
import unittest
from pathlib import Path

import validate_physical_get_admission_results as validator


class PhysicalGetAdmissionResultsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def write_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows(rows)

    def write_protocol(self) -> None:
        (self.root / "protocol.json").write_text(
            json.dumps(validator.FROZEN_PROTOCOL, indent=2, sort_keys=True) + "\n"
        )

    def write_complete_campaign(self, latency_by_worker: dict[int, float]) -> None:
        self.write_protocol()
        (self.root / "RAW_MEASUREMENTS_COMPLETE").write_text("complete\n")
        (self.root / "environment.txt").write_text(
            "\n".join(
                [
                    "campaign_id=cohere1m-ac4a68d-v1",
                    "source_commit=ac4a68da5a19ead15f896d7225244cea457d73a4",
                    "source_archive_sha256=78e62074a7868302cb8bd1fe6ae74814419be784c594fe2baea8bf71cd4b99c2",
                    "dataset_descriptor_sha256=54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254",
                    "instance_id=i-0641c0333f007a30f",
                    "instance_type=c7g.8xlarge",
                    "region=eu-central-1",
                    "architecture=aarch64",
                    "runner_sha256=b66406d223c88d75cfdf0848713bb84438292a20b1ecdb77855b300470d3d5c2",
                    "binary_sha256=5397c07417a0230bca52e67cc4799ea2b4f3714e57c09b883f35b3cae3e2fdd3",
                ]
            )
            + "\n"
        )
        schedule_rows: list[list[object]] = []
        for repetition_number in range(1, 6):
            repetition = f"r{repetition_number:02d}"
            arms = (
                ["production-cap-128", "high-cap-control-1024"]
                if repetition_number % 2
                else ["high-cap-control-1024", "production-cap-128"]
            )
            for position, arm in enumerate(arms):
                schedule_rows.append(
                    [
                        repetition,
                        position,
                        arm,
                        128 if arm.startswith("production") else 1024,
                        "1;8;32",
                    ]
                )
        self.write_csv(
            self.root / "schedule.csv",
            ["repetition", "position", "arm", "backing_get_concurrency", "workers"],
            schedule_rows,
        )
        build = self.root / "build"
        build.mkdir()
        (build / "BUILD_COMPLETE").write_text("complete\n")
        self.write_csv(
            build / "bench_build.csv",
            [
                "vector_element_type",
                "scan_codec",
                "records",
                "segment_bytes",
                "vector_sidecar_bytes",
                "global_scan_bytes",
                "total_active_index_bytes",
                "bytes_per_vector",
                "resident_bytes_estimate",
                "ram_budget_bytes",
                "collection_resident_bytes",
                "retained_bytes",
                "retained_capacity_bytes",
                "retained_peak_bytes",
                "transient_bytes",
                "transient_capacity_bytes",
                "transient_peak_bytes",
                "ingest_ms",
            ],
            [
                [
                    "float32",
                    "srht-pq-scan",
                    1_000_000,
                    1,
                    1,
                    1,
                    3,
                    3,
                    1,
                    536_870_912,
                    1,
                    0,
                    1,
                    1,
                    0,
                    1,
                    1,
                    1,
                ]
            ],
        )
        self.write_resources(build / "resources.csv")

        for repetition_number in range(1, 6):
            repetition = f"r{repetition_number:02d}"
            repetition_root = self.root / "results" / repetition
            repetition_root.mkdir(parents=True)
            (repetition_root / "REPETITION_COMPLETE").write_text("complete\n")
            for arm in ("production-cap-128", "high-cap-control-1024"):
                self.write_case(repetition_root / arm, latency_by_worker)

    def write_resources(self, path: Path) -> None:
        self.write_csv(
            path,
            [
                "elapsed_ms",
                "cpu_percent",
                "rss_bytes",
                "vms_bytes",
                "process_read_bytes",
                "process_write_bytes",
                "cache_disk_bytes",
                "scratch_disk_bytes",
                "network_receive_bytes",
                "network_transmit_bytes",
            ],
            [[0, 1, 1024, 2048, 0, 0, 0, 0, 1, 1]],
        )

    def write_case(self, root: Path, latency_by_worker: dict[int, float]) -> None:
        root.mkdir()
        (root / "CASE_COMPLETE").write_text("complete\n")
        self.write_csv(root / "bench_startup.csv", ["open_ms"], [[1]])
        self.write_csv(
            root / "bench_cache_states.csv",
            ["scan_codec", "queries"],
            [["srht-pq-scan", 1]],
        )
        summary_rows: list[list[object]] = []
        sample_rows: list[list[object]] = []
        for workers in (1, 8, 32):
            latency = latency_by_worker[workers]
            summary_rows.append(
                [
                    "srht-pq-scan",
                    "scan",
                    workers,
                    1000,
                    1000 / latency,
                    latency,
                    0,
                    latency,
                    latency,
                    latency,
                    latency,
                ]
            )
            for sample_index in range(1000):
                sample_rows.append(
                    [
                        "srht-pq-scan",
                        "scan",
                        "uncached",
                        50,
                        workers,
                        sample_index,
                        sample_index,
                        0,
                        latency,
                        0.96,
                        "srht-pq-scan",
                        1000,
                        0,
                        1,
                        2,
                        0,
                        100,
                        200,
                        2,
                        536_870_912,
                        1000,
                        10,
                        20,
                        20,
                        0,
                        0,
                        0,
                    ]
                )
        self.write_csv(
            root / "bench_concurrency.csv",
            [
                "scan_codec",
                "cache_execution",
                "workers",
                "total_queries",
                "qps",
                "mean_ms",
                "stddev_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "max_ms",
            ],
            summary_rows,
        )
        self.write_csv(
            root / "bench_concurrency_samples.csv",
            [
                "scan_codec",
                "cache_execution",
                "cache_profile",
                "target_cache_coverage_percent",
                "workers",
                "sample_index",
                "query_source_index",
                "target_hot_set_member",
                "latency_ms",
                "recall_at_10",
                "execution_engine",
                "bytes_read",
                "decoded_cache_hits",
                "disk_cache_reads",
                "backing_reads",
                "decoded_cache_bytes_read",
                "disk_cache_bytes_read",
                "backing_bytes_read",
                "network_gets",
                "ram_budget_bytes",
                "collection_resident_bytes",
                "retained_bytes",
                "retained_capacity_bytes",
                "retained_peak_bytes",
                "transient_bytes",
                "transient_capacity_bytes",
                "transient_peak_bytes",
            ],
            sample_rows,
        )
        self.write_resources(root / "resources.csv")

    def test_never_opens_measurements_before_root_terminality(self) -> None:
        self.write_protocol()
        broken = self.root / "results" / "r01" / "production-cap-128"
        broken.mkdir(parents=True)
        (broken / "bench_concurrency_samples.csv").write_text('not,csv\n"unterminated')
        with self.assertRaisesRegex(validator.ValidationError, "terminal marker"):
            validator.validate(self.root, verify_payload_hashes=False)

    def test_accepts_only_when_every_arm_and_worker_meets_frozen_gates(self) -> None:
        self.write_complete_campaign({1: 80.0, 8: 100.0, 32: 150.0})
        decision = validator.validate(self.root, verify_payload_hashes=False)
        self.assertTrue(decision["accepted"])
        self.assertEqual(decision["status"], "accepted")
        self.assertEqual(decision["query_samples"], 30_000)
        self.assertEqual(decision["cells"], 30)

    def test_structurally_valid_campaign_is_rejected_on_latency(self) -> None:
        self.write_complete_campaign({1: 80.0, 8: 250.0, 32: 900.0})
        decision = validator.validate(self.root, verify_payload_hashes=False)
        self.assertFalse(decision["accepted"])
        self.assertEqual(decision["status"], "valid-rejected")
        self.assertIn("p95", " ".join(decision["failures"]))

    def test_rejects_counter_mismatch_without_equating_logical_and_physical_bytes(
        self,
    ) -> None:
        self.write_complete_campaign({1: 80.0, 8: 100.0, 32: 150.0})
        sample_path = (
            self.root
            / "results"
            / "r03"
            / "production-cap-128"
            / "bench_concurrency_samples.csv"
        )
        with sample_path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["network_gets"] = "3"
        rows[0]["bytes_read"] = "7"
        rows[0]["disk_cache_bytes_read"] = "100"
        rows[0]["backing_bytes_read"] = "200"
        self.write_csv(sample_path, list(rows[0]), [list(row.values()) for row in rows])
        with self.assertRaisesRegex(
            validator.ValidationError, "network_gets.*backing_reads"
        ):
            validator.validate(self.root, verify_payload_hashes=False)

    def test_rejects_cross_arm_query_or_recall_divergence(self) -> None:
        self.write_complete_campaign({1: 80.0, 8: 100.0, 32: 150.0})
        sample_path = (
            self.root
            / "results"
            / "r02"
            / "high-cap-control-1024"
            / "bench_concurrency_samples.csv"
        )
        with sample_path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["recall_at_10"] = "0.8"
        self.write_csv(sample_path, list(rows[0]), [list(row.values()) for row in rows])
        with self.assertRaisesRegex(
            validator.ValidationError, "paired recall divergence"
        ):
            validator.validate(self.root, verify_payload_hashes=False)


if __name__ == "__main__":
    unittest.main()
