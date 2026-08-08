import csv
import hashlib
import json
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "scripts" / "validate_global_cell_stripes.py"
MANIFEST = ROOT / "docs" / "research" / "global-cell-stripe-confirmation.json"


def percentile(values, quantile):
    ordered = sorted(values)
    return ordered[int((len(ordered) - 1) * quantile + 0.5)]


class GlobalCellStripeConfirmationValidatorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.manifest = json.loads(MANIFEST.read_text())
        self.manifest_sha = hashlib.sha256(MANIFEST.read_bytes()).hexdigest()

    def tearDown(self):
        self.temp.cleanup()

    def run_validator(self, manifest=MANIFEST):
        return subprocess.run(
            ["python3", str(VALIDATOR), "--manifest", str(manifest), str(self.root)],
            text=True,
            capture_output=True,
            check=False,
        )

    def write_terminal_matrix(self):
        (self.root / self.manifest["root_complete_marker"]).touch()
        (self.root / "manifest.json").write_text(MANIFEST.read_text())
        query_count = self.manifest["queries_per_arm"]
        for repetition, order in enumerate(self.manifest["arm_orders"], 1):
            for order_position, stripe_bytes in enumerate(order):
                name = f"s{stripe_bytes // 1048576}m"
                arm = self.root / "repetitions" / f"r{repetition:02}" / name
                arm.mkdir(parents=True)
                for artifact in ("READ_QUALIFICATION_COMPLETE", "CELL_COMPLETE"):
                    (arm / artifact).touch()
                (arm / "process_exit.txt").write_text("0\n")
                (arm / "resources.csv").write_text("timestamp_ms,rss_bytes\n0,1024\n")
                (arm / "storage-access.csv").write_text("operation,path\nget,x\n")
                cache = self.root / "caches" / f"r{repetition:02}-{name}"
                (arm / "environment.txt").write_text(
                    f"source_sha256={'a' * 64}\n"
                    f"manifest_sha256={self.manifest_sha}\n"
                    f"base_source_sha256={self.manifest['base_source_sha256']}\n"
                    f"base_manifest_sha256={self.manifest['base_manifest_sha256']}\n"
                    f"base_samples_sha256={self.manifest['base_samples_sha256']}\n"
                    f"dataset_sha256={self.manifest['dataset_sha256']}\n"
                    f"base_cell={self.manifest['base_cell']}\n"
                    f"index_uri={self.manifest['base_index_uri']}\n"
                    f"cache_dir={cache}\n"
                    f"repetition={repetition}\n"
                    f"stripe_bytes={stripe_bytes}\n"
                    f"order_position={order_position}\n"
                )
                base_latency = 150.0 + repetition if stripe_bytes == 1048576 else 100.0 + repetition
                gets = 2 if stripe_bytes == 1048576 else 1
                latencies = [base_latency + query / 1000.0 for query in range(query_count)]
                with (arm / "reads.csv").open("w", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(
                        [
                            "query", "record_id", "hit_id", "contains_record_id", "latency_ms",
                            "requests", "gets", "puts", "deletes", "heads", "lists", "bytes_read",
                            "segments_searched", "global_base_approximate_us", "global_base_exact_rerank_us",
                            "global_delta_approximate_us", "global_delta_exact_rerank_us", "global_delta_wait_us",
                        ]
                    )
                    for query, latency in enumerate(latencies):
                        writer.writerow(
                            [query, f"id-{query}", f"id-{query}", "true", latency, gets, gets, 0, 0, 0, 0, 1024, 4, 1, 1, 1, 1, 1]
                        )
                fields = [
                    "protocol_kind", "source_sha256", "manifest_sha256", "base_source_sha256",
                    "base_manifest_sha256", "base_samples_sha256", "dataset_sha256", "base_cell",
                    "index_uri", "repetition", "order_position", "stripe_bytes", "queries",
                    "inserted_id_recall_at_10", "read_p50_ms", "read_p95_ms", "read_storage_requests",
                    "read_storage_gets", "read_storage_puts", "read_storage_deletes", "read_storage_heads",
                    "read_storage_lists", "read_bytes", "read_segments_searched",
                ]
                with (arm / "summary.csv").open("w", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=fields)
                    writer.writeheader()
                    writer.writerow(
                        {
                            "protocol_kind": self.manifest["protocol_kind"],
                            "source_sha256": "a" * 64,
                            "manifest_sha256": self.manifest_sha,
                            "base_source_sha256": self.manifest["base_source_sha256"],
                            "base_manifest_sha256": self.manifest["base_manifest_sha256"],
                            "base_samples_sha256": self.manifest["base_samples_sha256"],
                            "dataset_sha256": self.manifest["dataset_sha256"],
                            "base_cell": self.manifest["base_cell"],
                            "index_uri": self.manifest["base_index_uri"],
                            "repetition": repetition,
                            "order_position": order_position,
                            "stripe_bytes": stripe_bytes,
                            "queries": query_count,
                            "inserted_id_recall_at_10": 1.0,
                            "read_p50_ms": percentile(latencies, 0.50),
                            "read_p95_ms": percentile(latencies, 0.95),
                            "read_storage_requests": gets * query_count,
                            "read_storage_gets": gets * query_count,
                            "read_storage_puts": 0,
                            "read_storage_deletes": 0,
                            "read_storage_heads": 0,
                            "read_storage_lists": 0,
                            "read_bytes": 1024 * query_count,
                            "read_segments_searched": 4 * query_count,
                        }
                    )

    def rewrite_arm(self, repetition, name, latencies, *, bytes_per_query=1024):
        arm = self.root / "repetitions" / f"r{repetition:02}" / name
        reads_path = arm / "reads.csv"
        with reads_path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
            fields = list(rows[0])
        self.assertEqual(len(rows), len(latencies))
        for row, latency in zip(rows, latencies):
            row["latency_ms"] = str(latency)
            row["bytes_read"] = str(bytes_per_query)
        with reads_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        summary_path = arm / "summary.csv"
        with summary_path.open(newline="") as handle:
            summaries = list(csv.DictReader(handle))
            summary_fields = list(summaries[0])
        summaries[0]["read_p50_ms"] = str(percentile(latencies, 0.50))
        summaries[0]["read_p95_ms"] = str(percentile(latencies, 0.95))
        summaries[0]["read_bytes"] = str(bytes_per_query * len(latencies))
        with summary_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=summary_fields)
            writer.writeheader()
            writer.writerows(summaries)

    def test_accepts_terminal_confirmation_and_selects_four_mib(self):
        self.write_terminal_matrix()
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["winner"], "s4m")
        self.assertEqual(report["arms"]["s4m"]["queries"], 2500)
        self.assertEqual(report["arms"]["s4m"]["recall_at_10"], 1.0)
        self.assertTrue(all(report["selection_criteria"].values()))

    def test_checks_terminal_marker_before_opening_measurement_csvs(self):
        (self.root / "manifest.json").write_text(MANIFEST.read_text())
        malformed = self.root / "repetitions" / "r01" / "s1m"
        malformed.mkdir(parents=True)
        (malformed / "reads.csv").write_text("malformed")
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("campaign is incomplete", result.stderr)
        self.assertNotIn("reads.csv", result.stderr)

    def test_rejects_a_changed_frozen_shape_before_measurements(self):
        changed = dict(self.manifest)
        changed["dimensions"] = 384
        changed_path = self.root / "changed-manifest.json"
        changed_path.write_text(json.dumps(changed))
        result = self.run_validator(changed_path)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("dimensions", result.stderr)
        self.assertNotIn("campaign is incomplete", result.stderr)

    def test_does_not_promote_when_pooled_improvement_is_below_ten_percent(self):
        self.write_terminal_matrix()
        for repetition in range(1, 6):
            self.rewrite_arm(repetition, "s4m", [145.0 + query / 1000 for query in range(500)])
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertIsNone(report["winner"])
        self.assertFalse(report["selection_criteria"]["minimum_pooled_p95_improvement"])

    def test_does_not_promote_a_p50_regression_hidden_by_better_p95(self):
        self.write_terminal_matrix()
        for repetition in range(1, 6):
            control = [100.0] * 474 + [180.0] * 26
            candidate = [130.0] * 500
            self.rewrite_arm(repetition, "s1m", control)
            self.rewrite_arm(repetition, "s4m", candidate)
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertIsNone(report["winner"])
        self.assertTrue(report["selection_criteria"]["minimum_pooled_p95_improvement"])
        self.assertFalse(report["selection_criteria"]["maximum_pooled_p50_regression"])

    def test_does_not_promote_when_one_repeat_exceeds_the_tail_limit(self):
        self.write_terminal_matrix()
        self.rewrite_arm(1, "s4m", [100.0] * 474 + [210.0] * 26)
        for repetition in range(2, 6):
            self.rewrite_arm(repetition, "s4m", [100.0] * 500)
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertIsNone(report["winner"])
        self.assertTrue(report["selection_criteria"]["pooled_p95_below_limit"])
        self.assertFalse(report["selection_criteria"]["worst_repetition_p95_below_limit"])

    def test_does_not_promote_different_logical_bytes(self):
        self.write_terminal_matrix()
        for repetition in range(1, 6):
            self.rewrite_arm(
                repetition,
                "s4m",
                [100.0 + query / 1000 for query in range(500)],
                bytes_per_query=2048,
            )
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertIsNone(report["winner"])
        self.assertFalse(report["selection_criteria"]["identical_logical_bytes"])

    def test_rejects_query_identity_or_write_corruption(self):
        self.write_terminal_matrix()
        reads = self.root / "repetitions" / "r03" / "s4m" / "reads.csv"
        reads.write_text(reads.read_text().replace("id-17,id-17", "wrong,id-17", 1))
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("paired query IDs differ", result.stderr)

        self.temp.cleanup()
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.write_terminal_matrix()
        reads = self.root / "repetitions" / "r03" / "s4m" / "reads.csv"
        reads.write_text(reads.read_text().replace(",1,1,0,0,0,0,1024,", ",2,1,1,0,0,0,1024,", 1))
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("query PUT", result.stderr)


if __name__ == "__main__":
    unittest.main()
