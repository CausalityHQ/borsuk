import csv
import json
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "scripts" / "validate_global_cell_stripes.py"
MANIFEST = ROOT / "docs" / "research" / "global-cell-stripe-qualification.json"


class GlobalCellStripeValidatorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.manifest = json.loads(MANIFEST.read_text())

    def tearDown(self):
        self.temp.cleanup()

    def run_validator(self, recover=False):
        command = ["python3", str(VALIDATOR), "--manifest", str(MANIFEST)]
        if recover:
            command.append("--recover-terminal-validator-failure")
        command.append(str(self.root))
        return subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
        )

    def write_terminal_matrix(self):
        (self.root / self.manifest["root_complete_marker"]).touch()
        (self.root / "manifest.json").write_text(MANIFEST.read_text())
        for repetition, order in enumerate(self.manifest["arm_orders"], 1):
            for order_position, stripe_bytes in enumerate(order):
                arm = self.root / "repetitions" / f"r{repetition:02}" / f"s{stripe_bytes // 1048576}m"
                arm.mkdir(parents=True)
                (arm / "READ_QUALIFICATION_COMPLETE").touch()
                (arm / "CELL_COMPLETE").touch()
                (arm / "process_exit.txt").write_text("0\n")
                (arm / "resources.csv").write_text("timestamp_ms,rss_bytes\n0,1024\n")
                (arm / "storage-access.csv").write_text("operation,path\nget,x\n")
                cache_dir = self.root / "caches" / f"r{repetition:02}-s{stripe_bytes // 1048576}m"
                (arm / "environment.txt").write_text(
                    f"source_sha256={'a' * 64}\n"
                    f"manifest_sha256={self.manifest_sha}\n"
                    f"base_source_sha256={self.manifest['base_source_sha256']}\n"
                    f"base_manifest_sha256={self.manifest['base_manifest_sha256']}\n"
                    f"base_samples_sha256={self.manifest['base_samples_sha256']}\n"
                    f"dataset_sha256={self.manifest['dataset_sha256']}\n"
                    f"base_cell={self.manifest['base_cell']}\n"
                    f"index_uri={self.manifest['base_index_uri']}\n"
                    f"cache_dir={cache_dir}\n"
                    f"repetition={repetition}\n"
                    f"stripe_bytes={stripe_bytes}\n"
                    f"order_position={order_position}\n"
                )
                baseline = 120.0 + repetition
                if stripe_bytes == 2097152:
                    baseline -= 20.0
                elif stripe_bytes == 4194304:
                    baseline -= 10.0
                with (arm / "reads.csv").open("w", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(
                        [
                            "query",
                            "record_id",
                            "hit_id",
                            "contains_record_id",
                            "latency_ms",
                            "requests",
                            "gets",
                            "puts",
                            "deletes",
                            "heads",
                            "lists",
                            "bytes_read",
                            "segments_searched",
                            "global_base_approximate_us",
                            "global_base_exact_rerank_us",
                        ]
                    )
                    for query in range(100):
                        latency = baseline + query / 100.0
                        writer.writerow(
                            [query, f"id-{query}", f"id-{query}", "true", latency, 2, 2, 0, 0, 0, 0, 1024, 4, 1, 1]
                        )
                with (arm / "summary.csv").open("w", newline="") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=[
                            "protocol_kind",
                            "source_sha256",
                            "manifest_sha256",
                            "base_source_sha256",
                            "base_manifest_sha256",
                            "base_samples_sha256",
                            "dataset_sha256",
                            "base_cell",
                            "index_uri",
                            "repetition",
                            "order_position",
                            "stripe_bytes",
                            "queries",
                            "inserted_id_recall_at_10",
                            "read_p50_ms",
                            "read_p95_ms",
                            "read_storage_requests",
                            "read_storage_gets",
                            "read_storage_puts",
                            "read_storage_deletes",
                            "read_storage_heads",
                            "read_storage_lists",
                            "read_bytes",
                            "read_segments_searched",
                        ],
                    )
                    writer.writeheader()
                    writer.writerow(
                        {
                            "protocol_kind": "production-diagnostic",
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
                            "queries": 100,
                            "inserted_id_recall_at_10": 1.0,
                            "read_p50_ms": baseline + 0.50,
                            "read_p95_ms": baseline + 0.94,
                            "read_storage_requests": 200,
                            "read_storage_gets": 200,
                            "read_storage_puts": 0,
                            "read_storage_deletes": 0,
                            "read_storage_heads": 0,
                            "read_storage_lists": 0,
                            "read_bytes": 102400,
                            "read_segments_searched": 400,
                        }
                    )

    @property
    def manifest_sha(self):
        import hashlib

        return hashlib.sha256(MANIFEST.read_bytes()).hexdigest()

    def test_rejects_incomplete_root_before_opening_measurements(self):
        (self.root / "summary.csv").write_text("malformed")
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("campaign is incomplete", result.stderr)
        self.assertNotIn("summary", result.stderr)

    def test_rejects_failure_marker(self):
        (self.root / self.manifest["root_complete_marker"]).touch()
        (self.root / self.manifest["root_failure_marker"]).touch()
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("failure marker", result.stderr)

    def test_explicit_recovery_validates_all_arms_after_terminal_validator_failure(self):
        self.write_terminal_matrix()
        (self.root / self.manifest["root_complete_marker"]).unlink()
        (self.root / self.manifest["root_failure_marker"]).touch()

        ordinary = self.run_validator()
        self.assertNotEqual(ordinary.returncode, 0)
        self.assertIn("campaign is incomplete", ordinary.stderr)

        recovered = self.run_validator(recover=True)
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        self.assertEqual(json.loads(recovered.stdout)["terminal_mode"], "validator-failure-recovery")

    def test_validator_avoids_python_310_only_zip_strict_keyword(self):
        self.assertNotIn("strict=True", VALIDATOR.read_text())

    def test_accepts_exact_terminal_matrix_and_selects_paired_winner(self):
        self.write_terminal_matrix()
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["winner"], "s2m")
        self.assertEqual(report["arms"]["s2m"]["queries"], 500)
        self.assertEqual(report["arms"]["s2m"]["recall_at_10"], 1.0)

    def test_rejects_missing_arm_and_identity_or_recall_corruption(self):
        self.write_terminal_matrix()
        missing = self.root / "repetitions" / "r05" / "s4m" / "CELL_COMPLETE"
        missing.unlink()
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing", result.stderr)

        missing.touch()
        summary = self.root / "repetitions" / "r03" / "s2m" / "summary.csv"
        text = summary.read_text().replace(self.manifest["base_samples_sha256"], "f" * 64)
        summary.write_text(text)
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("base_samples_sha256", result.stderr)

    def test_rejects_reused_cache_directory(self):
        self.write_terminal_matrix()
        first = self.root / "repetitions" / "r01" / "s1m" / "environment.txt"
        second = self.root / "repetitions" / "r01" / "s2m" / "environment.txt"
        first_cache = next(line for line in first.read_text().splitlines() if line.startswith("cache_dir="))
        lines = [first_cache if line.startswith("cache_dir=") else line for line in second.read_text().splitlines()]
        second.write_text("\n".join(lines) + "\n")
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cache_dir", result.stderr)


if __name__ == "__main__":
    unittest.main()
