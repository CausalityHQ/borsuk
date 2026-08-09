import csv
import hashlib
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


VALIDATOR = ROOT / "scripts" / "validate_global_range_hedge_qualification.py"
MANIFEST = ROOT / "docs" / "research" / "global-range-hedge-qualification.json"
EXACT_MANIFEST = (
    ROOT / "docs" / "research" / "global-exact-rerank-hedge-qualification.json"
)


class GlobalRangeHedgeValidatorTest(unittest.TestCase):
    manifest_path = MANIFEST

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.manifest = json.loads(self.manifest_path.read_text())
        self.manifest_sha = hashlib.sha256(self.manifest_path.read_bytes()).hexdigest()

    def tearDown(self):
        self.temp.cleanup()

    def run_validator(self, recover=False):
        command = ["python3", str(VALIDATOR), "--manifest", str(self.manifest_path)]
        if recover:
            command.append("--recover-terminal-validator-failure")
        command.append(str(self.root))
        return subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
        )

    def arm_path(self, repetition, arm_name):
        return self.root / "repetitions" / f"r{repetition:02}" / arm_name

    def write_arm(
        self,
        repetition,
        arm_name,
        order_position,
        *,
        latencies=None,
        logical_bytes=1024,
        candidate_gets=False,
        backing_bytes=None,
        puts=0,
    ):
        arm = self.arm_path(repetition, arm_name)
        arm.mkdir(parents=True, exist_ok=True)
        hedge_after = "75" if arm_name == "candidate" else "none"
        protocol_kind = (
            "range-hedge-candidate"
            if arm_name == "candidate"
            else "range-hedge-control"
        )
        if latencies is None:
            latency = (100.0 if arm_name == "candidate" else 120.0) + repetition
            latencies = [latency] * 500
        self.assertEqual(len(latencies), 500)
        if backing_bytes is None:
            backing_bytes = 1100 if arm_name == "candidate" else 1000

        (arm / "READ_HEDGE_QUALIFICATION_COMPLETE").touch()
        (arm / "CELL_COMPLETE").touch()
        (arm / "process_exit.txt").write_text("0\n")
        (arm / "resources.csv").write_text("timestamp_ms,rss_bytes\n0,1024\n")
        (arm / "environment.txt").write_text(
            f"source_sha256={'a' * 64}\n"
            f"manifest_sha256={self.manifest_sha}\n"
            f"base_source_sha256={self.manifest['base_source_sha256']}\n"
            f"base_manifest_sha256={self.manifest['base_manifest_sha256']}\n"
            f"base_samples_sha256={self.manifest['base_samples_sha256']}\n"
            f"dataset_sha256={self.manifest['dataset_sha256']}\n"
            f"base_cell={self.manifest['base_cell']}\n"
            f"index_uri={self.manifest['base_index_uri']}\n"
            "cache_dir=none\n"
            "cache_enabled=false\n"
            f"repetition={repetition}\n"
            "writers=8\n"
            "operations_per_writer=1000\n"
            "records_per_operation=16\n"
            "dimensions=768\n"
            "read_writer=0\n"
            "queries=500\n"
            "max_read_segments=4\n"
            "stripe_bytes=1048576\n"
            f"hedge_after_ms={hedge_after}\n"
            f"order_position={order_position}\n"
        )

        total_gets = 0
        total_requests = 0
        total_logical_bytes = 0
        total_backing_bytes = 0
        total_puts = 0
        with (arm / "reads.csv").open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "query",
                    "record_id",
                    "hit_id",
                    "hit_ids",
                    "contains_record_id",
                    "latency_ms",
                    "requests",
                    "gets",
                    "puts",
                    "deletes",
                    "heads",
                    "lists",
                    "bytes_read",
                    "disk_cache_bytes_read",
                    "backing_bytes_read",
                    "segments_searched",
                    "global_base_approximate_us",
                    "global_base_exact_rerank_us",
                    "global_delta_approximate_us",
                    "global_delta_exact_rerank_us",
                    "global_delta_wait_us",
                ]
            )
            for query, latency in enumerate(latencies):
                gets = 3 if candidate_gets and query < 100 else 2
                requests = gets + puts
                record_id = f"group-o{query * 2 * 16:08}"
                writer.writerow(
                    [
                        query,
                        record_id,
                        record_id,
                        "|".join(
                            [record_id]
                            + [f"neighbor-{query}-{rank}" for rank in range(1, 10)]
                        ),
                        "true",
                        latency,
                        requests,
                        gets,
                        puts,
                        0,
                        0,
                        0,
                        logical_bytes,
                        0,
                        backing_bytes,
                        4,
                        1,
                        2,
                        3,
                        4,
                        5,
                    ]
                )
                total_gets += gets
                total_requests += requests
                total_puts += puts
                total_logical_bytes += logical_bytes
                total_backing_bytes += backing_bytes

        (arm / "storage-access.csv").write_text(
            "operation,object_role,path,physical_format,object_bytes,request_count,"
            "bytes_fetched,logical_projection,row_selection,logical_rows_requested,"
            "logical_rows_decoded,decode_cpu_ns,cache_state,status\n"
            f"read,exact_vectors,global-pq/exact-bundles/test.arrow,arrow,4096,"
            f"{total_gets},{total_backing_bytes},,,,,,backing,ok\n"
        )

        ordered = sorted(latencies)
        p50 = ordered[int((len(ordered) - 1) * 0.50 + 0.5)]
        p95 = ordered[int((len(ordered) - 1) * 0.95 + 0.5)]
        with (arm / "summary.csv").open("w", newline="") as handle:
            fieldnames = [
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
                "writers",
                "operations_per_writer",
                "records_per_operation",
                "dimensions",
                "read_writer",
                "stripe_bytes",
                "hedge_after_ms",
                "cache_enabled",
                "queries",
                "max_read_segments",
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
                "read_disk_cache_bytes",
                "read_backing_bytes",
                "read_segments_searched",
            ]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(
                {
                    "protocol_kind": protocol_kind,
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
                    "writers": 8,
                    "operations_per_writer": 1000,
                    "records_per_operation": 16,
                    "dimensions": 768,
                    "read_writer": 0,
                    "stripe_bytes": 1048576,
                    "hedge_after_ms": hedge_after,
                    "cache_enabled": "false",
                    "queries": 500,
                    "max_read_segments": 4,
                    "inserted_id_recall_at_10": 1.0,
                    "read_p50_ms": p50,
                    "read_p95_ms": p95,
                    "read_storage_requests": total_requests,
                    "read_storage_gets": total_gets,
                    "read_storage_puts": total_puts,
                    "read_storage_deletes": 0,
                    "read_storage_heads": 0,
                    "read_storage_lists": 0,
                    "read_bytes": total_logical_bytes,
                    "read_disk_cache_bytes": 0,
                    "read_backing_bytes": total_backing_bytes,
                    "read_segments_searched": 2000,
                }
            )

    def write_terminal_matrix(self):
        (self.root / self.manifest["root_complete_marker"]).touch()
        (self.root / "manifest.json").write_text(self.manifest_path.read_text())
        for repetition, order in enumerate(self.manifest["arm_orders"], 1):
            for order_position, arm_name in enumerate(order):
                self.write_arm(
                    repetition,
                    arm_name,
                    order_position,
                    candidate_gets=arm_name == "candidate",
                )

    def test_rejects_incomplete_root_before_opening_measurements(self):
        (self.root / "summary.csv").write_text("malformed")
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("campaign is incomplete", result.stderr)
        self.assertNotIn("summary", result.stderr)

    def test_accepts_terminal_matrix_and_promotes_only_the_qualified_candidate(self):
        self.write_terminal_matrix()
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["winner"], "candidate")
        self.assertEqual(report["arms"]["candidate"]["queries"], 2500)
        self.assertEqual(report["arms"]["candidate"]["recall_at_10"], 1.0)
        self.assertTrue(all(report["selection_criteria"].values()))

    def test_explicit_recovery_revalidates_a_terminal_validator_failure(self):
        self.write_terminal_matrix()
        (self.root / self.manifest["root_complete_marker"]).unlink()
        (self.root / self.manifest["root_failure_marker"]).touch()
        ordinary = self.run_validator()
        self.assertNotEqual(ordinary.returncode, 0)
        self.assertIn("campaign is incomplete", ordinary.stderr)

        recovered = self.run_validator(recover=True)
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        self.assertEqual(
            json.loads(recovered.stdout)["terminal_mode"], "validator-failure-recovery"
        )

    def test_rejects_missing_markers_wrong_hedge_cache_queries_recall_and_writes(self):
        mutations = []

        def missing_marker():
            self.arm_path(5, "candidate").joinpath("CELL_COMPLETE").unlink()

        def wrong_hedge():
            path = self.arm_path(2, "candidate") / "environment.txt"
            path.write_text(
                path.read_text().replace("hedge_after_ms=75", "hedge_after_ms=74")
            )

        def cache_enabled():
            path = self.arm_path(1, "control") / "environment.txt"
            path.write_text(
                path.read_text().replace("cache_enabled=false", "cache_enabled=true")
            )

        def query_mismatch():
            path = self.arm_path(3, "control") / "reads.csv"
            rows = path.read_text().splitlines()
            path.write_text("\n".join(rows[:-1]) + "\n")

        def recall_miss():
            path = self.arm_path(4, "candidate") / "reads.csv"
            path.write_text(path.read_text().replace(",true,", ",false,", 1))

        def write_request():
            order = self.manifest["arm_orders"][0].index("candidate")
            self.write_arm(1, "candidate", order, candidate_gets=True, puts=1)

        mutations.extend(
            [
                missing_marker,
                wrong_hedge,
                cache_enabled,
                query_mismatch,
                recall_miss,
                write_request,
            ]
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation.__name__):
                for child in self.root.iterdir():
                    if child.is_dir():
                        import shutil

                        shutil.rmtree(child)
                    else:
                        child.unlink()
                self.write_terminal_matrix()
                mutation()
                result = self.run_validator()
                self.assertNotEqual(result.returncode, 0, result.stderr)

    def test_logical_bytes_latency_pairing_and_amplification_gates_fail_closed(self):
        cases = {}

        def logical_bytes():
            for repetition, order in enumerate(self.manifest["arm_orders"], 1):
                position = order.index("candidate")
                self.write_arm(
                    repetition,
                    "candidate",
                    position,
                    logical_bytes=1025,
                    candidate_gets=True,
                )

        cases["identical_logical_bytes"] = logical_bytes

        def p95_and_worst():
            for repetition, order in enumerate(self.manifest["arm_orders"], 1):
                position = order.index("candidate")
                self.write_arm(
                    repetition,
                    "candidate",
                    position,
                    latencies=[210.0] * 500,
                    candidate_gets=True,
                )

        cases["pooled_p95_below_limit"] = p95_and_worst

        def paired_repetitions():
            for repetition in (3, 4, 5):
                order = self.manifest["arm_orders"][repetition - 1]
                self.write_arm(
                    repetition,
                    "candidate",
                    order.index("candidate"),
                    latencies=[150.0 + repetition] * 500,
                    candidate_gets=True,
                )

        paired_key = (
            "paired_better_repetitions"
            if "required_better_paired_repetitions" in self.manifest
            else "paired_nonworse_repetitions"
        )
        cases[paired_key] = paired_repetitions

        def p50_regression():
            for repetition, order in enumerate(self.manifest["arm_orders"], 1):
                self.write_arm(
                    repetition,
                    "control",
                    order.index("control"),
                    latencies=[100.0] * 251 + [150.0] * 249,
                )
                self.write_arm(
                    repetition,
                    "candidate",
                    order.index("candidate"),
                    latencies=[106.0] * 251 + [120.0] * 249,
                    candidate_gets=True,
                )

        cases["maximum_pooled_p50_regression"] = p50_regression

        def get_amplification():
            for repetition, order in enumerate(self.manifest["arm_orders"], 1):
                self.write_arm(
                    repetition,
                    "candidate",
                    order.index("candidate"),
                    candidate_gets=False,
                )
                path = self.arm_path(repetition, "candidate") / "reads.csv"
                with path.open(newline="") as handle:
                    rows = list(csv.DictReader(handle))
                for row in rows:
                    row["gets"] = "3"
                    row["requests"] = "3"
                with path.open("w", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
                    writer.writeheader()
                    writer.writerows(rows)
                summary_path = self.arm_path(repetition, "candidate") / "summary.csv"
                with summary_path.open(newline="") as handle:
                    summary = list(csv.DictReader(handle))[0]
                summary["read_storage_gets"] = "1500"
                summary["read_storage_requests"] = "1500"
                with summary_path.open("w", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=summary.keys())
                    writer.writeheader()
                    writer.writerow(summary)
                trace_path = (
                    self.arm_path(repetition, "candidate") / "storage-access.csv"
                )
                with trace_path.open(newline="") as handle:
                    trace = list(csv.DictReader(handle))[0]
                trace["request_count"] = "1500"
                with trace_path.open("w", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=trace.keys())
                    writer.writeheader()
                    writer.writerow(trace)

        cases["maximum_get_amplification"] = get_amplification

        def backing_amplification():
            for repetition, order in enumerate(self.manifest["arm_orders"], 1):
                self.write_arm(
                    repetition,
                    "candidate",
                    order.index("candidate"),
                    candidate_gets=True,
                    backing_bytes=1300,
                )

        cases["maximum_backing_byte_amplification"] = backing_amplification

        for criterion, mutation in cases.items():
            with self.subTest(criterion=criterion):
                for child in self.root.iterdir():
                    if child.is_dir():
                        import shutil

                        shutil.rmtree(child)
                    else:
                        child.unlink()
                self.write_terminal_matrix()
                mutation()
                result = self.run_validator()
                if (
                    criterion == "identical_logical_bytes"
                    and "required_better_paired_repetitions" in self.manifest
                ):
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("paired logical bytes", result.stderr)
                    continue
                self.assertEqual(result.returncode, 0, result.stderr)
                report = json.loads(result.stdout)
                self.assertIsNone(report["winner"])
                self.assertFalse(report["selection_criteria"][criterion])
                if criterion == "pooled_p95_below_limit":
                    self.assertFalse(
                        report["selection_criteria"]["worst_repetition_p95_below_limit"]
                    )


class GlobalExactRerankHedgeValidatorTest(GlobalRangeHedgeValidatorTest):
    manifest_path = EXACT_MANIFEST

    def test_rejects_paired_logical_byte_redistribution(self):
        self.write_terminal_matrix()
        path = self.arm_path(2, "candidate") / "reads.csv"
        with path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["bytes_read"] = str(int(rows[0]["bytes_read"]) + 1)
        rows[1]["bytes_read"] = str(int(rows[1]["bytes_read"]) - 1)
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("paired logical bytes", result.stderr)

    def test_rejects_a_paired_hit_identity_change(self):
        self.write_terminal_matrix()
        path = self.arm_path(2, "candidate") / "reads.csv"
        with path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["hit_id"] = "other-id"
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("hit IDs", result.stderr)

    def test_rejects_a_nonfirst_paired_hit_identity_change(self):
        self.write_terminal_matrix()
        path = self.arm_path(2, "candidate") / "reads.csv"
        path.write_text(
            path.read_text().replace("|neighbor-0-1|", "|different-second|", 1)
        )
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ordered top-10", result.stderr)

    def test_rejects_storage_trace_request_or_byte_drift(self):
        for field in ("request_count", "bytes_fetched"):
            with self.subTest(field=field):
                self.write_terminal_matrix()
                path = self.arm_path(1, "control") / "storage-access.csv"
                with path.open(newline="") as handle:
                    row = list(csv.DictReader(handle))[0]
                row[field] = str(int(row[field]) + 1)
                with path.open("w", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=row.keys())
                    writer.writeheader()
                    writer.writerow(row)
                result = self.run_validator()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("storage trace", result.stderr)
                for child in self.root.iterdir():
                    if child.is_dir():
                        import shutil

                        shutil.rmtree(child)
                    else:
                        child.unlink()

    def test_manifest_only_preflight_accepts_frozen_exact_and_rejects_drift(self):
        accepted = subprocess.run(
            [
                "python3",
                str(VALIDATOR),
                "--manifest",
                str(EXACT_MANIFEST),
                "--validate-manifest-only",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(accepted.returncode, 0, accepted.stderr)

        changed = self.root / "changed-manifest.json"
        manifest = {**self.manifest, "comparison_contract": "weakened"}
        changed.write_text(json.dumps(manifest))
        rejected = subprocess.run(
            [
                "python3",
                str(VALIDATOR),
                "--manifest",
                str(changed),
                "--validate-manifest-only",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(rejected.returncode, 0)


if __name__ == "__main__":
    unittest.main()
