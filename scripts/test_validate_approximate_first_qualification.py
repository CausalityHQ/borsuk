import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import validate_approximate_first_qualification as validator
import validate_exact_candidate_frontier as candidate_validator


class ApproximateFirstQualificationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.manifest = {
            "protocol": "approximate-first-cohere-1m-local-v1",
            "artifact_schema_version": 1,
            "dataset": "cohere-medium-1M",
            "dataset_descriptor_sha256": "d" * 64,
            "queries": 20,
            "query_seed": 17,
            "k": 10,
            "scan_codec": "srht-pq-scan",
            "cache_execution": "scan",
            "cache_profile": "uncached",
            "nprobes": [32],
            "max_candidates": [4096],
            "control": "exact-rerank-control",
            "treatment": "approximate-first",
            "required_mean_recall_at_10": 0.95,
            "required_p05_query_recall_at_10": 0.80,
            "maximum_treatment_exact_vectors": 0,
            "maximum_treatment_exact_rerank_us": 0,
            "maximum_disk_cache_reads": 0,
            "minimum_backing_read_reduction_fraction": 0.50,
            "minimum_backing_byte_reduction_fraction": 0.25,
            "minimum_p95_latency_improvement_ms": 5.0,
            "maximum_one_sided_sign_test_p": 0.01,
            "maximum_treatment_p95_ms": 200.0,
        }
        self.manifest_path = self.root / "manifest.json"
        self.manifest_path.write_text(json.dumps(self.manifest))
        (self.root / "qualification_identity.json").write_text(
            json.dumps(
                {
                    "source_commit": "a" * 40,
                    "manifest_sha256": hashlib.sha256(self.manifest_path.read_bytes()).hexdigest(),
                    "dataset_descriptor_sha256": "d" * 64,
                    "binary_sha256": "b" * 64,
                    "source_tree_clean": True,
                    "origin_main_ancestor": True,
                }
            )
        )

    def tearDown(self):
        self.temporary.cleanup()

    def write_terminal(self, mutate=None):
        rows = []
        truth = [str(value) for value in range(10)]
        for sample in range(self.manifest["queries"]):
            row = {
                "schema_version": 1,
                "repetition_id": "local-r01",
                "query_seed": 17,
                "sample_index": sample,
                "query_source_index": sample,
                "arm_order": "control,treatment" if sample % 2 == 0 else "treatment,control",
                "scan_codec": "srht-pq-scan",
                "cache_execution": "scan",
                "nprobe": self.manifest["nprobes"][0],
                "max_candidates": self.manifest["max_candidates"][0],
                "ground_truth_ids": truth,
                "control": self.arm(
                    "exact-rerank-control",
                    100.0,
                    20,
                    2000,
                    self.manifest["max_candidates"][0],
                ),
                "treatment": self.arm("approximate-first", 50.0, 5, 1000, 0),
            }
            if mutate:
                mutate(row, sample)
            rows.append(row)
        artifact = self.root / "bench_approximate_first_pairs.jsonl"
        artifact.write_text("".join(json.dumps(row) + "\n" for row in rows))
        (self.root / "APPROXIMATE_FIRST_PAIRS_COMPLETE").write_text(
            f"schema_version=1\nrows={len(rows)}\n"
        )

    @staticmethod
    def arm(mode, latency, reads, backing_bytes, exact_vectors):
        return {
            "mode": mode,
            "ordered_ids": [str(value) for value in range(10)],
            "recall_at_10": 1.0,
            "latency_ms": latency,
            "execution_engine": mode,
            "storage_gets": reads,
            "storage_heads": 0,
            "backing_reads": reads,
            "backing_bytes_read": backing_bytes,
            "decoded_cache_hits": 0,
            "decoded_cache_bytes_read": 0,
            "disk_cache_reads": 0,
            "disk_cache_bytes_read": 0,
            "bytes_read": backing_bytes,
            "global_identity_rows_resolved": 20,
            "global_exact_vectors_fetched": exact_vectors,
            "global_base_approximate_us": 1000,
            "global_base_exact_rerank_us": 1000 if exact_vectors else 0,
            "global_delta_approximate_us": 0,
            "global_delta_exact_rerank_us": 0,
            "global_delta_wait_us": 0,
            "collection_resident_bytes": 100,
            "retained_bytes": 0,
            "retained_capacity_bytes": 0,
            "retained_peak_bytes": 0,
            "transient_bytes": 10,
            "transient_capacity_bytes": 20,
            "transient_peak_bytes": 30,
        }

    def test_accepts_complete_high_quality_pareto_point(self):
        self.write_terminal()
        decision = validator.validate(self.root, self.manifest_path)
        self.assertTrue(decision["accepted"])
        self.assertEqual(decision["selected"]["nprobe"], 32)

    def test_never_opens_jsonl_without_terminal_marker(self):
        (self.root / "bench_approximate_first_pairs.jsonl").write_text("not json\n")
        with self.assertRaisesRegex(validator.ValidationError, "completion marker"):
            validator.validate(self.root, self.manifest_path)

    def test_rejects_quality_loss(self):
        def lower_recall(row, sample):
            if sample == 0:
                row["treatment"]["ordered_ids"] = [str(value) for value in range(7)] + ["x", "y", "z"]
                row["treatment"]["recall_at_10"] = 0.7

        self.write_terminal(lower_recall)
        decision = validator.validate(self.root, self.manifest_path)
        self.assertFalse(decision["accepted"])
        self.assertIn("recall", " ".join(decision["points"][0]["failures"]))

    def test_rejects_hidden_exact_work(self):
        self.write_terminal(
            lambda row, sample: row["treatment"].update(global_exact_vectors_fetched=1)
        )
        decision = validator.validate(self.root, self.manifest_path)
        self.assertFalse(decision["accepted"])
        self.assertIn("exact vectors", " ".join(decision["points"][0]["failures"]))

    def test_validator_does_not_require_python_zip_strict(self):
        source = Path(validator.__file__).read_text()
        self.assertNotIn("zip(control_latency, treatment_latency, strict=True)", source)

    def test_completed_measurement_can_be_recovered_after_evaluator_failure(self):
        self.write_terminal()
        (self.root / "APPROXIMATE_FIRST_QUALIFICATION_FAILED").write_text(
            "exit=2\nreason=evaluator-error\n"
        )
        with self.assertRaisesRegex(validator.ValidationError, "failure marker"):
            validator.validate(self.root, self.manifest_path)
        decision = validator.validate(
            self.root,
            self.manifest_path,
            completed_after_evaluator_failure=True,
        )
        self.assertTrue(decision["accepted"])
        self.assertTrue(decision["recovery_mode"])

    def test_exact_candidate_frontier_selects_quality_width(self):
        self.manifest["maximum_control_p95_ms"] = 200.0
        self.manifest_path.write_text(json.dumps(self.manifest))
        identity_path = self.root / "qualification_identity.json"
        identity = json.loads(identity_path.read_text())
        identity["manifest_sha256"] = hashlib.sha256(
            self.manifest_path.read_bytes()
        ).hexdigest()
        identity_path.write_text(json.dumps(identity))
        self.write_terminal()
        decision = candidate_validator.validate(self.root, self.manifest_path)
        self.assertTrue(decision["accepted"])
        self.assertEqual(decision["selected"]["max_candidates"], 4096)

    def test_exact_candidate_frontier_rejects_bad_query_tail(self):
        self.manifest["maximum_control_p95_ms"] = 200.0
        self.manifest_path.write_text(json.dumps(self.manifest))
        identity_path = self.root / "qualification_identity.json"
        identity = json.loads(identity_path.read_text())
        identity["manifest_sha256"] = hashlib.sha256(
            self.manifest_path.read_bytes()
        ).hexdigest()
        identity_path.write_text(json.dumps(identity))

        def lower_control_recall(row, sample):
            if sample == 0:
                row["control"]["ordered_ids"] = [str(value) for value in range(7)] + [
                    "x",
                    "y",
                    "z",
                ]
                row["control"]["recall_at_10"] = 0.7

        self.write_terminal(lower_control_recall)
        decision = candidate_validator.validate(self.root, self.manifest_path)
        self.assertFalse(decision["accepted"])
        self.assertIn("control p05 recall", decision["points"][0]["failures"])


if __name__ == "__main__":
    unittest.main()
