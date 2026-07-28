import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.publication_protocol import (
    build_schedule,
    hardware_relation,
    validate_manifest,
)

ROOT = Path(__file__).resolve().parents[1]


def valid_manifest(**overrides):
    value = {
        "schema_version": 2,
        "campaign_id": "publication-v2-confirmatory",
        "campaign_kind": "confirmatory",
        "master_seed": 1701,
        "repetitions": 5,
        "repetition_ids": ["r01", "r02", "r03", "r04", "r05"],
        "queries_per_repetition": 1000,
        "publish_p99": True,
        "search_config_frozen": True,
        "result_prefix": "publication/v2/results",
        "index_prefix": "publication/v2/indexes",
        "cache_prefix": "publication-v2-cache",
        "direct_dataset": "fashion-mnist-784",
        "dense_datasets": ["fashion-mnist-784", "glove-100"],
        "hybrid_datasets": ["scifact", "nfcorpus", "fiqa"],
        "hybrid_dense_config": {
            "backend": "sentence-transformers",
            "model": "BAAI/bge-small-en-v1.5",
            "revision": "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a",
            "query_prefix": (
                "Represent this sentence for searching relevant passages: "
            ),
            "normalized": True,
        },
        "borsuk_measurement_profile": {
            "recall_only": True,
            "skip_exact_recall": True,
            "cache_phases": ["uncached", "disk_cached"],
        },
        "hybrid_search_config": {
            "modes": [
                "dense",
                "sparse",
                "text",
                "dense+sparse",
                "dense+text",
                "sparse+text",
                "dense+sparse+text",
            ],
            "candidate_depth": 256,
            "max_segments": 64,
            "hot_fractions": ["0", "0.5", "1"],
            "fusion": "rrf",
            "rrf_k": 60,
            "repetitions_per_campaign_repetition": 1,
        },
        "systems": ["borsuk", "amazon-s3-vectors"],
        "primary_metric": "p95_latency_ms",
        "recall_metric": "recall_at_10",
        "production_codec": "srht-pq-scan",
        "direct_search_config": {
            "leaf_mode": "srht-pq-scan",
            "nprobe": 8,
            "max_candidates": 320,
            "k": 10,
        },
        "hardware_policy": {
            "borsuk_must_be": "weaker-or-equal-or-identical-client",
            "direct_comparison_boundary": "same-client-instance",
            "borsuk_query_compute": "measured-client-instance",
            "amazon_s3_vectors_service_compute": "undisclosed",
            "permitted_hardware_wording": (
                "same measured client; managed service compute undisclosed"
            ),
            "required_fields": [
                "logical_cpus",
                "ram_bytes",
                "accelerator",
                "storage_class",
            ],
        },
        "execution_order_policy": {
            "direct_system_pair": "adjacent-and-counterbalanced",
            "dense_dataset_order": "seeded-per-repetition",
            "hybrid_dataset_order": "seeded-per-repetition",
            "sync_before_cleanup": True,
        },
    }
    value.update(overrides)
    return value


class PublicationProtocolTests(unittest.TestCase):
    def test_valid_manifest_builds_deterministic_independent_schedule(self):
        manifest = validate_manifest(valid_manifest())
        first = build_schedule(manifest)
        second = build_schedule(manifest)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 5)
        self.assertEqual(first[0]["system_order"], "borsuk amazon-s3-vectors")
        self.assertEqual(first[1]["system_order"], "amazon-s3-vectors borsuk")
        self.assertEqual(
            set(first[0]["dense_dataset_order"].split()),
            set(manifest["dense_datasets"]),
        )
        self.assertEqual(
            set(first[0]["hybrid_dataset_order"].split()),
            set(manifest["hybrid_datasets"]),
        )
        self.assertNotIn("dataset_order", first[0])
        self.assertEqual(len({row["result_prefix"] for row in first}), 5)
        self.assertEqual(len({row["index_prefix"] for row in first}), 5)
        self.assertEqual(len({row["cache_key"] for row in first}), 5)
        self.assertTrue(all("pilot" not in json.dumps(row).lower() for row in first))

    def test_invalid_confirmatory_manifests_are_rejected(self):
        cases = [
            valid_manifest(campaign_kind="pilot"),
            valid_manifest(repetitions=2, repetition_ids=["r01", "r02"]),
            valid_manifest(queries_per_repetition=999),
            valid_manifest(search_config_frozen=False),
            valid_manifest(repetition_ids=["r01", "r01", "r03", "r04", "r05"]),
            valid_manifest(production_codec="pq-scan"),
            valid_manifest(hardware_policy={}),
            valid_manifest(execution_order_policy={}),
            valid_manifest(hybrid_dense_config={}),
            valid_manifest(borsuk_measurement_profile={}),
            valid_manifest(hybrid_search_config={}),
        ]
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaises(ValueError):
                    validate_manifest(case)

    def test_hardware_relation_requires_comparable_complete_fields(self):
        weak = {
            "logical_cpus": 8,
            "ram_bytes": 16,
            "accelerator": "none",
            "storage_class": "network-object",
        }
        strong = {**weak, "logical_cpus": 16, "ram_bytes": 32}
        self.assertEqual(hardware_relation(weak, strong), "weaker-or-equal")
        self.assertEqual(hardware_relation(strong, weak), "stronger-or-incomparable")
        self.assertEqual(hardware_relation({}, strong), "unknown")

    def test_cli_validates_and_writes_stable_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest_path = Path(directory) / "manifest.json"
            output = Path(directory) / "schedule.csv"
            manifest_path.write_text(json.dumps(valid_manifest()), encoding="utf-8")
            validated = subprocess.run(
                [
                    "python3",
                    str(ROOT / "scripts/publication_protocol.py"),
                    "validate",
                    str(manifest_path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("valid confirmatory manifest", validated.stdout)
            subprocess.run(
                [
                    "python3",
                    str(ROOT / "scripts/publication_protocol.py"),
                    "schedule",
                    str(manifest_path),
                    "--output",
                    str(output),
                ],
                check=True,
            )
            with output.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertNotIn(b"\r\n", output.read_bytes())
            self.assertEqual(len(rows), 5)
            self.assertEqual(rows[0]["repetition_id"], "r01")


if __name__ == "__main__":
    unittest.main()
