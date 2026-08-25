import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.publication_v3_protocol import (
    build_schedule_document,
    canonical_json_bytes,
    index_id,
    manifest_is_paid_ready,
    read_protocol,
    validate_manifest,
    validate_schedule_cell,
    validate_schedule_for_manifest,
    write_protocol,
)


def unstaged_source(url: str) -> dict[str, object]:
    return {
        "state": "unstaged",
        "expected_source": url,
        "license": "research-use",
    }


def valid_v3_manifest(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": 3,
        "campaign_id": "publication-v3-20260812",
        "campaign_kind": "publication",
        "master_seed": 1701,
        "repetitions": 5,
        "queries_per_repetition": 1000,
        "publish_p99": True,
        "source": {"state": "unfrozen"},
        "environment_contract": {
            "aws_account": "453182569524",
            "region": "eu-central-1",
            "architecture": "aarch64",
            "spot_default": True,
            "build_workers": {
                "borsuk": {
                    "instance_type": "r7g.8xlarge",
                    "vcpus": 32,
                    "memory_mib": 262144,
                },
                "amazon-s3-vectors": {
                    "instance_type": "r7g.8xlarge",
                    "vcpus": 32,
                    "memory_mib": 262144,
                },
                "faiss": {
                    "instance_type": "r7g.16xlarge",
                    "vcpus": 64,
                    "memory_mib": 524288,
                },
            },
            "runtime_clients": {
                "borsuk": {
                    "instance_type": "c7g.xlarge",
                    "vcpus": 4,
                    "memory_mib": 8192,
                    "resident_limit_mib": 2048,
                    "disk_cache_limit_mib": 1024,
                },
                "amazon-s3-vectors": {
                    "instance_type": "c7g.xlarge",
                    "vcpus": 4,
                    "memory_mib": 8192,
                    "resident_limit_mib": 2048,
                    "disk_cache_limit_mib": 1024,
                },
                "faiss": {
                    "instance_type": "c7g.2xlarge",
                    "vcpus": 8,
                    "memory_mib": 16384,
                    "resident_limit_mib": 12288,
                    "disk_cache_limit_mib": 1024,
                },
            },
            "build_storage": {
                "volume_type": "gp3",
                "volume_size_gib": 4096,
                "iops": 16000,
                "throughput_mib_s": 1000,
            },
            "runtime_storage": {
                "volume_type": "gp3",
                "volume_size_gib": 32,
                "iops": 3000,
                "throughput_mib_s": 125,
            },
            "runtime_data_contract": {
                "index_location": "s3",
                "dataset_location": "s3",
                "allow_local_corpus": False,
                "allow_local_index": False,
            },
            "interruption_contract": {
                "measurement_unit": "factor-arm",
                "sync_terminal_repetitions": True,
                "discard_interrupted_measurement_cell": True,
            },
        },
        "prefixes": {
            "result": "s3://borsuk-publication-test/publication/v3/20260812/results",
            "index": "s3://borsuk-publication-test/publication/v3/20260812/indexes",
            "dataset": "s3://borsuk-publication-test/publication/v3/20260812/datasets",
            "cache": "publication-v3-20260812-cache",
        },
        "index_profiles": {
            "borsuk": {
                "engine": "borsuk-v20",
                "logical_cells": 16384,
                "minimum_rows_per_logical_cell": 256,
                "training_rows_per_cell": 32,
                "training_iterations": 8,
                "routing": "hnsw",
                "hnsw_m": 32,
                "hnsw_ef_construction": 200,
                "global_scan_codec": "srht-pq-scan",
                "code_bytes": 128,
                "bundle_target_mib": 64,
                "row_group_target_mib": 8,
                "max_active_levels": 16,
            },
            "amazon-s3-vectors": {"engine": "amazon-s3-vectors-managed"},
            "faiss": {
                "engine": "faiss-ivf-flat",
                "nlist": 16384,
                "training_rows": 1000000,
                "minimum_training_rows_per_centroid": 39,
            },
        },
        "budget_contract": {
            "max_campaign_usd_micros": 50000000000,
            "max_cell_seconds": 7200,
            "max_index_build_seconds": 28800,
            "terminate_on_terminal_marker": True,
        },
        "datasets": [
            {
                "id": "fashion-mnist-784",
                "kind": "standard-ann",
                "scale": {"state": "exact", "rows": 60_000},
                "dimensions": 784,
                "metric": "l2",
                "source": unstaged_source(
                    "https://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5"
                ),
            },
            {
                "id": "synthetic-clustered-1m-768",
                "kind": "synthetic-dense",
                "scale": {"state": "exact", "rows": 1_000_000},
                "dimensions": 768,
                "metric": "cosine",
                "source": {
                    "state": "generated",
                    "generator": "synthetic-clustered-v1",
                    "seed": 4441,
                },
            },
            {
                "id": "scifact",
                "kind": "beir-hybrid",
                "scale": {"state": "exact", "rows": 5_183},
                "dimensions": 384,
                "metric": "cosine",
                "source": unstaged_source(
                    "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip"
                ),
            },
        ],
        "workloads": [
            {
                "id": "dense-read",
                "kind": "read-recall",
                "dataset_ids": [
                    "fashion-mnist-784",
                    "synthetic-clustered-1m-768",
                ],
                "systems": ["borsuk", "amazon-s3-vectors", "faiss"],
                "factors": {
                    "k": [10],
                    "leaf_page_budgets": [4, 32],
                    "cache_states": ["cold", "warm"],
                    "minimum_recall_ppm": 950000,
                },
            },
            {
                "id": "durable-lifecycle",
                "kind": "write-update-delete-compact",
                "dataset_ids": ["synthetic-clustered-1m-768"],
                "systems": ["borsuk"],
                "factors": {
                    "writers": [1, 4, 16],
                    "batch_sizes": [1, 64, 1024],
                    "insert_modes": ["general-upsert", "claim-free-put"],
                    "update_percent": [10],
                    "delete_percent": [10],
                    "minimum_lifecycle_accuracy_ppm": 1_000_000,
                },
            },
            {
                "id": "hybrid-read",
                "kind": "sparse-text-dense-hybrid",
                "dataset_ids": ["scifact"],
                "systems": ["borsuk"],
                "factors": {
                    "modes": ["dense", "sparse", "text", "dense-sparse-text"],
                    "candidate_depths": [64, 256],
                    "fusion": "rrf",
                    "rrf_k": 60,
                    "minimum_ndcg_ppm": 900000,
                },
            },
        ],
        "systems": ["borsuk", "amazon-s3-vectors", "faiss"],
    }
    value.update(overrides)
    return value


def paid_v3_manifest() -> dict[str, object]:
    value = json.loads(json.dumps(valid_v3_manifest()))
    value["source"] = {
        "state": "frozen",
        "git_commit": "1" * 40,
        "archive_sha256": "2" * 64,
        "cargo_lock_sha256": "3" * 64,
        "python_lock_sha256": "4" * 64,
        "node_lock_sha256": "5" * 64,
    }
    for dataset in value["datasets"]:
        source = dataset["source"]
        if source["state"] == "unstaged":
            dataset["source"] = {
                "state": "staged",
                "url": source["expected_source"],
                "sha256": "d" * 64,
                "license": source["license"],
            }
    return value


def paid_ready_v3_manifest() -> dict[str, object]:
    """Return a frozen fixture whose generated recipes have durable authority."""

    value = paid_v3_manifest()
    for dataset in value["datasets"]:
        source = dataset["source"]
        if source["state"] != "generated":
            continue
        attempt_root = f"{value['prefixes']['dataset']}/{dataset['id']}/attempts/0001"
        dataset["source"] = {
            "state": "staged-generated",
            "generator": source["generator"],
            "seed": source["seed"],
            "generator_source_archive_sha256": "a" * 64,
            "url": f"{attempt_root}/materialized",
            "sha256": "b" * 64,
            "receipt_uri": f"{attempt_root}/STAGING_COMPLETE.json",
            "receipt_sha256": "c" * 64,
        }
    return value


class PublicationV3ProtocolTests(unittest.TestCase):
    def test_read_workload_rejects_non_v20_leaf_page_budget(self) -> None:
        manifest = valid_v3_manifest()
        workload = next(
            item for item in manifest["workloads"] if item["kind"] == "read-recall"
        )
        workload["factors"]["leaf_page_budgets"] = [128]
        with self.assertRaisesRegex(ValueError, "leaf page budget"):
            validate_manifest(manifest)

    def test_index_identity_is_reusable_but_s3_authority_is_mandatory(self) -> None:
        manifest = validate_manifest(valid_v3_manifest())
        cells = build_schedule_document(manifest)["cells"]
        first = cells[0]
        reusable = next(
            cell
            for cell in cells
            if cell["system"] == first["system"]
            and cell["dataset"] == first["dataset"]
            and cell["index_profile"] == first["index_profile"]
            and cell["workload"]["kind"] == first["workload"]["kind"]
            and cell["repetition_id"] != first["repetition_id"]
        )
        self.assertEqual(index_id(first), index_id(reusable))

        invalid = valid_v3_manifest()
        invalid["prefixes"]["index"] = "/tmp/local-index"
        with self.assertRaisesRegex(ValueError, "index prefix must be an S3 URI"):
            validate_manifest(invalid)

    def test_borsuk_profile_rejects_non_power_of_two_code_width(self) -> None:
        manifest = paid_v3_manifest()
        manifest["index_profiles"]["borsuk"]["code_bytes"] = 96
        with self.assertRaisesRegex(ValueError, "power of two"):
            validate_manifest(manifest)

    def test_borsuk_turboquant_profile_uses_typed_codec_parameters(self) -> None:
        manifest = paid_v3_manifest()
        profile = manifest["index_profiles"]["borsuk"]
        profile["global_scan_codec"] = "fast-turboquant-scan"
        profile.pop("code_bytes")
        profile.update(
            {
                "turboquant_bits": 3,
                "turboquant_qjl_bits": 0,
                "turboquant_shards": 1,
            }
        )

        validated = validate_manifest(manifest)["index_profiles"]["borsuk"]

        self.assertEqual(validated["global_scan_codec"], "fast-turboquant-scan")
        self.assertEqual(validated["turboquant_bits"], 3)
        self.assertEqual(validated["turboquant_qjl_bits"], 0)
        self.assertEqual(validated["turboquant_shards"], 1)
        self.assertNotIn("code_bytes", validated)

        profile["code_bytes"] = 64
        with self.assertRaisesRegex(ValueError, "fields differ"):
            validate_manifest(manifest)
        profile.pop("code_bytes")
        profile["turboquant_qjl_bits"] = 16
        with self.assertRaisesRegex(ValueError, "does not accept.*QJL"):
            validate_manifest(manifest)

        profile["turboquant_qjl_bits"] = 0
        profile["global_scan_codec"] = "fast-turboquant-mse-scan"
        profile["turboquant_bits"] = 1
        profile["turboquant_shards"] = 3
        validated_mse = validate_manifest(manifest)["index_profiles"]["borsuk"]
        self.assertEqual(validated_mse["global_scan_codec"], "fast-turboquant-mse-scan")
        self.assertEqual(validated_mse["turboquant_bits"], 1)
        self.assertEqual(validated_mse["turboquant_shards"], 3)

    def test_borsuk_profile_rejects_unknown_global_scan_codec_before_scheduling(
        self,
    ) -> None:
        manifest = paid_v3_manifest()
        manifest["index_profiles"]["borsuk"]["global_scan_codec"] = (
            "fast-turboquant-prod-scan"
        )
        with self.assertRaisesRegex(ValueError, "global scan codec is unsupported"):
            validate_manifest(manifest)

    def test_protocol_is_exact_scheduled_cell_without_reconstruction(self) -> None:
        cell = build_schedule_document(validate_manifest(valid_v3_manifest()))["cells"][
            0
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "protocol.json"
            write_protocol(path, cell)
            self.assertEqual(path.read_bytes(), canonical_json_bytes(cell) + b"\n")
            self.assertEqual(read_protocol(path), cell)

            path.write_text(json.dumps(cell, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "canonical"):
                read_protocol(path)

    def test_schedule_cells_are_canonical_typed_and_alias_free(self) -> None:
        manifest = validate_manifest(valid_v3_manifest())
        first = build_schedule_document(manifest)
        second = build_schedule_document(manifest)
        self.assertEqual(first, second)
        self.assertEqual(
            set(first),
            {"schema_version", "campaign_id", "manifest_sha256", "cells"},
        )
        manifest_sha256 = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
        self.assertEqual(first["manifest_sha256"], manifest_sha256)
        self.assertEqual(
            [row["cell_id"] for row in first["cells"]],
            sorted(row["cell_id"] for row in first["cells"]),
        )
        self.assertEqual(canonical_json_bytes(first), canonical_json_bytes(second))
        self.assertEqual(len(first["cells"]), 5 * (6 + 1 + 1))
        for row in first["cells"]:
            self.assertEqual(
                set(row),
                {
                    "schema_version",
                    "campaign_id",
                    "manifest_sha256",
                    "cell_id",
                    "repetition_id",
                    "system",
                    "index_profile",
                    "query_seed",
                    "queries_per_repetition",
                    "dataset",
                    "workload",
                    "source",
                    "environment_contract",
                    "result_prefix",
                    "index_prefix",
                    "dataset_prefix",
                    "cache_key",
                },
            )
            self.assertNotIn("result_key", row)
            self.assertNotIn("index_key", row)
            self.assertEqual(row["manifest_sha256"], manifest_sha256)
            self.assertEqual(
                len(
                    {
                        row["result_prefix"],
                        row["index_prefix"],
                        row["dataset_prefix"],
                    }
                ),
                3,
            )
            self.assertEqual(validate_schedule_cell(row), row)

        index_groups: dict[tuple[str, str, str], set[str]] = {}
        for row in first["cells"]:
            key = (row["workload"]["id"], row["dataset"]["id"], row["system"])
            index_groups.setdefault(key, set()).add(row["index_prefix"])
        self.assertTrue(index_groups)
        self.assertTrue(all(len(prefixes) == 1 for prefixes in index_groups.values()))
        self.assertEqual(
            len({row["result_prefix"] for row in first["cells"]}),
            len(first["cells"]),
        )

        moved = valid_v3_manifest(
            prefixes={
                **valid_v3_manifest()["prefixes"],
                "result": "s3://borsuk-publication-test/publication/v3/20260812-moved/results",
            }
        )
        moved_schedule = build_schedule_document(validate_manifest(moved))
        self.assertNotEqual(first["manifest_sha256"], moved_schedule["manifest_sha256"])
        self.assertEqual(
            [row["cell_id"] for row in first["cells"]],
            [row["cell_id"] for row in moved_schedule["cells"]],
        )
        self.assertNotEqual(
            first["cells"][0]["result_prefix"],
            moved_schedule["cells"][0]["result_prefix"],
        )
        retuned = valid_v3_manifest()
        retuned["index_profiles"]["borsuk"]["hnsw_m"] = 48
        retuned_schedule = build_schedule_document(validate_manifest(retuned))
        self.assertTrue(
            {
                row["index_prefix"]
                for row in first["cells"]
                if row["system"] == "borsuk"
            }.isdisjoint(
                {
                    row["index_prefix"]
                    for row in retuned_schedule["cells"]
                    if row["system"] == "borsuk"
                }
            )
        )
        analysis_only = valid_v3_manifest()
        analysis_only["workloads"][0]["factors"]["minimum_recall_ppm"] = 900000
        analysis_schedule = build_schedule_document(validate_manifest(analysis_only))
        self.assertEqual(
            {row["index_prefix"] for row in first["cells"]},
            {row["index_prefix"] for row in analysis_schedule["cells"]},
        )
        different_hardware = valid_v3_manifest(
            environment_contract={
                **valid_v3_manifest()["environment_contract"],
                "runtime_clients": {
                    **valid_v3_manifest()["environment_contract"]["runtime_clients"],
                    "borsuk": {
                        **valid_v3_manifest()["environment_contract"][
                            "runtime_clients"
                        ]["borsuk"],
                        "instance_type": "c7g.large",
                        "vcpus": 2,
                        "memory_mib": 4096,
                    },
                },
            }
        )
        hardware_schedule = build_schedule_document(
            validate_manifest(different_hardware)
        )
        self.assertNotEqual(
            first["cells"][0]["cell_id"], hardware_schedule["cells"][0]["cell_id"]
        )

    def test_environment_separates_builders_from_bounded_s3_query_clients(self) -> None:
        environment = validate_manifest(valid_v3_manifest())["environment_contract"]
        self.assertEqual(environment["build_workers"]["borsuk"]["memory_mib"], 262144)
        self.assertEqual(environment["runtime_clients"]["borsuk"]["memory_mib"], 8192)
        self.assertLessEqual(
            environment["runtime_clients"]["borsuk"]["resident_limit_mib"], 2048
        )
        self.assertEqual(environment["runtime_storage"]["volume_size_gib"], 32)
        self.assertFalse(environment["runtime_data_contract"]["allow_local_corpus"])
        self.assertFalse(environment["runtime_data_contract"]["allow_local_index"])

        for mutation, error in (
            (("runtime_clients", "borsuk", "resident_limit_mib", 8192), "resident"),
            (("runtime_clients", "borsuk", "disk_cache_limit_mib", 4096), "cache"),
            (("runtime_storage", "volume_size_gib", None, 256), "runtime storage"),
            (("runtime_data_contract", "allow_local_index", None, True), "S3-only"),
        ):
            manifest = valid_v3_manifest()
            section, key, nested, value = mutation
            if nested is None:
                manifest["environment_contract"][section][key] = value
            else:
                manifest["environment_contract"][section][key][nested] = value
            with self.subTest(error=error), self.assertRaisesRegex(ValueError, error):
                validate_manifest(manifest)

    def test_v2_aliases_and_unknown_fields_are_rejected(self) -> None:
        row = build_schedule_document(validate_manifest(valid_v3_manifest()))["cells"][
            0
        ]
        bad_rows = (
            {**row, "result_key": row["result_prefix"]},
            {**row, "index_key": row["index_prefix"]},
            {**row, "extra": 1},
        )
        for bad in bad_rows:
            with self.subTest(fields=sorted(bad)):
                with self.assertRaises(ValueError):
                    validate_schedule_cell(bad)

    def test_schedule_is_verified_byte_for_byte_against_its_manifest(self) -> None:
        manifest = validate_manifest(valid_v3_manifest())
        schedule = build_schedule_document(manifest)
        self.assertEqual(validate_schedule_for_manifest(schedule, manifest), schedule)
        tampered = json.loads(json.dumps(schedule))
        tampered["cells"][0]["query_seed"] += 1
        with self.assertRaisesRegex(ValueError, "canonical schedule"):
            validate_schedule_for_manifest(tampered, manifest)
        moved_manifest = validate_manifest(valid_v3_manifest(master_seed=1702))
        with self.assertRaisesRegex(ValueError, "canonical schedule"):
            validate_schedule_for_manifest(schedule, moved_manifest)

    def test_manifest_rejects_unknown_fields_duplicates_and_invalid_source(
        self,
    ) -> None:
        cases = (
            {**valid_v3_manifest(), "unknown": True},
            valid_v3_manifest(systems=["borsuk", "borsuk"]),
            valid_v3_manifest(source={"state": "frozen", "git_commit": "bad"}),
        )
        for case in cases:
            with self.subTest(case=json.dumps(case, sort_keys=True)[:160]):
                with self.assertRaises(ValueError):
                    validate_manifest(case)

    def test_staged_generated_source_preserves_recipe_and_receipt_authority(
        self,
    ) -> None:
        manifest = valid_v3_manifest()
        generated = next(
            dataset
            for dataset in manifest["datasets"]
            if dataset["source"]["state"] == "generated"
        )
        attempt_root = (
            f"{manifest['prefixes']['dataset']}/{generated['id']}/attempts/0001"
        )
        generated["source"] = {
            "state": "staged-generated",
            "generator": "synthetic-clustered-v1",
            "seed": 4441,
            "generator_source_archive_sha256": "a" * 64,
            "url": f"{attempt_root}/materialized",
            "sha256": "b" * 64,
            "receipt_uri": f"{attempt_root}/STAGING_COMPLETE.json",
            "receipt_sha256": "c" * 64,
        }
        validated = validate_manifest(manifest)
        self.assertEqual(
            next(
                dataset
                for dataset in validated["datasets"]
                if dataset["id"] == generated["id"]
            )["source"],
            generated["source"],
        )
        for field in ("generator", "receipt_sha256"):
            malformed = json.loads(json.dumps(manifest))
            del next(
                dataset
                for dataset in malformed["datasets"]
                if dataset["id"] == generated["id"]
            )["source"][field]
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_manifest(malformed)

    def test_generated_contract_rejects_unsupported_or_unbounded_paid_recipes(
        self,
    ) -> None:
        for mutation, error in (
            (("source", "generator", "synthetic-unknown-v1"), "generator"),
            (("metric", None, "l2"), "generator.*metric"),
            (("scale", "rows", 1_000_001), "groups of 100"),
            (("dimensions", None, 2), "code capacity"),
            (("scale", "rows", 2_000_000_000), "object bound"),
        ):
            manifest = valid_v3_manifest()
            dataset = manifest["datasets"][1]
            section, field, value = mutation
            if field is None:
                dataset[section] = value
            else:
                dataset[section][field] = value
            with (
                self.subTest(mutation=mutation),
                self.assertRaisesRegex(ValueError, error),
            ):
                validate_manifest(manifest)

        manifest = valid_v3_manifest()
        dataset = manifest["datasets"][1]
        dataset["kind"] = "synthetic-binary"
        dataset["metric"] = "hamming"
        dataset["scale"]["rows"] = 1_000_000_000
        dataset["dimensions"] = 25
        dataset["source"]["generator"] = "synthetic-binary-v1"
        with self.assertRaisesRegex(ValueError, "truth margin"):
            validate_manifest(manifest)

        manifest = valid_v3_manifest()
        dataset = manifest["datasets"][1]
        source = dataset["source"]
        attempt_root = (
            f"{manifest['prefixes']['dataset']}/{dataset['id']}/attempts/0001"
        )
        dataset["source"] = {
            "state": "staged-generated",
            "generator": source["generator"],
            "seed": source["seed"],
            "generator_source_archive_sha256": "a" * 64,
            "url": f"{attempt_root}/materialized?foreign=true",
            "sha256": "b" * 64,
            "receipt_uri": f"{attempt_root}/STAGING_COMPLETE.json",
            "receipt_sha256": "c" * 64,
        }
        with self.assertRaisesRegex(ValueError, "canonical dataset staging path"):
            validate_manifest(manifest)

    def test_paid_ready_requires_generated_recipes_to_be_staged(self) -> None:
        manifest = paid_v3_manifest()
        self.assertFalse(manifest_is_paid_ready(validate_manifest(manifest)))
        for dataset in manifest["datasets"]:
            source = dataset["source"]
            if source["state"] != "generated":
                continue
            dataset["source"] = {
                "state": "staged-generated",
                "generator": source["generator"],
                "seed": source["seed"],
                "generator_source_archive_sha256": "a" * 64,
                "url": (
                    f"{manifest['prefixes']['dataset']}/{dataset['id']}/"
                    "attempts/0001/materialized"
                ),
                "sha256": "b" * 64,
                "receipt_uri": (
                    f"{manifest['prefixes']['dataset']}/{dataset['id']}/"
                    "attempts/0001/STAGING_COMPLETE.json"
                ),
                "receipt_sha256": "c" * 64,
            }
        self.assertTrue(manifest_is_paid_ready(validate_manifest(manifest)))

    def test_numerically_equal_integer_fields_have_one_identity(self) -> None:
        integer_manifest = valid_v3_manifest()
        float_manifest = json.loads(json.dumps(integer_manifest))
        float_manifest["datasets"][1]["scale"]["rows"] = 1_000_000.0
        float_manifest["datasets"][1]["source"]["seed"] = 4441.0
        self.assertEqual(
            canonical_json_bytes(validate_manifest(integer_manifest)),
            canonical_json_bytes(validate_manifest(float_manifest)),
        )

    def test_datatype_dimensions_must_exist_in_selected_datasets(self) -> None:
        manifest = valid_v3_manifest()
        manifest["workloads"].append(
            {
                "id": "bad-datatype-matrix",
                "kind": "datatype-simd-matrix",
                "dataset_ids": ["synthetic-clustered-1m-768"],
                "systems": ["borsuk"],
                "factors": {
                    "datatypes": ["f32"],
                    "dimensions": [384],
                    "kernels": ["simd"],
                    "k": [10],
                    "minimum_recall_ppm": 950000,
                },
            }
        )
        with self.assertRaisesRegex(ValueError, "dimensions without datasets"):
            validate_manifest(manifest)

    def test_profiles_are_feasible_for_each_dataset_scale(self) -> None:
        schedule = build_schedule_document(validate_manifest(valid_v3_manifest()))
        fashion = [
            cell
            for cell in schedule["cells"]
            if cell["dataset"]["id"] == "fashion-mnist-784"
        ]
        self.assertTrue(fashion)
        for cell in fashion:
            rows = cell["dataset"]["scale"]["rows"]
            profile = cell["index_profile"]
            if cell["system"] == "borsuk":
                self.assertLessEqual(
                    profile["logical_cells"] * profile["minimum_rows_per_logical_cell"],
                    rows,
                )
            elif cell["system"] == "faiss":
                self.assertLessEqual(profile["training_rows"], rows)
                self.assertGreaterEqual(
                    profile["training_rows"],
                    profile["nlist"] * profile["minimum_training_rows_per_centroid"],
                )

    def test_layout_build_factors_change_the_effective_index_profile(self) -> None:
        manifest = valid_v3_manifest()
        manifest["workloads"].append(
            {
                "id": "layout-256",
                "kind": "storage-layout-audit",
                "dataset_ids": ["synthetic-clustered-1m-768"],
                "systems": ["borsuk"],
                "factors": {
                    "bundle_target_mib": 256,
                    "row_group_target_mib": 16,
                    "max_active_levels": 8,
                    "require_multiple_data_bundles": True,
                },
            }
        )
        cells = build_schedule_document(validate_manifest(manifest))["cells"]
        layout = [cell for cell in cells if cell["workload"]["id"] == "layout-256"]
        self.assertTrue(layout)
        self.assertTrue(
            all(cell["index_profile"]["bundle_target_mib"] == 256 for cell in layout)
        )
        dense_prefixes = {
            cell["index_prefix"]
            for cell in cells
            if cell["workload"]["id"] == "dense-read"
            and cell["dataset"]["id"] == "synthetic-clustered-1m-768"
            and cell["system"] == "borsuk"
        }
        self.assertTrue(
            dense_prefixes.isdisjoint({cell["index_prefix"] for cell in layout})
        )

    def test_datatype_manifest_rejects_empty_or_synthesized_projection_arms(
        self,
    ) -> None:
        binary_only = valid_v3_manifest()
        binary_only["workloads"].append(
            {
                "id": "binary-only-on-float",
                "kind": "datatype-simd-matrix",
                "dataset_ids": ["synthetic-clustered-1m-768"],
                "systems": ["borsuk"],
                "factors": {
                    "datatypes": ["binary"],
                    "dimensions": [768],
                    "kernels": ["simd"],
                    "k": [10],
                    "minimum_recall_ppm": 950000,
                },
            }
        )
        with self.assertRaisesRegex(ValueError, "compatible datatype"):
            validate_manifest(binary_only)

        undeclared_binary = valid_v3_manifest()
        undeclared_binary["datasets"].append(
            {
                "id": "binary-768",
                "kind": "synthetic-binary",
                "scale": {"state": "exact", "rows": 1_000_000},
                "dimensions": 768,
                "metric": "hamming",
                "source": {
                    "state": "generated",
                    "generator": "synthetic-binary-v1",
                    "seed": 812,
                },
            }
        )
        undeclared_binary["workloads"].append(
            {
                "id": "undeclared-binary",
                "kind": "datatype-simd-matrix",
                "dataset_ids": ["binary-768"],
                "systems": ["borsuk"],
                "factors": {
                    "datatypes": ["f32", "int8"],
                    "dimensions": [768],
                    "kernels": ["simd"],
                    "k": [10],
                    "minimum_recall_ppm": 950000,
                },
            }
        )
        with self.assertRaisesRegex(ValueError, "compatible datatype"):
            validate_manifest(undeclared_binary)

    def test_release_manifest_covers_realistic_standard_and_100m_datasets(self) -> None:
        path = Path("docs/research/publication-v3-manifest.json")
        manifest = validate_manifest(json.loads(path.read_text(encoding="utf-8")))
        profile = manifest["index_profiles"]["borsuk"]
        self.assertEqual(profile["engine"], "borsuk-v20")
        self.assertEqual(profile["global_scan_codec"], "fast-turboquant-scan")
        self.assertEqual(profile["turboquant_bits"], 8)
        self.assertEqual(profile["turboquant_qjl_bits"], 0)
        self.assertEqual(profile["turboquant_shards"], 1)
        self.assertNotIn("code_bytes", profile)
        read_workloads = [
            workload
            for workload in manifest["workloads"]
            if workload["kind"] == "read-recall"
        ]
        self.assertTrue(read_workloads)
        self.assertTrue(
            all(
                workload["factors"]["leaf_page_budgets"] == [64]
                for workload in read_workloads
            )
        )
        self.assertTrue(
            all(
                workload["factors"]["minimum_recall_ppm"] == 950_000
                for workload in read_workloads
            )
        )
        dataset_ids = {dataset["id"] for dataset in manifest["datasets"]}
        self.assertTrue(
            {
                "deep-image-96",
                "fashion-mnist-784",
                "gist-960",
                "glove-100",
                "nytimes-256",
                "sift-128",
                "cohere-medium-1m-768",
                "cohere-large-10m-768",
                "laion-100m-768",
                "synthetic-clustered-100m-768",
                "synthetic-uniform-100m-768",
                "synthetic-binary-1m-768",
                "scifact",
                "nfcorpus",
                "fiqa",
            }.issubset(dataset_ids)
        )
        workload_kinds = {workload["kind"] for workload in manifest["workloads"]}
        self.assertTrue(
            {
                "read-recall",
                "write-update-delete-compact",
                "sparse-text-dense-hybrid",
                "filtered-namespace-read",
                "late-interaction-read",
                "datatype-simd-matrix",
                "storage-layout-audit",
            }.issubset(workload_kinds)
        )
        by_kind = {workload["kind"]: workload for workload in manifest["workloads"]}
        self.assertEqual(
            by_kind["datatype-simd-matrix"]["factors"]["datatypes"],
            ["f32", "f16", "bf16", "float8-e4m3", "float8-e5m2", "int8", "binary"],
        )
        self.assertEqual(
            by_kind["datatype-simd-matrix"]["factors"]["dimensions"],
            [384, 768, 1536],
        )
        self.assertTrue(
            {
                "synthetic-clustered-1m-384",
                "synthetic-clustered-1m-768",
                "synthetic-clustered-1m-1536",
                "synthetic-binary-1m-768",
            }.issubset(by_kind["datatype-simd-matrix"]["dataset_ids"])
        )
        self.assertTrue(
            by_kind["storage-layout-audit"]["factors"]["require_multiple_data_bundles"]
        )
        self.assertEqual(
            by_kind["write-update-delete-compact"]["factors"]["writers"],
            [1, 4, 16],
        )
        self.assertEqual(
            by_kind["write-update-delete-compact"]["factors"]["insert_modes"],
            ["general-upsert", "claim-free-put"],
        )
        self.assertEqual(
            by_kind["write-update-delete-compact"]["factors"]["update_percent"],
            [10],
        )
        self.assertEqual(
            by_kind["write-update-delete-compact"]["factors"]["delete_percent"],
            [10],
        )
        self.assertTrue(
            {
                "cohere-large-10m-768",
                "synthetic-clustered-100m-768",
            }.issubset(by_kind["write-update-delete-compact"]["dataset_ids"])
        )
        self.assertIn(
            "amazon-s3-vectors",
            by_kind["write-update-delete-compact"]["systems"],
        )
        self.assertEqual(
            by_kind["filtered-namespace-read"]["factors"]["minimum_recall_ppm"],
            950000,
        )
        self.assertEqual(
            by_kind["late-interaction-read"]["factors"]["minimum_ndcg_ppm"],
            900000,
        )
        self.assertEqual(
            by_kind["sparse-text-dense-hybrid"]["factors"]["minimum_ndcg_ppm"],
            900000,
        )
        self.assertEqual(
            by_kind["datatype-simd-matrix"]["factors"]["minimum_recall_ppm"],
            950000,
        )
        self.assertTrue(manifest["budget_contract"]["terminate_on_terminal_marker"])
        self.assertEqual(manifest["source"]["state"], "frozen")
        self.assertEqual(
            manifest["source"]["git_commit"],
            "9819244bb99873094d1c5ba9dce84c61c9848b85",
        )
        for field in (
            "archive_sha256",
            "cargo_lock_sha256",
            "python_lock_sha256",
            "node_lock_sha256",
        ):
            self.assertRegex(manifest["source"][field], r"^[0-9a-f]{64}$")

    def test_datatype_schedule_cells_contain_only_executable_arms(self) -> None:
        manifest = validate_manifest(
            json.loads(
                Path("docs/research/publication-v3-manifest.json").read_text(
                    encoding="utf-8"
                )
            )
        )
        cells = [
            cell
            for cell in build_schedule_document(manifest)["cells"]
            if cell["workload"]["kind"] == "datatype-simd-matrix"
        ]
        self.assertTrue(cells)
        for cell in cells:
            self.assertEqual(
                cell["workload"]["factors"]["dimensions"],
                [cell["dataset"]["dimensions"]],
            )
            binary = cell["dataset"]["metric"] in {"hamming", "jaccard"}
            self.assertEqual(
                cell["workload"]["factors"]["datatypes"] == ["binary"],
                binary,
            )

    def test_cli_reports_unfrozen_manifest_as_not_paid_ready(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest_path = Path(directory) / "manifest.json"
            manifest_path.write_bytes(canonical_json_bytes(valid_v3_manifest()) + b"\n")
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/publication_v3_protocol.py",
                    "validate",
                    str(manifest_path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        report = json.loads(result.stdout)
        self.assertFalse(report["paid_ready"])
        self.assertEqual(report["unstaged_datasets"], 3)

    def test_cli_verifies_schedule_against_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest_path = Path(directory) / "manifest.json"
            schedule_path = Path(directory) / "schedule.json"
            manifest = paid_v3_manifest()
            for dataset in manifest["datasets"]:
                source = dataset["source"]
                if source["state"] != "generated":
                    continue
                attempt_root = (
                    f"{manifest['prefixes']['dataset']}/{dataset['id']}/attempts/0001"
                )
                dataset["source"] = {
                    "state": "staged-generated",
                    "generator": source["generator"],
                    "seed": source["seed"],
                    "generator_source_archive_sha256": "a" * 64,
                    "url": f"{attempt_root}/materialized",
                    "sha256": "b" * 64,
                    "receipt_uri": f"{attempt_root}/STAGING_COMPLETE.json",
                    "receipt_sha256": "c" * 64,
                }
            schedule = build_schedule_document(validate_manifest(manifest))
            manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")
            schedule_path.write_bytes(canonical_json_bytes(schedule) + b"\n")
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/publication_v3_protocol.py",
                    "verify-schedule",
                    str(manifest_path),
                    str(schedule_path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        report = json.loads(result.stdout)
        self.assertEqual(report["cells"], len(schedule["cells"]))
        self.assertEqual(report["manifest_sha256"], schedule["manifest_sha256"])
        self.assertTrue(report["paid_ready"])

    def test_cli_never_schedules_an_unfrozen_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest_path = Path(directory) / "manifest.json"
            schedule_path = Path(directory) / "schedule.json"
            manifest_path.write_bytes(canonical_json_bytes(valid_v3_manifest()) + b"\n")
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/publication_v3_protocol.py",
                    "schedule",
                    str(manifest_path),
                    "--output",
                    str(schedule_path),
                ],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(schedule_path.exists())
            self.assertIn("frozen source", result.stderr)


if __name__ == "__main__":
    unittest.main()
