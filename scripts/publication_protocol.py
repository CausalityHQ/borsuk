#!/usr/bin/env python3
"""Frozen confirmatory benchmark manifest validation and scheduling."""

from __future__ import annotations

import argparse
import csv
import json
import random
from pathlib import Path
from typing import Any

SCHEDULE_FIELDS = (
    "repetition_id",
    "query_seed",
    "dense_dataset_order",
    "hybrid_dataset_order",
    "system_order",
    "result_prefix",
    "index_prefix",
    "cache_key",
)


def _required(value: dict[str, Any], name: str) -> Any:
    if name not in value:
        raise ValueError(f"missing required manifest field: {name}")
    return value[name]


def validate_manifest(value: dict[str, object]) -> dict[str, object]:
    normalized = dict(value)
    if normalized.get("schema_version") != 2:
        raise ValueError("publication manifest schema_version must be 2")
    if normalized.get("campaign_kind") != "confirmatory":
        raise ValueError("campaign_kind must be confirmatory")
    campaign_id = str(_required(normalized, "campaign_id"))
    if not campaign_id or "pilot" in campaign_id.lower():
        raise ValueError("confirmatory campaign_id must be non-pilot")
    repetitions = int(_required(normalized, "repetitions"))
    if repetitions < 3:
        raise ValueError("confirmatory campaigns require at least 3 repetitions")
    query_count = int(_required(normalized, "queries_per_repetition"))
    if query_count <= 0:
        raise ValueError("queries_per_repetition must be positive")
    if bool(normalized.get("publish_p99")) and query_count < 1000:
        raise ValueError("publishing p99 requires at least 1,000 queries")
    if normalized.get("search_config_frozen") is not True:
        raise ValueError("search_config_frozen must be true")
    if normalized.get("production_codec") != "srht-pq-scan":
        raise ValueError("production_codec must remain frozen as srht-pq-scan")
    direct_config = normalized.get("direct_search_config")
    if direct_config != {
        "leaf_mode": "srht-pq-scan",
        "nprobe": 8,
        "max_candidates": 320,
        "k": 10,
    }:
        raise ValueError("direct_search_config must remain frozen at srht/8/320/k10")
    hardware_policy = normalized.get("hardware_policy")
    if hardware_policy != {
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
    }:
        raise ValueError(
            "hardware_policy must freeze the same-client and undisclosed-service boundary"
        )
    execution_order_policy = normalized.get("execution_order_policy")
    if execution_order_policy != {
        "direct_system_pair": "adjacent-and-counterbalanced",
        "dense_dataset_order": "seeded-per-repetition",
        "hybrid_dataset_order": "seeded-per-repetition",
        "sync_before_cleanup": True,
    }:
        raise ValueError(
            "execution_order_policy must freeze adjacent direct pairs and seeded orders"
        )
    repetition_ids = [str(item) for item in _required(normalized, "repetition_ids")]
    if len(repetition_ids) != repetitions:
        raise ValueError("repetition_ids length must equal repetitions")
    if len(set(repetition_ids)) != len(repetition_ids):
        raise ValueError("repetition identifiers must be unique")
    if any(not item or "pilot" in item.lower() for item in repetition_ids):
        raise ValueError("repetition identifiers must be non-empty and non-pilot")
    systems = [str(item) for item in _required(normalized, "systems")]
    if systems != ["borsuk", "amazon-s3-vectors"]:
        raise ValueError("systems must be borsuk and amazon-s3-vectors")
    direct_dataset = str(_required(normalized, "direct_dataset"))
    if direct_dataset != "fashion-mnist-784":
        raise ValueError("the direct S3 Vectors dataset must be fashion-mnist-784")
    dense = [str(item) for item in _required(normalized, "dense_datasets")]
    hybrid = [str(item) for item in _required(normalized, "hybrid_datasets")]
    if direct_dataset not in dense or not dense or not hybrid:
        raise ValueError(
            "dataset lists must be non-empty and include the direct dataset"
        )
    if normalized.get("hybrid_dense_config") != {
        "backend": "sentence-transformers",
        "model": "BAAI/bge-small-en-v1.5",
        "revision": "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a",
        "query_prefix": ("Represent this sentence for searching relevant passages: "),
        "normalized": True,
    }:
        raise ValueError("hybrid_dense_config must freeze the publication encoder")
    if normalized.get("borsuk_measurement_profile") != {
        "recall_only": True,
        "skip_exact_recall": True,
        "cache_phases": ["uncached", "disk_cached"],
    }:
        raise ValueError(
            "borsuk_measurement_profile must exclude exact and non-claim phases"
        )
    if normalized.get("hybrid_search_config") != {
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
    }:
        raise ValueError(
            "hybrid_search_config must freeze the bounded seven-mode matrix"
        )
    for prefix_name in ("result_prefix", "index_prefix", "cache_prefix"):
        prefix = str(_required(normalized, prefix_name))
        if not prefix or "pilot" in prefix.lower():
            raise ValueError(f"{prefix_name} must be non-empty and non-pilot")
    int(_required(normalized, "master_seed"))
    normalized.update(
        repetitions=repetitions,
        queries_per_repetition=query_count,
        repetition_ids=repetition_ids,
        systems=systems,
        dense_datasets=dense,
        hybrid_datasets=hybrid,
    )
    return normalized


def build_schedule(manifest: dict[str, object]) -> list[dict[str, object]]:
    manifest = validate_manifest(manifest)
    seed = int(manifest["master_seed"])
    repetition_ids = list(manifest["repetition_ids"])
    rows: list[dict[str, object]] = []
    for position, repetition_id in enumerate(repetition_ids, start=1):
        repetition_rng = random.Random(seed + position)
        dense_dataset_order = list(manifest["dense_datasets"])
        hybrid_dataset_order = list(manifest["hybrid_datasets"])
        repetition_rng.shuffle(dense_dataset_order)
        repetition_rng.shuffle(hybrid_dataset_order)
        rows.append(
            {
                "repetition_id": repetition_id,
                "query_seed": seed + position,
                "dense_dataset_order": " ".join(dense_dataset_order),
                "hybrid_dataset_order": " ".join(hybrid_dataset_order),
                "system_order": (
                    "borsuk amazon-s3-vectors"
                    if position % 2
                    else "amazon-s3-vectors borsuk"
                ),
                "result_prefix": f"{manifest['result_prefix']}/{repetition_id}",
                "index_prefix": f"{manifest['index_prefix']}/{repetition_id}",
                "cache_key": f"{manifest['cache_prefix']}-{repetition_id}",
            }
        )
    return rows


def hardware_relation(borsuk: dict[str, object], external: dict[str, object]) -> str:
    required = ("logical_cpus", "ram_bytes", "accelerator", "storage_class")
    if any(key not in borsuk or key not in external for key in required):
        return "unknown"
    if (
        int(borsuk["logical_cpus"]) <= int(external["logical_cpus"])
        and int(borsuk["ram_bytes"]) <= int(external["ram_bytes"])
        and borsuk["accelerator"] == external["accelerator"]
        and borsuk["storage_class"] == external["storage_class"]
    ):
        return "weaker-or-equal"
    return "stronger-or-incomparable"


def _load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("manifest root must be an object")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("manifest", type=Path)
    schedule_parser = subparsers.add_parser("schedule")
    schedule_parser.add_argument("manifest", type=Path)
    schedule_parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest = validate_manifest(_load(args.manifest))
    if args.command == "validate":
        print("valid confirmatory manifest")
        return 0
    rows = build_schedule(manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=SCHEDULE_FIELDS,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} schedule rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
