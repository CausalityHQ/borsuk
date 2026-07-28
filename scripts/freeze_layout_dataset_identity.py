#!/usr/bin/env python3
"""Validate and freeze exact layout-qualification dataset input identities."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

REQUIRED_FILES = ("meta.json", "train.f32", "test.f32", "neighbors.i32")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _expected_meta(contract: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": str(contract["ann_benchmarks_id"]),
        "metric": str(contract["metric"]),
        "dim": int(contract["dimensions"]),
        "n_train": int(contract["train_vectors"]),
        "n_test": int(contract["test_vectors"]),
        "k": int(contract["ground_truth_k"]),
    }


def _expected_sizes(contract: dict[str, Any]) -> dict[str, int]:
    dimensions = int(contract["dimensions"])
    train_vectors = int(contract["train_vectors"])
    test_vectors = int(contract["test_vectors"])
    ground_truth_k = int(contract["ground_truth_k"])
    return {
        "train.f32": train_vectors * dimensions * 4,
        "test.f32": test_vectors * dimensions * 4,
        "neighbors.i32": test_vectors * ground_truth_k * 4,
    }


def build_manifest(dataset_root: Path, protocol: dict[str, Any]) -> dict[str, Any]:
    contracts = protocol.get("dataset_contracts")
    if not isinstance(contracts, dict) or not contracts:
        raise ValueError("qualification protocol has no dataset contracts")
    datasets: dict[str, Any] = {}
    for dataset_name, raw_contract in contracts.items():
        if not isinstance(raw_contract, dict):
            raise ValueError(f"invalid dataset contract for {dataset_name}")
        contract = {key: value for key, value in raw_contract.items()}
        dataset_dir = dataset_root / dataset_name
        try:
            meta = json.loads((dataset_dir / "meta.json").read_text())
        except (FileNotFoundError, json.JSONDecodeError) as error:
            raise ValueError(f"{dataset_name}: missing or invalid meta.json") from error
        expected_meta = _expected_meta(contract)
        if meta != expected_meta:
            raise ValueError(
                f"{dataset_name}: meta.json does not match the frozen contract"
            )

        expected_sizes = _expected_sizes(contract)
        files: dict[str, dict[str, Any]] = {}
        for filename in REQUIRED_FILES:
            path = dataset_dir / filename
            try:
                size = path.stat().st_size
            except FileNotFoundError as error:
                raise ValueError(f"{dataset_name}: missing {filename}") from error
            if filename in expected_sizes and size != expected_sizes[filename]:
                raise ValueError(
                    f"{dataset_name}: {filename} has {size} bytes; "
                    f"expected {expected_sizes[filename]}"
                )
            files[filename] = {"bytes": size, "sha256": _sha256(path)}
        datasets[str(dataset_name)] = {
            "contract": contract,
            "files": files,
        }
    manifest = {"schema_version": 1, "datasets": datasets}
    validate_manifest(manifest, protocol)
    return manifest


def validate_manifest(manifest: dict[str, Any], protocol: dict[str, Any]) -> None:
    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported dataset identity manifest schema")
    contracts = protocol.get("dataset_contracts")
    datasets = manifest.get("datasets")
    if not isinstance(contracts, dict) or not isinstance(datasets, dict):
        raise ValueError("invalid dataset identity manifest")
    if set(datasets) != set(contracts):
        raise ValueError("dataset identity set does not match the frozen protocol")
    for dataset_name, contract in contracts.items():
        record = datasets.get(dataset_name)
        if not isinstance(record, dict) or record.get("contract") != contract:
            raise ValueError(f"{dataset_name}: identity contract drift")
        files = record.get("files")
        if not isinstance(files, dict) or set(files) != set(REQUIRED_FILES):
            raise ValueError(f"{dataset_name}: incomplete dataset file identity")
        expected_sizes = _expected_sizes(contract)
        for filename, file_identity in files.items():
            if not isinstance(file_identity, dict):
                raise ValueError(f"{dataset_name}: invalid {filename} identity")
            size = file_identity.get("bytes")
            digest = file_identity.get("sha256")
            if not isinstance(size, int) or size <= 0:
                raise ValueError(f"{dataset_name}: invalid {filename} byte size")
            if filename in expected_sizes and size != expected_sizes[filename]:
                raise ValueError(f"{dataset_name}: {filename} byte-size drift")
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError(f"{dataset_name}: invalid {filename} SHA-256")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-root", type=Path, required=True)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    protocol = json.loads(args.protocol.read_text())
    manifest = build_manifest(args.dataset_root, protocol)
    with args.output.open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
