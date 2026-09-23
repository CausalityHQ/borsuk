"""Validate source-only PQ64 planes and Rust rosters against frozen V77."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v109_capped_reader_replay import load_manifest
from scripts.v114_1m_paired import FROZEN_MANIFEST_SHA256
from scripts.v114_exact_local_100k import _canonical, _sha256_file
from scripts.v115_source_router import load_source_router

FROZEN_REQUESTS_SHA256 = "fbb93b017df9ec6196286f082e4b5b55b48d00606ad2fc1f74d7e75cc5b7e75e"


def compare_planes(
    historical: dict[str, np.ndarray], actual: dict[str, np.ndarray],
) -> list[str]:
    names = ("summaries", "books", "codes", "low", "step")
    historical_names = {"summaries": "summaries", "books": "books",
                        "codes": "codes", "low": "low", "step": "span_step"}
    return [name for name in names
            if not np.array_equal(historical[historical_names[name]], actual[name])]


def compare_rosters(
    expected: list[dict[str, object]], actual: list[dict[str, object]],
) -> tuple[int, int]:
    if len(expected) != len(actual):
        raise ValueError("runtime roster count differs")
    ordered_mismatches = set_mismatches = 0
    for ordinal, (reference, observed) in enumerate(zip(expected, actual)):
        if (reference.get("query_ordinal") != ordinal
                or observed.get("query_ordinal") != ordinal
                or type(reference.get("nominees")) is not list
                or type(observed.get("nominees")) is not list
                or len(reference["nominees"]) != len(observed["nominees"])
                or len(set(reference["nominees"])) != len(reference["nominees"])
                or len(set(observed["nominees"])) != len(observed["nominees"])):
            raise ValueError(f"runtime roster geometry differs at {ordinal}")
        if reference["nominees"] != observed["nominees"]:
            ordered_mismatches += 1
        if set(reference["nominees"]) != set(observed["nominees"]):
            set_mismatches += 1
    return ordered_mismatches, set_mismatches


def validate(
    router_dir: Path, v77_manifest_path: Path, requests_path: Path,
    rust_path: Path, output_path: Path,
    *, expected_router_manifest_sha256: str,
) -> None:
    if (_sha256_file(v77_manifest_path) != FROZEN_MANIFEST_SHA256
            or _sha256_file(requests_path) != FROZEN_REQUESTS_SHA256
            or _sha256_file(router_dir / "manifest.json") != expected_router_manifest_sha256):
        raise ValueError("V115 frozen manifest, request or router SHA-256 differs")
    router_manifest, arrays = load_source_router(router_dir)
    if (router_manifest["geometry"] != {
        "rows": 1_000_000, "dimensions": 768, "page_rows": 256,
        "blocks_per_page": 2, "subspaces": 64, "pq_width": 12,
    } or router_manifest["generation"] != 1):
        raise ValueError("V115 frozen router geometry differs")
    historical = load_manifest(v77_manifest_path)
    plane_mismatches = compare_planes(historical, arrays)
    expected = [json.loads(line) for line in requests_path.read_text().splitlines()]
    actual = [json.loads(line) for line in rust_path.read_text().splitlines()]
    if len(expected) != 1000 or len(actual) != 1000:
        raise ValueError("V115 frozen query count differs")
    ordered, sets = compare_rosters(expected, actual)
    summary = {
        "schema": "borsuk-v115-source-router-parity-v1",
        "dataset": "ReLAION-1M",
        "split": "development-1000",
        "query_count": 1000,
        "router_manifest_sha256": expected_router_manifest_sha256,
        "v77_manifest_sha256": FROZEN_MANIFEST_SHA256,
        "requests_sha256": FROZEN_REQUESTS_SHA256,
        "rust_sha256": _sha256_file(rust_path),
        "plane_mismatches": plane_mismatches,
        "ordered_roster_mismatches": ordered,
        "roster_set_mismatches": sets,
        "ground_truth_used": False,
        "live_s3_measured": False,
    }
    output_path.write_text(_canonical(summary))
    if plane_mismatches or sets:
        raise ValueError("V115 source router plane or nominee set differs")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--router", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--requests", required=True, type=Path)
    parser.add_argument("--rust", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--router-manifest-sha256", required=True)
    args = parser.parse_args()
    validate(args.router, args.manifest, args.requests, args.rust, args.output,
             expected_router_manifest_sha256=args.router_manifest_sha256)


if __name__ == "__main__":
    main()
