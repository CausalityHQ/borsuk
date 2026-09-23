#!/usr/bin/env python3
"""Phase-separated fixed ReLAION-1M centroid-selector decision cell."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from scripts.launch_v98_hierarchical_row_router_spot import FROZEN_INPUTS
from scripts.native_one_million_group_selector import build_selector, read_selector
from scripts.native_one_million_selector_evaluation import evaluate_selector
from scripts.v97_row_width_screen import ObjectIdentity
from scripts.validate_native_one_million_selector import validate_selector

ROUTER = ObjectIdentity(
    "s3://borsuk-bench-453182569524-euc1/research/v85-pq16-1m-validation/"
    "da42b3da7a78a7a44ab259bb06f12c42527e8800/runs/"
    "v85-1m-validation-20260920T100638Z-da42b3d/a0001/artifacts/router.arrow",
    "e7beee3da272e694ff69f15d4343ff7c18f7d0b9e3d33788baea1ee34a25bf8e",
    2_363_962,
)
SOURCE_IDENTITIES = {
    role: FROZEN_INPUTS[role] for role in ("source", "generation", "base", "delta")
} | {"router": ROUTER}
DEVELOPMENT_IDENTITIES = {
    role: FROZEN_INPUTS[role] for role in ("queries", "truth")
}


def run_construct(root: Path) -> None:
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("selector construction query boundary differs")
    build_selector(root, root, SOURCE_IDENTITIES)


def run_evaluate(root: Path, out: Path, *, query_count: int = 1000) -> None:
    artifact = read_selector(root, SOURCE_IDENTITIES)
    evaluate_selector(
        artifact, root / "queries.parquet", root / "truth.parquet",
        out, DEVELOPMENT_IDENTITIES, query_count=query_count,
    )


def run_validate(root: Path, out: Path, *, query_count: int = 1000) -> None:
    validate_selector(
        root, root, root, out, SOURCE_IDENTITIES, DEVELOPMENT_IDENTITIES,
        query_count=query_count,
    )


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("construct", "evaluate", "validate"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    if args.phase == "construct":
        run_construct(args.root)
    elif args.phase == "evaluate":
        if args.out is None:
            raise ValueError("selector evaluation output required")
        run_evaluate(args.root, args.out)
    else:
        if args.out is None:
            raise ValueError("selector validation output required")
        run_validate(args.root, args.out)


if __name__ == "__main__":
    main()
