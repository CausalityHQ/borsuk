#!/usr/bin/env python3
"""Phase-separated fixed 1M adjacent-code-range selector screen."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from scripts.native_one_million_page_selector import (
    build_page_selector,
    read_page_selector,
)
from scripts.native_one_million_range_selector_evaluation import evaluate_range_selector
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)
from scripts.validate_native_one_million_range_selector import validate_range_selector

PRIOR_CENTROIDS_SHA256 = "757fbe7c6b2112ea5904a5bba0e26fb4ac929cc39ca1fc470b12dee35b61a704"
PRIOR_MEMBERSHIP_SHA256 = "55e36613b293ffc08c11b9da8c2f2293cf89e43310551f316f2c5b57a84e0d13"
PRIOR_PAGE_ORDER_SHA256 = "bb8ebb3642de174a338621f08d4a739b265d91914eb25ebe41831ffed0f33b4b"


def check_prior_page_artifact(seal: dict[str, object]) -> None:
    if (
        seal["centroids"]["sha256"] != PRIOR_CENTROIDS_SHA256
        or seal["membership"]["sha256"] != PRIOR_MEMBERSHIP_SHA256
        or seal["page_order_sha256"] != PRIOR_PAGE_ORDER_SHA256
    ):
        raise ValueError("range-selector source plane differs from prior terminal")


def run_construct(root: Path) -> None:
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("range-selector construction query boundary differs")
    artifact = build_page_selector(root, root, SOURCE_IDENTITIES)
    check_prior_page_artifact(artifact.seal)


def run_evaluate(root: Path, out: Path, *, query_count: int = 1000) -> None:
    artifact = read_page_selector(root, SOURCE_IDENTITIES)
    check_prior_page_artifact(artifact.seal)
    evaluate_range_selector(
        artifact, root / "queries.parquet", root / "truth.parquet",
        out, DEVELOPMENT_IDENTITIES, query_count=query_count,
    )


def run_validate(root: Path, out: Path, *, query_count: int = 1000) -> None:
    artifact = read_page_selector(root, SOURCE_IDENTITIES)
    check_prior_page_artifact(artifact.seal)
    validate_range_selector(
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
            raise ValueError("range-selector evaluation output required")
        run_evaluate(args.root, args.out)
    else:
        if args.out is None:
            raise ValueError("range-selector validation output required")
        run_validate(args.root, args.out)


if __name__ == "__main__":
    main()
