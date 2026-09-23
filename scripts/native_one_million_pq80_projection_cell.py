#!/usr/bin/env python3
"""Phase-separated fixed ReLAION-1M PQ80 locality projection."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from scripts.native_one_million_page_selector import read_page_selector
from scripts.native_one_million_range_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
    check_prior_page_artifact,
    run_construct,
)
from scripts.native_one_million_range_selector_evaluation import evaluate_range_selector
from scripts.validate_native_one_million_range_selector import validate_range_selector


def run_evaluate(root: Path, out: Path, *, query_count: int = 1000) -> None:
    artifact = read_page_selector(root, SOURCE_IDENTITIES)
    check_prior_page_artifact(artifact.seal)
    evaluate_range_selector(
        artifact, root / "queries.parquet", root / "truth.parquet", out,
        DEVELOPMENT_IDENTITIES, query_count=query_count, row_bytes=80,
    )


def run_validate(root: Path, out: Path, *, query_count: int = 1000) -> None:
    artifact = read_page_selector(root, SOURCE_IDENTITIES)
    check_prior_page_artifact(artifact.seal)
    validate_range_selector(
        root, root, root, out, SOURCE_IDENTITIES, DEVELOPMENT_IDENTITIES,
        query_count=query_count, row_bytes=80,
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
            raise ValueError("PQ80 projection evaluation output required")
        run_evaluate(args.root, args.out)
    else:
        if args.out is None:
            raise ValueError("PQ80 projection validation output required")
        run_validate(args.root, args.out)


if __name__ == "__main__":
    main()
