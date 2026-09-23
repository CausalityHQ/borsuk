#!/usr/bin/env python3
"""Phase-separated fixed ReLAION-1M page-representative selector screen."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from scripts.native_one_million_page_selector import (
    build_page_selector,
    read_page_selector,
)
from scripts.native_one_million_page_selector_evaluation import evaluate_page_selector
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)
from scripts.validate_native_one_million_page_selector import validate_page_selector


def run_construct(root: Path) -> None:
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("page-selector construction query boundary differs")
    build_page_selector(root, root, SOURCE_IDENTITIES)


def run_evaluate(root: Path, out: Path, *, query_count: int = 1000) -> None:
    artifact = read_page_selector(root, SOURCE_IDENTITIES)
    evaluate_page_selector(
        artifact, root / "queries.parquet", root / "truth.parquet",
        out, DEVELOPMENT_IDENTITIES, query_count=query_count,
    )


def run_validate(root: Path, out: Path, *, query_count: int = 1000) -> None:
    validate_page_selector(
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
            raise ValueError("page-selector evaluation output required")
        run_evaluate(args.root, args.out)
    else:
        if args.out is None:
            raise ValueError("page-selector validation output required")
        run_validate(args.root, args.out)


if __name__ == "__main__":
    main()
