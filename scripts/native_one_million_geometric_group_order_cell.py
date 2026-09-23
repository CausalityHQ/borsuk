#!/usr/bin/env python3
"""Phase-separated geometric 1M code-group physical order screen."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from scripts.native_one_million_geometric_group_order import build_geometric_group_order
from scripts.native_one_million_geometric_group_order_evaluation import (
    evaluate_geometric_group_order,
)
from scripts.native_one_million_page_selector import read_page_selector
from scripts.native_one_million_range_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
    check_prior_page_artifact,
)
from scripts.native_one_million_range_selector_cell import (
    run_construct as construct_prior_page_artifact,
)
from scripts.validate_native_one_million_geometric_group_order import (
    validate_geometric_group_order,
)


def run_construct(root: Path) -> None:
    construct_prior_page_artifact(root)
    artifact = read_page_selector(root, SOURCE_IDENTITIES)
    check_prior_page_artifact(artifact.seal)
    build_geometric_group_order(artifact, root)


def run_evaluate(root: Path, out: Path, *, query_count: int = 1000) -> None:
    artifact = read_page_selector(root, SOURCE_IDENTITIES)
    check_prior_page_artifact(artifact.seal)
    evaluate_geometric_group_order(
        artifact, root / "layout.json", root / "queries.parquet", root / "truth.parquet",
        out, DEVELOPMENT_IDENTITIES, query_count=query_count,
    )


def run_validate(root: Path, out: Path, *, query_count: int = 1000) -> None:
    validate_geometric_group_order(
        root, root, out, SOURCE_IDENTITIES, DEVELOPMENT_IDENTITIES,
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
            raise ValueError("geometric group evaluation output required")
        run_evaluate(args.root, args.out)
    else:
        if args.out is None:
            raise ValueError("geometric group validation output required")
        run_validate(args.root, args.out)


if __name__ == "__main__":
    main()
