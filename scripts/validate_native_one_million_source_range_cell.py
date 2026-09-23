#!/usr/bin/env python3
"""Replay sealed source-score planning and truth evaluation on the Spot host."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from collections.abc import Sequence
from pathlib import Path

from scripts.native_one_million_data_range_cell import _canonical, _identity
from scripts.native_one_million_source_range_cell import (
    PLAN_FILES,
    SCHEMA,
    run_construct,
    run_evaluate,
    run_plan,
)

SOURCE_FILES = (
    "source.parquet", "generation.json", "base.arrow", "delta.arrow",
    "prior-terminal.json", "prior-source-seal.json", "prior-plans.json",
    "prior-plan-seal.json", "codes.bin", "model.bin", "prior-membership.bin",
)
CONSTRUCT_FILES = (
    "page-map.bin", "page-groups.bin", "page-bytes.bin", "seal.json",
    "row-pages.bin", "range-seal.json",
)
EVALUATION_FILES = ("source-range-evidence.json", "source-range-result.json")


def _compare(stage: Path, root: Path, filenames: Sequence[str]) -> None:
    for filename in filenames:
        if _identity(stage / filename) != _identity(root / filename):
            raise ValueError(f"source-range validation {filename} differs")


def validate(root: Path, out: Path) -> dict[str, object]:
    for filename in (
        *SOURCE_FILES, *CONSTRUCT_FILES, *PLAN_FILES, *EVALUATION_FILES,
        "queries.parquet", "truth.parquet",
    ):
        if not (root / filename).is_file():
            raise ValueError(f"source-range validation input missing: {filename}")
    with tempfile.TemporaryDirectory(prefix="source-range-validate-", dir=root) as temporary:
        stage = Path(temporary)
        for filename in SOURCE_FILES:
            (stage / filename).symlink_to((root / filename).resolve())
        run_construct(stage)
        _compare(stage, root, CONSTRUCT_FILES)
        (stage / "queries.parquet").symlink_to((root / "queries.parquet").resolve())
        run_plan(stage, stage)
        _compare(stage, root, PLAN_FILES)
        (stage / "truth.parquet").symlink_to((root / "truth.parquet").resolve())
        replay = run_evaluate(stage, stage)
        _compare(stage, root, EVALUATION_FILES)
    evidence_body = (root / EVALUATION_FILES[0]).read_bytes()
    result_body = (root / EVALUATION_FILES[1]).read_bytes()
    evidence = json.loads(evidence_body)
    result = json.loads(result_body)
    if (
        evidence_body != _canonical(evidence)
        or result_body != _canonical(result)
        or result != replay
        or evidence.get("schema") != SCHEMA + "-evidence"
        or result.get("schema") != SCHEMA + "-result"
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or len(evidence.get("samples", [])) != 1000
    ):
        raise ValueError("source-range validation result differs")
    value = {
        "schema": SCHEMA + "-validation",
        "decision": result["decision"],
        "source_identity": _identity(root / "source.parquet"),
        "plans_sha256": _identity(root / PLAN_FILES[0])["sha256"],
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "result_sha256": hashlib.sha256(result_body).hexdigest(),
        "metrics": result["metrics"],
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "source-range-validation.json").write_bytes(_canonical(value))
    return value


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    validate(args.root, args.out)


if __name__ == "__main__":
    main()
