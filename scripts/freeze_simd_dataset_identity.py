#!/usr/bin/env python3
"""Freeze every prepared input byte used by one SIMD datatype dataset."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Iterable

HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_identity(
    dataset_dir: Path, *, dataset: str, source: str, synthetic: bool
) -> dict:
    if not dataset or not source:
        raise ValueError("dataset and source labels must be non-empty")
    if not dataset_dir.is_dir():
        raise ValueError(f"dataset directory does not exist: {dataset_dir}")
    files = []
    for path in sorted(dataset_dir.rglob("*")):
        if path.name == "dataset-identity.json":
            continue
        if path.is_symlink():
            raise ValueError(f"dataset identity does not permit symlinks: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(dataset_dir).as_posix()
        files.append(
            {
                "path": relative,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    if not files:
        raise ValueError("dataset contains no input files")
    identity = {
        "schema_version": 1,
        "dataset": dataset,
        "source": source,
        "synthetic": synthetic,
        "files": files,
    }
    validate_identity(identity)
    return identity


def validate_identity(identity: dict) -> None:
    if identity.get("schema_version") != 1:
        raise ValueError("unsupported SIMD dataset identity schema")
    if not isinstance(identity.get("dataset"), str) or not identity["dataset"]:
        raise ValueError("dataset identity has no dataset label")
    if not isinstance(identity.get("source"), str) or not identity["source"]:
        raise ValueError("dataset identity has no source label")
    if not isinstance(identity.get("synthetic"), bool):
        raise ValueError("dataset identity synthetic flag must be boolean")
    files = identity.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("dataset identity has no files")
    paths = []
    for item in files:
        if not isinstance(item, dict):
            raise ValueError("dataset file identity must be an object")
        path = item.get("path")
        if (
            not isinstance(path, str)
            or not path
            or path.startswith("/")
            or ".." in Path(path).parts
        ):
            raise ValueError("dataset file identity has an unsafe path")
        if not isinstance(item.get("bytes"), int) or item["bytes"] < 0:
            raise ValueError("dataset file identity has invalid byte size")
        if not isinstance(item.get("sha256"), str) or not HEX_SHA256.fullmatch(
            item["sha256"]
        ):
            raise ValueError("dataset file identity has invalid SHA-256")
        paths.append(path)
    if paths != sorted(set(paths)):
        raise ValueError("dataset file identities must be unique and sorted")


def validate_dataset_identity(dataset_dir: Path, identity: dict) -> None:
    """Re-hash the current payload and require exact agreement with the record."""
    validate_identity(identity)
    actual = build_identity(
        dataset_dir,
        dataset=identity["dataset"],
        source=identity["source"],
        synthetic=identity["synthetic"],
    )
    if actual != identity:
        raise ValueError(f"dataset payload drift: {dataset_dir}")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--dataset")
    parser.add_argument("--source")
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--verify-existing", action="store_true")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    output = args.dataset_dir / "dataset-identity.json"
    if args.verify_existing:
        if args.dataset is not None or args.source is not None or args.synthetic:
            raise SystemExit(
                "--verify-existing cannot be combined with identity creation fields"
            )
        try:
            identity = json.loads(output.read_text(encoding="utf-8"))
            validate_dataset_identity(args.dataset_dir, identity)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            raise SystemExit(
                f"SIMD dataset identity verification failed: {error}"
            ) from error
        return 0
    if not args.dataset or not args.source:
        raise SystemExit("identity creation requires --dataset and --source")
    if output.exists():
        raise SystemExit(f"refusing to overwrite existing identity: {output}")
    identity = build_identity(
        args.dataset_dir,
        dataset=args.dataset,
        source=args.source,
        synthetic=args.synthetic,
    )
    with output.open("x", encoding="utf-8") as handle:
        json.dump(identity, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
