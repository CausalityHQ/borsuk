#!/usr/bin/env python3
"""Fail a publication phase before it can consume the disk safety reserve."""

from __future__ import annotations

import argparse
from pathlib import Path
from shutil import disk_usage


def require_free_disk(root: Path, minimum_free_bytes: int, phase: str) -> int:
    if minimum_free_bytes < 0:
        raise ValueError("minimum free bytes must be non-negative")
    free_bytes = disk_usage(root).free
    if free_bytes < minimum_free_bytes:
        raise RuntimeError(
            "insufficient publication disk before "
            f"{phase}: free_bytes={free_bytes} required_bytes={minimum_free_bytes}"
        )
    return free_bytes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--minimum-free-bytes", type=int, required=True)
    parser.add_argument("--phase", required=True)
    args = parser.parse_args()
    try:
        free_bytes = require_free_disk(args.root, args.minimum_free_bytes, args.phase)
    except (OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    print(f"publication_disk_free_bytes={free_bytes}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
