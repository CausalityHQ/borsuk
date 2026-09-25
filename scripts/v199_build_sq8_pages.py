#!/usr/bin/env python3
"""Authenticate the immutable V164 SQ8 object and derive 32-row page hashes."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

OBJECT_SHA = "aecf0f2704f44906f411a74ab81b36e5e05f81bab35f4c70558e88acbc4d05c9"
ROWS, DIMENSIONS, PAGE_ROWS, GENERATION = 1_000_000, 768, 32, 196
PAGE_BYTES = PAGE_ROWS * (DIMENSIONS + 12)


def build(source: Path, manifest_path: Path, digests_path: Path) -> dict:
    if source.stat().st_size != ROWS * (DIMENSIONS + 12):
        raise ValueError("V199 SQ8 object length differs")
    object_hash = hashlib.sha256()
    sidecar_hash = hashlib.sha256()
    with source.open("rb") as original, digests_path.open("xb") as sidecar:
        for _ in range(ROWS // PAGE_ROWS):
            page = original.read(PAGE_BYTES)
            if len(page) != PAGE_BYTES:
                raise ValueError("V199 short SQ8 page")
            object_hash.update(page)
            digest = hashlib.sha256(page).digest()
            sidecar.write(digest)
            sidecar_hash.update(digest)
        if original.read(1):
            raise ValueError("V199 trailing SQ8 payload")
    if object_hash.hexdigest() != OBJECT_SHA:
        raise ValueError("V199 SQ8 object SHA-256 differs")
    manifest = {
        "schema": "borsuk-v115-sq8-page-authority-v2",
        "generation": GENERATION, "rows": ROWS,
        "dimensions": DIMENSIONS, "page_rows": PAGE_ROWS,
        "object_sha256": OBJECT_SHA,
        "page_digest_sha256": sidecar_hash.hexdigest(),
    }
    data = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    with manifest_path.open("xb") as output:
        output.write(data)
    return {"schema": "borsuk-v199-sq8-pages-v1", "object_sha256": OBJECT_SHA,
            "page_digest_sha256": sidecar_hash.hexdigest(),
            "manifest_sha256": hashlib.sha256(data).hexdigest(),
            "page_count": ROWS // PAGE_ROWS}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("digests", type=Path)
    args = parser.parse_args()
    print(json.dumps(build(args.source, args.manifest, args.digests), sort_keys=True))


if __name__ == "__main__":
    main()
