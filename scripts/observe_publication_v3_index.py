#!/usr/bin/env python3
"""Observe a sealed Publication V3 index without fetching its data objects."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from scripts.publication_v3_protocol import canonical_json_bytes
    from scripts.seal_publication_v3_index import (
        _list_objects,
        _s3_parts,
        session_configuration,
    )
except ModuleNotFoundError:
    from publication_v3_protocol import canonical_json_bytes
    from seal_publication_v3_index import (
        _list_objects,
        _s3_parts,
        session_configuration,
    )


def observe_index_inventory(
    client: object,
    *,
    bucket: str,
    prefix: str,
    roster: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Bind sealed SHA-256 values to a fresh exact S3 size/ETag listing."""

    sealed: dict[str, dict[str, object]] = {}
    for item in roster:
        if not isinstance(item, dict):
            raise ValueError("sealed object roster is invalid")
        path = str(item.get("path", ""))
        if not path or path in sealed:
            raise ValueError("sealed object roster paths are invalid")
        sealed[path] = item
    live = _list_objects(client, bucket=bucket, prefix=prefix)
    observed = {str(item["path"]): item for item in live}
    if set(observed) != set(sealed):
        raise ValueError("live S3 index differs from sealed roster")
    inventory: list[dict[str, object]] = []
    for path in sorted(sealed):
        expected = sealed[path]
        current = observed[path]
        if current["bytes"] != expected.get("bytes") or current["etag"] != expected.get(
            "etag"
        ):
            raise ValueError("live S3 index differs from sealed roster")
        inventory.append(
            {
                "path": path,
                "bytes": current["bytes"],
                "checksum": expected.get("checksum"),
                "etag": current["etag"],
            }
        )
    return inventory


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index-uri", required=True)
    parser.add_argument("--roster", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--profile")
    parser.add_argument("--region", required=True)
    args = parser.parse_args()
    import boto3

    roster = json.loads(args.roster.read_bytes())
    if not isinstance(roster, list):
        raise ValueError("sealed object roster must be a JSON list")
    bucket, prefix = _s3_parts(args.index_uri)
    client = boto3.Session(**session_configuration(args.profile, args.region)).client(
        "s3"
    )
    inventory = observe_index_inventory(
        client, bucket=bucket, prefix=prefix, roster=roster
    )
    args.output.write_bytes(canonical_json_bytes(inventory) + b"\n")
    print(json.dumps({"objects": len(inventory)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
