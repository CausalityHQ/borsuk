#!/usr/bin/env python3
"""Server-side copy one immutable Publication V3 index into a mutable arm."""

from __future__ import annotations

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import Callable
from urllib.parse import quote

try:
    from scripts.publication_v3_clones import (
        MAX_SINGLE_COPY_BYTES,
        build_clone_receipt,
        clone_index_uri,
    )
    from scripts.publication_v3_protocol import canonical_json_bytes
except ModuleNotFoundError:
    from publication_v3_clones import (
        MAX_SINGLE_COPY_BYTES,
        build_clone_receipt,
        clone_index_uri,
    )
    from publication_v3_protocol import canonical_json_bytes

AwsJson = Callable[[list[str]], dict[str, object]]


def _aws_json(arguments: list[str]) -> dict[str, object]:
    completed = subprocess.run(
        ["aws", "--profile", "causality", *arguments, "--output", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise ValueError("AWS clone operation returned invalid JSON") from error
    if not isinstance(value, dict):
        raise ValueError("AWS clone operation returned a non-object")
    return value


def _s3_location(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://") or "/" not in uri[5:]:
        raise ValueError("clone index URI is not an S3 prefix")
    bucket, prefix = uri[5:].split("/", 1)
    if not bucket or not prefix or prefix.endswith("/"):
        raise ValueError("clone index URI is invalid")
    return bucket, prefix


def clone_index_objects(
    *,
    cell: dict[str, object],
    arm: dict[str, object],
    attempt_id: str,
    base_receipt: dict[str, object],
    base_roster: list[dict[str, object]],
    aws_json: AwsJson = _aws_json,
    workers: int = 64,
) -> tuple[dict[str, object], bytes]:
    if workers <= 0 or workers > 128:
        raise ValueError("clone copy worker count is outside its bound")
    if not base_roster or any(
        not isinstance(item, dict)
        or isinstance(item.get("bytes"), bool)
        or not isinstance(item.get("bytes"), int)
        or item["bytes"] <= 0
        or item["bytes"] > MAX_SINGLE_COPY_BYTES
        for item in base_roster
    ):
        raise ValueError("clone roster exceeds the atomic copy object bound")
    base_uri = str(base_receipt.get("index_uri"))
    if base_uri != cell.get("index_prefix"):
        raise ValueError("clone base URI differs from its scheduled index")
    destination_uri = clone_index_uri(cell, arm=arm, attempt_id=attempt_id)
    source_bucket, source_prefix = _s3_location(base_uri)
    destination_bucket, destination_prefix = _s3_location(destination_uri)
    if source_bucket != destination_bucket:
        raise ValueError("clone must remain in the immutable index bucket")
    listing = aws_json(
        [
            "s3api",
            "list-objects-v2",
            "--bucket",
            destination_bucket,
            "--prefix",
            destination_prefix + "/",
            "--max-keys",
            "1",
        ]
    )
    if listing.get("KeyCount") != 0:
        raise ValueError("publication mutation requires a fresh clone prefix")

    def copy_one(item: dict[str, object]) -> dict[str, object]:
        path = item.get("path")
        byte_count = item.get("bytes")
        if not isinstance(path, str) or not path or path.startswith("/") or ".." in path.split("/"):
            raise ValueError("clone roster path is invalid")
        source_key = f"{source_prefix}/{path}"
        destination_key = f"{destination_prefix}/{path}"
        source = aws_json(
            ["s3api", "head-object", "--bucket", source_bucket, "--key", source_key]
        )
        source_etag = source.get("ETag")
        if source.get("ContentLength") != byte_count or source_etag != item.get("etag"):
            raise ValueError("clone source object differs from its immutable roster")
        aws_json(
            [
                "s3api",
                "copy-object",
                "--bucket",
                destination_bucket,
                "--key",
                destination_key,
                "--copy-source",
                quote(f"{source_bucket}/{source_key}", safe="/"),
                "--copy-source-if-match",
                source_etag,
                "--metadata-directive",
                "COPY",
            ]
        )
        destination = aws_json(
            [
                "s3api",
                "head-object",
                "--bucket",
                destination_bucket,
                "--key",
                destination_key,
            ]
        )
        destination_etag = destination.get("ETag")
        if destination.get("ContentLength") != byte_count:
            raise ValueError("clone destination object length differs")
        return {
            "path": path,
            "bytes": byte_count,
            "source_etag": source_etag,
            "destination_etag": destination_etag,
        }

    with ThreadPoolExecutor(max_workers=min(workers, len(base_roster))) as executor:
        inventory = list(executor.map(copy_one, base_roster))
    receipt = build_clone_receipt(
        cell=cell,
        arm=arm,
        attempt_id=attempt_id,
        base_receipt=base_receipt,
        base_roster=base_roster,
        copy_inventory=inventory,
    )
    return receipt, canonical_json_bytes(inventory) + b"\n"
