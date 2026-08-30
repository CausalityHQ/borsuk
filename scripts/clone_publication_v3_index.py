#!/usr/bin/env python3
"""Server-side copy one immutable Publication V3 index into a mutable arm."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
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
EXPECTED_BUCKET_OWNER = "453182569524"


def _aws_json(arguments: list[str]) -> dict[str, object]:
    command = ["aws"]
    profile = os.environ.get("AWS_PROFILE")
    if profile:
        command.extend(("--profile", profile))
    completed = subprocess.run(
        [*command, *arguments, "--output", "json"],
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
            "--expected-bucket-owner",
            EXPECTED_BUCKET_OWNER,
        ]
    )
    if listing.get("KeyCount") != 0:
        raise ValueError("publication mutation requires a fresh clone prefix")

    def copy_one(item: dict[str, object]) -> dict[str, object]:
        path = item.get("path")
        byte_count = item.get("bytes")
        if (
            not isinstance(path, str)
            or not path
            or path.startswith("/")
            or ".." in path.split("/")
        ):
            raise ValueError("clone roster path is invalid")
        source_key = f"{source_prefix}/{path}"
        destination_key = f"{destination_prefix}/{path}"
        source = aws_json(
            [
                "s3api",
                "head-object",
                "--bucket",
                source_bucket,
                "--key",
                source_key,
                "--expected-bucket-owner",
                EXPECTED_BUCKET_OWNER,
            ]
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
                "--expected-bucket-owner",
                EXPECTED_BUCKET_OWNER,
                "--expected-source-bucket-owner",
                EXPECTED_BUCKET_OWNER,
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
                "--expected-bucket-owner",
                EXPECTED_BUCKET_OWNER,
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


def _read_canonical(path: Path, maximum_bytes: int) -> object:
    payload = path.read_bytes()
    if not payload or len(payload) > maximum_bytes:
        raise ValueError(f"{path} exceeds its bounded JSON contract")
    document = payload[:-1] if payload.endswith(b"\n") else payload
    try:
        value = json.loads(document)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path} is not valid JSON") from error
    if canonical_json_bytes(value) != document:
        raise ValueError(f"{path} is not canonical JSON")
    return value


def clone_index_from_files(
    *,
    cell_path: Path,
    arm_path: Path,
    attempt_id: str,
    base_receipt_path: Path,
    base_roster_path: Path,
    receipt_output: Path,
    inventory_output: Path,
    aws_json: AwsJson = _aws_json,
    workers: int = 32,
) -> dict[str, object]:
    cell = _read_canonical(cell_path, 512 * 1024)
    arm = _read_canonical(arm_path, 64 * 1024)
    base_receipt = _read_canonical(base_receipt_path, 256 * 1024)
    base_roster = _read_canonical(base_roster_path, 32 * 1024 * 1024)
    if not isinstance(cell, dict) or not isinstance(arm, dict):
        raise ValueError("clone cell and arm must be JSON objects")
    if not isinstance(base_receipt, dict) or not isinstance(base_roster, list):
        raise ValueError("clone base authority has invalid JSON types")
    receipt, inventory = clone_index_objects(
        cell=cell,
        arm=arm,
        attempt_id=attempt_id,
        base_receipt=base_receipt,
        base_roster=base_roster,
        aws_json=aws_json,
        workers=workers,
    )
    receipt_output.write_bytes(canonical_json_bytes(receipt) + b"\n")
    inventory_output.write_bytes(inventory)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cell", type=Path, required=True)
    parser.add_argument("--arm", type=Path, required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--base-receipt", type=Path, required=True)
    parser.add_argument("--base-roster", type=Path, required=True)
    parser.add_argument("--receipt-output", type=Path, required=True)
    parser.add_argument("--inventory-output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=32)
    args = parser.parse_args()
    clone_index_from_files(
        cell_path=args.cell,
        arm_path=args.arm,
        attempt_id=args.attempt_id,
        base_receipt_path=args.base_receipt,
        base_roster_path=args.base_roster,
        receipt_output=args.receipt_output,
        inventory_output=args.inventory_output,
        workers=args.workers,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"publication-v3 clone failed: {error}", file=__import__("sys").stderr)
        raise SystemExit(2) from None
