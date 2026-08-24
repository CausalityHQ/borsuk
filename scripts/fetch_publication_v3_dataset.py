#!/usr/bin/env python3
"""Fetch an authenticated staged-generated dataset roster with bounded parallelism."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

try:
    from scripts.publication_v3_protocol import canonical_json_bytes
except ModuleNotFoundError:
    from publication_v3_protocol import canonical_json_bytes


def _s3_parts(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not key
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("generated dataset object URI must be canonical S3")
    return parsed.netloc, key


def validated_object_plan(
    dataset: dict[str, object],
    receipt: dict[str, object],
    *,
    roles: frozenset[str],
) -> tuple[dict[str, object], ...]:
    """Bind selected objects to the promoted recipe and complete receipt."""

    source = dataset.get("source")
    if not isinstance(source, dict):
        raise ValueError("generated dataset fetch requires dataset authority")
    if source.get("state") != "staged-generated":
        raise ValueError("generated dataset fetch requires staged-generated authority")
    if receipt.get("schema_version") != 1 or receipt.get("adapter") != "synthetic":
        raise ValueError("generated dataset receipt schema or adapter differs")
    provenance = receipt.get("source_provenance")
    scale = dataset.get("scale")
    if (
        not isinstance(provenance, dict)
        or not isinstance(scale, dict)
        or receipt.get("dataset_id") != dataset.get("id")
        or provenance.get("dataset") != dataset.get("id")
        or provenance.get("kind") != dataset.get("kind")
        or provenance.get("rows") != scale.get("rows")
        or provenance.get("dimensions") != dataset.get("dimensions")
        or provenance.get("metric") != dataset.get("metric")
        or any(
            provenance.get(field) != source.get(field)
            for field in ("generator", "seed")
        )
    ):
        raise ValueError("generated dataset receipt dataset contract differs")
    if (
        provenance.get("generator_source_archive_sha256")
        != source.get("generator_source_archive_sha256")
        or receipt.get("source_archive_sha256")
        != source.get("generator_source_archive_sha256")
        or receipt.get("dataset_content_sha256") != source.get("sha256")
        or receipt.get("output_uri") != source.get("url")
        or receipt.get("terminal_uri") != source.get("receipt_uri")
    ):
        raise ValueError("generated dataset receipt authority differs")
    objects = receipt.get("objects")
    if not isinstance(objects, list) or not objects:
        raise ValueError("generated dataset receipt object roster is empty")
    normalized: list[dict[str, object]] = []
    paths: set[str] = set()
    prefix = str(source["url"]).rstrip("/") + "/"
    for item in objects:
        if not isinstance(item, dict) or frozenset(item) != frozenset(
            {"role", "format", "uri", "sha256", "bytes", "rows"}
        ):
            raise ValueError("generated dataset roster object fields differ")
        uri = str(item["uri"])
        path = uri.removeprefix(prefix)
        if (
            not uri.startswith(prefix)
            or not path
            or path.startswith("/")
            or ".." in path.split("/")
            or path in paths
        ):
            raise ValueError("generated dataset roster path differs or duplicates")
        digest = str(item["sha256"])
        size = item["bytes"]
        rows = item["rows"]
        if (
            len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size <= 0
            or isinstance(rows, bool)
            or not isinstance(rows, int)
            or rows <= 0
        ):
            raise ValueError("generated dataset roster object authority is invalid")
        paths.add(path)
        normalized.append({**item, "path": path})
    identity = [
        {
            "role": item["role"],
            "format": item["format"],
            "path": item["path"],
            "sha256": item["sha256"],
            "bytes": item["bytes"],
            "rows": item["rows"],
        }
        for item in sorted(normalized, key=lambda item: str(item["uri"]))
    ]
    if hashlib.sha256(canonical_json_bytes(identity)).hexdigest() != source["sha256"]:
        raise ValueError("generated dataset roster aggregate checksum differs")
    selected = tuple(item for item in normalized if item["role"] in roles)
    if not selected or {str(item["role"]) for item in selected} != roles:
        raise ValueError("generated dataset selected roles are incomplete")
    return selected


def fetch_objects(
    plan: tuple[dict[str, object], ...],
    *,
    output: Path,
    region: str,
    owner: str,
    workers: int,
) -> None:
    if not 1 <= workers <= 64:
        raise ValueError("generated dataset fetch workers must be in 1..=64")
    import boto3
    from botocore.config import Config

    output.mkdir(parents=True, exist_ok=True)
    client = boto3.client(
        "s3",
        region_name=region,
        config=Config(max_pool_connections=workers, retries={"mode": "adaptive"}),
    )

    def file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()

    def fetch(item: dict[str, object]) -> None:
        target = output / str(item["path"])
        target.parent.mkdir(parents=True, exist_ok=True)
        bucket, key = _s3_parts(str(item["uri"]))
        descriptor, temporary_name = tempfile.mkstemp(
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".partial",
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            client.download_file(
                bucket,
                key,
                str(temporary),
                ExtraArgs={"ExpectedBucketOwner": owner, "ChecksumMode": "ENABLED"},
            )
            if (
                temporary.stat().st_size != item["bytes"]
                or file_sha256(temporary) != item["sha256"]
            ):
                raise ValueError("downloaded generated dataset object differs")
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        tuple(executor.map(fetch, plan))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cell", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--roles", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()
    cell = json.loads(args.cell.read_text(encoding="utf-8"))
    dataset = cell["dataset"]
    source = dataset["source"]
    receipt_bytes = args.receipt.read_bytes()
    if hashlib.sha256(receipt_bytes).hexdigest() != source.get("receipt_sha256"):
        raise ValueError("generated dataset receipt bytes differ from manifest")
    receipt = json.loads(receipt_bytes)
    roles = frozenset(filter(None, args.roles.split(",")))
    plan = validated_object_plan(dataset, receipt, roles=roles)
    fetch_objects(
        plan,
        output=args.output,
        region=args.region,
        owner=args.owner,
        workers=args.workers,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
