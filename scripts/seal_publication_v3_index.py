#!/usr/bin/env python3
"""Build an exact immutable Publication V3 roster from an S3 index prefix."""

from __future__ import annotations

import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Protocol
from urllib.parse import urlparse

try:
    from scripts.publication_v3_protocol import canonical_json_bytes
    from scripts.publication_v3_results import validate_object_roster
except ModuleNotFoundError:
    from publication_v3_protocol import canonical_json_bytes
    from publication_v3_results import validate_object_roster


class _Body(Protocol):
    def read(self) -> bytes: ...
    def close(self) -> None: ...


class _S3(Protocol):
    def get_paginator(self, name: str) -> object: ...
    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]: ...


ParquetRows = Callable[[str, bytes], int]


def session_configuration(profile: str | None, region: str) -> dict[str, str]:
    configuration = {"region_name": region}
    if profile:
        configuration["profile_name"] = profile
    return configuration


def classify_index_object(path: str) -> tuple[str, str]:
    """Return the evidence role and truthful physical format for a known path."""

    if not path or path.startswith("/") or ".." in path.split("/"):
        raise ValueError("index object path must be relative and canonical")
    if path.startswith("segments/") and path.endswith(".parquet"):
        return "data-bundle", "parquet"
    if path.startswith("id-directory/") and path.endswith(".parquet"):
        return "directory", "parquet"
    if path.startswith("fidx/") and path.endswith(".fidx"):
        return "query-page", "packed"
    if path.endswith(".arrow") and path.startswith(
        (
            "global-cell-cards/v15/groups/",
            "global-leaf/",
            "vectors/",
            "late-interaction/",
        )
    ):
        return "query-page", "arrow-ipc"
    if path.endswith(".parquet") and path.startswith(
        (
            "global-leaf/",
            "global-cell-cards/v15/roots/",
            "logical-cell-catalogs/",
            "manifests/",
            "positioned-log/",
            "routing/",
            "lexical/",
            "tombstones/",
        )
    ):
        return "query-page", "parquet"
    if (
        path == "collection/CURRENT"
        or path.startswith("collection/snapshots/")
        or path == "lane-log/ACTIVE"
        or (path.startswith("lane-log/lanes/") and path.endswith("/HEAD"))
        or (path.startswith("positioned-log/heads/") and path.endswith(".json"))
    ):
        return "control", "json"
    if (
        path.startswith("id-directory/claim-pages/")
        and path.endswith("/STATE")
    ) or (path.startswith("transactions/") and path.endswith("/STATE")):
        return "control", "packed"
    raise ValueError(f"index object path has no checked physical role: {path}")


def _validate_physical_bytes(path: str, physical_format: str, body: bytes) -> None:
    if not body:
        raise ValueError(f"index object is empty: {path}")
    if physical_format == "parquet" and not (
        body.startswith(b"PAR1") and body.endswith(b"PAR1")
    ):
        raise ValueError(f"Parquet object has invalid boundary magic: {path}")
    if physical_format == "arrow-ipc" and not (
        body.startswith(b"ARROW1") and body.endswith(b"ARROW1")
    ):
        raise ValueError(f"Arrow IPC object has invalid boundary magic: {path}")
    if physical_format == "json":
        try:
            value = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"JSON control object is invalid: {path}") from error
        if not isinstance(value, dict):
            raise ValueError(f"JSON control object is not a mapping: {path}")
    if physical_format == "packed" and path.endswith(".fidx") and len(body) < 96:
        raise ValueError(f"packed filter index is truncated: {path}")
    if physical_format == "packed" and path.endswith("/STATE") and body[:4] not in {
        b"BCL1",
        b"BWS1",
    }:
        raise ValueError(f"packed control object has invalid magic: {path}")


def _default_parquet_rows(path: str, body: bytes) -> int:
    if not path.startswith("segments/"):
        return 0
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "index sealing requires scripts/requirements-format-bench.txt"
        ) from error
    rows = pq.ParquetFile(pa.BufferReader(body)).metadata.num_rows
    if rows <= 0:
        raise ValueError(f"segment Parquet object has no rows: {path}")
    return int(rows)


def _list_objects(client: _S3, *, bucket: str, prefix: str) -> list[dict[str, object]]:
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    objects: list[dict[str, object]] = []
    for page in pages:
        for item in page.get("Contents", []):
            key = str(item.get("Key", ""))
            if not key.startswith(prefix) or key == prefix:
                raise ValueError("S3 listing escaped the exact index prefix")
            relative = key[len(prefix) :]
            size = item.get("Size")
            etag = item.get("ETag")
            if (
                isinstance(size, bool)
                or not isinstance(size, int)
                or size <= 0
                or not isinstance(etag, str)
            ):
                raise ValueError("S3 index listing metadata is invalid")
            objects.append({"key": key, "path": relative, "bytes": size, "etag": etag})
    if not objects:
        raise ValueError("S3 index prefix is empty")
    objects.sort(key=lambda item: str(item["path"]))
    if len({str(item["path"]) for item in objects}) != len(objects):
        raise ValueError("S3 index listing paths are not unique")
    return objects


def seal_index_inventory(
    client: _S3,
    *,
    bucket: str,
    prefix: str,
    parquet_rows: ParquetRows = _default_parquet_rows,
    workers: int = 8,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Hash and classify every object, then reject any concurrent prefix drift."""

    if not bucket or not prefix or not prefix.endswith("/"):
        raise ValueError("index bucket and slash-terminated prefix are required")
    if workers <= 0 or workers > 32:
        raise ValueError("seal workers must be in 1..=32")
    before = _list_objects(client, bucket=bucket, prefix=prefix)

    def seal_one(listed: dict[str, object]) -> dict[str, object]:
        response = client.get_object(Bucket=bucket, Key=str(listed["key"]))
        body_stream = response.get("Body")
        if body_stream is None or not hasattr(body_stream, "read"):
            raise ValueError("S3 object response has no streaming body")
        try:
            body = body_stream.read()
        finally:
            close = getattr(body_stream, "close", None)
            if callable(close):
                close()
        if (
            len(body) != listed["bytes"]
            or response.get("ContentLength") != listed["bytes"]
            or response.get("ETag") != listed["etag"]
        ):
            raise ValueError("S3 object changed while sealing")
        path = str(listed["path"])
        role, physical_format = classify_index_object(path)
        _validate_physical_bytes(path, physical_format, body)
        rows = parquet_rows(path, body) if physical_format == "parquet" else 0
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            raise ValueError("Parquet row reader returned an invalid count")
        return {
            "role": role,
            "path": path,
            "format": physical_format,
            "bytes": len(body),
            "rows": rows,
            "checksum": hashlib.sha256(body).hexdigest(),
            "etag": listed["etag"],
        }

    with ThreadPoolExecutor(max_workers=workers) as executor:
        roster = list(executor.map(seal_one, before))
    after = _list_objects(client, bucket=bucket, prefix=prefix)
    if before != after:
        raise ValueError("S3 index prefix changed while sealing")
    inventory = [
        {key: item[key] for key in ("path", "bytes", "checksum", "etag")}
        for item in roster
    ]
    return roster, inventory


def _s3_parts(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    prefix = parsed.path.lstrip("/").rstrip("/") + "/"
    if parsed.scheme != "s3" or not parsed.netloc or prefix == "/":
        raise ValueError("index URI must be an exact nonempty S3 prefix")
    return parsed.netloc, prefix


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index-uri", required=True)
    parser.add_argument("--logical-rows", required=True, type=int)
    parser.add_argument("--logical-cells", required=True, type=int)
    parser.add_argument("--roster-output", required=True, type=Path)
    parser.add_argument("--inventory-output", required=True, type=Path)
    parser.add_argument("--profile")
    parser.add_argument("--region", required=True)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    import boto3

    bucket, prefix = _s3_parts(args.index_uri)
    client = boto3.Session(**session_configuration(args.profile, args.region)).client("s3")
    roster, inventory = seal_index_inventory(
        client, bucket=bucket, prefix=prefix, workers=args.workers
    )
    summary = validate_object_roster(
        roster, logical_rows=args.logical_rows, logical_cells=args.logical_cells
    )
    args.roster_output.write_bytes(canonical_json_bytes(roster) + b"\n")
    args.inventory_output.write_bytes(canonical_json_bytes(inventory) + b"\n")
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
