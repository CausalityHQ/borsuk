#!/usr/bin/env python3
"""Dataset materialization descriptors for Publication V3."""

from __future__ import annotations

import hashlib
from pathlib import Path
from urllib.parse import unquote, urlparse

from scripts.publication_v3_protocol import canonical_json_bytes


DESCRIPTOR_FIELDS = frozenset(
    {
        "schema_version",
        "dataset_id",
        "kind",
        "rows",
        "dimensions",
        "metric",
        "materialization",
        "source",
        "content_sha256",
        "objects",
    }
)
OBJECT_FIELDS = frozenset({"role", "format", "path", "sha256", "bytes"})
MAX_DATASET_OBJECT_BYTES = 128 * 1024 * 1024


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _parquet_objects(root: Path) -> list[dict[str, object]]:
    paths = [root] if root.is_file() else sorted(root.rglob("*.parquet"))
    if not paths:
        raise ValueError("staged dataset has no Parquet objects")
    objects = []
    for path in paths:
        size = path.stat().st_size
        if size <= 8 or size > MAX_DATASET_OBJECT_BYTES:
            raise ValueError("staged dataset object exceeds its bounded Parquet size")
        with path.open("rb") as source:
            prefix = source.read(4)
            source.seek(-4, 2)
            suffix = source.read(4)
        if prefix != b"PAR1" or suffix != b"PAR1":
            raise ValueError("staged dataset object is not stock Parquet")
        relative = path.name if root.is_file() else path.relative_to(root).as_posix()
        objects.append(
            {
                "role": "dataset",
                "format": "parquet",
                "path": relative,
                "sha256": _file_sha256(path),
                "bytes": size,
            }
        )
    return objects


def dataset_materialization_sha256(root: Path) -> str:
    objects = _parquet_objects(root)
    identity = [
        {
            "path": item["path"],
            "sha256": item["sha256"],
            "bytes": item["bytes"],
        }
        for item in objects
    ]
    return hashlib.sha256(canonical_json_bytes(identity)).hexdigest()


def build_dataset_descriptor(dataset: dict[str, object]) -> dict[str, object]:
    source = dataset["source"]
    state = source["state"]
    if dataset["kind"] == "standard-ann" and state == "generated":
        raise ValueError("standard dataset cannot be replaced by generated data")
    objects: list[dict[str, object]] = []
    if state == "generated":
        raise ValueError("generated dataset has no checksummed materialized bytes")
    elif state == "staged":
        parsed = urlparse(str(source["url"]))
        if parsed.scheme != "file":
            raise ValueError("local descriptor inspection requires a staged file URL")
        root = Path(unquote(parsed.path))
        objects = _parquet_objects(root)
        content_sha256 = dataset_materialization_sha256(root)
        if content_sha256 != source["sha256"]:
            raise ValueError("staged dataset checksum differs from manifest")
        materialization = "staged-parquet"
    elif state == "unstaged":
        raise ValueError("external dataset is not staged")
    else:
        raise ValueError("unsupported dataset source state")
    descriptor = {
        "schema_version": 1,
        "dataset_id": dataset["id"],
        "kind": dataset["kind"],
        "rows": dataset["scale"]["rows"],
        "dimensions": dataset["dimensions"],
        "metric": dataset["metric"],
        "materialization": materialization,
        "source": source,
        "content_sha256": content_sha256,
        "objects": objects,
    }
    return validate_dataset_descriptor(descriptor, dataset)


def validate_dataset_descriptor(
    value: dict[str, object], dataset: dict[str, object]
) -> dict[str, object]:
    if frozenset(value) != DESCRIPTOR_FIELDS:
        raise ValueError("dataset descriptor fields differ")
    expected = {
        "dataset_id": dataset["id"],
        "kind": dataset["kind"],
        "rows": dataset["scale"]["rows"],
        "dimensions": dataset["dimensions"],
        "metric": dataset["metric"],
        "source": dataset["source"],
    }
    if value["schema_version"] != 1 or any(value[key] != item for key, item in expected.items()):
        raise ValueError("dataset descriptor differs from manifest")
    content_digest = str(value["content_sha256"])
    if len(content_digest) != 64 or any(
        character not in "0123456789abcdef" for character in content_digest
    ):
        raise ValueError("dataset descriptor content checksum is invalid")
    objects = value["objects"]
    if not isinstance(objects, list):
        raise ValueError("dataset descriptor objects must be a list")
    paths: set[str] = set()
    for item in objects:
        if not isinstance(item, dict) or frozenset(item) != OBJECT_FIELDS:
            raise ValueError("dataset object fields differ")
        path = str(item["path"])
        object_digest = str(item["sha256"])
        if not path or path.startswith("/") or ".." in path.split("/") or path in paths:
            raise ValueError("dataset object paths must be relative and unique")
        paths.add(path)
        if (
            item["format"] != "parquet"
            or not isinstance(item["bytes"], int)
            or item["bytes"] <= 0
            or item["bytes"] > MAX_DATASET_OBJECT_BYTES
            or len(object_digest) != 64
            or any(
                character not in "0123456789abcdef" for character in object_digest
            )
        ):
            raise ValueError("dataset object format or size is invalid")
    if value["materialization"] != "staged-parquet" or not objects:
        raise ValueError("dataset descriptor materialization is invalid")
    identity = [
        {
            "path": item["path"],
            "sha256": item["sha256"],
            "bytes": item["bytes"],
        }
        for item in objects
    ]
    if hashlib.sha256(canonical_json_bytes(identity)).hexdigest() != content_digest:
        raise ValueError("staged dataset object set differs")
    return value
