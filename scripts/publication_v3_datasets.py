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
OBJECT_FIELDS = frozenset({"role", "format", "url", "sha256", "bytes"})


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _generated_identity(dataset: dict[str, object]) -> str:
    source = dataset["source"]
    identity = {
        "schema_version": 1,
        "dataset_id": dataset["id"],
        "rows": dataset["scale"]["rows"],
        "dimensions": dataset["dimensions"],
        "metric": dataset["metric"],
        "generator": source["generator"],
        "seed": source["seed"],
        "encoding": "parquet-f32-v1",
    }
    return hashlib.sha256(canonical_json_bytes(identity)).hexdigest()


def build_dataset_descriptor(dataset: dict[str, object]) -> dict[str, object]:
    source = dataset["source"]
    state = source["state"]
    if dataset["kind"] == "standard-ann" and state == "generated":
        raise ValueError("standard dataset cannot be replaced by generated data")
    objects: list[dict[str, object]] = []
    if state == "generated":
        content_sha256 = _generated_identity(dataset)
        materialization = "deterministic-generator"
    elif state == "staged":
        parsed = urlparse(str(source["url"]))
        if parsed.scheme != "file":
            raise ValueError("local descriptor inspection requires a staged file URL")
        path = Path(unquote(parsed.path))
        if path.suffix != ".parquet" or not path.is_file():
            raise ValueError("staged dataset must be one existing Parquet file")
        checksum = _file_sha256(path)
        if checksum != source["sha256"]:
            raise ValueError("staged dataset checksum differs from manifest")
        payload = path.read_bytes()
        if len(payload) < 8 or payload[:4] != b"PAR1" or payload[-4:] != b"PAR1":
            raise ValueError("staged dataset is not a stock Parquet object")
        content_sha256 = checksum
        materialization = "staged-parquet"
        objects = [
            {
                "role": "dataset",
                "format": "parquet",
                "url": source["url"],
                "sha256": checksum,
                "bytes": len(payload),
            }
        ]
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
    digest = str(value["content_sha256"])
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("dataset descriptor content checksum is invalid")
    objects = value["objects"]
    if not isinstance(objects, list):
        raise ValueError("dataset descriptor objects must be a list")
    for item in objects:
        if not isinstance(item, dict) or frozenset(item) != OBJECT_FIELDS:
            raise ValueError("dataset object fields differ")
        if item["format"] != "parquet" or not isinstance(item["bytes"], int) or item["bytes"] <= 0:
            raise ValueError("dataset object format or size is invalid")
    if value["materialization"] == "deterministic-generator":
        if objects or digest != _generated_identity(dataset):
            raise ValueError("generated dataset identity differs")
    elif value["materialization"] == "staged-parquet":
        if len(objects) != 1 or objects[0]["sha256"] != digest:
            raise ValueError("staged dataset object differs")
    else:
        raise ValueError("dataset descriptor materialization is invalid")
    return value
