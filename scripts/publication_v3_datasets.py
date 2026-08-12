#!/usr/bin/env python3
"""Dataset materialization descriptors for Publication V3."""

from __future__ import annotations

import hashlib
import json
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
OBJECT_FIELDS = frozenset({"role", "format", "path", "sha256", "bytes", "rows"})
MAX_DATASET_OBJECT_BYTES = 128 * 1024 * 1024
MAX_DATASET_METADATA_BYTES = 256 * 1024
METADATA_FIELDS = frozenset({"name", "metric", "dim", "n_train", "n_test", "k"})
GENERATED_METADATA_FIELDS = METADATA_FIELDS | {"generator", "seed"}


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _require_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        raise RuntimeError("dataset inspection requires pyarrow") from error
    return pa, pq


def _parquet_object(root: Path, path: Path, role: str) -> dict[str, object]:
    _, pq = _require_pyarrow()
    size = path.stat().st_size
    if size <= 8 or size > MAX_DATASET_OBJECT_BYTES:
        raise ValueError("staged dataset object exceeds its bounded Parquet size")
    with path.open("rb") as source:
        prefix = source.read(4)
        source.seek(-4, 2)
        suffix = source.read(4)
    if prefix != b"PAR1" or suffix != b"PAR1":
        raise ValueError("staged dataset object is not stock Parquet")
    return {
        "role": role,
        "format": "parquet",
        "path": path.relative_to(root).as_posix(),
        "sha256": _file_sha256(path),
        "bytes": size,
        "rows": pq.read_metadata(path).num_rows,
    }


def _dataset_objects(root: Path) -> list[dict[str, object]]:
    if not root.is_dir():
        raise ValueError("staged dataset must be a directory")
    train_paths = sorted(root.glob("train-*.parquet"))
    if (root / "train.parquet").is_file():
        if train_paths:
            raise ValueError("staged dataset has ambiguous train Parquet objects")
        train_paths = [root / "train.parquet"]
    required = [root / "test.parquet", root / "neighbors.parquet", root / "meta.json"]
    if not train_paths or any(not path.is_file() for path in required):
        raise ValueError("staged dataset protocol objects are incomplete")
    recognized = set(train_paths + required)
    actual = {path for path in root.rglob("*") if path.is_file()}
    if actual != recognized:
        raise ValueError("staged dataset contains unrecognized protocol objects")

    objects = [_parquet_object(root, path, "train") for path in train_paths]
    objects.append(_parquet_object(root, root / "test.parquet", "query"))
    objects.append(_parquet_object(root, root / "neighbors.parquet", "ground-truth"))
    metadata_path = root / "meta.json"
    metadata_size = metadata_path.stat().st_size
    if metadata_size <= 0 or metadata_size > MAX_DATASET_METADATA_BYTES:
        raise ValueError("staged dataset metadata exceeds its bound")
    objects.append(
        {
            "role": "metadata",
            "format": "json",
            "path": "meta.json",
            "sha256": _file_sha256(metadata_path),
            "bytes": metadata_size,
            "rows": 1,
        }
    )
    return objects


def _validate_embedding_schema(path: Path, dimensions: int, role: str) -> None:
    pa, pq = _require_pyarrow()
    schema = pq.read_schema(path)
    if schema.names != ["emb"]:
        raise ValueError(f"{role} Parquet schema must contain only emb")
    data_type = schema.field("emb").type
    if not (
        pa.types.is_fixed_size_list(data_type)
        and data_type.list_size == dimensions
        and pa.types.is_float32(data_type.value_type)
    ):
        raise ValueError(f"{role} embedding schema differs from the manifest")


def _validate_truth_schema(path: Path, expected_rows: int, k: int) -> None:
    pa, pq = _require_pyarrow()
    table = pq.read_table(path, columns=["neighbors_id"])
    if table.schema.names != ["neighbors_id"] or table.num_rows != expected_rows:
        raise ValueError("ground-truth rows differ from query rows")
    data_type = table.schema.field("neighbors_id").type
    if not (
        (pa.types.is_list(data_type) or pa.types.is_fixed_size_list(data_type))
        and (pa.types.is_int32(data_type.value_type) or pa.types.is_int64(data_type.value_type))
    ):
        raise ValueError("ground-truth neighbor schema is invalid")
    if pa.types.is_fixed_size_list(data_type):
        widths_are_valid = data_type.list_size >= k
    else:
        widths_are_valid = all(len(value) >= k for value in table.column(0).to_pylist())
    if not widths_are_valid:
        raise ValueError("ground-truth width is below the publication k")


def _validate_physical_dataset(
    root: Path, dataset: dict[str, object], objects: list[dict[str, object]]
) -> None:
    dimensions = int(dataset["dimensions"])
    expected_train_rows = int(dataset["scale"]["rows"])
    train_objects = [item for item in objects if item["role"] == "train"]
    if sum(int(item["rows"]) for item in train_objects) != expected_train_rows:
        raise ValueError("train row count differs from the manifest")
    for item in train_objects:
        _validate_embedding_schema(root / str(item["path"]), dimensions, "train")

    query_object = next(item for item in objects if item["role"] == "query")
    truth_object = next(item for item in objects if item["role"] == "ground-truth")
    query_rows = int(query_object["rows"])
    _validate_embedding_schema(root / str(query_object["path"]), dimensions, "query")
    try:
        metadata = json.loads((root / "meta.json").read_text())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("dataset metadata is not valid UTF-8 JSON") from error
    if not isinstance(metadata, dict) or frozenset(metadata) not in {
        METADATA_FIELDS,
        GENERATED_METADATA_FIELDS,
    }:
        raise ValueError("dataset metadata fields differ")
    if "generator" in metadata and (
        not isinstance(metadata["generator"], str)
        or not metadata["generator"]
        or not isinstance(metadata["seed"], int)
        or isinstance(metadata["seed"], bool)
        or metadata["seed"] < 0
    ):
        raise ValueError("generated dataset provenance is invalid")
    expected_metadata = {
        "name": dataset["id"],
        "metric": "euclidean" if dataset["metric"] == "l2" else dataset["metric"],
        "dim": dimensions,
        "n_train": expected_train_rows,
        "n_test": query_rows,
    }
    if any(metadata[key] != value for key, value in expected_metadata.items()):
        raise ValueError("dataset metadata differs from physical data or manifest")
    k = metadata["k"]
    if not isinstance(k, int) or isinstance(k, bool) or k < 10:
        raise ValueError("dataset metadata k is invalid")
    if int(truth_object["rows"]) != query_rows:
        raise ValueError("ground-truth rows differ from query rows")
    _validate_truth_schema(root / str(truth_object["path"]), query_rows, k)


def _object_identity(objects: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "role": item["role"],
            "format": item["format"],
            "path": item["path"],
            "sha256": item["sha256"],
            "bytes": item["bytes"],
            "rows": item["rows"],
        }
        for item in objects
    ]


def dataset_materialization_sha256(root: Path) -> str:
    return _materialization_sha256(_dataset_objects(root))


def _materialization_sha256(objects: list[dict[str, object]]) -> str:
    return hashlib.sha256(canonical_json_bytes(_object_identity(objects))).hexdigest()


def build_dataset_descriptor(dataset: dict[str, object]) -> dict[str, object]:
    source = dataset["source"]
    state = source["state"]
    if dataset["kind"] == "standard-ann" and state == "generated":
        raise ValueError("standard dataset cannot be replaced by generated data")
    if state == "generated":
        raise ValueError("generated dataset has no checksummed materialized bytes")
    if state == "unstaged":
        raise ValueError("external dataset is not staged")
    if state != "staged":
        raise ValueError("unsupported dataset source state")
    parsed = urlparse(str(source["url"]))
    if parsed.scheme != "file":
        raise ValueError("local descriptor inspection requires a staged file URL")
    root = Path(unquote(parsed.path))
    objects = _dataset_objects(root)
    _validate_physical_dataset(root, dataset, objects)
    content_sha256 = _materialization_sha256(objects)
    if content_sha256 != source["sha256"]:
        raise ValueError("staged dataset checksum differs from manifest")
    descriptor = {
        "schema_version": 1,
        "dataset_id": dataset["id"],
        "kind": dataset["kind"],
        "rows": dataset["scale"]["rows"],
        "dimensions": dataset["dimensions"],
        "metric": dataset["metric"],
        "materialization": "staged-parquet",
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
    if value["schema_version"] != 1 or any(
        value[key] != item for key, item in expected.items()
    ):
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
        role = item["role"]
        expected_format = "json" if role == "metadata" else "parquet"
        size_bound = (
            MAX_DATASET_METADATA_BYTES if role == "metadata" else MAX_DATASET_OBJECT_BYTES
        )
        if not path or path.startswith("/") or ".." in path.split("/") or path in paths:
            raise ValueError("dataset object paths must be relative and unique")
        paths.add(path)
        if (
            role not in {"train", "query", "ground-truth", "metadata"}
            or item["format"] != expected_format
            or not isinstance(item["bytes"], int)
            or isinstance(item["bytes"], bool)
            or item["bytes"] <= 0
            or item["bytes"] > size_bound
            or not isinstance(item["rows"], int)
            or isinstance(item["rows"], bool)
            or item["rows"] <= 0
            or len(object_digest) != 64
            or any(character not in "0123456789abcdef" for character in object_digest)
        ):
            raise ValueError("dataset object format, rows, or size is invalid")
    roles = [item["role"] for item in objects]
    if (
        roles.count("query") != 1
        or roles.count("ground-truth") != 1
        or roles.count("metadata") != 1
        or roles.count("train") < 1
    ):
        raise ValueError("dataset object roles are incomplete")
    if value["materialization"] != "staged-parquet" or not objects:
        raise ValueError("dataset descriptor materialization is invalid")
    if (
        hashlib.sha256(canonical_json_bytes(_object_identity(objects))).hexdigest()
        != content_digest
    ):
        raise ValueError("staged dataset object set differs")
    return value
