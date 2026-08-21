#!/usr/bin/env python3
"""Dataset materialization descriptors for Publication V3."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from urllib.parse import unquote, urlparse

if __package__:
    from scripts.publication_v3_beir import expected_beir_metadata
    from scripts.publication_v3_protocol import canonical_json_bytes
else:
    from publication_v3_beir import expected_beir_metadata
    from publication_v3_protocol import canonical_json_bytes


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
    expected_train_names = [
        f"train-{index:08}.parquet" for index in range(len(train_paths))
    ]
    if [path.name for path in train_paths] != expected_train_names:
        raise ValueError(
            "staged dataset train shard names are not contiguous and canonical"
        )
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


def _canonical_shards(root: Path, prefix: str) -> list[Path]:
    paths = sorted(root.glob(f"{prefix}-*.parquet"))
    expected = [f"{prefix}-{index:08}.parquet" for index in range(len(paths))]
    if [path.name for path in paths] != expected:
        raise ValueError(f"staged BEIR {prefix} shard names are not canonical")
    return paths


def _beir_objects(root: Path) -> list[dict[str, object]]:
    if not root.is_dir():
        raise ValueError("staged BEIR dataset must be a directory")
    corpus = _canonical_shards(root, "corpus")
    queries = _canonical_shards(root, "queries")
    required = [root / "qrels.parquet", root / "meta.json"]
    if not corpus or not queries or any(not path.is_file() for path in required):
        raise ValueError("staged BEIR protocol objects are incomplete")
    recognized = set(corpus + queries + required)
    actual = {path for path in root.rglob("*") if path.is_file()}
    if actual != recognized:
        raise ValueError("staged BEIR dataset contains unrecognized protocol objects")
    objects = [_parquet_object(root, path, "corpus") for path in corpus]
    objects.extend(_parquet_object(root, path, "query") for path in queries)
    objects.append(_parquet_object(root, root / "qrels.parquet", "qrels"))
    metadata_path = root / "meta.json"
    metadata_size = metadata_path.stat().st_size
    if metadata_size <= 0 or metadata_size > MAX_DATASET_METADATA_BYTES:
        raise ValueError("staged BEIR metadata exceeds its bound")
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
    field = schema.field("emb")
    data_type = field.type
    if not (
        not field.nullable
        and pa.types.is_fixed_size_list(data_type)
        and data_type.list_size == dimensions
        and pa.types.is_float32(data_type.value_type)
        and not data_type.value_field.nullable
    ):
        raise ValueError(f"{role} embedding schema differs from the manifest")


def _validate_truth_schema(
    path: Path, expected_rows: int, k: int, train_rows: int
) -> None:
    pa, pq = _require_pyarrow()
    table = pq.read_table(path, columns=["neighbors_id"])
    if table.schema.names != ["neighbors_id"] or table.num_rows != expected_rows:
        raise ValueError("ground-truth rows differ from query rows")
    field = table.schema.field("neighbors_id")
    data_type = field.type
    if not (
        not field.nullable
        and (pa.types.is_list(data_type) or pa.types.is_fixed_size_list(data_type))
        and (
            pa.types.is_int32(data_type.value_type)
            or pa.types.is_int64(data_type.value_type)
        )
        and not data_type.value_field.nullable
    ):
        raise ValueError("ground-truth neighbor schema is invalid")
    if pa.types.is_fixed_size_list(data_type):
        widths_are_valid = data_type.list_size >= k
    else:
        widths_are_valid = all(len(value) >= k for value in table.column(0).to_pylist())
    if not widths_are_valid:
        raise ValueError("ground-truth width is below the publication k")
    neighbors = table.column(0).to_pylist()
    if any(
        not isinstance(identifier, int) or identifier < 0 or identifier >= train_rows
        for row in neighbors
        for identifier in row
    ):
        raise ValueError("ground-truth contains an out-of-range train id")


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
    _validate_truth_schema(
        root / str(truth_object["path"]), query_rows, k, expected_train_rows
    )


def _validate_beir_row_schema(path: Path, dimensions: int, role: str):
    pa, pq = _require_pyarrow()
    table = pq.read_table(path)
    if table.schema.names != [
        "id",
        "text",
        "emb",
        "sparse_indices",
        "sparse_values",
    ]:
        raise ValueError(f"BEIR {role} Parquet schema fields differ")
    identifier = table.schema.field("id")
    text = table.schema.field("text")
    embedding = table.schema.field("emb")
    indices = table.schema.field("sparse_indices")
    values = table.schema.field("sparse_values")
    if not (
        not identifier.nullable
        and pa.types.is_string(identifier.type)
        and not text.nullable
        and pa.types.is_string(text.type)
        and not embedding.nullable
        and pa.types.is_fixed_size_list(embedding.type)
        and embedding.type.list_size == dimensions
        and pa.types.is_float32(embedding.type.value_type)
        and not embedding.type.value_field.nullable
        and not indices.nullable
        and pa.types.is_list(indices.type)
        and pa.types.is_int32(indices.type.value_type)
        and not indices.type.value_field.nullable
        and not values.nullable
        and pa.types.is_list(values.type)
        and pa.types.is_float32(values.type.value_type)
        and not values.type.value_field.nullable
    ):
        raise ValueError(f"BEIR {role} Parquet schema types differ")
    return table


def _validate_physical_beir_dataset(
    root: Path, dataset: dict[str, object], objects: list[dict[str, object]]
) -> None:
    import numpy as np

    pa, pq = _require_pyarrow()
    dimensions = int(dataset["dimensions"])
    expected_documents = int(dataset["scale"]["rows"])
    corpus_objects = [item for item in objects if item["role"] == "corpus"]
    query_objects = [item for item in objects if item["role"] == "query"]
    if sum(int(item["rows"]) for item in corpus_objects) != expected_documents:
        raise ValueError("BEIR corpus row count differs from the manifest")
    corpus_ids: set[str] = set()
    query_ids: set[str] = set()
    corpus_non_zero = 0
    query_non_zero = 0
    sparse_dimension = 0
    for role, role_objects, identifiers in (
        ("corpus", corpus_objects, corpus_ids),
        ("query", query_objects, query_ids),
    ):
        for item in role_objects:
            table = _validate_beir_row_schema(
                root / str(item["path"]), dimensions, role
            )
            ids = table.column("id").to_pylist()
            if any(not isinstance(value, str) or not value for value in ids):
                raise ValueError(f"BEIR {role} ids must be nonempty UTF-8 strings")
            if len(set(ids)) != len(ids) or identifiers.intersection(ids):
                raise ValueError(f"BEIR {role} ids must be unique")
            identifiers.update(ids)
            vectors = (
                table.column("emb")
                .combine_chunks()
                .values.to_numpy(zero_copy_only=False)
            )
            if not np.isfinite(vectors).all():
                raise ValueError(f"BEIR {role} embeddings contain non-finite values")
            index_rows = table.column("sparse_indices").to_pylist()
            value_rows = table.column("sparse_values").to_pylist()
            for index_row, value_row in zip(index_rows, value_rows, strict=True):
                if len(index_row) != len(value_row):
                    raise ValueError("BEIR sparse index/value cardinality differs")
                if any(
                    left >= right
                    for left, right in zip(index_row, index_row[1:], strict=False)
                ):
                    raise ValueError("BEIR sparse indices must be strictly increasing")
                if any(index < 0 for index in index_row):
                    raise ValueError("BEIR sparse indices must be nonnegative")
                if any(not math.isfinite(value) for value in value_row):
                    raise ValueError("BEIR sparse values contain non-finite values")
                if index_row:
                    sparse_dimension = max(sparse_dimension, index_row[-1] + 1)
            if role == "corpus":
                corpus_non_zero += sum(map(len, index_rows))
            else:
                query_non_zero += sum(map(len, index_rows))
    qrels = pq.read_table(root / "qrels.parquet")
    if qrels.schema != pa.schema(
        [
            pa.field("query_id", pa.string(), nullable=False),
            pa.field("corpus_id", pa.string(), nullable=False),
            pa.field("score", pa.int32(), nullable=False),
        ]
    ):
        raise ValueError("BEIR qrels Parquet schema differs")
    qrel_rows = list(
        zip(
            qrels.column("query_id").to_pylist(),
            qrels.column("corpus_id").to_pylist(),
            qrels.column("score").to_pylist(),
            strict=True,
        )
    )
    if not qrel_rows or qrel_rows != sorted(
        qrel_rows, key=lambda row: (row[0], row[1])
    ):
        raise ValueError("BEIR qrels must be nonempty and canonically sorted")
    if len({(row[0], row[1]) for row in qrel_rows}) != len(qrel_rows):
        raise ValueError("BEIR qrels contain duplicate query/document pairs")
    if any(row[2] <= 0 for row in qrel_rows):
        raise ValueError("BEIR qrels scores must be positive")
    qrel_queries = {row[0] for row in qrel_rows}
    qrel_documents = {row[1] for row in qrel_rows}
    if qrel_queries != query_ids or not qrel_documents.issubset(corpus_ids):
        raise ValueError("BEIR qrels do not close over query and corpus ids")
    try:
        metadata = json.loads((root / "meta.json").read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("BEIR metadata is not valid UTF-8 JSON") from error
    expected = expected_beir_metadata(str(dataset["id"]))
    expected.update(
        {
            "documents": len(corpus_ids),
            "queries": len(query_ids),
            "qrels": len(qrel_rows),
        }
    )
    expected["sparse"].update(
        {
            "dimensions": sparse_dimension,
            "corpus_non_zero": corpus_non_zero,
            "query_non_zero": query_non_zero,
        }
    )
    if not isinstance(metadata, dict):
        raise ValueError("BEIR metadata must be a JSON object")
    vocabulary_digest = metadata.get("sparse", {}).get("vocabulary_sha256")
    if (
        not isinstance(vocabulary_digest, str)
        or len(vocabulary_digest) != 64
        or any(character not in "0123456789abcdef" for character in vocabulary_digest)
    ):
        raise ValueError("BEIR sparse vocabulary digest is invalid")
    expected["sparse"]["vocabulary_sha256"] = vocabulary_digest
    if metadata != expected:
        raise ValueError(
            "BEIR metadata differs from physical data or encoder authority"
        )


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
        for item in sorted(objects, key=lambda item: str(item["path"]))
    ]


def dataset_materialization_sha256(root: Path, *, kind: str) -> str:
    if kind == "beir-hybrid":
        objects = _beir_objects(root)
    elif kind in {"standard-ann", "realistic-dense"}:
        objects = _dataset_objects(root)
    else:
        raise ValueError(f"unsupported dataset materialization kind: {kind}")
    return _materialization_sha256(objects)


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
    if dataset["kind"] == "beir-hybrid":
        objects = _beir_objects(root)
        _validate_physical_beir_dataset(root, dataset, objects)
    else:
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
        allowed_roles = (
            {"corpus", "query", "qrels", "metadata"}
            if dataset["kind"] == "beir-hybrid"
            else {"train", "query", "ground-truth", "metadata"}
        )
        expected_format = "json" if role == "metadata" else "parquet"
        size_bound = (
            MAX_DATASET_METADATA_BYTES
            if role == "metadata"
            else MAX_DATASET_OBJECT_BYTES
        )
        if not path or path.startswith("/") or ".." in path.split("/") or path in paths:
            raise ValueError("dataset object paths must be relative and unique")
        paths.add(path)
        if (
            role not in allowed_roles
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
    complete = roles.count("metadata") == 1 and (
        roles.count("query") >= 1
        and roles.count("qrels") == 1
        and roles.count("corpus") >= 1
        if dataset["kind"] == "beir-hybrid"
        else roles.count("query") == 1
        and roles.count("ground-truth") == 1
        and roles.count("train") >= 1
    )
    if not complete:
        raise ValueError("dataset object roles are incomplete")
    if value["materialization"] != "staged-parquet" or not objects:
        raise ValueError("dataset descriptor materialization is invalid")
    if (
        hashlib.sha256(canonical_json_bytes(_object_identity(objects))).hexdigest()
        != content_digest
    ):
        raise ValueError("staged dataset object set differs")
    return value
