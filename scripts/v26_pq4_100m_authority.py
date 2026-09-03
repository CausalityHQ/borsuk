#!/usr/bin/env python3
"""Strict authority contract for the V26 PQ4 100M corpus partitions."""

from __future__ import annotations

import copy
import json
from typing import Any

_SCHEMA = "borsuk-v26-pq4-100m-corpus-authority-v1"
_DATASET_ID = "synthetic-clustered-100m-96"
_GENERATOR = "synthetic-clustered-v1"
_PHYSICAL_SCHEMA = "emb:fixed-size-list<element:f32;96>:non-null"
_TOTAL_ROWS = 100_000_000
_PARTITION_ROWS = 10_000_000
_PARTITIONS = 10


def _object(value: Any, keys: set[str], role: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != keys:
        raise ValueError(f"{role} schema differs")
    return value


def _integer(value: Any, role: str, *, positive: bool = False) -> int:
    if type(value) is not int or (positive and value <= 0):
        raise ValueError(f"{role} differs")
    return value


def _text(value: Any, role: str) -> str:
    if type(value) is not str or not value or any(character.isspace() for character in value):
        raise ValueError(f"{role} differs")
    return value


def _digest(value: Any, size: int, role: str) -> str:
    text = _text(value, role)
    if len(text) != size or any(character not in "0123456789abcdef" for character in text):
        raise ValueError(f"{role} differs")
    return text


def _identity(value: Any, expected_role: str) -> dict[str, Any]:
    identity = _object(
        value,
        {"encoded_bytes", "role", "sha256", "uri"},
        f"{expected_role} identity",
    )
    if identity["role"] != expected_role:
        raise ValueError(f"{expected_role} role differs")
    _digest(identity["sha256"], 64, f"{expected_role} SHA-256")
    _integer(identity["encoded_bytes"], f"{expected_role} length", positive=True)
    uri = _text(identity["uri"], f"{expected_role} URI")
    if not uri.startswith("s3://") or uri.endswith("/"):
        raise ValueError(f"{expected_role} URI differs")
    return identity


def validate_partition_authority(value: Any) -> dict[str, object]:
    """Validate and return one exact V26 100M corpus authority value."""

    authority = _object(
        value,
        {"binaries", "dataset", "evaluation", "partitions", "schema", "source"},
        "PQ4 100M authority",
    )
    if authority["schema"] != _SCHEMA:
        raise ValueError("PQ4 100M authority schema differs")

    source = _object(authority["source"], {"archive_sha256", "commit"}, "source")
    _digest(source["archive_sha256"], 64, "source archive SHA-256")
    _digest(source["commit"], 40, "source commit")

    dataset = _object(
        authority["dataset"],
        {
            "dimensions",
            "generator",
            "group_size",
            "id",
            "metric",
            "physical_schema",
            "queries",
            "seed",
            "total_rows",
        },
        "dataset",
    )
    expected_dataset = {
        "dimensions": 96,
        "generator": _GENERATOR,
        "group_size": 100,
        "id": _DATASET_ID,
        "metric": "cosine",
        "physical_schema": _PHYSICAL_SCHEMA,
        "queries": 100,
        "total_rows": _TOTAL_ROWS,
    }
    for key, expected in expected_dataset.items():
        if dataset[key] != expected or type(dataset[key]) is not type(expected):
            raise ValueError(f"dataset {key} differs")
    _integer(dataset["seed"], "dataset seed", positive=True)

    binaries = authority["binaries"]
    if type(binaries) is not list or len(binaries) != 3:
        raise ValueError("binary inventory differs")
    all_identities = [
        _identity(item, role)
        for item, role in zip(
            binaries,
            ("synthetic-generator", "pq4-stage", "pq4-build"),
            strict=True,
        )
    ]

    partitions = authority["partitions"]
    if type(partitions) is not list or len(partitions) != _PARTITIONS:
        raise ValueError("partition inventory differs")
    for ordinal, raw_partition in enumerate(partitions):
        partition = _object(
            raw_partition,
            {
                "files",
                "manifest",
                "ordinal_end",
                "ordinal_start",
                "shard_ordinal",
            },
            "partition",
        )
        start = ordinal * _PARTITION_ROWS
        end = start + _PARTITION_ROWS
        if (
            _integer(partition["shard_ordinal"], "shard ordinal") != ordinal
            or _integer(partition["ordinal_start"], "partition start") != start
            or _integer(partition["ordinal_end"], "partition end") != end
        ):
            raise ValueError("partition topology differs")
        all_identities.append(
            _identity(partition["manifest"], f"partition-manifest-{ordinal:04}")
        )
        files = partition["files"]
        if type(files) is not list or not files:
            raise ValueError("partition file inventory differs")
        next_ordinal = start
        for file_ordinal, raw_file in enumerate(files):
            file = _object(
                raw_file,
                {
                    "encoded_bytes",
                    "ordinal_end",
                    "ordinal_start",
                    "physical_schema",
                    "role",
                    "rows",
                    "sha256",
                    "uri",
                },
                "training file",
            )
            expected_role = f"training-shard-{ordinal:04}-{file_ordinal:04}"
            identity = _identity(
                {key: file[key] for key in ("encoded_bytes", "role", "sha256", "uri")},
                expected_role,
            )
            file_start = _integer(file["ordinal_start"], "training file start")
            file_end = _integer(file["ordinal_end"], "training file end")
            rows = _integer(file["rows"], "training file rows", positive=True)
            if (
                file["physical_schema"] != _PHYSICAL_SCHEMA
                or type(file["physical_schema"]) is not str
                or file_start != next_ordinal
                or file_end <= file_start
                or file_end - file_start != rows
                or file_end > end
            ):
                raise ValueError("training file authority differs")
            next_ordinal = file_end
            all_identities.append(identity)
        if next_ordinal != end:
            raise ValueError("partition file coverage differs")

    evaluation = _object(
        authority["evaluation"],
        {"query", "truth", "writer_shard_ordinal"},
        "evaluation",
    )
    if _integer(evaluation["writer_shard_ordinal"], "evaluation writer") != 0:
        raise ValueError("evaluation writer differs")
    all_identities.extend(
        [
            _identity(evaluation["query"], "query-parquet"),
            _identity(evaluation["truth"], "truth-parquet"),
        ]
    )
    uris = [identity["uri"] for identity in all_identities]
    if len(uris) != len(set(uris)):
        raise ValueError("artifact URIs are not role-disjoint")
    return copy.deepcopy(authority)


def canonical_partition_authority_bytes(value: Any) -> bytes:
    """Serialize one validated authority as sorted compact newline JSON."""

    normalized = validate_partition_authority(value)
    return json.dumps(
        normalized,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8") + b"\n"


def stage_partition_manifest(value: Any, shard_ordinal: int) -> dict[str, object]:
    """Project one validated corpus partition into the Rust stager's manifest."""

    if type(shard_ordinal) is not int or shard_ordinal not in range(_PARTITIONS):
        raise ValueError("stage shard ordinal differs")
    authority = validate_partition_authority(value)
    dataset = authority["dataset"]
    partition = authority["partitions"][shard_ordinal]
    ordered_inputs = []
    for file in partition["files"]:
        ordered_inputs.append(
            {
                "authority_kind": "training-shard",
                "dimensions": dataset["dimensions"],
                "identity": {
                    "digest": file["sha256"],
                    "digest_algorithm": "sha256",
                    "encoded_bytes": file["encoded_bytes"],
                    "role": file["role"],
                    "uri": file["uri"],
                },
                "metric": dataset["metric"],
                "ordinal_end": file["ordinal_end"],
                "ordinal_start": file["ordinal_start"],
                "physical_schema": file["physical_schema"],
                "rows": file["rows"],
            }
        )
    return {
        "dataset_id": dataset["id"],
        "ordered_inputs": ordered_inputs,
        "ordinal_end": partition["ordinal_end"],
        "ordinal_start": partition["ordinal_start"],
        "schema": "borsuk-v26-pq4-partition-manifest-v1",
        "shard_ordinal": shard_ordinal,
    }


def canonical_stage_partition_manifest_bytes(value: Any, shard_ordinal: int) -> bytes:
    """Serialize one exact Rust staging manifest as sorted compact newline JSON."""

    return json.dumps(
        stage_partition_manifest(value, shard_ordinal),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8") + b"\n"
