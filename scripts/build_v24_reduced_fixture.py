#!/usr/bin/env python3
"""Build deterministic reduced V24 Parquet inputs for claim-ineligible preflight."""

from __future__ import annotations

import hashlib
import json
import pathlib
import shutil

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

_DIMENSIONS = 96
_SEED = 1_311_768_467_463_790_320
_U32_MAX = (1 << 32) - 1


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _vectors(ordinals: np.ndarray) -> np.ndarray:
    ordinal_words = np.asarray(ordinals, dtype=np.uint64).reshape(-1, 1)
    dimension_words = np.arange(1, _DIMENSIONS + 1, dtype=np.uint64).reshape(1, -1)
    with np.errstate(over="ignore"):
        words = (
            ordinal_words * np.uint64(0x9E3779B97F4A7C15)
            + dimension_words * np.uint64(0xBF58476D1CE4E5B9)
            + np.uint64(_SEED)
        )
        words ^= words >> np.uint64(30)
        words *= np.uint64(0xBF58476D1CE4E5B9)
        words ^= words >> np.uint64(27)
        words *= np.uint64(0x94D049BB133111EB)
        words ^= words >> np.uint64(31)
    mantissas = (words >> np.uint64(40)).astype(np.uint32)
    values = mantissas.astype(np.float32) / np.float32(1 << 23) - np.float32(1.0)
    norms = np.sqrt(np.sum(values.astype(np.float64) ** 2, axis=1)).astype(np.float32)
    return values / norms.reshape(-1, 1)


def _fixed_f32(values: np.ndarray) -> pa.FixedSizeListArray:
    flat = pa.array(values.reshape(-1), type=pa.float32())
    return pa.FixedSizeListArray.from_arrays(
        flat,
        type=pa.list_(pa.field("element", pa.float32(), nullable=False), _DIMENSIONS),
    )


def _fixed_u32(values: np.ndarray, width: int) -> pa.FixedSizeListArray:
    flat = pa.array(values.reshape(-1), type=pa.uint32())
    return pa.FixedSizeListArray.from_arrays(
        flat,
        type=pa.list_(pa.field("element", pa.uint32(), nullable=False), width),
    )


def _write_parquet(path: pathlib.Path, table: pa.Table) -> None:
    pq.write_table(
        table,
        path,
        compression="NONE",
        use_dictionary=False,
        write_statistics=False,
        version="2.6",
    )


def _identity(
    path: pathlib.Path,
    role: str,
    generation: str,
    *,
    uri_name: str | None = None,
) -> dict[str, object]:
    payload = path.read_bytes()
    return {
        "digest": hashlib.sha256(payload).hexdigest(),
        "digest_algorithm": "sha256",
        "encoded_bytes": len(payload),
        "generation": generation,
        "role": role,
        "uri": f"s3://borsuk-v24-reduced/{uri_name or path.name}",
    }


def build_reduced_fixture(
    root: pathlib.Path,
    *,
    source_rows: int,
    witness_count: int,
    page_count: int,
    query_count: int,
    generation: str,
) -> dict[str, object]:
    """Write one deterministic reduced training/page/query/truth fixture."""

    if (
        root.is_symlink()
        or not root.is_dir()
        or any(root.iterdir())
        or source_rows < 257
        or not 2 <= witness_count <= source_rows
        or page_count < 8
        or query_count != 32
        or not generation
    ):
        raise ValueError("V24 reduced fixture authority differs")

    source_ordinals = np.arange(source_rows, dtype=np.uint64)
    source_vectors = _vectors(source_ordinals)
    construction_schema = pa.schema(
        [
            pa.field("source_ordinal", pa.uint64(), nullable=False),
            pa.field(
                "vector",
                pa.list_(
                    pa.field("element", pa.float32(), nullable=False), _DIMENSIONS
                ),
                nullable=False,
            ),
        ]
    )
    construction = root / "construction-rows.parquet"
    _write_parquet(
        construction,
        pa.Table.from_arrays(
            [pa.array(source_ordinals), _fixed_f32(source_vectors)],
            schema=construction_schema,
        ),
    )
    construction_sha256 = hashlib.sha256(construction.read_bytes()).hexdigest()

    page_order = np.concatenate(
        [
            np.arange(page, source_rows, page_count, dtype=np.uint64)
            for page in range(page_count)
        ]
    )
    page_schema = pa.schema(
        [
            pa.field("page_ordinal", pa.uint32(), nullable=False),
            pa.field("replica", pa.bool_(), nullable=False),
            pa.field("record_id", pa.string(), nullable=False),
            pa.field(
                "vector",
                pa.list_(
                    pa.field("element", pa.float32(), nullable=False), _DIMENSIONS
                ),
                nullable=False,
            ),
        ],
        metadata={
            b"construction_rows_sha256": construction_sha256.encode(),
            b"generation": generation.encode(),
        },
    )
    pages = root / "page-rows.parquet"
    _write_parquet(
        pages,
        pa.Table.from_arrays(
            [
                pa.array(page_order % page_count, type=pa.uint32()),
                pa.array(np.zeros(source_rows, dtype=np.bool_), type=pa.bool_()),
                pa.array([str(int(value)) for value in page_order], type=pa.string()),
                _fixed_f32(_vectors(page_order)),
            ],
            schema=page_schema,
        ),
    )

    query_ordinals = np.arange(query_count, dtype=np.uint32)
    query_schema = pa.schema(
        [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field(
                "vector",
                pa.list_(
                    pa.field("element", pa.float32(), nullable=False), _DIMENSIONS
                ),
                nullable=False,
            ),
        ]
    )
    queries = root / "queries.parquet"
    _write_parquet(
        queries,
        pa.Table.from_arrays(
            [pa.array(query_ordinals), _fixed_f32(_vectors(query_ordinals))],
            schema=query_schema,
        ),
    )

    primary = np.empty((query_count, 10), dtype=np.uint32)
    replica = np.full((query_count, 10), _U32_MAX, dtype=np.uint32)
    oracle = np.full((query_count, 8), _U32_MAX, dtype=np.uint32)
    for query in range(query_count):
        candidates = [
            int((query + rank * _DIMENSIONS) % source_rows) for rank in range(10)
        ]
        primary[query] = [candidate % page_count for candidate in candidates]
        pages_for_query = sorted(set(int(page) for page in primary[query]))[:8]
        oracle[query, : len(pages_for_query)] = pages_for_query
    truth_schema = pa.schema(
        [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field(
                "primary_pages",
                pa.list_(pa.field("element", pa.uint32(), nullable=False), 10),
                nullable=False,
            ),
            pa.field(
                "replica_pages",
                pa.list_(pa.field("element", pa.uint32(), nullable=False), 10),
                nullable=False,
            ),
            pa.field(
                "oracle_pages",
                pa.list_(pa.field("element", pa.uint32(), nullable=False), 8),
                nullable=False,
            ),
        ]
    )
    neighbors = root / "neighbors.parquet"
    _write_parquet(
        neighbors,
        pa.Table.from_arrays(
            [
                pa.array(query_ordinals),
                _fixed_u32(primary, 10),
                _fixed_u32(replica, 10),
                _fixed_u32(oracle, 8),
            ],
            schema=truth_schema,
        ),
    )

    manifest = {
        "claim_eligible": False,
        "generation": generation,
        "inputs": [_identity(construction, "construction-rows-parquet", generation)],
        "output_uris": {
            "witness-graph": "s3://borsuk-v24-reduced/witness-graph.arrow",
            "witnesses-arrow": "s3://borsuk-v24-reduced/witnesses.arrow",
        },
        "phase": "witness-training",
        "schema": "borsuk-v24-local-manifest-v1",
        "seed": _SEED,
        "source_row_count": source_rows,
        "witness_count": witness_count,
    }
    (root / "training-manifest.json").write_bytes(_canonical_json_bytes(manifest))
    return manifest


def _read_canonical_object(path: pathlib.Path) -> tuple[bytes, dict[str, object]]:
    raw = path.read_bytes()
    value = json.loads(raw)
    if type(value) is not dict or raw != _canonical_json_bytes(value):  # noqa: E721
        raise ValueError("V24 reduced parent result differs")
    return raw, value


def _copy_exact(source: pathlib.Path, destination: pathlib.Path) -> None:
    if source.is_symlink() or not source.is_file() or destination.exists():
        raise ValueError("V24 reduced phase input authority differs")
    shutil.copyfile(source, destination)


def prepare_posting_phase(
    root: pathlib.Path,
    training_output: pathlib.Path,
) -> pathlib.Path:
    """Bind one completed reduced training phase into posting inputs."""

    result_path = training_output / "result.json"
    result_raw, result = _read_canonical_object(result_path)
    expected_keys = {
        "claim_eligible",
        "distance_backend",
        "generation",
        "inputs",
        "outputs",
        "phase",
        "schema",
        "seed",
        "source_row_count",
        "witness_count",
    }
    if (
        set(result) != expected_keys
        or result["schema"] != "borsuk-v24-training-result-v1"
        or result["claim_eligible"] is not False
        or result["phase"] != "witness-training"
        or type(result["inputs"]) is not list
        or len(result["inputs"]) != 1
        or type(result["outputs"]) is not list
        or [identity["role"] for identity in result["outputs"]]
        != ["witness-graph", "witnesses-arrow"]
    ):
        raise ValueError("V24 reduced training result authority differs")
    generation = str(result["generation"])
    graph = training_output / "witness-graph.arrow"
    witnesses = training_output / "witnesses.arrow"
    observed_outputs = [
        _identity(graph, "witness-graph", generation),
        _identity(witnesses, "witnesses-arrow", generation),
    ]
    if result["outputs"] != observed_outputs:
        raise ValueError("V24 reduced training outputs differ")

    input_dir = root / "posting-input"
    input_dir.mkdir(mode=0o700)
    _copy_exact(result_path, input_dir / "training-result.json")
    _copy_exact(graph, input_dir / "witness-graph.arrow")
    _copy_exact(witnesses, input_dir / "witnesses.arrow")
    _copy_exact(root / "page-rows.parquet", input_dir / "page-rows.parquet")
    inputs = [
        _identity(
            input_dir / "training-result.json",
            "training-result",
            generation,
        ),
        _identity(input_dir / "witness-graph.arrow", "witness-graph", generation),
        _identity(input_dir / "witnesses.arrow", "witnesses-arrow", generation),
        _identity(
            input_dir / "page-rows.parquet",
            "page-rows-parquet",
            generation,
        ),
    ]
    manifest = {
        "claim_eligible": False,
        "construction_rows_digest": result["inputs"][0]["digest"],
        "generation": generation,
        "inputs": inputs,
        "output_uris": {
            "witness-postings": "s3://borsuk-v24-reduced/witness-postings.arrow"
        },
        "parent_result_sha256": hashlib.sha256(result_raw).hexdigest(),
        "phase": "posting-construction",
        "schema": "borsuk-v24-local-manifest-v1",
        "source_row_count": result["source_row_count"],
        "witness_count": result["witness_count"],
    }
    path = root / "posting-manifest.json"
    path.write_bytes(_canonical_json_bytes(manifest))
    return path


def prepare_development_phase(
    root: pathlib.Path,
    training_output: pathlib.Path,
    posting_output: pathlib.Path,
) -> pathlib.Path:
    """Bind exact reduced graph/postings/query/truth inputs for development."""

    _, training = _read_canonical_object(training_output / "result.json")
    generation = str(training["generation"])
    graph = training_output / "witness-graph.arrow"
    postings = posting_output / "witness-postings.arrow"
    queries = root / "queries.parquet"
    neighbors = root / "neighbors.parquet"
    for path in (graph, postings, queries, neighbors):
        if path.is_symlink() or not path.is_file() or path.stat().st_size == 0:
            raise ValueError("V24 reduced development input differs")
    input_dir = root / "development-input"
    input_dir.mkdir(mode=0o700)
    for source, name in (
        (graph, "witness-graph.arrow"),
        (postings, "witness-postings.arrow"),
        (queries, "queries.parquet"),
        (neighbors, "neighbors.parquet"),
    ):
        _copy_exact(source, input_dir / name)
    inputs = [
        _identity(input_dir / "witness-graph.arrow", "witness-graph", generation),
        _identity(input_dir / "witness-postings.arrow", "witness-postings", generation),
        _identity(input_dir / "queries.parquet", "query-parquet", generation),
        _identity(input_dir / "neighbors.parquet", "neighbors-parquet", generation),
    ]
    page_count = (
        int(
            np.max(
                pq.read_table(root / "page-rows.parquet", columns=["page_ordinal"])[
                    0
                ].to_numpy()
            )
        )
        + 1
    )
    query_count = pq.read_metadata(queries).num_rows
    manifest = {
        "claim_eligible": False,
        "generation": generation,
        "inputs": inputs,
        "output_uris": {
            "development-result": "s3://borsuk-v24-reduced/development-result.json"
        },
        "page_count": page_count,
        "phase": "development-evaluation",
        "query_count": query_count,
        "schema": "borsuk-v24-local-manifest-v1",
        "serving_bytes": 1_644_167_168,
        "witness_count": training["witness_count"],
    }
    path = root / "development-manifest.json"
    path.write_bytes(_canonical_json_bytes(manifest))
    return path
