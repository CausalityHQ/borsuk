#!/usr/bin/env python3
"""Build exact top-10 Parquet truth for one authenticated reduced V30 prefix."""

from __future__ import annotations

import hashlib
import json
import sys
from argparse import ArgumentParser
from collections.abc import Callable
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def _digest(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _matrix(payload: bytes, *, role: str, physical_rows: int) -> np.ndarray:
    parquet = pq.ParquetFile(pa.BufferReader(payload))
    child = pa.field("element", pa.float32(), nullable=False)
    expected = pa.schema([pa.field("emb", pa.list_(child, 96), nullable=False)])
    if parquet.schema_arrow != expected or parquet.metadata.num_rows != physical_rows:
        raise ValueError(f"V30 reduced truth {role} Parquet schema or rows differ")
    table = parquet.read(columns=["emb"])
    column = table["emb"].combine_chunks()
    if column.null_count or column.values.null_count:
        raise ValueError(f"V30 reduced truth {role} Parquet nullability differs")
    matrix = column.values.to_numpy(zero_copy_only=False).reshape(physical_rows, 96)
    if not np.isfinite(matrix).all():
        raise ValueError(f"V30 reduced truth {role} values differ")
    norms = np.einsum("ij,ij->i", matrix, matrix, dtype=np.float32)
    if np.any(np.abs(norms - np.float32(1.0)) > np.float32(2e-4)):
        raise ValueError(f"V30 reduced truth {role} norms differ")
    return matrix


def _truth_parquet(truth: list[list[int]]) -> bytes:
    child = pa.field("item", pa.int64(), nullable=False)
    flat = pa.array(np.asarray(truth, dtype=np.int64).reshape(-1), type=pa.int64())
    neighbors = pa.FixedSizeListArray.from_arrays(flat, 10)
    table = pa.Table.from_arrays(
        [neighbors],
        schema=pa.schema([pa.field("neighbors_id", pa.list_(child, 10), nullable=False)]),
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    return sink.getvalue().to_pybytes()


def _exact_distance_matrix(corpus: np.ndarray, queries: np.ndarray) -> np.ndarray:
    """Compute row/query squared L2 with a fixed per-dimension reduction order."""

    distances = np.empty((len(corpus), len(queries)), dtype=np.float64)
    for start in range(0, len(corpus), 4096):
        stop = min(start + 4096, len(corpus))
        block = corpus[start:stop].astype(np.float64)
        for query_index, query in enumerate(queries.astype(np.float64)):
            delta = block - query
            np.square(delta, out=delta)
            distances[start:stop, query_index] = np.sum(
                delta, axis=1, dtype=np.float64
            )
    return distances


def _shard_top_k(
    distances: np.ndarray, source_start: int, count: int
) -> tuple[np.ndarray, np.ndarray]:
    if (
        distances.ndim != 1
        or type(count) is not int
        or count < 1
        or len(distances) < count
        or not np.isfinite(distances).all()
    ):
        raise ValueError("V32 prefix truth distance authority differs")
    threshold = np.partition(distances, count - 1)[count - 1]
    lower = np.flatnonzero(distances < threshold)
    tied = np.flatnonzero(distances == threshold)
    selected = np.concatenate((lower, tied[: count - len(lower)]))
    ordinals = selected.astype(np.int64) + np.int64(source_start)
    order = np.lexsort((ordinals, distances[selected]))
    return distances[selected][order], ordinals[order]


def build_v32_streaming_prefix_truth(
    corpus_manifest: bytes,
    *,
    corpus_manifest_sha256: str,
    corpus_manifest_bytes: int,
    expected_source_rows: int,
    expected_shard_count: int,
    query_parquet: bytes,
    query_sha256: str,
    query_bytes: int,
    query_start: int,
    query_count: int,
    fetch: Callable[[str], bytes],
) -> tuple[bytes, bytes]:
    """Stream authenticated prefix shards and retain exact top-10 truth only."""

    if (
        type(corpus_manifest) is not bytes
        or not _digest(corpus_manifest_sha256)
        or len(corpus_manifest) != corpus_manifest_bytes
        or hashlib.sha256(corpus_manifest).hexdigest() != corpus_manifest_sha256
        or not corpus_manifest.endswith(b"\n")
    ):
        raise ValueError("V32 prefix truth manifest byte authority differs")
    try:
        manifest = json.loads(corpus_manifest)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V32 prefix truth manifest JSON differs") from error
    if (
        corpus_manifest
        != json.dumps(manifest, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
        or type(manifest) is not dict
        or set(manifest) != {"dataset_id", "schema_version", "shards", "source_rows"}
        or manifest["dataset_id"] != "deep-image-96"
        or type(manifest["schema_version"]) is not int
        or manifest["schema_version"] != 1
        or type(manifest["source_rows"]) is not int
        or manifest["source_rows"] != expected_source_rows
        or manifest["source_rows"] < 11
        or type(manifest["shards"]) is not list
        or len(manifest["shards"]) != expected_shard_count
        or type(expected_source_rows) is not int
        or type(expected_shard_count) is not int
        or expected_shard_count < 1
    ):
        raise ValueError("V32 prefix truth expected geometry differs")
    if (
        type(query_parquet) is not bytes
        or not _digest(query_sha256)
        or len(query_parquet) != query_bytes
        or hashlib.sha256(query_parquet).hexdigest() != query_sha256
        or type(query_start) is not int
        or query_start < 0
        or query_count != 32
    ):
        raise ValueError("V32 prefix truth query byte authority differs")
    query_rows = pq.ParquetFile(pa.BufferReader(query_parquet)).metadata.num_rows
    queries = _matrix(query_parquet, role="query", physical_rows=query_rows)
    if query_start + query_count > query_rows:
        raise ValueError("V32 prefix truth query range differs")
    queries = queries[query_start : query_start + query_count]
    best_distances = np.full((query_count, 11), np.inf, dtype=np.float64)
    best_ordinals = np.full((query_count, 11), np.iinfo(np.int64).max, dtype=np.int64)
    next_row = 0
    shard_uris: set[str] = set()
    for shard in manifest["shards"]:
        if (
            type(shard) is not dict
            or set(shard)
            != {
                "encoded_bytes",
                "physical_row_count",
                "row_count",
                "row_start",
                "sha256",
                "uri",
            }
            or type(shard["encoded_bytes"]) is not int
            or shard["encoded_bytes"] <= 0
            or type(shard["physical_row_count"]) is not int
            or type(shard["row_count"]) is not int
            or not 10 <= shard["row_count"] <= shard["physical_row_count"]
            or shard["row_start"] != next_row
            or not _digest(shard["sha256"])
            or type(shard["uri"]) is not str
            or not shard["uri"].startswith("s3://")
        ):
            raise ValueError("V32 prefix truth shard authority differs")
        if shard["uri"] in shard_uris:
            raise ValueError("V32 prefix truth shard role authority differs")
        shard_uris.add(shard["uri"])
        payload = fetch(shard["uri"])
        if (
            type(payload) is not bytes
            or len(payload) != shard["encoded_bytes"]
            or hashlib.sha256(payload).hexdigest() != shard["sha256"]
        ):
            raise ValueError("V32 prefix truth shard byte authority differs")
        corpus = _matrix(
            payload, role="corpus", physical_rows=shard["physical_row_count"]
        )[: shard["row_count"]]
        distances = _exact_distance_matrix(corpus, queries)
        for query_index in range(query_count):
            shard_count = min(11, len(corpus))
            shard_distances, shard_ordinals = _shard_top_k(
                distances[:, query_index], next_row, shard_count
            )
            candidates_distances = np.concatenate(
                (best_distances[query_index], shard_distances)
            )
            candidates_ordinals = np.concatenate(
                (best_ordinals[query_index], shard_ordinals)
            )
            order = np.lexsort((candidates_ordinals, candidates_distances))[:11]
            best_distances[query_index] = candidates_distances[order]
            best_ordinals[query_index] = candidates_ordinals[order]
        next_row += shard["row_count"]
    if next_row != manifest["source_rows"] or np.any(best_ordinals == np.iinfo(np.int64).max):
        raise ValueError("V32 prefix truth shard coverage differs")
    truth_ids = best_ordinals[:, :10]
    truth = _truth_parquet(truth_ids.tolist())
    receipt = json.dumps(
        {
            "claim_eligible": False,
            "corpus_manifest_sha256": corpus_manifest_sha256,
            "corpus_shards": manifest["shards"],
            "query_count": query_count,
            "query_sha256": query_sha256,
            "query_start": query_start,
            "rank_10_11_tie_queries": int(
                np.count_nonzero(best_distances[:, 9] == best_distances[:, 10])
            ),
            "shards_read": len(manifest["shards"]),
            "source_rows": manifest["source_rows"],
            "status": "passed",
            "truth_bytes": len(truth),
            "truth_ids_sha256": hashlib.sha256(
                np.asarray(truth_ids, dtype="<i8").tobytes()
            ).hexdigest(),
            "truth_sha256": hashlib.sha256(truth).hexdigest(),
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode() + b"\n"
    return truth, receipt


def build_v30_reduced_truth(
    corpus_parquet: bytes,
    *,
    corpus_sha256: str,
    corpus_bytes: int,
    physical_rows: int,
    source_rows: int,
    query_parquet: bytes,
    query_sha256: str,
    query_bytes: int,
    query_start: int,
    query_count: int,
) -> tuple[bytes, bytes]:
    """Authenticate two Parquet objects and return truth plus canonical receipt."""

    if (
        type(corpus_parquet) is not bytes
        or not _digest(corpus_sha256)
        or len(corpus_parquet) != corpus_bytes
        or hashlib.sha256(corpus_parquet).hexdigest() != corpus_sha256
    ):
        raise ValueError("V30 reduced truth corpus byte authority differs")
    if (
        type(query_parquet) is not bytes
        or not _digest(query_sha256)
        or len(query_parquet) != query_bytes
        or hashlib.sha256(query_parquet).hexdigest() != query_sha256
    ):
        raise ValueError("V30 reduced truth query byte authority differs")
    if (
        type(physical_rows) is not int
        or type(source_rows) is not int
        or not 10 <= source_rows <= physical_rows
        or type(query_start) is not int
        or query_start < 0
        or query_count != 32
    ):
        raise ValueError("V30 reduced truth shape differs")
    corpus = _matrix(corpus_parquet, role="corpus", physical_rows=physical_rows)[:source_rows]
    query_rows = pq.ParquetFile(pa.BufferReader(query_parquet)).metadata.num_rows
    queries = _matrix(query_parquet, role="query", physical_rows=query_rows)
    if query_start + query_count > len(queries):
        raise ValueError("V30 reduced truth query range differs")
    truth: list[list[int]] = []
    distances = _exact_distance_matrix(
        corpus, queries[query_start : query_start + query_count]
    )
    for query_index in range(query_count):
        _, ordered = _shard_top_k(distances[:, query_index], 0, 10)
        truth.append([int(value) for value in ordered])
    child = pa.field("item", pa.int64(), nullable=False)
    flat = pa.array(np.asarray(truth, dtype=np.int64).reshape(-1), type=pa.int64())
    neighbors = pa.FixedSizeListArray.from_arrays(flat, 10)
    table = pa.Table.from_arrays(
        [neighbors],
        schema=pa.schema([pa.field("neighbors_id", pa.list_(child, 10), nullable=False)]),
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    truth_bytes = sink.getvalue().to_pybytes()
    receipt = json.dumps(
        {
            "claim_eligible": False,
            "corpus_sha256": corpus_sha256,
            "query_count": query_count,
            "query_sha256": query_sha256,
            "query_start": query_start,
            "source_rows": source_rows,
            "status": "passed",
            "truth_bytes": len(truth_bytes),
            "truth_sha256": hashlib.sha256(truth_bytes).hexdigest(),
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode() + b"\n"
    return truth_bytes, receipt


def main(arguments: list[str] | None = None) -> int:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", required=True)
    parser.add_argument("--corpus-parquet", type=Path, required=True)
    parser.add_argument("--corpus-sha256", required=True)
    parser.add_argument("--corpus-bytes", type=int, required=True)
    parser.add_argument("--physical-rows", type=int, required=True)
    parser.add_argument("--source-rows", type=int, required=True)
    parser.add_argument("--query-parquet", type=Path, required=True)
    parser.add_argument("--query-sha256", required=True)
    parser.add_argument("--query-bytes", type=int, required=True)
    parser.add_argument("--query-start", type=int, required=True)
    parser.add_argument("--query-count", type=int, required=True)
    parser.add_argument("--truth-output", type=Path, required=True)
    args = parser.parse_args(arguments)
    truth, receipt = build_v30_reduced_truth(
        args.corpus_parquet.read_bytes(),
        corpus_sha256=args.corpus_sha256,
        corpus_bytes=args.corpus_bytes,
        physical_rows=args.physical_rows,
        source_rows=args.source_rows,
        query_parquet=args.query_parquet.read_bytes(),
        query_sha256=args.query_sha256,
        query_bytes=args.query_bytes,
        query_start=args.query_start,
        query_count=args.query_count,
    )
    with args.truth_output.open("xb") as output:
        output.write(truth)
    sys.stdout.buffer.write(receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
