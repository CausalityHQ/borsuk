#!/usr/bin/env python3
"""Build exact top-10 Parquet truth for one authenticated reduced V30 prefix."""

from __future__ import annotations

import hashlib
import json
import sys
from argparse import ArgumentParser
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
    ordinals = np.arange(source_rows, dtype=np.int64)
    truth: list[list[int]] = []
    corpus_norms = np.einsum("ij,ij->i", corpus, corpus, dtype=np.float32)
    for query in queries[query_start : query_start + query_count]:
        distances = corpus_norms + np.dot(query, query) - np.float32(2.0) * (corpus @ query)
        picked = np.argpartition(distances, 9)[:10]
        ordered = picked[np.lexsort((ordinals[picked], distances[picked]))]
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
