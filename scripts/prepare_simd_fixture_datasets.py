#!/usr/bin/env python3
"""Prepare deterministic synthetic binary and late-interaction SIMD fixtures."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from scripts.freeze_simd_dataset_identity import build_identity
except ModuleNotFoundError:
    from freeze_simd_dataset_identity import build_identity


def write_json(path: Path, value: dict) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_identity(
    directory: Path, *, dataset: str, source: str, synthetic: bool = True
) -> None:
    identity = build_identity(
        directory, dataset=dataset, source=source, synthetic=synthetic
    )
    write_json(directory / "dataset-identity.json", identity)


def prepare_binary(
    root: Path,
    *,
    documents: int,
    queries: int,
    dimensions: int,
    seed: int,
) -> Path:
    if documents < queries * 10:
        raise ValueError("binary documents must provide at least ten rows per query")
    if dimensions < 16:
        raise ValueError("binary dimensions must be at least 16")
    name = "simd-binary-128"
    directory = root / name
    directory.mkdir(parents=True, exist_ok=False)
    rng = np.random.default_rng(seed)
    train = rng.integers(0, 2, size=(documents, dimensions), dtype=np.uint8)
    test = rng.integers(0, 2, size=(queries, dimensions), dtype=np.uint8)
    test[:, 0] = 1
    for query in range(queries):
        base = test[query]
        for distance in range(10):
            row = base.copy()
            row[:distance] ^= 1
            train[query * 10 + distance] = row
    neighbors = np.empty((queries, 10), dtype="<i4")
    for query in range(queries):
        distances = np.count_nonzero(train != test[query], axis=1)
        neighbors[query] = np.argsort(distances, kind="stable")[:10]
    train.astype("<f4").tofile(directory / "train.f32")
    test.astype("<f4").tofile(directory / "test.f32")
    neighbors.tofile(directory / "neighbors.i32")
    write_json(
        directory / "meta.json",
        {
            "name": name,
            "metric": "hamming",
            "dim": dimensions,
            "n_train": documents,
            "n_test": queries,
            "k": 10,
        },
    )
    write_identity(
        directory,
        dataset=name,
        source=f"deterministic synthetic binary SIMD fixture seed={seed}",
    )
    return directory


def token_list_array(values: np.ndarray) -> pa.ListArray:
    rows, tokens, dimensions = values.shape
    flat = pa.array(values.reshape(-1), type=pa.float32())
    fixed = pa.FixedSizeListArray.from_arrays(flat, dimensions)
    offsets = pa.array(np.arange(0, (rows + 1) * tokens, tokens), type=pa.int32())
    return pa.ListArray.from_arrays(offsets, fixed)


def write_late_dataset(
    root: Path,
    *,
    element_type: str,
    documents: np.ndarray,
    queries: np.ndarray,
    relevant: list[list[str]],
    seed: int,
) -> Path:
    name = f"simd-late-interaction-128-{element_type}"
    directory = root / name
    directory.mkdir(parents=True, exist_ok=False)
    documents_path = directory / "documents.parquet"
    queries_path = directory / "queries.parquet"
    pq.write_table(
        pa.table(
            {
                "document_id": pa.array(
                    [f"d{row:08d}" for row in range(documents.shape[0])]
                ),
                "tokens": token_list_array(documents),
            }
        ),
        documents_path,
        compression="zstd",
    )
    pq.write_table(
        pa.table(
            {
                "query_id": pa.array(
                    [f"q{row:08d}" for row in range(queries.shape[0])]
                ),
                "tokens": token_list_array(queries),
                "relevant_ids": pa.array(relevant, type=pa.list_(pa.string())),
            }
        ),
        queries_path,
        compression="zstd",
    )
    generator_contract = {
        "seed": seed,
        "documents": int(documents.shape[0]),
        "queries": int(queries.shape[0]),
        "dimensions": int(documents.shape[2]),
        "document_tokens": int(documents.shape[1]),
        "query_tokens": int(queries.shape[1]),
        "vector_element_type": element_type,
    }
    source_sha = hashlib.sha256(
        json.dumps(generator_contract, sort_keys=True).encode()
    ).hexdigest()
    files = [
        {
            "path": path.name,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in (documents_path, queries_path)
    ]
    write_json(
        directory / "dataset.json",
        {
            "schema_version": 1,
            "dataset": name,
            "workload": "late_interaction_maxsim",
            "dimensions": documents.shape[2],
            "scale": documents.shape[0],
            "source": (
                "deterministic synthetic SIMD qualification fixture; "
                "not commercial benchmark evidence"
            ),
            "source_sha256": source_sha,
            "license": "CC0-1.0 generated fixture",
            "adapter": "borsuk_late_interaction",
            "files": files,
            "benchmark": {
                "seed": seed,
                "queries": queries.shape[0],
                "segment_max_vectors": min(documents.shape[0], 5_000),
                "documents_file": documents_path.name,
                "queries_file": queries_path.name,
                "candidates_per_query_token": [128],
                "vector_element_type": element_type,
            },
        },
    )
    write_identity(
        directory,
        dataset=name,
        source=f"deterministic synthetic late-interaction SIMD fixture seed={seed}",
    )
    return directory


def prepare_late(
    root: Path,
    *,
    documents: int,
    queries: int,
    dimensions: int,
    document_tokens: int,
    query_tokens: int,
    seed: int,
) -> tuple[Path, Path]:
    if documents < queries or query_tokens > document_tokens:
        raise ValueError(
            "late fixture requires documents >= queries and "
            "query tokens <= document tokens"
        )
    rng = np.random.default_rng(seed ^ 0x5A17)
    document_values = rng.normal(size=(documents, document_tokens, dimensions)).astype(
        np.float32
    )
    norms = np.linalg.norm(document_values, axis=2, keepdims=True)
    document_values /= np.maximum(norms, np.finfo(np.float32).eps)
    query_values = document_values[:queries, :query_tokens].copy()
    relevant = [[f"d{row:08d}"] for row in range(queries)]
    return (
        write_late_dataset(
            root,
            element_type="float32",
            documents=document_values,
            queries=query_values,
            relevant=relevant,
            seed=seed,
        ),
        write_late_dataset(
            root,
            element_type="float16",
            documents=document_values,
            queries=query_values,
            relevant=relevant,
            seed=seed,
        ),
    )


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--binary-documents", type=int, default=100_000)
    parser.add_argument("--late-documents", type=int, default=10_000)
    parser.add_argument("--queries", type=int, default=500)
    parser.add_argument("--dimensions", type=int, default=128)
    parser.add_argument("--document-tokens", type=int, default=16)
    parser.add_argument("--query-tokens", type=int, default=8)
    parser.add_argument("--seed", type=int, default=20260730)
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    args.output_root.mkdir(parents=True, exist_ok=True)
    prepare_binary(
        args.output_root,
        documents=args.binary_documents,
        queries=args.queries,
        dimensions=args.dimensions,
        seed=args.seed,
    )
    prepare_late(
        args.output_root,
        documents=args.late_documents,
        queries=args.queries,
        dimensions=args.dimensions,
        document_tokens=args.document_tokens,
        query_tokens=args.query_tokens,
        seed=args.seed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
