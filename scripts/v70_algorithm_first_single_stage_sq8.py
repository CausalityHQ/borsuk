#!/usr/bin/env python3
"""One round trip of SQ8 against two round trips of PQ192, on real S3.

V69 overturned the assumption V68 was written under. Latency tracks the number
of object-store requests, not the bytes: 243 requests carrying 17 MiB took
449 ms of I/O while 137 requests carrying 27 MiB took 291 ms. Merging harder is
cheaper even though it reads more.

That makes the design V67 turned up worth measuring for real. SQ8 codes return
99.182% Recall@100 with no rescoring at all, so the whole second stage - its
scattered shortlist, its extra round trip, and roughly a third of the requests
- simply disappears. It costs 3.8x the stage-one bytes, which V69 says is the
cheaper side of the trade.

Scoring is a dequantise-and-GEMM rather than an ADC table walk, so the NumPy
compute that dominated V68 largely goes away too. Rows carry a precomputed
squared norm so the scan never reconstructs a vector.
"""


from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

ROWS = 1_000_000
QUERIES = 1_000
NEIGHBORS = 100
DIMENSIONS = 768

PAGE_ROWS = 256
ROUTER_BLOCKS_PER_PAGE = 2
PQ_SUBSPACES = 192
CODE_ROW_BYTES = PQ_SUBSPACES
EXACT_ROW_BYTES = 8 + DIMENSIONS * 4

ROUTER_PAGES = 256
SHORTLIST = 512
STAGE_ONE_GAP_PAGES = 8
STAGE_TWO_GAP_ROWS = 256
READ_THREADS = 64

MEASURED_QUERIES = 200
CONCURRENCY_LADDER = (1, 8, 32, 64)
CHUNK_ROWS = 16_384


def fixed_list(table, name, width, rows):
    values = table[name].combine_chunks().values.to_numpy(zero_copy_only=False)
    result = np.array(values, dtype=np.float32, copy=True).reshape(rows, width)
    if not np.isfinite(result).all():
        raise ValueError(f"{name} differs")
    return result


def scalar(table, name, dtype):
    return np.asarray(
        table[name].combine_chunks().to_numpy(zero_copy_only=False), dtype=dtype
    )


def lloyd(data, clusters, iterations, seed):
    clusters = min(clusters, data.shape[0])
    generator = np.random.default_rng(seed)
    centroids = data[generator.choice(data.shape[0], clusters, replace=False)].copy()
    for _ in range(iterations):
        norms = np.einsum("ij,ij->i", centroids, centroids)
        assignment = np.argmin(norms[None, :] - 2.0 * (data @ centroids.T), axis=1)
        counts = np.bincount(assignment, minlength=clusters)
        order = np.argsort(assignment, kind="stable")
        starts = np.concatenate(([0], np.cumsum(counts)[:-1]))
        occupied = counts > 0
        centroids[occupied] = (
            np.add.reduceat(data[order], starts[occupied], axis=0)
            / counts[occupied][:, None]
        )
    return centroids.astype(np.float32, copy=False)


def train_pq(data, seed, sample_rows=100_000):
    width = DIMENSIONS // PQ_SUBSPACES
    generator = np.random.default_rng(seed)
    take = min(data.shape[0], sample_rows)
    sample = data[generator.choice(data.shape[0], take, replace=False)]
    books = np.empty((PQ_SUBSPACES, 256, width), dtype=np.float32)
    for index in range(PQ_SUBSPACES):
        lo, hi = index * width, (index + 1) * width
        books[index] = lloyd(np.ascontiguousarray(sample[:, lo:hi]), 256, 10, seed + index)
    return books


def encode_pq(data, books):
    width = DIMENSIONS // PQ_SUBSPACES
    codes = np.empty((data.shape[0], PQ_SUBSPACES), dtype=np.uint8)
    for index in range(PQ_SUBSPACES):
        lo, hi = index * width, (index + 1) * width
        book = books[index]
        norms = np.einsum("ij,ij->i", book, book)
        for start in range(0, data.shape[0], CHUNK_ROWS):
            stop = min(start + CHUNK_ROWS, data.shape[0])
            block = data[start:stop, lo:hi]
            codes[start:stop, index] = np.argmin(
                norms[None, :] - 2.0 * (block @ book.T), axis=1
            )
    return codes


def decode_pq(codes, books):
    width = DIMENSIONS // PQ_SUBSPACES
    out = np.empty((codes.shape[0], DIMENSIONS), dtype=np.float32)
    for index in range(PQ_SUBSPACES):
        out[:, index * width : (index + 1) * width] = books[index][codes[:, index]]
    return out


def adc_table(query, books):
    """Per-query asymmetric distance table: subspace x codeword."""
    width = DIMENSIONS // PQ_SUBSPACES
    table = np.empty((PQ_SUBSPACES, 256), dtype=np.float32)
    for index in range(PQ_SUBSPACES):
        delta = books[index] - query[index * width : (index + 1) * width]
        table[index] = np.einsum("ij,ij->i", delta, delta)
    return table


def adc_score(codes, table):
    offsets = (np.arange(PQ_SUBSPACES, dtype=np.int32) * 256)[None, :]
    return table.ravel()[codes.astype(np.int32) + offsets].sum(axis=1)


def block_means(ordered, block_rows):
    rows = ordered.shape[0]
    starts = np.arange(0, rows, block_rows)
    counts = np.diff(np.append(starts, rows)).astype(np.float32)
    return (np.add.reduceat(ordered, starts, axis=0) / counts[:, None]).astype(np.float32)


def coalesce(sorted_units: np.ndarray, gap: int):
    if sorted_units.size == 0:
        return []
    breaks = np.flatnonzero(np.diff(sorted_units) > gap + 1)
    starts = np.concatenate(([sorted_units[0]], sorted_units[breaks + 1]))
    ends = np.concatenate((sorted_units[breaks], [sorted_units[-1]]))
    return list(zip(starts.tolist(), ends.tolist()))


def nearest_rank(ordered, numerator, denominator):
    index = max(0, min(len(ordered) - 1, (len(ordered) * numerator - 1) // denominator))
    return float(ordered[index])


def latency_stats(values):
    ordered = np.sort(np.asarray(values, dtype=np.float64))
    return {
        "p50_ms": round(nearest_rank(ordered, 50, 100), 3),
        "p95_ms": round(nearest_rank(ordered, 95, 100), 3),
        "p99_ms": round(nearest_rank(ordered, 99, 100), 3),
        "mean_ms": round(float(ordered.mean()), 3),
        "maximum_ms": round(float(ordered[-1]), 3),
    }


class ObjectReader:
    """Ranged reads against one S3 object, issued concurrently."""

    def __init__(self, client, bucket: str, key: str, threads: int) -> None:
        self.client = client
        self.bucket = bucket
        self.key = key
        self.pool = ThreadPoolExecutor(max_workers=threads)

    def fetch(self, ranges):
        def one(bounds):
            first, last = bounds
            body = self.client.get_object(
                Bucket=self.bucket, Key=self.key, Range=f"bytes={first}-{last}"
            )["Body"].read()
            return first, body

        return list(self.pool.map(one, ranges))


class Index:
    def __init__(self, reader_codes, reader_exact, books, summaries, pages):
        self.codes = reader_codes
        self.exact = reader_exact
        self.books = books
        self.summaries = summaries
        self.summary_norms = np.einsum("ij,ij->i", summaries, summaries).astype(np.float32)
        self.pages = pages

    def search(self, query, neighbours=NEIGHBORS):
        timing = {}
        started = time.perf_counter()
        scores = self.summary_norms - 2.0 * (self.summaries @ query)
        blocks = self.pages * ROUTER_BLOCKS_PER_PAGE
        if scores.size < blocks:
            scores = np.concatenate(
                [scores, np.full(blocks - scores.size, np.inf, np.float32)]
            )
        page_scores = scores[:blocks].reshape(self.pages, ROUTER_BLOCKS_PER_PAGE).min(axis=1)
        chosen = np.sort(np.argpartition(page_scores, ROUTER_PAGES - 1)[:ROUTER_PAGES])
        timing["route_ms"] = (time.perf_counter() - started) * 1000.0

        started = time.perf_counter()
        page_ranges = coalesce(chosen, STAGE_ONE_GAP_PAGES)
        byte_ranges = [
            (first * PAGE_ROWS * CODE_ROW_BYTES,
             min((last + 1) * PAGE_ROWS, ROWS) * CODE_ROW_BYTES - 1)
            for first, last in page_ranges
        ]
        blobs = self.codes.fetch(byte_ranges)
        timing["stage_one_io_ms"] = (time.perf_counter() - started) * 1000.0
        stage_one_bytes = sum(len(body) for _, body in blobs)

        started = time.perf_counter()
        code_blocks, row_blocks = [], []
        for offset, body in blobs:
            count = len(body) // CODE_ROW_BYTES
            code_blocks.append(
                np.frombuffer(body, dtype=np.uint8).reshape(count, CODE_ROW_BYTES)
            )
            first_row = offset // CODE_ROW_BYTES
            row_blocks.append(np.arange(first_row, first_row + count, dtype=np.int64))
        codes = np.concatenate(code_blocks)
        rows = np.concatenate(row_blocks)
        approximate = adc_score(codes, adc_table(query, self.books))
        size = min(SHORTLIST, approximate.size)
        shortlist = np.sort(rows[np.argpartition(approximate, size - 1)[:size]])
        timing["stage_one_cpu_ms"] = (time.perf_counter() - started) * 1000.0

        started = time.perf_counter()
        row_ranges = coalesce(shortlist, STAGE_TWO_GAP_ROWS)
        exact_ranges = [
            (first * EXACT_ROW_BYTES, (last + 1) * EXACT_ROW_BYTES - 1)
            for first, last in row_ranges
        ]
        exact_blobs = self.exact.fetch(exact_ranges)
        timing["stage_two_io_ms"] = (time.perf_counter() - started) * 1000.0
        stage_two_bytes = sum(len(body) for _, body in exact_blobs)

        started = time.perf_counter()
        # Every fetched row is rescored, not just the shortlist rows. The gap
        # rows were paid for by the same GET, so scoring them is free recall.
        identifiers, vectors = [], []
        for _, body in exact_blobs:
            count = len(body) // EXACT_ROW_BYTES
            raw = np.frombuffer(body, dtype=np.uint8).reshape(count, EXACT_ROW_BYTES)
            identifiers.append(raw[:, :8].copy().view(np.int64).reshape(-1))
            vectors.append(
                raw[:, 8:].copy().view(np.float32).reshape(count, DIMENSIONS)
            )
        identifiers = np.concatenate(identifiers)
        vectors = np.concatenate(vectors)
        delta = vectors - query
        exact_scores = np.einsum("ij,ij->i", delta, delta)
        best = np.argpartition(exact_scores, min(neighbours, exact_scores.size) - 1)[
            :neighbours
        ]
        returned = identifiers[best]
        timing["stage_two_cpu_ms"] = (time.perf_counter() - started) * 1000.0

        timing["requests"] = len(byte_ranges) + len(exact_ranges)
        timing["stage_one_requests"] = len(byte_ranges)
        timing["stage_two_requests"] = len(exact_ranges)
        timing["bytes"] = stage_one_bytes + stage_two_bytes
        timing["total_ms"] = sum(
            timing[key]
            for key in (
                "route_ms",
                "stage_one_io_ms",
                "stage_one_cpu_ms",
                "stage_two_io_ms",
                "stage_two_cpu_ms",
            )
        )
        return returned, timing



SQ_ROW_BYTES = 8 + 4 + DIMENSIONS   # id, squared norm, one byte per dimension
SWEEP = [
    {"router_pages": pages, "stage_one_gap": gap}
    for pages in (128, 256, 512)
    for gap in (8, 16, 32)
]
SWEEP_QUERIES = 60


def sq8_parameters(ordered):
    low = ordered.min(axis=0)
    high = ordered.max(axis=0)
    span = np.maximum(high - low, 1e-12).astype(np.float32)
    return low.astype(np.float32), span


def sq8_encode(ordered, low, span):
    codes = np.empty((ordered.shape[0], DIMENSIONS), dtype=np.uint8)
    for start in range(0, ordered.shape[0], CHUNK_ROWS):
        stop = min(start + CHUNK_ROWS, ordered.shape[0])
        codes[start:stop] = np.clip(
            np.rint((ordered[start:stop] - low) / span * 255.0), 0, 255
        ).astype(np.uint8)
    return codes


class SqIndex:
    """Single-stage reader: route, fetch SQ8 rows, score, return."""

    def __init__(self, reader, low, span, summaries, pages):
        self.reader = reader
        self.low = low
        self.span = span / 255.0
        self.summaries = summaries
        self.summary_norms = np.einsum("ij,ij->i", summaries, summaries).astype(np.float32)
        self.pages = pages

    def search(self, query, neighbours=NEIGHBORS):
        timing = {}
        started = time.perf_counter()
        scores = self.summary_norms - 2.0 * (self.summaries @ query)
        blocks = self.pages * ROUTER_BLOCKS_PER_PAGE
        if scores.size < blocks:
            scores = np.concatenate([scores, np.full(blocks - scores.size, np.inf, np.float32)])
        page_scores = scores[:blocks].reshape(self.pages, ROUTER_BLOCKS_PER_PAGE).min(axis=1)
        chosen = np.sort(np.argpartition(page_scores, ROUTER_PAGES - 1)[:ROUTER_PAGES])
        timing["route_ms"] = (time.perf_counter() - started) * 1000.0

        started = time.perf_counter()
        ranges = [
            (first * PAGE_ROWS * SQ_ROW_BYTES,
             min((last + 1) * PAGE_ROWS, ROWS) * SQ_ROW_BYTES - 1)
            for first, last in coalesce(chosen, STAGE_ONE_GAP_PAGES)
        ]
        blobs = self.reader.fetch(ranges)
        timing["stage_one_io_ms"] = (time.perf_counter() - started) * 1000.0
        fetched = sum(len(body) for _, body in blobs)

        started = time.perf_counter()
        # Squared distance from the stored norm and one GEMV, so no row is ever
        # reconstructed: ||q-x||^2 = ||x||^2 - 2 q.x + ||q||^2, and q.x folds
        # the per-dimension scale into the query once.
        weights = query * self.span
        shift = float(query @ self.low) - float(query @ query) / 2.0
        identifiers, partial = [], []
        for _, body in blobs:
            count = len(body) // SQ_ROW_BYTES
            raw = np.frombuffer(body, dtype=np.uint8).reshape(count, SQ_ROW_BYTES)
            identifiers.append(raw[:, :8].copy().view(np.int64).reshape(-1))
            norms = raw[:, 8:12].copy().view(np.float32).reshape(-1)
            codes = raw[:, 12:].astype(np.float32)
            partial.append(norms - 2.0 * (codes @ weights + shift))
        identifiers = np.concatenate(identifiers)
        scored = np.concatenate(partial)
        best = np.argpartition(scored, min(neighbours, scored.size) - 1)[:neighbours]
        returned = identifiers[best]
        timing["stage_one_cpu_ms"] = (time.perf_counter() - started) * 1000.0
        timing["stage_two_io_ms"] = 0.0
        timing["stage_two_cpu_ms"] = 0.0
        timing["requests"] = len(ranges)
        timing["bytes"] = fetched
        timing["total_ms"] = (
            timing["route_ms"] + timing["stage_one_io_ms"] + timing["stage_one_cpu_ms"]
        )
        return returned, timing


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--source", type=Path)
    parser.add_argument("--development-query", type=Path)
    parser.add_argument("--ground-truth", type=Path)
    parser.add_argument("--layout-order", type=Path)
    parser.add_argument("--bucket")
    parser.add_argument("--prefix")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    for name in ("source", "development_query", "ground_truth", "layout_order",
                 "bucket", "prefix", "output"):
        if getattr(args, name) is None:
            parser.error(f"missing required argument: {name}")

    import boto3
    from botocore.config import Config

    overall = time.perf_counter()
    source = pq.read_table(args.source)
    feature_ids = scalar(source, "feature_row_id", np.uint64)
    vectors = fixed_list(source, "embedding", DIMENSIONS, ROWS)
    del source
    truth_ids = scalar(
        pq.read_table(args.ground_truth, columns=["feature_row_id"]),
        "feature_row_id", np.uint64,
    ).reshape(QUERIES, NEIGHBORS)
    queries = fixed_list(
        pq.read_table(args.development_query), "embedding", DIMENSIONS, QUERIES
    )
    order = np.asarray(np.load(args.layout_order), dtype=np.int32)
    ordered = np.ascontiguousarray(vectors[order])
    ordered_ids = feature_ids[order].astype(np.int64)
    del vectors
    pages = (ROWS + PAGE_ROWS - 1) // PAGE_ROWS

    build_started = time.perf_counter()
    low, span = sq8_parameters(ordered)
    codes = sq8_encode(ordered, low, span)
    dequantised = low + codes.astype(np.float32) * (span / 255.0)
    norms = np.einsum("ij,ij->i", dequantised, dequantised).astype(np.float32)
    blob = np.empty((ROWS, SQ_ROW_BYTES), dtype=np.uint8)
    blob[:, :8] = ordered_ids.view(np.uint8).reshape(ROWS, 8)
    blob[:, 8:12] = norms.view(np.uint8).reshape(ROWS, 4)
    blob[:, 12:] = codes
    del dequantised, codes
    books = train_pq(ordered, 6801)
    summaries = decode_pq(
        encode_pq(block_means(ordered, PAGE_ROWS // ROUTER_BLOCKS_PER_PAGE), books), books
    )
    del ordered
    build_seconds = time.perf_counter() - build_started

    client = boto3.client(
        "s3", config=Config(max_pool_connections=512, retries={"max_attempts": 3})
    )
    upload_started = time.perf_counter()
    path = Path("/mnt/sq8.bin")
    blob.tofile(path)
    client.upload_file(str(path), args.bucket, f"{args.prefix}/sq8.bin")
    path.unlink()
    upload_seconds = time.perf_counter() - upload_started
    uploaded = blob.nbytes
    del blob
    print(json.dumps({"phase": "built", "build_seconds": round(build_seconds, 2),
                      "upload_seconds": round(upload_seconds, 2)}), flush=True)

    index = SqIndex(
        ObjectReader(client, args.bucket, f"{args.prefix}/sq8.bin", 256),
        low, span, summaries, pages,
    )
    truth_sets = [set(truth_ids[i].astype(np.int64).tolist()) for i in range(QUERIES)]

    global ROUTER_PAGES, STAGE_ONE_GAP_PAGES
    cells = []
    for point in SWEEP:
        ROUTER_PAGES = point["router_pages"]
        STAGE_ONE_GAP_PAGES = point["stage_one_gap"]
        samples, hits = [], []
        for query in range(SWEEP_QUERIES):
            returned, timing = index.search(queries[query])
            samples.append(timing)
            hits.append(len(truth_sets[query] & set(returned.tolist())))
        recall = np.asarray(hits, dtype=np.int32)
        cells.append({
            **point,
            "queries": SWEEP_QUERIES,
            "aggregate_recall_ppm": int(round(float(recall.sum()) * 1_000_000 / (recall.size * NEIGHBORS))),
            "worst_recall_ppm": int(recall.min()) * 10_000,
            "total": latency_stats([s["total_ms"] for s in samples]),
            "io_only": latency_stats([s["stage_one_io_ms"] for s in samples]),
            "cpu_numpy": latency_stats([s["route_ms"] + s["stage_one_cpu_ms"] for s in samples]),
            "requests_p50": int(np.median([s["requests"] for s in samples])),
            "bytes_p50": int(np.median([s["bytes"] for s in samples])),
        })
        print(json.dumps({k: cells[-1][k] for k in
                          ("router_pages", "stage_one_gap", "aggregate_recall_ppm",
                           "worst_recall_ppm", "requests_p50")}, default=int), flush=True)

    result = {
        "schema": "borsuk-v70-algorithm-first-single-stage-sq8-result-v1",
        "claim_eligible": False,
        "evidence_kind": "measured-in-region-s3-single-round-trip-sq8",
        "storage": "real-s3-ranged-gets-no-local-cache",
        "cpu_path": "numpy-gemv-not-the-crate-simd-path",
        "sq_row_bytes": SQ_ROW_BYTES,
        "source_rows": ROWS,
        "neighbors": NEIGHBORS,
        "page_rows": PAGE_ROWS,
        "sweep_queries": SWEEP_QUERIES,
        "build": {
            "build_seconds": round(build_seconds, 3),
            "build_vectors_per_second": int(ROWS / build_seconds),
            "upload_seconds": round(upload_seconds, 3),
            "upload_bytes": int(uploaded),
            "upload_mib_per_second": round(uploaded / upload_seconds / 2**20, 2),
        },
        "cells": cells,
        "elapsed_seconds": round(time.perf_counter() - overall, 3),
        "validation_opened": False,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":"), default=int) + "\n").encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest()}), flush=True)


def self_test() -> None:
    global ROWS, DIMENSIONS
    ROWS, DIMENSIONS = 3_000, 48
    rng = np.random.default_rng(70)
    data = rng.standard_normal((ROWS, DIMENSIONS), dtype=np.float32)
    low, span = sq8_parameters(data)
    codes = sq8_encode(data, low, span)
    dequantised = low + codes.astype(np.float32) * (span / 255.0)
    if np.abs(dequantised - data).max() > span.max() / 255.0 * 1.01:
        raise AssertionError("SQ8 error exceeds one quantisation step")
    norms = np.einsum("ij,ij->i", dequantised, dequantised).astype(np.float32)
    # The fused score must equal the plain squared distance to the dequantised
    # row, or the scan silently ranks on something else.
    for query in rng.standard_normal((5, DIMENSIONS), dtype=np.float32):
        weights = query * (span / 255.0)
        shift = float(query @ low) - float(query @ query) / 2.0
        fused = norms - 2.0 * (codes.astype(np.float32) @ weights + shift)
        delta = dequantised - query
        direct = np.einsum("ij,ij->i", delta, delta)
        if not np.allclose(fused, direct, rtol=1e-2, atol=1e-2):
            raise AssertionError("fused SQ8 score differs from the direct distance")
        if not np.array_equal(np.argsort(fused)[:10], np.argsort(direct)[:10]):
            raise AssertionError("fused SQ8 ranking differs from the direct ranking")
    print(json.dumps({"self_test": "passed"}), flush=True)


if __name__ == "__main__":
    main()
