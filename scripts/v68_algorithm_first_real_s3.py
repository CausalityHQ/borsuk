#!/usr/bin/env python3
"""End-to-end measurement against real S3: build throughput, then read latency.

Every result through V67 counted bytes, pages and requests. None of them issued
a GET. This builds the V65/V66 index as real S3 objects and serves real queries
against them, so the latency and throughput numbers are measured rather than
modelled.

Layout is the preserved V63 k-means order. Three objects:

  codes.bin    PQ192 code per row, row-major in layout order
  exact.bin    id + f32 vector per row, row-major in layout order
  (router)     PQ192 page summaries, resident in the reader

A query scores the resident router, coalesces its chosen pages into ranges,
issues concurrent ranged GETs against codes.bin, scores those rows by
asymmetric distance, coalesces the surviving shortlist into ranges, issues
concurrent ranged GETs against exact.bin, and rescores exactly.

Latency is wall-clock in-region. CPU-side scoring here is NumPy, not the
crate's SIMD path, so the split between I/O and compute is reported separately
and the compute half is an upper bound rather than a product number.
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
STAGE_TWO_GAP_ROWS = 64
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
    truth = pq.read_table(args.ground_truth, columns=["feature_row_id"])
    truth_ids = scalar(truth, "feature_row_id", np.uint64).reshape(QUERIES, NEIGHBORS)
    queries = fixed_list(
        pq.read_table(args.development_query), "embedding", DIMENSIONS, QUERIES
    )
    order = np.asarray(np.load(args.layout_order), dtype=np.int32)
    if not np.array_equal(np.sort(order), np.arange(ROWS, dtype=np.int32)):
        raise ValueError("layout order is not a row permutation")
    ordered = np.ascontiguousarray(vectors[order])
    ordered_ids = feature_ids[order].astype(np.int64)
    del vectors
    pages = (ROWS + PAGE_ROWS - 1) // PAGE_ROWS

    # ---- build ----
    build_started = time.perf_counter()
    books = train_pq(ordered, 6801)
    train_seconds = time.perf_counter() - build_started
    encode_started = time.perf_counter()
    codes = encode_pq(ordered, books)
    encode_seconds = time.perf_counter() - encode_started
    summaries = decode_pq(
        encode_pq(block_means(ordered, PAGE_ROWS // ROUTER_BLOCKS_PER_PAGE), books), books
    )
    exact_blob = np.empty((ROWS, EXACT_ROW_BYTES), dtype=np.uint8)
    exact_blob[:, :8] = ordered_ids.view(np.uint8).reshape(ROWS, 8)
    exact_blob[:, 8:] = ordered.view(np.uint8).reshape(ROWS, DIMENSIONS * 4)
    build_seconds = time.perf_counter() - build_started

    client = boto3.client(
        "s3",
        config=Config(max_pool_connections=READ_THREADS * 2, retries={"max_attempts": 3}),
    )
    upload_started = time.perf_counter()
    for key, blob in (("codes.bin", codes), ("exact.bin", exact_blob)):
        path = Path(f"/mnt/{key}")
        blob.tofile(path)
        client.upload_file(str(path), args.bucket, f"{args.prefix}/{key}")
        path.unlink()
    upload_seconds = time.perf_counter() - upload_started
    uploaded_bytes = codes.nbytes + exact_blob.nbytes
    del exact_blob, codes, ordered
    print(json.dumps({"phase": "built and uploaded",
                      "build_seconds": round(build_seconds, 2),
                      "upload_seconds": round(upload_seconds, 2)}), flush=True)

    # ---- read ----
    index = Index(
        ObjectReader(client, args.bucket, f"{args.prefix}/codes.bin", READ_THREADS),
        ObjectReader(client, args.bucket, f"{args.prefix}/exact.bin", READ_THREADS),
        books,
        summaries,
        pages,
    )
    truth_sets = [set(truth_ids[i].astype(np.int64).tolist()) for i in range(QUERIES)]

    cold, recalls = [], []
    for query in range(MEASURED_QUERIES):
        returned, timing = index.search(queries[query])
        cold.append(timing)
        recalls.append(len(truth_sets[query] & set(returned.tolist())))
    recall = np.asarray(recalls, dtype=np.int32)

    warm = []
    for query in range(MEASURED_QUERIES):
        _, timing = index.search(queries[query])
        warm.append(timing)

    def summarise(samples, label):
        return {
            "pass": label,
            "queries": len(samples),
            "total": latency_stats([s["total_ms"] for s in samples]),
            "stage_one_io": latency_stats([s["stage_one_io_ms"] for s in samples]),
            "stage_two_io": latency_stats([s["stage_two_io_ms"] for s in samples]),
            "cpu_numpy": latency_stats(
                [s["stage_one_cpu_ms"] + s["stage_two_cpu_ms"] + s["route_ms"]
                 for s in samples]
            ),
            "requests_p50": int(np.median([s["requests"] for s in samples])),
            "requests_p95": int(
                nearest_rank(np.sort([s["requests"] for s in samples]), 95, 100)
            ),
            "bytes_p50": int(np.median([s["bytes"] for s in samples])),
            "bytes_p95": int(nearest_rank(np.sort([s["bytes"] for s in samples]), 95, 100)),
        }

    throughput = []
    for workers in CONCURRENCY_LADDER:
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(lambda q: index.search(queries[q])[1],
                          range(MEASURED_QUERIES)))
        elapsed = time.perf_counter() - started
        throughput.append(
            {"workers": workers, "queries": MEASURED_QUERIES,
             "elapsed_seconds": round(elapsed, 3),
             "qps": round(MEASURED_QUERIES / elapsed, 2)}
        )
        print(json.dumps(throughput[-1]), flush=True)

    result = {
        "schema": "borsuk-v68-algorithm-first-real-s3-result-v1",
        "claim_eligible": False,
        "evidence_kind": "measured-in-region-s3-latency-and-build-throughput",
        "storage": "real-s3-ranged-gets-no-local-cache",
        "cpu_path": "numpy-adc-not-the-crate-simd-path",
        "source_rows": ROWS,
        "dimensions": DIMENSIONS,
        "neighbors": NEIGHBORS,
        "page_rows": PAGE_ROWS,
        "router_pages": ROUTER_PAGES,
        "shortlist": SHORTLIST,
        "read_threads": READ_THREADS,
        "build": {
            "pq_train_seconds": round(train_seconds, 3),
            "pq_encode_seconds": round(encode_seconds, 3),
            "build_seconds": round(build_seconds, 3),
            "build_vectors_per_second": int(ROWS / build_seconds),
            "upload_seconds": round(upload_seconds, 3),
            "upload_bytes": int(uploaded_bytes),
            "upload_mib_per_second": round(uploaded_bytes / upload_seconds / 2**20, 2),
            "end_to_end_vectors_per_second": int(ROWS / (build_seconds + upload_seconds)),
        },
        "recall": {
            "queries": int(recall.size),
            "aggregate_ppm": int(round(float(recall.sum()) * 1_000_000 / (recall.size * NEIGHBORS))),
            "worst_ppm": int(recall.min()) * 10_000,
        },
        "latency": [summarise(cold, "first_pass"), summarise(warm, "repeated_pass")],
        "throughput": throughput,
        "elapsed_seconds": round(time.perf_counter() - overall, 3),
        "validation_opened": False,
    }
    payload = (
        json.dumps(result, sort_keys=True, separators=(",", ":"), default=int) + "\n"
    ).encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest()}), flush=True)


def self_test() -> None:
    global ROWS, DIMENSIONS, PQ_SUBSPACES, CODE_ROW_BYTES
    ROWS, DIMENSIONS, PQ_SUBSPACES = 2_048, 32, 16
    CODE_ROW_BYTES = PQ_SUBSPACES
    rng = np.random.default_rng(68)
    data = rng.standard_normal((ROWS, DIMENSIONS), dtype=np.float32)
    books = train_pq(data, 1, sample_rows=2_048)
    codes = encode_pq(data, books)
    if codes.shape != (ROWS, PQ_SUBSPACES) or codes.dtype != np.uint8:
        raise AssertionError("code shape or dtype differs")
    # Asymmetric distance must equal the distance to the reconstruction.
    reconstruction = decode_pq(codes, books)
    for query in rng.standard_normal((5, DIMENSIONS), dtype=np.float32):
        adc = adc_score(codes, adc_table(query, books))
        delta = reconstruction - query
        direct = np.einsum("ij,ij->i", delta, delta)
        if not np.allclose(adc, direct, rtol=1e-3, atol=1e-3):
            raise AssertionError("ADC score differs from the reconstruction distance")
    # Range arithmetic must cover exactly the rows it claims.
    for gap in (0, 4, 64):
        units = np.unique(rng.integers(0, 500, 40))
        for first, last in coalesce(units, gap):
            if first > last:
                raise AssertionError("range bounds inverted")
        covered = set()
        for first, last in coalesce(units, gap):
            covered.update(range(first, last + 1))
        if not set(units.tolist()) <= covered:
            raise AssertionError("coalesced ranges do not cover every selected unit")
    print(json.dumps({"self_test": "passed"}), flush=True)


if __name__ == "__main__":
    main()
