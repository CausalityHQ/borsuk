#!/usr/bin/env python3
"""Can a resident per-row coarse code replace the page-summary router?

V63 showed the layout holds every query's top-100 inside 64 pages, with a p95
of 35. V71's router needs 256 pages to reach 99.2%. That four-to-sevenfold
routing inefficiency is the entire reason a query reads 27 to 63 MiB, and no
amount of gap tuning touches it - V64 and V66 only ever summarised *pages*.

This tests the untested axis: a coarse product-quantised code for every row,
held resident, used to pick rows directly. Their pages are then read. If a
16-to-32-byte code cuts the page budget from 256 to 64, bytes and requests both
fall about fourfold with no second wave and no loss of recall.

Scored by reconstruction, which is equivalent to asymmetric scoring against the
same codes and lets the whole corpus be ranked with one sgemm. Recall is
containment of the true top-100 in the selected pages, which V70 showed the SQ8
scan then returns intact. Codes are corpus-only.
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




CODEBOOK_PLANS = ((16, "pq16"), (32, "pq32"), (64, "pq64"))
SHORTLIST_ROWS = (512, 2_048, 8_192, 32_768)
GAP_PAGES = 2
QUERY_CHUNK = 50
SQ_ROW_BYTES = 8 + 4 + DIMENSIONS


def coalesce(sorted_units, gap):
    if sorted_units.size == 0:
        return 0, 0
    breaks = np.flatnonzero(np.diff(sorted_units) > gap + 1)
    starts = np.concatenate(([sorted_units[0]], sorted_units[breaks + 1]))
    ends = np.concatenate((sorted_units[breaks], [sorted_units[-1]]))
    return starts.size, int((ends - starts + 1).sum())


def nearest_rank(ordered, numerator, denominator):
    index = max(0, min(len(ordered) - 1, (len(ordered) * numerator - 1) // denominator))
    return int(ordered[index])


def stats(values):
    ordered = np.sort(values)
    return {
        "p50": nearest_rank(ordered, 50, 100),
        "p95": nearest_rank(ordered, 95, 100),
        "maximum": int(ordered[-1]),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--source", type=Path)
    parser.add_argument("--development-query", type=Path)
    parser.add_argument("--ground-truth", type=Path)
    parser.add_argument("--layout-order", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.self_test:
        units = np.array([1, 2, 3, 9, 40], dtype=np.int64)
        if coalesce(units, 0) != (3, 5) or coalesce(units, 6) != (2, 10):
            raise AssertionError("coalescing differs")
        print(json.dumps({"self_test": "passed"}), flush=True)
        return
    for name in ("source", "development_query", "ground_truth", "layout_order", "output"):
        if getattr(args, name) is None:
            parser.error(f"missing required argument: {name}")

    started = time.perf_counter()
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

    # Identifier -> original row -> layout position. Indexing the inverse
    # permutation by a sorted-array offset instead of by a row silently yields
    # a random mapping, which reads as chance-level recall rather than as an
    # error, so the round trip is checked rather than assumed.
    ranking = np.argsort(feature_ids, kind="stable")
    located = np.searchsorted(feature_ids[ranking], truth_ids)
    if np.any(located == ROWS) or not np.array_equal(
        feature_ids[ranking][located], truth_ids
    ):
        raise ValueError("ground truth references an unknown identifier")
    original_rows = ranking[located]
    position = np.empty(ROWS, dtype=np.int64)
    position[order] = np.arange(ROWS, dtype=np.int64)
    truth_rows = position[original_rows]
    if not np.array_equal(ordered_ids[truth_rows], truth_ids.astype(np.int64)):
        raise ValueError("identifier round trip through the layout differs")
    truth_pages = truth_rows // PAGE_ROWS

    cells = []
    for subspaces, label in CODEBOOK_PLANS:
        reconstruction = pq_like(ordered, subspaces, 7200 + subspaces)
        norms = np.einsum("ij,ij->i", reconstruction, reconstruction).astype(np.float32)
        hits = {size: np.zeros(QUERIES, dtype=np.int32) for size in SHORTLIST_ROWS}
        touched = {size: np.zeros(QUERIES, dtype=np.int32) for size in SHORTLIST_ROWS}
        requests = {size: np.zeros(QUERIES, dtype=np.int32) for size in SHORTLIST_ROWS}
        covered = {size: np.zeros(QUERIES, dtype=np.int32) for size in SHORTLIST_ROWS}
        for start in range(0, QUERIES, QUERY_CHUNK):
            stop = min(start + QUERY_CHUNK, QUERIES)
            block = norms[None, :] - 2.0 * (queries[start:stop] @ reconstruction.T)
            for local, query in enumerate(range(start, stop)):
                row_scores = block[local]
                biggest = max(SHORTLIST_ROWS)
                head = np.argpartition(row_scores, biggest - 1)[:biggest]
                head = head[np.argsort(row_scores[head], kind="stable")]
                for size in SHORTLIST_ROWS:
                    selected = np.unique(head[:size] // PAGE_ROWS)
                    touched[size][query] = selected.size
                    count, span = coalesce(selected, GAP_PAGES)
                    requests[size][query] = count
                    covered[size][query] = span
                    hits[size][query] = np.isin(truth_pages[query], selected).sum()
        for size in SHORTLIST_ROWS:
            resident = subspaces
            cells.append({
                "router": f"resident_row_{label}",
                "code_row_bytes": subspaces,
                "resident_gib_at_100m_x100": int(round(resident * 1e8 / 2**30 * 100)),
                "shortlist_rows": size,
                "aggregate_recall_ppm": int(round(
                    float(hits[size].sum()) * 1_000_000 / (QUERIES * NEIGHBORS))),
                "worst_recall_ppm": int(hits[size].min()) * 10_000,
                "pages_touched": stats(touched[size]),
                "requests_gap2": stats(requests[size]),
                "sq8_bytes_p95": nearest_rank(np.sort(covered[size]), 95, 100)
                                 * PAGE_ROWS * SQ_ROW_BYTES,
            })
            print(json.dumps(cells[-1], default=int), flush=True)
        del reconstruction

    result = {
        "schema": "borsuk-v72-resident-row-router-result-v1",
        "claim_eligible": False,
        "evidence_kind": "page-containment-for-a-resident-per-row-coarse-code-not-latency",
        "codes_built_without_queries_or_ground_truth": True,
        "source_rows": ROWS,
        "queries": QUERIES,
        "neighbors": NEIGHBORS,
        "page_rows": PAGE_ROWS,
        "gap_pages": GAP_PAGES,
        "shortlist_rows": list(SHORTLIST_ROWS),
        "cells": cells,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "validation_opened": False,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":"), default=int) + "\n").encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest()}), flush=True)


def pq_like(data, subspaces, seed, sample_rows=100_000):
    width = DIMENSIONS // subspaces
    generator = np.random.default_rng(seed)
    take = min(data.shape[0], sample_rows)
    sample = data[generator.choice(data.shape[0], take, replace=False)]
    out = np.empty_like(data)
    for index in range(subspaces):
        lo, hi = index * width, (index + 1) * width
        book = lloyd(np.ascontiguousarray(sample[:, lo:hi]), 256, 10, seed + index)
        norms = np.einsum("ij,ij->i", book, book)
        for start in range(0, data.shape[0], CHUNK_ROWS):
            stop = min(start + CHUNK_ROWS, data.shape[0])
            block = data[start:stop, lo:hi]
            out[start:stop, lo:hi] = book[
                np.argmin(norms[None, :] - 2.0 * (block @ book.T), axis=1)
            ]
    return out


if __name__ == "__main__":
    main()
