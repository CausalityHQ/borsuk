#!/usr/bin/env python3
"""Build the V73 serving format at 10M rows on a real corpus with real truth.

Every figure so far is 1M x 768 ReLAION. Scale was the one goal criterion with
no measurement behind it, and the registered 100M corpus needs a multi-hour
source scan. deep-image-96 is a way to get a real 10x answer in minutes:
9,990,000 vectors with shipped ground truth, already used elsewhere in this
repository's standard-dataset matrix.

The dataset is angular. Vectors and queries are normalised to unit length here,
which makes squared-L2 ranking identical to cosine ranking, so the shipped
ground truth is the right answer for the metric the serving path computes.

Emits the same three artifacts the 1M path uses - an SQ8 row object, and a
manifest carrying page summaries, codebooks, per-row codes, queries and truth -
so the native reader runs against it unchanged.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import urllib.request
from pathlib import Path

import numpy as np

PAGE_ROWS = 256
ROUTER_BLOCKS_PER_PAGE = 2
SUMMARY_SUBSPACES = 48
ROUTER_SUBSPACES = 48
CHUNK_ROWS = 16_384
MAGIC = b"BRSKV77\x00"
SOURCE_URL = "https://ann-benchmarks.com/deep-image-96-angular.hdf5"


def lloyd(data, clusters, iterations, seed):
    clusters = min(clusters, data.shape[0])
    generator = np.random.default_rng(seed)
    centroids = data[generator.choice(data.shape[0], clusters, replace=False)].copy()
    for _ in range(iterations):
        norms = np.einsum("ij,ij->i", centroids, centroids)
        assignment = np.empty(data.shape[0], dtype=np.int32)
        for start in range(0, data.shape[0], CHUNK_ROWS):
            stop = min(start + CHUNK_ROWS, data.shape[0])
            assignment[start:stop] = np.argmin(
                norms[None, :] - 2.0 * (data[start:stop] @ centroids.T), axis=1
            )
        counts = np.bincount(assignment, minlength=clusters)
        order = np.argsort(assignment, kind="stable")
        starts = np.concatenate(([0], np.cumsum(counts)[:-1]))
        occupied = counts > 0
        centroids[occupied] = (
            np.add.reduceat(data[order], starts[occupied], axis=0)
            / counts[occupied][:, None]
        )
    return centroids.astype(np.float32, copy=False)


def centroid_chain(centroids):
    clusters = centroids.shape[0]
    norms = np.einsum("ij,ij->i", centroids, centroids)
    start = int(np.argmin(norms - 2.0 * (centroids @ centroids.mean(axis=0))))
    remaining = np.ones(clusters, dtype=bool)
    chain = np.empty(clusters, dtype=np.int32)
    current = start
    for position in range(clusters):
        chain[position] = current
        remaining[current] = False
        if position + 1 == clusters:
            break
        distances = norms - 2.0 * (centroids @ centroids[current])
        distances[~remaining] = np.inf
        current = int(np.argmin(distances))
    return chain


def train_codebooks(sample, dimensions, subspaces, seed):
    width = dimensions // subspaces
    books = np.empty((subspaces, 256, width), dtype=np.float32)
    for index in range(subspaces):
        lo, hi = index * width, (index + 1) * width
        books[index] = lloyd(
            np.ascontiguousarray(sample[:, lo:hi]), 256, 10, seed + index
        )
    return books


def encode(data, books, dimensions, subspaces):
    width = dimensions // subspaces
    codes = np.empty((data.shape[0], subspaces), dtype=np.uint8)
    for index in range(subspaces):
        lo, hi = index * width, (index + 1) * width
        book = books[index]
        norms = np.einsum("ij,ij->i", book, book)
        for start in range(0, data.shape[0], CHUNK_ROWS):
            stop = min(start + CHUNK_ROWS, data.shape[0])
            codes[start:stop, index] = np.argmin(
                norms[None, :] - 2.0 * (data[start:stop, lo:hi] @ book.T), axis=1
            )
    return codes


def decode(codes, books, dimensions, subspaces):
    width = dimensions // subspaces
    out = np.empty((codes.shape[0], dimensions), dtype=np.float32)
    for index in range(subspaces):
        out[:, index * width : (index + 1) * width] = books[index][codes[:, index]]
    return out


def block_means(ordered, block_rows):
    rows = ordered.shape[0]
    starts = np.arange(0, rows, block_rows)
    counts = np.diff(np.append(starts, rows)).astype(np.float32)
    return (np.add.reduceat(ordered, starts, axis=0) / counts[:, None]).astype(np.float32)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--source", type=Path)
    parser.add_argument("--clusters", type=int, default=16_384)
    parser.add_argument("--queries", type=int, default=1_000)
    parser.add_argument("--sq8-out", type=Path)
    parser.add_argument("--manifest-out", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if args.self_test:
        rng = np.random.default_rng(82)
        data = rng.standard_normal((2_048, 24), dtype=np.float32)
        data /= np.linalg.norm(data, axis=1, keepdims=True)
        books = train_codebooks(data, 24, 12, 1)
        codes = encode(data, books, 24, 12)
        restored = decode(codes, books, 24, 12)
        if restored.shape != data.shape or not np.isfinite(restored).all():
            raise AssertionError("codec round trip differs")
        if np.linalg.norm(restored - data, axis=1).mean() >= np.linalg.norm(
            data - data.mean(axis=0), axis=1
        ).mean():
            raise AssertionError("codec is no better than the corpus mean")
        chain = centroid_chain(lloyd(data, 16, 5, 3))
        if sorted(chain.tolist()) != list(range(16)):
            raise AssertionError("centroid chain is not a permutation")
        print(json.dumps({"self_test": "passed"}), flush=True)
        return
    for name in ("source", "sq8_out", "manifest_out", "report"):
        if getattr(args, name) is None:
            parser.error(f"missing required argument: {name}")

    import h5py

    overall = time.perf_counter()
    if not args.source.exists():
        # The host rejects urllib's default User-Agent with 403; the
        # repository's own fetcher already sends a curl agent for this reason.
        request = urllib.request.Request(  # noqa: S310
            SOURCE_URL, headers={"User-Agent": "curl/8"}
        )
        with urllib.request.urlopen(request) as response:  # noqa: S310
            with args.source.open("wb") as handle:
                while True:
                    block = response.read(1 << 22)
                    if not block:
                        break
                    handle.write(block)
    with h5py.File(args.source, "r") as handle:
        train = np.asarray(handle["train"], dtype=np.float32)
        test = np.asarray(handle["test"], dtype=np.float32)[: args.queries]
        truth = np.asarray(handle["neighbors"], dtype=np.int64)[: args.queries, :100]
    rows, dimensions = train.shape
    queries, neighbors = truth.shape
    if dimensions % ROUTER_SUBSPACES or dimensions % SUMMARY_SUBSPACES:
        raise ValueError("dimension does not divide the codebook layout")

    # Angular dataset: unit-normalising makes squared-L2 rank identically to
    # cosine, so the shipped truth is correct for what the serving path computes.
    train /= np.maximum(np.linalg.norm(train, axis=1, keepdims=True), 1e-12)
    test /= np.maximum(np.linalg.norm(test, axis=1, keepdims=True), 1e-12)

    build_started = time.perf_counter()
    generator = np.random.default_rng(8201)
    sample = train[generator.choice(rows, min(rows, args.clusters * 64), replace=False)]
    centroids = lloyd(sample, args.clusters, 12, 8202)
    centroid_norms = np.einsum("ij,ij->i", centroids, centroids)
    assignment = np.empty(rows, dtype=np.int32)
    radius = np.empty(rows, dtype=np.float32)
    for start in range(0, rows, CHUNK_ROWS):
        stop = min(start + CHUNK_ROWS, rows)
        scores = centroid_norms[None, :] - 2.0 * (train[start:stop] @ centroids.T)
        best = np.argmin(scores, axis=1)
        assignment[start:stop] = best
        radius[start:stop] = np.take_along_axis(scores, best[:, None], 1)[:, 0]
    chain = centroid_chain(centroids)
    chain_rank = np.empty(args.clusters, dtype=np.int64)
    chain_rank[chain] = np.arange(args.clusters)
    key = chain_rank[assignment].astype(np.float64) * (
        float(radius.max()) - float(radius.min()) + 1.0
    ) + radius.astype(np.float64)
    order = np.argsort(key, kind="stable").astype(np.int64)
    ordered = np.ascontiguousarray(train[order])
    del train, sample, key, radius, assignment

    low = ordered.min(axis=0).astype(np.float32)
    span = np.maximum(ordered.max(axis=0) - low, 1e-12).astype(np.float32)
    sq8 = np.clip(np.rint((ordered - low) / span * 255.0), 0, 255).astype(np.uint8)
    restored = low + sq8.astype(np.float32) * (span / 255.0)
    norms = np.einsum("ij,ij->i", restored, restored).astype(np.float32)
    del restored
    row_bytes = 8 + 4 + dimensions
    blob = np.empty((rows, row_bytes), dtype=np.uint8)
    blob[:, :8] = order.astype(np.int64).view(np.uint8).reshape(rows, 8)
    blob[:, 8:12] = norms.view(np.uint8).reshape(rows, 4)
    blob[:, 12:] = sq8
    del sq8, norms

    sample = ordered[generator.choice(rows, min(rows, 200_000), replace=False)]
    books = train_codebooks(sample, dimensions, ROUTER_SUBSPACES, 8203)
    row_codes = encode(ordered, books, dimensions, ROUTER_SUBSPACES)
    summary_books = train_codebooks(sample, dimensions, SUMMARY_SUBSPACES, 8204)
    raw_summaries = block_means(ordered, PAGE_ROWS // ROUTER_BLOCKS_PER_PAGE)
    summaries = decode(
        encode(raw_summaries, summary_books, dimensions, SUMMARY_SUBSPACES),
        summary_books, dimensions, SUMMARY_SUBSPACES,
    )
    del ordered, raw_summaries
    pages = (rows + PAGE_ROWS - 1) // PAGE_ROWS
    wanted = pages * ROUTER_BLOCKS_PER_PAGE
    if summaries.shape[0] < wanted:
        summaries = np.concatenate(
            [summaries, np.repeat(summaries[-1:], wanted - summaries.shape[0], axis=0)]
        )
    build_seconds = time.perf_counter() - build_started

    blob.tofile(args.sq8_out)
    with args.manifest_out.open("wb") as handle:
        handle.write(MAGIC)
        for value in (rows, dimensions, PAGE_ROWS, pages, queries, neighbors,
                      ROUTER_SUBSPACES, dimensions // ROUTER_SUBSPACES,
                      ROUTER_BLOCKS_PER_PAGE):
            handle.write(np.uint64(value).tobytes())
        handle.write(np.ascontiguousarray(summaries, dtype=np.float32).tobytes())
        handle.write(np.ascontiguousarray(low, dtype=np.float32).tobytes())
        handle.write(np.ascontiguousarray(span / 255.0, dtype=np.float32).tobytes())
        handle.write(np.ascontiguousarray(books, dtype=np.float32).tobytes())
        handle.write(np.ascontiguousarray(row_codes, dtype=np.uint8).tobytes())
        handle.write(np.ascontiguousarray(test, dtype=np.float32).tobytes())
        handle.write(np.ascontiguousarray(truth, dtype=np.int64).tobytes())

    report = {
        "schema": "borsuk-v82-scale-build-result-v1",
        "dataset": "deep-image-96-angular",
        "rows": int(rows),
        "dimensions": int(dimensions),
        "queries": int(queries),
        "neighbors": int(neighbors),
        "clusters": int(args.clusters),
        "pages": int(pages),
        "router_subspaces": ROUTER_SUBSPACES,
        "sq8_row_bytes": int(row_bytes),
        "sq8_bytes": int(blob.nbytes),
        "manifest_bytes": int(args.manifest_out.stat().st_size),
        "resident_router_bytes_per_row": ROUTER_SUBSPACES,
        "build_seconds": round(build_seconds, 3),
        "build_vectors_per_second": int(rows / build_seconds),
        "elapsed_seconds": round(time.perf_counter() - overall, 3),
    }
    args.report.write_text(json.dumps(report, sort_keys=True) + "\n")
    print(json.dumps(report, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
