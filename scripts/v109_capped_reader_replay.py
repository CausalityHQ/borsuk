#!/usr/bin/env python3
"""Read-free V63/V77 same-layout capped SQ8 replay on a Spot worker.

The manifest and SQ8 object are authenticated by the worker before this
program starts. No S3 query read occurs here. The two returned-result arms
share the same PQ64 nomination; only their physical range plans differ.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.v109_range_plan import admit_ranked_pages, plan_page_ranges

MAGIC = b"BRSKV77\x00"
ROWS = 1_000_000
DIMENSIONS = 768
PAGE_ROWS = 256
ROW_BYTES = 780
MAX_GETS = 32
MAX_BYTES = 16_777_216
SQ8_SHA256 = "2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b"
SQ8_DTYPE = np.dtype([("id", "<i8"), ("norm", "<f4"),
                      ("code", "u1", (DIMENSIONS,))], align=False)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_manifest(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    if raw[:8] != MAGIC:
        raise ValueError("V77 manifest magic differs")
    header = np.frombuffer(raw, dtype="<u8", count=9, offset=8)
    rows, dimensions, page_rows, pages, queries, neighbors, subspaces, width, blocks = map(int, header)
    if (rows, dimensions, page_rows, pages, queries, neighbors,
        subspaces, width, blocks) != (ROWS, DIMENSIONS, PAGE_ROWS, 3907, 1000,
                                       100, 64, 12, 2):
        raise ValueError("V77 manifest geometry differs")
    cursor = 8 + 9 * 8

    def take(dtype: str, shape: tuple[int, ...]) -> np.ndarray:
        nonlocal cursor
        count = int(np.prod(shape))
        result = np.frombuffer(raw, dtype=dtype, count=count, offset=cursor).reshape(shape)
        cursor += result.nbytes
        return result

    summaries = take("<f4", (pages * blocks, dimensions))
    low = take("<f4", (dimensions,))
    span_step = take("<f4", (dimensions,))
    books = take("<f4", (subspaces, 256, width))
    codes = take("u1", (rows, subspaces))
    query_vectors = take("<f4", (queries, dimensions))
    truth = take("<i8", (queries, neighbors))
    if cursor != len(raw) or not np.isfinite(summaries).all() or not np.isfinite(books).all():
        raise ValueError("V77 manifest length or numerical values differ")
    return {"raw": raw, "summaries": summaries, "low": low,
            "span_step": span_step, "books": books, "codes": codes,
            "queries": query_vectors, "truth": truth}


def nominate(
    query: np.ndarray, manifest: dict[str, object], *, regions: int, shortlist: int
) -> tuple[list[int], list[int]]:
    summaries = manifest["summaries"]
    books = manifest["books"]
    codes = manifest["codes"]
    pages = (ROWS + PAGE_ROWS - 1) // PAGE_ROWS
    summary_norms = np.einsum("ij,ij->i", summaries, summaries)
    page_scores = (summary_norms - 2.0 * (summaries @ query)).reshape(pages, 2).min(axis=1)
    chosen = np.lexsort((np.arange(pages), page_scores))[:regions]
    chosen.sort()
    rows = np.concatenate([
        np.arange(page * PAGE_ROWS, min((page + 1) * PAGE_ROWS, ROWS), dtype=np.int32)
        for page in chosen
    ])
    delta = books - query.reshape(64, 1, 12)
    table = np.einsum("ijk,ijk->ij", delta, delta)
    scores = np.zeros(rows.size, dtype=np.float32)
    for subspace in range(64):
        scores += table[subspace, codes[rows, subspace]]
    best = np.lexsort((rows, scores))[:shortlist]
    ranked: dict[int, float] = {}
    for index in best:
        page = int(rows[index]) // PAGE_ROWS
        ranked[page] = min(ranked.get(page, float("inf")), float(scores[index]))
    ordered = sorted(ranked, key=lambda page: (ranked[page], page))
    return ordered, sorted(ranked)


def score_ranges(
    sq8: np.memmap,
    query: np.ndarray,
    manifest: dict[str, object],
    ranges: tuple[tuple[int, int], ...],
) -> list[int]:
    weights = query * manifest["span_step"]
    shift = float(query @ manifest["low"] - (query @ query) / 2.0)
    identifiers: list[np.ndarray] = []
    distances: list[np.ndarray] = []
    for start, end in ranges:
        if start % ROW_BYTES or end % ROW_BYTES or start >= end:
            raise ValueError("SQ8 interval differs")
        rows = sq8[start // ROW_BYTES:end // ROW_BYTES]
        identifiers.append(rows["id"])
        inner = rows["code"].astype(np.float32) @ weights
        distances.append(rows["norm"] - 2.0 * (inner + shift))
    if not identifiers:
        raise ValueError("empty reader plan")
    ids = np.concatenate(identifiers)
    scores = np.concatenate(distances)
    selected = np.lexsort((ids, scores))[:100]
    return [int(value) for value in ids[selected]]


def replay(
    manifest_path: Path, sq8_path: Path, output_path: Path, *,
    query_count: int, regions: int, shortlist: int,
) -> None:
    if not 1 <= query_count <= 1000 or regions not in (256, 1024, 3907) or shortlist != 512:
        raise ValueError("replay operating point differs")
    if sq8_path.stat().st_size != ROWS * ROW_BYTES or SQ8_DTYPE.itemsize != ROW_BYTES:
        raise ValueError("SQ8 object size or record geometry differs")
    sq8_sha256 = sha256_file(sq8_path)
    if sq8_sha256 != SQ8_SHA256:
        raise ValueError("V70 SQ8 object SHA-256 differs")
    manifest = load_manifest(manifest_path)
    sq8 = np.memmap(sq8_path, dtype=SQ8_DTYPE, mode="r", shape=(ROWS,))
    header = {"schema": "borsuk-v109-capped-replay-v1", "source": "V63/V70/V77",
              "manifest_sha256": sha256_file(manifest_path),
              "sq8_sha256": sq8_sha256, "query_count": query_count,
              "regions": regions, "shortlist": shortlist,
              "max_gets": MAX_GETS, "max_bytes": MAX_BYTES,
              "data_reads": "local-authenticated-object-no-query-S3-GET"}
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    with temporary.open("w") as handle:
        handle.write(json.dumps(header, sort_keys=True, separators=(",", ":")) + "\n")
        for ordinal in range(query_count):
            query = manifest["queries"][ordinal]
            ranked_pages, historical_pages = nominate(
                query, manifest, regions=regions, shortlist=shortlist
            )
            old = plan_page_ranges(historical_pages, gap_pages=2, rows=ROWS,
                                   page_rows=PAGE_ROWS, row_bytes=ROW_BYTES)
            capped = admit_ranked_pages(
                ranked_pages, rows=ROWS, page_rows=PAGE_ROWS, row_bytes=ROW_BYTES,
                max_gets=MAX_GETS, max_bytes=MAX_BYTES
            )
            if not capped.plan.within(gets=MAX_GETS, bytes_limit=MAX_BYTES):
                raise AssertionError("capped planner exceeded a physical cap")
            old_ids = score_ranges(sq8, query, manifest, old.ranges)
            capped_ids = score_ranges(sq8, query, manifest, capped.plan.ranges)
            truth = set(map(int, manifest["truth"][ordinal]))
            row = {"query_ordinal": ordinal, "ranked_pages": ranked_pages,
                   "historical_pages": historical_pages,
                   "historical_gets": old.gets, "historical_bytes": old.bytes,
                   "historical_returned_ids": old_ids,
                   "historical_hits": len(truth.intersection(old_ids)),
                   "capped_nominated_pages": capped.nominated_pages,
                   "capped_rejected_pages": capped.rejected_pages,
                   "capped_gets": capped.plan.gets, "capped_bytes": capped.plan.bytes,
                   "capped_returned_ids": capped_ids,
                   "capped_hits": len(truth.intersection(capped_ids))}
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(output_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--sq8", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--regions", type=int, default=1024)
    args = parser.parse_args()
    replay(args.manifest, args.sq8, args.output, query_count=args.queries,
           regions=args.regions, shortlist=512)


if __name__ == "__main__":
    main()
