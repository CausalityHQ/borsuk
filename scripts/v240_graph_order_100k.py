#!/usr/bin/env python3
"""Query-blind graph bandwidth order, then sealed candidate page replay."""

import argparse
import hashlib
import json
import struct
from math import ceil
from pathlib import Path

import numpy as np

ROWS = 100_000
PAGE_ROWS = 256
PAGE_BYTES = PAGE_ROWS * (768 + 12)
SIZES = (512, 1024, 2048, 4096)
GRAPH_SHA = "d8b70919243a7cd6ecb9448ce23f776374738476c1882cbc6a651fb34753af2f"
RAW_SHA = "61e6e5b6a42931ce76cc496eee864e847593c295dfd0dba26db01c9021ccf7bf"


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def base_graph(path: Path):
    from scipy.sparse import csr_matrix
    if sha(path) != GRAPH_SHA:
        raise ValueError("V218 graph SHA differs")
    body = path.read_bytes()
    if (body[:8] != b"BORSVG01" or struct.unpack_from("<I", body, 8)[0] != 1
            or struct.unpack_from("<Q", body, 12)[0] != ROWS):
        raise ValueError("graph format or row count differs")
    starts = np.empty(ROWS, dtype=np.int64)
    lengths = np.empty(ROWS, dtype=np.int32)
    position = 96
    for row in range(ROWS):
        if position >= len(body):
            raise ValueError("truncated graph tower")
        layers = body[position]
        position += 1
        if not 1 <= layers <= 17:
            raise ValueError("graph tower count differs")
        for layer in range(layers):
            if position + 2 > len(body):
                raise ValueError("truncated graph degree")
            degree = struct.unpack_from("<H", body, position)[0]
            position += 2
            if degree > 256 or position + 4 * degree > len(body):
                raise ValueError("graph degree or edge bytes differ")
            if layer == layers - 1:
                starts[row], lengths[row] = position, degree
            position += 4 * degree
    if position != len(body):
        raise ValueError("graph trailing bytes")
    indptr = np.empty(ROWS + 1, dtype=np.int64)
    indptr[0] = 0
    np.cumsum(lengths, out=indptr[1:])
    indices = np.empty(int(indptr[-1]), dtype=np.int32)
    for row in range(ROWS):
        edge = np.frombuffer(body, dtype="<u4", count=int(lengths[row]), offset=int(starts[row]))
        if (edge >= ROWS).any() or (edge == row).any():
            raise ValueError("graph edge differs")
        indices[indptr[row]:indptr[row + 1]] = edge
    return csr_matrix((np.ones(len(indices), dtype=np.uint8), indices, indptr),
                      shape=(ROWS, ROWS))


def layout(graph_path: Path, order_path: Path, seal_path: Path) -> None:
    from scipy.sparse.csgraph import reverse_cuthill_mckee
    if order_path.exists() or seal_path.exists():
        raise ValueError("layout output exists")
    order = reverse_cuthill_mckee(base_graph(graph_path), symmetric_mode=False)
    if (len(order) != ROWS or not np.array_equal(np.sort(order), np.arange(ROWS))):
        raise ValueError("graph order is not a permutation")
    np.save(order_path, order.astype("<u4"), allow_pickle=False)
    seal_path.write_text(json.dumps({
        "schema": "borsuk-v240-graph-order-100k-layout-v1",
        "method": "scipy-1.14.1-reverse-cuthill-mckee-undirected-base-graph",
        "rows": ROWS, "graph_sha256": GRAPH_SHA,
        "order_sha256": sha(order_path), "query_or_truth_used": False,
    }, sort_keys=True) + "\n")


def percentile(values: list[int], p: int) -> int:
    return sorted(values)[ceil(len(values) * p / 100) - 1]


def replay(order_path: Path, seal_path: Path, raw_path: Path,
           counts_path: Path, summary_path: Path) -> None:
    if counts_path.exists() or summary_path.exists():
        raise ValueError("replay output exists")
    seal = json.loads(seal_path.read_text())
    if (seal.get("schema") != "borsuk-v240-graph-order-100k-layout-v1"
            or seal.get("graph_sha256") != GRAPH_SHA
            or seal.get("query_or_truth_used") is not False
            or seal.get("order_sha256") != sha(order_path)
            or sha(raw_path) != RAW_SHA):
        raise ValueError("layout or sealed candidates differ")
    order = np.load(order_path, allow_pickle=False)
    if (order.shape != (ROWS,) or not np.array_equal(np.sort(order), np.arange(ROWS))):
        raise ValueError("layout permutation differs")
    inverse = np.empty(ROWS, dtype=np.int32)
    inverse[order] = np.arange(ROWS, dtype=np.int32)
    counts = {mode: {size: [] for size in SIZES} for mode in ("old", "graph_local")}
    with raw_path.open() as source, counts_path.open("x") as output:
        for ordinal, line in enumerate(source):
            if ordinal >= 1000:
                raise ValueError("candidate panel too long")
            item = json.loads(line)
            rows = np.asarray(item["graph_rows"], dtype=np.int32)
            if (item["ordinal"] != ordinal or rows.shape != (4096,)
                    or rows.min() < 0 or rows.max() >= ROWS
                    or len(np.unique(rows)) != 4096):
                raise ValueError("candidate row roster differs")
            changed = inverse[rows]
            one = {"ordinal": ordinal, "old": {}, "graph_local": {}}
            for size in SIZES:
                old_pages = int(np.unique(rows[:size] // PAGE_ROWS).size)
                new_pages = int(np.unique(changed[:size] // PAGE_ROWS).size)
                counts["old"][size].append(old_pages)
                counts["graph_local"][size].append(new_pages)
                one["old"][str(size)] = old_pages
                one["graph_local"][str(size)] = new_pages
            output.write(json.dumps(one, sort_keys=True) + "\n")
    if len(counts["old"][1024]) != 1000 or percentile(counts["old"][1024], 95) != 147:
        raise ValueError("V239 old-layout p95 did not reproduce")
    stats = {mode: {str(size): {
        "p50_pages": percentile(values, 50),
        "p90_pages": percentile(values, 90),
        "p95_pages": percentile(values, 95),
        "p99_pages": percentile(values, 99),
        "p95_minimum_page_bytes": percentile(values, 95) * PAGE_BYTES,
    } for size, values in by_size.items()} for mode, by_size in counts.items()}
    summary_path.write_text(json.dumps({
        "schema": "borsuk-v240-graph-order-100k-replay-v1",
        "dataset": "ReLAION-100k D768", "queries": 1000, "candidate_prefix": 1024,
        "page_rows": PAGE_ROWS, "page_bytes": PAGE_BYTES,
        "layout_seal_sha256": sha(seal_path), "order_sha256": sha(order_path),
        "candidate_raw_sha256": RAW_SHA, "counts_sha256": sha(counts_path),
        "stats": stats,
        "gate_pass": stats["graph_local"]["1024"]["p95_minimum_page_bytes"] <= 16 * 1024 * 1024,
    }, sort_keys=True) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    build = sub.add_parser("layout")
    for name in ("graph", "order", "seal"):
        build.add_argument("--" + name, type=Path, required=True)
    score = sub.add_parser("replay")
    for name in ("order", "seal", "raw", "counts", "summary"):
        score.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    if args.mode == "layout":
        layout(args.graph, args.order, args.seal)
    else:
        replay(args.order, args.seal, args.raw, args.counts, args.summary)
