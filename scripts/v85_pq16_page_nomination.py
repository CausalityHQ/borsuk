#!/usr/bin/env python3
"""Claim-ineligible PQ16 page-nomination fail-fast screen for V85.

The screen reads authenticated local artifacts only.  It trains PQ16 from the
base corpus without queries or truth, ranks base rows with ADC, maps the fixed
top-2,048 rows to the existing physical pages, and treats the authenticated
delta tier as resident.  It measures page containment and planned S3 range
work; it does not claim serving latency or page-SQ8 rerank quality.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import pathlib
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq


@dataclasses.dataclass(frozen=True)
class PageEntry:
    offset: int
    encoded_bytes: int


def _sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _coalesced_work(
    selected_pages: list[int], page_entries: dict[int, PageEntry], gap_pages: int
) -> tuple[int, int]:
    if not selected_pages:
        return 0, 0
    groups: list[list[int]] = [[selected_pages[0]]]
    for page in selected_pages[1:]:
        if page - groups[-1][-1] <= gap_pages + 1:
            groups[-1].append(page)
        else:
            groups.append([page])
    encoded_bytes = 0
    for group in groups:
        first = page_entries[group[0]]
        last = page_entries[group[-1]]
        encoded_bytes += last.offset + last.encoded_bytes - first.offset
    return len(groups), encoded_bytes


def evaluate_page_nominations(
    *,
    ranked_base_ids: np.ndarray,
    truth_ids: np.ndarray,
    base_page_by_id: dict[int, int],
    resident_delta_ids: set[int],
    page_entries: dict[int, PageEntry],
    neighbors: int,
    gap_pages: int,
    max_gets: int,
    max_bytes: int,
    min_recall_ppm: int,
) -> dict[str, Any]:
    if (
        ranked_base_ids.ndim != 2
        or truth_ids.ndim != 2
        or ranked_base_ids.shape[0] != truth_ids.shape[0]
        or truth_ids.shape[1] != neighbors
        or neighbors <= 0
        or gap_pages < 0
    ):
        raise ValueError("PQ16 page-nomination shape differs")
    samples = []
    for query in range(truth_ids.shape[0]):
        selected_pages = sorted(
            {
                base_page_by_id[int(row_id)]
                for row_id in ranked_base_ids[query]
                if int(row_id) in base_page_by_id
            }
        )
        if any(page not in page_entries for page in selected_pages):
            raise ValueError("PQ16 page nomination references an unknown page")
        gets, encoded_bytes = _coalesced_work(
            selected_pages, page_entries, gap_pages
        )
        selected = set(selected_pages)
        hit_ids = [
            int(row_id)
            for row_id in truth_ids[query]
            if int(row_id) in resident_delta_ids
            or base_page_by_id.get(int(row_id)) in selected
        ]
        samples.append(
            {
                "bytes": encoded_bytes,
                "gets": gets,
                "hit_ids": hit_ids,
                "hits": len(hit_ids),
                "query": query,
                "selected_pages": selected_pages,
                "truth_ids": [int(value) for value in truth_ids[query]],
            }
        )
    total_hits = sum(sample["hits"] for sample in samples)
    aggregate_recall_ppm = round(
        total_hits * 1_000_000 / (len(samples) * neighbors)
    )
    worst_recall_ppm = round(
        min(sample["hits"] for sample in samples) * 1_000_000 / neighbors
    )
    max_gets_per_query = max(sample["gets"] for sample in samples)
    max_bytes_per_query = max(sample["bytes"] for sample in samples)
    return {
        "aggregate_recall_ppm": aggregate_recall_ppm,
        "gate_passed": aggregate_recall_ppm >= min_recall_ppm
        and worst_recall_ppm >= min_recall_ppm
        and max_gets_per_query <= max_gets
        and max_bytes_per_query <= max_bytes,
        "max_bytes_per_query": max_bytes_per_query,
        "max_gets_per_query": max_gets_per_query,
        "samples": samples,
        "worst_recall_ppm": worst_recall_ppm,
    }


def _train_pq16(vectors: np.ndarray, seed: int) -> tuple[np.ndarray, np.ndarray]:
    rows, dimensions = vectors.shape
    subspaces = 16
    if dimensions % subspaces != 0 or rows < 256:
        raise ValueError("PQ16 training shape differs")
    width = dimensions // subspaces
    generator = np.random.default_rng(seed)
    sample = vectors[
        generator.choice(rows, min(rows, 100_000), replace=False)
    ]
    books = np.empty((subspaces, 256, width), dtype=np.float32)
    codes = np.empty((rows, subspaces), dtype=np.uint8)
    for subspace in range(subspaces):
        lo, hi = subspace * width, (subspace + 1) * width
        training = np.ascontiguousarray(sample[:, lo:hi])
        centroids = training[
            generator.choice(training.shape[0], 256, replace=False)
        ].copy()
        for _ in range(10):
            norms = np.einsum("ij,ij->i", centroids, centroids)
            assignment = np.empty(training.shape[0], dtype=np.int32)
            for start in range(0, training.shape[0], 8192):
                stop = min(start + 8192, training.shape[0])
                block = training[start:stop]
                assignment[start:stop] = np.argmin(
                    norms[None, :] - 2.0 * (block @ centroids.T), axis=1
                )
            counts = np.bincount(assignment, minlength=256)
            order = np.argsort(assignment, kind="stable")
            starts = np.concatenate(([0], np.cumsum(counts)[:-1]))
            occupied = counts > 0
            centroids[occupied] = (
                np.add.reduceat(training[order], starts[occupied], axis=0)
                / counts[occupied, None]
            )
        books[subspace] = centroids
        norms = np.einsum("ij,ij->i", centroids, centroids)
        for start in range(0, rows, 8192):
            stop = min(start + 8192, rows)
            block = vectors[start:stop, lo:hi]
            codes[start:stop, subspace] = np.argmin(
                norms[None, :] - 2.0 * (block @ centroids.T), axis=1
            )
    return books, codes


def _rank_pq16(
    queries: np.ndarray,
    base_ids: np.ndarray,
    books: np.ndarray,
    codes: np.ndarray,
    shortlist_rows: int,
) -> np.ndarray:
    subspaces, _, width = books.shape
    ranked = np.empty((queries.shape[0], shortlist_rows), dtype=np.int64)
    offsets = np.arange(subspaces, dtype=np.int32) * 256
    code_offsets = codes.astype(np.int32) + offsets[None, :]
    for query_ordinal, query in enumerate(queries):
        table = np.empty((subspaces, 256), dtype=np.float32)
        for subspace in range(subspaces):
            lo, hi = subspace * width, (subspace + 1) * width
            delta = books[subspace] - query[lo:hi]
            table[subspace] = np.einsum("ij,ij->i", delta, delta)
        scores = table.reshape(-1)[code_offsets].sum(axis=1)
        head = np.argpartition(scores, shortlist_rows - 1)[:shortlist_rows]
        ordered = head[np.lexsort((base_ids[head], scores[head]))]
        ranked[query_ordinal] = base_ids[ordered]
    return ranked


def _read_fixed_list(path: pathlib.Path, field: str, dimensions: int) -> np.ndarray:
    table = pq.read_table(path)
    column = table.column(field).combine_chunks()
    values = np.asarray(column.values.to_numpy(), dtype=np.float32).reshape(
        -1, dimensions
    )
    if column.null_count or not np.isfinite(values).all():
        raise ValueError(f"{field} authority differs")
    return values


def _read_page_run(
    path: pathlib.Path, run: dict[str, Any], dimensions: int
) -> tuple[dict[int, int], dict[int, PageEntry]]:
    identity = run["object"]
    if path.stat().st_size != identity["bytes"] or _sha256_file(path) != identity["sha256"]:
        raise ValueError("PQ16 run identity differs")
    body = path.read_bytes()
    page_by_id: dict[int, int] = {}
    page_entries: dict[int, PageEntry] = {}
    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("sequence", pa.uint64(), nullable=False),
            pa.field("state", pa.uint8(), nullable=False),
            pa.field(
                "code",
                pa.list_(pa.field("element", pa.uint8(), nullable=False), dimensions),
                nullable=False,
            ),
        ]
    )
    for page in run["pages"]:
        start, stop = page["offset"], page["offset"] + page["bytes"]
        table = ipc.open_stream(pa.py_buffer(body[start:stop])).read_all()
        if table.schema != expected_schema or table.num_rows != page["rows"]:
            raise ValueError("PQ16 page authority differs")
        ids = table.column("id").to_pylist()
        states = table.column("state").to_pylist()
        if any(state != 0 for state in states):
            raise ValueError("PQ16 page contains a non-live row")
        for row_id in ids:
            if int(row_id) in page_by_id:
                raise ValueError("PQ16 run contains a duplicate row")
            page_by_id[int(row_id)] = int(page["page"])
        page_entries[int(page["page"])] = PageEntry(
            offset=int(page["offset"]), encoded_bytes=int(page["bytes"])
        )
    return page_by_id, page_entries


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("source", "queries", "truth", "generation", "base", "delta", "output"):
        parser.add_argument(f"--{name}", type=pathlib.Path, required=True)
    for name in ("source", "queries", "truth", "generation"):
        parser.add_argument(f"--{name}-uri", required=True)
        parser.add_argument(f"--{name}-sha256", required=True)
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument("--neighbors", type=int, default=100)
    parser.add_argument("--queries-count", type=int, default=32)
    parser.add_argument("--shortlist-rows", type=int, default=2048)
    parser.add_argument("--gap-pages", type=int, default=2)
    parser.add_argument("--seed", type=int, default=7216)
    args = parser.parse_args()

    identities = {}
    for name in ("source", "queries", "truth", "generation"):
        path = getattr(args, name)
        digest = getattr(args, f"{name}_sha256")
        uri = getattr(args, f"{name}_uri")
        if not uri.startswith("s3://") or _sha256_file(path) != digest:
            raise ValueError(f"{name} identity differs")
        identities[name] = {
            "bytes": path.stat().st_size,
            "sha256": digest,
            "uri": uri,
        }

    generation_body = args.generation.read_bytes()
    generation = json.loads(generation_body)
    if _canonical_bytes(generation) != generation_body:
        raise ValueError("generation canonical bytes differ")
    if generation.get("dimensions") != args.dimensions:
        raise ValueError("generation dimensions differ")
    base_runs = [run for run in generation["runs"] if run["kind"] == "base"]
    delta_runs = [run for run in generation["runs"] if run["kind"] == "delta"]
    if len(base_runs) != 1 or len(delta_runs) != 1:
        raise ValueError("screen requires exactly one base and one delta run")
    base_page_by_id, page_entries = _read_page_run(
        args.base, base_runs[0], args.dimensions
    )
    delta_page_by_id, _ = _read_page_run(args.delta, delta_runs[0], args.dimensions)

    source = pq.read_table(args.source)
    source_ids = np.asarray(
        source.column("feature_row_id").combine_chunks().to_numpy(), dtype=np.int64
    )
    vectors = np.asarray(
        source.column("embedding").combine_chunks().values.to_numpy(),
        dtype=np.float32,
    ).reshape(-1, args.dimensions)
    if len(set(source_ids.tolist())) != len(source_ids) or not np.isfinite(vectors).all():
        raise ValueError("source authority differs")
    row_by_id = {int(row_id): row for row, row_id in enumerate(source_ids)}
    if set(base_page_by_id) | set(delta_page_by_id) != set(row_by_id):
        raise ValueError("generation rows differ from source")
    base_ids = np.asarray(sorted(base_page_by_id), dtype=np.int64)
    base_vectors = np.ascontiguousarray(
        vectors[[row_by_id[int(row_id)] for row_id in base_ids]]
    )
    queries = _read_fixed_list(args.queries, "vector", args.dimensions)
    truth_table = pq.read_table(args.truth)
    truth_ids = np.asarray(
        truth_table.column("neighbors").combine_chunks().values.to_numpy(),
        dtype=np.int64,
    ).reshape(-1, args.neighbors)
    if queries.shape[0] != args.queries_count or truth_ids.shape[0] != args.queries_count:
        raise ValueError("evaluation query count differs")

    books, codes = _train_pq16(base_vectors, args.seed)
    ranked = _rank_pq16(
        queries, base_ids, books, codes, args.shortlist_rows
    )
    evaluation = evaluate_page_nominations(
        ranked_base_ids=ranked,
        truth_ids=truth_ids,
        base_page_by_id=base_page_by_id,
        resident_delta_ids=set(delta_page_by_id),
        page_entries=page_entries,
        neighbors=args.neighbors,
        gap_pages=args.gap_pages,
        max_gets=32,
        max_bytes=16 * 1024 * 1024,
        min_recall_ppm=990_000,
    )
    result = {
        **evaluation,
        "claim_eligible": False,
        "code_row_bytes": 16,
        "codes_built_without_queries_or_truth": True,
        "dimensions": args.dimensions,
        "evidence_kind": "offline-page-containment-not-serving-quality-or-latency",
        "gap_pages": args.gap_pages,
        "inputs": identities,
        "neighbors": args.neighbors,
        "queries": args.queries_count,
        "resident_bytes_at_100m": 1_600_000_000,
        "rows": len(source_ids),
        "schema": "borsuk-v85-pq16-page-nomination-result-v1",
        "shortlist_rows": args.shortlist_rows,
    }
    body = _canonical_bytes(result)
    args.output.write_bytes(body)
    print(json.dumps({
        "aggregate_recall_ppm": result["aggregate_recall_ppm"],
        "gate_passed": result["gate_passed"],
        "max_bytes_per_query": result["max_bytes_per_query"],
        "max_gets_per_query": result["max_gets_per_query"],
        "result_sha256": hashlib.sha256(body).hexdigest(),
        "worst_recall_ppm": result["worst_recall_ppm"],
    }, separators=(",", ":"), sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
