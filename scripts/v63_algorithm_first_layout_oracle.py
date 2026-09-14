#!/usr/bin/env python3
"""Query-independent page-layout oracle ceiling for the 1M x 768 ReLAION2B screen.

This probe answers one question: for a row layout built without ever seeing a
query or the ground truth, what is the maximum Recall@100 any router could ever
reach by reading its best K pages?

It is an upper bound. It does not measure routing, latency, S3 requests, or
returned recall. A layout that fails here can never be repaired by a better
router; a layout that passes here has only proven that discovery is the
remaining problem.

Two layout families are screened:

* linear     - one contiguous row permutation cut into fixed-size pages. Every
               row appears exactly once, so the best-K-page oracle is exact.
* replicated - SPANN-style postings. Each row is written into the posting list
               of its r nearest centroids, so a row can appear in r pages. The
               best-K-page oracle here is a greedy max-coverage lower bound on
               the true optimum, which keeps the reported ceiling honest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

ROWS = 1_000_000
QUERIES = 1_000
NEIGHBORS = 100
DIMENSIONS = 768
GRAPH_DEGREE = 64

PAGE_ROWS = (32, 64, 128, 256)
PAGE_BUDGET_LADDER = (4, 8, 12, 16, 24, 32, 48, 64, 96, 128)
KMEANS_CLUSTERS = (1_024, 8_192)
REPLICATION_FACTORS = (1, 2, 4, 8)
MAXIMUM_REPLICATION = max(REPLICATION_FACTORS)

# Per-row encoded cost models for the same layout, smallest realistic first.
ROW_BYTE_MODELS = {
    "sq8_plain": 16 + DIMENSIONS,
    "f32_plain": 16 + DIMENSIONS * 4,
    "f32_graph": 16 + DIMENSIONS * 4 + GRAPH_DEGREE * 4,
}
PAGE_HEADER_BYTES = 64

# Promotion gate, evaluated against the sq8_plain cost model.
GATE_PAGES = 32
GATE_BYTES = 16 * 1024 * 1024
GATE_AGGREGATE_PPM = 995_000
GATE_WORST_PPM = 800_000


def fixed_list(table, name: str, width: int, rows: int) -> np.ndarray:
    column = table[name].combine_chunks()
    values = column.values.to_numpy(zero_copy_only=False)
    result = np.array(values, dtype=np.float32, copy=True).reshape(rows, width)
    if result.shape != (rows, width) or not np.isfinite(result).all():
        raise ValueError(f"{name} differs")
    return result


def scalar(table, name: str, dtype) -> np.ndarray:
    return np.asarray(
        table[name].combine_chunks().to_numpy(zero_copy_only=False), dtype=dtype
    )


def load_inputs(source_path: Path, truth_path: Path):
    source = pq.read_table(source_path)
    truth = pq.read_table(
        truth_path, columns=["query_ordinal", "rank", "feature_row_id"]
    )
    if source.num_rows != ROWS or truth.num_rows != QUERIES * NEIGHBORS:
        raise ValueError("row count differs")
    vectors = fixed_list(source, "embedding", DIMENSIONS, ROWS)
    feature_ids = scalar(source, "feature_row_id", np.uint64)
    if len(np.unique(feature_ids)) != ROWS:
        raise ValueError("source feature IDs are not unique")
    truth_queries = scalar(truth, "query_ordinal", np.int64)
    truth_ranks = scalar(truth, "rank", np.int64)
    truth_ids = scalar(truth, "feature_row_id", np.uint64).reshape(QUERIES, NEIGHBORS)
    if not np.array_equal(
        truth_queries, np.repeat(np.arange(QUERIES), NEIGHBORS)
    ) or not np.array_equal(truth_ranks, np.tile(np.arange(NEIGHBORS), QUERIES)):
        raise ValueError("ground-truth order differs")
    order = np.argsort(feature_ids, kind="stable")
    sorted_ids = feature_ids[order]
    positions = np.searchsorted(sorted_ids, truth_ids)
    if np.any(positions == ROWS) or not np.array_equal(
        sorted_ids[positions], truth_ids
    ):
        raise ValueError("ground truth references an unknown feature ID")
    truth_rows = order[positions].astype(np.int32, copy=False)
    return vectors, truth_rows


def validate_permutation(order: np.ndarray, label: str) -> np.ndarray:
    order = np.asarray(order, dtype=np.int32)
    if order.shape != (ROWS,) or not np.array_equal(
        np.sort(order), np.arange(ROWS, dtype=np.int32)
    ):
        raise ValueError(f"{label} is not a row permutation")
    return order


def load_order(path: Path, label: str) -> np.ndarray:
    return validate_permutation(np.load(path), label)


def centroid_chain(centroids: np.ndarray) -> np.ndarray:
    """Greedy nearest-neighbour chain over centroids, starting near the mean.

    Places geometrically adjacent clusters next to each other so that a page
    straddling two clusters still holds nearby rows.
    """
    clusters = centroids.shape[0]
    norms = np.einsum("ij,ij->i", centroids, centroids)
    mean = centroids.mean(axis=0)
    start = int(np.argmin(norms - 2.0 * centroids @ mean))
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


class PostingLayout:
    """One materialised posting-page layout for a fixed replication factor."""

    __slots__ = (
        "row_pages",
        "total_pages",
        "padded_slots",
        "pages_by_cluster",
        "start_by_cluster",
    )

    def __init__(
        self,
        row_pages: np.ndarray,
        total_pages: int,
        padded_slots: int,
        pages_by_cluster: np.ndarray,
        start_by_cluster: np.ndarray,
    ) -> None:
        self.row_pages = row_pages
        self.total_pages = total_pages
        self.padded_slots = padded_slots
        self.pages_by_cluster = pages_by_cluster
        self.start_by_cluster = start_by_cluster


class CoarsePartition:
    """Corpus-only geometric partition. Never sees a query or the ground truth."""

    def __init__(self, vectors: np.ndarray, clusters: int, seed: int) -> None:
        import faiss

        started = time.perf_counter()
        kmeans = faiss.Kmeans(
            DIMENSIONS,
            clusters,
            niter=15,
            verbose=False,
            seed=seed,
            max_points_per_centroid=128,
        )
        kmeans.train(vectors)
        centroids = np.asarray(kmeans.centroids, dtype=np.float32)
        index = faiss.IndexFlatL2(DIMENSIONS)
        index.add(centroids)
        distances, assignment = index.search(vectors, MAXIMUM_REPLICATION)
        self.clusters = clusters
        self.seed = seed
        self.centroids = centroids
        self.assignment = assignment.astype(np.int32, copy=False)
        self.distances = distances.astype(np.float32, copy=False)
        self.chain = centroid_chain(centroids)
        self.chain_rank = np.empty(clusters, dtype=np.int64)
        self.chain_rank[self.chain] = np.arange(clusters, dtype=np.int64)
        self._probe_order = None
        sizes = np.bincount(self.assignment[:, 0], minlength=clusters)
        self.telemetry = {
            "clusters": clusters,
            "seed": seed,
            "train_and_assign_seconds": round(time.perf_counter() - started, 3),
            "empty_clusters": int((sizes == 0).sum()),
            "minimum_primary_cluster_rows": int(sizes.min()),
            "median_primary_cluster_rows": int(np.median(sizes)),
            "maximum_primary_cluster_rows": int(sizes.max()),
        }

    def probe_order(self, queries: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Rank every cluster by centroid distance for every query.

        This is the whole router: a query reads posting pages in ascending
        centroid distance. It is a fixed rule with no query-side training and
        no access to the ground truth.
        """
        if self._probe_order is None:
            centroid_norms = np.einsum(
                "ij,ij->i", self.centroids, self.centroids
            ).astype(np.float32)
            distances = centroid_norms[None, :] - 2.0 * (queries @ self.centroids.T)
            order = np.argsort(distances, axis=1, kind="stable").astype(np.int32)
            rank = np.empty_like(order)
            rows = np.arange(order.shape[0], dtype=np.int32)[:, None]
            rank[rows, order] = np.arange(self.clusters, dtype=np.int32)[None, :]
            self._probe_order = (order, rank)
        return self._probe_order

    def contiguous_order(self) -> np.ndarray:
        """One row per page slot, clusters laid out along the centroid chain."""
        primary = self.assignment[:, 0].astype(np.int64)
        radius = self.distances[:, 0].astype(np.float64)
        key = self.chain_rank[primary] * (float(radius.max()) + 1.0) + radius
        order = np.argsort(key, kind="stable").astype(np.int32, copy=False)
        return validate_permutation(order, f"kmeans_{self.clusters}")

    def replicated_pages(self, replication: int, page_rows: int):
        """SPANN-style postings: row -> its `replication` nearest clusters.

        Returns (row_pages, total_pages, padded_slots). `row_pages[row, slot]`
        is the page holding that row's `slot`-th posting entry.
        """
        assignment = self.assignment[:, :replication]
        radius = self.distances[:, :replication]
        flat_cluster = assignment.reshape(-1)
        flat_radius = radius.reshape(-1).astype(np.float64)
        flat_row = np.repeat(
            np.arange(ROWS, dtype=np.int32), replication
        )
        flat_slot = np.tile(
            np.arange(replication, dtype=np.int8), ROWS
        )
        key = self.chain_rank[flat_cluster] * (float(flat_radius.max()) + 1.0)
        key += flat_radius
        entry_order = np.argsort(key, kind="stable")
        sorted_cluster = flat_cluster[entry_order]
        counts = np.bincount(sorted_cluster, minlength=self.clusters)
        chain_counts = counts[self.chain]
        cluster_pages = (chain_counts + page_rows - 1) // page_rows
        page_base = np.concatenate(
            ([0], np.cumsum(cluster_pages, dtype=np.int64)[:-1])
        )
        within = np.arange(sorted_cluster.size, dtype=np.int64)
        starts = np.concatenate(
            ([0], np.cumsum(chain_counts, dtype=np.int64)[:-1])
        )
        rank_in_chain = self.chain_rank[sorted_cluster]
        within -= starts[rank_in_chain]
        page_of_entry = page_base[rank_in_chain] + within // page_rows
        row_pages = np.empty((ROWS, replication), dtype=np.int64)
        row_pages[flat_row[entry_order], flat_slot[entry_order]] = page_of_entry
        total_pages = int(cluster_pages.sum())
        pages_by_cluster = np.zeros(self.clusters, dtype=np.int64)
        start_by_cluster = np.zeros(self.clusters, dtype=np.int64)
        pages_by_cluster[self.chain] = cluster_pages
        start_by_cluster[self.chain] = page_base
        return PostingLayout(
            row_pages=row_pages,
            total_pages=total_pages,
            padded_slots=total_pages * page_rows - int(counts.sum()),
            pages_by_cluster=pages_by_cluster,
            start_by_cluster=start_by_cluster,
        )


def nearest_rank(ordered: np.ndarray, numerator: int, denominator: int) -> int:
    index = max(0, min(len(ordered) - 1, (len(ordered) * numerator - 1) // denominator))
    return int(ordered[index])


def page_byte_models(page_rows: int) -> dict[str, int]:
    return {
        model: PAGE_HEADER_BYTES + page_rows * row_bytes
        for model, row_bytes in ROW_BYTE_MODELS.items()
    }


def build_curve(hits: np.ndarray, page_bytes: dict[str, int], page_rows: int) -> list:
    curve = []
    for index, budget in enumerate(PAGE_BUDGET_LADDER):
        row = hits[index]
        curve.append(
            {
                "pages": budget,
                "rows_scanned": budget * page_rows,
                "aggregate_oracle_recall_ppm": int(
                    round(float(row.sum()) * 1_000_000 / (QUERIES * NEIGHBORS))
                ),
                "worst_query_oracle_recall_ppm": int(row.min()) * 10_000,
                "p05_query_oracle_recall_ppm": nearest_rank(np.sort(row), 5, 100)
                * 10_000,
                "bytes": {model: budget * value for model, value in page_bytes.items()},
            }
        )
    return curve


def gate_pages(curve: list) -> int | None:
    for entry in curve:
        if (
            entry["pages"] <= GATE_PAGES
            and entry["bytes"]["sq8_plain"] <= GATE_BYTES
            and entry["aggregate_oracle_recall_ppm"] >= GATE_AGGREGATE_PPM
            and entry["worst_query_oracle_recall_ppm"] >= GATE_WORST_PPM
        ):
            return entry["pages"]
    return None


def evaluate_linear(
    layout: str, order: np.ndarray, truth_rows: np.ndarray, page_rows: int
) -> dict:
    row_to_position = np.empty(ROWS, dtype=np.int32)
    row_to_position[order] = np.arange(ROWS, dtype=np.int32)
    pages = row_to_position[truth_rows] // page_rows

    hits = np.zeros((len(PAGE_BUDGET_LADDER), QUERIES), dtype=np.int32)
    distinct = np.zeros(QUERIES, dtype=np.int32)
    for query in range(QUERIES):
        counts = np.sort(np.unique(pages[query], return_counts=True)[1])[::-1]
        distinct[query] = counts.size
        cumulative = np.cumsum(counts)
        for index, budget in enumerate(PAGE_BUDGET_LADDER):
            hits[index, query] = cumulative[min(budget, counts.size) - 1]

    page_bytes = page_byte_models(page_rows)
    curve = build_curve(hits, page_bytes, page_rows)
    sorted_distinct = np.sort(distinct)
    return {
        "family": "linear",
        "layout": layout,
        "replication": 1,
        "page_rows": page_rows,
        "stored_pages": (ROWS + page_rows - 1) // page_rows,
        "storage_multiplier_x1000": 1_000,
        "oracle_kind": "exact-best-k-pages",
        "page_bytes": page_bytes,
        "curve": curve,
        "distinct_gt_pages": {
            "p50": nearest_rank(sorted_distinct, 50, 100),
            "p90": nearest_rank(sorted_distinct, 90, 100),
            "p99": nearest_rank(sorted_distinct, 99, 100),
            "maximum": int(sorted_distinct[-1]),
            "mean_x1000": int(round(float(distinct.mean()) * 1000)),
        },
        "gate_passing_pages": gate_pages(curve),
    }


def evaluate_replicated(
    layout: str,
    posting: PostingLayout,
    truth_rows: np.ndarray,
    replication: int,
    page_rows: int,
) -> dict:
    row_pages = posting.row_pages
    total_pages = posting.total_pages
    padded_slots = posting.padded_slots
    maximum_budget = PAGE_BUDGET_LADDER[-1]
    hits = np.zeros((len(PAGE_BUDGET_LADDER), QUERIES), dtype=np.int32)
    distinct = np.zeros(QUERIES, dtype=np.int32)
    ladder_index = {budget: index for index, budget in enumerate(PAGE_BUDGET_LADDER)}
    for query in range(QUERIES):
        entries = row_pages[truth_rows[query]]
        unique_pages, inverse = np.unique(entries, return_inverse=True)
        distinct[query] = unique_pages.size
        membership = np.zeros((unique_pages.size, NEIGHBORS), dtype=bool)
        membership[
            inverse.reshape(NEIGHBORS, replication).ravel(),
            np.repeat(np.arange(NEIGHBORS), replication),
        ] = True
        # Greedy max-coverage with incrementally maintained gains: each covered
        # neighbour is discounted from every page exactly once, so the whole
        # selection costs O(pages * NEIGHBORS) rather than O(rounds * pages *
        # NEIGHBORS).
        gains = membership.sum(axis=1).astype(np.int32)
        covered = np.zeros(NEIGHBORS, dtype=bool)
        total = 0
        exhausted_at = maximum_budget
        for round_index in range(1, maximum_budget + 1):
            best = int(np.argmax(gains))
            gain = int(gains[best])
            if gain > 0:
                newly = membership[best] & ~covered
                covered |= newly
                gains -= membership[:, newly].sum(axis=1).astype(np.int32)
                total += gain
            if round_index in ladder_index:
                hits[ladder_index[round_index], query] = total
            if gain == 0 or total == NEIGHBORS:
                exhausted_at = round_index
                break
        for budget, index in ladder_index.items():
            if budget > exhausted_at:
                hits[index, query] = total

    page_bytes = page_byte_models(page_rows)
    curve = build_curve(hits, page_bytes, page_rows)
    sorted_distinct = np.sort(distinct)
    baseline_pages = (ROWS + page_rows - 1) // page_rows
    return {
        "family": "replicated",
        "layout": layout,
        "replication": replication,
        "page_rows": page_rows,
        "stored_pages": total_pages,
        "padded_empty_slots": int(padded_slots),
        "storage_multiplier_x1000": int(round(total_pages * 1000 / baseline_pages)),
        "oracle_kind": "greedy-max-coverage-lower-bound-on-best-k-pages",
        "page_bytes": page_bytes,
        "curve": curve,
        "distinct_gt_pages": {
            "p50": nearest_rank(sorted_distinct, 50, 100),
            "p90": nearest_rank(sorted_distinct, 90, 100),
            "p99": nearest_rank(sorted_distinct, 99, 100),
            "maximum": int(sorted_distinct[-1]),
            "mean_x1000": int(round(float(distinct.mean()) * 1000)),
        },
        "gate_passing_pages": gate_pages(curve),
    }


def evaluate_routed(
    layout: str,
    partition: CoarsePartition,
    posting: PostingLayout,
    queries: np.ndarray,
    truth_rows: np.ndarray,
    replication: int,
    page_rows: int,
) -> dict:
    """Measured containment for the fixed nearest-centroid page router.

    Pages are read in ascending centroid distance, whole posting list at a
    time, with the budget's final list truncated to its closest-first page
    prefix. Because every fetched row is exactly rescored, a ground-truth row
    that lands in a fetched page is returned, so this containment equals
    Recall@100 for an exact-rerank serving path.
    """
    order, rank = partition.probe_order(queries)
    pages_along_order = posting.pages_by_cluster[order]
    cumulative = np.cumsum(pages_along_order, axis=1)

    truth_clusters = partition.assignment[truth_rows][:, :, :replication]
    truth_pages = posting.row_pages[truth_rows]
    truth_ranks = np.take_along_axis(
        rank[:, None, :], truth_clusters.astype(np.int64), axis=2
    )

    query_rows = np.arange(QUERIES, dtype=np.int64)
    hits = np.zeros((len(PAGE_BUDGET_LADDER), QUERIES), dtype=np.int32)
    fetched_pages = np.zeros((len(PAGE_BUDGET_LADDER), QUERIES), dtype=np.int32)
    for index, budget in enumerate(PAGE_BUDGET_LADDER):
        whole = (cumulative <= budget).sum(axis=1)
        consumed = np.where(
            whole > 0, cumulative[query_rows, np.maximum(whole - 1, 0)], 0
        )
        partial_index = np.minimum(whole, partition.clusters - 1)
        partial_cluster = order[query_rows, partial_index]
        partial_pages = np.where(whole < partition.clusters, budget - consumed, 0)
        partial_pages = np.minimum(
            partial_pages, posting.pages_by_cluster[partial_cluster]
        )
        partial_limit = posting.start_by_cluster[partial_cluster] + partial_pages
        found = truth_ranks < whole[:, None, None]
        found |= (truth_ranks == whole[:, None, None]) & (
            truth_pages < partial_limit[:, None, None]
        )
        hits[index] = found.any(axis=2).sum(axis=1)
        fetched_pages[index] = np.minimum(consumed + partial_pages, budget)

    page_bytes = page_byte_models(page_rows)
    curve = build_curve(hits, page_bytes, page_rows)
    for index, entry in enumerate(curve):
        entry["p50_fetched_pages"] = nearest_rank(np.sort(fetched_pages[index]), 50, 100)
        entry["p95_fetched_pages"] = nearest_rank(np.sort(fetched_pages[index]), 95, 100)
        entry["exact_scores_at_p95"] = entry["p95_fetched_pages"] * page_rows
    baseline_pages = (ROWS + page_rows - 1) // page_rows
    return {
        "family": "routed",
        "layout": layout,
        "replication": replication,
        "page_rows": page_rows,
        "stored_pages": posting.total_pages,
        "storage_multiplier_x1000": int(
            round(posting.total_pages * 1000 / baseline_pages)
        ),
        "oracle_kind": "measured-nearest-centroid-router-equals-exact-rerank-recall",
        "page_bytes": page_bytes,
        "curve": curve,
        "distinct_gt_pages": {"p50": 0, "p90": 0, "p99": 0, "maximum": 0, "mean_x1000": 0},
        "gate_passing_pages": gate_pages(curve),
    }


def summarize(cell: dict) -> dict:
    at_32 = next(
        entry for entry in cell["curve"] if entry["pages"] == GATE_PAGES
    )
    return {
        "family": cell["family"],
        "layout": cell["layout"],
        "replication": cell["replication"],
        "page_rows": cell["page_rows"],
        "aggregate_at_32_ppm": at_32["aggregate_oracle_recall_ppm"],
        "worst_at_32_ppm": at_32["worst_query_oracle_recall_ppm"],
        "distinct_gt_pages_p50": cell["distinct_gt_pages"]["p50"],
        "storage_multiplier_x1000": cell["storage_multiplier_x1000"],
        "gate_passing_pages": cell["gate_passing_pages"],
    }


def self_test() -> None:
    """Tiny synthetic end-to-end boundary run with exact ground truth.

    This is a correctness boundary, not evidence. It proves the layout,
    posting-page and oracle arithmetic behave before the 1M run spends a
    machine on them.
    """
    global ROWS, QUERIES, NEIGHBORS, DIMENSIONS
    global PAGE_ROWS, PAGE_BUDGET_LADDER, KMEANS_CLUSTERS
    global REPLICATION_FACTORS, MAXIMUM_REPLICATION

    ROWS = 4_096
    QUERIES = 32
    NEIGHBORS = 10
    DIMENSIONS = 16
    PAGE_ROWS = (8, 16)
    PAGE_BUDGET_LADDER = (1, 2, 4, 8, 16, 32)
    KMEANS_CLUSTERS = (32,)
    REPLICATION_FACTORS = (1, 2)
    MAXIMUM_REPLICATION = 2

    rng = np.random.default_rng(20260914)
    vectors = rng.standard_normal((ROWS, DIMENSIONS), dtype=np.float32)
    queries = rng.standard_normal((QUERIES, DIMENSIONS), dtype=np.float32)
    norms = np.einsum("ij,ij->i", vectors, vectors)
    truth_rows = np.empty((QUERIES, NEIGHBORS), dtype=np.int32)
    for index in range(QUERIES):
        distances = norms - 2.0 * (vectors @ queries[index])
        truth_rows[index] = np.argsort(distances, kind="stable")[:NEIGHBORS]

    identity = np.arange(ROWS, dtype=np.int32)
    for page_rows in PAGE_ROWS:
        cell = evaluate_linear("original", identity, truth_rows, page_rows)
        aggregates = [
            entry["aggregate_oracle_recall_ppm"] for entry in cell["curve"]
        ]
        if aggregates != sorted(aggregates):
            raise AssertionError("linear oracle recall is not monotone in pages")
        if cell["curve"][-1]["pages"] < cell["distinct_gt_pages"]["maximum"]:
            raise AssertionError("self-test ladder cannot reach full containment")
        if aggregates[-1] != 1_000_000:
            raise AssertionError("linear oracle never reaches full containment")

    partition = CoarsePartition(vectors, KMEANS_CLUSTERS[0], seed=7)
    validate_permutation(partition.contiguous_order(), "self-test contiguous")
    layout = f"kmeans_{KMEANS_CLUSTERS[0]}"
    for replication in REPLICATION_FACTORS:
        for page_rows in PAGE_ROWS:
            posting = partition.replicated_pages(replication, page_rows)
            row_pages = posting.row_pages
            if row_pages.shape != (ROWS, replication):
                raise AssertionError("posting page map shape differs")
            if row_pages.min() < 0 or row_pages.max() >= posting.total_pages:
                raise AssertionError("posting page id out of range")
            occupancy = np.bincount(
                row_pages.reshape(-1), minlength=posting.total_pages
            )
            if occupancy.max() > page_rows:
                raise AssertionError("a posting page holds more rows than it can")
            if int(occupancy.sum()) != ROWS * replication:
                raise AssertionError("posting entry count differs from replication")
            if posting.padded_slots != (
                posting.total_pages * page_rows - ROWS * replication
            ):
                raise AssertionError("padded slot accounting differs")
            if int(posting.pages_by_cluster.sum()) != posting.total_pages:
                raise AssertionError("per-cluster page counts differ from the total")
            for row in range(0, ROWS, 257):
                if len(set(row_pages[row].tolist())) != replication:
                    raise AssertionError("a row shares one page with itself")
            for cluster in range(partition.clusters):
                count = int(posting.pages_by_cluster[cluster])
                if count == 0:
                    continue
                start = int(posting.start_by_cluster[cluster])
                owned = row_pages[
                    np.any(
                        partition.assignment[:, :replication] == cluster, axis=1
                    )
                ]
                inside = (owned >= start) & (owned < start + count)
                if int(inside.sum()) != int(
                    np.any(
                        partition.assignment[:, :replication] == cluster, axis=1
                    ).sum()
                ):
                    raise AssertionError("cluster page range does not own its rows")
            oracle = evaluate_replicated(
                layout, posting, truth_rows, replication, page_rows
            )
            routed = evaluate_routed(
                layout, partition, posting, queries, truth_rows, replication, page_rows
            )
            for cell, kind in ((oracle, "oracle"), (routed, "routed")):
                aggregates = [
                    entry["aggregate_oracle_recall_ppm"] for entry in cell["curve"]
                ]
                if aggregates != sorted(aggregates):
                    raise AssertionError(f"{kind} recall is not monotone in pages")
                if aggregates[-1] != 1_000_000:
                    raise AssertionError(f"{kind} never reaches full containment")
            for entry in routed["curve"]:
                if entry["p95_fetched_pages"] > entry["pages"]:
                    raise AssertionError("the router fetched more pages than its budget")
            if oracle["storage_multiplier_x1000"] < replication * 1_000:
                raise AssertionError("replicated storage multiplier is below its floor")
    print(json.dumps({"self_test": "passed"}, sort_keys=True), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--source", type=Path)
    parser.add_argument("--development-query", type=Path)
    parser.add_argument("--ground-truth", type=Path)
    parser.add_argument("--bfs-order", type=Path)
    parser.add_argument("--random-order", type=Path)
    parser.add_argument("--artifact-directory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    missing = [
        name
        for name in (
            "source",
            "development_query",
            "ground_truth",
            "bfs_order",
            "random_order",
            "artifact_directory",
            "output",
        )
        if getattr(args, name) is None
    ]
    if missing:
        parser.error(f"missing required arguments: {', '.join(missing)}")

    started = time.perf_counter()
    vectors, truth_rows = load_inputs(args.source, args.ground_truth)
    query_table = pq.read_table(args.development_query)
    if query_table.num_rows != QUERIES:
        raise ValueError("development query row count differs")
    queries = fixed_list(query_table, "embedding", DIMENSIONS, QUERIES)
    args.artifact_directory.mkdir(parents=True, exist_ok=True)

    cells: list[dict] = []
    linear_layouts = {
        "original": np.arange(ROWS, dtype=np.int32),
        "graph_bfs": load_order(args.bfs_order, "graph_bfs"),
        "random": load_order(args.random_order, "random"),
    }
    partition_telemetry = []
    partitions: dict[str, CoarsePartition] = {}
    for clusters in KMEANS_CLUSTERS:
        name = f"kmeans_{clusters}"
        partition = CoarsePartition(vectors, clusters, seed=20260914 + clusters)
        partitions[name] = partition
        order = partition.contiguous_order()
        linear_layouts[name] = order
        path = args.artifact_directory / f"{name}-order.npy"
        np.save(path, order)
        telemetry = dict(partition.telemetry)
        telemetry["layout"] = name
        telemetry["artifact_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        partition_telemetry.append(telemetry)
        print(json.dumps(telemetry, sort_keys=True), flush=True)

    for name, order in linear_layouts.items():
        for page_rows in PAGE_ROWS:
            cell = evaluate_linear(name, order, truth_rows, page_rows)
            cells.append(cell)
            print(json.dumps(summarize(cell), sort_keys=True), flush=True)

    for name, partition in partitions.items():
        for replication in REPLICATION_FACTORS:
            for page_rows in PAGE_ROWS:
                posting = partition.replicated_pages(replication, page_rows)
                for cell in (
                    evaluate_replicated(
                        name, posting, truth_rows, replication, page_rows
                    ),
                    evaluate_routed(
                        name,
                        partition,
                        posting,
                        queries,
                        truth_rows,
                        replication,
                        page_rows,
                    ),
                ):
                    cells.append(cell)
                    print(json.dumps(summarize(cell), sort_keys=True), flush=True)

    promoted = [cell for cell in cells if cell["gate_passing_pages"] is not None]
    best = max(
        cells,
        key=lambda cell: next(
            entry["aggregate_oracle_recall_ppm"]
            for entry in cell["curve"]
            if entry["pages"] == GATE_PAGES
        ),
    )
    result = {
        "schema": "borsuk-v63-algorithm-first-layout-oracle-result-v1",
        "claim_eligible": False,
        "evidence_kind": "oracle-page-containment-ceiling-not-routing-not-latency",
        "layouts_built_without_queries_or_ground_truth": True,
        "source_rows": ROWS,
        "dimensions": DIMENSIONS,
        "queries": QUERIES,
        "neighbors": NEIGHBORS,
        "page_budget_ladder": list(PAGE_BUDGET_LADDER),
        "page_rows_ladder": list(PAGE_ROWS),
        "replication_factors": list(REPLICATION_FACTORS),
        "row_byte_models": ROW_BYTE_MODELS,
        "gate": {
            "pages": GATE_PAGES,
            "bytes": GATE_BYTES,
            "cost_model": "sq8_plain",
            "aggregate_ppm": GATE_AGGREGATE_PPM,
            "worst_query_ppm": GATE_WORST_PPM,
        },
        "partitions": partition_telemetry,
        "cells": cells,
        "promoted_cells": [summarize(cell) for cell in promoted],
        "best_cell_at_gate_budget": summarize(best),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "next_action": (
            "build-and-measure-query-only-page-router-on-the-promoted-layout"
            if promoted
            else "raise-replication-or-shrink-the-neighbor-set-the-layout-must-contain"
        ),
        "validation_opened": False,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(
        json.dumps(
            {
                "result_sha256": hashlib.sha256(payload).hexdigest(),
                "promoted_cells": result["promoted_cells"],
                "best_cell_at_gate_budget": result["best_cell_at_gate_budget"],
                "next_action": result["next_action"],
            },
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
