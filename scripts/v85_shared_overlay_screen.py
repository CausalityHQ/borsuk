#!/usr/bin/env python3
"""Fail-fast shared-PQ/base-page plus resident-delta quality screen."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from typing import Any

import numpy as np
import pyarrow.parquet as pq

_PAGE_HEADER_BYTES = 64
_V85_PAIRED_PREFIX_QUERIES = 32
_V85_PAIRED_PREFIX_SHA256 = (
    "354ddcb810a13ee8fe981b6904bdfeb21084fe423c79deca8be75f32d21b0aa1"
)


def _fit_grouped_page_posterior(
    groups: list[tuple[np.ndarray, np.ndarray]],
    *,
    ridge: float,
    iterations: int,
) -> dict[str, Any]:
    """Fit one deterministic conditional page-share model."""

    if not groups or not np.isfinite(ridge) or ridge <= 0.0 or iterations <= 0:
        raise ValueError("posterior training groups differ")
    dimensions = np.asarray(groups[0][0]).shape[1]
    prepared = []
    for features, targets in groups:
        features = np.asarray(features, dtype=np.float64)
        targets = np.asarray(targets, dtype=np.float64)
        if (
            features.ndim != 2
            or features.shape[0] < 2
            or features.shape[1] != dimensions
            or targets.shape != (features.shape[0],)
            or not np.all(np.isfinite(features))
            or not np.all(np.isfinite(targets))
            or np.any(targets < 0.0)
            or not np.isclose(float(np.sum(targets)), 1.0, atol=1e-12)
        ):
            raise ValueError("posterior training groups differ")
        prepared.append((features, targets))

    weights = np.zeros(dimensions, dtype=np.float64)
    regularizer = np.eye(dimensions, dtype=np.float64) * ridge
    regularizer[0, 0] = ridge * 1e-6

    def fit_state(
        candidate: np.ndarray,
    ) -> tuple[float, np.ndarray, np.ndarray]:
        objective = float(0.5 * candidate @ regularizer @ candidate)
        gradient = regularizer @ candidate
        hessian = regularizer.copy()
        for features, targets in prepared:
            logits = features @ candidate
            maximum = float(np.max(logits))
            exponentials = np.exp(logits - maximum)
            objective += maximum + float(np.log(np.sum(exponentials)))
            objective -= float(targets @ logits)
            probabilities = exponentials
            probabilities /= float(np.sum(probabilities))
            gradient += features.T @ (probabilities - targets)
            mean = features.T @ probabilities
            hessian += features.T @ (probabilities[:, None] * features)
            hessian -= np.outer(mean, mean)
        return objective, gradient, hessian

    used_iterations = 0
    stop_reason = "iterations"
    for iteration in range(1, iterations + 1):
        used_iterations = iteration
        objective, gradient, hessian = fit_state(weights)
        gradient_max_abs = float(np.max(np.abs(gradient)))
        if gradient_max_abs <= 1e-8:
            stop_reason = "gradient"
            break
        step = np.linalg.solve(hessian, gradient)
        descent = float(gradient @ step)
        scale = 1.0
        accepted = False
        for _ in range(24):
            candidate = weights - scale * step
            candidate_objective, _, _ = fit_state(candidate)
            if candidate_objective <= objective - 1e-4 * scale * descent:
                weights = candidate
                accepted = True
                break
            scale *= 0.5
        if not accepted:
            stop_reason = "line-search"
            break
        if float(np.max(np.abs(scale * step))) <= 1e-10:
            stop_reason = "step"
            break
    objective, gradient, _ = fit_state(weights)
    gradient_max_abs = float(np.max(np.abs(gradient)))
    if not np.all(np.isfinite(weights)):
        raise ValueError("posterior training groups differ")
    converged = gradient_max_abs <= 1e-6
    return {
        "converged": converged,
        "gradient_max_abs": gradient_max_abs,
        "iterations": used_iterations,
        "objective": objective,
        "stop_reason": "gradient" if converged else stop_reason,
        "weights": weights,
    }


def _page_posterior_features(
    query: np.ndarray,
    row_scores: np.ndarray,
    *,
    projection: np.ndarray,
    page_summaries: np.ndarray,
    page_rows: int,
    top_rows: int,
    centroid_candidates: int,
) -> tuple[np.ndarray, np.ndarray]:
    page_count = page_summaries.shape[0]
    take = min(top_rows, row_scores.size)
    head = np.argpartition(row_scores, take - 1)[:take]
    head = head[np.lexsort((head, row_scores[head]))]
    head_pages = head // page_rows
    counts = np.bincount(head_pages, minlength=page_count)
    minimum = np.full(page_count, np.inf, dtype=np.float64)
    np.minimum.at(minimum, head_pages, row_scores[head].astype(np.float64))
    reciprocal = np.zeros(page_count, dtype=np.float64)
    np.add.at(
        reciprocal,
        head_pages,
        np.reciprocal(np.arange(1, take + 1, dtype=np.float64)),
    )

    projected_query = query.astype(np.float32, copy=False) @ projection
    summary_delta = page_summaries - projected_query[None, :]
    summary_distance = np.einsum("ij,ij->i", summary_delta, summary_delta)
    summary_take = min(centroid_candidates, page_count)
    summary_head = np.argpartition(summary_distance, summary_take - 1)[:summary_take]
    candidates = np.union1d(np.unique(head_pages), summary_head)
    if candidates.size < 2:
        raise ValueError("posterior candidates differ")

    row_low = float(row_scores[head[0]])
    row_high = float(row_scores[head[-1]])
    row_scale = max(row_high - row_low, 1e-12)
    row_feature = np.full(candidates.size, -16.0, dtype=np.float64)
    present = np.isfinite(minimum[candidates])
    row_feature[present] = -np.clip(
        (minimum[candidates[present]] - row_low) / row_scale, 0.0, 16.0
    )
    summary_low = float(np.min(summary_distance))
    summary_high = float(np.max(summary_distance[summary_head]))
    summary_scale = max(summary_high - summary_low, 1e-12)
    features = np.column_stack(
        (
            np.ones(candidates.size, dtype=np.float64),
            row_feature,
            np.log1p(counts[candidates].astype(np.float64)),
            np.log1p(reciprocal[candidates] * float(take)),
            -np.clip(
                (summary_distance[candidates] - summary_low) / summary_scale,
                0.0,
                16.0,
            ),
        )
    )
    return candidates.astype(np.int64, copy=False), features


def _build_page_posterior(
    base: np.ndarray,
    base_ids: np.ndarray,
    base_codes: np.ndarray,
    books: list[np.ndarray],
    *,
    page_rows: int,
    training_queries: int,
    training_neighbors: int,
    top_rows: int,
    projection_dimensions: int,
    centroid_candidates: int,
    seed: int,
) -> dict[str, Any]:
    """Fit a base-only page-neighbor-share posterior."""

    if (
        base.ndim != 2
        or base.shape[0] < 3
        or base_ids.shape != (base.shape[0],)
        or base_codes.shape[0] != base.shape[0]
        or not np.all(np.isfinite(base))
        or page_rows <= 0
        or not 0 < training_queries <= base.shape[0]
        or not 0 < training_neighbors < base.shape[0]
        or top_rows <= 0
        or not 0 < projection_dimensions <= base.shape[1]
        or centroid_candidates <= 0
    ):
        raise ValueError("page posterior training differs")
    generator = np.random.default_rng(seed)
    projection = generator.choice(
        np.asarray([-1.0, 1.0], dtype=np.float32),
        size=(base.shape[1], projection_dimensions),
    ) / np.float32(np.sqrt(projection_dimensions))
    page_count = (base.shape[0] + page_rows - 1) // page_rows
    page_centers = np.empty((page_count, base.shape[1]), dtype=np.float32)
    for page in range(page_count):
        start = page * page_rows
        stop = min(start + page_rows, base.shape[0])
        page_centers[page] = np.mean(base[start:stop], axis=0, dtype=np.float32)
    page_summaries = page_centers @ projection
    base_norms = np.einsum("ij,ij->i", base, base)

    identifiers = base_ids.astype(np.uint64, copy=False)
    keys = identifiers ^ np.uint64(seed)
    keys ^= keys >> np.uint64(30)
    keys *= np.uint64(0xBF58476D1CE4E5B9)
    keys ^= keys >> np.uint64(27)
    keys *= np.uint64(0x94D049BB133111EB)
    keys ^= keys >> np.uint64(31)
    training_indices = np.lexsort((base_ids, keys))[:training_queries]
    groups = []
    covered_truth = 0
    for row in training_indices:
        query = base[row]
        exact_distance = (
            base_norms
            - np.float32(2.0) * (base @ query)
            + np.float32(np.dot(query, query))
        )
        exact_distance[row] = np.inf
        truth = np.argpartition(exact_distance, training_neighbors - 1)[
            :training_neighbors
        ]
        truth_pages, truth_counts = np.unique(
            truth // page_rows, return_counts=True
        )
        row_scores = _adc_scores(query, base_codes, books)
        row_scores[row] = np.inf
        candidates, features = _page_posterior_features(
            query,
            row_scores,
            projection=projection,
            page_summaries=page_summaries,
            page_rows=page_rows,
            top_rows=top_rows,
            centroid_candidates=centroid_candidates,
        )
        target_by_page = {
            int(truth_pages[index]): int(truth_counts[index])
            for index in range(truth_pages.size)
        }
        targets = np.asarray(
            [target_by_page.get(int(page), 0) for page in candidates],
            dtype=np.float64,
        )
        covered = int(np.sum(targets))
        covered_truth += covered
        if covered == 0:
            continue
        targets /= float(covered)
        groups.append((features, targets))
    fit = _fit_grouped_page_posterior(groups, ridge=0.05, iterations=24)
    weights = np.asarray(fit["weights"], dtype=np.float64)
    artifact = {
        "centroid_candidates": centroid_candidates,
        "fit": {
            "converged": bool(fit["converged"]),
            "gradient_max_abs": float(fit["gradient_max_abs"]),
            "iterations": int(fit["iterations"]),
            "objective": float(fit["objective"]),
            "stop_reason": str(fit["stop_reason"]),
        },
        "page_rows": page_rows,
        "page_summaries": page_summaries,
        "projection": projection,
        "top_rows": top_rows,
        "training_neighbors": training_neighbors,
        "training_candidate_recall_ppm": round(
            covered_truth
            * 1_000_000
            / (training_queries * training_neighbors)
        ),
        "training_queries": training_queries,
        "weights": weights,
    }
    artifact["artifact_bytes"] = int(
        projection.nbytes + page_summaries.nbytes + weights.nbytes
    )
    return artifact


def _score_page_posterior(
    query: np.ndarray,
    row_scores: np.ndarray,
    artifact: dict[str, Any],
) -> np.ndarray:
    page_summaries = np.asarray(artifact["page_summaries"], dtype=np.float32)
    candidates, features = _page_posterior_features(
        np.asarray(query, dtype=np.float32),
        np.asarray(row_scores, dtype=np.float32),
        projection=np.asarray(artifact["projection"], dtype=np.float32),
        page_summaries=page_summaries,
        page_rows=int(artifact["page_rows"]),
        top_rows=int(artifact["top_rows"]),
        centroid_candidates=int(artifact["centroid_candidates"]),
    )
    logits = features @ np.asarray(artifact["weights"], dtype=np.float64)
    probabilities = np.exp(logits - float(np.max(logits)))
    probabilities /= float(np.sum(probabilities))
    scores = np.zeros(page_summaries.shape[0], dtype=np.float64)
    scores[candidates] = probabilities
    return scores


def _lloyd(
    data: np.ndarray, clusters: int, seed: int, iterations: int
) -> np.ndarray:
    count = min(clusters, data.shape[0])
    generator = np.random.default_rng(seed)
    centers = data[generator.choice(data.shape[0], count, replace=False)].copy()
    for _ in range(iterations):
        center_norms = np.einsum("ij,ij->i", centers, centers)
        assignment = np.argmin(
            center_norms[None, :] - np.float32(2.0) * (data @ centers.T), axis=1
        )
        populations = np.bincount(assignment, minlength=count)
        order = np.argsort(assignment, kind="stable")
        starts = np.concatenate(([0], np.cumsum(populations)[:-1]))
        occupied = populations > 0
        centers[occupied] = (
            np.add.reduceat(data[order], starts[occupied], axis=0)
            / populations[occupied, None]
        )
    return centers.astype(np.float32, copy=False)


def _train_pq(
    base: np.ndarray,
    subspaces: int,
    clusters: int,
    seed: int,
    sample_rows: int,
    iterations: int,
) -> list[np.ndarray]:
    generator = np.random.default_rng(seed)
    sample_count = min(base.shape[0], sample_rows)
    sample = base[
        generator.choice(base.shape[0], sample_count, replace=False)
    ]
    width = base.shape[1] // subspaces
    return [
        _lloyd(
            np.ascontiguousarray(sample[:, index * width : (index + 1) * width]),
            clusters,
            seed + index,
            iterations,
        )
        for index in range(subspaces)
    ]


def _encode_pq(
    vectors: np.ndarray, books: list[np.ndarray], chunk_rows: int
) -> np.ndarray:
    subspaces = len(books)
    width = vectors.shape[1] // subspaces
    codes = np.empty((vectors.shape[0], subspaces), dtype=np.uint8)
    for index, book in enumerate(books):
        norms = np.einsum("ij,ij->i", book, book)
        for start in range(0, vectors.shape[0], chunk_rows):
            stop = min(start + chunk_rows, vectors.shape[0])
            block = vectors[start:stop, index * width : (index + 1) * width]
            codes[start:stop, index] = np.argmin(
                norms[None, :] - np.float32(2.0) * (block @ book.T), axis=1
            )
    return codes


def _adc_scores(query: np.ndarray, codes: np.ndarray, books: list[np.ndarray]) -> np.ndarray:
    width = query.size // len(books)
    scores = np.zeros(codes.shape[0], dtype=np.float32)
    for index, book in enumerate(books):
        delta = book - query[index * width : (index + 1) * width]
        table = np.einsum("ij,ij->i", delta, delta)
        scores += table[codes[:, index]]
    return scores


def _sq8(vectors: np.ndarray) -> np.ndarray:
    low = np.min(vectors, axis=0).astype(np.float32)
    high = np.max(vectors, axis=0).astype(np.float32)
    step = ((high - low) / np.float32(255.0)).astype(np.float32)
    step[step == 0.0] = np.float32(1.0)
    code = np.clip(
        np.rint((vectors - low[None, :]) / step[None, :]), 0, 255
    ).astype(np.uint8)
    return low[None, :] + code.astype(np.float32) * step[None, :]


def _page_sq8(vectors: np.ndarray, page_rows: int) -> np.ndarray:
    decoded = np.empty_like(vectors)
    for start in range(0, vectors.shape[0], page_rows):
        stop = min(start + page_rows, vectors.shape[0])
        decoded[start:stop] = _sq8(vectors[start:stop])
    return decoded


def _coalesce(pages: np.ndarray, gap: int = 2) -> list[tuple[int, int]]:
    if pages.size == 0:
        return []
    breaks = np.flatnonzero(np.diff(pages) > gap + 1)
    starts = np.concatenate(([pages[0]], pages[breaks + 1]))
    ends = np.concatenate((pages[breaks], [pages[-1]]))
    return [(int(starts[index]), int(ends[index])) for index in range(starts.size)]


def _page_payload_bytes(*, dimensions: int, page_rows: int) -> int:
    if dimensions <= 0 or page_rows <= 0:
        raise ValueError("page shape differs")
    quantizer_bytes = 2 * dimensions * np.dtype(np.float32).itemsize
    record_bytes = 12 + dimensions
    return _PAGE_HEADER_BYTES + quantizer_bytes + page_rows * record_bytes


def _maximum_physical_oracle_hits(
    page_hits: dict[int, int],
    *,
    page_count: int,
    max_pages: int,
    max_ranges: int,
) -> int:
    """Return exact maximum hit weight under contiguous physical-read budgets."""

    if page_count <= 0 or max_pages <= 0 or max_ranges <= 0:
        raise ValueError("physical oracle budget differs")
    if any(
        not 0 <= page < page_count or hits <= 0
        for page, hits in page_hits.items()
    ):
        raise ValueError("physical oracle pages differ")
    if not page_hits:
        return 0

    unreachable = np.int32(-1_000_000_000)
    shape = (max_ranges + 1, max_pages + 1)
    off = np.full(shape, unreachable, dtype=np.int32)
    on = np.full(shape, unreachable, dtype=np.int32)
    off[0, 0] = 0
    previous = -1
    for page, hits in sorted(page_hits.items()):
        skipped = np.maximum(off, on)
        selected = np.full(shape, unreachable, dtype=np.int32)

        bridge_cost = page - previous
        if bridge_cost <= max_pages:
            selected[:, bridge_cost:] = np.maximum(
                selected[:, bridge_cost:], on[:, : max_pages + 1 - bridge_cost]
            )
        selected[1:, 1:] = np.maximum(selected[1:, 1:], skipped[:-1, :-1])
        selected[selected != unreachable] += np.int32(hits)

        off = skipped
        on = selected
        previous = page

    return int(max(np.max(off), np.max(on)))


def _candidate_physical_oracle_hits(
    candidate_pages: np.ndarray,
    truth_base_positions: np.ndarray,
    *,
    delta_hits: int,
    page_rows: int,
    page_count: int,
    max_pages: int,
    max_ranges: int,
) -> int:
    """Return attainable truth hits after candidate generation."""

    candidate_pages = np.asarray(candidate_pages, dtype=np.int64)
    truth_base_positions = np.asarray(truth_base_positions, dtype=np.int64)
    if (
        candidate_pages.ndim != 1
        or truth_base_positions.ndim != 1
        or delta_hits < 0
        or page_rows <= 0
        or np.any(candidate_pages < 0)
        or np.any(candidate_pages >= page_count)
        or np.any(truth_base_positions < 0)
        or np.any(truth_base_positions >= page_count * page_rows)
    ):
        raise ValueError("posterior candidate oracle differs")
    truth_pages, truth_counts = np.unique(
        truth_base_positions // page_rows, return_counts=True
    )
    permitted = np.isin(truth_pages, candidate_pages)
    page_hits = {
        int(truth_pages[index]): int(truth_counts[index])
        for index in range(truth_pages.size)
        if permitted[index]
    }
    return delta_hits + _maximum_physical_oracle_hits(
        page_hits,
        page_count=page_count,
        max_pages=max_pages,
        max_ranges=max_ranges,
    )


def _select_optimal_weighted_pages(
    page_weights: np.ndarray,
    *,
    max_span_pages: int,
    max_ranges: int,
) -> tuple[np.ndarray, list[tuple[int, int]]]:
    """Select the exact maximum-weight dense page set for the screen."""

    page_weights = np.asarray(page_weights)
    if (
        page_weights.ndim != 1
        or page_weights.size == 0
        or max_span_pages <= 0
        or max_ranges <= 0
        or not np.all(np.isfinite(page_weights))
        or np.any(page_weights < 0)
    ):
        raise ValueError("weighted page planner input differs")
    weights = page_weights.astype(np.float64, copy=False)
    unreachable = np.float64(-np.inf)
    shape = (max_ranges + 1, max_span_pages + 1)
    off = np.full(shape, unreachable, dtype=np.float64)
    on = np.full(shape, unreachable, dtype=np.float64)
    off[0, 0] = 0
    off_choices = np.zeros((weights.size, *shape), dtype=np.uint8)
    on_choices = np.zeros((weights.size, *shape), dtype=np.uint8)

    for page, weight in enumerate(weights):
        next_off = np.maximum(off, on)
        off_choices[page] = on > off

        next_on = np.full(shape, unreachable, dtype=np.float64)
        continuing = on[:, :-1]
        next_on[:, 1:] = continuing
        on_choices[page, :, 1:][continuing != unreachable] = 1

        starting = off[:-1, :-1]
        replace = starting > next_on[1:, 1:]
        next_on[1:, 1:][replace] = starting[replace]
        on_choices[page, 1:, 1:][replace] = 2
        reachable = next_on != unreachable
        next_on[reachable] += weight

        off = next_off
        on = next_on

    best_value = unreachable
    best = (0, 0, 0)
    for pages in range(max_span_pages + 1):
        for ranges in range(max_ranges + 1):
            for state, values in enumerate((off, on)):
                value = values[ranges, pages]
                if value > best_value:
                    best_value = value
                    best = (ranges, pages, state)

    ranges, pages, state = best
    selected = np.zeros(weights.size, dtype=np.bool_)
    for page in range(weights.size - 1, -1, -1):
        if state == 0:
            state = int(off_choices[page, ranges, pages])
            continue
        selected[page] = True
        choice = int(on_choices[page, ranges, pages])
        pages -= 1
        if choice == 2:
            ranges -= 1
            state = 0
        elif choice == 1:
            state = 1
        else:
            raise AssertionError("weighted page planner backtrack differs")

    selected_pages = np.flatnonzero(selected)
    return selected_pages, _coalesce(selected_pages, gap=0)


def _stable_landmark_positions(
    base_ids: np.ndarray, landmark_count: int, seed: int
) -> np.ndarray:
    values = base_ids.astype(np.uint64, copy=True)
    values += np.uint64(seed) + np.uint64(0x9E3779B97F4A7C15)
    values = (values ^ (values >> np.uint64(30))) * np.uint64(
        0xBF58476D1CE4E5B9
    )
    values = (values ^ (values >> np.uint64(27))) * np.uint64(
        0x94D049BB133111EB
    )
    values ^= values >> np.uint64(31)
    order = np.lexsort((base_ids, values))
    return np.sort(order[:landmark_count])


def _build_landmark_incidence(
    base: np.ndarray,
    base_ids: np.ndarray,
    *,
    page_rows: int,
    landmark_count: int,
    neighbor_count: int,
    pages_per_landmark: int,
    seed: int,
    chunk_rows: int,
) -> dict[str, np.ndarray | int]:
    """Build query-independent exact-neighborhood landmark page incidence."""

    base = np.ascontiguousarray(base, dtype=np.float32)
    base_ids = np.asarray(base_ids, dtype=np.int64)
    if (
        base.ndim != 2
        or base.shape[0] < 2
        or base_ids.shape != (base.shape[0],)
        or np.unique(base_ids).size != base_ids.size
        or not np.isfinite(base).all()
        or page_rows <= 0
        or not 0 < landmark_count <= base.shape[0]
        or not 0 < neighbor_count < base.shape[0]
        or pages_per_landmark <= 0
        or chunk_rows <= 0
    ):
        raise ValueError("landmark incidence authority differs")

    landmark_positions = _stable_landmark_positions(base_ids, landmark_count, seed)
    landmarks = np.ascontiguousarray(base[landmark_positions])
    keep = neighbor_count + 1
    best_distances = np.full((landmark_count, keep), np.inf, dtype=np.float32)
    best_positions = np.full((landmark_count, keep), -1, dtype=np.int64)
    base_norms = np.einsum("ij,ij->i", base, base)
    landmark_norms = np.einsum("ij,ij->i", landmarks, landmarks)
    landmark_t = landmarks.T
    for start in range(0, base.shape[0], chunk_rows):
        stop = min(start + chunk_rows, base.shape[0])
        distances = (
            base_norms[start:stop, None]
            + landmark_norms[None, :]
            - np.float32(2.0) * (base[start:stop] @ landmark_t)
        )
        np.maximum(distances, np.float32(0.0), out=distances)
        distances = distances.T
        local_keep = min(keep, stop - start)
        local_columns = np.argpartition(distances, local_keep - 1, axis=1)[
            :, :local_keep
        ]
        local_distances = np.take_along_axis(distances, local_columns, axis=1)
        local_positions = local_columns.astype(np.int64) + start
        merged_distances = np.concatenate((best_distances, local_distances), axis=1)
        merged_positions = np.concatenate((best_positions, local_positions), axis=1)
        chosen = np.argpartition(merged_distances, keep - 1, axis=1)[:, :keep]
        best_distances = np.take_along_axis(merged_distances, chosen, axis=1)
        best_positions = np.take_along_axis(merged_positions, chosen, axis=1)

    page_ordinals = np.full(
        (landmark_count, pages_per_landmark), -1, dtype=np.int64
    )
    incidence_weights = np.zeros(
        (landmark_count, pages_per_landmark), dtype=np.float32
    )
    discarded_mass = np.empty(landmark_count, dtype=np.float32)
    for landmark in range(landmark_count):
        positions = best_positions[landmark]
        distances = best_distances[landmark]
        eligible = (positions >= 0) & (positions != landmark_positions[landmark])
        positions = positions[eligible]
        distances = distances[eligible]
        order = np.lexsort((base_ids[positions], distances))[:neighbor_count]
        positions = positions[order]
        distances = distances[order]
        if positions.size != neighbor_count:
            raise AssertionError("landmark neighborhood differs")
        tau_index = min(99, neighbor_count - 1)
        tau = max(float(distances[tau_index] - distances[0]), 1.0e-12)
        weights = np.exp(-(distances - distances[0]) / tau).astype(np.float64)
        pages, inverse = np.unique(positions // page_rows, return_inverse=True)
        page_weights = np.zeros(pages.size, dtype=np.float64)
        np.add.at(page_weights, inverse, weights)
        total = float(np.sum(page_weights))
        page_order = np.lexsort((pages, -page_weights))[:pages_per_landmark]
        retained = page_order.size
        page_ordinals[landmark, :retained] = pages[page_order]
        incidence_weights[landmark, :retained] = (
            page_weights[page_order] / total
        ).astype(np.float32)
        discarded_mass[landmark] = np.float32(
            1.0 - float(np.sum(page_weights[page_order])) / total
        )

    return {
        "discarded_mass": discarded_mass,
        "incidence_weights": incidence_weights,
        "landmark_ids": base_ids[landmark_positions].copy(),
        "landmarks": landmarks,
        "page_count": (base.shape[0] + page_rows - 1) // page_rows,
        "page_ordinals": page_ordinals,
    }


def _landmark_page_scores(
    query: np.ndarray,
    artifact: dict[str, np.ndarray | int],
    *,
    query_landmarks: int,
) -> np.ndarray:
    landmarks = np.asarray(artifact["landmarks"], dtype=np.float32)
    landmark_ids = np.asarray(artifact["landmark_ids"], dtype=np.int64)
    page_ordinals = np.asarray(artifact["page_ordinals"], dtype=np.int64)
    incidence_weights = np.asarray(artifact["incidence_weights"], dtype=np.float32)
    query = np.asarray(query, dtype=np.float32)
    page_count = int(artifact["page_count"])
    if (
        query.shape != (landmarks.shape[1],)
        or not np.isfinite(query).all()
        or not 0 < query_landmarks <= landmarks.shape[0]
    ):
        raise ValueError("landmark query differs")

    delta = landmarks - query[None, :]
    distances = np.einsum("ij,ij->i", delta, delta)
    order = np.lexsort((landmark_ids, distances))[:query_landmarks]
    selected_distances = distances[order]
    tau = max(float(selected_distances[-1] - selected_distances[0]), 1.0e-12)
    alpha = np.exp(-(selected_distances - selected_distances[0]) / tau)
    alpha /= np.sum(alpha)
    scores = np.zeros(page_count, dtype=np.float64)
    for query_rank, landmark in enumerate(order):
        pages = page_ordinals[landmark]
        present = pages >= 0
        np.add.at(
            scores,
            pages[present],
            float(alpha[query_rank]) * incidence_weights[landmark, present],
        )
    return scores


def _select_rank_weighted_pages(
    scores: np.ndarray,
    *,
    page_rows: int,
    top_rows: int,
    max_span_pages: int,
    max_ranges: int,
    gap: int,
    planner: str = "greedy",
) -> tuple[np.ndarray, list[tuple[int, int]]]:
    take = min(top_rows, scores.size)
    head = np.argpartition(scores, take - 1)[:take]
    cutoff = np.max(scores[head])
    below = np.flatnonzero(scores < cutoff)
    remaining = take - below.size
    equal = np.flatnonzero(scores == cutoff)[:remaining]
    head = np.concatenate((below, equal))

    head = head[np.lexsort((head, scores[head]))]
    pages = head // page_rows
    page_count = (scores.size + page_rows - 1) // page_rows
    weights = np.asarray(
        [1_000_000_000 // (rank + 1) for rank in range(head.size)],
        dtype=np.uint64,
    )
    page_weights = np.zeros(page_count, dtype=np.uint64)
    np.add.at(page_weights, pages, weights)
    minimum = np.full(page_count, np.inf, dtype=np.float32)
    np.minimum.at(minimum, pages, scores[head])
    if planner == "exact":
        return _select_optimal_weighted_pages(
            page_weights,
            max_span_pages=max_span_pages,
            max_ranges=max_ranges,
        )
    if planner != "greedy":
        raise ValueError("rank-weighted planner differs")
    candidates = np.flatnonzero(page_weights)
    candidates = candidates[
        np.lexsort(
            (
                candidates,
                minimum[candidates],
                -page_weights[candidates].astype(np.int64),
            )
        )
    ]

    selected = np.empty(0, dtype=np.int64)
    ranges: list[tuple[int, int]] = []
    for page in candidates:
        proposed = np.sort(np.append(selected, page))
        proposed_ranges = _coalesce(proposed, gap)
        span_pages = sum(end - start + 1 for start, end in proposed_ranges)
        if len(proposed_ranges) <= max_ranges and span_pages <= max_span_pages:
            selected = proposed
            ranges = proposed_ranges
    return selected, ranges


def _top_ids(
    query: np.ndarray, vectors: np.ndarray, ids: np.ndarray, neighbors: int
) -> list[int]:
    delta = vectors - query[None, :]
    scores = np.einsum("ij,ij->i", delta, delta)
    ordered = np.lexsort((ids, scores))[:neighbors]
    return [int(ids[index]) for index in ordered]


def _nearest_percentile(values: list[int], quantile: float) -> int:
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * quantile)]


def _paired_bootstrap_recall_interval(
    differences: np.ndarray,
    *,
    neighbors: int,
    seed: int,
    repetitions: int,
) -> list[int]:
    differences = np.asarray(differences, dtype=np.int64)
    if (
        differences.ndim != 1
        or differences.size == 0
        or neighbors <= 0
        or repetitions <= 0
    ):
        raise ValueError("paired bootstrap differs")
    generator = np.random.default_rng(seed)
    means = np.empty(repetitions, dtype=np.float64)
    chunk_repetitions = 2_048
    for start in range(0, repetitions, chunk_repetitions):
        stop = min(start + chunk_repetitions, repetitions)
        sample_indices = generator.integers(
            0,
            differences.size,
            size=(stop - start, differences.size),
        )
        means[start:stop] = np.mean(differences[sample_indices], axis=1)
    lower, upper = np.quantile(means, [0.025, 0.975], method="nearest")
    return [
        round(float(lower) * 1_000_000 / neighbors),
        round(float(upper) * 1_000_000 / neighbors),
    ]


def _paired_rescore_evidence(
    rank_weighted_cells: list[dict[str, Any]],
    page_posterior_cells: list[dict[str, Any]],
    *,
    neighbors: int,
    historical_prefix_queries: int,
    historical_prefix_sha256: str,
    bootstrap_seed: int,
    bootstrap_repetitions: int,
) -> dict[str, Any]:
    """Bind and compare the three frozen V85 arms query by query."""

    def rank_cell(top_rows: int) -> dict[str, Any]:
        matches = [
            cell
            for cell in rank_weighted_cells
            if cell.get("planner") == "exact"
            and cell.get("page_cap") == 81
            and cell.get("top_rows") == top_rows
        ]
        if len(matches) != 1:
            raise ValueError("paired rescore arms differ")
        return matches[0]

    if (
        neighbors <= 0
        or historical_prefix_queries <= 0
        or len(historical_prefix_sha256) != 64
        or any(character not in "0123456789abcdef" for character in historical_prefix_sha256)
        or bootstrap_repetitions <= 0
        or len(page_posterior_cells) != 1
        or page_posterior_cells[0].get("top_rows") != 2_048
    ):
        raise ValueError("paired rescore authority differs")
    rank_1024 = rank_cell(1_024)
    rank_2048 = rank_cell(2_048)
    posterior = page_posterior_cells[0]
    arm_samples = {
        "rank_1024": rank_1024.get("samples"),
        "rank_2048": rank_2048.get("samples"),
        "posterior": posterior.get("samples"),
    }
    if not all(isinstance(samples, list) and samples for samples in arm_samples.values()):
        raise ValueError("paired rescore samples differ")
    query_count = len(arm_samples["rank_1024"])
    if (
        historical_prefix_queries > query_count
        or any(len(samples) != query_count for samples in arm_samples.values())
    ):
        raise ValueError("paired rescore samples differ")

    per_query = []
    for query in range(query_count):
        samples = {name: values[query] for name, values in arm_samples.items()}
        if any(sample.get("query") != query for sample in samples.values()):
            raise ValueError("paired rescore query order differs")
        hits = {}
        for name, sample in samples.items():
            for metric in ("exact_hits", "page_sq8_hits"):
                value = sample.get(metric)
                if (
                    not isinstance(value, int)
                    or isinstance(value, bool)
                    or not 0 <= value <= neighbors
                ):
                    raise ValueError("paired rescore hits differ")
                hits[f"{name}_{metric}"] = value
        per_query.append(
            {
                "posterior_exact_hits": hits["posterior_exact_hits"],
                "posterior_page_sq8_hits": hits["posterior_page_sq8_hits"],
                "query": query,
                "rank_1024_exact_hits": hits["rank_1024_exact_hits"],
                "rank_1024_page_sq8_hits": hits["rank_1024_page_sq8_hits"],
                "rank_2048_exact_hits": hits["rank_2048_exact_hits"],
                "rank_2048_page_sq8_hits": hits["rank_2048_page_sq8_hits"],
            }
        )

    prefix_payload = {
        "page_posterior": arm_samples["posterior"][:historical_prefix_queries],
        "rank_exact_top_1024": arm_samples["rank_1024"][:historical_prefix_queries],
        "rank_exact_top_2048": arm_samples["rank_2048"][:historical_prefix_queries],
    }
    prefix_bytes = json.dumps(
        prefix_payload, separators=(",", ":"), sort_keys=True
    ).encode()
    observed_prefix_sha256 = hashlib.sha256(prefix_bytes).hexdigest()
    if observed_prefix_sha256 != historical_prefix_sha256:
        raise ValueError("paired rescore historical prefix differs")

    def arm_summary(prefix: str) -> dict[str, int]:
        exact_hits = sum(sample[f"{prefix}_exact_hits"] for sample in per_query)
        page_hits = sum(
            sample[f"{prefix}_page_sq8_hits"] for sample in per_query
        )
        denominator = query_count * neighbors
        return {
            "exact_hits": exact_hits,
            "exact_recall_ppm": round(exact_hits * 1_000_000 / denominator),
            "page_sq8_hits": page_hits,
            "page_sq8_recall_ppm": round(page_hits * 1_000_000 / denominator),
        }

    def comparison(reference: str) -> dict[str, Any]:
        compared = {}
        for metric in ("page_sq8", "exact"):
            differences = np.asarray(
                [
                    sample[f"posterior_{metric}_hits"]
                    - sample[f"{reference}_{metric}_hits"]
                    for sample in per_query
                ],
                dtype=np.int64,
            )
            difference_hits = int(np.sum(differences))
            compared[metric] = {
                "difference_hits": difference_hits,
                "difference_recall_ppm": round(
                    difference_hits * 1_000_000 / (query_count * neighbors)
                ),
                "paired_bootstrap_95_recall_ppm": (
                    _paired_bootstrap_recall_interval(
                        differences,
                        neighbors=neighbors,
                        seed=bootstrap_seed,
                        repetitions=bootstrap_repetitions,
                    )
                ),
            }
        return compared

    return {
        "arms": {
            "posterior_2048": arm_summary("posterior"),
            "rank_exact_1024": arm_summary("rank_1024"),
            "rank_exact_2048": arm_summary("rank_2048"),
        },
        "bootstrap": {
            "method": "paired-query-percentile",
            "repetitions": bootstrap_repetitions,
            "seed": bootstrap_seed,
        },
        "claim_eligible": False,
        "comparisons": {
            "posterior_minus_rank_1024": comparison("rank_1024"),
            "posterior_minus_rank_2048": comparison("rank_2048"),
        },
        "historical_prefix": {
            "query_count": historical_prefix_queries,
            "sha256": observed_prefix_sha256,
            "verified": True,
        },
        "per_query": per_query,
        "query_count": query_count,
        "schema": "borsuk-v85-paired-rescore-v1",
    }


def evaluate_physical_oracle(
    *,
    base_ids: np.ndarray,
    delta_ids: np.ndarray,
    truth_ids: np.ndarray,
    dimensions: int,
    page_rows: int = 256,
    max_base_bytes: int = 16 * 1024 * 1024,
    max_base_gets: int = 32,
) -> dict[str, Any]:
    """Compute exact attainable GT coverage under physical page/range budgets."""

    base_ids = np.asarray(base_ids, dtype=np.int64)
    delta_ids = np.asarray(delta_ids, dtype=np.int64)
    truth_ids = np.asarray(truth_ids, dtype=np.int64)
    if (
        base_ids.ndim != 1
        or delta_ids.ndim != 1
        or truth_ids.ndim != 2
        or base_ids.size == 0
        or delta_ids.size == 0
        or truth_ids.shape[0] == 0
        or truth_ids.shape[1] == 0
        or dimensions <= 0
        or page_rows <= 0
        or max_base_bytes <= 0
        or max_base_gets <= 0
        or np.unique(np.concatenate((base_ids, delta_ids))).size
        != base_ids.size + delta_ids.size
    ):
        raise ValueError("physical oracle authority differs")

    page_payload_bytes = _page_payload_bytes(
        dimensions=dimensions, page_rows=page_rows
    )
    max_base_pages = max_base_bytes // page_payload_bytes
    if max_base_pages == 0:
        raise ValueError("physical oracle budget differs")

    base_order = np.argsort(base_ids, kind="stable")
    sorted_base_ids = base_ids[base_order]
    delta_order = np.argsort(delta_ids, kind="stable")
    sorted_delta_ids = delta_ids[delta_order]
    oracle_hits = 0
    base_truth_hits = 0
    delta_truth_hits = 0
    query_recall = []
    page_count = (base_ids.size + page_rows - 1) // page_rows
    truth_width = truth_ids.shape[1]
    for expected_ids in truth_ids:
        base_offsets = np.searchsorted(sorted_base_ids, expected_ids)
        base_present = base_offsets < sorted_base_ids.size
        base_present[base_present] &= (
            sorted_base_ids[base_offsets[base_present]] == expected_ids[base_present]
        )
        delta_offsets = np.searchsorted(sorted_delta_ids, expected_ids)
        delta_present = delta_offsets < sorted_delta_ids.size
        delta_present[delta_present] &= (
            sorted_delta_ids[delta_offsets[delta_present]]
            == expected_ids[delta_present]
        )
        if np.any(base_present == delta_present):
            raise ValueError("physical oracle truth identifiers differ")

        base_positions = base_order[base_offsets[base_present]]
        pages, counts = np.unique(base_positions // page_rows, return_counts=True)
        page_hits = {
            int(pages[index]): int(counts[index]) for index in range(pages.size)
        }
        resident_hits = int(np.count_nonzero(delta_present))
        query_hits = resident_hits + _maximum_physical_oracle_hits(
            page_hits,
            page_count=page_count,
            max_pages=max_base_pages,
            max_ranges=max_base_gets,
        )
        oracle_hits += query_hits
        base_truth_hits += int(np.count_nonzero(base_present))
        delta_truth_hits += resident_hits
        query_recall.append(round(query_hits * 1_000_000 / truth_width))

    denominator = truth_ids.shape[0] * truth_width
    recall_ppm = round(oracle_hits * 1_000_000 / denominator)
    return {
        "claim_eligible": False,
        "page_payload_bytes": page_payload_bytes,
        "physical_oracle": {
            "base_truth_hits": base_truth_hits,
            "delta_truth_hits": delta_truth_hits,
            "hits": oracle_hits,
            "max_base_bytes": max_base_bytes,
            "max_base_gets": max_base_gets,
            "max_base_pages": max_base_pages,
            "min_recall_ppm": 995_000,
            "passed": recall_ppm >= 995_000,
            "recall_ppm": recall_ppm,
            "worst_query_recall_ppm": min(query_recall),
        },
        "schema": "borsuk-v85-physical-oracle-v1",
    }


def evaluate_exact_row_control(
    base: np.ndarray,
    delta: np.ndarray,
    queries: np.ndarray,
    *,
    base_ids: np.ndarray,
    delta_ids: np.ndarray,
    truth_ids: np.ndarray,
    page_rows: int = 256,
    neighbors: int = 100,
    top_rows: int = 2_048,
    page_cap: int = 81,
    max_base_bytes: int = 16 * 1024 * 1024,
    max_base_gets: int = 32,
) -> dict[str, Any]:
    """Measure the page aggregator with exact row distances and no PQ work."""

    base = np.asarray(base, dtype=np.float32)
    delta = np.asarray(delta, dtype=np.float32)
    queries = np.asarray(queries, dtype=np.float32)
    base_ids = np.asarray(base_ids, dtype=np.int64)
    delta_ids = np.asarray(delta_ids, dtype=np.int64)
    truth_ids = np.asarray(truth_ids, dtype=np.int64)
    if (
        base.ndim != 2
        or delta.ndim != 2
        or queries.ndim != 2
        or base.shape[0] == 0
        or delta.shape[0] == 0
        or queries.shape[0] == 0
        or base.shape[1] != delta.shape[1]
        or base.shape[1] != queries.shape[1]
        or base_ids.shape != (base.shape[0],)
        or delta_ids.shape != (delta.shape[0],)
        or truth_ids.shape != (queries.shape[0], neighbors)
        or np.unique(np.concatenate((base_ids, delta_ids))).size
        != base.shape[0] + delta.shape[0]
        or page_rows <= 0
        or neighbors <= 0
        or top_rows <= 0
        or page_cap <= 0
        or max_base_bytes <= 0
        or max_base_gets <= 0
        or not all(np.isfinite(value).all() for value in (base, delta, queries))
    ):
        raise ValueError("exact row control authority differs")

    base = np.ascontiguousarray(base)
    delta = np.ascontiguousarray(delta)
    queries = np.ascontiguousarray(queries)
    base_page_sq8 = _page_sq8(base, page_rows)
    delta_sq8 = _sq8(delta)
    oracle_result = evaluate_physical_oracle(
        base_ids=base_ids,
        delta_ids=delta_ids,
        truth_ids=truth_ids,
        dimensions=base.shape[1],
        page_rows=page_rows,
        max_base_bytes=max_base_bytes,
        max_base_gets=max_base_gets,
    )
    page_payload_bytes = oracle_result["page_payload_bytes"]
    max_base_pages = oracle_result["physical_oracle"]["max_base_pages"]
    base_norms = np.einsum("ij,ij->i", base, base)
    samples = []
    exact_hits = 0
    page_sq8_hits = 0
    for query_index, query in enumerate(queries):
        exact_row_scores = (
            base_norms
            - np.float32(2.0) * (base @ query)
            + np.float32(np.dot(query, query))
        )
        _, ranges = _select_rank_weighted_pages(
            exact_row_scores,
            page_rows=page_rows,
            top_rows=top_rows,
            max_span_pages=min(page_cap, max_base_pages),
            max_ranges=max_base_gets,
            gap=2,
            planner="exact",
        )
        base_candidates = np.concatenate(
            [
                np.arange(
                    start * page_rows,
                    min((end + 1) * page_rows, base.shape[0]),
                    dtype=np.int64,
                )
                for start, end in ranges
            ]
        )
        candidate_ids = np.concatenate((base_ids[base_candidates], delta_ids))
        exact_vectors = np.concatenate((base[base_candidates], delta), axis=0)
        page_sq8_vectors = np.concatenate(
            (base_page_sq8[base_candidates], delta_sq8), axis=0
        )
        exact_result = _top_ids(query, exact_vectors, candidate_ids, neighbors)
        page_sq8_result = _top_ids(
            query, page_sq8_vectors, candidate_ids, neighbors
        )
        expected = set(int(value) for value in truth_ids[query_index])
        query_exact_hits = len(expected.intersection(exact_result))
        query_page_sq8_hits = len(expected.intersection(page_sq8_result))
        exact_hits += query_exact_hits
        page_sq8_hits += query_page_sq8_hits
        samples.append(
            {
                "base_bytes": int(
                    sum(end - start + 1 for start, end in ranges)
                    * page_payload_bytes
                ),
                "base_gets": len(ranges),
                "exact_hits": query_exact_hits,
                "page_sq8_hits": query_page_sq8_hits,
                "query": query_index,
            }
        )
    denominator = queries.shape[0] * neighbors
    base_bytes = [sample["base_bytes"] for sample in samples]
    base_gets = [sample["base_gets"] for sample in samples]
    cell = {
        "base_bytes_max": max(base_bytes),
        "base_bytes_p50": _nearest_percentile(base_bytes, 0.50),
        "base_bytes_p95": _nearest_percentile(base_bytes, 0.95),
        "base_gets_max": max(base_gets),
        "base_gets_p50": _nearest_percentile(base_gets, 0.50),
        "base_gets_p95": _nearest_percentile(base_gets, 0.95),
        "exact_recall_ppm": round(exact_hits * 1_000_000 / denominator),
        "page_cap": page_cap,
        "page_sq8_recall_ppm": round(page_sq8_hits * 1_000_000 / denominator),
        "planner": "exact",
        "samples": samples,
        "top_rows": top_rows,
    }
    return {
        "claim_eligible": False,
        "cpu_parallelism": "sequential-query-loop;blas-thread-count-external",
        "exact_row_control_cells": [cell],
        "physical_oracle": oracle_result["physical_oracle"],
        "schema": "borsuk-v85-exact-row-control-v1",
    }


def evaluate_overlay(
    base: np.ndarray,
    delta: np.ndarray,
    queries: np.ndarray,
    *,
    page_rows: int = 256,
    neighbors: int = 100,
    subspaces: int = 64,
    clusters: int = 256,
    shortlists: tuple[int, ...] = (256, 512, 1024),
    rank_top_rows: tuple[int, ...] = (512, 1024, 2048),
    rank_page_caps: tuple[int, ...] = (64, 81),
    rank_planners: tuple[str, ...] = ("greedy", "exact"),
    logical_run_counts: tuple[int, ...] = (1, 10, 100),
    seed: int = 85,
    base_ids: np.ndarray | None = None,
    delta_ids: np.ndarray | None = None,
    truth_ids: np.ndarray | None = None,
    training_sample_rows: int = 100_000,
    encode_chunk_rows: int = 16_384,
    max_base_bytes: int = 16 * 1024 * 1024,
    max_base_gets: int = 32,
    landmark_count: int = 0,
    landmark_neighbors: int = 1_024,
    pages_per_landmark: int = 256,
    query_landmarks: int = 8,
    landmark_chunk_rows: int = 8_192,
    pq_lloyd_iterations: int = 10,
    posterior_training_queries: int = 0,
    posterior_training_neighbors: int = 100,
    posterior_top_rows: int = 2_048,
    posterior_projection_dimensions: int = 64,
    posterior_centroid_candidates: int = 256,
) -> dict[str, Any]:
    """Evaluate one shared base router with a fully resident delta tier."""

    if (
        base.ndim != 2
        or delta.ndim != 2
        or queries.ndim != 2
        or base.shape[1] != delta.shape[1]
        or base.shape[1] != queries.shape[1]
        or base.shape[0] == 0
        or delta.shape[0] == 0
        or queries.shape[0] == 0
        or base.shape[1] % subspaces != 0
        or page_rows <= 0
        or neighbors <= 0
        or not rank_top_rows
        or not rank_page_caps
        or not rank_planners
        or min(rank_top_rows) <= 0
        or min(rank_page_caps) <= 0
        or any(planner not in {"greedy", "exact"} for planner in rank_planners)
        or training_sample_rows <= 0
        or encode_chunk_rows <= 0
        or max_base_bytes <= 0
        or max_base_gets <= 0
        or pq_lloyd_iterations <= 0
        or landmark_count < 0
        or posterior_training_queries < 0
        or (
            posterior_training_queries > 0
            and (
                posterior_training_queries > base.shape[0]
                or not 0 < posterior_training_neighbors < base.shape[0]
                or posterior_top_rows <= 0
                or not 0 < posterior_projection_dimensions <= base.shape[1]
                or posterior_centroid_candidates <= 0
            )
        )
        or (
            landmark_count > 0
            and (
                landmark_count > base.shape[0]
                or not 0 < landmark_neighbors < base.shape[0]
                or pages_per_landmark <= 0
                or not 0 < query_landmarks <= landmark_count
                or landmark_chunk_rows <= 0
            )
        )
    ):
        raise ValueError("overlay shape differs")
    if not all(np.isfinite(value).all() for value in (base, delta, queries)):
        raise ValueError("overlay vectors must be finite")

    base = np.ascontiguousarray(base, dtype=np.float32)
    delta = np.ascontiguousarray(delta, dtype=np.float32)
    queries = np.ascontiguousarray(queries, dtype=np.float32)
    if base_ids is None:
        base_ids = np.arange(base.shape[0], dtype=np.int64)
    if delta_ids is None:
        delta_ids = np.arange(
            base.shape[0], base.shape[0] + delta.shape[0], dtype=np.int64
        )
    base_ids = np.asarray(base_ids, dtype=np.int64)
    delta_ids = np.asarray(delta_ids, dtype=np.int64)
    if (
        base_ids.shape != (base.shape[0],)
        or delta_ids.shape != (delta.shape[0],)
        or np.unique(np.concatenate((base_ids, delta_ids))).size
        != base.shape[0] + delta.shape[0]
    ):
        raise ValueError("overlay identifiers differ")
    if truth_ids is not None:
        truth_ids = np.asarray(truth_ids, dtype=np.int64)
        if truth_ids.shape != (queries.shape[0], neighbors):
            raise ValueError("overlay truth differs")

    sample_count = min(base.shape[0], training_sample_rows)
    books = _train_pq(
        base,
        subspaces,
        clusters,
        seed,
        sample_count,
        pq_lloyd_iterations,
    )
    base_codes = _encode_pq(base, books, encode_chunk_rows)
    base_sq8 = _sq8(base)
    base_page_sq8 = _page_sq8(base, page_rows)
    delta_sq8 = _sq8(delta)
    if truth_ids is None:
        all_ids = np.concatenate((base_ids, delta_ids))
        exact_corpus = np.concatenate((base, delta), axis=0)
        truth = [_top_ids(query, exact_corpus, all_ids, neighbors) for query in queries]
        truth_width = min(neighbors, exact_corpus.shape[0])
    else:
        truth = [row.tolist() for row in truth_ids]
        truth_width = neighbors

    oracle_result = evaluate_physical_oracle(
        base_ids=base_ids,
        delta_ids=delta_ids,
        truth_ids=np.asarray(truth, dtype=np.int64),
        dimensions=base.shape[1],
        page_rows=page_rows,
        max_base_bytes=max_base_bytes,
        max_base_gets=max_base_gets,
    )
    page_payload_bytes = oracle_result["page_payload_bytes"]
    max_base_pages = oracle_result["physical_oracle"]["max_base_pages"]

    router_scores = [_adc_scores(query, base_codes, books) for query in queries]
    cells = []
    for shortlist in shortlists:
        samples = []
        exact_hits = 0
        hybrid_hits = 0
        page_sq8_hits = 0
        sq8_hits = 0
        for query_index, query in enumerate(queries):
            scores = router_scores[query_index]
            take = min(shortlist, scores.size)
            head = np.argpartition(scores, take - 1)[:take]
            pages = np.unique(head // page_rows)
            ranges = _coalesce(pages)
            base_candidates = np.concatenate(
                [
                    np.arange(
                        start * page_rows,
                        min((end + 1) * page_rows, base.shape[0]),
                        dtype=np.int64,
                    )
                    for start, end in ranges
                ]
            )
            candidate_ids = np.concatenate((base_ids[base_candidates], delta_ids))
            exact_vectors = np.concatenate((base[base_candidates], delta), axis=0)
            hybrid_vectors = np.concatenate(
                (base_sq8[base_candidates], delta), axis=0
            )
            page_sq8_vectors = np.concatenate(
                (base_page_sq8[base_candidates], delta_sq8), axis=0
            )
            sq8_vectors = np.concatenate((base_sq8[base_candidates], delta_sq8), axis=0)
            exact_result = _top_ids(query, exact_vectors, candidate_ids, neighbors)
            hybrid_result = _top_ids(query, hybrid_vectors, candidate_ids, neighbors)
            page_sq8_result = _top_ids(
                query, page_sq8_vectors, candidate_ids, neighbors
            )
            sq8_result = _top_ids(query, sq8_vectors, candidate_ids, neighbors)
            expected = set(truth[query_index])
            exact_hits += len(expected.intersection(exact_result))
            hybrid_hits += len(expected.intersection(hybrid_result))
            page_sq8_hits += len(expected.intersection(page_sq8_result))
            sq8_hits += len(expected.intersection(sq8_result))
            samples.append(
                {
                    "base_bytes": int(
                        sum(end - start + 1 for start, end in ranges)
                        * page_payload_bytes
                    ),
                    "base_gets": len(ranges),
                    "exact_result_ids": exact_result,
                    "query": query_index,
                    "result_ids": page_sq8_result,
                }
            )
        denominator = len(queries) * truth_width
        first_result = samples[0]["result_ids"]
        base_bytes = [sample["base_bytes"] for sample in samples]
        base_gets = [sample["base_gets"] for sample in samples]
        cells.append(
            {
                "base_bytes_max": max(base_bytes),
                "base_bytes_p50": _nearest_percentile(base_bytes, 0.50),
                "base_bytes_p95": _nearest_percentile(base_bytes, 0.95),
                "base_gets_max": max(base_gets),
                "base_gets_p50": _nearest_percentile(base_gets, 0.50),
                "base_gets_p95": _nearest_percentile(base_gets, 0.95),
                "exact_recall_ppm": round(exact_hits * 1_000_000 / denominator),
                "hybrid_recall_ppm": round(hybrid_hits * 1_000_000 / denominator),
                "logical_run_results": [first_result] * len(logical_run_counts),
                "result_ids": first_result,
                "shortlist_rows": shortlist,
                "page_sq8_recall_ppm": round(
                    page_sq8_hits * 1_000_000 / denominator
                ),
                "sq8_recall_ppm": round(sq8_hits * 1_000_000 / denominator),
            }
        )
    passing_shortlists = [
        cell["shortlist_rows"]
        for cell in cells
        if cell["page_sq8_recall_ppm"] >= 990_000
        and cell["base_gets_max"] <= max_base_gets
        and cell["base_bytes_max"] <= max_base_bytes
    ]
    rank_weighted_cells = []
    for top_rows, page_cap, planner in (
        (top_rows, page_cap, planner)
        for top_rows in rank_top_rows
        for page_cap in rank_page_caps
        for planner in rank_planners
    ):
            samples = []
            exact_hits = 0
            page_sq8_hits = 0
            for query_index, query in enumerate(queries):
                _, ranges = _select_rank_weighted_pages(
                    router_scores[query_index],
                    page_rows=page_rows,
                    top_rows=top_rows,
                    max_span_pages=min(page_cap, max_base_pages),
                    max_ranges=max_base_gets,
                    gap=2,
                    planner=planner,
                )
                base_candidates = np.concatenate(
                    [
                        np.arange(
                            start * page_rows,
                            min((end + 1) * page_rows, base.shape[0]),
                            dtype=np.int64,
                        )
                        for start, end in ranges
                    ]
                )
                candidate_ids = np.concatenate((base_ids[base_candidates], delta_ids))
                exact_vectors = np.concatenate((base[base_candidates], delta), axis=0)
                page_sq8_vectors = np.concatenate(
                    (base_page_sq8[base_candidates], delta_sq8), axis=0
                )
                exact_result = _top_ids(query, exact_vectors, candidate_ids, neighbors)
                page_sq8_result = _top_ids(
                    query, page_sq8_vectors, candidate_ids, neighbors
                )
                expected = set(truth[query_index])
                query_exact_hits = len(expected.intersection(exact_result))
                query_page_sq8_hits = len(
                    expected.intersection(page_sq8_result)
                )
                exact_hits += query_exact_hits
                page_sq8_hits += query_page_sq8_hits
                samples.append(
                    {
                        "base_bytes": int(
                            sum(end - start + 1 for start, end in ranges)
                            * page_payload_bytes
                        ),
                        "base_gets": len(ranges),
                        "exact_hits": query_exact_hits,
                        "page_sq8_hits": query_page_sq8_hits,
                        "query": query_index,
                    }
                )
            base_bytes = [sample["base_bytes"] for sample in samples]
            base_gets = [sample["base_gets"] for sample in samples]
            rank_weighted_cells.append(
                {
                    "base_bytes_max": max(base_bytes),
                    "base_bytes_p50": _nearest_percentile(base_bytes, 0.50),
                    "base_bytes_p95": _nearest_percentile(base_bytes, 0.95),
                    "base_gets_max": max(base_gets),
                    "base_gets_p50": _nearest_percentile(base_gets, 0.50),
                    "base_gets_p95": _nearest_percentile(base_gets, 0.95),
                    "exact_recall_ppm": round(
                        exact_hits * 1_000_000 / denominator
                    ),
                    "page_cap": page_cap,
                    "planner": planner,
                    "page_sq8_recall_ppm": round(
                        page_sq8_hits * 1_000_000 / denominator
                    ),
                    "samples": samples,
                    "top_rows": top_rows,
                }
            )
    passing_rank_weighted_cells = [
        {
            "page_cap": cell["page_cap"],
            "planner": cell["planner"],
            "top_rows": cell["top_rows"],
        }
        for cell in rank_weighted_cells
        if cell["page_sq8_recall_ppm"] >= 990_000
        and cell["base_gets_max"] <= max_base_gets
        and cell["base_bytes_max"] <= max_base_bytes
    ]
    landmark_incidence_cells = []
    if landmark_count > 0:
        landmark_artifact = _build_landmark_incidence(
            base,
            base_ids,
            page_rows=page_rows,
            landmark_count=landmark_count,
            neighbor_count=landmark_neighbors,
            pages_per_landmark=pages_per_landmark,
            seed=seed,
            chunk_rows=landmark_chunk_rows,
        )
        samples = []
        exact_hits = 0
        page_sq8_hits = 0
        for query_index, query in enumerate(queries):
            page_scores = _landmark_page_scores(
                query, landmark_artifact, query_landmarks=query_landmarks
            )
            _, ranges = _select_optimal_weighted_pages(
                page_scores,
                max_span_pages=max_base_pages,
                max_ranges=max_base_gets,
            )
            base_candidates = np.concatenate(
                [
                    np.arange(
                        start * page_rows,
                        min((end + 1) * page_rows, base.shape[0]),
                        dtype=np.int64,
                    )
                    for start, end in ranges
                ]
            )
            candidate_ids = np.concatenate((base_ids[base_candidates], delta_ids))
            exact_vectors = np.concatenate((base[base_candidates], delta), axis=0)
            page_sq8_vectors = np.concatenate(
                (base_page_sq8[base_candidates], delta_sq8), axis=0
            )
            exact_result = _top_ids(query, exact_vectors, candidate_ids, neighbors)
            page_sq8_result = _top_ids(
                query, page_sq8_vectors, candidate_ids, neighbors
            )
            expected = set(truth[query_index])
            exact_hits += len(expected.intersection(exact_result))
            page_sq8_hits += len(expected.intersection(page_sq8_result))
            samples.append(
                {
                    "base_bytes": int(
                        sum(end - start + 1 for start, end in ranges)
                        * page_payload_bytes
                    ),
                    "base_gets": len(ranges),
                }
            )
        base_bytes = [sample["base_bytes"] for sample in samples]
        base_gets = [sample["base_gets"] for sample in samples]
        discarded_mass = np.asarray(landmark_artifact["discarded_mass"])
        artifact_bytes = (
            landmark_count * base.shape[1] * 4
            + landmark_count * pages_per_landmark * 8
            + (landmark_count + 1) * 4
        )
        landmark_incidence_cells.append(
            {
                "artifact_bytes": artifact_bytes,
                "base_bytes_max": max(base_bytes),
                "base_bytes_p50": _nearest_percentile(base_bytes, 0.50),
                "base_bytes_p95": _nearest_percentile(base_bytes, 0.95),
                "base_gets_max": max(base_gets),
                "base_gets_p50": _nearest_percentile(base_gets, 0.50),
                "base_gets_p95": _nearest_percentile(base_gets, 0.95),
                "discarded_mass_max_ppm": round(
                    float(np.max(discarded_mass)) * 1_000_000
                ),
                "exact_recall_ppm": round(
                    exact_hits * 1_000_000 / denominator
                ),
                "landmark_count": landmark_count,
                "landmark_neighbors": landmark_neighbors,
                "page_sq8_recall_ppm": round(
                    page_sq8_hits * 1_000_000 / denominator
                ),
                "pages_per_landmark": pages_per_landmark,
                "query_landmarks": query_landmarks,
            }
        )
    passing_landmark_cells = [
        cell["landmark_count"]
        for cell in landmark_incidence_cells
        if cell["page_sq8_recall_ppm"] >= 990_000
        and cell["base_gets_max"] <= max_base_gets
        and cell["base_bytes_max"] <= max_base_bytes
    ]
    page_posterior_cells = []
    if posterior_training_queries > 0:
        posterior_artifact = _build_page_posterior(
            base,
            base_ids,
            base_codes,
            books,
            page_rows=page_rows,
            training_queries=posterior_training_queries,
            training_neighbors=posterior_training_neighbors,
            top_rows=posterior_top_rows,
            projection_dimensions=posterior_projection_dimensions,
            centroid_candidates=posterior_centroid_candidates,
            seed=seed,
        )
        samples = []
        exact_hits = 0
        page_sq8_hits = 0
        candidate_oracle_hits = 0
        candidate_oracle_query_hits = []
        base_id_order = np.argsort(base_ids, kind="stable")
        sorted_base_ids = base_ids[base_id_order]
        sorted_delta_ids = np.sort(delta_ids, kind="stable")
        for query_index, query in enumerate(queries):
            page_scores = _score_page_posterior(
                query, router_scores[query_index], posterior_artifact
            )
            candidate_pages = np.flatnonzero(page_scores > 0.0)
            query_truth = np.asarray(truth[query_index], dtype=np.int64)
            base_lookup = np.searchsorted(sorted_base_ids, query_truth)
            base_matches = base_lookup < sorted_base_ids.size
            base_matches[base_matches] &= (
                sorted_base_ids[base_lookup[base_matches]]
                == query_truth[base_matches]
            )
            delta_lookup = np.searchsorted(sorted_delta_ids, query_truth)
            delta_matches = delta_lookup < sorted_delta_ids.size
            delta_matches[delta_matches] &= (
                sorted_delta_ids[delta_lookup[delta_matches]]
                == query_truth[delta_matches]
            )
            if not np.all(base_matches | delta_matches):
                raise ValueError("overlay truth identifiers differ")
            base_positions = base_id_order[base_lookup[base_matches]]
            query_candidate_oracle_hits = _candidate_physical_oracle_hits(
                candidate_pages,
                base_positions,
                delta_hits=int(np.sum(delta_matches)),
                page_rows=page_rows,
                page_count=page_scores.size,
                max_pages=max_base_pages,
                max_ranges=max_base_gets,
            )
            candidate_oracle_hits += query_candidate_oracle_hits
            candidate_oracle_query_hits.append(query_candidate_oracle_hits)
            _, ranges = _select_optimal_weighted_pages(
                page_scores,
                max_span_pages=max_base_pages,
                max_ranges=max_base_gets,
            )
            base_candidates = np.concatenate(
                [
                    np.arange(
                        start * page_rows,
                        min((end + 1) * page_rows, base.shape[0]),
                        dtype=np.int64,
                    )
                    for start, end in ranges
                ]
            )
            candidate_ids = np.concatenate((base_ids[base_candidates], delta_ids))
            exact_vectors = np.concatenate((base[base_candidates], delta), axis=0)
            page_sq8_vectors = np.concatenate(
                (base_page_sq8[base_candidates], delta_sq8), axis=0
            )
            exact_result = _top_ids(query, exact_vectors, candidate_ids, neighbors)
            page_sq8_result = _top_ids(
                query, page_sq8_vectors, candidate_ids, neighbors
            )
            expected = set(truth[query_index])
            query_exact_hits = len(expected.intersection(exact_result))
            query_page_sq8_hits = len(expected.intersection(page_sq8_result))
            exact_hits += query_exact_hits
            page_sq8_hits += query_page_sq8_hits
            samples.append(
                {
                    "base_bytes": int(
                        sum(end - start + 1 for start, end in ranges)
                        * page_payload_bytes
                    ),
                    "base_gets": len(ranges),
                    "candidate_oracle_hits": query_candidate_oracle_hits,
                    "exact_hits": query_exact_hits,
                    "page_sq8_hits": query_page_sq8_hits,
                    "query": query_index,
                }
            )
        base_bytes = [sample["base_bytes"] for sample in samples]
        base_gets = [sample["base_gets"] for sample in samples]
        artifact_bytes = int(posterior_artifact["artifact_bytes"])
        page_posterior_cells.append(
            {
                "artifact_bytes": artifact_bytes,
                "base_bytes_max": max(base_bytes),
                "base_bytes_p50": _nearest_percentile(base_bytes, 0.50),
                "base_bytes_p95": _nearest_percentile(base_bytes, 0.95),
                "base_gets_max": max(base_gets),
                "base_gets_p50": _nearest_percentile(base_gets, 0.50),
                "base_gets_p95": _nearest_percentile(base_gets, 0.95),
                "centroid_candidates": posterior_centroid_candidates,
                "candidate_oracle_recall_ppm": round(
                    candidate_oracle_hits * 1_000_000 / denominator
                ),
                "candidate_oracle_worst_query_recall_ppm": round(
                    min(candidate_oracle_query_hits)
                    * 1_000_000
                    / truth_width
                ),
                "exact_recall_ppm": round(
                    exact_hits * 1_000_000 / denominator
                ),
                "fit": posterior_artifact["fit"],
                "page_sq8_recall_ppm": round(
                    page_sq8_hits * 1_000_000 / denominator
                ),
                "projection_dimensions": posterior_projection_dimensions,
                "samples": samples,
                "top_rows": posterior_top_rows,
                "training_neighbors": posterior_training_neighbors,
                "training_queries": posterior_training_queries,
            }
        )
    passing_page_posterior_cells = [
        cell["training_queries"]
        for cell in page_posterior_cells
        if cell["fit"]["converged"]
        and cell["page_sq8_recall_ppm"] >= 990_000
        and cell["base_gets_max"] <= max_base_gets
        and cell["base_bytes_max"] <= max_base_bytes
    ]
    return {
        "cells": cells,
        "base_quantizer": "per-page-sq8",
        "base_quantizer_location": "page-payload",
        "base_quantizer_resident_bytes": 0,
        "claim_eligible": False,
        "cpu_parallelism": "sequential-per-query",
        "delta_resident_bytes": int(
            delta.shape[0] * (12 + delta.shape[1]) + 2 * delta.shape[1] * 4
        ),
        "delta_exact_resident_bytes": int(
            delta.shape[0] * (8 + delta.shape[1] * 4)
        ),
        "delta_rows": int(delta.shape[0]),
        "logical_run_counts": list(logical_run_counts),
        "landmark_incidence_cells": landmark_incidence_cells,
        "landmark_incidence_gate": {
            "max_base_bytes": max_base_bytes,
            "max_base_gets": max_base_gets,
            "min_page_sq8_recall_ppm": 990_000,
            "passed": bool(passing_landmark_cells),
            "passing_landmark_counts": passing_landmark_cells,
        },
        "page_payload_bytes": page_payload_bytes,
        "page_posterior_cells": page_posterior_cells,
        "page_posterior_gate": {
            "max_base_bytes": max_base_bytes,
            "max_base_gets": max_base_gets,
            "min_page_sq8_recall_ppm": 990_000,
            "passed": bool(passing_page_posterior_cells),
            "passing_training_query_counts": passing_page_posterior_cells,
        },
        "physical_oracle": oracle_result["physical_oracle"],
        "rank_weighted_cells": rank_weighted_cells,
        "rank_weighted_gate": {
            "max_base_bytes": max_base_bytes,
            "max_base_gets": max_base_gets,
            "min_page_sq8_recall_ppm": 990_000,
            "passed": bool(passing_rank_weighted_cells),
            "passing_cells": passing_rank_weighted_cells,
        },
        "promotion_gate": {
            "max_base_bytes": max_base_bytes,
            "max_base_gets": max_base_gets,
            "min_sq8_recall_ppm": 990_000,
            "passed": bool(passing_shortlists),
            "passing_shortlists": passing_shortlists,
        },
        "pq_lloyd_iterations": pq_lloyd_iterations,
        "schema": "borsuk-v85-shared-overlay-screen-v7",
        "training_sample_rows": sample_count,
        "training_rows": int(base.shape[0]),
    }


def _fixed_list(path: pathlib.Path, column: str, rows: int) -> np.ndarray:
    chunks = []
    seen = 0
    for batch in pq.ParquetFile(path).iter_batches(columns=[column], batch_size=rows):
        take = min(batch.num_rows, rows - seen)
        values = batch.column(0).slice(0, take)
        chunks.append(np.asarray(values.values.to_numpy(), dtype=np.float32))
        seen += take
        if seen == rows:
            break
    if seen != rows:
        raise ValueError("Parquet row count differs")
    width = chunks[0].size // min(rows, len(chunks[0]))
    return np.ascontiguousarray(np.concatenate(chunks).reshape(rows, width))


def _scalar(path: pathlib.Path, column: str, rows: int) -> np.ndarray:
    chunks = []
    seen = 0
    for batch in pq.ParquetFile(path).iter_batches(columns=[column], batch_size=rows):
        take = min(batch.num_rows, rows - seen)
        chunks.append(np.asarray(batch.column(0).slice(0, take)))
        seen += take
        if seen == rows:
            break
    if seen != rows:
        raise ValueError("Parquet row count differs")
    return np.asarray(np.concatenate(chunks), dtype=np.int64)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=pathlib.Path, required=True)
    parser.add_argument("--queries", type=pathlib.Path, required=True)
    parser.add_argument("--ground-truth", type=pathlib.Path, required=True)
    parser.add_argument("--layout-order", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--base-rows", type=int, default=9_000)
    parser.add_argument("--query-count", type=int, default=32)
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument("--oracle-only", action="store_true")
    parser.add_argument("--landmark-incidence", action="store_true")
    parser.add_argument("--page-posterior", action="store_true")
    parser.add_argument("--exact-row-control", action="store_true")
    parser.add_argument("--paired-rescore", action="store_true")
    args = parser.parse_args()
    if not 0 < args.base_rows < args.rows:
        parser.error("base rows must be inside the corpus")

    source_ids = _scalar(args.source, "feature_row_id", args.rows)
    truth_ids = _scalar(
        args.ground_truth, "feature_row_id", args.query_count * 100
    ).reshape(args.query_count, 100)
    order = np.asarray(np.load(args.layout_order), dtype=np.int64)
    base_order = order[order < args.base_rows]
    if base_order.size != args.base_rows or np.unique(base_order).size != args.base_rows:
        raise ValueError("base layout order differs")
    if args.exact_row_control and (args.oracle_only or args.landmark_incidence or args.page_posterior):
        parser.error("exact row control must run alone")
    if args.paired_rescore and (
        args.oracle_only
        or args.landmark_incidence
        or args.page_posterior
        or args.exact_row_control
    ):
        parser.error("paired rescore must run alone")
    if args.paired_rescore and args.query_count != 1_000:
        parser.error("paired rescore requires the complete 1,000-query development split")
    if args.oracle_only:
        result = evaluate_physical_oracle(
            base_ids=source_ids[base_order],
            delta_ids=source_ids[args.base_rows : args.rows],
            truth_ids=truth_ids,
            dimensions=args.dimensions,
        )
    else:
        source = _fixed_list(args.source, "embedding", args.rows)
        queries = _fixed_list(args.queries, "embedding", args.query_count)
        evaluation = evaluate_exact_row_control if args.exact_row_control else evaluate_overlay
        evaluation_args = {
            "base_ids": source_ids[base_order],
            "delta_ids": source_ids[args.base_rows : args.rows],
            "truth_ids": truth_ids,
        }
        if not args.exact_row_control:
            if args.paired_rescore:
                evaluation_args.update(
                    logical_run_counts=(),
                    posterior_training_queries=256,
                    rank_page_caps=(81,),
                    rank_planners=("exact",),
                    rank_top_rows=(1_024, 2_048),
                    shortlists=(),
                )
            else:
                evaluation_args.update(
                    landmark_count=1_024 if args.landmark_incidence else 0,
                    posterior_training_queries=256 if args.page_posterior else 0,
                )
        result = evaluation(
            source[base_order],
            source[args.base_rows : args.rows],
            queries,
            **evaluation_args,
        )
        if args.paired_rescore:
            result["paired_rescore"] = _paired_rescore_evidence(
                result["rank_weighted_cells"],
                result["page_posterior_cells"],
                neighbors=100,
                historical_prefix_queries=_V85_PAIRED_PREFIX_QUERIES,
                historical_prefix_sha256=_V85_PAIRED_PREFIX_SHA256,
                bootstrap_seed=85_000,
                bootstrap_repetitions=100_000,
            )
    body = json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n"
    args.output.write_text(body)
    print(body, end="")


if __name__ == "__main__":
    main()
