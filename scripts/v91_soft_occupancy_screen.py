"""Fixed soft top-100 page-occupancy reducer for the V91 falsifier."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from typing import Any

import numpy as np

from scripts.v85_shared_overlay_screen import _fixed_list, _page_payload_bytes, _scalar
from scripts.v86_coarse_to_fine_screen import (
    _CODE_PAGE_HEADER_BYTES,
    CoarseToFineArtifact,
    build_coarse_to_fine_artifact,
    evaluate_coarse_to_fine,
    validate_layout_order,
    validate_truth_rows,
)
from scripts.v87_summary_capacity_screen import (
    _REGISTERED_CONTROL_ARTIFACT_SHA256,
    select_development_rows,
    summarize_development_arm,
    summarize_wave1_attribution,
)
from scripts.v88_planner_objective_screen import (
    _rank_cut_attribution,
    validate_result_budgets,
)
from scripts.v90_residual_row_sketch_screen import (
    _BASE_ROWS,
    _CLUSTERS,
    _CONTROL_SEED,
    _CONTROL_SUBSPACES,
    _DEVELOPMENT_QUERIES,
    _DIMENSIONS,
    _LOADED_QUERIES,
    _NEIGHBORS,
    _PAGE_ROWS,
    _ROWS,
    _SKETCH_SEED,
    _SKETCH_SUBSPACES,
    _TRAINING_ITERATIONS,
    _TRAINING_SAMPLE_ROWS,
    _WAVE1_MAX_RANGES,
    _WAVE1_MAX_SPAN_PAGES,
    _WAVE1_RANK_PAGES,
    _WAVE2_MAX_RANGES,
    _WAVE2_MAX_SPAN_PAGES,
    _WAVE2_TOP_ROWS,
    ResidualRowSketchArtifact,
    _array_sha256,
    _candidate_fenced_truth_oracle,
    _control_page_scores,
    _rank_scores_from_candidate_estimates,
    _residual_candidate_row_estimates,
    _validate_sketch,
    build_residual_row_sketch,
)
from scripts.v90_residual_row_sketch_screen import (
    build_run_metadata as _v90_run_metadata,
)

_SOFT_MASS_SCALE = 1 << 12
_SOFT_TARGET_ROWS = 100
_SOFT_TEMPERATURE_ROWS = 200
_SOFT_BISECTION_ITERATIONS = 48
_SOFT_PRIMARY_MULTIPLIER = 1 << 33
_REGISTERED_RESIDUAL_SKETCH_SHA256 = (
    "21aae14867749ac8dfed26043f8cb225024d4928bb81db32d488dd0c23a5a3bd"
)


def _soft_top100_page_weights_with_diagnostics(
    row_estimates: np.ndarray,
    row_pages: np.ndarray,
    valid_rows: np.ndarray,
    *,
    page_count: int,
) -> tuple[np.ndarray, dict[str, float]]:
    """Reduce candidate-row ADC estimates and retain replay diagnostics."""

    estimates = np.asarray(row_estimates, dtype=np.float32)
    pages = np.asarray(row_pages)
    valid = np.asarray(valid_rows)
    if (
        estimates.ndim != 1
        or pages.shape != estimates.shape
        or pages.dtype.kind not in "iu"
        or valid.shape != estimates.shape
        or valid.dtype != np.bool_
        or page_count <= 0
        or np.any(pages[valid] < 0)
        or np.any(pages[valid] >= page_count)
        or np.count_nonzero(valid) < _SOFT_TEMPERATURE_ROWS
        or not np.isfinite(estimates[valid]).all()
    ):
        raise ValueError("V91 soft occupancy authority differs")

    valid_estimates = estimates[valid].astype(np.float64)
    order_statistics = np.partition(
        valid_estimates, (_SOFT_TARGET_ROWS - 1, _SOFT_TEMPERATURE_ROWS - 1)
    )
    distance_100 = float(order_statistics[_SOFT_TARGET_ROWS - 1])
    distance_200 = float(order_statistics[_SOFT_TEMPERATURE_ROWS - 1])
    temperature = max(
        distance_200 - distance_100,
        2.0**-20 * max(1.0, abs(distance_100)),
    )
    lower = float(np.min(valid_estimates))
    upper = distance_100 + temperature
    for _ in range(_SOFT_BISECTION_ITERATIONS):
        midpoint = (lower + upper) * 0.5
        occupancy = np.clip(
            (midpoint - valid_estimates) / temperature, 0.0, 1.0
        )
        if float(np.sum(occupancy, dtype=np.float64)) < _SOFT_TARGET_ROWS:
            lower = midpoint
        else:
            upper = midpoint
    threshold = (lower + upper) * 0.5
    occupancy = np.clip(
        (threshold - valid_estimates) / temperature, 0.0, 1.0
    )
    page_mass = np.zeros(page_count, dtype=np.float64)
    np.add.at(page_mass, pages[valid].astype(np.int64, copy=False), occupancy)
    return np.floor(page_mass * _SOFT_MASS_SCALE).astype(np.uint64), {
        "distance_100": distance_100,
        "distance_200": distance_200,
        "temperature": temperature,
        "threshold": threshold,
    }


def soft_top100_page_weights(
    row_estimates: np.ndarray,
    row_pages: np.ndarray,
    valid_rows: np.ndarray,
    *,
    page_count: int,
) -> np.ndarray:
    """Reduce candidate-row ADC estimates to fixed additive page mass."""
    return _soft_top100_page_weights_with_diagnostics(
        row_estimates, row_pages, valid_rows, page_count=page_count
    )[0]


def _validate_soft_evidence(
    page_scores: np.ndarray,
    page_mass: np.ndarray,
    *,
    rank_limit: int,
) -> None:
    """Require conserved mass and forbid positive mass outside the rank fence."""

    scores = np.asarray(page_scores, dtype=np.float32)
    mass = np.asarray(page_mass)
    if (
        scores.ndim != 2
        or scores.shape != mass.shape
        or mass.dtype.kind not in "iu"
        or (mass.dtype.kind == "i" and np.any(mass < 0))
        or not np.isfinite(scores).all()
        or rank_limit <= 0
    ):
        raise ValueError("V91 soft evidence differs")
    take = min(rank_limit, scores.shape[1])
    page_ids = np.arange(scores.shape[1], dtype=np.int64)
    minimum_mass = _SOFT_TARGET_ROWS * _SOFT_MASS_SCALE - scores.shape[1]
    maximum_mass = _SOFT_TARGET_ROWS * _SOFT_MASS_SCALE
    for score_row, mass_row in zip(scores, mass, strict=True):
        total = int(np.sum(mass_row.astype(object)))
        members = np.zeros(scores.shape[1], dtype=bool)
        members[np.lexsort((page_ids, score_row))[:take]] = True
        if total < minimum_mass or total > maximum_mass or np.any(mass_row[~members]):
            raise ValueError("V91 soft evidence differs")


def _compound_soft_page_weights(
    page_mass: np.ndarray,
    page_scores: np.ndarray,
    *,
    rank_limit: int,
    max_span_pages: int,
) -> np.ndarray:
    """Use rank evidence only as a strictly lower-order planner tie-break."""

    mass = np.asarray(page_mass, dtype=np.uint64)
    scores = np.asarray(page_scores, dtype=np.float32)
    if (
        mass.ndim != 1
        or scores.shape != mass.shape
        or not np.isfinite(scores).all()
        or rank_limit <= 0
        or max_span_pages <= 0
    ):
        raise ValueError("V91 soft evidence differs")
    page_ids = np.arange(mass.size, dtype=np.int64)
    order = np.lexsort((page_ids, scores))
    take = min(rank_limit, mass.size)
    rank_mass = np.asarray(
        [1_000_000_000 // (rank + 1) for rank in range(take)],
        dtype=np.uint64,
    )
    maximum_secondary = sum(
        int(value) for value in rank_mass[: min(take, max_span_pages)]
    )
    if _SOFT_PRIMARY_MULTIPLIER <= maximum_secondary:
        raise ValueError("V91 soft evidence differs")
    secondary = np.zeros(mass.size, dtype=np.uint64)
    secondary[order[:take]] = rank_mass
    if int(np.max(mass, initial=np.uint64(0))) > (
        2**53 - maximum_secondary
    ) // _SOFT_PRIMARY_MULTIPLIER:
        raise ValueError("V91 soft evidence differs")
    combined = mass * np.uint64(_SOFT_PRIMARY_MULTIPLIER) + secondary
    if np.any(combined.astype(np.float64) > 2**53):
        raise ValueError("V91 soft evidence differs")
    return combined


def _residual_page_evidence_pair(
    query: np.ndarray,
    control_page_scores: np.ndarray,
    control_artifact: CoarseToFineArtifact,
    artifact: ResidualRowSketchArtifact,
    *,
    rank_limit: int,
    validated_control_sha256: str,
) -> tuple[np.ndarray, np.ndarray, dict[str, float]]:
    candidates, estimates, valid = _residual_candidate_row_estimates(
        query,
        control_page_scores,
        control_artifact,
        artifact,
        rank_limit=rank_limit,
        validated_control_sha256=validated_control_sha256,
    )
    rank_scores = _rank_scores_from_candidate_estimates(
        candidates,
        estimates,
        control_page_scores,
        page_rows=artifact.page_rows,
    )
    row_pages = np.broadcast_to(candidates[:, None], estimates.shape)
    page_mass, diagnostics = _soft_top100_page_weights_with_diagnostics(
        estimates.reshape(-1),
        row_pages.reshape(-1),
        valid.reshape(-1),
        page_count=control_page_scores.size,
    )
    return rank_scores, page_mass, diagnostics


def residual_soft_page_weights(
    query: np.ndarray,
    control_page_scores: np.ndarray,
    control_artifact: CoarseToFineArtifact,
    artifact: ResidualRowSketchArtifact,
    *,
    rank_limit: int,
    validated_control_sha256: str,
) -> np.ndarray:
    """Return additive soft top-100 mass inside the control-page fence."""

    _, weights, _ = _residual_page_evidence_pair(
        query,
        control_page_scores,
        control_artifact,
        artifact,
        rank_limit=rank_limit,
        validated_control_sha256=validated_control_sha256,
    )
    return weights


def _soft_evidence_digest(
    sketch_sha256: str,
    scores_sha256: str,
    mass_sha256: str,
    weights_sha256: str,
) -> str:
    authority = (
        "borsuk-v91-soft-top100-v1:"
        f"target={_SOFT_TARGET_ROWS}:"
        f"temperature-rows={_SOFT_TEMPERATURE_ROWS}:"
        f"scale={_SOFT_MASS_SCALE}:"
        f"bisections={_SOFT_BISECTION_ITERATIONS}:"
        f"primary-multiplier={_SOFT_PRIMARY_MULTIPLIER}:"
    )
    return hashlib.sha256(
        (
            authority
            + sketch_sha256
            + scores_sha256
            + mass_sha256
            + weights_sha256
        ).encode("ascii")
    ).hexdigest()


def _candidate_fence_digest(page_scores: np.ndarray, rank_limit: int) -> str:
    scores = np.asarray(page_scores, dtype=np.float32)
    if (
        scores.ndim != 2
        or scores.shape[0] == 0
        or scores.shape[1] == 0
        or not np.isfinite(scores).all()
        or rank_limit <= 0
    ):
        raise ValueError("V91 candidate fence differs")
    take = min(rank_limit, scores.shape[1])
    page_ids = np.arange(scores.shape[1], dtype=np.int64)
    digest = hashlib.sha256()
    digest.update(np.asarray(scores.shape, dtype="<u8").tobytes())
    digest.update(np.asarray([take], dtype="<u8").tobytes())
    for row in scores:
        members = np.sort(np.lexsort((page_ids, row))[:take]).astype(
            "<u4", copy=False
        )
        digest.update(members.tobytes())
    return digest.hexdigest()


def evaluate_soft_occupancy_pair(
    base: np.ndarray,
    delta: np.ndarray,
    queries: np.ndarray,
    *,
    control_artifact: CoarseToFineArtifact,
    sketch_artifact: ResidualRowSketchArtifact,
    base_ids: np.ndarray,
    delta_ids: np.ndarray,
    truth_ids: np.ndarray,
    query_ordinals: np.ndarray,
    page_rows: int,
    neighbors: int,
    control_subspaces: int,
    control_clusters: int,
    wave1_rank_pages: int,
    wave1_max_span_pages: int,
    wave1_max_ranges: int,
    wave2_top_rows: int,
    wave2_max_span_pages: int,
    wave2_max_ranges: int,
    code_page_bytes: int | None = None,
    data_page_bytes: int | None = None,
) -> dict[str, Any]:
    """Compare V90 row-min evidence with fixed V91 soft occupancy."""

    base = np.asarray(base, dtype=np.float32)
    queries = np.asarray(queries, dtype=np.float32)
    _validate_sketch(
        sketch_artifact, rows=base.shape[0], dimensions=base.shape[1]
    )
    control_sha256 = control_artifact.digest()
    sketch_sha256 = sketch_artifact.digest()
    if (
        sketch_artifact.base_vectors_sha256
        != _array_sha256(base, np.dtype(np.float32))
        or sketch_artifact.control_artifact_sha256 != control_sha256
        or sketch_artifact.page_rows != page_rows
    ):
        raise ValueError("V91 sketch binding differs")
    page_count = (base.shape[0] + page_rows - 1) // page_rows
    score_rows: list[np.ndarray] = []
    mass_rows: list[np.ndarray] = []
    diagnostic_rows: list[dict[str, float]] = []
    for query in queries:
        scores, mass, diagnostics = _residual_page_evidence_pair(
            query,
            _control_page_scores(query, control_artifact, page_count),
            control_artifact,
            sketch_artifact,
            rank_limit=wave1_rank_pages,
            validated_control_sha256=control_sha256,
        )
        score_rows.append(scores)
        mass_rows.append(mass)
        diagnostic_rows.append(diagnostics)
    row_min_scores = np.vstack(score_rows)
    soft_page_mass = np.vstack(mass_rows)
    _validate_soft_evidence(
        row_min_scores, soft_page_mass, rank_limit=wave1_rank_pages
    )
    soft_weights = np.vstack(
        [
            _compound_soft_page_weights(
                mass,
                scores,
                rank_limit=wave1_rank_pages,
                max_span_pages=wave1_max_span_pages,
            )
            for mass, scores in zip(soft_page_mass, row_min_scores, strict=True)
        ]
    )
    scores_sha256 = _array_sha256(row_min_scores, np.dtype(np.float32))
    mass_sha256 = _array_sha256(soft_page_mass, np.dtype(np.uint64))
    weights_sha256 = _array_sha256(soft_weights, np.dtype(np.uint64))
    control_evidence_sha256 = hashlib.sha256(
        ("borsuk-v91-v90-row-min-v1:" + sketch_sha256 + scores_sha256).encode(
            "ascii"
        )
    ).hexdigest()
    soft_evidence_sha256 = _soft_evidence_digest(
        sketch_sha256, scores_sha256, mass_sha256, weights_sha256
    )
    candidate_fence_sha256 = _candidate_fence_digest(
        row_min_scores, wave1_rank_pages
    )
    if code_page_bytes is None:
        code_page_bytes = page_rows * control_subspaces + _CODE_PAGE_HEADER_BYTES
    if data_page_bytes is None:
        data_page_bytes = _page_payload_bytes(
            dimensions=base.shape[1], page_rows=page_rows
        )
    common = {
        "artifact": control_artifact,
        "base_ids": base_ids,
        "delta_ids": delta_ids,
        "truth_ids": truth_ids,
        "query_ordinals": query_ordinals,
        "page_rows": page_rows,
        "neighbors": neighbors,
        "subspaces": control_subspaces,
        "clusters": control_clusters,
        "wave1_rank_pages": wave1_rank_pages,
        "wave1_max_span_pages": wave1_max_span_pages,
        "wave1_max_ranges": wave1_max_ranges,
        "wave2_top_rows": wave2_top_rows,
        "wave2_max_span_pages": wave2_max_span_pages,
        "wave2_max_ranges": wave2_max_ranges,
        "code_page_bytes": code_page_bytes,
        "data_page_bytes": data_page_bytes,
    }
    arms: dict[str, dict[str, Any]] = {}
    evidence = (
        (
            "control",
            row_min_scores,
            None,
            control_evidence_sha256,
            "residual-pq16-row-min",
        ),
        (
            "challenger",
            None,
            soft_weights,
            soft_evidence_sha256,
            "residual-pq16-soft-top100-reciprocal-rank-fallback",
        ),
    )
    for name, scores, weights, digest, label in evidence:
        result = evaluate_coarse_to_fine(
            base,
            delta,
            queries,
            wave1_page_scores_by_query=(
                row_min_scores if weights is not None else scores
            ),
            wave1_page_weights_by_query=weights,
            wave1_page_evidence_sha256=digest,
            **common,
        )
        arms[name] = {
            "artifact_sha256": control_sha256,
            "budgets": validate_result_budgets(
                result,
                wave1_page_bytes=code_page_bytes,
                wave1_max_pages=wave1_max_span_pages,
                wave1_max_ranges=wave1_max_ranges,
                wave2_page_bytes=data_page_bytes,
                wave2_max_pages=wave2_max_span_pages,
                wave2_max_ranges=wave2_max_ranges,
            ),
            "page_evidence": label,
            "rank_cut_base_truth_hits": _rank_cut_attribution(result),
            "result": result,
            "wave1_attribution": summarize_wave1_attribution(result),
        }
    return {
        "actual_s3_requests": 0,
        "arms": arms,
        "candidate_fence_oracle": _candidate_fenced_truth_oracle(
            arms["control"]["result"],
            neighbors=neighbors,
            page_count=page_count,
            page_rows=page_rows,
            rank_limit=wave1_rank_pages,
            max_pages=wave1_max_span_pages,
            max_ranges=wave1_max_ranges,
        ),
        "candidate_fence_sha256": candidate_fence_sha256,
        "changed_parameter": "wave1_page_utility",
        "claim_eligible": False,
        "delta_representation": "production-sq8",
        "schema": "borsuk-v91-soft-occupancy-screen-v1",
        "sketch_sha256": sketch_sha256,
        "row_min_scores_sha256": scores_sha256,
        "soft_page_mass_sha256": mass_sha256,
        "soft_page_weights_sha256": weights_sha256,
        "soft_reducer_diagnostics": diagnostic_rows,
        "soft_evidence_sha256": soft_evidence_sha256,
    }


def build_run_metadata() -> dict[str, Any]:
    """Return the fixed V91 authority and honest 100M projection."""

    metadata = _v90_run_metadata()
    metadata["configuration"].update(
        {
            "soft_bisection_iterations": _SOFT_BISECTION_ITERATIONS,
            "soft_fallback_objective": "reciprocal-rank",
            "soft_mass_scale": _SOFT_MASS_SCALE,
            "soft_primary_multiplier": _SOFT_PRIMARY_MULTIPLIER,
            "soft_target_rows": _SOFT_TARGET_ROWS,
            "soft_temperature_rows": _SOFT_TEMPERATURE_ROWS,
            "blas_threads": 16,
            "query_parallelism": 1,
            "rayon_work_stealing": False,
            "worker_model": "fixed-16-thread-blas",
        }
    )
    metadata["projection_100m"].update(
        {
            "added_resident_bytes": 0,
            "candidate_row_estimate_bytes_per_query": (
                _WAVE1_RANK_PAGES * _PAGE_ROWS * 4
            ),
            "occupancy_row_evaluations_per_query": (
                _WAVE1_RANK_PAGES
                * _PAGE_ROWS
                * _SOFT_BISECTION_ITERATIONS
            ),
        }
    )
    metadata["scope"] = "fixed-1m-burned-development-soft-occupancy-falsifier"
    return metadata


def _arm_hits(result: dict[str, Any]) -> tuple[int, int, int, int]:
    samples = result["samples"]
    total = sum(int(sample["page_sq8_hits"]) for sample in samples)
    base = sum(int(sample["page_sq8_base_hits"]) for sample in samples)
    query_15 = next(
        int(sample["page_sq8_hits"])
        for sample in samples
        if int(sample["query"]) == 15
    )
    planner_misses = sum(
        int(sample["wave1_planner_missed_base_truth_hits"])
        for sample in samples
    )
    return total, base, query_15, planner_misses


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the fixed V91 soft top-100 occupancy falsifier."
    )
    parser.add_argument("--source", type=pathlib.Path, required=True)
    parser.add_argument("--queries", type=pathlib.Path, required=True)
    parser.add_argument("--ground-truth", type=pathlib.Path, required=True)
    parser.add_argument("--layout-order", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()

    source_ids = _scalar(args.source, "feature_row_id", _ROWS)
    if np.unique(source_ids).size != _ROWS:
        raise ValueError("source identifiers differ")
    loaded_truth = validate_truth_rows(
        _scalar(args.ground_truth, "query_ordinal", _LOADED_QUERIES * _NEIGHBORS),
        _scalar(args.ground_truth, "rank", _LOADED_QUERIES * _NEIGHBORS),
        _scalar(args.ground_truth, "feature_row_id", _LOADED_QUERIES * _NEIGHBORS),
        queries=_LOADED_QUERIES,
        neighbors=_NEIGHBORS,
    )
    full_order = np.asarray(np.load(args.layout_order), dtype=np.int64)
    if (
        full_order.shape != (_ROWS,)
        or np.any(full_order < 0)
        or np.any(full_order >= _ROWS)
        or np.unique(full_order).size != _ROWS
    ):
        raise ValueError("full layout order differs")
    base_order = validate_layout_order(
        full_order[full_order < _BASE_ROWS], rows=_ROWS, base_rows=_BASE_ROWS
    )
    source = _fixed_list(args.source, "embedding", _ROWS)
    loaded_queries = _fixed_list(args.queries, "embedding", _LOADED_QUERIES)
    if source.shape != (_ROWS, _DIMENSIONS) or loaded_queries.shape != (
        _LOADED_QUERIES,
        _DIMENSIONS,
    ):
        raise ValueError("V91 vector dimensions differ")
    queries, truth, query_ordinals = select_development_rows(
        loaded_queries, loaded_truth, development_queries=_DEVELOPMENT_QUERIES
    )
    base = np.ascontiguousarray(source[base_order])
    base_ids = np.ascontiguousarray(source_ids[base_order])
    control_artifact = build_coarse_to_fine_artifact(
        base,
        page_rows=_PAGE_ROWS,
        subspaces=_CONTROL_SUBSPACES,
        clusters=_CLUSTERS,
        sample_rows=_TRAINING_SAMPLE_ROWS,
        seed=_CONTROL_SEED,
        iterations=_TRAINING_ITERATIONS,
    )
    control_sha256 = control_artifact.digest()
    if control_sha256 != _REGISTERED_CONTROL_ARTIFACT_SHA256:
        raise ValueError("V91 registered control artifact differs")
    sketch_artifact = build_residual_row_sketch(
        base,
        control_artifact,
        subspaces=_SKETCH_SUBSPACES,
        clusters=_CLUSTERS,
        sample_rows=_TRAINING_SAMPLE_ROWS,
        seed=_SKETCH_SEED,
        iterations=_TRAINING_ITERATIONS,
    )
    if sketch_artifact.digest() != _REGISTERED_RESIDUAL_SKETCH_SHA256:
        raise ValueError("V91 registered residual sketch differs")
    result = evaluate_soft_occupancy_pair(
        base,
        np.ascontiguousarray(source[_BASE_ROWS:]),
        queries,
        control_artifact=control_artifact,
        sketch_artifact=sketch_artifact,
        base_ids=base_ids,
        delta_ids=source_ids[_BASE_ROWS:],
        truth_ids=truth,
        query_ordinals=query_ordinals,
        page_rows=_PAGE_ROWS,
        neighbors=_NEIGHBORS,
        control_subspaces=_CONTROL_SUBSPACES,
        control_clusters=_CLUSTERS,
        wave1_rank_pages=_WAVE1_RANK_PAGES,
        wave1_max_span_pages=_WAVE1_MAX_SPAN_PAGES,
        wave1_max_ranges=_WAVE1_MAX_RANGES,
        wave2_top_rows=_WAVE2_TOP_ROWS,
        wave2_max_span_pages=_WAVE2_MAX_SPAN_PAGES,
        wave2_max_ranges=_WAVE2_MAX_RANGES,
    )
    for arm in result["arms"].values():
        arm["gate"] = summarize_development_arm(
            arm["result"], neighbors=_NEIGHBORS
        )
    control_hits = _arm_hits(result["arms"]["control"]["result"])
    if control_hits != (3_167, 2_846, 90, 26):
        raise ValueError("V91 registered V90 control differs")
    challenger_hits = _arm_hits(result["arms"]["challenger"]["result"])
    passed = (
        challenger_hits[0] >= 3_176
        and challenger_hits[1] >= 2_854
        and challenger_hits[2] >= 90
        and result["candidate_fence_oracle"]["can_pass_registered_gate"]
    )
    result["decision"] = {
        "accepted": "challenger" if passed else None,
        "reason": "registered-gate-passed" if passed else "registered-gate-failed",
    }
    result["registered_gate"] = {
        "base_hits": 2_854,
        "query_15_hits": 90,
        "total_hits": 3_176,
    }
    result.update(build_run_metadata())
    body = json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n"
    args.output.write_text(body)
    print(body, end="")


if __name__ == "__main__":
    main()
