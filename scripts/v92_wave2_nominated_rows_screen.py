"""Fixed wave-two row-nomination falsifier over V90 residual evidence."""

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
    plan_ranked_pages,
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
    _control_page_scores,
    _rank_scores_from_candidate_estimates,
    _residual_candidate_row_estimates,
    _validate_sketch,
    build_residual_row_sketch,
)
from scripts.v90_residual_row_sketch_screen import (
    build_run_metadata as _v90_run_metadata,
)

_NOMINATION_ROWS = 512
_MINIMUM_REACHABLE_MISSED_TRUTH = 8
_REGISTERED_RESIDUAL_SKETCH_SHA256 = (
    "21aae14867749ac8dfed26043f8cb225024d4928bb81db32d488dd0c23a5a3bd"
)


def nominate_unselected_rows(
    candidates: np.ndarray,
    estimates: np.ndarray,
    valid: np.ndarray,
    *,
    selected_pages: np.ndarray,
    page_rows: int,
    rows: int,
    take: int,
) -> np.ndarray:
    """Rank valid residual estimates whose logical pages wave one did not select."""

    candidates = np.asarray(candidates, dtype=np.int64)
    estimates = np.asarray(estimates, dtype=np.float32)
    valid = np.asarray(valid, dtype=bool)
    selected_pages = np.asarray(selected_pages, dtype=np.int64)
    if (
        candidates.ndim != 1
        or estimates.shape != valid.shape
        or estimates.shape != (candidates.size, page_rows)
        or np.unique(candidates).size != candidates.size
        or selected_pages.ndim != 1
        or np.unique(selected_pages).size != selected_pages.size
        or np.any(candidates < 0)
        or np.any(selected_pages < 0)
        or page_rows <= 0
        or rows <= 0
        or take <= 0
    ):
        raise ValueError("V92 nomination authority differs")
    offsets = np.arange(page_rows, dtype=np.int64)
    positions = candidates[:, None] * page_rows + offsets[None, :]
    eligible = valid & (positions < rows) & ~np.isin(candidates[:, None], selected_pages)
    eligible_positions = positions[eligible]
    eligible_estimates = estimates[eligible]
    if not np.isfinite(eligible_estimates).all():
        raise ValueError("V92 nomination authority differs")
    order = np.lexsort((eligible_positions, eligible_estimates))
    return np.ascontiguousarray(eligible_positions[order[:take]], dtype=np.int64)


def nomination_reachability(
    *,
    nominations: np.ndarray,
    truth_positions: np.ndarray,
    selected_pages: np.ndarray,
    page_rows: int,
) -> dict[str, Any]:
    """Describe truth-only reachability without feeding labels into selection."""

    nominations = np.asarray(nominations, dtype=np.int64)
    truth_positions = np.asarray(truth_positions, dtype=np.int64)
    selected_pages = np.asarray(selected_pages, dtype=np.int64)
    if (
        nominations.ndim != 1
        or truth_positions.ndim != 1
        or selected_pages.ndim != 1
        or np.unique(nominations).size != nominations.size
        or np.any(nominations < 0)
        or np.any(truth_positions < 0)
        or np.any(selected_pages < 0)
        or page_rows <= 0
    ):
        raise ValueError("V92 reachability evidence differs")
    missed = truth_positions[~np.isin(truth_positions // page_rows, selected_pages)]
    rank_by_position = {
        int(position): rank for rank, position in enumerate(nominations)
    }
    ranks = [rank_by_position[int(position)] for position in missed if int(position) in rank_by_position]
    return {
        "missed_truth_rows": int(missed.size),
        "nominated_missed_truth_rows": len(ranks),
        "missed_truth_nomination_ranks": ranks,
    }


def _arm(
    result: dict[str, Any],
    *,
    artifact_sha256: str,
    label: str,
    code_page_bytes: int,
    data_page_bytes: int,
    wave1_max_span_pages: int,
    wave1_max_ranges: int,
    wave2_max_span_pages: int,
    wave2_max_ranges: int,
) -> dict[str, Any]:
    return {
        "artifact_sha256": artifact_sha256,
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


def evaluate_wave2_nominated_pair(
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
    nomination_rows: int,
    minimum_reachable_missed_truth: int,
    code_page_bytes: int | None = None,
    data_page_bytes: int | None = None,
) -> dict[str, Any]:
    """Run V90 once, then conditionally add label-blind wave-two evidence."""

    base = np.asarray(base, dtype=np.float32)
    queries = np.asarray(queries, dtype=np.float32)
    base_ids = np.asarray(base_ids, dtype=np.int64)
    truth_ids = np.asarray(truth_ids, dtype=np.int64)
    _validate_sketch(sketch_artifact, rows=base.shape[0], dimensions=base.shape[1])
    control_sha256 = control_artifact.digest()
    sketch_sha256 = sketch_artifact.digest()
    if (
        sketch_artifact.base_vectors_sha256
        != _array_sha256(base, np.dtype(np.float32))
        or sketch_artifact.control_artifact_sha256 != control_sha256
        or sketch_artifact.page_rows != page_rows
        or base_ids.shape != (base.shape[0],)
        or truth_ids.shape != (queries.shape[0], neighbors)
        or nomination_rows <= 0
        or minimum_reachable_missed_truth < 0
    ):
        raise ValueError("V92 authority differs")
    page_count = (base.shape[0] + page_rows - 1) // page_rows
    row_min_rows: list[np.ndarray] = []
    candidate_rows: list[tuple[np.ndarray, np.ndarray, np.ndarray]] = []
    for query in queries:
        control_scores = _control_page_scores(query, control_artifact, page_count)
        candidates, estimates, valid = _residual_candidate_row_estimates(
            query,
            control_scores,
            control_artifact,
            sketch_artifact,
            rank_limit=wave1_rank_pages,
            validated_control_sha256=control_sha256,
        )
        row_min_rows.append(
            _rank_scores_from_candidate_estimates(
                candidates, estimates, control_scores, page_rows=page_rows
            )
        )
        candidate_rows.append((candidates, estimates, valid))
    row_min_scores = np.vstack(row_min_rows)
    row_min_sha256 = _array_sha256(row_min_scores, np.dtype(np.float32))
    control_evidence_sha256 = hashlib.sha256(
        ("borsuk-v92-v90-row-min-v1:" + sketch_sha256 + row_min_sha256).encode(
            "ascii"
        )
    ).hexdigest()
    if code_page_bytes is None:
        code_page_bytes = page_rows * control_subspaces + _CODE_PAGE_HEADER_BYTES
    if data_page_bytes is None:
        data_page_bytes = _page_payload_bytes(
            dimensions=base.shape[1], page_rows=page_rows
        )
    planned_wave1_pages = [
        plan_ranked_pages(
            scores,
            rank_limit=wave1_rank_pages,
            max_span_pages=wave1_max_span_pages,
            max_ranges=wave1_max_ranges,
        )[0]
        for scores in row_min_scores
    ]
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
        "wave1_page_scores_by_query": row_min_scores,
        "wave1_page_evidence_sha256": control_evidence_sha256,
    }
    id_to_position = {int(identifier): position for position, identifier in enumerate(base_ids)}
    nominations: list[np.ndarray] = []
    reachability_rows: list[dict[str, Any]] = []
    for query_index, (candidates, estimates, valid) in enumerate(candidate_rows):
        selected_pages = planned_wave1_pages[query_index]
        nominated = nominate_unselected_rows(
            candidates,
            estimates,
            valid,
            selected_pages=selected_pages,
            page_rows=page_rows,
            rows=base.shape[0],
            take=nomination_rows,
        )
        if nominated.size != nomination_rows:
            raise ValueError("V92 nomination authority differs")
        nominations.append(nominated)
        truth_positions = np.asarray(
            [
                id_to_position[int(identifier)]
                for identifier in truth_ids[query_index]
                if int(identifier) in id_to_position
            ],
            dtype=np.int64,
        )
        diagnostics = nomination_reachability(
            nominations=nominated,
            truth_positions=truth_positions,
            selected_pages=selected_pages,
            page_rows=page_rows,
        )
        diagnostics["query"] = int(query_ordinals[query_index])
        reachability_rows.append(diagnostics)
    nomination_matrix = np.vstack(nominations)
    nomination_array_sha256 = _array_sha256(
        nomination_matrix, np.dtype(np.int64)
    )
    nomination_sha256 = hashlib.sha256(
        (
            "borsuk-v92-wave2-nominations-v1:"
            + sketch_sha256
            + row_min_sha256
            + nomination_array_sha256
        ).encode("ascii")
    ).hexdigest()
    reachable = sum(
        int(row["nominated_missed_truth_rows"]) for row in reachability_rows
    )
    reachability = {
        "minimum_nominated_missed_truth_rows": minimum_reachable_missed_truth,
        "nominated_missed_truth_rows": reachable,
        "passed": reachable >= minimum_reachable_missed_truth,
        "queries": reachability_rows,
    }
    result: dict[str, Any] = {
        "actual_s3_requests": 0,
        "arms": {},
        "changed_parameter": "wave2_page_evidence",
        "claim_eligible": False,
        "decision": {
            "accepted": None,
            "reason": (
                "reachability-floor-passed"
                if reachability["passed"]
                else "reachability-floor-failed"
            ),
        },
        "delta_representation": "production-sq8",
        "reachability": reachability,
        "row_min_scores_sha256": row_min_sha256,
        "schema": "borsuk-v92-wave2-nominated-rows-screen-v1",
        "sketch_sha256": sketch_sha256,
        "wave2_nomination_array_sha256": nomination_array_sha256,
        "wave2_nomination_sha256": nomination_sha256,
    }
    if not reachability["passed"]:
        return result
    control_result = evaluate_coarse_to_fine(base, delta, queries, **common)
    for query_index, sample in enumerate(control_result["samples"]):
        if sample["wave1_pages"] != [
            int(page) for page in planned_wave1_pages[query_index]
        ]:
            raise ValueError("V92 wave-one replay differs")
    arms = result["arms"]
    arms["control"] = _arm(
        control_result,
        artifact_sha256=control_sha256,
        label="residual-pq16-row-min",
        code_page_bytes=code_page_bytes,
        data_page_bytes=data_page_bytes,
        wave1_max_span_pages=wave1_max_span_pages,
        wave1_max_ranges=wave1_max_ranges,
        wave2_max_span_pages=wave2_max_span_pages,
        wave2_max_ranges=wave2_max_ranges,
    )
    challenger_result = evaluate_coarse_to_fine(
        base,
        delta,
        queries,
        wave2_nominated_positions_by_query=nomination_matrix,
        wave2_nomination_sha256=nomination_sha256,
        **common,
    )
    arms["challenger"] = _arm(
        challenger_result,
        artifact_sha256=control_sha256,
        label="residual-pq16-row-min-plus-unselected-row-nominations",
        code_page_bytes=code_page_bytes,
        data_page_bytes=data_page_bytes,
        wave1_max_span_pages=wave1_max_span_pages,
        wave1_max_ranges=wave1_max_ranges,
        wave2_max_span_pages=wave2_max_span_pages,
        wave2_max_ranges=wave2_max_ranges,
    )
    return result


def build_run_metadata() -> dict[str, Any]:
    """Return the fixed V92 authority and bounded serving projection."""

    metadata = _v90_run_metadata()
    metadata["configuration"].update(
        {
            "blas_threads": 16,
            "minimum_reachable_missed_truth": _MINIMUM_REACHABLE_MISSED_TRUTH,
            "nomination_rows": _NOMINATION_ROWS,
            "query_parallelism": 1,
            "rayon_work_stealing": False,
            "worker_model": "fixed-16-thread-blas",
        }
    )
    metadata["projection_100m"].update(
        {
            "added_resident_bytes": 0,
            "nomination_scratch_bytes_per_query": (
                _WAVE1_RANK_PAGES * _PAGE_ROWS * 4
            ),
        }
    )
    metadata["scope"] = "fixed-1m-burned-development-wave2-nomination-falsifier"
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
    worst = min(int(sample["page_sq8_hits"]) for sample in samples)
    return total, base, query_15, worst


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the fixed V92 wave-two row-nomination falsifier."
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
        _scalar(
            args.ground_truth,
            "feature_row_id",
            _LOADED_QUERIES * _NEIGHBORS,
        ),
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
        raise ValueError("V92 vector dimensions differ")
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
    if control_artifact.digest() != _REGISTERED_CONTROL_ARTIFACT_SHA256:
        raise ValueError("V92 registered control artifact differs")
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
        raise ValueError("V92 registered residual sketch differs")
    result = evaluate_wave2_nominated_pair(
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
        nomination_rows=_NOMINATION_ROWS,
        minimum_reachable_missed_truth=_MINIMUM_REACHABLE_MISSED_TRUTH,
    )
    for arm in result["arms"].values():
        arm["gate"] = summarize_development_arm(
            arm["result"], neighbors=_NEIGHBORS
        )
    control_hits = _arm_hits(result["arms"]["control"]["result"])
    if control_hits != (3_167, 2_846, 90, 90):
        raise ValueError("V92 registered V90 control differs")
    if "challenger" in result["arms"]:
        challenger_hits = _arm_hits(result["arms"]["challenger"]["result"])
        control_samples = result["arms"]["control"]["result"]["samples"]
        challenger_samples = result["arms"]["challenger"]["result"]["samples"]
        if len(control_samples) != len(challenger_samples):
            raise ValueError("V92 challenger samples differ")
        no_large_regression = all(
            int(challenger_samples[index]["page_sq8_hits"])
            >= int(control_samples[index]["page_sq8_hits"]) - 1
            for index in range(len(control_samples))
        )
        passed = (
            challenger_hits[0] >= 3_176
            and challenger_hits[1] >= 2_854
            and challenger_hits[2] >= 90
            and challenger_hits[3] >= 90
            and no_large_regression
        )
        result["decision"] = {
            "accepted": "challenger" if passed else None,
            "reason": "registered-gate-passed" if passed else "registered-gate-failed",
        }
    result["registered_gate"] = {
        "base_hits": 2_854,
        "minimum_reachable_missed_truth": _MINIMUM_REACHABLE_MISSED_TRUTH,
        "query_15_hits": 90,
        "total_hits": 3_176,
        "worst_query_hits": 90,
    }
    result.update(build_run_metadata())
    body = json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n"
    args.output.write_text(body)
    print(body, end="")


if __name__ == "__main__":
    main()
