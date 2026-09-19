#!/usr/bin/env python3
"""Protected wave-two rescue falsifier over the registered V90 artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from typing import Any

import numpy as np

from scripts.v85_shared_overlay_screen import (
    _adc_scores,
    _fixed_list,
    _page_payload_bytes,
    _scalar,
)
from scripts.v86_coarse_to_fine_screen import (
    _CODE_PAGE_HEADER_BYTES,
    CoarseToFineArtifact,
    _contiguous_page_ranges,
    _page_rows,
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
    _control_page_scores,
    _rank_scores_from_candidate_estimates,
    _residual_candidate_row_estimates,
    _validate_sketch,
    build_residual_row_sketch,
)
from scripts.v90_residual_row_sketch_screen import (
    build_run_metadata as _v90_run_metadata,
)
from scripts.v92_wave2_nominated_rows_screen import nominate_unselected_rows

_NOMINATION_ROWS = 256
_RESCUE_PAGE_CAP = 24
_EXPANDED_WAVE2_MAX_PAGES = _WAVE2_MAX_SPAN_PAGES + _RESCUE_PAGE_CAP
_EXPANDED_WAVE2_MAX_RANGES = _WAVE2_MAX_RANGES + _RESCUE_PAGE_CAP
_REGISTERED_RESIDUAL_SKETCH_SHA256 = (
    "21aae14867749ac8dfed26043f8cb225024d4928bb81db32d488dd0c23a5a3bd"
)


def rank_rescue_pages(
    ordered_positions: np.ndarray,
    *,
    baseline_pages: np.ndarray,
    page_rows: int,
    page_count: int,
    take_rows: int,
    max_pages: int,
) -> np.ndarray:
    """Aggregate a frozen row ranking into bounded pages outside the baseline."""

    positions = np.asarray(ordered_positions, dtype=np.int64)
    baseline = np.asarray(baseline_pages, dtype=np.int64)
    if (
        positions.ndim != 1
        or baseline.ndim != 1
        or np.unique(positions).size != positions.size
        or np.unique(baseline).size != baseline.size
        or np.any(positions < 0)
        or np.any(positions >= page_count * page_rows)
        or np.any(baseline < 0)
        or np.any(baseline >= page_count)
        or page_rows <= 0
        or page_count <= 0
        or take_rows <= 0
        or max_pages <= 0
    ):
        raise ValueError("V93 rescue ranking differs")
    weights: dict[int, int] = {}
    baseline_set = {int(page) for page in baseline}
    for rank, position in enumerate(positions[:take_rows]):
        page = int(position // page_rows)
        if page not in baseline_set:
            weights[page] = weights.get(page, 0) + 1_000_000_000 // (rank + 1)
    ordered = sorted(weights, key=lambda page: (-weights[page], page))[:max_pages]
    return np.asarray(ordered, dtype=np.int64)


def code_rescore_rescue_pages(
    query: np.ndarray,
    artifact: CoarseToFineArtifact,
    *,
    wave1_pages: np.ndarray,
    baseline_wave2_pages: np.ndarray,
    nominated_rescue_pages: np.ndarray,
    rows: int,
    page_rows: int,
    top_rows: int,
    max_pages: int,
) -> np.ndarray:
    """Use PQ192 code pages to filter PQ16-nominated rescue pages."""

    query = np.asarray(query, dtype=np.float32)
    wave1_pages = np.asarray(wave1_pages, dtype=np.int64)
    baseline_wave2_pages = np.asarray(baseline_wave2_pages, dtype=np.int64)
    nominated_rescue_pages = np.asarray(nominated_rescue_pages, dtype=np.int64)
    page_count = (rows + page_rows - 1) // page_rows
    if (
        query.shape != (sum(book.shape[1] for book in artifact.books),)
        or not np.isfinite(query).all()
        or wave1_pages.ndim != 1
        or baseline_wave2_pages.ndim != 1
        or nominated_rescue_pages.ndim != 1
        or np.unique(wave1_pages).size != wave1_pages.size
        or np.unique(baseline_wave2_pages).size != baseline_wave2_pages.size
        or np.unique(nominated_rescue_pages).size != nominated_rescue_pages.size
        or np.any(wave1_pages < 0)
        or np.any(wave1_pages >= page_count)
        or np.any(nominated_rescue_pages < 0)
        or np.any(nominated_rescue_pages >= page_count)
        or np.intersect1d(wave1_pages, nominated_rescue_pages).size
        or rows <= 0
        or page_rows <= 0
        or top_rows <= 0
        or max_pages <= 0
    ):
        raise ValueError("V93 code rescue differs")
    wave1_positions = _page_rows(wave1_pages, rows=rows, page_rows=page_rows)
    rescue_positions = _page_rows(
        nominated_rescue_pages, rows=rows, page_rows=page_rows
    )
    positions = np.concatenate((wave1_positions, rescue_positions))
    scores = _adc_scores(query, artifact.row_codes[positions], list(artifact.books))
    take = min(top_rows, positions.size)
    order = np.lexsort((positions, scores))[:take]
    nominated_set = {int(page) for page in nominated_rescue_pages}
    baseline_set = {int(page) for page in baseline_wave2_pages}
    weights: dict[int, int] = {}
    for rank, position in enumerate(positions[order]):
        page = int(position // page_rows)
        if page in nominated_set and page not in baseline_set:
            weights[page] = weights.get(page, 0) + 1_000_000_000 // (rank + 1)
    ordered = sorted(weights, key=lambda page: (-weights[page], page))[:max_pages]
    return np.asarray(ordered, dtype=np.int64)


def _rescue_digest(pages: np.ndarray) -> str:
    return hashlib.sha256(
        np.ascontiguousarray(pages, dtype="<i8").tobytes()
    ).hexdigest()


def _record_rescue_code_budgets(
    samples: list[dict[str, Any]],
    rescue_code_pages_by_query: list[np.ndarray] | None,
    *,
    code_page_bytes: int,
) -> dict[str, int]:
    rescue_rows = (
        [np.empty(0, dtype=np.int64) for _ in samples]
        if rescue_code_pages_by_query is None
        else rescue_code_pages_by_query
    )
    if len(rescue_rows) != len(samples) or code_page_bytes <= 0:
        raise ValueError("V93 rescue code budget differs")
    maxima = {
        "max_rescue_code_bytes": 0,
        "max_rescue_code_pages": 0,
        "max_rescue_code_ranges": 0,
        "max_total_bytes": 0,
        "max_total_requests": 0,
    }
    for query_index, sample in enumerate(samples):
        pages = np.asarray(rescue_rows[query_index], dtype=np.int64)
        if (
            pages.ndim != 1
            or np.unique(pages).size != pages.size
            or np.any(pages < 0)
            or set(int(page) for page in pages).intersection(sample["wave1_pages"])
        ):
            raise ValueError("V93 rescue code budget differs")
        pages = np.sort(pages)
        ranges = _contiguous_page_ranges(pages) if pages.size else []
        rescue_bytes = int(pages.size * code_page_bytes)
        rescue_gets = len(ranges)
        if rescue_code_pages_by_query is not None:
            sample.update(
                {
                    "rescue_code_bytes": rescue_bytes,
                    "rescue_code_gets": rescue_gets,
                    "rescue_code_pages": [int(page) for page in pages],
                    "rescue_code_ranges": [list(pair) for pair in ranges],
                }
            )
        maxima["max_rescue_code_bytes"] = max(
            maxima["max_rescue_code_bytes"], rescue_bytes
        )
        maxima["max_rescue_code_pages"] = max(
            maxima["max_rescue_code_pages"], int(pages.size)
        )
        maxima["max_rescue_code_ranges"] = max(
            maxima["max_rescue_code_ranges"], rescue_gets
        )
        maxima["max_total_bytes"] = max(
            maxima["max_total_bytes"],
            int(sample["wave1_bytes"])
            + rescue_bytes
            + int(sample["wave2_bytes"]),
        )
        maxima["max_total_requests"] = max(
            maxima["max_total_requests"],
            int(sample["wave1_gets"]) + rescue_gets + int(sample["wave2_gets"]),
        )
    return maxima


def _arm(
    result: dict[str, Any],
    *,
    artifact_sha256: str,
    label: str,
    code_page_bytes: int,
    data_page_bytes: int,
    rescue_code_pages_by_query: list[np.ndarray] | None = None,
) -> dict[str, Any]:
    budgets = validate_result_budgets(
        result,
        wave1_page_bytes=code_page_bytes,
        wave1_max_pages=_WAVE1_MAX_SPAN_PAGES,
        wave1_max_ranges=_WAVE1_MAX_RANGES,
        wave2_page_bytes=data_page_bytes,
        wave2_max_pages=_EXPANDED_WAVE2_MAX_PAGES,
        wave2_max_ranges=_EXPANDED_WAVE2_MAX_RANGES,
    )
    budgets.update(
        _record_rescue_code_budgets(
            result["samples"],
            rescue_code_pages_by_query,
            code_page_bytes=code_page_bytes,
        )
    )
    return {
        "artifact_sha256": artifact_sha256,
        "budgets": budgets,
        "page_evidence": label,
        "rank_cut_base_truth_hits": _rank_cut_attribution(result),
        "result": result,
        "wave1_attribution": summarize_wave1_attribution(result),
    }


def evaluate_protected_rescue_arms(
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
) -> dict[str, Any]:
    """Build shared evidence once and evaluate three protected rescue policies."""

    base = np.asarray(base, dtype=np.float32)
    queries = np.asarray(queries, dtype=np.float32)
    _validate_sketch(sketch_artifact, rows=base.shape[0], dimensions=base.shape[1])
    control_sha256 = control_artifact.digest()
    sketch_sha256 = sketch_artifact.digest()
    if (
        sketch_artifact.base_vectors_sha256
        != _array_sha256(base, np.dtype(np.float32))
        or sketch_artifact.control_artifact_sha256 != control_sha256
        or sketch_artifact.page_rows != _PAGE_ROWS
    ):
        raise ValueError("V93 authority differs")
    page_count = (base.shape[0] + _PAGE_ROWS - 1) // _PAGE_ROWS
    evidence: list[tuple[np.ndarray, np.ndarray, np.ndarray]] = []
    row_min_rows: list[np.ndarray] = []
    for query in queries:
        control_scores = _control_page_scores(query, control_artifact, page_count)
        candidates, estimates, valid = _residual_candidate_row_estimates(
            query,
            control_scores,
            control_artifact,
            sketch_artifact,
            rank_limit=_WAVE1_RANK_PAGES,
            validated_control_sha256=control_sha256,
        )
        row_min_rows.append(
            _rank_scores_from_candidate_estimates(
                candidates, estimates, control_scores, page_rows=_PAGE_ROWS
            )
        )
        evidence.append((candidates, estimates, valid))
    row_min_scores = np.vstack(row_min_rows)
    row_min_sha256 = _array_sha256(row_min_scores, np.dtype(np.float32))
    page_evidence_sha256 = hashlib.sha256(
        ("borsuk-v93-v90-row-min-v1:" + sketch_sha256 + row_min_sha256).encode(
            "ascii"
        )
    ).hexdigest()
    code_page_bytes = _PAGE_ROWS * _CONTROL_SUBSPACES + _CODE_PAGE_HEADER_BYTES
    data_page_bytes = _page_payload_bytes(
        dimensions=base.shape[1], page_rows=_PAGE_ROWS
    )
    common = {
        "artifact": control_artifact,
        "base_ids": base_ids,
        "delta_ids": delta_ids,
        "truth_ids": truth_ids,
        "query_ordinals": query_ordinals,
        "page_rows": _PAGE_ROWS,
        "neighbors": _NEIGHBORS,
        "subspaces": _CONTROL_SUBSPACES,
        "clusters": _CLUSTERS,
        "wave1_rank_pages": _WAVE1_RANK_PAGES,
        "wave1_max_span_pages": _WAVE1_MAX_SPAN_PAGES,
        "wave1_max_ranges": _WAVE1_MAX_RANGES,
        "wave2_top_rows": _WAVE2_TOP_ROWS,
        "wave2_max_span_pages": _WAVE2_MAX_SPAN_PAGES,
        "wave2_max_ranges": _WAVE2_MAX_RANGES,
        "code_page_bytes": code_page_bytes,
        "data_page_bytes": data_page_bytes,
        "wave1_page_scores_by_query": row_min_scores,
        "wave1_page_evidence_sha256": page_evidence_sha256,
    }
    control_result = evaluate_coarse_to_fine(base, delta, queries, **common)
    rescue_rows: dict[str, list[np.ndarray]] = {
        "budget_only": [],
        "direct_rescue": [],
        "code_rescue": [],
    }
    for query_index, query in enumerate(queries):
        sample = control_result["samples"][query_index]
        wave1_pages = np.asarray(sample["wave1_pages"], dtype=np.int64)
        baseline_wave2_pages = np.asarray(sample["wave2_pages"], dtype=np.int64)
        wave1_positions = _page_rows(
            wave1_pages, rows=base.shape[0], page_rows=_PAGE_ROWS
        )
        wave1_scores = _adc_scores(
            query,
            control_artifact.row_codes[wave1_positions],
            list(control_artifact.books),
        )
        shortlist_order = np.lexsort((wave1_positions, wave1_scores))[
            : min(_WAVE2_TOP_ROWS, wave1_positions.size)
        ]
        pq_shortlist = wave1_positions[shortlist_order]
        budget_pages = rank_rescue_pages(
            pq_shortlist,
            baseline_pages=baseline_wave2_pages,
            page_rows=_PAGE_ROWS,
            page_count=page_count,
            take_rows=_WAVE2_TOP_ROWS,
            max_pages=_RESCUE_PAGE_CAP,
        )
        candidates, estimates, valid = evidence[query_index]
        nominations = nominate_unselected_rows(
            candidates,
            estimates,
            valid,
            selected_pages=wave1_pages,
            page_rows=_PAGE_ROWS,
            rows=base.shape[0],
            take=_NOMINATION_ROWS,
        )
        direct_pages = rank_rescue_pages(
            nominations,
            baseline_pages=baseline_wave2_pages,
            page_rows=_PAGE_ROWS,
            page_count=page_count,
            take_rows=_NOMINATION_ROWS,
            max_pages=_RESCUE_PAGE_CAP,
        )
        code_pages = code_rescore_rescue_pages(
            query,
            control_artifact,
            wave1_pages=wave1_pages,
            baseline_wave2_pages=baseline_wave2_pages,
            nominated_rescue_pages=direct_pages,
            rows=base.shape[0],
            page_rows=_PAGE_ROWS,
            top_rows=_WAVE2_TOP_ROWS,
            max_pages=_RESCUE_PAGE_CAP,
        )
        for name, pages in (
            ("budget_only", budget_pages),
            ("direct_rescue", direct_pages),
            ("code_rescue", code_pages),
        ):
            if (
                name != "code_rescue"
                and pages.size != _RESCUE_PAGE_CAP
            ) or (
                name == "code_rescue"
                and not 0 <= pages.size <= _RESCUE_PAGE_CAP
            ):
                raise ValueError("V93 rescue page authority differs")
            rescue_rows[name].append(pages)
    arms: dict[str, dict[str, Any]] = {
        "control": _arm(
            control_result,
            artifact_sha256=control_sha256,
            label="protected-v90-control",
            code_page_bytes=code_page_bytes,
            data_page_bytes=data_page_bytes,
        )
    }
    rescue_digests: dict[str, str] = {}
    for name, label in (
        ("budget_only", "protected-pq192-budget-only"),
        ("direct_rescue", "protected-pq16-direct-rescue"),
        ("code_rescue", "protected-pq16-to-pq192-code-rescue"),
    ):
        pages = np.full(
            (queries.shape[0], _RESCUE_PAGE_CAP), -1, dtype=np.int64
        )
        for query_index, row in enumerate(rescue_rows[name]):
            pages[query_index, : row.size] = row
        digest = _rescue_digest(pages)
        arm_result = evaluate_coarse_to_fine(
            base,
            delta,
            queries,
            wave2_rescue_pages_by_query=pages,
            wave2_rescue_sha256=digest,
            **common,
        )
        for query_index, sample in enumerate(arm_result["samples"]):
            baseline = set(control_result["samples"][query_index]["wave2_pages"])
            if not baseline.issubset(sample["wave2_pages"]):
                raise ValueError("V93 baseline protection differs")
        arms[name] = _arm(
            arm_result,
            artifact_sha256=control_sha256,
            label=label,
            code_page_bytes=code_page_bytes,
            data_page_bytes=data_page_bytes,
            rescue_code_pages_by_query=(
                rescue_rows["direct_rescue"] if name == "code_rescue" else None
            ),
        )
        rescue_digests[name] = digest
    return {
        "actual_s3_requests": 0,
        "arms": arms,
        "changed_parameter": "protected_wave2_rescue_policy",
        "claim_eligible": False,
        "delta_representation": "production-sq8",
        "rescue_sha256": rescue_digests,
        "row_min_scores_sha256": row_min_sha256,
        "schema": "borsuk-v93-protected-rescue-screen-v1",
        "sketch_sha256": sketch_sha256,
    }


def build_run_metadata() -> dict[str, Any]:
    metadata = _v90_run_metadata()
    code_page_bytes = _PAGE_ROWS * _CONTROL_SUBSPACES + _CODE_PAGE_HEADER_BYTES
    data_page_bytes = _page_payload_bytes(
        dimensions=_DIMENSIONS, page_rows=_PAGE_ROWS
    )
    rescue_code_bytes = _RESCUE_PAGE_CAP * code_page_bytes
    metadata["configuration"].update(
        {
            "blas_threads": 16,
            "nomination_rows": _NOMINATION_ROWS,
            "query_parallelism": 1,
            "rayon_work_stealing": False,
            "rescue_page_cap": _RESCUE_PAGE_CAP,
            "wave2_max_bytes": _EXPANDED_WAVE2_MAX_PAGES * data_page_bytes,
            "wave2_max_pages": _EXPANDED_WAVE2_MAX_PAGES,
            "wave2_max_ranges": _EXPANDED_WAVE2_MAX_RANGES,
            "whole_query_max_bytes": (
                metadata["configuration"]["wave1_max_bytes"]
                + rescue_code_bytes
                + _EXPANDED_WAVE2_MAX_PAGES * data_page_bytes
            ),
            "whole_query_max_requests": (
                _WAVE1_MAX_RANGES
                + _RESCUE_PAGE_CAP
                + _EXPANDED_WAVE2_MAX_RANGES
            ),
            "worker_model": "fixed-16-thread-blas",
        }
    )
    metadata["projection_100m"].update(
        {
            "added_resident_bytes": 0,
            "maximum_rescue_code_page_bytes": rescue_code_bytes,
            "maximum_rescue_data_page_bytes": _RESCUE_PAGE_CAP * data_page_bytes,
            "serving_cpu_qualified": False,
            "serving_latency_qualified": False,
            "serving_memory_qualified": False,
        }
    )
    metadata["scope"] = "fixed-1m-burned-development-protected-rescue-falsifier"
    return metadata


def _hits(result: dict[str, Any]) -> tuple[int, int, int, int]:
    samples = result["samples"]
    return (
        sum(int(sample["page_sq8_hits"]) for sample in samples),
        sum(int(sample["page_sq8_base_hits"]) for sample in samples),
        next(
            int(sample["page_sq8_hits"])
            for sample in samples
            if int(sample["query"]) == 15
        ),
        min(int(sample["page_sq8_hits"]) for sample in samples),
    )


def finalize_decision(result: dict[str, Any]) -> None:
    for arm in result["arms"].values():
        arm["gate"] = summarize_development_arm(arm["result"], neighbors=_NEIGHBORS)
    control = result["arms"]["control"]["result"]
    control_hits = _hits(control)
    if control_hits != (3_167, 2_846, 90, 90):
        raise ValueError("V93 registered control differs")
    if sum(
        int(sample["wave2_base_truth_page_hits"])
        + int(sample["delta_truth_hits"])
        for sample in result["arms"]["direct_rescue"]["result"]["samples"]
    ) != 3_183:
        raise ValueError("V93 registered counterfactual differs")
    candidates = []
    for name in ("budget_only", "code_rescue", "direct_rescue"):
        arm_result = result["arms"][name]["result"]
        hits = _hits(arm_result)
        no_large_regression = all(
            int(arm_result["samples"][index]["page_sq8_hits"])
            >= int(control["samples"][index]["page_sq8_hits"]) - 1
            for index in range(len(control["samples"]))
        )
        passed = (
            hits[0] >= 3_176
            and hits[1] >= 2_854
            and hits[2] >= 90
            and hits[3] >= 90
            and no_large_regression
        )
        if passed:
            candidates.append(name)
    accepted = next(
        (name for name in ("budget_only", "code_rescue", "direct_rescue") if name in candidates),
        None,
    )
    result["decision"] = {
        "accepted": accepted,
        "reason": "registered-gate-passed" if accepted else "registered-gate-failed",
    }
    result["registered_gate"] = {
        "base_hits": 2_854,
        "control_hits": 3_167,
        "direct_candidate_truth_hits": 3_183,
        "maximum_query_regression": 1,
        "query_15_hits": 90,
        "total_hits": 3_176,
        "worst_query_hits": 90,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the fixed V93 protected rescue falsifier.")
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
        raise ValueError("V93 vector dimensions differ")
    queries, truth, query_ordinals = select_development_rows(
        loaded_queries, loaded_truth, development_queries=_DEVELOPMENT_QUERIES
    )
    base = np.ascontiguousarray(source[base_order])
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
        raise ValueError("V93 registered control artifact differs")
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
        raise ValueError("V93 registered residual sketch differs")
    result = evaluate_protected_rescue_arms(
        base,
        np.ascontiguousarray(source[_BASE_ROWS:]),
        queries,
        control_artifact=control_artifact,
        sketch_artifact=sketch_artifact,
        base_ids=np.ascontiguousarray(source_ids[base_order]),
        delta_ids=source_ids[_BASE_ROWS:],
        truth_ids=truth,
        query_ordinals=query_ordinals,
    )
    finalize_decision(result)
    result.update(build_run_metadata())
    body = json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n"
    args.output.write_text(body)
    print(body, end="")


if __name__ == "__main__":
    main()
