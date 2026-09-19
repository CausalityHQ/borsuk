#!/usr/bin/env python3
"""Paired burned-dev falsifier for the wave-one planner objective."""

from __future__ import annotations

import argparse
import json
import pathlib
from collections.abc import Iterable
from typing import Any

import numpy as np

from scripts.v85_shared_overlay_screen import (
    _fixed_list,
    _maximum_physical_oracle_hits,
    _page_payload_bytes,
    _scalar,
)
from scripts.v86_coarse_to_fine_screen import (
    CoarseToFineArtifact,
    build_coarse_to_fine_artifact,
    evaluate_coarse_to_fine,
    validate_layout_order,
    validate_truth_rows,
)
from scripts.v87_summary_capacity_screen import (
    choose_summary_candidate,
    select_development_rows,
    summarize_development_arm,
    summarize_wave1_attribution,
    validate_control_reproduction,
)

_ROWS = 1_000_000
_BASE_ROWS = 900_000
_DEVELOPMENT_QUERIES = 32
_LOADED_QUERIES = 328
_NEIGHBORS = 100
_DIMENSIONS = 768
_PAGE_ROWS = 256
_PQ_SUBSPACES = 192
_CODE_PAGE_HEADER_BYTES = 64
_WAVE1_RANK_PAGES = 1_024
_WAVE1_MAX_SPAN_PAGES = 340
_WAVE1_MAX_RANGES = 32
_WAVE2_TOP_ROWS = 512
_WAVE2_MAX_SPAN_PAGES = 81
_WAVE2_MAX_RANGES = 32


def _expand_ranges(ranges: Iterable[Iterable[int]]) -> list[int]:
    pages: list[int] = []
    prior_stop = -2
    for pair in ranges:
        values = list(pair)
        if len(values) != 2:
            raise ValueError("V88 physical budget differs")
        first, stop = (int(value) for value in values)
        if first < 0 or stop < first or first <= prior_stop + 1:
            raise ValueError("V88 physical budget differs")
        pages.extend(range(first, stop + 1))
        prior_stop = stop
    return pages


def validate_result_budgets(
    result: dict[str, Any],
    *,
    wave1_page_bytes: int,
    wave1_max_pages: int,
    wave1_max_ranges: int,
    wave2_page_bytes: int,
    wave2_max_pages: int,
    wave2_max_ranges: int,
) -> dict[str, int]:
    """Independently reconstruct every reported range and byte count."""

    samples = result.get("samples")
    if (
        not isinstance(samples, list)
        or not samples
        or min(
            wave1_page_bytes,
            wave1_max_pages,
            wave1_max_ranges,
            wave2_page_bytes,
            wave2_max_pages,
            wave2_max_ranges,
        )
        <= 0
    ):
        raise ValueError("V88 physical budget differs")
    maxima = {
        "max_wave1_bytes": 0,
        "max_wave1_pages": 0,
        "max_wave1_ranges": 0,
        "max_wave2_bytes": 0,
        "max_wave2_pages": 0,
        "max_wave2_ranges": 0,
    }
    for sample in samples:
        for prefix, page_bytes, max_pages, max_ranges in (
            ("wave1", wave1_page_bytes, wave1_max_pages, wave1_max_ranges),
            ("wave2", wave2_page_bytes, wave2_max_pages, wave2_max_ranges),
        ):
            ranges = sample[f"{prefix}_ranges"]
            pages = sample[f"{prefix}_pages"]
            expanded = _expand_ranges(ranges)
            reported_bytes = int(sample[f"{prefix}_bytes"])
            reported_gets = int(sample[f"{prefix}_gets"])
            if (
                pages != expanded
                or reported_gets != len(ranges)
                or reported_bytes != len(pages) * page_bytes
                or len(pages) > max_pages
                or len(ranges) > max_ranges
            ):
                raise ValueError("V88 physical budget differs")
            maxima[f"max_{prefix}_bytes"] = max(
                maxima[f"max_{prefix}_bytes"], reported_bytes
            )
            maxima[f"max_{prefix}_pages"] = max(
                maxima[f"max_{prefix}_pages"], len(pages)
            )
            maxima[f"max_{prefix}_ranges"] = max(
                maxima[f"max_{prefix}_ranges"], len(ranges)
            )
    return maxima


def _truth_oracle(result: dict[str, Any], *, page_count: int) -> dict[str, int]:
    per_query = []
    for sample in result["samples"]:
        pages = np.asarray(sample["truth_base_pages"], dtype=np.int64)
        unique, counts = np.unique(pages, return_counts=True)
        hits = _maximum_physical_oracle_hits(
            {
                int(page): int(counts[index])
                for index, page in enumerate(unique)
            },
            page_count=page_count,
            max_pages=_WAVE1_MAX_SPAN_PAGES,
            max_ranges=_WAVE1_MAX_RANGES,
        )
        per_query.append(int(hits))
    return {"base_truth_hits": int(sum(per_query)), "worst_query_hits": min(per_query)}


def _rank_cut_attribution(result: dict[str, Any]) -> dict[str, int]:
    ranks = [
        int(rank)
        for sample in result["samples"]
        for rank in sample["truth_page_ranks"]
    ]
    return {
        str(cut): sum(rank < cut for rank in ranks)
        for cut in (100, 256, 340, 512, 1_024)
    }


def evaluate_planner_objective_pair(
    base: np.ndarray,
    delta: np.ndarray,
    queries: np.ndarray,
    *,
    artifact: CoarseToFineArtifact,
    base_ids: np.ndarray,
    delta_ids: np.ndarray,
    truth_ids: np.ndarray,
    query_ordinals: np.ndarray,
    page_rows: int = _PAGE_ROWS,
    neighbors: int = _NEIGHBORS,
    subspaces: int = _PQ_SUBSPACES,
    clusters: int = 256,
    wave1_rank_pages: int = _WAVE1_RANK_PAGES,
    wave1_max_span_pages: int = _WAVE1_MAX_SPAN_PAGES,
    wave1_max_ranges: int = _WAVE1_MAX_RANGES,
    wave2_top_rows: int = _WAVE2_TOP_ROWS,
    wave2_max_span_pages: int = _WAVE2_MAX_SPAN_PAGES,
    wave2_max_ranges: int = _WAVE2_MAX_RANGES,
    code_page_bytes: int | None = None,
    data_page_bytes: int | None = None,
) -> dict[str, Any]:
    """Evaluate reciprocal-rank and coverage-first planning over one artifact."""

    if code_page_bytes is None:
        code_page_bytes = page_rows * subspaces + _CODE_PAGE_HEADER_BYTES
    if data_page_bytes is None:
        data_page_bytes = _page_payload_bytes(
            dimensions=base.shape[1], page_rows=page_rows
        )
    common = {
        "artifact": artifact,
        "base_ids": base_ids,
        "delta_ids": delta_ids,
        "truth_ids": truth_ids,
        "query_ordinals": query_ordinals,
        "page_rows": page_rows,
        "neighbors": neighbors,
        "subspaces": subspaces,
        "clusters": clusters,
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
    for name, objective in (
        ("control", "reciprocal-rank"),
        ("challenger", "coverage-first"),
    ):
        result = evaluate_coarse_to_fine(
            base,
            delta,
            queries,
            wave1_objective=objective,
            **common,
        )
        if result["artifact_sha256"] != artifact.digest():
            raise ValueError("V88 result artifact binding differs")
        arms[name] = {
            "artifact_sha256": artifact.digest(),
            "budgets": validate_result_budgets(
                result,
                wave1_page_bytes=code_page_bytes,
                wave1_max_pages=wave1_max_span_pages,
                wave1_max_ranges=wave1_max_ranges,
                wave2_page_bytes=data_page_bytes,
                wave2_max_pages=wave2_max_span_pages,
                wave2_max_ranges=wave2_max_ranges,
            ),
            "objective": objective,
            "rank_cut_base_truth_hits": _rank_cut_attribution(result),
            "result": result,
            "wave1_attribution": summarize_wave1_attribution(result),
        }
    return {
        "actual_s3_requests": 0,
        "arms": arms,
        "changed_parameter": "wave1_planner_objective",
        "claim_eligible": False,
        "delta_representation": "production-sq8",
        "schema": "borsuk-v88-planner-objective-screen-v1",
    }


def build_run_metadata() -> dict[str, Any]:
    code_page_bytes = _PAGE_ROWS * _PQ_SUBSPACES + _CODE_PAGE_HEADER_BYTES
    data_page_bytes = _page_payload_bytes(
        dimensions=_DIMENSIONS, page_rows=_PAGE_ROWS
    )
    projected_pages = 390_625
    dense_traceback_bytes = (
        2
        * projected_pages
        * (_WAVE1_MAX_RANGES + 1)
        * (_WAVE1_MAX_SPAN_PAGES + 1)
    )
    return {
        "configuration": {
            "base_rows": _BASE_ROWS,
            "development_queries": _DEVELOPMENT_QUERIES,
            "dimensions": _DIMENSIONS,
            "page_rows": _PAGE_ROWS,
            "queries": _DEVELOPMENT_QUERIES,
            "rows": _ROWS,
            "wave1_max_bytes": _WAVE1_MAX_SPAN_PAGES * code_page_bytes,
            "wave1_max_pages": _WAVE1_MAX_SPAN_PAGES,
            "wave1_max_ranges": _WAVE1_MAX_RANGES,
            "wave1_rank_pages": _WAVE1_RANK_PAGES,
            "wave2_max_bytes": _WAVE2_MAX_SPAN_PAGES * data_page_bytes,
            "wave2_max_pages": _WAVE2_MAX_SPAN_PAGES,
            "wave2_max_ranges": _WAVE2_MAX_RANGES,
        },
        "confirmation_required": {
            "queries": 128,
            "requires_new_authenticated_input": True,
            "start": 328,
        },
        "io_evidence": "planned-only-no-s3-query-requests",
        "planned_max_requests_per_query": {"challenger": 64, "control": 64},
        "projection_100m": {
            "dense_wave1_traceback_bytes_per_query": dense_traceback_bytes,
            "serving_memory_qualified": False,
            "summary_adc_lookups_per_query": projected_pages * 2 * _PQ_SUBSPACES,
            "summary_resident_bytes": projected_pages * 2 * _PQ_SUBSPACES,
        },
        "scope": "fixed-1m-burned-development-planner-objective-falsifier",
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the fixed V88 paired wave-one planner falsifier."
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
    size = _LOADED_QUERIES * _NEIGHBORS
    loaded_truth = validate_truth_rows(
        _scalar(args.ground_truth, "query_ordinal", size),
        _scalar(args.ground_truth, "rank", size),
        _scalar(args.ground_truth, "feature_row_id", size),
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
        raise ValueError("V88 vector dimensions differ")
    queries, truth, query_ordinals = select_development_rows(
        loaded_queries,
        loaded_truth,
        development_queries=_DEVELOPMENT_QUERIES,
    )
    base = np.ascontiguousarray(source[base_order])
    artifact = build_coarse_to_fine_artifact(
        base,
        page_rows=_PAGE_ROWS,
        subspaces=_PQ_SUBSPACES,
        clusters=256,
        sample_rows=100_000,
        seed=85,
        iterations=10,
    )
    result = evaluate_planner_objective_pair(
        base,
        np.ascontiguousarray(source[_BASE_ROWS:]),
        queries,
        artifact=artifact,
        base_ids=source_ids[base_order],
        delta_ids=source_ids[_BASE_ROWS:],
        truth_ids=truth,
        query_ordinals=query_ordinals,
    )
    for arm in result["arms"].values():
        arm["gate"] = summarize_development_arm(
            arm["result"], neighbors=_NEIGHBORS
        )
        arm["wave1_truth_oracle"] = _truth_oracle(
            arm["result"], page_count=(base.shape[0] + _PAGE_ROWS - 1) // _PAGE_ROWS
        )
    validate_control_reproduction(
        result["arms"]["control"]["artifact_sha256"],
        result["arms"]["control"]["gate"],
    )
    result["decision"] = choose_summary_candidate(
        result["arms"]["control"]["result"],
        result["arms"]["challenger"]["result"],
        neighbors=_NEIGHBORS,
    )
    result.update(build_run_metadata())
    body = json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n"
    args.output.write_text(body)
    print(body, end="")


if __name__ == "__main__":
    main()
