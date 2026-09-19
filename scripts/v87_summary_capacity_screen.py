#!/usr/bin/env python3
"""Paired burned-dev falsifier for V86 page-summary capacity."""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

import numpy as np

from scripts.v85_shared_overlay_screen import (
    _encode_pq,
    _fixed_list,
    _page_payload_bytes,
    _scalar,
)
from scripts.v86_coarse_to_fine_screen import (
    CoarseToFineArtifact,
    _means_per_page,
    build_coarse_to_fine_artifact,
    evaluate_coarse_to_fine,
    validate_layout_order,
    validate_truth_rows,
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
_CONTROL_SUMMARIES = 2
_CHALLENGER_SUMMARIES = 8
_WAVE1_MAX_SPAN_PAGES = 340
_WAVE2_MAX_SPAN_PAGES = 81
_REGISTERED_CONTROL_ARTIFACT_SHA256 = (
    "649c739a81fcb930afdbd15b3692357a45a3106cdb7385aa37e8ed48badd1c56"
)
_REGISTERED_CONTROL_GATE = {
    "base_recall_ppm": 985_412,
    "hits": 3_158,
    "passed": False,
    "query_15_hits": 88,
    "recall_ppm": 986_875,
    "worst_query_hits": 88,
}


def select_development_rows(
    queries: np.ndarray,
    truth_ids: np.ndarray,
    *,
    development_queries: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Select the burned prefix after authenticating the complete inputs."""

    queries = np.asarray(queries, dtype=np.float32)
    truth_ids = np.asarray(truth_ids, dtype=np.int64)
    if (
        queries.ndim != 2
        or truth_ids.ndim != 2
        or queries.shape[0] != truth_ids.shape[0]
        or not 0 < development_queries <= queries.shape[0]
    ):
        raise ValueError("V87 development selection differs")
    ordinals = np.arange(development_queries, dtype=np.int64)
    return (
        np.ascontiguousarray(queries[ordinals]),
        np.ascontiguousarray(truth_ids[ordinals]),
        ordinals,
    )


def derive_summary_artifact(
    base: np.ndarray,
    routing_artifact: CoarseToFineArtifact,
    *,
    summaries_per_page: int,
) -> CoarseToFineArtifact:
    """Reuse immutable PQ books and row codes with new block summaries."""

    base = np.asarray(base, dtype=np.float32)
    if (
        base.ndim != 2
        or base.shape[0] != routing_artifact.training_rows
        or not np.isfinite(base).all()
        or not 0 < summaries_per_page <= routing_artifact.page_rows
    ):
        raise ValueError("V87 summary artifact authority differs")
    summaries = _means_per_page(
        np.ascontiguousarray(base),
        routing_artifact.page_rows,
        summaries_per_page,
    )
    return CoarseToFineArtifact(
        books=routing_artifact.books,
        row_codes=routing_artifact.row_codes,
        summary_codes=_encode_pq(
            summaries,
            list(routing_artifact.books),
            max(routing_artifact.page_rows, 1_024),
        ),
        page_rows=routing_artifact.page_rows,
        training_rows=routing_artifact.training_rows,
    )


def _summary_count(artifact: CoarseToFineArtifact) -> int:
    page_count = (
        artifact.training_rows + artifact.page_rows - 1
    ) // artifact.page_rows
    if (
        page_count <= 0
        or artifact.summary_codes.ndim != 2
        or artifact.summary_codes.shape[0] % page_count
    ):
        raise ValueError("V87 summary count differs")
    return artifact.summary_codes.shape[0] // page_count


def summarize_development_arm(
    result: dict[str, Any], *, neighbors: int
) -> dict[str, Any]:
    """Recompute the frozen development gate from canonical samples."""

    samples = result.get("samples")
    if (
        not isinstance(samples, list)
        or len(samples) != _DEVELOPMENT_QUERIES
        or neighbors <= 0
        or [sample.get("query") for sample in samples]
        != list(range(_DEVELOPMENT_QUERIES))
    ):
        raise ValueError("V87 development samples differ")
    hits = [int(sample["page_sq8_hits"]) for sample in samples]
    base_truth = sum(int(sample["base_truth_hits"]) for sample in samples)
    base_hits = sum(int(sample["page_sq8_base_hits"]) for sample in samples)
    if (
        any(not 0 <= value <= neighbors for value in hits)
        or base_truth <= 0
        or not 0 <= base_hits <= base_truth
    ):
        raise ValueError("V87 development evidence differs")
    total = sum(hits)
    base_recall_ppm = round(base_hits * 1_000_000 / base_truth)
    return {
        "base_recall_ppm": base_recall_ppm,
        "hits": total,
        "passed": total >= 3_176
        and hits[15] >= 90
        and base_recall_ppm >= 991_000,
        "query_15_hits": hits[15],
        "recall_ppm": round(
            total * 1_000_000 / (_DEVELOPMENT_QUERIES * neighbors)
        ),
        "worst_query_hits": min(hits),
    }


def choose_summary_candidate(
    control: dict[str, Any],
    challenger: dict[str, Any],
    *,
    neighbors: int,
) -> dict[str, str | None]:
    """Apply the preregistered paired kill decision."""

    control_gate = summarize_development_arm(control, neighbors=neighbors)
    challenger_gate = summarize_development_arm(challenger, neighbors=neighbors)
    if control_gate["passed"]:
        return {"accepted": None, "reason": "control-already-passes"}
    if not challenger_gate["passed"]:
        return {"accepted": None, "reason": "challenger-failed-gate"}
    return {"accepted": "challenger", "reason": "challenger-only-passes"}


def validate_control_reproduction(
    artifact_sha256: str, gate: dict[str, Any]
) -> None:
    """Require the paired control to reproduce immutable V86 evidence."""

    if (
        artifact_sha256 != _REGISTERED_CONTROL_ARTIFACT_SHA256
        or gate != _REGISTERED_CONTROL_GATE
    ):
        raise ValueError("V87 control reproduction differs")


def summarize_wave1_attribution(result: dict[str, Any]) -> dict[str, int]:
    """Aggregate rank-versus-planner loss from per-query evidence."""

    samples = result.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ValueError("V87 wave-one attribution differs")
    names = (
        "base_truth_hits",
        "wave1_base_truth_hits",
        "wave1_gap_only_base_truth_hits",
        "wave1_planner_missed_base_truth_hits",
        "wave1_rank_visible_base_truth_hits",
        "wave1_rank_visible_selected_base_truth_hits",
    )
    totals = {name: 0 for name in names}
    for sample in samples:
        values = {name: int(sample[name]) for name in names}
        if (
            any(value < 0 for value in values.values())
            or values["wave1_rank_visible_selected_base_truth_hits"]
            + values["wave1_planner_missed_base_truth_hits"]
            != values["wave1_rank_visible_base_truth_hits"]
            or values["wave1_rank_visible_selected_base_truth_hits"]
            + values["wave1_gap_only_base_truth_hits"]
            != values["wave1_base_truth_hits"]
            or values["wave1_base_truth_hits"] > values["base_truth_hits"]
        ):
            raise ValueError("V87 wave-one attribution differs")
        for name, value in values.items():
            totals[name] += value
    return {
        "base_truth_hits": totals["base_truth_hits"],
        "gap_only_base_truth_hits": totals[
            "wave1_gap_only_base_truth_hits"
        ],
        "planner_missed_base_truth_hits": totals[
            "wave1_planner_missed_base_truth_hits"
        ],
        "rank_visible_base_truth_hits": totals[
            "wave1_rank_visible_base_truth_hits"
        ],
        "rank_visible_selected_base_truth_hits": totals[
            "wave1_rank_visible_selected_base_truth_hits"
        ],
        "wave1_base_truth_hits": totals["wave1_base_truth_hits"],
    }


def evaluate_summary_capacity_pair(
    base: np.ndarray,
    delta: np.ndarray,
    queries: np.ndarray,
    *,
    control_artifact: CoarseToFineArtifact,
    challenger_artifact: CoarseToFineArtifact,
    base_ids: np.ndarray,
    delta_ids: np.ndarray,
    truth_ids: np.ndarray,
    query_ordinals: np.ndarray,
    page_rows: int = _PAGE_ROWS,
    neighbors: int = _NEIGHBORS,
    subspaces: int = _PQ_SUBSPACES,
    clusters: int = 256,
    wave1_rank_pages: int = 1_024,
    wave1_max_span_pages: int = _WAVE1_MAX_SPAN_PAGES,
    wave1_max_ranges: int = 32,
    wave2_top_rows: int = 512,
    wave2_max_span_pages: int = _WAVE2_MAX_SPAN_PAGES,
    wave2_max_ranges: int = 32,
    code_page_bytes: int | None = None,
    data_page_bytes: int | None = None,
) -> dict[str, Any]:
    """Evaluate two summary counts over exactly one PQ routing artifact."""

    control_summaries = _summary_count(control_artifact)
    challenger_summaries = _summary_count(challenger_artifact)
    routing_digest = control_artifact.routing_digest()
    if (
        control_summaries <= 0
        or challenger_summaries <= control_summaries
        or challenger_artifact.routing_digest() != routing_digest
    ):
        raise ValueError("V87 paired artifact authority differs")
    common = {
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
    control = evaluate_coarse_to_fine(
        base, delta, queries, artifact=control_artifact, **common
    )
    challenger = evaluate_coarse_to_fine(
        base, delta, queries, artifact=challenger_artifact, **common
    )
    if (
        control["artifact_sha256"] != control_artifact.digest()
        or challenger["artifact_sha256"] != challenger_artifact.digest()
    ):
        raise ValueError("V87 result artifact binding differs")
    return {
        "actual_s3_requests": 0,
        "arms": {
            "challenger": {
                "artifact_sha256": challenger_artifact.digest(),
                "result": challenger,
                "summaries_per_page": challenger_summaries,
                "wave1_attribution": summarize_wave1_attribution(challenger),
            },
            "control": {
                "artifact_sha256": control_artifact.digest(),
                "result": control,
                "summaries_per_page": control_summaries,
                "wave1_attribution": summarize_wave1_attribution(control),
            },
        },
        "changed_parameter": "fixed_block_summaries_per_page",
        "challenger_family": "fixed-contiguous-block-means",
        "claim_eligible": False,
        "delta_representation": "production-sq8",
        "routing_sha256": routing_digest,
        "schema": "borsuk-v87-fixed-block-summary-capacity-screen-v1",
    }


def build_run_metadata() -> dict[str, Any]:
    code_page_bytes = _PAGE_ROWS * _PQ_SUBSPACES + _CODE_PAGE_HEADER_BYTES
    data_page_bytes = _page_payload_bytes(
        dimensions=_DIMENSIONS, page_rows=_PAGE_ROWS
    )
    projected_pages = 390_625
    return {
        "configuration": {
            "base_rows": _BASE_ROWS,
            "challenger_summaries_per_page": _CHALLENGER_SUMMARIES,
            "control_summaries_per_page": _CONTROL_SUMMARIES,
            "development_queries": _DEVELOPMENT_QUERIES,
            "dimensions": _DIMENSIONS,
            "page_rows": _PAGE_ROWS,
            "pq_training_runs": 1,
            "queries": _DEVELOPMENT_QUERIES,
            "rows": _ROWS,
            "wave1_max_bytes": _WAVE1_MAX_SPAN_PAGES * code_page_bytes,
            "wave1_max_pages": _WAVE1_MAX_SPAN_PAGES,
            "wave1_max_ranges": 32,
            "wave2_max_bytes": _WAVE2_MAX_SPAN_PAGES * data_page_bytes,
            "wave2_max_pages": _WAVE2_MAX_SPAN_PAGES,
            "wave2_max_ranges": 32,
        },
        "confirmation_reserved": {"end": 455, "start": 328},
        "io_evidence": "planned-only-no-s3-query-requests",
        "planned_max_requests_per_query": {"challenger": 64, "control": 64},
        "projection_100m": {
            "challenger_summary_adc_lookups_per_query": projected_pages
            * _CHALLENGER_SUMMARIES
            * _PQ_SUBSPACES,
            "challenger_summary_resident_bytes": projected_pages
            * _CHALLENGER_SUMMARIES
            * _PQ_SUBSPACES,
            "serving_cpu_qualified": False,
        },
        "promotion_requires_hierarchical_cpu_qualification": True,
        "scope": "fixed-1m-burned-development-fixed-block-summary-falsifier",
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the fixed V87 paired page-summary falsifier."
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
        raise ValueError("V87 vector dimensions differ")
    queries, truth, query_ordinals = select_development_rows(
        loaded_queries,
        loaded_truth,
        development_queries=_DEVELOPMENT_QUERIES,
    )
    base = np.ascontiguousarray(source[base_order])
    control_artifact = build_coarse_to_fine_artifact(
        base,
        page_rows=_PAGE_ROWS,
        subspaces=_PQ_SUBSPACES,
        clusters=256,
        sample_rows=100_000,
        seed=85,
        iterations=10,
        summaries_per_page=_CONTROL_SUMMARIES,
    )
    challenger_artifact = derive_summary_artifact(
        base,
        control_artifact,
        summaries_per_page=_CHALLENGER_SUMMARIES,
    )
    result = evaluate_summary_capacity_pair(
        base,
        np.ascontiguousarray(source[_BASE_ROWS:]),
        queries,
        control_artifact=control_artifact,
        challenger_artifact=challenger_artifact,
        base_ids=source_ids[base_order],
        delta_ids=source_ids[_BASE_ROWS:],
        truth_ids=truth,
        query_ordinals=query_ordinals,
    )
    for arm in result["arms"].values():
        arm["gate"] = summarize_development_arm(
            arm["result"], neighbors=_NEIGHBORS
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
