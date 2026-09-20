#!/usr/bin/env python3
"""Untuned confirmation of the frozen V93 direct-rescue policy."""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

import numpy as np

import scripts.v93_protected_rescue_screen as v93
from scripts.v85_shared_overlay_screen import _fixed_list, _scalar
from scripts.v86_coarse_to_fine_screen import (
    build_coarse_to_fine_artifact,
    validate_layout_order,
    validate_truth_rows,
)
from scripts.v87_summary_capacity_screen import _REGISTERED_CONTROL_ARTIFACT_SHA256
from scripts.v90_residual_row_sketch_screen import (
    _BASE_ROWS,
    _CLUSTERS,
    _CONTROL_SEED,
    _CONTROL_SUBSPACES,
    _DIMENSIONS,
    _NEIGHBORS,
    _PAGE_ROWS,
    _ROWS,
    _SKETCH_SEED,
    _SKETCH_SUBSPACES,
    _TRAINING_ITERATIONS,
    _TRAINING_SAMPLE_ROWS,
    build_residual_row_sketch,
)

_CONFIRMATION_START = 328
_CONFIRMATION_QUERIES = 128
_LOADED_QUERIES = _CONFIRMATION_START + _CONFIRMATION_QUERIES


def select_confirmation_rows(
    queries: np.ndarray,
    truth_ids: np.ndarray,
    *,
    confirmation_start: int,
    confirmation_queries: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Select only the preregistered untouched confirmation interval."""

    queries = np.asarray(queries, dtype=np.float32)
    truth_ids = np.asarray(truth_ids, dtype=np.int64)
    end = confirmation_start + confirmation_queries
    if (
        queries.ndim != 2
        or truth_ids.ndim != 2
        or queries.shape[0] != truth_ids.shape[0]
        or confirmation_start < 0
        or confirmation_queries <= 0
        or end > queries.shape[0]
    ):
        raise ValueError("V94 confirmation selection differs")
    ordinals = np.arange(confirmation_start, end, dtype=np.int64)
    return (
        np.ascontiguousarray(queries[ordinals]),
        np.ascontiguousarray(truth_ids[ordinals]),
        ordinals,
    )


def summarize_confirmation_arm(
    result: dict[str, Any], *, neighbors: int
) -> dict[str, Any]:
    """Recompute the frozen 128-query confirmation gate."""

    samples = result.get("samples")
    expected = list(range(_CONFIRMATION_START, _LOADED_QUERIES))
    if (
        not isinstance(samples, list)
        or len(samples) != _CONFIRMATION_QUERIES
        or neighbors <= 0
        or [sample.get("query") for sample in samples] != expected
    ):
        raise ValueError("V94 confirmation samples differ")
    hits = [int(sample["page_sq8_hits"]) for sample in samples]
    base_hits = sum(int(sample["page_sq8_base_hits"]) for sample in samples)
    base_truth = sum(int(sample["base_truth_hits"]) for sample in samples)
    if (
        any(not 0 <= value <= neighbors for value in hits)
        or base_truth <= 0
        or not 0 <= base_hits <= base_truth
    ):
        raise ValueError("V94 confirmation evidence differs")
    total = sum(hits)
    summary = {
        "base_recall_ppm": round(base_hits * 1_000_000 / base_truth),
        "hits": total,
        "recall_ppm": round(total * 1_000_000 / (len(samples) * neighbors)),
        "worst_query_hits": min(hits),
    }
    summary["passed"] = bool(
        total >= 12_685
        and summary["worst_query_hits"] >= 85
        and summary["base_recall_ppm"] >= 991_000
    )
    return summary


def evaluate_confirmation_arms(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Evaluate only the control and frozen V93 direct-rescue arm."""

    result = v93.evaluate_protected_rescue_arms(
        *args, policies=("direct_rescue",), **kwargs
    )
    result["changed_parameter"] = "none-frozen-v93-direct-rescue"
    result["schema"] = "borsuk-v94-protected-rescue-confirmation-v1"
    return result


def finalize_confirmation(result: dict[str, Any]) -> None:
    """Bind absolute confirmation gates and paired non-regression."""

    control = result["arms"]["control"]["result"]
    direct = result["arms"]["direct_rescue"]["result"]
    control_gate = summarize_confirmation_arm(control, neighbors=_NEIGHBORS)
    direct_gate = summarize_confirmation_arm(direct, neighbors=_NEIGHBORS)
    result["arms"]["control"]["gate"] = control_gate
    result["arms"]["direct_rescue"]["gate"] = direct_gate
    paired_non_regression = all(
        int(direct["samples"][index]["page_sq8_hits"])
        >= int(control["samples"][index]["page_sq8_hits"]) - 1
        for index in range(len(control["samples"]))
    )
    total_hit_delta = direct_gate["hits"] - control_gate["hits"]
    passed = bool(
        direct_gate["passed"] and total_hit_delta >= 4 and paired_non_regression
    )
    result["decision"] = {
        "accepted": "direct_rescue" if passed else None,
        "paired_non_regression": paired_non_regression,
        "passed": passed,
        "reason": "confirmation-passed" if passed else "confirmation-failed",
        "total_hit_delta_vs_control": total_hit_delta,
    }
    result["registered_gate"] = {
        "base_recall_ppm": 991_000,
        "confirmation_end_exclusive": _LOADED_QUERIES,
        "confirmation_start": _CONFIRMATION_START,
        "maximum_query_regression": 1,
        "minimum_total_hit_benefit": 4,
        "queries": _CONFIRMATION_QUERIES,
        "recall_hits": 12_685,
        "worst_query_hits": 85,
    }


def build_run_metadata() -> dict[str, Any]:
    """Return the frozen confirmation scope without new tunable knobs."""

    metadata = v93.build_run_metadata()
    metadata["configuration"]["queries"] = _CONFIRMATION_QUERIES
    metadata["confirmation"] = {
        "end_exclusive": _LOADED_QUERIES,
        "queries": _CONFIRMATION_QUERIES,
        "start": _CONFIRMATION_START,
        "tuning_allowed": False,
    }
    metadata["planned_max_requests_per_query"] = {
        key: metadata["planned_max_requests_per_query"][key]
        for key in ("control", "direct_rescue")
    }
    metadata["scope"] = "fixed-1m-untouched-confirmation-protected-rescue"
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Confirm the frozen V93 direct-rescue policy."
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
        raise ValueError("V94 vector dimensions differ")
    queries, truth, query_ordinals = select_confirmation_rows(
        loaded_queries,
        loaded_truth,
        confirmation_start=_CONFIRMATION_START,
        confirmation_queries=_CONFIRMATION_QUERIES,
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
        raise ValueError("V94 registered control artifact differs")
    sketch_artifact = build_residual_row_sketch(
        base,
        control_artifact,
        subspaces=_SKETCH_SUBSPACES,
        clusters=_CLUSTERS,
        sample_rows=_TRAINING_SAMPLE_ROWS,
        seed=_SKETCH_SEED,
        iterations=_TRAINING_ITERATIONS,
    )
    if sketch_artifact.digest() != v93._REGISTERED_RESIDUAL_SKETCH_SHA256:
        raise ValueError("V94 registered residual sketch differs")
    result = evaluate_confirmation_arms(
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
    finalize_confirmation(result)
    result.update(build_run_metadata())
    body = json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n"
    args.output.write_text(body)
    print(body, end="")


if __name__ == "__main__":
    main()
