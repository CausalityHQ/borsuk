#!/usr/bin/env python3
"""Make conservative, correctness-gated storage-layout promotion decisions."""

from __future__ import annotations

import argparse
import csv
import math
import random
import statistics
from pathlib import Path
from typing import Any, Sequence

BASELINE_ARM = "fixed-parquet"
CANDIDATE_ARMS = (
    "fixed-vortex-full",
    "fixed-vortex-range",
    "mixed-vortex-full",
    "mixed-vortex-range",
)
MINIMUM_DATASETS = 2
REQUIRED_DATASETS = ("fashion-mnist-784", "glove-100")
REQUIRED_BACKENDS = ("local_disk", "s3")
MINIMUM_REPETITIONS = 5
MAX_RECALL_LOSS = 0.005
TARGET_P95_RATIO = 0.95
MAX_FAMILYWISE_CONFIDENCE_UPPER = 1.0
MAX_P99_RATIO = 1.02
MAX_REQUEST_RATIO = 1.05
MAX_BYTES_RATIO = 1.05
MAX_BUILD_RATIO = 1.10
MAX_SEGMENT_BYTES_RATIO = 1.05
MAX_INDEX_BYTES_RATIO = 1.05
MAX_PEAK_RSS_RATIO = 1.05
MAX_CPU_RATIO = 1.10
BOOTSTRAP_SAMPLES = 2_000
FAMILYWISE_ALPHA = 0.05
BOOTSTRAP_UPPER_QUANTILE = 1.0 - FAMILYWISE_ALPHA / len(CANDIDATE_ARMS)
OPERATIONAL_FIELDS = (
    "physical_requests",
    "bytes_read",
    "build_ms",
    "segment_bytes",
    "total_active_index_bytes",
    "peak_rss_bytes",
    "cpu_core_ms",
)


def percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        raise ValueError("percentile requires samples")
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _bootstrap_p95_ratio(
    baseline: Sequence[dict[str, Any]],
    candidate: Sequence[dict[str, Any]],
    *,
    seed: int,
) -> float:
    rng = random.Random(seed)
    by_repetition: dict[str, list[tuple[float, float]]] = {}
    for baseline_row, candidate_row in zip(baseline, candidate, strict=True):
        repetition = str(baseline_row["repetition_id"])
        if repetition != str(candidate_row["repetition_id"]):
            raise ValueError("paired rows disagree on repetition")
        by_repetition.setdefault(repetition, []).append(
            (
                float(baseline_row["latency_ms"]),
                float(candidate_row["latency_ms"]),
            )
        )
    repetitions = sorted(by_repetition)
    ratios = []
    for _ in range(BOOTSTRAP_SAMPLES):
        sampled_baseline = []
        sampled_candidate = []
        for _ in repetitions:
            repetition = repetitions[rng.randrange(len(repetitions))]
            pairs = by_repetition[repetition]
            for _ in pairs:
                baseline_latency, candidate_latency = pairs[rng.randrange(len(pairs))]
                sampled_baseline.append(baseline_latency)
                sampled_candidate.append(candidate_latency)
        baseline_p95 = percentile(sampled_baseline, 0.95)
        candidate_p95 = percentile(sampled_candidate, 0.95)
        ratios.append(candidate_p95 / baseline_p95)
    return percentile(ratios, BOOTSTRAP_UPPER_QUANTILE)


def _complete_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    complete = [row for row in rows if row.get("status") == "ok"]
    if len(complete) != len(rows):
        raise ValueError("qualification input contains non-ok rows")
    return complete


def _pair_rows(
    baseline_rows: Sequence[dict[str, Any]],
    candidate_rows: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    baseline = {_pair_key(row): row for row in baseline_rows}
    candidate = {_pair_key(row): row for row in candidate_rows}
    if len(baseline) != len(baseline_rows) or len(candidate) != len(candidate_rows):
        raise ValueError("qualification input has duplicate query source indices")
    common = sorted(set(baseline).intersection(candidate))
    return [baseline[item] for item in common], [candidate[item] for item in common]


def _pair_key(row: dict[str, Any]) -> tuple[str, int]:
    return str(row["repetition_id"]), int(row["query_source_index"])


def _has_complete_operational_evidence(rows: Sequence[dict[str, Any]]) -> bool:
    try:
        values = [float(row[field]) for row in rows for field in OPERATIONAL_FIELDS]
    except (KeyError, TypeError, ValueError):
        return False
    return bool(values) and all(math.isfinite(value) and value >= 0 for value in values)


def _mean_ratio(
    baseline: Sequence[dict[str, Any]],
    candidate: Sequence[dict[str, Any]],
    field: str,
) -> float:
    baseline_value = statistics.fmean(float(row[field]) for row in baseline)
    candidate_value = statistics.fmean(float(row[field]) for row in candidate)
    if baseline_value <= 0:
        return 1.0 if candidate_value <= 0 else math.inf
    return candidate_value / baseline_value


def _dataset_decision(
    dataset: str,
    backend: str,
    arm: str,
    baseline_rows: Sequence[dict[str, Any]],
    candidate_rows: Sequence[dict[str, Any]],
    minimum_samples: int,
) -> dict[str, Any]:
    baseline, candidate = _pair_rows(baseline_rows, candidate_rows)
    baseline_repetitions = {str(row["repetition_id"]) for row in baseline_rows}
    candidate_repetitions = {str(row["repetition_id"]) for row in candidate_rows}
    sample_gate = (
        baseline_repetitions == candidate_repetitions
        and {_pair_key(row) for row in baseline_rows}
        == {_pair_key(row) for row in candidate_rows}
        and len(baseline_repetitions) >= MINIMUM_REPETITIONS
        and all(
            sum(str(row["repetition_id"]) == repetition for row in baseline)
            >= minimum_samples
            and sum(str(row["repetition_id"]) == repetition for row in candidate)
            >= minimum_samples
            for repetition in baseline_repetitions
        )
    )
    if not baseline or not candidate:
        sample_gate = False
    sample_gate = (
        sample_gate
        and _has_complete_operational_evidence(baseline)
        and _has_complete_operational_evidence(candidate)
    )
    baseline_latency = [float(row["latency_ms"]) for row in baseline]
    candidate_latency = [float(row["latency_ms"]) for row in candidate]
    p95_ratio = (
        percentile(candidate_latency, 0.95) / percentile(baseline_latency, 0.95)
        if sample_gate
        else math.inf
    )
    p99_ratio = (
        percentile(candidate_latency, 0.99) / percentile(baseline_latency, 0.99)
        if sample_gate
        else math.inf
    )
    recall_loss = (
        statistics.fmean(float(row["recall_at_10"]) for row in baseline)
        - statistics.fmean(float(row["recall_at_10"]) for row in candidate)
        if sample_gate
        else math.inf
    )
    confidence_upper = (
        _bootstrap_p95_ratio(
            baseline,
            candidate,
            seed=int.from_bytes(f"{dataset}/{backend}/{arm}".encode(), "little")
            % 2**32,
        )
        if sample_gate
        else math.inf
    )
    correctness = recall_loss <= MAX_RECALL_LOSS
    confidence = (
        p95_ratio <= TARGET_P95_RATIO
        and confidence_upper < MAX_FAMILYWISE_CONFIDENCE_UPPER
        and p99_ratio <= MAX_P99_RATIO
    )
    request_ratio = (
        _mean_ratio(baseline, candidate, "physical_requests")
        if sample_gate
        else math.inf
    )
    bytes_ratio = (
        _mean_ratio(baseline, candidate, "bytes_read") if sample_gate else math.inf
    )
    build_ratio = (
        _mean_ratio(baseline, candidate, "build_ms") if sample_gate else math.inf
    )
    segment_bytes_ratio = (
        _mean_ratio(baseline, candidate, "segment_bytes") if sample_gate else math.inf
    )
    index_bytes_ratio = (
        _mean_ratio(baseline, candidate, "total_active_index_bytes")
        if sample_gate
        else math.inf
    )
    peak_rss_ratio = (
        _mean_ratio(baseline, candidate, "peak_rss_bytes") if sample_gate else math.inf
    )
    cpu_ratio = (
        _mean_ratio(baseline, candidate, "cpu_core_ms") if sample_gate else math.inf
    )
    operational = (
        request_ratio <= MAX_REQUEST_RATIO
        and bytes_ratio <= MAX_BYTES_RATIO
        and build_ratio <= MAX_BUILD_RATIO
        and segment_bytes_ratio <= MAX_SEGMENT_BYTES_RATIO
        and index_bytes_ratio <= MAX_INDEX_BYTES_RATIO
        and peak_rss_ratio <= MAX_PEAK_RSS_RATIO
        and cpu_ratio <= MAX_CPU_RATIO
    )
    if not sample_gate:
        decision, reason = "no-promotion", "sample gate failed"
    elif not correctness:
        decision, reason = "no-promotion", "correctness gate failed"
    elif not confidence:
        decision, reason = "no-promotion", "confidence gate failed"
    elif not operational:
        decision, reason = "no-promotion", "operational regression gate failed"
    else:
        decision, reason = "promote", "correct and statistically bounded win"
    return {
        "dataset": dataset,
        "backend": backend,
        "arm": arm,
        "decision": decision,
        "reason": reason,
        "paired_samples": len(baseline),
        "repetitions": len(baseline_repetitions.intersection(candidate_repetitions)),
        "p95_ratio": p95_ratio,
        "p95_ratio_familywise_ci_upper": confidence_upper,
        "p99_ratio": p99_ratio,
        "recall_loss": recall_loss,
        "request_ratio": request_ratio,
        "bytes_ratio": bytes_ratio,
        "build_ratio": build_ratio,
        "segment_bytes_ratio": segment_bytes_ratio,
        "index_bytes_ratio": index_bytes_ratio,
        "peak_rss_ratio": peak_rss_ratio,
        "cpu_ratio": cpu_ratio,
    }


def analyze_rows(
    input_rows: Sequence[dict[str, Any]],
    *,
    minimum_samples: int = 30,
) -> list[dict[str, Any]]:
    if minimum_samples <= 0:
        raise ValueError("minimum_samples must be positive")
    rows = _complete_rows(input_rows)
    backends = sorted({str(row["backend"]) for row in rows})
    observed_datasets = {str(row["dataset"]) for row in rows}
    unknown_datasets = observed_datasets.difference(REQUIRED_DATASETS)
    if unknown_datasets:
        raise ValueError(
            f"qualification input contains unknown datasets: {sorted(unknown_datasets)}"
        )
    observed_arms = {str(row["arm"]) for row in rows}
    unknown_arms = observed_arms.difference({BASELINE_ARM, *CANDIDATE_ARMS})
    if unknown_arms:
        raise ValueError(
            f"qualification input contains unknown arms: {sorted(unknown_arms)}"
        )
    arms = list(CANDIDATE_ARMS)
    decisions: list[dict[str, Any]] = []
    for backend in backends:
        backend_rows = [row for row in rows if row["backend"] == backend]
        datasets = sorted({str(row["dataset"]) for row in backend_rows})
        for arm in arms:
            dataset_decisions = []
            for dataset in datasets:
                selected = [row for row in backend_rows if row["dataset"] == dataset]
                baseline = [row for row in selected if row["arm"] == BASELINE_ARM]
                candidate = [row for row in selected if row["arm"] == arm]
                if not baseline and not candidate:
                    continue
                decision = _dataset_decision(
                    dataset,
                    backend,
                    arm,
                    baseline,
                    candidate,
                    minimum_samples,
                )
                decisions.append(decision)
                dataset_decisions.append(decision)
            passed = [row for row in dataset_decisions if row["decision"] == "promote"]
            reasons = {row["reason"] for row in dataset_decisions}
            if len(dataset_decisions) < MINIMUM_DATASETS:
                overall_decision = "no-promotion"
                reason = "promotion requires at least two datasets"
            elif len(passed) != len(dataset_decisions):
                overall_decision = "no-promotion"
                if any("sample gate" in item for item in reasons):
                    reason = "sample gate failed on at least one dataset"
                elif any("correctness" in item for item in reasons):
                    reason = "correctness gate failed on at least one dataset"
                elif any("operational" in item for item in reasons):
                    reason = (
                        "operational regression gate failed on at least one dataset"
                    )
                else:
                    reason = "confidence gate failed on at least one dataset"
            else:
                overall_decision = "promote"
                reason = "all dataset gates passed"
            decisions.append(
                {
                    "dataset": "all",
                    "backend": backend,
                    "arm": arm,
                    "decision": overall_decision,
                    "reason": reason,
                    "datasets_tested": len(dataset_decisions),
                    "datasets_passed": len(passed),
                    "worst_p95_ratio": max(
                        (row["p95_ratio"] for row in dataset_decisions),
                        default=math.inf,
                    ),
                    "worst_p95_ratio_familywise_ci_upper": max(
                        (
                            row["p95_ratio_familywise_ci_upper"]
                            for row in dataset_decisions
                        ),
                        default=math.inf,
                    ),
                    "worst_p99_ratio": max(
                        (row["p99_ratio"] for row in dataset_decisions),
                        default=math.inf,
                    ),
                    "worst_recall_loss": max(
                        (row["recall_loss"] for row in dataset_decisions),
                        default=math.inf,
                    ),
                    "worst_request_ratio": max(
                        (row["request_ratio"] for row in dataset_decisions),
                        default=math.inf,
                    ),
                    "worst_bytes_ratio": max(
                        (row["bytes_ratio"] for row in dataset_decisions),
                        default=math.inf,
                    ),
                    "worst_build_ratio": max(
                        (row["build_ratio"] for row in dataset_decisions),
                        default=math.inf,
                    ),
                    "worst_segment_bytes_ratio": max(
                        (row["segment_bytes_ratio"] for row in dataset_decisions),
                        default=math.inf,
                    ),
                    "worst_index_bytes_ratio": max(
                        (row["index_bytes_ratio"] for row in dataset_decisions),
                        default=math.inf,
                    ),
                    "worst_peak_rss_ratio": max(
                        (row["peak_rss_ratio"] for row in dataset_decisions),
                        default=math.inf,
                    ),
                    "worst_cpu_ratio": max(
                        (row["cpu_ratio"] for row in dataset_decisions),
                        default=math.inf,
                    ),
                }
            )

    for arm in arms:
        backend_decisions = [
            row
            for row in decisions
            if row["dataset"] == "all" and row["backend"] != "all" and row["arm"] == arm
        ]
        by_backend = {str(row["backend"]): row for row in backend_decisions}
        required = set(REQUIRED_BACKENDS)
        passed = [
            row
            for backend, row in by_backend.items()
            if backend in required and row["decision"] == "promote"
        ]
        reasons = {str(row["reason"]) for row in backend_decisions}
        if set(by_backend) != required:
            overall_decision = "no-promotion"
            reason = "promotion requires both required backends"
        elif len(passed) != len(REQUIRED_BACKENDS):
            overall_decision = "no-promotion"
            if any("two datasets" in item for item in reasons):
                reason = "promotion requires two datasets on every backend"
            elif any("sample gate" in item for item in reasons):
                reason = "sample gate failed on at least one backend"
            elif any("correctness" in item for item in reasons):
                reason = "correctness gate failed on at least one backend"
            elif any("operational" in item for item in reasons):
                reason = "operational regression gate failed on at least one backend"
            else:
                reason = "confidence gate failed on at least one backend"
        else:
            overall_decision = "promote"
            reason = "all required backend and dataset gates passed"

        def worst(
            field: str,
            rows: tuple[dict[str, str | int | float], ...] = tuple(backend_decisions),
        ) -> float:
            return max(
                (float(row[field]) for row in rows),
                default=math.inf,
            )

        decisions.append(
            {
                "dataset": "all",
                "backend": "all",
                "arm": arm,
                "decision": overall_decision,
                "reason": reason,
                "backends_tested": len(by_backend),
                "backends_passed": len(passed),
                "worst_p95_ratio": worst("worst_p95_ratio"),
                "worst_p95_ratio_familywise_ci_upper": worst(
                    "worst_p95_ratio_familywise_ci_upper"
                ),
                "worst_p99_ratio": worst("worst_p99_ratio"),
                "worst_recall_loss": worst("worst_recall_loss"),
                "worst_request_ratio": worst("worst_request_ratio"),
                "worst_bytes_ratio": worst("worst_bytes_ratio"),
                "worst_build_ratio": worst("worst_build_ratio"),
                "worst_segment_bytes_ratio": worst("worst_segment_bytes_ratio"),
                "worst_index_bytes_ratio": worst("worst_index_bytes_ratio"),
                "worst_peak_rss_ratio": worst("worst_peak_rss_ratio"),
                "worst_cpu_ratio": worst("worst_cpu_ratio"),
            }
        )
    return decisions


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError("qualification samples are empty")
    return rows


def write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite {path}")
    fields = sorted({field for row in rows for field in row})
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minimum-samples", type=int, default=30)
    args = parser.parse_args()
    write_csv(
        args.output,
        analyze_rows(read_csv(args.samples), minimum_samples=args.minimum_samples),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
