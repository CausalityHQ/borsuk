#!/usr/bin/env python3
"""Open-loop load generator for the colocated BORSUK REST benchmark."""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import math
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


@dataclasses.dataclass(frozen=True)
class Sample:
    endpoint: str
    scheduled_ns: int
    started_ns: int
    completed_ns: int
    status: int
    recall_at_10: float | None
    engine: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "endpoint": self.endpoint,
            "scheduled_ns": self.scheduled_ns,
            "started_ns": self.started_ns,
            "completed_ns": self.completed_ns,
            "status": self.status,
            "latency_ms": (self.completed_ns - self.scheduled_ns) / 1_000_000,
            "schedule_lag_ms": (self.started_ns - self.scheduled_ns) / 1_000_000,
            "recall_at_10": self.recall_at_10,
            "engine": self.engine,
        }


def scheduled_offsets_ns(rate_per_second: float, duration_seconds: float) -> list[int]:
    if not math.isfinite(rate_per_second) or rate_per_second < 0:
        raise ValueError("request rate must be finite and nonnegative")
    if not math.isfinite(duration_seconds) or duration_seconds <= 0:
        raise ValueError("duration must be finite and positive")
    if rate_per_second == 0:
        return []
    interval = 1_000_000_000 / rate_per_second
    count = math.ceil(rate_per_second * duration_seconds)
    limit = round(duration_seconds * 1_000_000_000)
    return [round(index * interval) for index in range(count) if round(index * interval) < limit]


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    if not 0 < quantile <= 1:
        raise ValueError("quantile must be in (0, 1]")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def _endpoint_summary(samples: list[Sample], duration_seconds: float) -> dict[str, object]:
    latencies = [(sample.completed_ns - sample.scheduled_ns) / 1_000_000 for sample in samples]
    lags = [(sample.started_ns - sample.scheduled_ns) / 1_000_000 for sample in samples]
    recalls = [sample.recall_at_10 for sample in samples if sample.recall_at_10 is not None]
    engines = sorted({sample.engine for sample in samples if sample.engine is not None})
    errors = sum(sample.status >= 500 or sample.status == 0 for sample in samples)
    return {
        "requests": len(samples),
        "completed_qps": len(samples) / duration_seconds,
        "errors": errors,
        "rejected_429": sum(sample.status == 429 for sample in samples),
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "max_ms": max(latencies, default=0.0),
        "schedule_lag_p99_ms": percentile(lags, 0.99),
        "mean_recall_at_10": sum(recalls) / len(recalls) if recalls else None,
        "engines": engines,
    }


def summarize(phase: str, duration_seconds: float, samples: list[Sample]) -> dict[str, object]:
    groups = {
        endpoint: [sample for sample in samples if sample.endpoint == endpoint]
        for endpoint in ("cheap", "search")
    }
    return {
        "schema_version": 1,
        "phase": phase,
        "duration_seconds": duration_seconds,
        **{
            endpoint: _endpoint_summary(group, duration_seconds)
            for endpoint, group in groups.items()
            if group
        },
    }


def evaluate_phase(
    phase: str,
    baseline: dict[str, Any],
    summary: dict[str, Any],
) -> list[str]:
    failures: list[str] = []
    cheap = summary.get("cheap")
    baseline_cheap = baseline.get("cheap")
    if isinstance(cheap, dict) and isinstance(baseline_cheap, dict):
        multiplier, additive = (1.5, 5.0) if phase == "mixed-overload" else (1.25, 2.0)
        limit = max(float(baseline_cheap["p99_ms"]) * multiplier, float(baseline_cheap["p99_ms"]) + additive)
        if float(cheap["p99_ms"]) > limit:
            failures.append(f"cheap p99 {cheap['p99_ms']}ms exceeds {limit}ms")
        requests = int(cheap["requests"])
        if requests == 0 or int(cheap["errors"]) / requests >= 0.001:
            failures.append("cheap endpoint error rate is not below 0.1%")
    search = summary.get("search")
    if isinstance(search, dict):
        recall = search.get("mean_recall_at_10")
        if recall is None or float(recall) < 0.95:
            failures.append(f"vector recall@10 {recall} is below 0.95")
        engines = search.get("engines")
        if engines != ["bounded-cell-card-v14"]:
            failures.append(
                "vector engine must be exactly bounded-cell-card-v14; "
                f"observed {engines}"
            )
    return failures


def _recall_at_10(actual: list[str], expected: list[str]) -> float:
    denominator = min(10, len(expected))
    if denominator == 0:
        raise ValueError("query oracle has no neighbors")
    return len(set(actual[:10]).intersection(expected[:10])) / denominator


def _request(
    base_url: str,
    endpoint: str,
    scheduled_ns: int,
    query: dict[str, object] | None,
    timeout_seconds: float,
) -> Sample:
    started = time.monotonic_ns()
    recall = None
    engine = None
    if endpoint == "search":
        if query is None:
            raise ValueError("search request has no query")
        body = json.dumps({"vector": query["vector"], "k": 10}, separators=(",", ":")).encode()
        request = urllib.request.Request(
            f"{base_url}/api/search",
            data=body,
            headers={"content-type": "application/json"},
            method="POST",
        )
    else:
        request = urllib.request.Request(f"{base_url}/api/item/benchmark", method="GET")
    status = 0
    payload: dict[str, object] = {}
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status = response.status
            payload = json.loads(response.read())
    except urllib.error.HTTPError as error:
        status = error.code
        error.read()
    except (OSError, TimeoutError, json.JSONDecodeError):
        status = 0
    completed = time.monotonic_ns()
    if endpoint == "search" and status == 200 and query is not None:
        actual = [str(item["id"]) for item in payload.get("hits", [])]  # type: ignore[union-attr]
        recall = _recall_at_10(actual, [str(item) for item in query["neighbors"]])  # type: ignore[index]
        engine_value = payload.get("engine")
        engine = str(engine_value) if engine_value is not None else None
    return Sample(endpoint, scheduled_ns, started, completed, status, recall, engine)


def run_phase(
    base_url: str,
    duration_seconds: float,
    cheap_qps: float,
    search_qps: float,
    queries: list[dict[str, object]],
    workers: int,
    timeout_seconds: float,
) -> list[Sample]:
    if search_qps > 0 and not queries:
        raise ValueError("search phase requires query vectors and oracle neighbors")
    epoch = time.monotonic_ns() + 1_000_000_000
    schedule = [(offset, "cheap") for offset in scheduled_offsets_ns(cheap_qps, duration_seconds)]
    schedule += [(offset, "search") for offset in scheduled_offsets_ns(search_qps, duration_seconds)]
    schedule.sort(key=lambda item: (item[0], item[1]))
    futures: list[concurrent.futures.Future[Sample]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for ordinal, (offset, endpoint) in enumerate(schedule):
            scheduled = epoch + offset
            delay = (scheduled - time.monotonic_ns()) / 1_000_000_000
            if delay > 0:
                time.sleep(delay)
            query = queries[ordinal % len(queries)] if endpoint == "search" else None
            futures.append(
                executor.submit(_request, base_url, endpoint, scheduled, query, timeout_seconds)
            )
        return [future.result() for future in futures]


def _load_queries(path: Path | None) -> list[dict[str, object]]:
    if path is None:
        return []
    queries = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    for query in queries:
        if set(query) != {"vector", "neighbors"} or not query["vector"] or not query["neighbors"]:
            raise ValueError("query JSONL rows must contain nonempty vector and neighbors")
    return queries


def _write_canonical(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--cheap-qps", type=float, required=True)
    parser.add_argument("--search-qps", type=float, required=True)
    parser.add_argument("--queries", type=Path)
    parser.add_argument("--workers", type=int, default=128)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--samples", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    args = parser.parse_args()
    queries = _load_queries(args.queries)
    samples = run_phase(
        args.base_url.rstrip("/"),
        args.duration_seconds,
        args.cheap_qps,
        args.search_qps,
        queries,
        args.workers,
        args.timeout_seconds,
    )
    args.samples.write_text("".join(json.dumps(sample.as_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n" for sample in samples), encoding="utf-8")
    summary = summarize(args.phase, args.duration_seconds, samples)
    failures: list[str] = []
    if args.baseline is not None:
        failures = evaluate_phase(args.phase, json.loads(args.baseline.read_text()), summary)
    summary["gate_failures"] = failures
    summary["passed"] = not failures
    _write_canonical(args.summary, summary)
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
