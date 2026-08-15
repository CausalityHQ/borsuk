#!/usr/bin/env python3
"""Execute one frozen open-loop REST coexistence attempt."""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.request
from pathlib import Path
from typing import Any

try:
    from .rest_coexistence_load import evaluate_phase, run_phase, summarize
except ImportError:
    from rest_coexistence_load import evaluate_phase, run_phase, summarize


def accepted_search_qps(summary: dict[str, Any]) -> float:
    search = summary.get("search")
    if not isinstance(search, dict):
        return 0.0
    completed = int(search["successful_requests"])
    return completed / float(summary["duration_seconds"])


def staircase_rates(smoke: bool) -> list[int]:
    return [16, 32, 64, 96] if smoke else [8, 16, 32, 64, 96, 128]


def validated_authority_sha256(value: str) -> str:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("attempt authority must be a lowercase SHA-256 digest")
    return value


def select_sustainable_search_qps(rows: list[dict[str, Any]]) -> float:
    qualified = [
        (float(row["offered_qps"]), float(row["accepted_qps"]))
        for row in rows
        if row["summary"].get("passed")
        and int(row["summary"].get("search", {}).get("rejected_429", 0)) == 0
    ]
    if not qualified:
        raise ValueError("REST staircase has no clean recall-qualified rate")
    return max(qualified)[1]


def staircase_has_only_expected_capacity_failures(rows: list[dict[str, Any]]) -> bool:
    allowed = {
        "non-overload vector phase returned HTTP 429",
        "vector p99 exceeds the frozen 100ms limit",
    }
    return all(
        set(row["summary"].get("gate_failures", ())).issubset(allowed)
        for row in rows
    )


def validate_effective_limits(expected: dict[str, int], actual: dict[str, Any]) -> None:
    mapping = {
        "cpu_threads": "borsuk_cpu_threads",
        "io_threads": "borsuk_io_threads",
        "s3_get_concurrency": "borsuk_s3_get_concurrency",
        "search_admission": "borsuk_search_capacity",
        "leaf_read_width": "borsuk_leaf_read_width",
        "max_inflight_leaf_reads": "borsuk_leaf_read_capacity",
        "page_budget": "borsuk_page_budget",
        "ram_budget_bytes": "borsuk_ram_budget_bytes",
        "disk_cache_bytes": "borsuk_disk_cache_bytes",
    }
    observed = {key: int(actual.get(field, -1)) for key, field in mapping.items()}
    wanted = {key: int(expected[key]) for key in mapping}
    if observed != wanted:
        raise ValueError(f"effective REST limits differ: expected {wanted}, observed {observed}")


def _canonical(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _metrics(base_url: str) -> dict[str, Any]:
    with urllib.request.urlopen(f"{base_url}/metrics", timeout=10) as response:
        return json.loads(response.read())


def _queries(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _run(
    output: Path,
    base_url: str,
    name: str,
    duration: float,
    cheap_qps: float,
    search_qps: float,
    queries: list[dict[str, object]],
    baseline: dict[str, Any] | None,
) -> dict[str, Any]:
    samples = run_phase(base_url, duration, cheap_qps, search_qps, queries, 256, 30.0)
    summary = summarize(name, duration, samples)
    failures = evaluate_phase(name, summary if baseline is None else baseline, summary)
    summary["gate_failures"] = failures
    summary["passed"] = not failures
    (output / f"{name}.samples.jsonl").write_text(
        "".join(
            json.dumps(item.as_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False)
            + "\n"
            for item in samples
        ),
        encoding="utf-8",
    )
    _canonical(output / f"{name}.summary.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-runtime", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    base_url = args.base_url.rstrip("/")
    expected = json.loads(args.expected_runtime.read_text(encoding="utf-8"))
    attempt_authority_sha256 = validated_authority_sha256(
        os.environ.get("BORSUK_ATTEMPT_AUTHORITY_SHA256", "")
    )
    before = _metrics(base_url)
    validate_effective_limits(expected, before)
    queries = _queries(args.queries)
    baseline_duration = 10.0 if args.smoke else 30.0
    phase_duration = 10.0 if args.smoke else 120.0
    run_phase(
        base_url,
        5.0 if args.smoke else 30.0,
        20.0,
        4.0,
        queries,
        64,
        30.0,
    )
    baseline = _run(
        args.output, base_url, "cheap-baseline", baseline_duration, 200.0, 0.0, [], None
    )
    rates = staircase_rates(args.smoke)
    staircase: list[dict[str, Any]] = []
    for rate in rates:
        summary = _run(
            args.output,
            base_url,
            f"staircase-{rate}",
            phase_duration,
            0.0,
            float(rate),
            queries,
            baseline,
        )
        staircase.append(
            {
                "offered_qps": rate,
                "accepted_qps": accepted_search_qps(summary),
                "summary": summary,
            }
        )
    sustainable = select_sustainable_search_qps(staircase)
    mixed_normal = _run(
        args.output,
        base_url,
        "mixed-normal",
        phase_duration,
        200.0,
        sustainable * 0.70,
        queries,
        baseline,
    )
    mixed_overload = _run(
        args.output,
        base_url,
        "mixed-overload",
        phase_duration,
        200.0,
        sustainable * 1.50,
        queries,
        baseline,
    )
    after = _metrics(base_url)
    validate_effective_limits(expected, after)
    passed = bool(
        baseline["passed"]
        and staircase_has_only_expected_capacity_failures(staircase)
        and mixed_normal["passed"]
        and mixed_overload["passed"]
    )
    receipt = {
        "schema_version": 2,
        "status": "complete" if passed else "failed",
        "attempt_authority_sha256": attempt_authority_sha256,
        "effective_limits": before,
        "effective_limits_after": after,
        "sustainable_search_qps": sustainable,
        "staircase": staircase,
        "summaries": {
            "baseline": baseline,
            "mixed_normal": mixed_normal,
            "mixed_overload": mixed_overload,
        },
    }
    _canonical(args.output / "REST_RESULT.json", receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
