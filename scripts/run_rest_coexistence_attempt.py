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

CAUSALITY_AWS_ACCOUNT = "453182569524"
REST_RESULT_SCHEMA_VERSION = 6
FLOW_CONTROL_COUNTERS = (
    "borsuk_leaf_read_wait_count",
    "borsuk_leaf_read_wait_micros",
    "borsuk_search_rejected",
    "borsuk_search_wait_count",
    "borsuk_search_wait_micros",
)


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


def validated_aws_identity(
    controller_profile: str,
    expected_account: str,
    runtime_account: str,
) -> dict[str, str]:
    if not controller_profile:
        raise ValueError("controller AWS profile must be nonempty")
    if re.fullmatch(r"[0-9]{12}", expected_account) is None:
        raise ValueError("expected AWS account must be a 12-digit account ID")
    if expected_account != CAUSALITY_AWS_ACCOUNT:
        raise ValueError(
            f"expected account must be the Causality AWS account {CAUSALITY_AWS_ACCOUNT}"
        )
    if runtime_account != expected_account:
        raise ValueError(
            "runtime AWS account differs from the controller-authorized account"
        )
    return {
        "controller_aws_profile": controller_profile,
        "aws_account_id": runtime_account,
    }


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


def overload_search_qps(sustainable: float, rates: list[int]) -> float:
    if sustainable <= 0 or not rates:
        raise ValueError("overload rate requires positive sustainable and staircase rates")
    return max(sustainable * 1.50, float(max(rates)))


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
        "exact_candidates": "borsuk_exact_candidates",
        "exact_read_max_physical_amplification": (
            "borsuk_exact_read_max_physical_amplification"
        ),
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


def flow_control_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, int]:
    delta: dict[str, int] = {}
    for field in FLOW_CONTROL_COUNTERS:
        first = before.get(field)
        last = after.get(field)
        if type(first) is not int or type(last) is not int:
            raise ValueError(f"REST flow-control counter {field} is missing or non-integer")
        if last < first:
            raise ValueError(f"REST flow-control counter {field} regressed")
        delta[field] = last - first
    return delta


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
    flow_before = _metrics(base_url)
    samples = run_phase(base_url, duration, cheap_qps, search_qps, queries, 256, 30.0)
    flow_after = _metrics(base_url)
    summary = summarize(name, duration, samples)
    summary["flow_control_delta"] = flow_control_delta(flow_before, flow_after)
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
    parser.add_argument("--controller-aws-profile", required=True)
    parser.add_argument("--expected-aws-account", required=True)
    parser.add_argument("--runtime-aws-account", required=True)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    base_url = args.base_url.rstrip("/")
    expected = json.loads(args.expected_runtime.read_text(encoding="utf-8"))
    attempt_authority_sha256 = validated_authority_sha256(
        os.environ.get("BORSUK_ATTEMPT_AUTHORITY_SHA256", "")
    )
    aws_identity = validated_aws_identity(
        args.controller_aws_profile,
        args.expected_aws_account,
        args.runtime_aws_account,
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
        overload_search_qps(sustainable, rates),
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
        "schema_version": REST_RESULT_SCHEMA_VERSION,
        **aws_identity,
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
