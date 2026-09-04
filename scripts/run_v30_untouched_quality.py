#!/usr/bin/env python3
"""Run one sealed V30 cohort through the real Rust S3 qualifier."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from argparse import ArgumentParser
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

if __package__:
    from scripts.run_v30_reduced_quality import _query_result, _truth
else:
    from run_v30_reduced_quality import _query_result, _truth

QUERY_COUNT = 32
RECALL_K = 10


@dataclass(frozen=True)
class LocalArtifact:
    path: Path
    sha256: str
    encoded_bytes: int


@dataclass(frozen=True)
class V30UntouchedPlan:
    qualifier: Path
    manifest: LocalArtifact
    artifact_dir: Path
    query: LocalArtifact
    truth: LocalArtifact
    page_s3_prefix: str
    source_rows: int
    query_start: int
    query_count: int
    leaf_beam: int
    page_count: int


def _digest(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _validate_artifact(artifact: LocalArtifact) -> None:
    if (
        not artifact.path.is_absolute()
        or not _digest(artifact.sha256)
        or type(artifact.encoded_bytes) is not int
        or artifact.encoded_bytes <= 0
    ):
        raise ValueError("V30 untouched artifact authority differs")


def _validate(plan: V30UntouchedPlan) -> None:
    _validate_artifact(plan.manifest)
    _validate_artifact(plan.query)
    _validate_artifact(plan.truth)
    if (
        not plan.qualifier.is_absolute()
        or not plan.artifact_dir.is_absolute()
        or not plan.page_s3_prefix.startswith("s3://")
        or plan.page_s3_prefix.endswith("/")
        or type(plan.source_rows) is not int
        or plan.source_rows <= 0
        or type(plan.query_start) is not int
        or plan.query_start < 0
        or plan.query_count != QUERY_COUNT
        or plan.leaf_beam not in {192, 512}
        or type(plan.page_count) is not int
        or not 1 <= plan.page_count <= 16
    ):
        raise ValueError("V30 untouched plan authority differs")


def build_qualifier_commands(plan: V30UntouchedPlan) -> tuple[tuple[str, ...], ...]:
    """Return the frozen qualifier invocations for the sealed cohort."""

    _validate(plan)
    common = (
        str(plan.qualifier),
        "--execute",
        "--manifest",
        str(plan.manifest.path),
        "--manifest-sha256",
        plan.manifest.sha256,
        "--manifest-bytes",
        str(plan.manifest.encoded_bytes),
        "--artifact-dir",
        str(plan.artifact_dir),
        "--query-parquet",
        str(plan.query.path),
        "--query-sha256",
        plan.query.sha256,
        "--query-bytes",
        str(plan.query.encoded_bytes),
    )
    arm = (
        "--leaf-beam",
        str(plan.leaf_beam),
        "--page-count",
        str(plan.page_count),
        "--k",
        "10",
        "--s3-page-prefix",
        plan.page_s3_prefix,
    )
    return (
        common
        + (
            "--query-start",
            str(plan.query_start),
            "--query-count",
            str(plan.query_count),
        )
        + arm,
    )


def _read_exact(artifact: LocalArtifact) -> bytes:
    data = artifact.path.read_bytes()
    if (
        len(data) != artifact.encoded_bytes
        or hashlib.sha256(data).hexdigest() != artifact.sha256
    ):
        raise ValueError("V30 untouched artifact bytes differ")
    return data


def _batch_results(
    payload: bytes, *, expected_pages: int
) -> tuple[tuple[tuple[int, ...], dict[str, int]], ...]:
    if type(payload) is not bytes or not payload.endswith(b"\n") or b"\n" in payload[:-1]:
        raise ValueError("V30 batch result canonical bytes differ")
    value = json.loads(payload)
    expected = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    if (
        payload != expected
        or type(value) is not dict
        or set(value) != {"claim_eligible", "results", "schema_version"}
        or value["claim_eligible"] is not False
        or value["schema_version"] != 2
        or type(value["results"]) is not list
        or len(value["results"]) != QUERY_COUNT
    ):
        raise ValueError("V30 batch result authority differs")
    return tuple(
        _query_result(
            json.dumps(result, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
            + b"\n",
            expected_pages=expected_pages,
        )
        for result in value["results"]
    )


def run_v30_untouched_quality(
    plan: V30UntouchedPlan,
    *,
    invoke: Callable[[tuple[str, ...]], bytes],
) -> bytes:
    """Execute and independently reduce one preregistered untouched cohort."""

    commands = build_qualifier_commands(plan)
    truth_bytes = _read_exact(plan.truth)
    truth = _truth(
        truth_bytes,
        plan.truth.sha256,
        query_start=plan.query_start,
        source_rows=plan.source_rows,
    )
    parsed = _batch_results(invoke(commands[0]), expected_pages=plan.page_count)
    expected_leaves = {100_000: 256, 9_990_000: 32_768}[plan.source_rows]
    if any(
        work["roots_scored"] != 0
        or work["leaves_scored"] != expected_leaves
        or work["codes_scanned"] != 0
        or work["candidates_retained"] != 0
        or not plan.page_count <= work["pages_considered"] <= plan.leaf_beam * 64
        for _matches, work in parsed
    ):
        raise ValueError("V30 production routing work differs")
    hits = tuple(
        len(frozenset(matches) & truth[ordinal])
        for ordinal, (matches, _work) in enumerate(parsed)
    )
    aggregate = sum(hits) * 1_000_000 // (QUERY_COUNT * RECALL_K)
    minimum = min(hits) * 1_000_000 // RECALL_K
    floor_compliance = sum(hit >= 8 for hit in hits) * 1_000_000 // QUERY_COUNT
    cpu_p99 = max(work["process_cpu_ns"] for _matches, work in parsed)
    cold_p99 = max(work["elapsed_ns"] for _matches, work in parsed)
    maximum_peak_rss = max(work["peak_rss_bytes"] for _matches, work in parsed)
    maximum_bytes = max(work["encoded_bytes"] for _matches, work in parsed)
    maximum_codes = max(work["codes_scanned"] for _matches, work in parsed)
    maximum_gets = max(work["get_count"] for _matches, work in parsed)
    failed_gates = [
        name
        for name, failed in (
            ("aggregate-recall", aggregate < 995_000),
            ("floor-compliance", floor_compliance < 997_500),
            ("minimum-recall", minimum < 800_000),
            ("cpu-p99", cpu_p99 > 15_000_000),
            ("cold-p99", cold_p99 > 100_000_000),
            ("peak-rss", maximum_peak_rss > 3 * 1024**3),
            (
                "query-elapsed-stop",
                max(work["elapsed_ns"] for _matches, work in parsed) > 150_000_000,
            ),
        )
        if failed
    ]
    value = {
        "aggregate_recall_ppm": aggregate,
        "claim_eligible": False,
        "floor_compliance_ppm": floor_compliance,
        "failed_gates": failed_gates,
        "manifest_sha256": plan.manifest.sha256,
        "maximum_codes_scanned": maximum_codes,
        "maximum_encoded_bytes": maximum_bytes,
        "maximum_get_count": maximum_gets,
        "maximum_routing_cpu_ns": max(work["routing_cpu_ns"] for _matches, work in parsed),
        "maximum_page_read_cpu_ns": max(work["page_read_cpu_ns"] for _matches, work in parsed),
        "maximum_exact_rerank_cpu_ns": max(
            work["exact_rerank_cpu_ns"] for _matches, work in parsed
        ),
        "maximum_routing_elapsed_ns": max(
            work["routing_elapsed_ns"] for _matches, work in parsed
        ),
        "maximum_page_read_elapsed_ns": max(
            work["page_read_elapsed_ns"] for _matches, work in parsed
        ),
        "maximum_exact_rerank_elapsed_ns": max(
            work["exact_rerank_elapsed_ns"] for _matches, work in parsed
        ),
        "maximum_peak_rss_bytes": maximum_peak_rss,
        "measured_cold_p99_ns": cold_p99,
        "measured_process_cpu_p99_ns": cpu_p99,
        "minimum_recall_ppm": minimum,
        "perfect_queries": sum(hit == RECALL_K for hit in hits),
        "query_count": QUERY_COUNT,
        "query_sha256": plan.query.sha256,
        "query_start": plan.query_start,
        "schema_version": 2,
        "samples": [
            {
                "candidates_retained": work["candidates_retained"],
                "codes_scanned": work["codes_scanned"],
                "elapsed_ns": work["elapsed_ns"],
                "encoded_bytes": work["encoded_bytes"],
                "get_count": work["get_count"],
                "hits": hits[ordinal],
                "matched_source_ordinals": list(matches),
                "process_cpu_ns": work["process_cpu_ns"],
                "routing_cpu_ns": work["routing_cpu_ns"],
                "page_read_cpu_ns": work["page_read_cpu_ns"],
                "exact_rerank_cpu_ns": work["exact_rerank_cpu_ns"],
                "routing_elapsed_ns": work["routing_elapsed_ns"],
                "page_read_elapsed_ns": work["page_read_elapsed_ns"],
                "exact_rerank_elapsed_ns": work["exact_rerank_elapsed_ns"],
                "peak_rss_bytes": work["peak_rss_bytes"],
                "query_ordinal": plan.query_start + ordinal,
                "recall_ppm": hits[ordinal] * 1_000_000 // RECALL_K,
                "selected_pages": work["selected_pages"],
            }
            for ordinal, (matches, work) in enumerate(parsed)
        ],
        "source_rows": plan.source_rows,
        "status": "failed" if failed_gates else "passed",
        "truth_sha256": plan.truth.sha256,
    }
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _invoke(command: tuple[str, ...]) -> bytes:
    completed = subprocess.run(command, check=False, capture_output=True)
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"V30 qualifier failed ({completed.returncode}): {detail}")
    return completed.stdout


def main(arguments: list[str] | None = None) -> int:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", required=True)
    parser.add_argument("--qualifier", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--manifest-bytes", type=int, required=True)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--query-parquet", type=Path, required=True)
    parser.add_argument("--query-sha256", required=True)
    parser.add_argument("--query-bytes", type=int, required=True)
    parser.add_argument("--truth-parquet", type=Path, required=True)
    parser.add_argument("--truth-sha256", required=True)
    parser.add_argument("--truth-bytes", type=int, required=True)
    parser.add_argument("--s3-page-prefix", required=True)
    parser.add_argument("--source-rows", type=int, required=True)
    parser.add_argument("--query-start", type=int, required=True)
    parser.add_argument("--query-count", type=int, required=True)
    parser.add_argument("--page-count", type=int, required=True)
    parser.add_argument("--leaf-beam", type=int, required=True)
    args = parser.parse_args(arguments)
    plan = V30UntouchedPlan(
        qualifier=args.qualifier,
        manifest=LocalArtifact(
            args.manifest, args.manifest_sha256, args.manifest_bytes
        ),
        artifact_dir=args.artifact_dir,
        query=LocalArtifact(args.query_parquet, args.query_sha256, args.query_bytes),
        truth=LocalArtifact(args.truth_parquet, args.truth_sha256, args.truth_bytes),
        page_s3_prefix=args.s3_page_prefix,
        source_rows=args.source_rows,
        query_start=args.query_start,
        query_count=args.query_count,
        leaf_beam=args.leaf_beam,
        page_count=args.page_count,
    )
    sys.stdout.buffer.write(run_v30_untouched_quality(plan, invoke=_invoke))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
