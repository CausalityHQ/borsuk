#!/usr/bin/env python3
"""Fail-fast V27 S3 page-campaign projection and launch boundary."""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from dataclasses import dataclass

MAX_GETS = 10
MAX_ENCODED_BYTES = 4_587_520
MAX_CPU_P99_MS = 15.0
MAX_S3_P99_MS = 150.0
PERFECT_RECALL_PPM = 1_000_000


@dataclass(frozen=True)
class S3LatencyProfile:
    """Measured one-wave object-store latency and aggregate throughput."""

    request_latency_ms: float
    aggregate_bytes_per_second: int


@dataclass(frozen=True)
class V27QueryEvidence:
    """Truthful reduced-run quality and work counters."""

    cpu_p99_ms: float
    get_count: int
    encoded_bytes: int
    recall_ppm: int
    minimum_recall_ppm: int


@dataclass(frozen=True)
class V27LatencyProjection:
    """Exact decomposition of one concurrent S3 page wave."""

    get_count: int
    encoded_bytes: int
    request_waves: int
    request_ms: float
    transfer_ms: float
    cpu_p99_ms: float
    projected_p99_ms: float


def _real_number(value: object) -> bool:
    return type(value) in {int, float} and math.isfinite(float(value))


def project_v27_query_latency(
    evidence: V27QueryEvidence, profile: S3LatencyProfile
) -> V27LatencyProjection:
    """Project cold p99 from exact bytes and one concurrent page-read wave."""

    if (
        type(evidence.get_count) is not int
        or evidence.get_count <= 0
        or type(evidence.encoded_bytes) is not int
        or evidence.encoded_bytes <= 0
        or not _real_number(evidence.cpu_p99_ms)
        or evidence.cpu_p99_ms < 0
        or not _real_number(profile.request_latency_ms)
        or profile.request_latency_ms < 0
        or type(profile.aggregate_bytes_per_second) is not int
        or profile.aggregate_bytes_per_second <= 0
    ):
        raise ValueError("V27 latency projection authority differs")
    transfer_ms = (
        evidence.encoded_bytes / profile.aggregate_bytes_per_second * 1_000.0
    )
    projected = float(evidence.cpu_p99_ms) + float(profile.request_latency_ms) + transfer_ms
    return V27LatencyProjection(
        get_count=evidence.get_count,
        encoded_bytes=evidence.encoded_bytes,
        request_waves=1,
        request_ms=float(profile.request_latency_ms),
        transfer_ms=transfer_ms,
        cpu_p99_ms=float(evidence.cpu_p99_ms),
        projected_p99_ms=projected,
    )


def preflight_v27_reduced_campaign(
    evidence: V27QueryEvidence,
    profile: S3LatencyProfile,
    *,
    launch: Callable[[], object],
) -> bytes:
    """Reject a bad reduced arm before invoking exactly one external launch."""

    if type(evidence.recall_ppm) is not int or evidence.recall_ppm != PERFECT_RECALL_PPM:
        raise ValueError("V27 aggregate-recall gate failed")
    if (
        type(evidence.minimum_recall_ppm) is not int
        or evidence.minimum_recall_ppm != PERFECT_RECALL_PPM
    ):
        raise ValueError("V27 minimum-recall gate failed")
    if evidence.get_count > MAX_GETS:
        raise ValueError("V27 requests gate failed")
    if evidence.encoded_bytes > MAX_ENCODED_BYTES:
        raise ValueError("V27 bytes gate failed")
    if not _real_number(evidence.cpu_p99_ms) or evidence.cpu_p99_ms > MAX_CPU_P99_MS:
        raise ValueError("V27 cpu gate failed")
    projection = project_v27_query_latency(evidence, profile)
    if projection.projected_p99_ms > MAX_S3_P99_MS:
        raise ValueError("V27 latency gate failed")

    value = {
        "claim_eligible": False,
        "encoded_bytes": evidence.encoded_bytes,
        "get_count": evidence.get_count,
        "minimum_recall_ppm": evidence.minimum_recall_ppm,
        "projected_p99_micros": round(projection.projected_p99_ms * 1_000),
        "recall_ppm": evidence.recall_ppm,
        "request_waves": projection.request_waves,
        "schema": "borsuk-v27-reduced-s3-preflight-v1",
        "status": "passed",
    }
    receipt = (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )
    launch()
    return receipt
