#!/usr/bin/env python3
"""Pure fail-fast latency projection for the V32 S3 serving path."""

from __future__ import annotations

import json
from dataclasses import dataclass

PAGE_COUNT = 16
MAX_ENCODED_BYTES = 3_145_728
PERFECT_RECALL_PPM = 1_000_000
QUERY_COUNT = 32
STANDARD_P99_NS = 150_000_000
EXPRESS_P99_NS = 15_000_000


@dataclass(frozen=True)
class V32LatencyEvidence:
    """Measured compute and exact bounded work from one V32 quality run."""

    routing_p99_ns: int
    decode_rerank_p99_ns: int
    get_count: int
    encoded_bytes: int
    recall_ppm: int
    minimum_recall_ppm: int
    perfect_queries: int


@dataclass(frozen=True)
class V32S3LatencyProfile:
    """Injected one-wave request latency and aggregate transfer throughput."""

    tier: str
    request_p99_ns: int
    aggregate_bytes_per_second: int


@dataclass(frozen=True)
class V32LatencyProjection:
    """Integer-only decomposition of one concurrent 16-object read wave."""

    tier: str
    get_count: int
    encoded_bytes: int
    request_waves: int
    routing_ns: int
    request_ns: int
    transfer_ns: int
    decode_rerank_ns: int
    projected_p99_ns: int


def _nonnegative_integer(value: object) -> bool:
    return type(value) is int and value >= 0


def project_v32_s3_latency(
    evidence: V32LatencyEvidence,
    profile: V32S3LatencyProfile,
) -> V32LatencyProjection:
    """Project p99 without sleeping, S3 access, or serializing concurrent RTTs."""

    if (
        not _nonnegative_integer(evidence.routing_p99_ns)
        or not _nonnegative_integer(evidence.decode_rerank_p99_ns)
        or type(evidence.get_count) is not int
        or evidence.get_count != PAGE_COUNT
        or type(evidence.encoded_bytes) is not int
        or not 0 < evidence.encoded_bytes <= MAX_ENCODED_BYTES
        or profile.tier not in {"standard", "express"}
        or not _nonnegative_integer(profile.request_p99_ns)
        or type(profile.aggregate_bytes_per_second) is not int
        or profile.aggregate_bytes_per_second <= 0
    ):
        raise ValueError("V32 latency projection authority differs")
    transfer_ns = (
        evidence.encoded_bytes * 1_000_000_000
        + profile.aggregate_bytes_per_second
        - 1
    ) // profile.aggregate_bytes_per_second
    projected_p99_ns = (
        evidence.routing_p99_ns
        + profile.request_p99_ns
        + transfer_ns
        + evidence.decode_rerank_p99_ns
    )
    return V32LatencyProjection(
        tier=profile.tier,
        get_count=evidence.get_count,
        encoded_bytes=evidence.encoded_bytes,
        request_waves=1,
        routing_ns=evidence.routing_p99_ns,
        request_ns=profile.request_p99_ns,
        transfer_ns=transfer_ns,
        decode_rerank_ns=evidence.decode_rerank_p99_ns,
        projected_p99_ns=projected_p99_ns,
    )


def preflight_v32_s3_latency(
    evidence: V32LatencyEvidence,
    profile: V32S3LatencyProfile,
) -> bytes:
    """Reject non-perfect or latency-infeasible work before external execution."""

    if type(evidence.recall_ppm) is not int or evidence.recall_ppm != PERFECT_RECALL_PPM:
        raise ValueError("V32 aggregate recall gate failed")
    if (
        type(evidence.minimum_recall_ppm) is not int
        or evidence.minimum_recall_ppm != PERFECT_RECALL_PPM
    ):
        raise ValueError("V32 minimum recall gate failed")
    if type(evidence.perfect_queries) is not int or evidence.perfect_queries != QUERY_COUNT:
        raise ValueError("V32 perfect queries gate failed")
    if type(evidence.get_count) is not int or evidence.get_count != PAGE_COUNT:
        raise ValueError("V32 requests gate failed")
    if (
        type(evidence.encoded_bytes) is not int
        or not 0 < evidence.encoded_bytes <= MAX_ENCODED_BYTES
    ):
        raise ValueError("V32 bytes gate failed")
    projection = project_v32_s3_latency(evidence, profile)
    limit = STANDARD_P99_NS if profile.tier == "standard" else EXPRESS_P99_NS
    if projection.projected_p99_ns > limit:
        raise ValueError(f"V32 {profile.tier} latency gate failed")
    value = {
        "claim_eligible": False,
        "decode_rerank_ns": projection.decode_rerank_ns,
        "encoded_bytes": projection.encoded_bytes,
        "get_count": projection.get_count,
        "projected_p99_ns": projection.projected_p99_ns,
        "request_ns": projection.request_ns,
        "request_waves": projection.request_waves,
        "routing_ns": projection.routing_ns,
        "schema": "borsuk-v32-s3-latency-preflight-v1",
        "status": "passed",
        "tier": projection.tier,
        "transfer_ns": projection.transfer_ns,
    }
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )
