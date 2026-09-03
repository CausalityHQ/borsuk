#!/usr/bin/env python3
"""Fail-fast controller and monitor for the V26 PQ4 100M Spot campaign."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class MonitorLimits:
    """Registered resource, progress, and wall-clock stop limits."""

    build_rss_bytes: int
    serve_rss_bytes: int
    psi_full_avg10: float
    build_stall_seconds: int
    serve_stall_seconds: int
    build_wall_seconds: int
    serve_wall_seconds: int


@dataclass(frozen=True)
class MonitorSnapshot:
    """One authoritative worker health/progress observation."""

    elapsed_seconds: int
    phase: Literal["build", "serve"]
    progress: int
    rss_bytes: int
    psi_full_avg10: float
    swap_bytes: int
    instance_state: str
    system_status: str
    instance_status: str


@dataclass(frozen=True)
class MonitorDecision:
    """Continue or stop disposition for one observation."""

    action: Literal["continue", "stop"]
    reason: str


@dataclass(frozen=True)
class S3LatencyProfile:
    """Measured or preregistered S3 transfer assumptions."""

    name: str
    request_latency_ms: float
    aggregate_bytes_per_second: int
    parallel_requests: int


@dataclass(frozen=True)
class S3TransferProjection:
    """Fail-fast wall projection from exact request and byte counts."""

    profile: S3LatencyProfile
    requests: int
    bytes_read: int
    request_waves: int
    request_seconds: float
    transfer_seconds: float
    wall_seconds: float
    wall_budget_seconds: float
    within_wall_budget: bool


class S3RequestCounter:
    """Count planned S3 reads by disjoint role without issuing requests."""

    def __init__(self) -> None:
        self._roles: dict[str, tuple[int, int]] = {}

    def add(self, role: str, *, requests: int, bytes_read: int) -> None:
        """Register one exact nonempty role count once."""

        if (
            not role
            or role in self._roles
            or isinstance(requests, bool)
            or not isinstance(requests, int)
            or requests <= 0
            or isinstance(bytes_read, bool)
            or not isinstance(bytes_read, int)
            or bytes_read <= 0
        ):
            raise ValueError("S3 request count differs")
        self._roles[role] = (requests, bytes_read)

    def counts(self) -> tuple[int, int]:
        """Return exact total requests and bytes."""

        return (
            sum(value[0] for value in self._roles.values()),
            sum(value[1] for value in self._roles.values()),
        )


def estimate_s3_transfer(
    counter: S3RequestCounter,
    profile: S3LatencyProfile,
    *,
    wall_budget_seconds: float,
) -> S3TransferProjection:
    """Project cold transfer wall time without touching S3.

    Request latency is paid once per parallel wave. Throughput is aggregate across all requests,
    so concurrency is deliberately not applied to the byte-transfer term a second time.
    """

    if (
        not profile.name
        or not math.isfinite(profile.request_latency_ms)
        or profile.request_latency_ms < 0
        or isinstance(profile.aggregate_bytes_per_second, bool)
        or profile.aggregate_bytes_per_second <= 0
        or isinstance(profile.parallel_requests, bool)
        or profile.parallel_requests <= 0
        or not math.isfinite(wall_budget_seconds)
        or wall_budget_seconds <= 0
    ):
        raise ValueError("S3 latency profile differs")
    requests, bytes_read = counter.counts()
    if requests <= 0 or bytes_read <= 0:
        raise ValueError("S3 transfer plan is empty")
    request_waves = math.ceil(requests / profile.parallel_requests)
    request_seconds = request_waves * profile.request_latency_ms / 1_000.0
    transfer_seconds = bytes_read / profile.aggregate_bytes_per_second
    wall_seconds = request_seconds + transfer_seconds
    return S3TransferProjection(
        profile=profile,
        requests=requests,
        bytes_read=bytes_read,
        request_waves=request_waves,
        request_seconds=request_seconds,
        transfer_seconds=transfer_seconds,
        wall_seconds=wall_seconds,
        wall_budget_seconds=wall_budget_seconds,
        within_wall_budget=wall_seconds <= wall_budget_seconds,
    )


class CampaignMonitor:
    """Stateful monitor that never launches or replaces work."""

    def __init__(self, limits: MonitorLimits, *, initial_swap_bytes: int) -> None:
        if initial_swap_bytes < 0:
            raise ValueError("initial swap differs")
        self._limits = limits
        self._initial_swap_bytes = initial_swap_bytes
        self._phase: str | None = None
        self._last_progress = 0
        self._last_progress_at = 0
        self._psi_breaches = 0

    def observe(self, value: MonitorSnapshot) -> MonitorDecision:
        """Classify one snapshot against the registered stop rules."""

        if value.phase not in {"build", "serve"}:
            raise ValueError("monitor phase differs")
        if self._phase != value.phase:
            self._phase = value.phase
            self._last_progress = value.progress
            self._last_progress_at = value.elapsed_seconds
            self._psi_breaches = 0
        elif value.progress > self._last_progress:
            self._last_progress = value.progress
            self._last_progress_at = value.elapsed_seconds

        if value.instance_state != "running":
            return MonitorDecision("stop", "instance-terminal")
        if value.system_status != "ok":
            return MonitorDecision("stop", "ec2-system")
        if value.instance_status != "ok":
            return MonitorDecision("stop", "ec2-instance")
        rss_limit = (
            self._limits.build_rss_bytes
            if value.phase == "build"
            else self._limits.serve_rss_bytes
        )
        if value.rss_bytes > rss_limit:
            return MonitorDecision("stop", f"{value.phase}-rss")
        if value.swap_bytes > self._initial_swap_bytes:
            return MonitorDecision("stop", "swap-growth")
        wall_limit = (
            self._limits.build_wall_seconds
            if value.phase == "build"
            else self._limits.serve_wall_seconds
        )
        if value.elapsed_seconds > wall_limit:
            return MonitorDecision("stop", f"{value.phase}-wall")

        if value.psi_full_avg10 > self._limits.psi_full_avg10:
            self._psi_breaches += 1
        else:
            self._psi_breaches = 0
        if self._psi_breaches >= 3:
            return MonitorDecision("stop", "memory-psi")

        stall_limit = (
            self._limits.build_stall_seconds
            if value.phase == "build"
            else self._limits.serve_stall_seconds
        )
        if value.elapsed_seconds - self._last_progress_at > stall_limit:
            return MonitorDecision("stop", f"{value.phase}-stalled")
        return MonitorDecision("continue", "healthy")


@dataclass
class _Attempt:
    attempt_id: str
    instance_id: str
    status: str


class AttemptRegistry:
    """Prevent duplicate workers and allow replacement only after Spot interruption."""

    def __init__(self, shard_ordinals: range) -> None:
        if tuple(shard_ordinals) != tuple(range(10)):
            raise ValueError("campaign shard ordinals differ")
        self._attempts: dict[int, _Attempt] = {}

    def start(self, shard_ordinal: int, attempt_id: str, instance_id: str) -> None:
        if shard_ordinal not in range(10) or not attempt_id or not instance_id:
            raise ValueError("attempt identity differs")
        previous = self._attempts.get(shard_ordinal)
        if previous is not None and previous.status != "spot-interrupted":
            raise ValueError("shard already has a non-replaceable attempt")
        self._attempts[shard_ordinal] = _Attempt(attempt_id, instance_id, "active")

    def finish(self, shard_ordinal: int, attempt_id: str, status: str) -> None:
        attempt = self._attempts.get(shard_ordinal)
        if (
            attempt is None
            or attempt.status != "active"
            or attempt.attempt_id != attempt_id
            or status not in {"passed", "failed", "spot-interrupted"}
        ):
            raise ValueError("attempt terminal transition differs")
        attempt.status = status

    def active_attempts(self) -> dict[int, tuple[str, str]]:
        return {
            ordinal: (attempt.attempt_id, attempt.instance_id)
            for ordinal, attempt in self._attempts.items()
            if attempt.status == "active"
        }
