#!/usr/bin/env python3
"""Fail-fast controller and monitor for the V26 PQ4 100M Spot campaign."""

from __future__ import annotations

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
