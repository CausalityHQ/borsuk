#!/usr/bin/env python3
"""Paired 10,000-resample intervals for immutable V104 evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Literal, Sequence

import numpy as np

from scripts.v104_exact_retention_ladder import REGISTERED_RETENTION_PAGES


@dataclass(frozen=True, slots=True)
class V104PairedInterval:
    """One retention arm's paired interval against the 1,024-page control."""

    retained_pages: int
    average_recall10_ppm: tuple[int, int]
    average_recall100_ppm: tuple[int, int]
    p05_recall100_ppm: tuple[int, int]


@dataclass(frozen=True, slots=True)
class V104CiReceipt:
    """Canonical CI receipt bound to one immutable V104 result."""

    schema: str
    result_sha256: str
    query_count: int
    bootstrap_seed: int
    bootstrap_resamples: int
    bootstrap_matrix_sha256: str
    control_retained_pages: int
    arms: tuple[V104PairedInterval, ...]
    status: Literal["verified"]


def _paired_interval(
    challenger: Sequence[int],
    control: Sequence[int],
    matrix: np.ndarray,
    *,
    p05: bool,
) -> tuple[int, int]:
    left = np.asarray(challenger, dtype=np.int64)
    right = np.asarray(control, dtype=np.int64)
    if left.shape != right.shape or left.ndim != 1 or left.size == 0:
        raise ValueError("V104 CI sample cardinality differs")
    differences = np.empty(matrix.shape[0], dtype=np.float64)
    p05_index = max(0, (left.size * 5 + 99) // 100 - 1)
    for start in range(0, matrix.shape[0], 256):
        stop = min(start + 256, matrix.shape[0])
        draws = matrix[start:stop]
        left_draws = left[draws]
        right_draws = right[draws]
        if p05:
            differences[start:stop] = (
                np.partition(left_draws, p05_index, axis=1)[:, p05_index]
                - np.partition(right_draws, p05_index, axis=1)[:, p05_index]
            )
        else:
            differences[start:stop] = (left_draws - right_draws).mean(axis=1)
    lower, upper = np.quantile(differences, [0.025, 0.975], method="nearest")
    return int(np.rint(lower)), int(np.rint(upper))


def reduce_v104_paired_intervals(
    body: bytes, *, expected_result_sha256: str
) -> V104CiReceipt:
    """Authenticate one result and compute paired arm-minus-control intervals."""

    if (
        len(expected_result_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_result_sha256)
        or hashlib.sha256(body).hexdigest() != expected_result_sha256
    ):
        raise ValueError("V104 CI result identity differs")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V104 CI result JSON differs") from error
    canonical = (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )
    if (
        type(value) is not dict
        or canonical != body
        or value.get("schema") != "borsuk-v104-exact-retention-ladder-v1"
        or type(value.get("query_count")) is not int
        or type(value.get("arms")) is not list
    ):
        raise ValueError("V104 CI result authority differs")
    query_count = value["query_count"]
    arms = value["arms"]
    if (
        query_count <= 0
        or len(arms) != len(REGISTERED_RETENTION_PAGES)
        or tuple(arm.get("retained_pages") for arm in arms)
        != REGISTERED_RETENTION_PAGES
    ):
        raise ValueError("V104 CI arm authority differs")
    samples_by_arm: list[list[dict[str, object]]] = []
    for arm in arms:
        samples = arm.get("samples")
        if (
            type(samples) is not list
            or len(samples) != query_count
            or tuple(sample.get("query_ordinal") for sample in samples)
            != tuple(range(query_count))
        ):
            raise ValueError("V104 CI sample authority differs")
        for sample in samples:
            if (
                type(sample.get("recall10_ppm")) is not int
                or type(sample.get("recall100_ppm")) is not int
            ):
                raise ValueError("V104 CI sample authority differs")
        samples_by_arm.append(samples)

    seed = 7_216
    resamples = 10_000
    matrix = np.random.default_rng(seed).integers(
        0, query_count, size=(resamples, query_count), dtype=np.int32
    )
    control = samples_by_arm[-1]
    intervals = []
    for retained_pages, samples in zip(
        REGISTERED_RETENTION_PAGES[:-1], samples_by_arm[:-1], strict=True
    ):
        challenger10 = [int(sample["recall10_ppm"]) for sample in samples]
        challenger100 = [int(sample["recall100_ppm"]) for sample in samples]
        control10 = [int(sample["recall10_ppm"]) for sample in control]
        control100 = [int(sample["recall100_ppm"]) for sample in control]
        intervals.append(
            V104PairedInterval(
                retained_pages=retained_pages,
                average_recall10_ppm=_paired_interval(
                    challenger10, control10, matrix, p05=False
                ),
                average_recall100_ppm=_paired_interval(
                    challenger100, control100, matrix, p05=False
                ),
                p05_recall100_ppm=_paired_interval(
                    challenger100, control100, matrix, p05=True
                ),
            )
        )
    return V104CiReceipt(
        schema="borsuk-v104-exact-retention-ci-v1",
        result_sha256=expected_result_sha256,
        query_count=query_count,
        bootstrap_seed=seed,
        bootstrap_resamples=resamples,
        bootstrap_matrix_sha256=hashlib.sha256(matrix.tobytes(order="C")).hexdigest(),
        control_retained_pages=1_024,
        arms=tuple(intervals),
        status="verified",
    )


def canonical_v104_ci_bytes(receipt: V104CiReceipt) -> bytes:
    """Serialize one typed, verified V104 CI receipt."""

    if (
        not isinstance(receipt, V104CiReceipt)
        or receipt.schema != "borsuk-v104-exact-retention-ci-v1"
        or receipt.status != "verified"
        or receipt.bootstrap_seed != 7_216
        or receipt.bootstrap_resamples != 10_000
        or receipt.control_retained_pages != 1_024
        or tuple(item.retained_pages for item in receipt.arms)
        != REGISTERED_RETENTION_PAGES[:-1]
    ):
        raise ValueError("V104 CI receipt differs")
    return (
        json.dumps(
            asdict(receipt), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
