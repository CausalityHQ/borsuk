#!/usr/bin/env python3
"""Paired V105 768-page confirmation against immutable V99 exact evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Literal

import numpy as np

from scripts.v104_exact_retention_ladder_ci import _paired_interval


@dataclass(frozen=True, slots=True)
class V105Comparison:
    """Canonical matched comparison of two immutable exact-ranking results."""

    schema: str
    v105_result_sha256: str
    v99_result_sha256: str
    query_count: int
    challenger_retained_pages: int
    control_retained_pages: int
    bootstrap_seed: int
    bootstrap_resamples: int
    bootstrap_matrix_sha256: str
    average_recall10_ppm: tuple[int, int]
    average_recall100_ppm: tuple[int, int]
    p05_recall100_ppm: tuple[int, int]
    challenger_quality_gate_passed: bool
    challenger_resource_gate_passed: bool
    classification: Literal["exact-retention-confirmed", "exact-retention-rejected"]
    status: Literal["verified"]


def _load(body: bytes, expected_sha256: str, label: str) -> dict[str, object]:
    if (
        len(expected_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_sha256)
        or hashlib.sha256(body).hexdigest() != expected_sha256
    ):
        raise ValueError(f"V105 comparison {label} identity differs")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"V105 comparison {label} JSON differs") from error
    canonical = (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )
    if type(value) is not dict or canonical != body:
        raise ValueError(f"V105 comparison {label} canonical bytes differs")
    return value


def compare_v105_to_v99(
    v105_body: bytes,
    v99_body: bytes,
    *,
    expected_v105_sha256: str,
    expected_v99_sha256: str,
) -> V105Comparison:
    """Pair one full V105 result with V99's existing exact 1,024-page samples."""

    challenger = _load(v105_body, expected_v105_sha256, "V105 result")
    control = _load(v99_body, expected_v99_sha256, "V99 result")
    if (
        challenger.get("schema") != "borsuk-v105-exact-retention-confirmation-v1"
        or control.get("schema") != "borsuk-v99-ranked-gap-range-router-v1"
        or type(challenger.get("query_count")) is not int
        or challenger["query_count"] != control.get("query_count")
    ):
        raise ValueError("V105 comparison result authority differs")
    query_count = int(challenger["query_count"])
    challenger_authority = challenger.get("authority")
    control_authority = control.get("authority")
    if type(challenger_authority) is not dict or type(control_authority) is not dict:
        raise ValueError("V105 comparison authority differs")
    for field in (
        "critique_result_sha256",
        "page_map_sha256",
        "dimensions",
        "seed",
        "identities",
    ):
        if challenger_authority.get(field) != control_authority.get(field):
            raise ValueError("V105 comparison authority differs")
    challenger_config = challenger.get("config")
    control_config = control.get("config")
    if type(challenger_config) is not dict or type(control_config) is not dict:
        raise ValueError("V105 comparison configuration differs")
    for field in (
        "pages_per_root",
        "maximum_root_groups",
        "maximum_exposed_pages",
        "shortlist_rows",
        "maximum_gets",
        "maximum_bytes",
    ):
        if challenger_config.get(field) != control_config.get(field):
            raise ValueError("V105 comparison configuration differs")
    if (
        control_config.get("retained_pages") != 1_024
        or control_config.get("maximum_scanned_rows") != 262_144
    ):
        raise ValueError("V105 comparison control differs")

    arm = challenger.get("arm")
    control_samples = control.get("exact_samples")
    if (
        type(arm) is not dict
        or arm.get("retained_pages") != 768
        or type(arm.get("samples")) is not list
        or type(control_samples) is not list
        or len(arm["samples"]) != query_count
        or len(control_samples) != query_count
    ):
        raise ValueError("V105 comparison sample authority differs")
    challenger10: list[int] = []
    challenger100: list[int] = []
    control10: list[int] = []
    control100: list[int] = []
    for ordinal, (left, right) in enumerate(
        zip(arm["samples"], control_samples, strict=True)
    ):
        if type(left) is not dict or type(right) is not dict:
            raise ValueError("V105 comparison query binding differs")
        for field in ("query_ordinal", "truth_ids", "truth_pages"):
            if left.get(field) != right.get(field):
                raise ValueError("V105 comparison query binding differs")
        if left.get("query_ordinal") != ordinal:
            raise ValueError("V105 comparison query binding differs")
        values = (
            left.get("recall10_ppm"),
            left.get("recall100_ppm"),
            right.get("recall10_ppm"),
            right.get("recall100_ppm"),
        )
        if any(type(value) is not int for value in values):
            raise ValueError("V105 comparison recall evidence differs")
        challenger10.append(values[0])
        challenger100.append(values[1])
        control10.append(values[2])
        control100.append(values[3])

    aggregate = arm.get("aggregate")
    if type(aggregate) is not dict:
        raise ValueError("V105 comparison aggregate differs")
    quality = aggregate.get("quality_gate_passed")
    resource = aggregate.get("resource_gate_passed")
    if type(quality) is not bool or type(resource) is not bool:
        raise ValueError("V105 comparison aggregate differs")
    classification = (
        "exact-retention-confirmed"
        if quality and resource
        else "exact-retention-rejected"
    )
    if challenger.get("classification") != classification:
        raise ValueError("V105 comparison classification differs")

    seed = 7_216
    resamples = 10_000
    matrix = np.random.default_rng(seed).integers(
        0, query_count, size=(resamples, query_count), dtype=np.int32
    )
    return V105Comparison(
        schema="borsuk-v105-v99-exact-comparison-v1",
        v105_result_sha256=expected_v105_sha256,
        v99_result_sha256=expected_v99_sha256,
        query_count=query_count,
        challenger_retained_pages=768,
        control_retained_pages=1_024,
        bootstrap_seed=seed,
        bootstrap_resamples=resamples,
        bootstrap_matrix_sha256=hashlib.sha256(matrix.tobytes(order="C")).hexdigest(),
        average_recall10_ppm=_paired_interval(
            challenger10, control10, matrix, p05=False
        ),
        average_recall100_ppm=_paired_interval(
            challenger100, control100, matrix, p05=False
        ),
        p05_recall100_ppm=_paired_interval(
            challenger100, control100, matrix, p05=True
        ),
        challenger_quality_gate_passed=quality,
        challenger_resource_gate_passed=resource,
        classification=classification,
        status="verified",
    )


def canonical_v105_comparison_bytes(receipt: V105Comparison) -> bytes:
    """Serialize one typed, verified V105/V99 comparison receipt."""

    if (
        not isinstance(receipt, V105Comparison)
        or receipt.schema != "borsuk-v105-v99-exact-comparison-v1"
        or receipt.status != "verified"
        or receipt.challenger_retained_pages != 768
        or receipt.control_retained_pages != 1_024
        or receipt.bootstrap_seed != 7_216
        or receipt.bootstrap_resamples != 10_000
    ):
        raise ValueError("V105 comparison receipt differs")
    return (
        json.dumps(
            asdict(receipt), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
