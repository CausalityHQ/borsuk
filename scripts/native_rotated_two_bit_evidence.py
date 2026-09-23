#!/usr/bin/env python3
"""Canonical per-query evidence for rotated two-bit group scoring."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

from scripts.native_geometric_layout_screen import ArtifactIdentity
from scripts.native_rotated_two_bit_evaluation import (
    TwoBitSample,
    aggregate_two_bit_samples,
)

SCHEMA = "borsuk-rotated-two-bit-evidence-v1"
_PAGE_FIELDS = frozenset(
    {"retained_pages", "grouped_pages", "exact_pages", "primary_pages", "diagnostic_pages"}
)
_RANGE_FIELDS = frozenset({"group_ranges"})


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def write_two_bit_evidence(path: Path, samples: Sequence[TwoBitSample]) -> ArtifactIdentity:
    metrics = aggregate_two_bit_samples(samples)
    body = _canonical(
        {"schema": SCHEMA, "samples": [dataclasses.asdict(sample) for sample in samples], "metrics": metrics}
    )
    path.write_bytes(body)
    return ArtifactIdentity("rotated-two-bit-evidence", path.resolve().as_uri(), hashlib.sha256(body).hexdigest(), len(body))


def read_two_bit_evidence(
    path: Path, identity: ArtifactIdentity
) -> tuple[tuple[TwoBitSample, ...], dict[str, int | str]]:
    body = path.read_bytes()
    if (
        identity.role != "rotated-two-bit-evidence"
        or identity.encoded_bytes != len(body)
        or identity.sha256 != hashlib.sha256(body).hexdigest()
    ):
        raise ValueError("two-bit evidence identity differs")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("two-bit evidence payload differs") from error
    if (
        type(value) is not dict
        or set(value) != {"schema", "samples", "metrics"}
        or value["schema"] != SCHEMA
        or type(value["samples"]) is not list
        or type(value["metrics"]) is not dict
        or body != _canonical(value)
    ):
        raise ValueError("two-bit evidence payload differs")
    fields = {field.name for field in dataclasses.fields(TwoBitSample)}
    samples: list[TwoBitSample] = []
    for item in value["samples"]:
        if type(item) is not dict or set(item) != fields:
            raise ValueError("two-bit evidence sample differs")
        parsed = dict(item)
        for field in _PAGE_FIELDS:
            pages = parsed[field]
            if type(pages) is not list or any(type(page) is not int for page in pages):
                raise ValueError("two-bit evidence pages differ")
            parsed[field] = tuple(pages)
        for field in _RANGE_FIELDS:
            ranges = parsed[field]
            if (
                type(ranges) is not list
                or any(
                    type(group) is not list
                    or len(group) != 5
                    or any(type(number) is not int for number in group[:4])
                    or type(group[4]) is not str
                    for group in ranges
                )
            ):
                raise ValueError("two-bit evidence ranges differ")
            parsed[field] = tuple(tuple(group) for group in ranges)
        if any(
            type(number) is not int
            for name, number in parsed.items()
            if name not in _PAGE_FIELDS | _RANGE_FIELDS
        ):
            raise ValueError("two-bit evidence scalar differs")
        samples.append(TwoBitSample(**parsed))
    actual = tuple(samples)
    metrics = aggregate_two_bit_samples(actual)
    if metrics != value["metrics"]:
        raise ValueError("two-bit evidence aggregate differs")
    return actual, metrics
