#!/usr/bin/env python3
"""Canonical per-query evidence for page-centered group code evaluation."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

from scripts.native_geometric_layout_screen import ArtifactIdentity
from scripts.native_page_centered_group_evaluation import (
    GroupSample,
    aggregate_group_samples,
)

SCHEMA = "borsuk-page-centered-group-evidence-v1"
_PAGE_FIELDS = frozenset(
    {"retained_pages", "grouped_pages", "oracle_pages", "exact_pages", "coded_pages"}
)
_RANGE_FIELDS = frozenset({"group_ranges"})


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def write_group_evidence(path: Path, samples: Sequence[GroupSample]) -> ArtifactIdentity:
    """Write all 1,000 samples and the deterministic producer aggregate."""
    metrics = aggregate_group_samples(samples)
    body = _canonical(
        {
            "schema": SCHEMA,
            "samples": [dataclasses.asdict(sample) for sample in samples],
            "metrics": metrics,
        }
    )
    path.write_bytes(body)
    return ArtifactIdentity(
        "page-centered-group-evidence",
        path.resolve().as_uri(),
        hashlib.sha256(body).hexdigest(),
        len(body),
    )


def read_group_evidence(
    path: Path, identity: ArtifactIdentity
) -> tuple[tuple[GroupSample, ...], dict[str, int | str]]:
    """Authenticate canonical bytes and reconstruct strictly typed samples."""
    body = path.read_bytes()
    if (
        identity.role != "page-centered-group-evidence"
        or identity.encoded_bytes != len(body)
        or identity.sha256 != hashlib.sha256(body).hexdigest()
    ):
        raise ValueError("group evidence identity differs")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("group evidence payload differs") from error
    if (
        type(value) is not dict
        or set(value) != {"schema", "samples", "metrics"}
        or value["schema"] != SCHEMA
        or type(value["samples"]) is not list
        or type(value["metrics"]) is not dict
        or body != _canonical(value)
    ):
        raise ValueError("group evidence payload differs")
    fields = {field.name for field in dataclasses.fields(GroupSample)}
    samples: list[GroupSample] = []
    for item in value["samples"]:
        if type(item) is not dict or set(item) != fields:
            raise ValueError("group evidence sample differs")
        parsed = dict(item)
        for field in _PAGE_FIELDS:
            pages = parsed[field]
            if type(pages) is not list or any(type(page) is not int for page in pages):
                raise ValueError("group evidence pages differ")
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
                raise ValueError("group evidence ranges differ")
            parsed[field] = tuple(tuple(group) for group in ranges)
        if any(
            type(number) is not int
            for name, number in parsed.items()
            if name not in _PAGE_FIELDS | _RANGE_FIELDS
        ):
            raise ValueError("group evidence scalar differs")
        samples.append(GroupSample(**parsed))
    actual = tuple(samples)
    metrics = aggregate_group_samples(actual)
    if metrics != value["metrics"]:
        raise ValueError("group evidence aggregate differs")
    return actual, metrics
