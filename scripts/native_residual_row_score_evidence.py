#!/usr/bin/env python3
"""Canonical, digest-bound per-query residual and paired-control evidence."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

from scripts.native_geometric_layout_screen import ArtifactIdentity
from scripts.native_residual_row_score_evaluation import (
    ResidualSample,
    aggregate_residual_samples,
)
from scripts.native_row_score_evaluation import RowScoreSample

SCHEMA = "borsuk-residual-row-score-evidence-v1"
_BASELINE_PAGES = frozenset(
    {"retained_pages", "restricted_oracle_pages", "pq_pages", "exact_pages"}
)


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def write_residual_evidence(
    path: Path, samples: Sequence[ResidualSample]
) -> ArtifactIdentity:
    """Write every code range, selected page and paired hit count."""
    metrics = aggregate_residual_samples(samples)
    payload = _canonical(
        {
            "schema": SCHEMA,
            "samples": [dataclasses.asdict(sample) for sample in samples],
            "metrics": metrics,
        }
    )
    path.write_bytes(payload)
    return ArtifactIdentity(
        "residual-row-score-evidence",
        path.resolve().as_uri(),
        hashlib.sha256(payload).hexdigest(),
        len(payload),
    )


def _pages(value: object, label: str) -> tuple[int, ...]:
    if type(value) is not list or any(type(number) is not int for number in value):
        raise ValueError(f"residual evidence {label} differs")
    return tuple(value)


def _blocks(value: object, label: str) -> tuple[tuple[int, int, int, int], ...]:
    if (
        type(value) is not list
        or any(
            type(block) is not list
            or len(block) != 4
            or any(type(number) is not int for number in block)
            for block in value
        )
    ):
        raise ValueError(f"residual evidence {label} differs")
    return tuple(tuple(block) for block in value)


def _baseline(value: object) -> RowScoreSample:
    fields = {field.name for field in dataclasses.fields(RowScoreSample)}
    if type(value) is not dict or set(value) != fields:
        raise ValueError("residual evidence baseline differs")
    values = dict(value)
    for name in _BASELINE_PAGES:
        values[name] = _pages(values[name], name)
    values["code_blocks"] = _blocks(values["code_blocks"], "baseline code blocks")
    if any(
        type(number) is not int
        for name, number in values.items()
        if name not in _BASELINE_PAGES | {"code_blocks"}
    ):
        raise ValueError("residual evidence baseline scalar differs")
    return RowScoreSample(**values)


def read_residual_evidence(
    path: Path, identity: ArtifactIdentity
) -> tuple[tuple[ResidualSample, ...], dict[str, int | str]]:
    """Authenticate canonical bytes and recompute every aggregate."""
    payload = path.read_bytes()
    if (
        identity.role != "residual-row-score-evidence"
        or identity.encoded_bytes != len(payload)
        or identity.sha256 != hashlib.sha256(payload).hexdigest()
    ):
        raise ValueError("residual evidence identity differs")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("residual evidence payload differs") from error
    if (
        type(value) is not dict
        or set(value) != {"schema", "samples", "metrics"}
        or value["schema"] != SCHEMA
        or type(value["samples"]) is not list
        or type(value["metrics"]) is not dict
        or payload != _canonical(value)
    ):
        raise ValueError("residual evidence payload differs")
    fields = {field.name for field in dataclasses.fields(ResidualSample)}
    samples = []
    for item in value["samples"]:
        if type(item) is not dict or set(item) != fields:
            raise ValueError("residual evidence sample differs")
        values = dict(item)
        values["baseline"] = _baseline(values["baseline"])
        values["code_blocks"] = _blocks(values["code_blocks"], "code blocks")
        values["residual_pages"] = _pages(values["residual_pages"], "pages")
        if any(
            type(number) is not int
            for name, number in values.items()
            if name not in {"baseline", "code_blocks", "residual_pages"}
        ):
            raise ValueError("residual evidence scalar differs")
        samples.append(ResidualSample(**values))
    actual = tuple(samples)
    metrics = aggregate_residual_samples(actual)
    if metrics != value["metrics"]:
        raise ValueError("residual evidence aggregates differ")
    return actual, metrics
