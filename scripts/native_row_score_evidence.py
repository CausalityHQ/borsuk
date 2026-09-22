#!/usr/bin/env python3
"""Canonical per-query evidence for the row-score 100k falsifier."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

from scripts.native_geometric_layout_screen import ArtifactIdentity
from scripts.native_row_score_evaluation import RowScoreSample, aggregate_samples

SCHEMA = "borsuk-row-score-evidence-v1"
_TUPLE_FIELDS = frozenset(
    {"retained_pages", "restricted_oracle_pages", "pq_pages", "exact_pages"}
)


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def write_evidence(path: Path, samples: Sequence[RowScoreSample]) -> ArtifactIdentity:
    """Write all query choices and four diagnostic containment arms."""
    metrics = aggregate_samples(samples)
    payload = _canonical(
        {
            "schema": SCHEMA,
            "samples": [dataclasses.asdict(sample) for sample in samples],
            "metrics": metrics,
        }
    )
    path.write_bytes(payload)
    return ArtifactIdentity(
        "row-score-evidence", path.resolve().as_uri(),
        hashlib.sha256(payload).hexdigest(), len(payload),
    )


def read_evidence(
    path: Path, identity: ArtifactIdentity
) -> tuple[tuple[RowScoreSample, ...], dict[str, int | str]]:
    """Authenticate canonical bytes and recompute all aggregate fields."""
    payload = path.read_bytes()
    if (
        identity.role != "row-score-evidence"
        or identity.encoded_bytes != len(payload)
        or identity.sha256 != hashlib.sha256(payload).hexdigest()
    ):
        raise ValueError("row-score evidence identity differs")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("row-score evidence payload differs") from error
    if (
        type(value) is not dict
        or set(value) != {"schema", "samples", "metrics"}
        or value["schema"] != SCHEMA
        or type(value["samples"]) is not list
        or type(value["metrics"]) is not dict
        or payload != _canonical(value)
    ):
        raise ValueError("row-score evidence payload differs")
    fields = {field.name for field in dataclasses.fields(RowScoreSample)}
    samples = []
    for item in value["samples"]:
        if type(item) is not dict or set(item) != fields:
            raise ValueError("row-score evidence sample differs")
        values = dict(item)
        for name in _TUPLE_FIELDS:
            raw = values[name]
            if type(raw) is not list or any(type(number) is not int for number in raw):
                raise ValueError("row-score evidence page list differs")
            values[name] = tuple(raw)
        blocks = values["code_blocks"]
        if (
            type(blocks) is not list
            or any(
                type(block) is not list
                or len(block) != 4
                or any(type(number) is not int for number in block)
                for block in blocks
            )
        ):
            raise ValueError("row-score evidence code blocks differ")
        values["code_blocks"] = tuple(tuple(block) for block in blocks)
        if any(type(number) is not int for name, number in values.items() if name not in _TUPLE_FIELDS | {"code_blocks"}):
            raise ValueError("row-score evidence scalar differs")
        samples.append(RowScoreSample(**values))
    actual = tuple(samples)
    metrics = aggregate_samples(actual)
    if metrics != value["metrics"]:
        raise ValueError("row-score evidence aggregates differ")
    return actual, metrics
