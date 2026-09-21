#!/usr/bin/env python3
"""Independently validate the native geometric layout screen evidence."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    EvaluationLimits,
    LayoutMethod,
)


@dataclasses.dataclass(frozen=True, slots=True)
class LayoutScreenAuthority:
    schema: str
    source: ArtifactIdentity
    truth: ArtifactIdentity
    memberships: tuple[ArtifactIdentity, ...]
    evidence: tuple[ArtifactIdentity, ...]
    result: ArtifactIdentity
    rows: int
    dimensions: int
    metric: str
    seed: int
    limits: EvaluationLimits
    expected_control_mean_ppm: int
    expected_control_p05_ppm: int
    expected_control_worst_ppm: int
    control_tolerance_ppm: int

    def __post_init__(self) -> None:
        if self.schema != "borsuk-native-geometric-layout-screen-authority-v1":
            raise ValueError("layout screen authority schema differs")
        if (
            self.source.role != "source"
            or self.truth.role != "truth"
            or self.result.role != "result"
            or self.rows <= 0
            or self.dimensions <= 0
            or self.metric not in {"l2", "cosine"}
            or self.seed <= 0
        ):
            raise ValueError("layout screen authority differs")
        methods = list(LayoutMethod)
        if [item.role for item in self.memberships] != [
            f"membership:{method.value}" for method in methods
        ] or [item.role for item in self.evidence] != [
            f"evidence:{method.value}" for method in methods
        ]:
            raise ValueError("layout screen artifact roster differs")
        for value in (
            self.expected_control_mean_ppm,
            self.expected_control_p05_ppm,
            self.expected_control_worst_ppm,
            self.control_tolerance_ppm,
        ):
            if type(value) is not int or not 0 <= value <= 1_000_000:
                raise ValueError("layout screen control authority differs")


@dataclasses.dataclass(frozen=True, slots=True)
class ValidationPaths:
    source: Path
    truth: Path
    memberships: tuple[tuple[LayoutMethod, Path], ...]
    evidence: tuple[tuple[LayoutMethod, Path], ...]
    result: Path


@dataclasses.dataclass(frozen=True, slots=True)
class ValidatedLayoutDecision:
    decisions: tuple[tuple[LayoutMethod, str], ...]


def _membership_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("stable_id", pa.binary(), nullable=False),
            pa.field("source_ordinal", pa.uint32(), nullable=False),
            pa.field("page_ordinal", pa.uint32(), nullable=False),
            pa.field("in_page_ordinal", pa.uint16(), nullable=False),
            pa.field("page_rows", pa.uint16(), nullable=False),
            pa.field("encoded_page_bytes", pa.uint32(), nullable=False),
            pa.field("method", pa.string(), nullable=False),
            pa.field("source_sha256", pa.binary(32), nullable=False),
            pa.field("seed", pa.uint64(), nullable=False),
            pa.field("construction_sha256", pa.binary(32), nullable=False),
        ]
    )


def _evidence_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field(
                "selected_page_ordinals",
                pa.list_(pa.field("element", pa.uint32(), nullable=False)),
                nullable=False,
            ),
            pa.field("hits_at_10", pa.uint8(), nullable=False),
            pa.field("hits_at_100", pa.uint8(), nullable=False),
            pa.field("encoded_bytes", pa.uint32(), nullable=False),
            pa.field("recall_at_10_ppm", pa.uint32(), nullable=False),
            pa.field("recall_at_100_ppm", pa.uint32(), nullable=False),
        ]
    )


def _authenticate(path: Path, expected: ArtifactIdentity) -> bytes:
    payload = path.read_bytes()
    if (
        len(payload) != expected.encoded_bytes
        or hashlib.sha256(payload).hexdigest() != expected.sha256
    ):
        raise ValueError(f"{expected.role} artifact identity differs")
    return payload


def _stable_id(value: object) -> bytes:
    if type(value) is bytes and value:
        return value
    if type(value) is int and 0 <= value < 1 << 128:
        return str(value).encode()
    raise ValueError("stable ID differs")


def _read_source(path: Path, expected: ArtifactIdentity, rows: int) -> tuple[bytes, ...]:
    _authenticate(path, expected)
    table = pq.read_table(path, columns=["feature_row_id"])
    if table.num_rows != rows:
        raise ValueError("layout source rows differ")
    ids = tuple(_stable_id(value) for value in table.column(0).combine_chunks().to_pylist())
    if len(set(ids)) != rows:
        raise ValueError("layout source stable IDs differ")
    return ids


def _read_truth(path: Path, expected: ArtifactIdentity) -> tuple[tuple[bytes, ...], ...]:
    _authenticate(path, expected)
    schema = pa.schema(
        [
            pa.field("query", pa.uint32(), nullable=False),
            pa.field(
                "neighbors",
                pa.list_(pa.field("element", pa.int64(), nullable=False), 100),
                nullable=False,
            ),
        ]
    )
    if pq.read_schema(path) != schema:
        raise ValueError("layout truth schema differs")
    table = pq.read_table(path)
    if table.num_rows == 0:
        raise ValueError("layout truth rows differ")
    queries = table["query"].combine_chunks().to_pylist()
    if queries != list(range(table.num_rows)):
        raise ValueError("layout truth query order differs")
    result = tuple(
        tuple(_stable_id(value) for value in neighbors)
        for neighbors in table["neighbors"].combine_chunks().to_pylist()
    )
    if any(len(query) != 100 or len(set(query)) != 100 for query in result):
        raise ValueError("layout truth neighbor identity differs")
    return result


def _read_membership(
    path: Path,
    expected: ArtifactIdentity,
    method: LayoutMethod,
    authority: LayoutScreenAuthority,
    source_ids: Sequence[bytes],
) -> tuple[dict[bytes, int], dict[int, int]]:
    _authenticate(path, expected)
    if pq.read_schema(path) != _membership_schema():
        raise ValueError("layout membership physical schema differs")
    table = pq.read_table(path)
    columns = {name: table[name].combine_chunks().to_pylist() for name in table.column_names}
    if table.num_rows != authority.rows:
        raise ValueError("layout membership rows differ")
    source_ordinals = columns["source_ordinal"]
    if set(source_ordinals) != set(range(authority.rows)):
        raise ValueError("layout membership source coverage differs")
    stable_ids = columns["stable_id"]
    if any(stable_ids[index] != source_ids[source_ordinals[index]] for index in range(authority.rows)):
        raise ValueError("layout membership source binding differs")
    if (
        set(columns["method"]) != {method.value}
        or set(columns["source_sha256"]) != {bytes.fromhex(authority.source.sha256)}
        or set(columns["seed"]) != {authority.seed}
        or len(set(columns["construction_sha256"])) != 1
    ):
        raise ValueError("layout membership authority differs")
    owner_by_id: dict[bytes, int] = {}
    page_bytes: dict[int, int] = {}
    pages: dict[int, list[int]] = {}
    for index, stable_id in enumerate(stable_ids):
        page = columns["page_ordinal"][index]
        owner_by_id[stable_id] = page
        pages.setdefault(page, []).append(index)
        prior = page_bytes.setdefault(page, columns["encoded_page_bytes"][index])
        if prior != columns["encoded_page_bytes"][index]:
            raise ValueError("layout membership page bytes differ")
    if len(owner_by_id) != authority.rows or set(pages) != set(range(len(pages))):
        raise ValueError("layout membership owners differ")
    for page, indexes in pages.items():
        if [columns["in_page_ordinal"][index] for index in indexes] != list(range(len(indexes))):
            raise ValueError("layout membership in-page order differs")
        if any(columns["page_rows"][index] != len(indexes) for index in indexes):
            raise ValueError("layout membership page row count differs")
        if page_bytes[page] <= 0:
            raise ValueError("layout membership page bytes differ")
    return owner_by_id, page_bytes


def _oracle(
    page_hits: Mapping[int, int],
    page_bytes: Mapping[int, int],
    limits: EvaluationLimits,
) -> tuple[tuple[int, ...], int, int]:
    states: dict[tuple[int, int], tuple[int, tuple[int, ...]]] = {(0, 0): (0, ())}
    for page in sorted(page_hits):
        additions: dict[tuple[int, int], tuple[int, tuple[int, ...]]] = {}
        for (count, hits), (encoded, selected) in list(states.items()):
            if count >= limits.maximum_pages:
                continue
            next_encoded = encoded + page_bytes[page]
            if next_encoded > limits.maximum_bytes:
                continue
            key = (count + 1, min(100, hits + page_hits[page]))
            value = (next_encoded, selected + (page,))
            if key not in additions or value < additions[key]:
                additions[key] = value
        for key, value in additions.items():
            if key not in states or value < states[key]:
                states[key] = value
    candidates = [
        (hits, -encoded, tuple(-page for page in selected), selected, encoded)
        for (_, hits), (encoded, selected) in states.items()
    ]
    hits, _, _, selected, encoded = max(candidates)
    return selected, hits, encoded


def _recompute_samples(
    owner_by_id: Mapping[bytes, int],
    page_bytes: Mapping[int, int],
    truth: Sequence[Sequence[bytes]],
    limits: EvaluationLimits,
) -> list[dict[str, Any]]:
    samples = []
    for query_ordinal, neighbors in enumerate(truth):
        hits = {page: 0 for page in page_bytes}
        hits10 = {page: 0 for page in page_bytes}
        for rank, stable_id in enumerate(neighbors):
            if stable_id not in owner_by_id:
                raise ValueError("layout truth references an unknown source ID")
            page = owner_by_id[stable_id]
            hits[page] += 1
            if rank < 10:
                hits10[page] += 1
        selected, hits100, encoded = _oracle(hits, page_bytes, limits)
        selected10 = sum(hits10[page] for page in selected)
        samples.append(
            {
                "query_ordinal": query_ordinal,
                "selected_page_ordinals": list(selected),
                "hits_at_10": selected10,
                "hits_at_100": hits100,
                "encoded_bytes": encoded,
                "recall_at_10_ppm": selected10 * 100_000,
                "recall_at_100_ppm": hits100 * 10_000,
            }
        )
    return samples


def _read_evidence(
    path: Path,
    expected: ArtifactIdentity,
) -> list[dict[str, Any]]:
    _authenticate(path, expected)
    if pq.read_schema(path) != _evidence_schema():
        raise ValueError("layout evidence physical schema differs")
    table = pq.read_table(path)
    columns = {name: table[name].combine_chunks().to_pylist() for name in table.column_names}
    return [
        {name: columns[name][index] for name in table.column_names}
        for index in range(table.num_rows)
    ]


def _identity_payload(identity: ArtifactIdentity) -> dict[str, object]:
    return dataclasses.asdict(identity)


def _decision(method: LayoutMethod, mean: int, p05: int) -> str:
    if method is LayoutMethod.ID_ORDER_256:
        return "control"
    if mean < 975_000 or p05 < 900_000:
        return "killed"
    if mean >= 990_000 and p05 >= 950_000:
        return "advance"
    return "insufficient"


def validate_result(
    paths: ValidationPaths,
    expected: LayoutScreenAuthority,
) -> ValidatedLayoutDecision:
    methods = list(LayoutMethod)
    if [method for method, _ in paths.memberships] != methods or [
        method for method, _ in paths.evidence
    ] != methods:
        raise ValueError("layout validation path roster differs")
    source_ids = _read_source(paths.source, expected.source, expected.rows)
    truth = _read_truth(paths.truth, expected.truth)
    raw_result = _authenticate(paths.result, expected.result)
    try:
        result = json.loads(raw_result)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("layout result JSON differs") from error
    canonical = json.dumps(result, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    if raw_result != canonical:
        raise ValueError("layout result canonical bytes differ")
    if type(result) is not dict or set(result) != {
        "arms",
        "claim_eligible",
        "dimensions",
        "limits",
        "metric",
        "rows",
        "schema",
        "seed",
        "source",
        "truth",
    }:
        raise ValueError("layout result schema differs")
    if (
        result["schema"] != "borsuk-native-geometric-layout-screen-result-v1"
        or result["claim_eligible"] is not False
        or result["rows"] != expected.rows
        or result["dimensions"] != expected.dimensions
        or result["metric"] != expected.metric
        or result["seed"] != expected.seed
        or result["limits"]
        != {
            "maximum_bytes": expected.limits.maximum_bytes,
            "maximum_pages": expected.limits.maximum_pages,
        }
        or result["source"] != _identity_payload(expected.source)
        or result["truth"] != _identity_payload(expected.truth)
    ):
        raise ValueError("layout result authority differs")
    if type(result["arms"]) is not list or len(result["arms"]) != len(methods):
        raise ValueError("layout result arm roster differs")

    decisions = []
    for ordinal, method in enumerate(methods):
        owner_by_id, page_bytes = _read_membership(
            paths.memberships[ordinal][1],
            expected.memberships[ordinal],
            method,
            expected,
            source_ids,
        )
        recomputed = _recompute_samples(owner_by_id, page_bytes, truth, expected.limits)
        observed = _read_evidence(paths.evidence[ordinal][1], expected.evidence[ordinal])
        if observed != recomputed:
            raise ValueError("layout per-query evidence differs")
        query_count = len(recomputed)
        recall10 = sum(sample["hits_at_10"] for sample in recomputed) * 1_000_000 // (
            query_count * 10
        )
        mean = sum(sample["hits_at_100"] for sample in recomputed) * 1_000_000 // (
            query_count * 100
        )
        recalls = sorted(sample["recall_at_100_ppm"] for sample in recomputed)
        p05 = recalls[math.ceil(0.05 * query_count) - 1]
        worst = recalls[0]
        decision = _decision(method, mean, p05)
        arm = result["arms"][ordinal]
        if type(arm) is not dict or set(arm) != {
            "decision",
            "evidence",
            "mean_recall_at_100_ppm",
            "membership",
            "method",
            "p05_recall_at_100_ppm",
            "recall_at_10_ppm",
            "worst_recall_at_100_ppm",
        }:
            raise ValueError("layout result arm schema differs")
        if arm != {
            "decision": decision,
            "evidence": _identity_payload(expected.evidence[ordinal]),
            "mean_recall_at_100_ppm": mean,
            "membership": _identity_payload(expected.memberships[ordinal]),
            "method": method.value,
            "p05_recall_at_100_ppm": p05,
            "recall_at_10_ppm": recall10,
            "worst_recall_at_100_ppm": worst,
        }:
            raise ValueError("layout result arm evidence differs")
        decisions.append((method, decision))

    control_arm = result["arms"][0]
    if (
        abs(control_arm["mean_recall_at_100_ppm"] - expected.expected_control_mean_ppm)
        > expected.control_tolerance_ppm
        or abs(control_arm["p05_recall_at_100_ppm"] - expected.expected_control_p05_ppm)
        > expected.control_tolerance_ppm
        or abs(control_arm["worst_recall_at_100_ppm"] - expected.expected_control_worst_ppm)
        > expected.control_tolerance_ppm
    ):
        raise ValueError("layout reproduction control differs")
    return ValidatedLayoutDecision(decisions=tuple(decisions))
