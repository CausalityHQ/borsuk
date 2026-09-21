#!/usr/bin/env python3
"""Independently validate the native geometric layout screen evidence."""

from __future__ import annotations

import dataclasses
import hashlib
import heapq
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    EvaluationLimits,
    GeometricRouterArtifacts,
    LayoutAuthority,
    LayoutMethod,
    geometric_router_evidence_schema,
    read_geometric_router_parquet,
    read_membership_parquet,
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


@dataclasses.dataclass(frozen=True, slots=True)
class GeometricRouterScreenAuthority:
    schema: str
    source: ArtifactIdentity
    queries: ArtifactIdentity
    truth: ArtifactIdentity
    membership: ArtifactIdentity
    tree: ArtifactIdentity
    pages: ArtifactIdentity
    evidence: ArtifactIdentity
    result: ArtifactIdentity
    rows: int
    dimensions: int
    metric: str
    seed: int
    method: LayoutMethod
    maximum_page_rows: int
    maximum_page_bytes: int
    leaf_frontier: int
    limits: EvaluationLimits

    def __post_init__(self) -> None:
        roles = (
            (self.source, "source"),
            (self.queries, "queries"),
            (self.truth, "truth"),
            (self.membership, "geometric-membership"),
            (self.tree, "geometric-router-tree"),
            (self.pages, "geometric-page-representatives"),
            (self.evidence, "geometric-router-evidence"),
            (self.result, "result"),
        )
        if (
            self.schema != "borsuk-native-geometric-router-screen-authority-v1"
            or any(identity.role != role for identity, role in roles)
            or type(self.rows) is not int
            or self.rows <= 0
            or type(self.dimensions) is not int
            or self.dimensions <= 0
            or self.metric != "l2"
            or type(self.seed) is not int
            or self.seed <= 0
            or self.method is not LayoutMethod.TWO_MEANS_480K
            or type(self.maximum_page_rows) is not int
            or self.maximum_page_rows <= 0
            or type(self.maximum_page_bytes) is not int
            or self.maximum_page_bytes <= 0
            or type(self.leaf_frontier) is not int
            or self.leaf_frontier <= 0
        ):
            raise ValueError("geometric router screen authority differs")


@dataclasses.dataclass(frozen=True, slots=True)
class GeometricRouterValidationPaths:
    source: Path
    queries: Path
    truth: Path
    membership: Path
    tree: Path
    pages: Path
    evidence: Path
    result: Path


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


def _geometric_source_schema(dimensions: int) -> pa.Schema:
    return pa.schema(
        [
            pa.field("feature_row_id", pa.uint64(), nullable=False),
            pa.field(
                "embedding",
                pa.list_(pa.field("item", pa.float32(), nullable=False), dimensions),
                nullable=False,
            ),
        ]
    )


def _geometric_query_schema(dimensions: int) -> pa.Schema:
    return pa.schema(
        [
            pa.field("query", pa.uint32(), nullable=False),
            pa.field(
                "vector",
                pa.list_(pa.field("element", pa.float32(), nullable=False), dimensions),
                nullable=False,
            ),
        ]
    )


def _read_geometric_source(
    path: Path,
    expected: ArtifactIdentity,
    rows: int,
    dimensions: int,
) -> tuple[tuple[bytes, ...], np.ndarray]:
    _authenticate(path, expected)
    if pq.read_schema(path) != _geometric_source_schema(dimensions):
        raise ValueError("geometric router source physical schema differs")
    table = pq.read_table(path)
    ids = table["feature_row_id"].combine_chunks()
    embeddings = table["embedding"].combine_chunks()
    if (
        table.num_rows != rows
        or ids.null_count != 0
        or embeddings.null_count != 0
        or embeddings.values.null_count != 0
    ):
        raise ValueError("geometric router source shape differs")
    stable_ids = tuple(_stable_id(value) for value in ids.to_pylist())
    vectors = np.asarray(
        embeddings.values.to_numpy(zero_copy_only=False), dtype=np.float32
    ).reshape(rows, dimensions)
    if len(set(stable_ids)) != rows or not np.isfinite(vectors).all():
        raise ValueError("geometric router source values differ")
    return stable_ids, vectors


def _read_geometric_queries(
    path: Path,
    expected: ArtifactIdentity,
    dimensions: int,
) -> np.ndarray:
    _authenticate(path, expected)
    if pq.read_schema(path) != _geometric_query_schema(dimensions):
        raise ValueError("geometric router query physical schema differs")
    table = pq.read_table(path)
    ordinals = table["query"].combine_chunks()
    vectors = table["vector"].combine_chunks()
    if (
        table.num_rows == 0
        or ordinals.null_count != 0
        or vectors.null_count != 0
        or vectors.values.null_count != 0
        or ordinals.to_pylist() != list(range(table.num_rows))
    ):
        raise ValueError("geometric router query shape differs")
    matrix = np.asarray(
        vectors.values.to_numpy(zero_copy_only=False), dtype=np.float32
    ).reshape(table.num_rows, dimensions)
    if not np.isfinite(matrix).all():
        raise ValueError("geometric router query values differ")
    return matrix


def _validator_projection_token(seed: int, index: int) -> bytes:
    return hashlib.sha256(
        seed.to_bytes(8, "little") + index.to_bytes(4, "little")
    ).digest()


def _validator_srht(query: np.ndarray, seed: int, output_dimensions: int) -> np.ndarray:
    dimensions = len(query)
    padded_dimensions = 1 << (dimensions - 1).bit_length()
    projected = np.zeros(padded_dimensions, dtype=np.float32)
    projected[:dimensions] = query
    signs = np.asarray(
        [
            1.0 if _validator_projection_token(seed, index)[0] & 1 == 0 else -1.0
            for index in range(padded_dimensions)
        ],
        dtype=np.float32,
    )
    projected *= signs
    width = 1
    while width < padded_dimensions:
        for start in range(0, padded_dimensions, width * 2):
            left = projected[start : start + width].copy()
            right = projected[start + width : start + width * 2].copy()
            projected[start : start + width] = left + right
            projected[start + width : start + width * 2] = left - right
        width *= 2
    projected *= np.float32(1.0 / math.sqrt(padded_dimensions))
    permutation = sorted(
        range(padded_dimensions),
        key=lambda index: (
            int.from_bytes(_validator_projection_token(seed, index)[1:9], "little"),
            index,
        ),
    )
    return np.ascontiguousarray(projected[permutation[:output_dimensions]])


def _independent_route(
    router: GeometricRouterArtifacts,
    query: np.ndarray,
    *,
    leaf_frontier: int,
    limits: EvaluationLimits,
) -> tuple[tuple[int, ...], tuple[int, ...], int, int]:
    projected = _validator_srht(query, router.seed, router.projected_dimensions)
    frontier: list[tuple[float, int, int]] = [
        (0.0, int(router.root.is_leaf), router.root.ordinal)
    ]
    retained: list[int] = []
    internal_nodes = 0
    retained_limit = min(leaf_frontier, len(router.pages))
    while frontier and len(retained) < retained_limit:
        penalty, is_leaf, ordinal = heapq.heappop(frontier)
        if is_leaf:
            retained.append(ordinal)
            continue
        node = router.nodes[ordinal]
        signed = math.fsum(
            coefficient * float(projected[index])
            for index, coefficient in enumerate(node.normal)
        ) + node.adjusted_offset
        if not math.isfinite(signed) or node.normal_norm_squared <= 0.0:
            raise ValueError("geometric router split score differs")
        near, far = (node.left, node.right) if signed <= 0.0 else (node.right, node.left)
        heapq.heappush(frontier, (penalty, int(near.is_leaf), near.ordinal))
        heapq.heappush(
            frontier,
            (
                max(penalty, signed * signed / node.normal_norm_squared),
                int(far.is_leaf),
                far.ordinal,
            ),
        )
        internal_nodes += 1
    if len(retained) != retained_limit or len(set(retained)) != retained_limit:
        raise ValueError("geometric router retained leaf evidence differs")
    query64 = query.astype(np.float64)
    ranked_pages = []
    for page_ordinal in retained:
        centroid = np.asarray(router.pages[page_ordinal].centroid, dtype=np.float64)
        delta = query64 - centroid
        distance = float(np.dot(delta, delta))
        if not math.isfinite(distance):
            raise ValueError("geometric router page score differs")
        ranked_pages.append((distance, page_ordinal))
    ranked_pages.sort()
    selected: list[int] = []
    encoded_bytes = 0
    for _, page_ordinal in ranked_pages:
        page_bytes = router.pages[page_ordinal].encoded_page_bytes
        if encoded_bytes + page_bytes > limits.maximum_bytes:
            continue
        selected.append(page_ordinal)
        encoded_bytes += page_bytes
        if len(selected) == limits.maximum_pages:
            break
    if not selected:
        raise ValueError("geometric router independent route selected no page")
    return tuple(sorted(selected)), tuple(retained), encoded_bytes, internal_nodes


def _independent_sq8_page(
    source_ordinals: Sequence[int],
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
) -> tuple[tuple[bytes, ...], np.ndarray]:
    selected = np.ascontiguousarray(vectors[list(source_ordinals)], dtype=np.float32)
    low = selected.min(axis=0)
    high = selected.max(axis=0)
    step = np.asarray((high - low) / np.float32(255.0), dtype=np.float32)
    safe_step = np.where(step == 0.0, np.float32(1.0), step)
    codes = np.asarray(
        np.clip(np.rint((selected - low) / safe_step), 0, 255), dtype=np.uint8
    )
    reconstructed = np.asarray(
        low + codes.astype(np.float32) * step, dtype=np.float32
    )
    return tuple(stable_ids[index] for index in source_ordinals), reconstructed


def _read_geometric_evidence(
    path: Path, expected: ArtifactIdentity
) -> list[dict[str, Any]]:
    _authenticate(path, expected)
    if pq.read_schema(path) != geometric_router_evidence_schema():
        raise ValueError("geometric router evidence physical schema differs")
    table = pq.read_table(path)
    columns = {name: table[name].combine_chunks().to_pylist() for name in table.column_names}
    rows = [
        {name: columns[name][index] for name in table.column_names}
        for index in range(table.num_rows)
    ]
    for ordinal, row in enumerate(rows):
        if (
            row["query_ordinal"] != ordinal
            or type(row["routing_nanoseconds"]) is not int
            or row["routing_nanoseconds"] <= 0
        ):
            raise ValueError("geometric router evidence timing or order differs")
    return rows


def _aggregate_hits(values: Sequence[int], denominator: int) -> tuple[int, int, int]:
    if not values:
        raise ValueError("geometric router aggregate evidence is empty")
    recalls = sorted(value * 1_000_000 // denominator for value in values)
    return (
        sum(values) * 1_000_000 // (len(values) * denominator),
        recalls[math.ceil(0.05 * len(recalls)) - 1],
        recalls[0],
    )


def validate_geometric_router_result(
    paths: GeometricRouterValidationPaths,
    expected: GeometricRouterScreenAuthority,
) -> str:
    stable_ids, vectors = _read_geometric_source(
        paths.source, expected.source, expected.rows, expected.dimensions
    )
    queries = _read_geometric_queries(paths.queries, expected.queries, expected.dimensions)
    truth = _read_truth(paths.truth, expected.truth)
    if len(truth) != len(queries):
        raise ValueError("geometric router query/truth cardinality differs")
    layout_authority = LayoutAuthority(
        schema="borsuk-native-geometric-layout-authority-v1",
        source=expected.source,
        rows=expected.rows,
        dimensions=expected.dimensions,
        metric=expected.metric,
        seed=expected.seed,
        method=expected.method,
        maximum_page_rows=expected.maximum_page_rows,
        maximum_page_bytes=expected.maximum_page_bytes,
    )
    _authenticate(paths.membership, expected.membership)
    membership = read_membership_parquet(paths.membership, layout_authority, stable_ids)
    router = read_geometric_router_parquet(
        paths.tree,
        paths.pages,
        layout_authority,
        membership,
        expected.tree,
        expected.pages,
    )
    observed = _read_geometric_evidence(paths.evidence, expected.evidence)
    if len(observed) != len(queries):
        raise ValueError("geometric router evidence row count differs")

    page_sources: dict[int, list[int]] = {}
    owner_by_id: dict[bytes, int] = {}
    for row in membership:
        page_sources.setdefault(row.page_ordinal, []).append(row.source_ordinal)
        owner_by_id[row.stable_id] = row.page_ordinal
    page_rows = {
        page: _independent_sq8_page(ordinals, stable_ids, vectors)
        for page, ordinals in page_sources.items()
    }
    recomputed: list[dict[str, Any]] = []
    for query_ordinal, query in enumerate(queries):
        neighbors = truth[query_ordinal]
        if any(stable_id not in owner_by_id for stable_id in neighbors):
            raise ValueError("geometric router truth references unknown source ID")
        pages, retained, encoded_bytes, internal_nodes = _independent_route(
            router,
            query,
            leaf_frontier=expected.leaf_frontier,
            limits=expected.limits,
        )
        selected = set(pages)
        ranked: list[tuple[float, bytes]] = []
        query64 = query.astype(np.float64)
        for page in pages:
            ids, reconstructed = page_rows[page]
            delta = reconstructed.astype(np.float64) - query64
            distances = np.einsum("ij,ij->i", delta, delta)
            ranked.extend(
                (float(distance), stable_id)
                for distance, stable_id in zip(distances, ids, strict=True)
            )
        ranked.sort(key=lambda item: (item[0], item[1]))
        returned = {stable_id for _, stable_id in ranked[:100]}
        row = {
            "query_ordinal": query_ordinal,
            "internal_nodes_visited": internal_nodes,
            "retained_leaf_pages": list(retained),
            "selected_page_ordinals": list(pages),
            "encoded_bytes": encoded_bytes,
            "containment_hits_at_10": sum(
                owner_by_id[stable_id] in selected for stable_id in neighbors[:10]
            ),
            "containment_hits_at_100": sum(
                owner_by_id[stable_id] in selected for stable_id in neighbors
            ),
            "ranked_sq8_hits_at_10": sum(
                stable_id in returned for stable_id in neighbors[:10]
            ),
            "ranked_sq8_hits_at_100": sum(
                stable_id in returned for stable_id in neighbors
            ),
        }
        if {
            key: value
            for key, value in observed[query_ordinal].items()
            if key != "routing_nanoseconds"
        } != row:
            raise ValueError("geometric router per-query evidence differs")
        recomputed.append(row)

    containment10, _, _ = _aggregate_hits(
        [row["containment_hits_at_10"] for row in recomputed], 10
    )
    containment100, containment_p05, containment_worst = _aggregate_hits(
        [row["containment_hits_at_100"] for row in recomputed], 100
    )
    ranked10, _, _ = _aggregate_hits(
        [row["ranked_sq8_hits_at_10"] for row in recomputed], 10
    )
    ranked100, ranked_p05, ranked_worst = _aggregate_hits(
        [row["ranked_sq8_hits_at_100"] for row in recomputed], 100
    )
    decision = (
        "pass"
        if ranked10 >= 960_000 and ranked100 >= 975_000 and ranked_p05 >= 900_000
        else "killed"
    )
    aggregate = {
        "containment_mean_recall_at_100_ppm": containment100,
        "containment_p05_recall_at_100_ppm": containment_p05,
        "containment_recall_at_10_ppm": containment10,
        "containment_worst_recall_at_100_ppm": containment_worst,
        "decision": decision,
        "max_bytes": max(row["encoded_bytes"] for row in recomputed),
        "max_pages": max(len(row["selected_page_ordinals"]) for row in recomputed),
        "query_count": len(recomputed),
        "ranked_sq8_mean_recall_at_100_ppm": ranked100,
        "ranked_sq8_p05_recall_at_100_ppm": ranked_p05,
        "ranked_sq8_recall_at_10_ppm": ranked10,
        "ranked_sq8_worst_recall_at_100_ppm": ranked_worst,
    }

    raw_result = _authenticate(paths.result, expected.result)
    try:
        result = json.loads(raw_result)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("geometric router result JSON differs") from error
    canonical = json.dumps(result, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    if raw_result != canonical or type(result) is not dict or set(result) != {
        "claim_eligible",
        "dimensions",
        "evidence",
        "leaf_frontier",
        "layout_method",
        "limits",
        "maximum_page_bytes",
        "maximum_page_rows",
        "membership",
        "metric",
        "pages",
        "queries",
        "result",
        "rows",
        "schema",
        "seed",
        "source",
        "tree",
        "truth",
    }:
        raise ValueError("geometric router result schema differs")
    if result != {
        "claim_eligible": False,
        "dimensions": expected.dimensions,
        "evidence": _identity_payload(expected.evidence),
        "leaf_frontier": expected.leaf_frontier,
        "layout_method": expected.method.value,
        "limits": {
            "maximum_bytes": expected.limits.maximum_bytes,
            "maximum_pages": expected.limits.maximum_pages,
        },
        "maximum_page_bytes": expected.maximum_page_bytes,
        "maximum_page_rows": expected.maximum_page_rows,
        "membership": _identity_payload(expected.membership),
        "metric": expected.metric,
        "pages": _identity_payload(expected.pages),
        "queries": _identity_payload(expected.queries),
        "result": aggregate,
        "rows": expected.rows,
        "schema": "borsuk-native-geometric-router-screen-result-v1",
        "seed": expected.seed,
        "source": _identity_payload(expected.source),
        "tree": _identity_payload(expected.tree),
        "truth": _identity_payload(expected.truth),
    }:
        raise ValueError("geometric router result evidence differs")
    return decision
