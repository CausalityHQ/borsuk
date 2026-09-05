"""Deterministic metadata-only reducers for the V33 shape proxy."""

import math
import struct
from dataclasses import dataclass


def _distance(left, right):
    if len(left) != len(right) or not left:
        raise ValueError("vector dimension differs")
    total = 0.0
    for a, b in zip(left, right, strict=True):
        if not math.isfinite(a) or not math.isfinite(b):
            raise ValueError("vector is nonfinite")
        delta = a - b
        total += delta * delta
    if not math.isfinite(total):
        raise ValueError("distance is nonfinite")
    return total


def _f16(value):
    if not math.isfinite(value):
        raise ValueError("prototype is nonfinite")
    rounded = struct.unpack("<e", struct.pack("<e", value))[0]
    if not math.isfinite(rounded):
        raise ValueError("prototype cannot be represented as f16")
    return rounded


@dataclass(frozen=True)
class ParentSummary:
    ordinal: int
    rows: int
    centroid: tuple[float, ...]


@dataclass(frozen=True)
class GroupProxy:
    ordinal: int
    rows: int
    prototypes: tuple[tuple[float, ...], ...]


@dataclass(frozen=True)
class OwnerInclusionResult:
    included_owners: int
    maximum_rows: int
    passed: bool
    perfect_queries: int
    query_count: int
    total_owners: int


def _validate_parents(parents):
    if not parents:
        raise ValueError("group has no parents")
    dimension = len(parents[0].centroid)
    seen = set()
    for parent in parents:
        if (
            type(parent.ordinal) is not int
            or parent.ordinal < 0
            or parent.ordinal in seen
            or type(parent.rows) is not int
            or parent.rows <= 0
            or len(parent.centroid) != dimension
        ):
            raise ValueError("parent authority differs")
        _distance(parent.centroid, parent.centroid)
        seen.add(parent.ordinal)
    return dimension


def build_group_prototypes(parents, *, prototype_count, iterations):
    """Build a fixed population-weighted farthest-first/Lloyd proxy."""

    parents = tuple(sorted(parents, key=lambda parent: parent.ordinal))
    dimension = _validate_parents(parents)
    if (
        type(prototype_count) is not int
        or prototype_count <= 0
        or type(iterations) is not int
        or iterations <= 0
    ):
        raise ValueError("prototype configuration differs")
    prototype_count = min(prototype_count, len(parents))
    total_rows = sum(parent.rows for parent in parents)
    mean = tuple(
        sum(parent.rows * parent.centroid[d] for parent in parents) / total_rows
        for d in range(dimension)
    )
    first = min(parents, key=lambda parent: (_distance(parent.centroid, mean), parent.ordinal))
    centers = [first.centroid]
    selected = {first.ordinal}
    while len(centers) < prototype_count:
        choice = min(
            (parent for parent in parents if parent.ordinal not in selected),
            key=lambda parent: (
                -min(_distance(parent.centroid, center) for center in centers),
                parent.ordinal,
            ),
        )
        selected.add(choice.ordinal)
        centers.append(choice.centroid)

    for _ in range(iterations):
        assignments = [[] for _ in centers]
        for parent in parents:
            index = min(
                range(len(centers)),
                key=lambda candidate: (_distance(parent.centroid, centers[candidate]), candidate),
            )
            assignments[index].append(parent)
        updated = []
        for index, members in enumerate(assignments):
            if not members:
                updated.append(centers[index])
                continue
            rows = sum(parent.rows for parent in members)
            updated.append(
                tuple(
                    sum(parent.rows * parent.centroid[d] for parent in members) / rows
                    for d in range(dimension)
                )
            )
        centers = updated
    return tuple(tuple(_f16(value) for value in center) for center in centers)


def materialize_group_proxies(groups, parents, *, prototype_count, iterations):
    """Bind a frozen group partition to parent populations and prototypes."""

    parents = tuple(sorted(parents, key=lambda parent: parent.ordinal))
    _validate_parents(parents)
    parent_by_ordinal = {parent.ordinal: parent for parent in parents}
    seen = set()
    materialized = []
    for expected_ordinal, group in enumerate(groups):
        ordinal, rows, parent_ordinals = group
        parent_ordinals = tuple(parent_ordinals)
        if (
            type(ordinal) is not int
            or ordinal != expected_ordinal
            or type(rows) is not int
            or rows <= 0
            or not parent_ordinals
            or tuple(sorted(parent_ordinals)) != parent_ordinals
            or any(
                type(parent) is not int
                or parent not in parent_by_ordinal
                or parent in seen
                for parent in parent_ordinals
            )
        ):
            raise ValueError("group partition authority differs")
        members = tuple(parent_by_ordinal[parent] for parent in parent_ordinals)
        if sum(parent.rows for parent in members) != rows:
            raise ValueError("group population differs")
        seen.update(parent_ordinals)
        materialized.append(
            GroupProxy(
                ordinal=ordinal,
                rows=rows,
                prototypes=build_group_prototypes(
                    members,
                    prototype_count=prototype_count,
                    iterations=iterations,
                ),
            )
        )
    if seen != set(parent_by_ordinal):
        raise ValueError("group partition is not exhaustive")
    return tuple(materialized)


def _validate_groups(groups):
    if not groups:
        raise ValueError("group proxy is empty")
    seen = set()
    dimension = None
    for group in groups:
        if (
            type(group.ordinal) is not int
            or group.ordinal < 0
            or group.ordinal in seen
            or type(group.rows) is not int
            or group.rows <= 0
            or not group.prototypes
        ):
            raise ValueError("group proxy authority differs")
        for prototype in group.prototypes:
            if dimension is None:
                dimension = len(prototype)
            if len(prototype) != dimension:
                raise ValueError("group prototype dimension differs")
            _distance(prototype, prototype)
        seen.add(group.ordinal)
    if seen != set(range(len(groups))):
        raise ValueError("group ordinals are not dense")
    return dimension


def rank_groups(groups, query):
    groups = tuple(groups)
    dimension = _validate_groups(groups)
    if len(query) != dimension:
        raise ValueError("query dimension differs")
    ranked = []
    for group in groups:
        score = min(_distance(query, prototype) for prototype in group.prototypes)
        ranked.append((score, group.ordinal))
    return tuple(ordinal for _, ordinal in sorted(ranked))


def select_group_prefix(groups, ranked, *, row_limit, group_limit):
    groups = tuple(groups)
    _validate_groups(groups)
    if (
        type(row_limit) is not int
        or row_limit <= 0
        or type(group_limit) is not int
        or group_limit <= 0
    ):
        raise ValueError("selection bound differs")
    ranked = tuple(ranked)
    if len(ranked) != len(set(ranked)) or any(
        type(ordinal) is not int or not 0 <= ordinal < len(groups) for ordinal in ranked
    ):
        raise ValueError("group rank authority differs")
    selected = []
    rows = 0
    for ordinal in ranked:
        if len(selected) == group_limit or rows + groups[ordinal].rows > row_limit:
            break
        selected.append(ordinal)
        rows += groups[ordinal].rows
    if not selected:
        raise ValueError("no complete group fits selection bounds")
    return tuple(selected)


def evaluate_owner_inclusion(groups, queries, *, row_limit, group_limit):
    groups = tuple(groups)
    _validate_groups(groups)
    included = 0
    total = 0
    perfect = 0
    maximum_rows = 0
    query_count = 0
    for query, owners in queries:
        owners = tuple(owners)
        if not owners or len(owners) != len(set(owners)) or any(
            type(owner) is not int or not 0 <= owner < len(groups) for owner in owners
        ):
            raise ValueError("truth-owner authority differs")
        selected = select_group_prefix(
            groups,
            rank_groups(groups, query),
            row_limit=row_limit,
            group_limit=group_limit,
        )
        selected_set = set(selected)
        hits = sum(owner in selected_set for owner in owners)
        rows = sum(groups[ordinal].rows for ordinal in selected)
        included += hits
        total += len(owners)
        perfect += hits == len(owners)
        maximum_rows = max(maximum_rows, rows)
        query_count += 1
    if query_count == 0:
        raise ValueError("query cohort is empty")
    return OwnerInclusionResult(
        included_owners=included,
        maximum_rows=maximum_rows,
        passed=included == total and perfect == query_count,
        perfect_queries=perfect,
        query_count=query_count,
        total_owners=total,
    )
