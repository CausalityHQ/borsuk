#!/usr/bin/env python3
"""Source-only geometric physical order for existing one-million code groups."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np

from scripts.native_one_million_page_selector import PageSelectorArtifact
from scripts.v97_row_width_screen import _canonical_json_bytes

SCHEMA = "borsuk-one-million-geometric-group-order-v1"
DISTANCE_RULE = "minimum-float64-squared-l2-between-float16-page-centroids"


def geometric_group_order(artifact: PageSelectorArtifact) -> tuple[int, ...]:
    """Chain each role's whole groups by nearest page pair, with fixed ties."""
    centers = artifact.page_centroids.astype(np.float64)
    order: list[int] = []
    for role in ("base", "delta"):
        logical = [i for i, group in enumerate(artifact.groups) if group.role == role]
        if not logical or [artifact.groups[i].ordinal for i in logical] != list(range(len(logical))):
            raise ValueError("geometric group role order differs")
        role_offset = sum(
            group.end_page - group.first_page
            for group in artifact.groups if group.role == "base"
        ) if role == "delta" else 0
        first = role_offset + artifact.groups[logical[0]].first_page
        last = role_offset + artifact.groups[logical[-1]].end_page
        # Whole page-distance matrix is at most 3,639² float64 values per
        # role; the 3-GiB gate applies to the enclosing construction phase.
        pages = centers[first:last]
        squares = np.einsum("ij,ij->i", pages, pages, dtype=np.float64)
        distances = squares[:, None] + squares[None, :] - 2 * (pages @ pages.T)
        np.maximum(distances, 0, out=distances)
        group_distances = np.full((len(logical), len(logical)), np.inf)
        for left, left_index in enumerate(logical):
            g = artifact.groups[left_index]
            a, z = g.first_page, g.end_page
            for right in range(left + 1, len(logical)):
                h = artifact.groups[logical[right]]
                b, y = h.first_page, h.end_page
                value = float(np.min(distances[a:z, b:y]))
                group_distances[left, right] = value
                group_distances[right, left] = value
        unvisited = set(range(1, len(logical)))
        current = 0
        order.append(logical[current])
        while unvisited:
            chosen = min(unvisited, key=lambda i: (group_distances[current, i], i))
            order.append(logical[chosen])
            unvisited.remove(chosen)
            current = chosen
    if len(order) != len(artifact.groups) or set(order) != set(range(len(order))):
        raise ValueError("geometric group permutation differs")
    return tuple(order)


def build_geometric_group_order(artifact: PageSelectorArtifact, out: Path) -> dict[str, object]:
    order = geometric_group_order(artifact)
    projected_bytes = [
        4 + 4 * (artifact.groups[i].end_page - artifact.groups[i].first_page)
        + 96 * artifact.groups[i].row_count
        for i in order
    ]
    value: dict[str, object] = {
        "schema": SCHEMA,
        "distance_rule": DISTANCE_RULE,
        "projected_row_bytes": 96,
        "physical_group_bytes": projected_bytes,
        "source_seal_sha256": hashlib.sha256(_canonical_json_bytes(artifact.seal)).hexdigest(),
        "physical_to_logical": list(order),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "layout.json").write_bytes(_canonical_json_bytes(value))
    return value


def read_geometric_group_order(artifact: PageSelectorArtifact, path: Path) -> tuple[int, ...]:
    body = path.read_bytes()
    try:
        value = json.loads(body)
        order = value["physical_to_logical"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise ValueError("geometric group layout differs") from error
    if (
        body != _canonical_json_bytes(value)
        or set(value) != {
            "schema", "distance_rule", "projected_row_bytes", "physical_group_bytes",
            "source_seal_sha256", "physical_to_logical",
        }
        or value["schema"] != SCHEMA
        or value["distance_rule"] != DISTANCE_RULE
        or value["projected_row_bytes"] != 96
        or value["source_seal_sha256"] != hashlib.sha256(_canonical_json_bytes(artifact.seal)).hexdigest()
        or type(order) is not list
        or len(order) != len(artifact.groups)
        or any(type(i) is not int for i in order)
        or set(order) != set(range(len(order)))
        or [artifact.groups[i].role for i in order] != sorted((group.role for group in artifact.groups))
        or value["physical_group_bytes"] != [
            4 + 4 * (artifact.groups[i].end_page - artifact.groups[i].first_page)
            + 96 * artifact.groups[i].row_count
            for i in order
        ]
    ):
        raise ValueError("geometric group layout authority differs")
    return tuple(order)
