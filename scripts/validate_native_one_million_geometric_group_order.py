#!/usr/bin/env python3
"""Independent geometric-group order and 1M range-plan replay."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from collections.abc import Mapping
from pathlib import Path

import numpy as np

from scripts.native_one_million_geometric_group_order import read_geometric_group_order
from scripts.native_one_million_geometric_group_order_evaluation import BASELINE
from scripts.native_one_million_page_selector import read_page_selector
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.v97_row_width_screen import ObjectIdentity, _canonical_json_bytes
from scripts.validate_native_one_million_page_selector import rebuild_page_arrays


def independently_order_groups(centers: np.ndarray, groups: tuple) -> tuple[int, ...]:
    """Rebuild nearest-page chain using bounded direct differences."""
    positions: list[int] = []
    offset = 0
    for role in ("base", "delta"):
        logical = [i for i, group in enumerate(groups) if group.role == role]
        page_count = sum(groups[i].end_page - groups[i].first_page for i in logical)
        role_pages = centers[offset:offset + page_count].astype(np.float64)
        if len(role_pages) != page_count or not logical:
            raise ValueError("independent layout role differs")
        # Direct subtraction and ordered reduction deliberately differ from
        # the producer's matrix-product distance construction.
        distances = np.full((len(logical), len(logical)), np.inf)
        for a, group_index in enumerate(logical):
            group = groups[group_index]
            selected_pages = role_pages[group.first_page:group.end_page]
            for b in range(a + 1, len(logical)):
                other = groups[logical[b]]
                target_pages = role_pages[other.first_page:other.end_page]
                delta = selected_pages[:, None, :] - target_pages[None, :, :]
                value = float(np.min(np.sum(delta * delta, axis=2, dtype=np.float64)))
                distances[a, b] = value
                distances[b, a] = value
        remaining = set(range(1, len(logical)))
        current = 0
        positions.append(logical[current])
        while remaining:
            next_group = min(remaining, key=lambda i: (distances[current, i], i))
            positions.append(logical[next_group])
            remaining.remove(next_group)
            current = next_group
        offset += page_count
    return tuple(positions)


def _intervals(groups: tuple, selected: set[int]) -> list[list[object]]:
    intervals: list[list[object]] = []
    for i in sorted(selected):
        group = groups[i]
        if intervals and intervals[-1][0] == group.role and intervals[-1][2] == group.ordinal:
            intervals[-1][2] = group.ordinal + 1
        else:
            intervals.append([group.role, group.ordinal, group.ordinal + 1])
    return intervals


def _plan(groups: tuple, ranked: list[int], lengths: list[int]) -> tuple[list[int], list[list[object]], int]:
    chosen: set[int] = set()
    admitted: list[int] = []
    used = 0
    for i in ranked:
        next_used = used + lengths[i]
        if next_used > 16_777_216:
            continue
        candidate = chosen | {i}
        if len(_intervals(groups, candidate)) > 32:
            continue
        chosen = candidate
        admitted.append(i)
        used = next_used
    return admitted, _intervals(groups, chosen), used


def _metrics(samples: list[dict[str, object]]) -> dict[str, int | str]:
    count = len(samples)
    hits100 = sorted(int(item["hits_at_100"]) for item in samples)
    hits10 = [int(item["hits_at_10"]) for item in samples]
    return {
        "query_count": count,
        "mean_recall_at_100_ppm": sum(hits100) * 10000 // count,
        "p05_recall_at_100_ppm": hits100[math.ceil(count * .05) - 1] * 10000,
        "mean_recall_at_10_ppm": sum(hits10) * 100000 // count,
        "max_projected_code_bytes": max(int(item["projected_code_bytes"]) for item in samples),
        "max_code_gets": max(int(item["projected_code_gets"]) for item in samples),
        "max_groups_selected": max(len(item.get("selected_physical_groups", item.get("selected_groups", []))) for item in samples),
        "row_bytes": 96,
    }


def validate_geometric_group_order(
    root: Path, evidence_dir: Path, out: Path,
    source_identities: Mapping[str, ObjectIdentity],
    development_identities: Mapping[str, ObjectIdentity], *, query_count: int = 1000,
) -> dict[str, object]:
    artifact = read_page_selector(root, source_identities)
    centroids, membership, page_groups, page_order_sha, groups_data = rebuild_page_arrays(
        root, source_identities,
    )
    if (
        centroids != (root / "centroids.bin").read_bytes()
        or membership != (root / "membership.bin").read_bytes()
        or page_groups != artifact.seal["page_groups"]
        or page_order_sha != artifact.seal["page_order_sha256"]
        or groups_data != artifact.seal["groups"]
    ):
        raise ValueError("independent layout source replay differs")
    order = read_geometric_group_order(artifact, root / "layout.json")
    if order != independently_order_groups(artifact.page_centroids, artifact.groups):
        raise ValueError("independent geometric group order differs")
    vectors, truth_ids = _query_truth(
        root / "queries.parquet", root / "truth.parquet", development_identities,
        query_count=query_count,
    )
    owner = dict(zip(
        (int(i) for i in artifact.membership_ids),
        (int(i) for i in artifact.membership_groups), strict=True,
    ))
    physical = []
    role_ordinals = {"base": 0, "delta": 0}
    for logical in order:
        group = artifact.groups[logical]
        physical.append(type(group)(
            group.role, role_ordinals[group.role], group.first_page, group.end_page,
            group.row_count, 4 + 4 * (group.end_page - group.first_page) + 96 * group.row_count,
        ))
        role_ordinals[group.role] += 1
    baseline = tuple(type(g)(
        g.role, g.ordinal, g.first_page, g.end_page, g.row_count,
        4 + 4 * (g.end_page - g.first_page) + 96 * g.row_count,
    ) for g in artifact.groups)
    reverse = {logical: physical_index for physical_index, logical in enumerate(order)}
    original_lengths = [g.code_bytes for g in baseline]
    physical_lengths = [g.code_bytes for g in physical]
    rows: list[dict[str, object]] = []
    controls: list[dict[str, object]] = []
    centers = artifact.page_centroids.astype(np.float64)
    for ordinal, query in enumerate(vectors):
        delta = centers - query.astype(np.float64)
        page_scores = np.sum(delta * delta, axis=1, dtype=np.float64)
        minima = [math.inf] * len(artifact.groups)
        for page, score in enumerate(page_scores):
            group = int(artifact.page_groups[page])
            minima[group] = min(minima[group], float(score))
        ranked = sorted(range(len(artifact.groups)), key=lambda i: (
            minima[i], artifact.groups[i].role, artifact.groups[i].ordinal,
        ))
        selected, intervals, bytes_used = _plan(
            tuple(physical), [reverse[i] for i in ranked], physical_lengths,
        )
        control, control_intervals, control_bytes = _plan(baseline, ranked, original_lengths)
        row = [int(i) for i in truth_ids[ordinal]]
        if len(set(row)) != 100 or any(i not in owner for i in row):
            raise ValueError("independent geometric truth IDs differ")
        chosen = {order[i] for i in selected}
        control_set = set(control)
        rows.append({
            "query_ordinal": ordinal,
            "selected_physical_groups": selected,
            "intervals": intervals,
            "projected_code_gets": len(intervals),
            "projected_code_bytes": bytes_used,
            "hits_at_10": sum(owner[i] in chosen for i in row[:10]),
            "hits_at_100": sum(owner[i] in chosen for i in row),
        })
        controls.append({
            "query_ordinal": ordinal,
            "selected_groups": control,
            "intervals": control_intervals,
            "projected_code_gets": len(control_intervals),
            "projected_code_bytes": control_bytes,
            "hits_at_10": sum(owner[i] in control_set for i in row[:10]),
            "hits_at_100": sum(owner[i] in control_set for i in row),
        })
    evidence_body = (evidence_dir / "evidence.json").read_bytes()
    result_body = (evidence_dir / "result.json").read_bytes()
    evidence = json.loads(evidence_body)
    result = json.loads(result_body)
    if (
        evidence_body != _canonical_json_bytes(evidence)
        or result_body != _canonical_json_bytes(result)
        or evidence.get("schema") != "borsuk-one-million-geometric-group-order-evidence-v1"
        or evidence.get("samples") != rows
        or evidence.get("baseline_samples") != controls
    ):
        raise ValueError("independent geometric evidence differs")
    baseline_metrics = _metrics(controls)
    baseline_pass = (
        baseline_metrics["mean_recall_at_100_ppm"] >= 975000
        and baseline_metrics["p05_recall_at_100_ppm"] >= 900000
        and baseline_metrics["mean_recall_at_10_ppm"] >= 960000
        and baseline_metrics["max_code_gets"] <= 32
        and baseline_metrics["max_projected_code_bytes"] <= 16_777_216
    )
    baseline_metrics["decision"] = (
        "pq96-locality-projection-feasible" if baseline_pass
        else "pq96-locality-projection-killed"
    )
    if (
        baseline_metrics["mean_recall_at_100_ppm"],
        baseline_metrics["p05_recall_at_100_ppm"],
        baseline_metrics["mean_recall_at_10_ppm"],
        sum(item["hits_at_100"] < 90 for item in controls),
    ) != BASELINE:
        raise ValueError("independent geometric baseline differs")
    metrics = _metrics(rows)
    below90 = sum(item["hits_at_100"] < 90 for item in rows)
    decision = (
        "geometric-group-order-feasible"
        if metrics["mean_recall_at_100_ppm"] >= BASELINE[0]
        and metrics["p05_recall_at_100_ppm"] >= 900000
        and metrics["mean_recall_at_10_ppm"] >= BASELINE[2]
        and below90 <= 49
        and metrics["max_code_gets"] <= 32
        and metrics["max_projected_code_bytes"] <= 16_777_216
        else "geometric-group-order-killed"
    )
    metrics["below_90_gt100_queries"] = below90
    metrics["decision"] = decision
    if (
        evidence.get("metrics") != metrics
        or evidence.get("baseline_metrics") != baseline_metrics
        or set(result) != {
            "schema", "decision", "claim_eligible", "metrics", "baseline_metrics",
            "query_identity", "truth_identity", "selector_seal_sha256",
            "layout_sha256", "evidence_sha256",
        }
        or result.get("schema") != "borsuk-one-million-geometric-group-order-result-v1"
        or result.get("metrics") != metrics
        or result.get("baseline_metrics") != baseline_metrics
        or result.get("decision") != decision
        or result.get("claim_eligible") is not False
        or result.get("query_identity") != dataclasses.asdict(development_identities["queries"])
        or result.get("truth_identity") != dataclasses.asdict(development_identities["truth"])
        or result.get("selector_seal_sha256") != hashlib.sha256((root / "seal.json").read_bytes()).hexdigest()
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or result.get("layout_sha256") != hashlib.sha256((root / "layout.json").read_bytes()).hexdigest()
    ):
        raise ValueError("independent geometric result differs")
    validation = {
        "schema": "borsuk-one-million-geometric-group-order-validation-v1",
        "decision": decision,
        "metrics": metrics,
        "baseline_metrics": baseline_metrics,
        "layout_sha256": result["layout_sha256"],
        "evidence_sha256": result["evidence_sha256"],
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "validation.json").write_bytes(_canonical_json_bytes(validation))
    return validation
