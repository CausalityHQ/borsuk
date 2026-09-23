#!/usr/bin/env python3
"""Project one sealed physical group order with the fixed page score."""

from __future__ import annotations

import dataclasses
import hashlib
from collections.abc import Mapping
from pathlib import Path

import numpy as np

from scripts.native_one_million_geometric_group_order import read_geometric_group_order
from scripts.native_one_million_page_selector import PageSelectorArtifact
from scripts.native_one_million_page_selector_evaluation import rank_page_groups
from scripts.native_one_million_range_selector_evaluation import (
    aggregate_range_samples,
    plan_group_ranges,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.v97_row_width_screen import ObjectIdentity, _canonical_json_bytes

SCHEMA = "borsuk-one-million-geometric-group-order-result-v1"
EVIDENCE_SCHEMA = "borsuk-one-million-geometric-group-order-evidence-v1"
BASELINE = (981510, 890000, 992800, 51)


def physical_groups(
    artifact: PageSelectorArtifact, order: tuple[int, ...], *, row_bytes: int = 96,
) -> tuple:
    if row_bytes != 96 or len(order) != len(artifact.groups) or set(order) != set(range(len(order))):
        raise ValueError("geometric group physical contract differs")
    role_ordinals = {"base": 0, "delta": 0}
    groups = []
    for logical in order:
        group = artifact.groups[logical]
        ordinal = role_ordinals[group.role]
        role_ordinals[group.role] += 1
        groups.append(dataclasses.replace(
            group, ordinal=ordinal,
            code_bytes=4 + 4 * (group.end_page - group.first_page) + row_bytes * group.row_count,
        ))
    return tuple(groups)


def evaluate_geometric_group_order(
    artifact: PageSelectorArtifact, layout_path: Path, queries: Path, truth: Path,
    out: Path, identities: Mapping[str, ObjectIdentity], *, query_count: int = 1000,
) -> dict[str, object]:
    order = read_geometric_group_order(artifact, layout_path)
    groups = physical_groups(artifact, order)
    reverse = [0] * len(order)
    for physical, logical in enumerate(order):
        reverse[logical] = physical
    vectors, truth_ids = _query_truth(queries, truth, identities, query_count=query_count)
    positions = np.searchsorted(artifact.membership_ids, truth_ids)
    if np.any(positions >= len(artifact.membership_ids)) or not np.array_equal(
        artifact.membership_ids[positions], truth_ids,
    ):
        raise ValueError("geometric group truth IDs differ")
    owners = artifact.membership_groups[positions]
    baseline_groups = physical_groups(artifact, tuple(range(len(order))))
    samples: list[dict[str, object]] = []
    baseline_samples: list[dict[str, object]] = []
    for ordinal, query in enumerate(vectors):
        ranked_logical = rank_page_groups(query, artifact)
        ranked_physical = tuple(reverse[i] for i in ranked_logical)
        selected, intervals, gets, bytes_used = plan_group_ranges(groups, ranked_physical)
        logical_selected = {order[i] for i in selected}
        row = owners[ordinal]
        samples.append({
            "query_ordinal": ordinal,
            "selected_physical_groups": list(selected),
            "intervals": [list(item) for item in intervals],
            "projected_code_gets": gets,
            "projected_code_bytes": bytes_used,
            "hits_at_10": sum(int(i) in logical_selected for i in row[:10]),
            "hits_at_100": sum(int(i) in logical_selected for i in row),
        })
        control, control_intervals, control_gets, control_bytes = plan_group_ranges(
            baseline_groups, ranked_logical,
        )
        control_set = set(control)
        baseline_samples.append({
            "query_ordinal": ordinal,
            "selected_groups": list(control),
            "intervals": [list(item) for item in control_intervals],
            "projected_code_gets": control_gets,
            "projected_code_bytes": control_bytes,
            "hits_at_10": sum(int(i) in control_set for i in row[:10]),
            "hits_at_100": sum(int(i) in control_set for i in row),
        })
    baseline_metrics = aggregate_range_samples(baseline_samples, row_bytes=96)
    if (
        baseline_metrics["mean_recall_at_100_ppm"],
        baseline_metrics["p05_recall_at_100_ppm"],
        baseline_metrics["mean_recall_at_10_ppm"],
        sum(item["hits_at_100"] < 90 for item in baseline_samples),
    ) != BASELINE:
        raise ValueError("geometric group baseline differs from closed PQ96 attempt")
    candidate_samples = [
        {
            "query_ordinal": item["query_ordinal"],
            "selected_groups": item["selected_physical_groups"],
            "projected_code_gets": item["projected_code_gets"],
            "projected_code_bytes": item["projected_code_bytes"],
            "hits_at_10": item["hits_at_10"],
            "hits_at_100": item["hits_at_100"],
        }
        for item in samples
    ]
    metrics = aggregate_range_samples(candidate_samples, row_bytes=96)
    below90 = sum(item["hits_at_100"] < 90 for item in samples)
    pass_gate = (
        metrics["mean_recall_at_100_ppm"] >= BASELINE[0]
        and metrics["p05_recall_at_100_ppm"] >= 900000
        and metrics["mean_recall_at_10_ppm"] >= BASELINE[2]
        and below90 <= 49
        and metrics["max_code_gets"] <= 32
        and metrics["max_projected_code_bytes"] <= 16_777_216
    )
    metrics["below_90_gt100_queries"] = below90
    metrics["decision"] = (
        "geometric-group-order-feasible" if pass_gate else "geometric-group-order-killed"
    )
    evidence = {
        "schema": EVIDENCE_SCHEMA,
        "samples": samples,
        "baseline_samples": baseline_samples,
        "metrics": metrics,
        "baseline_metrics": baseline_metrics,
    }
    out.mkdir(parents=True, exist_ok=True)
    evidence_body = _canonical_json_bytes(evidence)
    (out / "evidence.json").write_bytes(evidence_body)
    result: dict[str, object] = {
        "schema": SCHEMA,
        "decision": metrics["decision"],
        "claim_eligible": False,
        "metrics": metrics,
        "baseline_metrics": baseline_metrics,
        "query_identity": dataclasses.asdict(identities["queries"]),
        "truth_identity": dataclasses.asdict(identities["truth"]),
        "selector_seal_sha256": hashlib.sha256(_canonical_json_bytes(artifact.seal)).hexdigest(),
        "layout_sha256": hashlib.sha256(layout_path.read_bytes()).hexdigest(),
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
    }
    (out / "result.json").write_bytes(_canonical_json_bytes(result))
    return result
