#!/usr/bin/env python3
"""Paired one-million source-only page-mass routing projection."""

from __future__ import annotations

import dataclasses
import hashlib
from collections.abc import Mapping
from pathlib import Path

import numpy as np

from scripts.native_one_million_page_dispersion_mass import (
    rank_mass_groups,
    read_page_moments,
)
from scripts.native_one_million_page_selector import PageSelectorArtifact
from scripts.native_one_million_page_selector_evaluation import rank_page_groups
from scripts.native_one_million_range_selector_evaluation import (
    aggregate_range_samples,
    plan_group_ranges,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.v97_row_width_screen import ObjectIdentity, _canonical_json_bytes

SCHEMA = "borsuk-one-million-page-dispersion-mass-result-v1"
EVIDENCE_SCHEMA = "borsuk-one-million-page-dispersion-mass-evidence-v1"
BASELINE = (981510, 890000, 992800, 51)
BASELINE_SAMPLES_SHA256 = "ce61ad527968c5bf1bb5bc45e576b4f94eeb690c9ac7000fff6acf6f2ead713d"


def evaluate_page_dispersion_mass(
    artifact: PageSelectorArtifact, moments_path: Path, queries: Path, truth: Path,
    out: Path, identities: Mapping[str, ObjectIdentity], *, query_count: int = 1000,
) -> dict[str, object]:
    moments = read_page_moments(moments_path, artifact)
    vectors, truth_ids = _query_truth(queries, truth, identities, query_count=query_count)
    positions = np.searchsorted(artifact.membership_ids, truth_ids)
    if np.any(positions >= len(artifact.membership_ids)) or not np.array_equal(
        artifact.membership_ids[positions], truth_ids,
    ):
        raise ValueError("page mass truth IDs differ")
    owners = artifact.membership_groups[positions]
    groups = tuple(dataclasses.replace(
        group,
        code_bytes=4 + 4 * (group.end_page - group.first_page) + 96 * group.row_count,
    ) for group in artifact.groups)
    samples: list[dict[str, object]] = []
    controls: list[dict[str, object]] = []
    for ordinal, query in enumerate(vectors):
        rank = rank_mass_groups(query, artifact, moments)
        selected, intervals, gets, bytes_used = plan_group_ranges(groups, rank)
        control_rank = rank_page_groups(query, artifact)
        control, control_intervals, control_gets, control_bytes = plan_group_ranges(
            groups, control_rank,
        )
        selected_set = set(selected)
        control_set = set(control)
        row = owners[ordinal]
        samples.append({
            "query_ordinal": ordinal,
            "ranked_groups": list(rank),
            "selected_groups": list(selected),
            "intervals": [list(item) for item in intervals],
            "projected_code_gets": gets,
            "projected_code_bytes": bytes_used,
            "hits_at_10": sum(int(i) in selected_set for i in row[:10]),
            "hits_at_100": sum(int(i) in selected_set for i in row),
        })
        controls.append({
            "query_ordinal": ordinal,
            "selected_groups": list(control),
            "intervals": [list(item) for item in control_intervals],
            "projected_code_gets": control_gets,
            "projected_code_bytes": control_bytes,
            "hits_at_10": sum(int(i) in control_set for i in row[:10]),
            "hits_at_100": sum(int(i) in control_set for i in row),
        })
    baseline_metrics = aggregate_range_samples(controls, row_bytes=96)
    if (
        baseline_metrics["mean_recall_at_100_ppm"],
        baseline_metrics["p05_recall_at_100_ppm"],
        baseline_metrics["mean_recall_at_10_ppm"],
        sum(item["hits_at_100"] < 90 for item in controls),
    ) != BASELINE:
        raise ValueError("page mass baseline differs from closed PQ96 attempt")
    if hashlib.sha256(_canonical_json_bytes(controls)).hexdigest() != BASELINE_SAMPLES_SHA256:
        raise ValueError("page mass baseline per-query plans differ from closed PQ96 attempt")
    metrics = aggregate_range_samples(samples, row_bytes=96)
    below90 = sum(item["hits_at_100"] < 90 for item in samples)
    metrics["below_90_gt100_queries"] = below90
    metrics["decision"] = (
        "page-dispersion-mass-feasible"
        if metrics["mean_recall_at_100_ppm"] >= BASELINE[0]
        and metrics["p05_recall_at_100_ppm"] >= 900000
        and metrics["mean_recall_at_10_ppm"] >= BASELINE[2]
        and below90 <= 49
        and metrics["max_code_gets"] <= 32
        and metrics["max_projected_code_bytes"] <= 16_777_216
        else "page-dispersion-mass-killed"
    )
    evidence = {
        "schema": EVIDENCE_SCHEMA,
        "samples": samples,
        "baseline_samples": controls,
        "metrics": metrics,
        "baseline_metrics": baseline_metrics,
    }
    out.mkdir(parents=True, exist_ok=True)
    evidence_body = _canonical_json_bytes(evidence)
    (out / "evidence.json").write_bytes(evidence_body)
    result: dict[str, object] = {
        "schema": SCHEMA,
        "claim_eligible": False,
        "decision": metrics["decision"],
        "metrics": metrics,
        "baseline_metrics": baseline_metrics,
        "selector_seal_sha256": hashlib.sha256(_canonical_json_bytes(artifact.seal)).hexdigest(),
        "moments_sha256": hashlib.sha256(moments_path.read_bytes()).hexdigest(),
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "query_identity": dataclasses.asdict(identities["queries"]),
        "truth_identity": dataclasses.asdict(identities["truth"]),
    }
    (out / "result.json").write_bytes(_canonical_json_bytes(result))
    return result
