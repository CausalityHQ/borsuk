#!/usr/bin/env python3
"""Fixed adjacent code-group Range GET projection from page scores."""

from __future__ import annotations

import dataclasses
import hashlib
import math
from collections.abc import Mapping, Sequence
from pathlib import Path

import numpy as np

from scripts.native_one_million_group_selector import Group
from scripts.native_one_million_page_selector import PageSelectorArtifact
from scripts.native_one_million_page_selector_evaluation import rank_page_groups
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.v97_row_width_screen import ObjectIdentity, _canonical_json_bytes

SCHEMA = "borsuk-one-million-range-selector-result-v1"
EVIDENCE_SCHEMA = "borsuk-one-million-range-selector-evidence-v1"
MAXIMUM_GETS = 32
MAXIMUM_BYTES = 16_777_216


def plan_group_ranges(
    groups: Sequence[Group], ranked: Sequence[int], *,
    maximum_gets: int = MAXIMUM_GETS, maximum_bytes: int = MAXIMUM_BYTES,
) -> tuple[tuple[int, ...], tuple[tuple[str, int, int], ...], int, int]:
    """Admit ranked groups if their contiguous union fits both wave caps."""
    if (
        not groups or len(ranked) != len(groups)
        or set(ranked) != set(range(len(groups)))
        or maximum_gets <= 0 or maximum_bytes <= 0
    ):
        raise ValueError("range-selector plan authority differs")
    selected = [False] * len(groups)
    ordered: list[int] = []
    gets = 0
    bytes_used = 0
    for index in ranked:
        group = groups[index]
        left = (
            index > 0 and selected[index - 1]
            and groups[index - 1].role == group.role
            and groups[index - 1].ordinal + 1 == group.ordinal
        )
        right = (
            index + 1 < len(groups) and selected[index + 1]
            and groups[index + 1].role == group.role
            and group.ordinal + 1 == groups[index + 1].ordinal
        )
        next_gets = gets + 1 - int(left) - int(right)
        next_bytes = bytes_used + group.code_bytes
        if next_gets > maximum_gets or next_bytes > maximum_bytes:
            continue
        selected[index] = True
        ordered.append(index)
        gets = next_gets
        bytes_used = next_bytes
    intervals: list[tuple[str, int, int]] = []
    for index, enabled in enumerate(selected):
        if not enabled:
            continue
        group = groups[index]
        if intervals and intervals[-1][0] == group.role and intervals[-1][2] == group.ordinal:
            prior = intervals[-1]
            intervals[-1] = (prior[0], prior[1], group.ordinal + 1)
        else:
            intervals.append((group.role, group.ordinal, group.ordinal + 1))
    if gets != len(intervals) or bytes_used != sum(groups[index].code_bytes for index in ordered):
        raise ValueError("range-selector plan accounting differs")
    return tuple(ordered), tuple(intervals), gets, bytes_used


def aggregate_range_samples(samples: list[dict[str, object]]) -> dict[str, int | str]:
    if not samples or any(sample["query_ordinal"] != i for i, sample in enumerate(samples)):
        raise ValueError("range-selector samples differ")
    count = len(samples)
    hit100 = [int(sample["hits_at_100"]) for sample in samples]
    hit10 = [int(sample["hits_at_10"]) for sample in samples]
    if any(not 0 <= value <= 100 for value in hit100) or any(not 0 <= value <= 10 for value in hit10):
        raise ValueError("range-selector hits differ")
    metrics: dict[str, int | str] = {
        "query_count": count,
        "mean_recall_at_100_ppm": sum(hit100) * 10000 // count,
        "p05_recall_at_100_ppm": sorted(hit100)[math.ceil(count * .05) - 1] * 10000,
        "mean_recall_at_10_ppm": sum(hit10) * 100000 // count,
        "max_projected_code_bytes": max(int(sample["projected_code_bytes"]) for sample in samples),
        "max_code_gets": max(int(sample["projected_code_gets"]) for sample in samples),
        "max_groups_selected": max(len(sample["selected_groups"]) for sample in samples),
    }
    metrics["decision"] = (
        "adjacent-code-range-selector-feasible"
        if metrics["mean_recall_at_100_ppm"] >= 975000
        and metrics["p05_recall_at_100_ppm"] >= 900000
        and metrics["mean_recall_at_10_ppm"] >= 960000
        and metrics["max_projected_code_bytes"] <= MAXIMUM_BYTES
        and metrics["max_code_gets"] <= MAXIMUM_GETS
        else "adjacent-code-range-selector-killed"
    )
    return metrics


def evaluate_range_selector(
    artifact: PageSelectorArtifact, queries: Path, truth: Path, out: Path,
    identities: Mapping[str, ObjectIdentity], *, query_count: int = 1000,
) -> dict[str, object]:
    vectors, truth_ids = _query_truth(queries, truth, identities, query_count=query_count)
    positions = np.searchsorted(artifact.membership_ids, truth_ids)
    if np.any(positions >= len(artifact.membership_ids)) or not np.array_equal(artifact.membership_ids[positions], truth_ids):
        raise ValueError("range-selector truth IDs differ")
    owners = artifact.membership_groups[positions]
    samples: list[dict[str, object]] = []
    for ordinal, query in enumerate(vectors):
        ranked = rank_page_groups(query, artifact)
        selected, intervals, gets, bytes_used = plan_group_ranges(artifact.groups, ranked)
        chosen = set(selected)
        row = owners[ordinal]
        samples.append({
            "query_ordinal": ordinal,
            "selected_groups": list(selected),
            "intervals": [list(value) for value in intervals],
            "projected_code_gets": gets,
            "projected_code_bytes": bytes_used,
            "hits_at_10": sum(int(value) in chosen for value in row[:10]),
            "hits_at_100": sum(int(value) in chosen for value in row),
        })
    metrics = aggregate_range_samples(samples)
    evidence = {"schema": EVIDENCE_SCHEMA, "samples": samples, "metrics": metrics}
    out.mkdir(parents=True, exist_ok=True)
    evidence_body = _canonical_json_bytes(evidence)
    (out / "evidence.json").write_bytes(evidence_body)
    result: dict[str, object] = {
        "schema": SCHEMA, "decision": metrics["decision"], "claim_eligible": False,
        "query_identity": dataclasses.asdict(identities["queries"]),
        "truth_identity": dataclasses.asdict(identities["truth"]),
        "selector_seal_sha256": hashlib.sha256(_canonical_json_bytes(artifact.seal)).hexdigest(),
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "metrics": metrics,
    }
    (out / "result.json").write_bytes(_canonical_json_bytes(result))
    return result
