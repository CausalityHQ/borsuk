#!/usr/bin/env python3
"""Fixed 32-group selection from source-trained per-page centroids."""

from __future__ import annotations

import dataclasses
import hashlib
from collections.abc import Mapping
from pathlib import Path

import numpy as np

from scripts.native_one_million_group_selector import DIMENSIONS
from scripts.native_one_million_page_selector import PageSelectorArtifact
from scripts.native_one_million_selector_evaluation import (
    MAXIMUM_GETS,
    _query_truth,
    aggregate_samples,
)
from scripts.v97_row_width_screen import ObjectIdentity, _canonical_json_bytes

SCHEMA = "borsuk-one-million-page-selector-result-v1"
EVIDENCE_SCHEMA = "borsuk-one-million-page-selector-evidence-v1"


def select_page_groups(query: np.ndarray, artifact: PageSelectorArtifact) -> tuple[int, ...]:
    if (
        query.shape != (DIMENSIONS,)
        or not np.isfinite(query).all()
        or len(artifact.groups) < MAXIMUM_GETS
        or artifact.page_centroids.shape != (len(artifact.page_groups), DIMENSIONS)
    ):
        raise ValueError("page-selector query differs")
    delta = artifact.page_centroids.astype(np.float64) - np.asarray(query, dtype=np.float64)
    scores = np.einsum("ij,ij->i", delta, delta, dtype=np.float64)
    minima = np.full(len(artifact.groups), np.inf, dtype=np.float64)
    np.minimum.at(minima, artifact.page_groups, scores)
    if not np.isfinite(minima).all():
        raise ValueError("page-selector scores differ")
    return tuple(sorted(
        range(len(artifact.groups)),
        key=lambda index: (float(minima[index]), artifact.groups[index].role, artifact.groups[index].ordinal),
    )[:MAXIMUM_GETS])


def evaluate_page_selector(
    artifact: PageSelectorArtifact, queries: Path, truth: Path, out: Path,
    identities: Mapping[str, ObjectIdentity], *, query_count: int = 1000,
) -> dict[str, object]:
    vectors, truth_ids = _query_truth(queries, truth, identities, query_count=query_count)
    membership_ids = artifact.membership_ids
    membership_groups = artifact.membership_groups
    positions = np.searchsorted(membership_ids, truth_ids)
    if np.any(positions >= len(membership_ids)) or not np.array_equal(membership_ids[positions], truth_ids):
        raise ValueError("page-selector truth IDs differ")
    truth_groups = membership_groups[positions]
    samples: list[dict[str, object]] = []
    for ordinal, query in enumerate(vectors):
        selected = select_page_groups(query, artifact)
        chosen = set(selected)
        owner = truth_groups[ordinal]
        samples.append({
            "query_ordinal": ordinal,
            "selected_groups": list(selected),
            "projected_code_bytes": sum(artifact.groups[index].code_bytes for index in selected),
            "hits_at_10": sum(int(value) in chosen for value in owner[:10]),
            "hits_at_100": sum(int(value) in chosen for value in owner),
        })
    metrics = aggregate_samples(samples)
    metrics["decision"] = (
        "page-representative-selector-feasible"
        if metrics["decision"] == "selector-feasible"
        else "page-representative-selector-killed"
    )
    evidence = {"schema": EVIDENCE_SCHEMA, "samples": samples, "metrics": metrics}
    out.mkdir(parents=True, exist_ok=True)
    evidence_body = _canonical_json_bytes(evidence)
    (out / "evidence.json").write_bytes(evidence_body)
    result: dict[str, object] = {
        "schema": SCHEMA,
        "decision": metrics["decision"], "claim_eligible": False,
        "query_identity": dataclasses.asdict(identities["queries"]),
        "truth_identity": dataclasses.asdict(identities["truth"]),
        "selector_seal_sha256": hashlib.sha256(_canonical_json_bytes(artifact.seal)).hexdigest(),
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "metrics": metrics,
    }
    (out / "result.json").write_bytes(_canonical_json_bytes(result))
    return result
