#!/usr/bin/env python3
"""Independent 1M page-rank and adjacent-code-range decision replay."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from collections.abc import Mapping
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_one_million_group_selector import DIMENSIONS
from scripts.native_one_million_page_selector import (
    PageSelectorArtifact,
    read_page_selector,
)
from scripts.v97_row_width_screen import ObjectIdentity
from scripts.validate_native_one_million_page_selector import rebuild_page_arrays
from scripts.validate_native_one_million_selector import _canonical, _check


def _intervals(groups: tuple, selected: set[int]) -> list[list[object]]:
    intervals: list[list[object]] = []
    for index in sorted(selected):
        group = groups[index]
        if intervals and intervals[-1][0] == group.role and intervals[-1][2] == group.ordinal:
            intervals[-1][2] = group.ordinal + 1
        else:
            intervals.append([group.role, group.ordinal, group.ordinal + 1])
    return intervals


def replay_range_evidence(
    artifact: PageSelectorArtifact, queries: Path, truth: Path, evidence_dir: Path,
    identities: Mapping[str, ObjectIdentity], *, query_count: int = 1000,
    byte_only: bool = False, pq96: bool = False,
) -> dict[str, object]:
    if set(identities) != {"queries", "truth"} or query_count <= 0 or (byte_only and pq96):
        raise ValueError("independent range query contract differs")
    _check(queries, identities["queries"], "queries")
    _check(truth, identities["truth"], "truth")
    qt = pq.read_table(queries)
    tt = pq.read_table(truth)
    query_schema = pa.schema([
        pa.field("query_ordinal", pa.uint32(), nullable=False),
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("embedding", pa.list_(pa.field("item", pa.float32(), nullable=False), DIMENSIONS), nullable=False),
    ])
    truth_schema = pa.schema([
        pa.field("query_ordinal", pa.uint32(), nullable=False),
        pa.field("rank", pa.uint16(), nullable=False),
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("squared_distance", pa.float64(), nullable=False),
    ])
    if qt.schema != query_schema or qt.num_rows != query_count or tt.schema != truth_schema or tt.num_rows != query_count * 100:
        raise ValueError("independent range query schema differs")
    query_ordinals = qt.column("query_ordinal").combine_chunks().to_pylist()
    truth_ordinals = tt.column("query_ordinal").combine_chunks().to_pylist()
    ranks = tt.column("rank").combine_chunks().to_pylist()
    truth_ids = tt.column("feature_row_id").combine_chunks().to_pylist()
    if query_ordinals != list(range(query_count)) or truth_ordinals != [i for i in range(query_count) for _ in range(100)] or ranks != list(range(100)) * query_count:
        raise ValueError("independent range query order differs")
    vectors = np.asarray(qt.column("embedding").combine_chunks().values.to_numpy(), dtype=np.float32).reshape(query_count, DIMENSIONS)
    if not np.isfinite(vectors).all():
        raise ValueError("independent range query vectors differ")
    owner = dict(zip((int(v) for v in artifact.membership_ids), (int(v) for v in artifact.membership_groups), strict=True))
    centers = artifact.page_centroids.astype(np.float64)
    lengths = [
        4 + 4 * (group.end_page - group.first_page) + 96 * group.row_count
        if pq96 else group.code_bytes
        for group in artifact.groups
    ]
    samples: list[dict[str, object]] = []
    for ordinal, query in enumerate(vectors):
        delta = centers - query.astype(np.float64)
        page_scores = np.sum(delta * delta, axis=1, dtype=np.float64)
        minima = [math.inf] * len(artifact.groups)
        for page, score in enumerate(page_scores):
            group = int(artifact.page_groups[page])
            minima[group] = min(minima[group], float(score))
        ranked = sorted(range(len(artifact.groups)), key=lambda i: (minima[i], artifact.groups[i].role, artifact.groups[i].ordinal))
        chosen: set[int] = set()
        admitted: list[int] = []
        bytes_used = 0
        for index in ranked:
            next_bytes = bytes_used + lengths[index]
            if next_bytes > 16_777_216:
                continue
            candidate = chosen | {index}
            if not byte_only and len(_intervals(artifact.groups, candidate)) > 32:
                continue
            chosen = candidate
            admitted.append(index)
            bytes_used = next_bytes
        intervals = _intervals(artifact.groups, chosen)
        row = [int(v) for v in truth_ids[ordinal * 100 : (ordinal + 1) * 100]]
        if len(set(row)) != 100 or any(v not in owner for v in row):
            raise ValueError("independent range truth IDs differ")
        samples.append({
            "query_ordinal": ordinal,
            "selected_groups": admitted,
            "intervals": intervals,
            "projected_code_gets": len(intervals),
            "projected_code_bytes": bytes_used,
            "hits_at_10": sum(owner[v] in chosen for v in row[:10]),
            "hits_at_100": sum(owner[v] in chosen for v in row),
        })
    evidence_body = (evidence_dir / "evidence.json").read_bytes()
    result_body = (evidence_dir / "result.json").read_bytes()
    evidence = json.loads(evidence_body)
    result = json.loads(result_body)
    if evidence_body != _canonical(evidence) or result_body != _canonical(result):
        raise ValueError("independent range evidence canonical bytes differ")
    expected_evidence_schema = (
        "borsuk-one-million-byte-ceiling-evidence-v1" if byte_only
        else "borsuk-one-million-pq96-locality-evidence-v1" if pq96
        else "borsuk-one-million-range-selector-evidence-v1"
    )
    if evidence.get("schema") != expected_evidence_schema or evidence.get("samples") != samples:
        raise ValueError("independent range samples differ")
    hits100 = [sample["hits_at_100"] for sample in samples]
    hits10 = [sample["hits_at_10"] for sample in samples]
    metrics = {
        "query_count": query_count,
        "mean_recall_at_100_ppm": sum(hits100) * 10000 // query_count,
        "p05_recall_at_100_ppm": sorted(hits100)[math.ceil(query_count * .05) - 1] * 10000,
        "mean_recall_at_10_ppm": sum(hits10) * 100000 // query_count,
        "max_projected_code_bytes": max(sample["projected_code_bytes"] for sample in samples),
        "max_code_gets": max(sample["projected_code_gets"] for sample in samples),
        "max_groups_selected": max(len(sample["selected_groups"]) for sample in samples),
    }
    quality_pass = (
        metrics["mean_recall_at_100_ppm"] >= 975000
        and metrics["p05_recall_at_100_ppm"] >= 900000
        and metrics["mean_recall_at_10_ppm"] >= 960000
        and metrics["max_projected_code_bytes"] <= 16777216
    )
    if pq96:
        metrics["row_bytes"] = 96
    metrics["decision"] = (
        ("score-ranked-byte-ceiling-feasible" if quality_pass else "score-ranked-byte-ceiling-fails")
        if byte_only else
        ("pq96-locality-projection-feasible" if quality_pass and metrics["max_code_gets"] <= 32 else "pq96-locality-projection-killed")
        if pq96 else
        ("adjacent-code-range-selector-feasible" if quality_pass and metrics["max_code_gets"] <= 32 else "adjacent-code-range-selector-killed")
    )
    expected_result_schema = (
        "borsuk-one-million-byte-ceiling-result-v1" if byte_only
        else "borsuk-one-million-pq96-locality-result-v1" if pq96
        else "borsuk-one-million-range-selector-result-v1"
    )
    if (
        evidence.get("metrics") != metrics
        or result.get("metrics") != metrics
        or result.get("decision") != metrics["decision"]
        or result.get("schema") != expected_result_schema
        or result.get("claim_eligible") is not False
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or result.get("selector_seal_sha256") != hashlib.sha256(_canonical(artifact.seal)).hexdigest()
        or result.get("query_identity") != dataclasses.asdict(identities["queries"])
        or result.get("truth_identity") != dataclasses.asdict(identities["truth"])
    ):
        raise ValueError("independent range result differs")
    return metrics


def validate_range_selector(
    root: Path, selector_dir: Path, evidence_dir: Path, out: Path,
    source_identities: Mapping[str, ObjectIdentity],
    development_identities: Mapping[str, ObjectIdentity], *, query_count: int = 1000,
    byte_only: bool = False, pq96: bool = False,
) -> dict[str, object]:
    artifact = read_page_selector(selector_dir, source_identities)
    centroids, membership, page_groups, page_order_sha, groups = rebuild_page_arrays(root, source_identities)
    if (
        centroids != (selector_dir / "centroids.bin").read_bytes()
        or membership != (selector_dir / "membership.bin").read_bytes()
        or page_groups != artifact.seal["page_groups"]
        or page_order_sha != artifact.seal["page_order_sha256"]
        or groups != artifact.seal["groups"]
    ):
        raise ValueError("independent range source replay differs")
    metrics = replay_range_evidence(
        artifact, root / "queries.parquet", root / "truth.parquet", evidence_dir,
        development_identities, query_count=query_count, byte_only=byte_only, pq96=pq96,
    )
    validation: dict[str, object] = {
        "schema": (
            "borsuk-one-million-byte-ceiling-validation-v1" if byte_only
            else "borsuk-one-million-pq96-locality-validation-v1" if pq96
            else "borsuk-one-million-range-selector-validation-v1"
        ),
        "decision": metrics["decision"], "metrics": metrics,
        "selector_seal_sha256": hashlib.sha256((selector_dir / "seal.json").read_bytes()).hexdigest(),
        "evidence_sha256": hashlib.sha256((evidence_dir / "evidence.json").read_bytes()).hexdigest(),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "validation.json").write_bytes(_canonical(validation))
    return validation
