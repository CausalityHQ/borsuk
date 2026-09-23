#!/usr/bin/env python3
"""Independent source moments, mass-ranking and 1M range-plan replay."""

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
from scipy.special import ndtr

from scripts.native_one_million_group_selector import DIMENSIONS
from scripts.native_one_million_page_dispersion_mass import read_page_moments
from scripts.native_one_million_page_dispersion_mass_evaluation import (
    BASELINE,
    BASELINE_SAMPLES_SHA256,
)
from scripts.native_one_million_page_selector import (
    _page_membership,
    read_page_selector,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.v97_row_width_screen import ObjectIdentity, _canonical_json_bytes
from scripts.validate_native_one_million_geometric_group_order import (
    _metrics,
    _plan,
)
from scripts.validate_native_one_million_page_selector import rebuild_page_arrays


def independently_rebuild_moments(
    root: Path, artifact: object, source_identities: Mapping[str, ObjectIdentity],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    groups, id_to_page, page_groups, order_sha = _page_membership(root, source_identities)
    if (
        groups != artifact.groups
        or not np.array_equal(page_groups, artifact.page_groups)
        or order_sha != artifact.seal["page_order_sha256"]
    ):
        raise ValueError("independent page mass membership differs")
    pages = len(page_groups)
    counts = np.zeros(pages, dtype=np.uint32)
    totals = np.zeros((pages, DIMENSIONS), dtype=np.float64)
    source = pq.ParquetFile(root / "source.parquet")
    observed: set[int] = set()
    for batch in source.iter_batches(batch_size=2048):
        table = pa.Table.from_batches([batch])
        ids = table.column("feature_row_id").combine_chunks().to_numpy()
        values = np.asarray(
            table.column("embedding").combine_chunks().values.to_numpy(),
            dtype=np.float32,
        ).reshape(-1, DIMENSIONS)
        if not np.isfinite(values).all():
            raise ValueError("independent page mass source differs")
        for row_id, vector in zip(ids, values, strict=True):
            key = int(row_id)
            if key in observed or key not in id_to_page:
                raise ValueError("independent page mass IDs differ")
            observed.add(key)
            page = id_to_page[key]
            counts[page] += 1
            totals[page] += vector.astype(np.float64)
    if len(observed) != len(id_to_page) or np.any(counts == 0):
        raise ValueError("independent page mass counts differ")
    means = totals / counts[:, None]
    squares = np.zeros_like(totals)
    rows = 0
    for batch in source.iter_batches(batch_size=2048):
        table = pa.Table.from_batches([batch])
        ids = table.column("feature_row_id").combine_chunks().to_numpy()
        values = np.asarray(
            table.column("embedding").combine_chunks().values.to_numpy(),
            dtype=np.float32,
        ).reshape(-1, DIMENSIONS)
        for row_id, vector in zip(ids, values, strict=True):
            page = id_to_page[int(row_id)]
            residual = vector.astype(np.float64) - means[page]
            squares[page] += residual * residual
            rows += 1
    if rows != len(id_to_page):
        raise ValueError("independent page mass second pass differs")
    return counts, means, squares / counts[:, None]


def independently_rank_mass(
    query: np.ndarray, artifact: object,
    counts: np.ndarray, means: np.ndarray, variances: np.ndarray,
) -> list[int]:
    difference = means - query.astype(np.float64)
    square = difference * difference
    distance_mean = np.sum(square, axis=1, dtype=np.float64) + np.sum(
        variances, axis=1, dtype=np.float64,
    )
    distance_scale = np.sqrt(
        4 * np.sum(variances * square, axis=1, dtype=np.float64)
        + 2 * np.sum(variances * variances, axis=1, dtype=np.float64)
    )
    lower = float(np.min(distance_mean) - 12 * np.max(distance_scale) - 1)
    upper = float(np.max(distance_mean) + 12 * np.max(distance_scale) + 1)

    def weights(threshold: float) -> np.ndarray:
        zero = distance_scale == 0
        cdf = np.empty(len(counts), dtype=np.float64)
        cdf[zero] = (threshold >= distance_mean[zero]).astype(np.float64)
        cdf[~zero] = ndtr((threshold - distance_mean[~zero]) / distance_scale[~zero])
        return counts * cdf

    for _ in range(48):
        midpoint = (lower + upper) / 2
        if float(np.sum(weights(midpoint), dtype=np.float64)) >= 100:
            upper = midpoint
        else:
            lower = midpoint
    mass = np.bincount(
        artifact.page_groups.astype(np.int64), weights=weights(upper),
        minlength=len(artifact.groups),
    )
    return sorted(range(len(artifact.groups)), key=lambda i: (
        -float(mass[i]), artifact.groups[i].role, artifact.groups[i].ordinal,
    ))


def validate_page_dispersion_mass(
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
        raise ValueError("independent page mass source differs")
    stored = read_page_moments(root / "moments.bin", artifact)
    counts, means, variances = independently_rebuild_moments(root, artifact, source_identities)
    if (
        not np.array_equal(counts, stored.counts)
        or not np.array_equal(means, stored.means)
        or not np.array_equal(variances, stored.variances)
    ):
        raise ValueError("independent page moments differ")
    vectors, truth_ids = _query_truth(
        root / "queries.parquet", root / "truth.parquet", development_identities,
        query_count=query_count,
    )
    owner = dict(zip(
        (int(i) for i in artifact.membership_ids),
        (int(i) for i in artifact.membership_groups), strict=True,
    ))
    groups = tuple(type(group)(
        group.role, group.ordinal, group.first_page, group.end_page, group.row_count,
        4 + 4 * (group.end_page - group.first_page) + 96 * group.row_count,
    ) for group in artifact.groups)
    lengths = [group.code_bytes for group in groups]
    rows: list[dict[str, object]] = []
    controls: list[dict[str, object]] = []
    centers = artifact.page_centroids.astype(np.float64)
    for ordinal, query in enumerate(vectors):
        ranked = independently_rank_mass(query, artifact, counts, means, variances)
        selected, intervals, bytes_used = _plan(groups, ranked, lengths)
        delta = centers - query.astype(np.float64)
        # Preserve the frozen control's floating reduction and tie behavior;
        # the planner and containment replay below remain independent.
        page_scores = np.einsum("ij,ij->i", delta, delta, dtype=np.float64)
        minima = [math.inf] * len(groups)
        for page, score in enumerate(page_scores):
            index = int(artifact.page_groups[page])
            minima[index] = min(minima[index], float(score))
        baseline_rank = sorted(range(len(groups)), key=lambda i: (
            minima[i], groups[i].role, groups[i].ordinal,
        ))
        control, control_intervals, control_bytes = _plan(groups, baseline_rank, lengths)
        truth = [int(i) for i in truth_ids[ordinal]]
        if len(set(truth)) != 100 or any(i not in owner for i in truth):
            raise ValueError("independent page mass truth IDs differ")
        chosen = set(selected)
        control_set = set(control)
        rows.append({
            "query_ordinal": ordinal,
            "ranked_groups": ranked,
            "selected_groups": selected,
            "intervals": intervals,
            "projected_code_gets": len(intervals),
            "projected_code_bytes": bytes_used,
            "hits_at_10": sum(owner[i] in chosen for i in truth[:10]),
            "hits_at_100": sum(owner[i] in chosen for i in truth),
        })
        controls.append({
            "query_ordinal": ordinal,
            "selected_groups": control,
            "intervals": control_intervals,
            "projected_code_gets": len(control_intervals),
            "projected_code_bytes": control_bytes,
            "hits_at_10": sum(owner[i] in control_set for i in truth[:10]),
            "hits_at_100": sum(owner[i] in control_set for i in truth),
        })
    evidence_body = (evidence_dir / "evidence.json").read_bytes()
    result_body = (evidence_dir / "result.json").read_bytes()
    evidence = json.loads(evidence_body)
    result = json.loads(result_body)
    if (
        evidence_body != _canonical_json_bytes(evidence)
        or result_body != _canonical_json_bytes(result)
        or evidence.get("schema") != "borsuk-one-million-page-dispersion-mass-evidence-v1"
        or evidence.get("samples") != rows
        or evidence.get("baseline_samples") != controls
    ):
        raise ValueError("independent page mass evidence differs")
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
        raise ValueError("independent page mass baseline differs")
    if hashlib.sha256(_canonical_json_bytes(controls)).hexdigest() != BASELINE_SAMPLES_SHA256:
        raise ValueError("independent page mass baseline plans differ")
    metrics = _metrics(rows)
    below90 = sum(item["hits_at_100"] < 90 for item in rows)
    metrics["below_90_gt100_queries"] = below90
    decision = (
        "page-dispersion-mass-feasible"
        if metrics["mean_recall_at_100_ppm"] >= BASELINE[0]
        and metrics["p05_recall_at_100_ppm"] >= 900000
        and metrics["mean_recall_at_10_ppm"] >= BASELINE[2]
        and below90 <= 49
        and metrics["max_code_gets"] <= 32
        and metrics["max_projected_code_bytes"] <= 16_777_216
        else "page-dispersion-mass-killed"
    )
    metrics["decision"] = decision
    if (
        evidence.get("metrics") != metrics
        or evidence.get("baseline_metrics") != baseline_metrics
        or set(result) != {
            "schema", "claim_eligible", "decision", "metrics", "baseline_metrics",
            "selector_seal_sha256", "moments_sha256", "evidence_sha256",
            "query_identity", "truth_identity",
        }
        or result.get("schema") != "borsuk-one-million-page-dispersion-mass-result-v1"
        or result.get("claim_eligible") is not False
        or result.get("decision") != decision
        or result.get("metrics") != metrics
        or result.get("baseline_metrics") != baseline_metrics
        or result.get("selector_seal_sha256") != hashlib.sha256((root / "seal.json").read_bytes()).hexdigest()
        or result.get("moments_sha256") != hashlib.sha256((root / "moments.bin").read_bytes()).hexdigest()
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or result.get("query_identity") != dataclasses.asdict(development_identities["queries"])
        or result.get("truth_identity") != dataclasses.asdict(development_identities["truth"])
    ):
        raise ValueError("independent page mass result differs")
    validation = {
        "schema": "borsuk-one-million-page-dispersion-mass-validation-v1",
        "decision": decision,
        "metrics": metrics,
        "baseline_metrics": baseline_metrics,
        "moments_sha256": result["moments_sha256"],
        "evidence_sha256": result["evidence_sha256"],
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "validation.json").write_bytes(_canonical_json_bytes(validation))
    return validation
