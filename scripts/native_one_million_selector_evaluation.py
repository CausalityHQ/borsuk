#!/usr/bin/env python3
"""One fixed centroid-to-32-group ReLAION-1M development selector."""

from __future__ import annotations

import dataclasses
import hashlib
import math
from collections.abc import Mapping
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_one_million_group_selector import DIMENSIONS, SelectorArtifact
from scripts.v97_row_width_screen import (
    ObjectIdentity,
    _authenticate_object,
    _canonical_json_bytes,
)

SCHEMA = "borsuk-one-million-centroid-selector-result-v1"
EVIDENCE_SCHEMA = "borsuk-one-million-centroid-selector-evidence-v1"
MAXIMUM_GETS = 32
MAXIMUM_BYTES = 16_777_216
NEIGHBORS = 100


def select_groups(query: np.ndarray, artifact: SelectorArtifact) -> tuple[int, ...]:
    """Rank all sealed float16 centroids with stable role/ordinal ties."""
    if (
        query.shape != (DIMENSIONS,)
        or not np.isfinite(query).all()
        or len(artifact.groups) < MAXIMUM_GETS
        or artifact.centroids.shape != (len(artifact.groups), DIMENSIONS)
    ):
        raise ValueError("selector query authority differs")
    difference = artifact.centroids.astype(np.float64) - np.asarray(query, dtype=np.float64)
    scores = np.einsum("ij,ij->i", difference, difference, dtype=np.float64)
    if not np.isfinite(scores).all():
        raise ValueError("selector scores differ")
    return tuple(sorted(
        range(len(artifact.groups)),
        key=lambda index: (
            float(scores[index]), artifact.groups[index].role,
            artifact.groups[index].ordinal,
        ),
    )[:MAXIMUM_GETS])


def _query_truth(
    queries: Path,
    truth: Path,
    identities: Mapping[str, ObjectIdentity],
    *,
    query_count: int,
) -> tuple[np.ndarray, np.ndarray]:
    if set(identities) != {"queries", "truth"} or query_count <= 0:
        raise ValueError("selector development contract differs")
    _authenticate_object("queries", queries, identities["queries"])
    _authenticate_object("truth", truth, identities["truth"])
    query_schema = pa.schema([
        pa.field("query_ordinal", pa.uint32(), nullable=False),
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("embedding", pa.list_(pa.field("item", pa.float32(), nullable=False), DIMENSIONS), nullable=False),
    ])
    query_table = pq.read_table(queries)
    if query_table.schema != query_schema or query_table.num_rows != query_count:
        raise ValueError("selector queries differ")
    ordinals = np.asarray(query_table.column("query_ordinal").combine_chunks().to_numpy(), dtype=np.uint32)
    vectors_array = query_table.column("embedding").combine_chunks()
    vectors = np.asarray(vectors_array.values.to_numpy(), dtype=np.float32).reshape(-1, DIMENSIONS)
    if (
        vectors_array.null_count
        or not np.array_equal(ordinals, np.arange(query_count, dtype=np.uint32))
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("selector queries differ")
    truth_schema = pa.schema([
        pa.field("query_ordinal", pa.uint32(), nullable=False),
        pa.field("rank", pa.uint16(), nullable=False),
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("squared_distance", pa.float64(), nullable=False),
    ])
    truth_table = pq.read_table(truth)
    if truth_table.schema != truth_schema or truth_table.num_rows != query_count * NEIGHBORS:
        raise ValueError("selector truth differs")
    truth_ordinals = np.asarray(truth_table.column("query_ordinal").combine_chunks().to_numpy(), dtype=np.uint32)
    ranks = np.asarray(truth_table.column("rank").combine_chunks().to_numpy(), dtype=np.uint16)
    ids_unsigned = np.asarray(truth_table.column("feature_row_id").combine_chunks().to_numpy(), dtype=np.uint64)
    distances = np.asarray(truth_table.column("squared_distance").combine_chunks().to_numpy(), dtype=np.float64).reshape(query_count, NEIGHBORS)
    if (
        np.any(ids_unsigned > np.iinfo(np.int64).max)
        or not np.array_equal(truth_ordinals, np.repeat(np.arange(query_count, dtype=np.uint32), NEIGHBORS))
        or not np.array_equal(ranks, np.tile(np.arange(NEIGHBORS, dtype=np.uint16), query_count))
        or not np.isfinite(distances).all()
        or np.any(distances < 0)
        or np.any(distances[:, 1:] < distances[:, :-1])
    ):
        raise ValueError("selector truth differs")
    ids = ids_unsigned.astype(np.int64, copy=False).reshape(query_count, NEIGHBORS)
    if any(len(set(int(value) for value in row)) != NEIGHBORS for row in ids):
        raise ValueError("selector truth differs")
    return vectors, ids


def aggregate_samples(samples: list[dict[str, object]]) -> dict[str, int | str]:
    if not samples or any(sample["query_ordinal"] != index for index, sample in enumerate(samples)):
        raise ValueError("selector samples differ")
    count = len(samples)
    hit100 = [int(sample["hits_at_100"]) for sample in samples]
    hit10 = [int(sample["hits_at_10"]) for sample in samples]
    max_bytes = max(int(sample["projected_code_bytes"]) for sample in samples)
    if any(not 0 <= hits <= NEIGHBORS for hits in hit100) or any(not 0 <= hits <= 10 for hits in hit10):
        raise ValueError("selector hits differ")
    metrics: dict[str, int | str] = {
        "query_count": count,
        "mean_recall_at_100_ppm": sum(hit100) * 1_000_000 // (count * NEIGHBORS),
        "p05_recall_at_100_ppm": sorted(hit100)[math.ceil(count * 0.05) - 1] * 10_000,
        "mean_recall_at_10_ppm": sum(hit10) * 1_000_000 // (count * 10),
        "max_projected_code_bytes": max_bytes,
        "max_code_gets": max(len(sample["selected_groups"]) for sample in samples),
    }
    metrics["decision"] = (
        "selector-feasible"
        if metrics["mean_recall_at_100_ppm"] >= 975_000
        and metrics["p05_recall_at_100_ppm"] >= 900_000
        and metrics["mean_recall_at_10_ppm"] >= 960_000
        and metrics["max_projected_code_bytes"] <= MAXIMUM_BYTES
        and metrics["max_code_gets"] <= MAXIMUM_GETS
        else "group-centroid-selector-killed"
    )
    return metrics


def evaluate_selector(
    artifact: SelectorArtifact,
    queries: Path,
    truth: Path,
    out: Path,
    identities: Mapping[str, ObjectIdentity],
    *,
    query_count: int = 1000,
) -> dict[str, object]:
    vectors, truth_ids = _query_truth(queries, truth, identities, query_count=query_count)
    membership_ids = artifact.membership_ids
    membership_groups = artifact.membership_groups
    if len(membership_ids) == 0 or not np.all(membership_ids[1:] > membership_ids[:-1]):
        raise ValueError("selector membership differs")
    positions = np.searchsorted(membership_ids, truth_ids)
    if np.any(positions >= len(membership_ids)) or not np.array_equal(membership_ids[positions], truth_ids):
        raise ValueError("selector truth IDs differ")
    truth_groups = membership_groups[positions]
    samples: list[dict[str, object]] = []
    for ordinal, query in enumerate(vectors):
        selected = select_groups(query, artifact)
        selected_set = set(selected)
        owner = truth_groups[ordinal]
        sample: dict[str, object] = {
            "query_ordinal": ordinal,
            "selected_groups": list(selected),
            "projected_code_bytes": sum(artifact.groups[index].code_bytes for index in selected),
            "hits_at_10": sum(int(value) in selected_set for value in owner[:10]),
            "hits_at_100": sum(int(value) in selected_set for value in owner),
        }
        samples.append(sample)
    metrics = aggregate_samples(samples)
    evidence = {"schema": EVIDENCE_SCHEMA, "samples": samples, "metrics": metrics}
    out.mkdir(parents=True, exist_ok=True)
    evidence_body = _canonical_json_bytes(evidence)
    (out / "evidence.json").write_bytes(evidence_body)
    result: dict[str, object] = {
        "schema": SCHEMA,
        "decision": metrics["decision"],
        "claim_eligible": False,
        "query_identity": dataclasses.asdict(identities["queries"]),
        "truth_identity": dataclasses.asdict(identities["truth"]),
        "selector_seal_sha256": hashlib.sha256(_canonical_json_bytes(artifact.seal)).hexdigest(),
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "metrics": metrics,
    }
    (out / "result.json").write_bytes(_canonical_json_bytes(result))
    return result
