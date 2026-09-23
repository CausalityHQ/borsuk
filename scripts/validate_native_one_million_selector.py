#!/usr/bin/env python3
"""Independent source and frozen-query replay of the 1M centroid selector."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import struct
from collections.abc import Mapping
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from scripts.native_one_million_group_selector import (
    DIMENSIONS,
    GROUP_PAGES,
    MEMBERSHIP_DTYPE,
    SelectorArtifact,
    read_selector,
)
from scripts.v97_row_width_screen import ObjectIdentity


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _check(path: Path, identity: ObjectIdentity, role: str) -> None:
    if path.stat().st_size != identity.bytes or _digest(path) != identity.sha256:
        raise ValueError(role + " identity differs")


def rebuild_source_selector(
    root: Path,
    identities: Mapping[str, ObjectIdentity],
    *,
    batch_rows: int = 4096,
) -> tuple[bytes, bytes, str, list[dict[str, object]]]:
    """Reparse physical pages and sum source vectors without producer helpers."""
    if set(identities) != {"source", "generation", "base", "delta", "router"} or not 1 <= batch_rows <= 4096:
        raise ValueError("independent source contract differs")
    for role, name in (("source", "source.parquet"), ("generation", "generation.json"),
                       ("base", "base.arrow"), ("delta", "delta.arrow"), ("router", "router.arrow")):
        _check(root / name, identities[role], role)
    generation_body = (root / "generation.json").read_bytes()
    generation = json.loads(generation_body)
    if generation_body != _canonical(generation) or generation.get("dimensions") != DIMENSIONS or len(generation.get("runs", [])) != 2:
        raise ValueError("independent generation differs")
    expected_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("sequence", pa.uint64(), nullable=False),
        pa.field("state", pa.uint8(), nullable=False),
        pa.field("code", pa.list_(pa.field("element", pa.uint8(), nullable=False), DIMENSIONS), nullable=False),
    ])
    mapping: dict[int, int] = {}
    group_counts: list[int] = []
    group_roster: list[dict[str, object]] = []
    page_order = hashlib.sha256()
    for role_index, role in enumerate(("base", "delta")):
        run = generation["runs"][role_index]
        if run.get("kind") != role or run.get("object") != dataclasses.asdict(identities[role]):
            raise ValueError("independent run differs")
        pages = run["pages"]
        if not pages:
            raise ValueError("independent page roster differs")
        group_base = len(group_counts)
        group_count = math.ceil(len(pages) / GROUP_PAGES)
        group_counts.extend([0] * group_count)
        for ordinal in range(group_count):
            first = ordinal * GROUP_PAGES
            group_roster.append({
                "role": role, "ordinal": ordinal, "first_page": first,
                "end_page": min(first + GROUP_PAGES, len(pages)),
            })
        with (root / (role + ".arrow")).open("rb") as handle:
            next_offset = 0
            for ordinal, page in enumerate(pages):
                if (
                    set(page) != {"bytes", "offset", "page", "rows"}
                    or page["page"] != ordinal or page["offset"] != next_offset
                    or page["bytes"] <= 0 or not 1 <= page["rows"] <= 256
                ):
                    raise ValueError("independent page roster differs")
                handle.seek(page["offset"])
                payload = handle.read(page["bytes"])
                if len(payload) != page["bytes"]:
                    raise ValueError("independent page bytes differ")
                table = ipc.open_stream(payload).read_all()
                if table.schema != expected_schema or table.num_rows != page["rows"]:
                    raise ValueError("independent page schema differs")
                ids = table.column("id").to_pylist()
                states = table.column("state").to_pylist()
                if any(state != 0 for state in states):
                    raise ValueError("independent page state differs")
                group = group_base + ordinal // GROUP_PAGES
                for row_id in ids:
                    if row_id < 0 or row_id in mapping:
                        raise ValueError("independent page rows overlap")
                    mapping[row_id] = group
                    group_counts[group] += 1
                    page_order.update(struct.pack("<BIQ", role_index, ordinal, row_id))
                next_offset += page["bytes"]
            if next_offset != identities[role].bytes:
                raise ValueError("independent page object length differs")
    source = pq.ParquetFile(root / "source.parquet")
    expected_source_schema = pa.schema([
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("embedding", pa.list_(pa.field("item", pa.float32(), nullable=False), DIMENSIONS), nullable=False),
    ])
    if source.schema_arrow != expected_source_schema:
        raise ValueError("independent source schema differs")
    sums = np.zeros((len(group_counts), DIMENSIONS), dtype=np.float64)
    seen: set[int] = set()
    for batch in source.iter_batches(batch_size=batch_rows):
        table = pa.Table.from_batches([batch])
        ids = table.column("feature_row_id").combine_chunks().to_numpy()
        vector_column = table.column("embedding").combine_chunks()
        vectors = np.asarray(vector_column.values.to_numpy(), dtype=np.float32).reshape(-1, DIMENSIONS)
        if vector_column.null_count or not np.isfinite(vectors).all():
            raise ValueError("independent source vectors differ")
        for row_id_raw, vector in zip(ids, vectors, strict=True):
            row_id = int(row_id_raw)
            if row_id in seen or row_id not in mapping:
                raise ValueError("independent source rows differ")
            seen.add(row_id)
            sums[mapping[row_id]] += vector.astype(np.float64)
    if len(seen) != len(mapping) or any(count <= 0 for count in group_counts):
        raise ValueError("independent source rows differ")
    centroids = np.asarray(sums / np.asarray(group_counts, dtype=np.float64)[:, None], dtype="<f2")
    membership = np.empty(len(mapping), dtype=MEMBERSHIP_DTYPE)
    for index, row_id in enumerate(sorted(mapping)):
        membership[index] = row_id, mapping[row_id]
    for index, item in enumerate(group_roster):
        item["row_count"] = group_counts[index]
        item["code_bytes"] = 200 * group_counts[index] + 4 + 4 * (item["end_page"] - item["first_page"])
    return centroids.tobytes(order="C"), membership.tobytes(order="C"), page_order.hexdigest(), group_roster


def replay_selector_evidence(
    artifact: SelectorArtifact,
    queries: Path,
    truth: Path,
    evidence_dir: Path,
    identities: Mapping[str, ObjectIdentity],
    *,
    query_count: int = 1000,
) -> dict[str, object]:
    """Recompute rankings with scalar page-owner lookup and an independent score."""
    if set(identities) != {"queries", "truth"} or query_count <= 0:
        raise ValueError("independent query contract differs")
    _check(queries, identities["queries"], "queries")
    _check(truth, identities["truth"], "truth")
    query_table = pq.read_table(queries)
    truth_table = pq.read_table(truth)
    expected_query_schema = pa.schema([
        pa.field("query_ordinal", pa.uint32(), nullable=False),
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("embedding", pa.list_(pa.field("item", pa.float32(), nullable=False), DIMENSIONS), nullable=False),
    ])
    expected_truth_schema = pa.schema([
        pa.field("query_ordinal", pa.uint32(), nullable=False),
        pa.field("rank", pa.uint16(), nullable=False),
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("squared_distance", pa.float64(), nullable=False),
    ])
    if query_table.schema != expected_query_schema or query_table.num_rows != query_count or truth_table.schema != expected_truth_schema or truth_table.num_rows != query_count * 100:
        raise ValueError("independent query schema differs")
    vectors = np.asarray(query_table.column("embedding").combine_chunks().values.to_numpy(), dtype=np.float32).reshape(query_count, DIMENSIONS)
    query_ordinals = query_table.column("query_ordinal").combine_chunks().to_pylist()
    truth_ordinals = truth_table.column("query_ordinal").combine_chunks().to_pylist()
    ranks = truth_table.column("rank").combine_chunks().to_pylist()
    truth_ids = truth_table.column("feature_row_id").combine_chunks().to_pylist()
    if (
        query_ordinals != list(range(query_count))
        or truth_ordinals != [ordinal for ordinal in range(query_count) for _ in range(100)]
        or ranks != list(range(100)) * query_count
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("independent query authority differs")
    owner = dict(zip((int(value) for value in artifact.membership_ids), (int(value) for value in artifact.membership_groups), strict=True))
    samples: list[dict[str, object]] = []
    centers = artifact.centroids.astype(np.float64)
    for ordinal, query in enumerate(vectors):
        delta = centers - query.astype(np.float64)
        scores = np.sum(delta * delta, axis=1, dtype=np.float64)
        selected = sorted(range(len(artifact.groups)), key=lambda index: (float(scores[index]), artifact.groups[index].role, artifact.groups[index].ordinal))[:32]
        selected_set = set(selected)
        row = [int(value) for value in truth_ids[ordinal * 100 : (ordinal + 1) * 100]]
        if len(set(row)) != 100 or any(value not in owner for value in row):
            raise ValueError("independent truth authority differs")
        samples.append({
            "query_ordinal": ordinal,
            "selected_groups": selected,
            "projected_code_bytes": sum(artifact.groups[index].code_bytes for index in selected),
            "hits_at_10": sum(owner[value] in selected_set for value in row[:10]),
            "hits_at_100": sum(owner[value] in selected_set for value in row),
        })
    evidence_body = (evidence_dir / "evidence.json").read_bytes()
    result_body = (evidence_dir / "result.json").read_bytes()
    evidence = json.loads(evidence_body)
    result = json.loads(result_body)
    if evidence_body != _canonical(evidence) or result_body != _canonical(result):
        raise ValueError("independent evidence canonical bytes differ")
    if evidence.get("samples") != samples or evidence.get("schema") != "borsuk-one-million-centroid-selector-evidence-v1":
        raise ValueError("independent evidence samples differ")
    hits100 = [sample["hits_at_100"] for sample in samples]
    hits10 = [sample["hits_at_10"] for sample in samples]
    metrics = {
        "query_count": query_count,
        "mean_recall_at_100_ppm": sum(hits100) * 10000 // query_count,
        "p05_recall_at_100_ppm": sorted(hits100)[math.ceil(query_count * 0.05) - 1] * 10000,
        "mean_recall_at_10_ppm": sum(hits10) * 100000 // query_count,
        "max_projected_code_bytes": max(sample["projected_code_bytes"] for sample in samples),
        "max_code_gets": max(len(sample["selected_groups"]) for sample in samples),
    }
    decision = (
        "selector-feasible" if metrics["mean_recall_at_100_ppm"] >= 975000
        and metrics["p05_recall_at_100_ppm"] >= 900000
        and metrics["mean_recall_at_10_ppm"] >= 960000
        and metrics["max_projected_code_bytes"] <= 16777216
        and metrics["max_code_gets"] <= 32
        else "group-centroid-selector-killed"
    )
    metrics["decision"] = decision
    if (
        evidence.get("metrics") != metrics
        or result.get("metrics") != metrics
        or result.get("decision") != decision
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or result.get("selector_seal_sha256") != hashlib.sha256(_canonical(artifact.seal)).hexdigest()
        or result.get("query_identity") != dataclasses.asdict(identities["queries"])
        or result.get("truth_identity") != dataclasses.asdict(identities["truth"])
        or result.get("schema") != "borsuk-one-million-centroid-selector-result-v1"
    ):
        raise ValueError("independent result authority differs")
    return metrics


def validate_selector(
    root: Path,
    selector_dir: Path,
    evidence_dir: Path,
    out: Path,
    source_identities: Mapping[str, ObjectIdentity],
    development_identities: Mapping[str, ObjectIdentity],
    *,
    query_count: int = 1000,
) -> dict[str, object]:
    artifact = read_selector(selector_dir, source_identities)
    centroids, membership, page_order_sha, groups = rebuild_source_selector(root, source_identities)
    if (
        centroids != (selector_dir / "centroids.bin").read_bytes()
        or membership != (selector_dir / "membership.bin").read_bytes()
        or page_order_sha != artifact.seal["page_order_sha256"]
        or groups != artifact.seal["groups"]
    ):
        raise ValueError("independent selector source replay differs")
    metrics = replay_selector_evidence(
        artifact, root / "queries.parquet", root / "truth.parquet", evidence_dir,
        development_identities, query_count=query_count,
    )
    validation: dict[str, object] = {
        "schema": "borsuk-one-million-centroid-selector-validation-v1",
        "decision": metrics["decision"],
        "metrics": metrics,
        "selector_seal_sha256": hashlib.sha256((selector_dir / "seal.json").read_bytes()).hexdigest(),
        "evidence_sha256": hashlib.sha256((evidence_dir / "evidence.json").read_bytes()).hexdigest(),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "validation.json").write_bytes(_canonical(validation))
    return validation
