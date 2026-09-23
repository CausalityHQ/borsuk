#!/usr/bin/env python3
"""Independent physical-page and query replay for the 1M page selector."""

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
)
from scripts.native_one_million_page_selector import (
    PageSelectorArtifact,
    read_page_selector,
)
from scripts.v97_row_width_screen import ObjectIdentity
from scripts.validate_native_one_million_selector import _canonical, _check


def rebuild_page_arrays(
    root: Path, identities: Mapping[str, ObjectIdentity], *, batch_rows: int = 4096,
) -> tuple[bytes, bytes, list[int], str, list[dict[str, object]]]:
    if set(identities) != {"source", "generation", "base", "delta", "router"} or not 1 <= batch_rows <= 4096:
        raise ValueError("independent page source contract differs")
    for role, name in (("source", "source.parquet"), ("generation", "generation.json"),
                       ("base", "base.arrow"), ("delta", "delta.arrow"), ("router", "router.arrow")):
        _check(root / name, identities[role], role)
    generation_body = (root / "generation.json").read_bytes()
    generation = json.loads(generation_body)
    if generation_body != _canonical(generation) or generation.get("dimensions") != DIMENSIONS or len(generation.get("runs", [])) != 2:
        raise ValueError("independent page generation differs")
    page_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("sequence", pa.uint64(), nullable=False),
        pa.field("state", pa.uint8(), nullable=False),
        pa.field("code", pa.list_(pa.field("element", pa.uint8(), nullable=False), DIMENSIONS), nullable=False),
    ])
    owner: dict[int, int] = {}
    page_groups: list[int] = []
    group_roster: list[dict[str, object]] = []
    counts: list[int] = []
    order = hashlib.sha256()
    for role_index, role in enumerate(("base", "delta")):
        run = generation["runs"][role_index]
        if run.get("kind") != role or run.get("object") != dataclasses.asdict(identities[role]):
            raise ValueError("independent page run differs")
        pages = run["pages"]
        if not pages:
            raise ValueError("independent page roster differs")
        base = len(counts)
        for ordinal in range(math.ceil(len(pages) / GROUP_PAGES)):
            first = ordinal * GROUP_PAGES
            group_roster.append({
                "role": role, "ordinal": ordinal, "first_page": first,
                "end_page": min(first + GROUP_PAGES, len(pages)),
            })
            counts.append(0)
        with (root / (role + ".arrow")).open("rb") as handle:
            next_offset = 0
            for ordinal, page in enumerate(pages):
                if (set(page) != {"bytes", "offset", "page", "rows"}
                    or page["page"] != ordinal or page["offset"] != next_offset
                    or page["bytes"] <= 0 or not 1 <= page["rows"] <= 256):
                    raise ValueError("independent page roster differs")
                handle.seek(next_offset)
                payload = handle.read(page["bytes"])
                if len(payload) != page["bytes"]:
                    raise ValueError("independent page bytes differ")
                table = ipc.open_stream(payload).read_all()
                if table.schema != page_schema or table.num_rows != page["rows"]:
                    raise ValueError("independent page schema differs")
                ids = table.column("id").to_pylist()
                states = table.column("state").to_pylist()
                if any(value != 0 for value in states):
                    raise ValueError("independent page state differs")
                page_index = len(page_groups)
                group_index = base + ordinal // GROUP_PAGES
                page_groups.append(group_index)
                for row_id in ids:
                    if row_id < 0 or row_id in owner:
                        raise ValueError("independent page rows overlap")
                    owner[row_id] = page_index
                    counts[group_index] += 1
                    order.update(struct.pack("<BIQ", role_index, ordinal, row_id))
                next_offset += page["bytes"]
            if next_offset != identities[role].bytes:
                raise ValueError("independent page object length differs")
    source = pq.ParquetFile(root / "source.parquet")
    expected = pa.schema([
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("embedding", pa.list_(pa.field("item", pa.float32(), nullable=False), DIMENSIONS), nullable=False),
    ])
    if source.schema_arrow != expected:
        raise ValueError("independent page source schema differs")
    sums = np.zeros((len(page_groups), DIMENSIONS), dtype=np.float64)
    page_counts = np.zeros(len(page_groups), dtype=np.int64)
    seen: set[int] = set()
    for batch in source.iter_batches(batch_size=batch_rows):
        table = pa.Table.from_batches([batch])
        ids = table.column("feature_row_id").combine_chunks().to_numpy()
        column = table.column("embedding").combine_chunks()
        vectors = np.asarray(column.values.to_numpy(), dtype=np.float32).reshape(-1, DIMENSIONS)
        if column.null_count or not np.isfinite(vectors).all():
            raise ValueError("independent page vectors differ")
        for raw, vector in zip(ids, vectors, strict=True):
            row_id = int(raw)
            if row_id not in owner or row_id in seen:
                raise ValueError("independent page source rows differ")
            seen.add(row_id)
            page = owner[row_id]
            sums[page] += vector.astype(np.float64)
            page_counts[page] += 1
    if len(seen) != len(owner) or np.any(page_counts <= 0):
        raise ValueError("independent page source rows differ")
    centers = np.asarray(sums / page_counts[:, None], dtype="<f2")
    membership = np.empty(len(owner), dtype=MEMBERSHIP_DTYPE)
    for index, row_id in enumerate(sorted(owner)):
        membership[index] = row_id, page_groups[owner[row_id]]
    for index, item in enumerate(group_roster):
        item["row_count"] = counts[index]
        item["code_bytes"] = 200 * counts[index] + 4 + 4 * (item["end_page"] - item["first_page"])
    return centers.tobytes(order="C"), membership.tobytes(order="C"), page_groups, order.hexdigest(), group_roster


def replay_page_evidence(
    artifact: PageSelectorArtifact, queries: Path, truth: Path, evidence_dir: Path,
    identities: Mapping[str, ObjectIdentity], *, query_count: int = 1000,
) -> dict[str, object]:
    if set(identities) != {"queries", "truth"}:
        raise ValueError("independent page query contract differs")
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
        raise ValueError("independent page query schema differs")
    query_ordinals = qt.column("query_ordinal").combine_chunks().to_pylist()
    truth_ordinals = tt.column("query_ordinal").combine_chunks().to_pylist()
    ranks = tt.column("rank").combine_chunks().to_pylist()
    truth_ids = tt.column("feature_row_id").combine_chunks().to_pylist()
    if query_ordinals != list(range(query_count)) or truth_ordinals != [i for i in range(query_count) for _ in range(100)] or ranks != list(range(100)) * query_count:
        raise ValueError("independent page query order differs")
    vectors = np.asarray(qt.column("embedding").combine_chunks().values.to_numpy(), dtype=np.float32).reshape(query_count, DIMENSIONS)
    if not np.isfinite(vectors).all():
        raise ValueError("independent page query vectors differ")
    owner = dict(zip((int(v) for v in artifact.membership_ids), (int(v) for v in artifact.membership_groups), strict=True))
    samples: list[dict[str, object]] = []
    centers = artifact.page_centroids.astype(np.float64)
    for ordinal, query in enumerate(vectors):
        delta = centers - query.astype(np.float64)
        page_scores = np.sum(delta * delta, axis=1, dtype=np.float64)
        minima = [math.inf] * len(artifact.groups)
        for page, score in enumerate(page_scores):
            group = int(artifact.page_groups[page])
            minima[group] = min(minima[group], float(score))
        selected = sorted(range(len(artifact.groups)), key=lambda i: (minima[i], artifact.groups[i].role, artifact.groups[i].ordinal))[:32]
        chosen = set(selected)
        row = [int(v) for v in truth_ids[ordinal * 100 : (ordinal + 1) * 100]]
        if len(set(row)) != 100 or any(v not in owner for v in row):
            raise ValueError("independent page truth IDs differ")
        samples.append({
            "query_ordinal": ordinal, "selected_groups": selected,
            "projected_code_bytes": sum(artifact.groups[i].code_bytes for i in selected),
            "hits_at_10": sum(owner[v] in chosen for v in row[:10]),
            "hits_at_100": sum(owner[v] in chosen for v in row),
        })
    evidence_body = (evidence_dir / "evidence.json").read_bytes()
    result_body = (evidence_dir / "result.json").read_bytes()
    evidence = json.loads(evidence_body)
    result = json.loads(result_body)
    if evidence_body != _canonical(evidence) or result_body != _canonical(result):
        raise ValueError("independent page evidence canonical bytes differ")
    if evidence.get("schema") != "borsuk-one-million-page-selector-evidence-v1" or evidence.get("samples") != samples:
        raise ValueError("independent page samples differ")
    hits100 = [sample["hits_at_100"] for sample in samples]
    hits10 = [sample["hits_at_10"] for sample in samples]
    metrics = {
        "query_count": query_count,
        "mean_recall_at_100_ppm": sum(hits100) * 10000 // query_count,
        "p05_recall_at_100_ppm": sorted(hits100)[math.ceil(query_count * .05) - 1] * 10000,
        "mean_recall_at_10_ppm": sum(hits10) * 100000 // query_count,
        "max_projected_code_bytes": max(sample["projected_code_bytes"] for sample in samples),
        "max_code_gets": max(len(sample["selected_groups"]) for sample in samples),
    }
    metrics["decision"] = (
        "page-representative-selector-feasible"
        if metrics["mean_recall_at_100_ppm"] >= 975000
        and metrics["p05_recall_at_100_ppm"] >= 900000
        and metrics["mean_recall_at_10_ppm"] >= 960000
        and metrics["max_projected_code_bytes"] <= 16777216
        and metrics["max_code_gets"] <= 32
        else "page-representative-selector-killed"
    )
    if (
        evidence.get("metrics") != metrics
        or result.get("metrics") != metrics
        or result.get("decision") != metrics["decision"]
        or result.get("schema") != "borsuk-one-million-page-selector-result-v1"
        or result.get("claim_eligible") is not False
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or result.get("selector_seal_sha256") != hashlib.sha256(_canonical(artifact.seal)).hexdigest()
        or result.get("query_identity") != dataclasses.asdict(identities["queries"])
        or result.get("truth_identity") != dataclasses.asdict(identities["truth"])
    ):
        raise ValueError("independent page result differs")
    return metrics


def validate_page_selector(
    root: Path, selector_dir: Path, evidence_dir: Path, out: Path,
    source_identities: Mapping[str, ObjectIdentity],
    development_identities: Mapping[str, ObjectIdentity], *, query_count: int = 1000,
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
        raise ValueError("independent page source replay differs")
    metrics = replay_page_evidence(
        artifact, root / "queries.parquet", root / "truth.parquet", evidence_dir,
        development_identities, query_count=query_count,
    )
    validation: dict[str, object] = {
        "schema": "borsuk-one-million-page-selector-validation-v1",
        "decision": metrics["decision"], "metrics": metrics,
        "selector_seal_sha256": hashlib.sha256((selector_dir / "seal.json").read_bytes()).hexdigest(),
        "evidence_sha256": hashlib.sha256((evidence_dir / "evidence.json").read_bytes()).hexdigest(),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "validation.json").write_bytes(_canonical(validation))
    return validation
