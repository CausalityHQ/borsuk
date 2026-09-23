#!/usr/bin/env python3
"""Rebuild 100k OPQ8 source artifacts and replay route/containment plans."""

from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path

import numpy as np
from scipy.linalg import svd

from scripts.native_hundred_thousand_opq8_cell import (
    FROZEN_INPUTS,
    HISTORICAL_EVIDENCE,
    HISTORICAL_SAMPLES_SHA256,
    _canonical,
    _read_prior_code_seal,
)
from scripts.native_hundred_thousand_opq8_router import (
    CENTROIDS,
    DIMENSIONS,
    DOMAIN,
    LLOYD_STEPS,
    OUTER_STEPS,
    SEED,
    SUBSPACES,
    TRAINING_ROWS,
    read_opq8_model,
)
from scripts.native_page_microcluster_cell import (
    _authenticate,
    _read_inputs,
    _read_queries_truth,
)
from scripts.native_row_score_code_artifacts import (
    _physical_order,
    _physical_order_sha256,
)
from scripts.validate_native_geometric_layout_result import _read_geometric_queries


def _rebuild_model_and_codes(
    vectors: np.ndarray, physical: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    prefix = DOMAIN + struct.pack("<I", SEED)
    selected = sorted(
        range(len(vectors)),
        key=lambda i: (hashlib.sha256(prefix + struct.pack("<Q", i)).digest(), i),
    )[:TRAINING_ROWS]
    selected_array = np.asarray(selected, dtype=np.int64)
    average = np.mean(vectors, axis=0, dtype=np.float64).astype("<f4")
    training = vectors[selected_array].astype(np.float64) - average.astype(np.float64)
    width = DIMENSIONS // SUBSPACES

    def nearest(rows: np.ndarray, centers: np.ndarray) -> np.ndarray:
        assignments = np.empty(len(rows), dtype=np.uint8)
        norms = np.sum(centers * centers, axis=1, dtype=np.float64)
        for start in range(0, len(rows), 4096):
            block = rows[start:start + 4096]
            assignments[start:start + len(block)] = np.argmin(
                norms[None, :] - 2 * (block @ centers.T), axis=1,
            ).astype(np.uint8)
        return assignments

    books = np.empty((SUBSPACES, CENTROIDS, width), dtype=np.float64)
    for part in range(SUBSPACES):
        rows = training[:, part * width:(part + 1) * width]
        chosen = [0]
        distance = np.sum((rows - rows[0]) ** 2, axis=1, dtype=np.float64)
        for _ in range(1, CENTROIDS):
            distance[chosen[-1]] = -1
            maximum = np.max(distance)
            index = min(np.flatnonzero(distance == maximum), key=lambda j: selected[j])
            chosen.append(index)
            distance = np.minimum(
                distance, np.sum((rows - rows[index]) ** 2, axis=1, dtype=np.float64),
            )
        books[part] = rows[chosen]

    def update(rows: np.ndarray, centers: np.ndarray) -> np.ndarray:
        output = centers.copy()
        for _ in range(LLOYD_STEPS):
            assignments = nearest(rows, output)
            populations = np.bincount(assignments, minlength=CENTROIDS)
            sums = np.zeros_like(output)
            np.add.at(sums, assignments, rows)
            occupied = populations > 0
            output[occupied] = sums[occupied] / populations[occupied, None]
        return output

    rotation = np.eye(DIMENSIONS, dtype=np.float64)
    for _ in range(OUTER_STEPS):
        transformed = training @ rotation
        decoded = np.empty_like(transformed)
        for part in range(SUBSPACES):
            lo = part * width
            rows = transformed[:, lo:lo + width]
            books[part] = update(rows, books[part])
            decoded[:, lo:lo + width] = books[part][nearest(rows, books[part])]
        left, _, right = svd(training.T @ decoded, full_matrices=False, lapack_driver="gesvd")
        rotation = left @ right
    rotation = rotation.astype("<f4")
    transformed = training @ rotation.astype(np.float64)
    for part in range(SUBSPACES):
        lo = part * width
        books[part] = update(transformed[:, lo:lo + width], books[part])
    books = books.astype("<f4")
    codes = np.empty((len(physical), SUBSPACES), dtype=np.uint8)
    for start in range(0, len(physical), 4096):
        stop = min(start + 4096, len(physical))
        rows = (vectors[physical[start:stop]].astype(np.float64) - average.astype(np.float64)) @ rotation.astype(np.float64)
        for part in range(SUBSPACES):
            lo = part * width
            codes[start:stop, part] = nearest(rows[:, lo:lo + width], books[part].astype(np.float64))
    return average, rotation, books, codes, selected_array


def _independent_rank(
    query: np.ndarray, mean: np.ndarray, rotation: np.ndarray,
    books: np.ndarray, codes: np.ndarray, counts: tuple[int, ...],
) -> list[int]:
    rotated = (query.astype(np.float64) - mean.astype(np.float64)) @ rotation.astype(np.float64)
    distances = np.zeros(len(codes), dtype=np.float64)
    for subspace in range(SUBSPACES):
        first = subspace * (DIMENSIONS // SUBSPACES)
        center = books[subspace].astype(np.float64)
        residual = center - rotated[first:first + DIMENSIONS // SUBSPACES]
        lookup = np.sum(residual * residual, axis=1, dtype=np.float64)
        distances += lookup[codes[:, subspace]]
    scores = distances.astype(np.float32)
    offsets = np.concatenate(([0], np.cumsum(counts)))
    keys: list[tuple[float, float, int]] = []
    for group in range((len(counts) + 3) // 4):
        lo = int(offsets[group * 4])
        hi = int(offsets[min((group + 1) * 4, len(counts))])
        candidate = scores[lo:hi]
        row_order = np.lexsort((np.arange(lo, hi), candidate))[:min(4, len(candidate))]
        top = candidate[row_order].astype(np.float64)
        keys.append((float(np.sum(top, dtype=np.float64) / len(top)), float(top[0]), group))
    return [item[2] for item in sorted(keys)]


def _independent_plan(ranked: list[int], ranges: list[list[object]]) -> tuple[list[int], int]:
    if len(ranked) != len(ranges) or set(ranked) != set(range(len(ranges))):
        raise ValueError("independent OPQ8 rank differs")
    selected: list[int] = []
    total = 0
    for index in ranked:
        length = ranges[index][3]
        if len(selected) == 32:
            break
        if total + length <= 16_777_216:
            selected.append(index)
            total += length
    return selected, total


def validate_opq8(root: Path, out: Path) -> dict[str, object]:
    ids, vectors, membership = _read_inputs(root, FROZEN_INPUTS)
    prior = _read_prior_code_seal(root)
    ordinals, counts, _ = _physical_order(ids, membership, FROZEN_INPUTS.layout.seed)
    if (
        list(counts) != prior["page_row_counts"]
        or _physical_order_sha256(ordinals) != prior["physical_order_sha256"]
    ):
        raise ValueError("independent OPQ8 physical order differs")
    seal_body = (root / "seal.json").read_bytes()
    seal = json.loads(seal_body)
    if (
        seal_body != _canonical(seal)
        or seal["physical_order_sha256"] != prior["physical_order_sha256"]
        or seal["group_ranges"] != prior["group_ranges"]
    ):
        raise ValueError("independent OPQ8 source seal differs")
    model = read_opq8_model(root / "model.bin")
    code_body = (root / "codes.bin").read_bytes()
    if len(code_body) != 100_000 * SUBSPACES:
        raise ValueError("independent OPQ8 code length differs")
    codes = np.frombuffer(code_body, dtype=np.uint8).reshape(100_000, SUBSPACES)
    if (
        hashlib.sha256((root / "model.bin").read_bytes()).hexdigest() != seal["model_sha256"]
        or hashlib.sha256(code_body).hexdigest() != seal["codes_sha256"]
    ):
        raise ValueError("independent OPQ8 source artifact differs")
    mean, rotation, books, reencoded, selected = _rebuild_model_and_codes(
        vectors, np.asarray(ordinals, dtype=np.int64),
    )
    if (
        not np.array_equal(mean, model.mean)
        or not np.array_equal(rotation, model.rotation)
        or not np.array_equal(books, model.books)
        or hashlib.sha256(selected.astype("<u8").tobytes()).hexdigest() != seal["training_ordinals_sha256"]
    ):
        raise ValueError("independent OPQ8 source training differs")
    if not np.array_equal(reencoded, codes):
        raise ValueError("independent OPQ8 source codes differ")
    queries = _read_geometric_queries(
        root / "queries.parquet", FROZEN_INPUTS.queries, FROZEN_INPUTS.layout.dimensions,
    )
    plans_body = (root / "plans.json").read_bytes()
    plans = json.loads(plans_body)
    if (
        plans_body != _canonical(plans)
        or plans["seal_sha256"] != hashlib.sha256(seal_body).hexdigest()
        or len(plans["samples"]) != len(queries) == 1000
    ):
        raise ValueError("independent OPQ8 plans differ")
    counts_tuple = tuple(int(i) for i in counts)
    for ordinal, query in enumerate(queries):
        ranked = _independent_rank(
            query, model.mean, model.rotation, model.books, codes, counts_tuple,
        )
        chosen, total = _independent_plan(ranked, prior["group_ranges"])
        if plans["samples"][ordinal] != {
            "query_ordinal": ordinal,
            "ranked_groups": ranked,
            "selected_groups": chosen,
            "code_gets": len(chosen),
            "code_bytes": total,
        }:
            raise ValueError("independent OPQ8 query plan differs")
    _authenticate(root / "historical-evidence.json", HISTORICAL_EVIDENCE)
    historical = json.loads((root / "historical-evidence.json").read_bytes())
    if hashlib.sha256(_canonical(historical["samples"])).hexdigest() != HISTORICAL_SAMPLES_SHA256:
        raise ValueError("independent OPQ8 historical samples differ")
    _, truth = _read_queries_truth(root, FROZEN_INPUTS)
    owners = {row.stable_id: row.page_ordinal // 4 for row in membership}
    rows = []
    for ordinal, plan in enumerate(plans["samples"]):
        chosen = set(plan["selected_groups"])
        row = truth[ordinal]
        if len(row) != 100 or len(set(row)) != 100 or any(i not in owners for i in row):
            raise ValueError("independent OPQ8 GT100 differs")
        rows.append({
            **plan,
            "grouped_hits_at_10": sum(owners[i] in chosen for i in row[:10]),
            "grouped_hits_at_100": sum(owners[i] in chosen for i in row),
        })
    evidence_body = (root / "evidence.json").read_bytes()
    result_body = (root / "result.json").read_bytes()
    evidence = json.loads(evidence_body)
    result = json.loads(result_body)
    if (
        evidence_body != _canonical(evidence)
        or result_body != _canonical(result)
        or evidence["samples"] != rows
    ):
        raise ValueError("independent OPQ8 evidence differs")
    hits100 = sorted(row["grouped_hits_at_100"] for row in rows)
    metrics = {
        "query_count": 1000,
        "grouped_gt100_hits": sum(hits100),
        "grouped_p05_gt100_hits": hits100[49],
        "grouped_gt10_hits": sum(row["grouped_hits_at_10"] for row in rows),
        "grouped_sub90_queries": sum(i < 90 for i in hits100),
        "maximum_code_gets": max(row["code_gets"] for row in rows),
        "maximum_code_bytes": max(row["code_bytes"] for row in rows),
    }
    decision = (
        "opq8-group-containment-advance"
        if metrics["grouped_gt100_hits"] >= 98_418
        and metrics["grouped_p05_gt100_hits"] >= 92
        and metrics["grouped_gt10_hits"] >= 9_951
        and metrics["grouped_sub90_queries"] < 36
        and metrics["maximum_code_gets"] <= 32
        and metrics["maximum_code_bytes"] <= 16_777_216
        else "opq8-group-containment-killed"
    )
    if (
        evidence["metrics"] != metrics
        or evidence["decision"] != decision
        or result["metrics"] != metrics
        or result["decision"] != decision
        or result["claim_eligible"] is not False
        or result["source_seal_sha256"] != hashlib.sha256(seal_body).hexdigest()
        or result["plans_sha256"] != hashlib.sha256(plans_body).hexdigest()
        or result["evidence_sha256"] != hashlib.sha256(evidence_body).hexdigest()
    ):
        raise ValueError("independent OPQ8 result differs")
    validation = {
        "schema": "borsuk-hundred-thousand-opq8-validation-v1",
        "decision": decision,
        "metrics": metrics,
        "model_sha256": seal["model_sha256"],
        "codes_sha256": seal["codes_sha256"],
        "plans_sha256": result["plans_sha256"],
        "evidence_sha256": result["evidence_sha256"],
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "validation.json").write_bytes(_canonical(validation))
    return validation
