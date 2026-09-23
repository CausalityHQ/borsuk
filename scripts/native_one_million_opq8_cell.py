#!/usr/bin/env python3
"""Phase-separated original-layout 1M OPQ8 containment falsifier."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_geometric_layout_screen import ArtifactIdentity
from scripts.native_hundred_thousand_opq8_paired_cell import OPQ_SOURCE
from scripts.native_hundred_thousand_opq8_router import (
    rank_opq8_row_groups,
    row_adc_scores,
)
from scripts.native_one_million_group_selector import (
    DIMENSIONS,
    Group,
    _page_map,
    _source_schema,
)
from scripts.native_one_million_opq8 import build_1m_opq8, read_1m_opq8
from scripts.native_one_million_page_selector import read_page_selector
from scripts.native_one_million_page_selector_evaluation import rank_page_groups
from scripts.native_one_million_range_selector_evaluation import plan_group_ranges
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.v97_row_width_screen import _authenticate_object, _canonical_json_bytes

SCHEMA = "borsuk-one-million-opq8-containment-v1"
MODEL_IDENTITY = OPQ_SOURCE["model"]
CONTROL_PREFIX = (
    "s3://borsuk-bench-453182569524-euc1/research/native-one-million-pq96-selector/"
    "1a1d2e7380c4d6635a2b370784ea009aa3af0851/runs/relaion-1m-dev1000-a0001/artifacts/"
)
CONTROL_SOURCE = {
    "centroids": ArtifactIdentity("centroids", CONTROL_PREFIX + "centroids.bin", "757fbe7c6b2112ea5904a5bba0e26fb4ac929cc39ca1fc470b12dee35b61a704", 11_179_008),
    "membership": ArtifactIdentity("membership", CONTROL_PREFIX + "membership.bin", "55e36613b293ffc08c11b9da8c2f2293cf89e43310551f316f2c5b57a84e0d13", 12_000_000),
    "seal": ArtifactIdentity("seal", CONTROL_PREFIX + "seal.json", "ad2c1e079609618ab7428e043364cf4685a1d9cbc2162f7695c7200206f8de41", 120_815),
}
HISTORICAL_EVIDENCE = ArtifactIdentity(
    "evidence", CONTROL_PREFIX + "evidence.json",
    "2ece968e85d77c46b2cf5dc7b058e0570b198d5f2d5ece5aa255ec17e07f1468", 1_303_923,
)


def _authenticate(path: Path, identity: ArtifactIdentity) -> None:
    body = path.read_bytes()
    if len(body) != identity.encoded_bytes or hashlib.sha256(body).hexdigest() != identity.sha256:
        raise ValueError(f"{identity.role} identity differs")


def _canonical(value: object) -> bytes:
    return _canonical_json_bytes(value)


def _read_queries(path: Path, identity: object, query_count: int) -> np.ndarray:
    _authenticate_object("queries", path, identity)
    schema = pa.schema([
        pa.field("query_ordinal", pa.uint32(), nullable=False),
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("embedding", pa.list_(pa.field("item", pa.float32(), nullable=False), DIMENSIONS), nullable=False),
    ])
    table = pq.read_table(path)
    if table.schema != schema or table.num_rows != query_count:
        raise ValueError("OPQ8 queries differ")
    ordinals = table.column("query_ordinal").combine_chunks().to_numpy()
    values = table.column("embedding").combine_chunks()
    queries = np.asarray(values.values.to_numpy(), dtype=np.float32).reshape(-1, DIMENSIONS)
    if values.null_count or not np.array_equal(ordinals, np.arange(query_count, dtype=np.uint32)) or not np.isfinite(queries).all():
        raise ValueError("OPQ8 queries differ")
    return queries


def _projected(groups: Sequence[Group]) -> tuple[Group, ...]:
    return tuple(dataclasses.replace(
        group, code_bytes=4 + 4 * (group.end_page - group.first_page) + 96 * group.row_count,
    ) for group in groups)


def _plan(groups: Sequence[Group], ranked: Sequence[int]) -> dict[str, object]:
    selected, intervals, gets, used = plan_group_ranges(groups, ranked)
    return {
        "ranked_groups": list(ranked), "selected_groups": list(selected),
        "intervals": [list(item) for item in intervals],
        "projected_code_gets": gets, "projected_code_bytes": used,
    }


def _control(root: Path, artifact: object) -> object:
    for role, filename in (("centroids", "centroids.bin"), ("membership", "membership.bin"), ("seal", "seal.json")):
        _authenticate(root / "control" / filename, CONTROL_SOURCE[role])
    control = read_page_selector(root / "control", SOURCE_IDENTITIES)
    if (
        control.groups != artifact.groups
        or not np.array_equal(control.membership_ids, artifact.membership_ids)
        or not np.array_equal(control.membership_groups, artifact.membership_groups)
    ):
        raise ValueError("OPQ8 control physical layout differs")
    return control


def _source_distance_group_scores(
    root: Path, queries: np.ndarray, groups: Sequence[Group],
    id_to_group: Mapping[int, int], *, batch_rows: int = 4096,
) -> tuple[np.ndarray, np.ndarray]:
    _authenticate_object("source", root / "source.parquet", SOURCE_IDENTITIES["source"])
    parquet = pq.ParquetFile(root / "source.parquet")
    if parquet.schema_arrow != _source_schema() or not 1 <= batch_rows <= 4096:
        raise ValueError("OPQ8 source-distance schema differs")
    best = np.full((len(queries), len(groups), 4), np.inf, dtype=np.float64)
    qnorm = np.sum(queries.astype(np.float64) ** 2, axis=1, dtype=np.float64)
    seen: set[int] = set()
    for batch in parquet.iter_batches(batch_size=batch_rows):
        table = pa.Table.from_batches([batch])
        ids = table.column("feature_row_id").combine_chunks().to_numpy()
        values = table.column("embedding").combine_chunks()
        vectors = np.asarray(values.values.to_numpy(), dtype=np.float32).reshape(-1, DIMENSIONS)
        if values.null_count or not np.isfinite(vectors).all():
            raise ValueError("OPQ8 source-distance rows differ")
        owner = np.empty(len(ids), dtype=np.int32)
        for index, row_id in enumerate(ids):
            stable = int(row_id)
            if stable not in id_to_group or stable in seen:
                raise ValueError("OPQ8 source-distance rows differ")
            seen.add(stable)
            owner[index] = id_to_group[stable]
        squared = qnorm[:, None] + np.sum(vectors.astype(np.float64) ** 2, axis=1)[None, :]
        squared -= 2 * (queries @ vectors.T).astype(np.float64)
        np.maximum(squared, 0, out=squared)
        for group in np.unique(owner):
            index = int(group)
            merged = np.concatenate((best[:, index, :], squared[:, owner == group]), axis=1)
            best[:, index, :] = np.partition(merged, kth=3, axis=1)[:, :4]
    if len(seen) != len(id_to_group):
        raise ValueError("OPQ8 source-distance rows differ")
    means = np.empty((len(queries), len(groups)), dtype=np.float64)
    minima = np.min(best, axis=2)
    for group, item in enumerate(groups):
        take = min(4, item.row_count)
        ordered = np.sort(best[:, group, :], axis=1)[:, :take]
        if not np.isfinite(ordered).all():
            raise ValueError("OPQ8 source-distance group differs")
        means[:, group] = np.mean(ordered, axis=1, dtype=np.float64)
    return means, minima


def run_construct(root: Path, *, expected_rows: int = 1_000_000, expected_groups: int = 910) -> None:
    artifact = build_1m_opq8(root, root, SOURCE_IDENTITIES, MODEL_IDENTITY)
    if len(artifact.codes) != expected_rows or len(artifact.groups) != expected_groups:
        raise ValueError("OPQ8 frozen source population differs")


def run_plan(
    root: Path, out: Path, *, query_count: int = 1000,
    expected_rows: int = 1_000_000, expected_groups: int = 910,
) -> dict[str, object]:
    if (root / "truth.parquet").exists() or (root / "historical-evidence.json").exists():
        raise ValueError("OPQ8 query-only plan boundary differs")
    artifact = read_1m_opq8(root, SOURCE_IDENTITIES, MODEL_IDENTITY)
    if len(artifact.codes) != expected_rows or len(artifact.groups) != expected_groups:
        raise ValueError("OPQ8 frozen source population differs")
    queries = _read_queries(root / "queries.parquet", DEVELOPMENT_IDENTITIES["queries"], query_count)
    control = _control(root, artifact)
    groups = _projected(artifact.groups)
    group_counts = tuple(group.row_count for group in artifact.groups)
    ties = tuple((group.role, group.ordinal) for group in artifact.groups)
    _, id_to_group, _ = _page_map(root, SOURCE_IDENTITIES)
    exact_mean, exact_min = _source_distance_group_scores(root, queries, artifact.groups, id_to_group)
    samples = []
    for ordinal, query in enumerate(queries):
        scores = row_adc_scores(query, artifact.model, artifact.codes)
        ranked = rank_opq8_row_groups(scores, group_counts, ties)
        diagnostic = tuple(sorted(
            range(len(groups)), key=lambda index: (
                float(exact_mean[ordinal, index]), float(exact_min[ordinal, index]),
                ties[index][0], ties[index][1],
            ),
        ))
        samples.append({
            "query_ordinal": ordinal,
            "candidate": _plan(groups, ranked),
            "control": _plan(groups, rank_page_groups(query, control)),
            "source_distance": _plan(groups, diagnostic),
        })
    plans = {
        "schema": SCHEMA + "-plans", "source_seal_sha256": hashlib.sha256((root / "seal.json").read_bytes()).hexdigest(),
        "control_seal_sha256": hashlib.sha256((root / "control/seal.json").read_bytes()).hexdigest(),
        "query_identity": dataclasses.asdict(DEVELOPMENT_IDENTITIES["queries"]),
        "samples": samples,
    }
    out.mkdir(parents=True, exist_ok=True)
    plans_body = _canonical(plans)
    (out / "plans.json").write_bytes(plans_body)
    (out / "plan-seal.json").write_bytes(_canonical({
        "schema": SCHEMA + "-plan-seal",
        "plans_sha256": hashlib.sha256(plans_body).hexdigest(),
        "source_seal_sha256": plans["source_seal_sha256"],
        "control_seal_sha256": plans["control_seal_sha256"],
        "query_identity": plans["query_identity"],
    }))
    return plans


def _arm_hits(plan: dict[str, object], truth_groups: np.ndarray) -> dict[str, object]:
    chosen = set(plan["selected_groups"])
    mask = "".join("1" if int(group) in chosen else "0" for group in truth_groups)
    return {"hits_at_10": mask[:10].count("1"), "hits_at_100": mask.count("1"), "hit_mask": mask}


def _metrics(samples: Sequence[dict[str, object]]) -> dict[str, int]:
    count = len(samples)
    metrics: dict[str, int] = {"query_count": count}
    for arm in ("candidate", "control", "source_distance"):
        plans = [sample[arm]["plan"] for sample in samples]
        hits = [sample[arm]["hits_at_100"] for sample in samples]
        metrics[arm + "_gt100_hits"] = sum(hits)
        metrics[arm + "_gt10_hits"] = sum(sample[arm]["hits_at_10"] for sample in samples)
        metrics[arm + "_p05_gt100_hits"] = sorted(hits)[math.ceil(.05 * count) - 1]
        metrics[arm + "_sub90_queries"] = sum(value < 90 for value in hits)
        metrics[arm + "_maximum_gets"] = max(plan["projected_code_gets"] for plan in plans)
        metrics[arm + "_maximum_bytes"] = max(plan["projected_code_bytes"] for plan in plans)
        metrics[arm + "_maximum_groups"] = max(len(plan["selected_groups"]) for plan in plans)
        metrics[arm + "_total_gets"] = sum(plan["projected_code_gets"] for plan in plans)
        metrics[arm + "_total_bytes"] = sum(plan["projected_code_bytes"] for plan in plans)
        metrics[arm + "_total_groups"] = sum(len(plan["selected_groups"]) for plan in plans)
    metrics["candidate_more_bytes_queries"] = sum(
        sample["candidate"]["plan"]["projected_code_bytes"] > sample["control"]["plan"]["projected_code_bytes"]
        for sample in samples
    )
    metrics["candidate_more_groups_queries"] = sum(
        len(sample["candidate"]["plan"]["selected_groups"]) > len(sample["control"]["plan"]["selected_groups"])
        for sample in samples
    )
    return metrics


def _passes_fixed_gate(metrics: Mapping[str, int], arm: str) -> bool:
    prefix = arm + "_"
    return (
        metrics[prefix + "gt100_hits"] >= 98_151
        and metrics[prefix + "p05_gt100_hits"] >= 90
        and metrics[prefix + "gt10_hits"] >= 9_928
        and metrics[prefix + "sub90_queries"] <= 49
        and metrics[prefix + "maximum_gets"] <= 32
        and metrics[prefix + "maximum_bytes"] <= 16_777_216
    )


def run_evaluate(
    root: Path, planning: Path, out: Path, *, query_count: int = 1000,
    expected_rows: int = 1_000_000, expected_groups: int = 910,
) -> dict[str, object]:
    artifact = read_1m_opq8(root, SOURCE_IDENTITIES, MODEL_IDENTITY)
    if len(artifact.codes) != expected_rows or len(artifact.groups) != expected_groups:
        raise ValueError("OPQ8 frozen source population differs")
    plans_body = (planning / "plans.json").read_bytes()
    plans = json.loads(plans_body)
    plan_seal_body = (planning / "plan-seal.json").read_bytes()
    plan_seal = json.loads(plan_seal_body)
    if (
        plans_body != _canonical(plans)
        or plans.get("schema") != SCHEMA + "-plans"
        or plans.get("source_seal_sha256") != hashlib.sha256((root / "seal.json").read_bytes()).hexdigest()
        or plans.get("control_seal_sha256") != hashlib.sha256((root / "control/seal.json").read_bytes()).hexdigest()
        or plans.get("query_identity") != dataclasses.asdict(DEVELOPMENT_IDENTITIES["queries"])
        or len(plans.get("samples", [])) != query_count
        or plan_seal_body != _canonical(plan_seal)
        or plan_seal != {
            "schema": SCHEMA + "-plan-seal",
            "plans_sha256": hashlib.sha256(plans_body).hexdigest(),
            "source_seal_sha256": plans["source_seal_sha256"],
            "control_seal_sha256": plans["control_seal_sha256"],
            "query_identity": plans["query_identity"],
        }
    ):
        raise ValueError("OPQ8 sealed plans differ")
    _authenticate(root / "historical-evidence.json", HISTORICAL_EVIDENCE)
    history_body = (root / "historical-evidence.json").read_bytes()
    historical = json.loads(history_body)
    if history_body != _canonical(historical) or len(historical.get("samples", [])) != query_count:
        raise ValueError("OPQ8 historical evidence differs")
    _, truth = _query_truth(
        root / "queries.parquet", root / "truth.parquet", DEVELOPMENT_IDENTITIES,
        query_count=query_count,
    )
    positions = np.searchsorted(artifact.membership_ids, truth)
    if np.any(positions >= len(artifact.membership_ids)) or not np.array_equal(artifact.membership_ids[positions], truth):
        raise ValueError("OPQ8 truth source IDs differ")
    truth_groups = artifact.membership_groups[positions]
    projected = _projected(artifact.groups)
    samples = []
    for ordinal, item in enumerate(plans["samples"]):
        if item["query_ordinal"] != ordinal:
            raise ValueError("OPQ8 plan order differs")
        arm_data = {}
        for arm in ("candidate", "control", "source_distance"):
            plan = item[arm]
            if (
                len(plan["ranked_groups"]) != len(artifact.groups)
                or set(plan["ranked_groups"]) != set(range(len(artifact.groups)))
                or _plan(projected, plan["ranked_groups"]) != plan
            ):
                raise ValueError("OPQ8 group plan differs")
            arm_data[arm] = {"plan": plan, **_arm_hits(plan, truth_groups[ordinal])}
        old = historical["samples"][ordinal]
        control = arm_data["control"]
        if (
            old["query_ordinal"] != ordinal
            or any(old[field] != control["plan"][field] for field in (
                "selected_groups", "intervals", "projected_code_gets", "projected_code_bytes",
            ))
            or old["hits_at_10"] != control["hits_at_10"]
            or old["hits_at_100"] != control["hits_at_100"]
        ):
            raise ValueError("OPQ8 historical control replay differs")
        samples.append({"query_ordinal": ordinal, **arm_data})
    metrics = _metrics(samples)
    advance = _passes_fixed_gate(metrics, "candidate")
    source_distance_passes = _passes_fixed_gate(metrics, "source_distance")
    material_expansion = (
        100 * metrics["candidate_total_bytes"] > 101 * metrics["control_total_bytes"]
        or 100 * metrics["candidate_total_groups"] > 101 * metrics["control_total_groups"]
    )
    decision = "opq8-one-million-containment-advance" if advance else "opq8-one-million-containment-killed"
    evidence = {"schema": SCHEMA + "-evidence", "samples": samples, "metrics": metrics}
    out.mkdir(parents=True, exist_ok=True)
    evidence_body = _canonical(evidence)
    (out / "evidence.json").write_bytes(evidence_body)
    result = {
        "schema": SCHEMA + "-result", "claim_eligible": False, "decision": decision,
        "metrics": metrics, "source_seal_sha256": plans["source_seal_sha256"],
        "source_distance_passes": source_distance_passes,
        "material_plan_expansion": material_expansion,
        "plan_sha256": hashlib.sha256(plans_body).hexdigest(),
        "historical_sha256": hashlib.sha256(history_body).hexdigest(),
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
    }
    (out / "result.json").write_bytes(_canonical(result))
    return result


def run_validate(
    root: Path, planning: Path, evaluation: Path, out: Path, *, query_count: int = 1000,
    expected_rows: int = 1_000_000, expected_groups: int = 910,
) -> dict[str, object]:
    """Rebuild source codes and both query phases in an isolated directory."""
    for filename in ("codes.bin", "membership.bin", "seal.json", "model.bin"):
        if not (root / filename).is_file():
            raise ValueError("OPQ8 validation source missing")
    with tempfile.TemporaryDirectory(prefix="opq8-validate-", dir=root) as temporary:
        stage = Path(temporary)
        for filename in (
            "source.parquet", "generation.json", "base.arrow", "delta.arrow",
            "router.arrow", "model.bin",
        ):
            (stage / filename).symlink_to((root / filename).resolve())
        run_construct(stage, expected_rows=expected_rows, expected_groups=expected_groups)
        for filename in ("codes.bin", "membership.bin", "seal.json"):
            if (stage / filename).read_bytes() != (root / filename).read_bytes():
                raise ValueError(f"OPQ8 validation {filename} differs")
        (stage / "control").symlink_to((root / "control").resolve(), target_is_directory=True)
        (stage / "queries.parquet").symlink_to((root / "queries.parquet").resolve())
        run_plan(stage, stage / "planning", query_count=query_count,
                 expected_rows=expected_rows, expected_groups=expected_groups)
        for filename in ("plans.json", "plan-seal.json"):
            if (stage / "planning" / filename).read_bytes() != (planning / filename).read_bytes():
                raise ValueError(f"OPQ8 validation {filename} differs")
        for filename in ("truth.parquet", "historical-evidence.json"):
            (stage / filename).symlink_to((root / filename).resolve())
        result = run_evaluate(stage, stage / "planning", stage / "evaluation",
                              query_count=query_count, expected_rows=expected_rows,
                              expected_groups=expected_groups)
        for filename in ("evidence.json", "result.json"):
            if (stage / "evaluation" / filename).read_bytes() != (evaluation / filename).read_bytes():
                raise ValueError(f"OPQ8 validation {filename} differs")
    validation = {
        "schema": SCHEMA + "-validation", "decision": result["decision"],
        "metrics": result["metrics"],
        "codes_sha256": hashlib.sha256((root / "codes.bin").read_bytes()).hexdigest(),
        "plans_sha256": hashlib.sha256((planning / "plans.json").read_bytes()).hexdigest(),
        "evidence_sha256": hashlib.sha256((evaluation / "evidence.json").read_bytes()).hexdigest(),
        "result_sha256": hashlib.sha256((evaluation / "result.json").read_bytes()).hexdigest(),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "validation.json").write_bytes(_canonical(validation))
    return validation


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("construct", "plan", "evaluate", "validate"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    if args.phase == "construct":
        run_construct(args.root)
    elif args.phase == "plan":
        if args.out is None:
            raise ValueError("OPQ8 plan output required")
        run_plan(args.root, args.out)
    elif args.phase == "evaluate":
        if args.out is None:
            raise ValueError("OPQ8 evaluate output required")
        run_evaluate(args.root, args.root, args.out)
    else:
        if args.out is None:
            raise ValueError("OPQ8 validate output required")
        run_validate(args.root, args.root, args.root, args.out)


if __name__ == "__main__":
    main()
