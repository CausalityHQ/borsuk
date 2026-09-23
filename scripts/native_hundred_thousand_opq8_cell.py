#!/usr/bin/env python3
"""Phase-separated 100k OPQ8 group-containment falsifier."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import ArtifactIdentity
from scripts.native_hundred_thousand_opq8_router import (
    CENTROIDS,
    LLOYD_STEPS,
    OUTER_STEPS,
    SCHEMA,
    SEED,
    SUBSPACES,
    TRAINING_ROWS,
    encode_opq8,
    rank_opq8_groups,
    read_opq8_model,
    row_adc_scores,
    train_opq8,
    write_opq8_model,
)
from scripts.native_page_microcluster_cell import (
    FROZEN_INPUTS,
    _authenticate,
    _read_inputs,
    _read_queries_truth,
)
from scripts.native_row_score_code_artifacts import (
    _physical_order,
    _physical_order_sha256,
)
from scripts.validate_native_geometric_layout_result import _read_geometric_queries

PRIOR_CODE_SEAL = ArtifactIdentity(
    "code-seal",
    "s3://borsuk-bench-453182569524-euc1/research/native-rotated-two-bit/"
    "80ddf40533aefd3c24b7d3ea4539887aa94f91bb/"
    "runs/relaion-100k-dev1000-a0001/artifacts/seal.json",
    "ee0d87790e8e65970340126dfea62f4eb625910df974a992ee5af58411f8067e",
    5283,
)
HISTORICAL_EVIDENCE = ArtifactIdentity(
    "rotated-two-bit-evidence",
    "s3://borsuk-bench-453182569524-euc1/research/native-rotated-two-bit/"
    "80ddf40533aefd3c24b7d3ea4539887aa94f91bb/"
    "runs/relaion-100k-dev1000-a0001/artifacts/evidence.json",
    "219f7cee65e9930ed06bd7297b681962e9eaaf9d4b24dc7a36c5fff24a81bbdf",
    4_587_410,
)
HISTORICAL_SAMPLES_SHA256 = "b1d5c637fc88a8edaf371ab314c28708d70d3cb881d246d995599a17a0b5d20a"


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def _read_prior_code_seal(root: Path) -> dict[str, object]:
    _authenticate(root / "prior-code-seal.json", PRIOR_CODE_SEAL)
    body = (root / "prior-code-seal.json").read_bytes()
    seal = json.loads(body)
    if (
        body != _canonical(seal)
        or set(seal) != {
            "dimensions", "group_pages", "group_ranges", "groups_sha256",
            "layout_seed", "mean_sha256", "membership_sha256",
            "page_row_counts", "physical_order_sha256", "rotation_seed",
            "rotation_signs_sha256", "row_bytes", "rows", "schema",
            "source_sha256", "tree_sha256",
        }
        or seal["schema"] != "borsuk-rotated-two-bit-group-codes-v1"
        or seal["dimensions"] != FROZEN_INPUTS.layout.dimensions
        or seal["source_sha256"] != FROZEN_INPUTS.layout.source.sha256
        or seal["membership_sha256"] != FROZEN_INPUTS.membership.sha256
        or seal["layout_seed"] != FROZEN_INPUTS.layout.seed
        or seal["rows"] != 100_000
        or seal["group_pages"] != 4
        or seal["row_bytes"] != 200
        or len(seal["page_row_counts"]) != 166
        or len(seal["group_ranges"]) != 42
    ):
        raise ValueError("OPQ8 prior two-bit code seal differs")
    return seal


def run_construct(root: Path) -> None:
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("OPQ8 source-only boundary differs")
    ids, vectors, membership = _read_inputs(root, FROZEN_INPUTS)
    prior = _read_prior_code_seal(root)
    ordinals, counts, _ = _physical_order(ids, membership, FROZEN_INPUTS.layout.seed)
    if (
        list(counts) != prior["page_row_counts"]
        or _physical_order_sha256(ordinals) != prior["physical_order_sha256"]
    ):
        raise ValueError("OPQ8 physical order differs from prior two-bit plane")
    model, selected = train_opq8(vectors)
    if hashlib.sha256(model.mean.astype("<f4").tobytes()).hexdigest() != prior["mean_sha256"]:
        raise ValueError("OPQ8 source mean differs from prior two-bit plane")
    codes = encode_opq8(vectors, np.asarray(ordinals, dtype=np.int64), model)
    write_opq8_model(root / "model.bin", model)
    (root / "codes.bin").write_bytes(codes.tobytes(order="C"))
    model_body = (root / "model.bin").read_bytes()
    code_body = (root / "codes.bin").read_bytes()
    seal = {
        "schema": SCHEMA,
        "source": dataclasses.asdict(FROZEN_INPUTS.layout.source),
        "membership": dataclasses.asdict(FROZEN_INPUTS.membership),
        "prior_code_seal": dataclasses.asdict(PRIOR_CODE_SEAL),
        "physical_order_sha256": prior["physical_order_sha256"],
        "group_ranges": prior["group_ranges"],
        "page_row_counts": prior["page_row_counts"],
        "model_sha256": hashlib.sha256(model_body).hexdigest(),
        "model_bytes": len(model_body),
        "codes_sha256": hashlib.sha256(code_body).hexdigest(),
        "codes_bytes": len(code_body),
        "training_ordinals_sha256": hashlib.sha256(selected.astype("<u8").tobytes()).hexdigest(),
        "training_rows": TRAINING_ROWS,
        "seed": SEED,
        "subspaces": SUBSPACES,
        "centroids": CENTROIDS,
        "outer_steps": OUTER_STEPS,
        "lloyd_steps": LLOYD_STEPS,
    }
    (root / "seal.json").write_bytes(_canonical(seal))


def _read_sealed(root: Path) -> tuple[object, np.ndarray, dict[str, object]]:
    body = (root / "seal.json").read_bytes()
    seal = json.loads(body)
    prior = _read_prior_code_seal(root)
    if (
        body != _canonical(seal)
        or seal.get("schema") != SCHEMA
        or seal.get("source") != dataclasses.asdict(FROZEN_INPUTS.layout.source)
        or seal.get("membership") != dataclasses.asdict(FROZEN_INPUTS.membership)
        or seal.get("prior_code_seal") != dataclasses.asdict(PRIOR_CODE_SEAL)
        or seal.get("physical_order_sha256") != prior["physical_order_sha256"]
        or seal.get("group_ranges") != prior["group_ranges"]
        or seal.get("page_row_counts") != prior["page_row_counts"]
        or seal.get("training_rows") != TRAINING_ROWS
        or seal.get("seed") != SEED
        or seal.get("subspaces") != SUBSPACES
        or seal.get("centroids") != CENTROIDS
        or seal.get("outer_steps") != OUTER_STEPS
        or seal.get("lloyd_steps") != LLOYD_STEPS
    ):
        raise ValueError("OPQ8 source seal authority differs")
    model_body = (root / "model.bin").read_bytes()
    code_body = (root / "codes.bin").read_bytes()
    if (
        len(model_body) != seal["model_bytes"]
        or hashlib.sha256(model_body).hexdigest() != seal["model_sha256"]
        or len(code_body) != 800_000
        or seal["codes_bytes"] != 800_000
        or hashlib.sha256(code_body).hexdigest() != seal["codes_sha256"]
    ):
        raise ValueError("OPQ8 model/code identity differs")
    model = read_opq8_model(root / "model.bin")
    codes = np.frombuffer(code_body, dtype=np.uint8).reshape(100_000, SUBSPACES)
    return model, codes, seal


def _plan_groups(ranked: Sequence[int], ranges: Sequence[Sequence[object]]) -> tuple[list[int], int]:
    if len(ranked) != len(ranges) or set(ranked) != set(range(len(ranges))):
        raise ValueError("OPQ8 group rank differs")
    selected: list[int] = []
    used = 0
    for group in ranked:
        length = ranges[group][3]
        if type(length) is not int or length <= 0:
            raise ValueError("OPQ8 group length differs")
        if len(selected) == 32:
            break
        if used + length <= 16_777_216:
            selected.append(group)
            used += length
    return selected, used


def run_plan(root: Path, out: Path) -> None:
    if (root / "truth.parquet").exists() or (root / "historical-evidence.json").exists():
        raise ValueError("OPQ8 query-only plan boundary differs")
    model, codes, seal = _read_sealed(root)
    queries = _read_geometric_queries(
        root / "queries.parquet", FROZEN_INPUTS.queries, FROZEN_INPUTS.layout.dimensions,
    )
    if len(queries) != 1000:
        raise ValueError("OPQ8 frozen query count differs")
    samples = []
    counts = tuple(int(i) for i in seal["page_row_counts"])
    ranges = seal["group_ranges"]
    for ordinal, query in enumerate(queries):
        scores = row_adc_scores(query, model, codes)
        ranked = rank_opq8_groups(scores, counts)
        selected, used = _plan_groups(ranked, ranges)
        samples.append({
            "query_ordinal": ordinal,
            "ranked_groups": list(ranked),
            "selected_groups": selected,
            "code_gets": len(selected),
            "code_bytes": used,
        })
    out.mkdir(parents=True, exist_ok=True)
    (out / "plans.json").write_bytes(_canonical({
        "schema": "borsuk-hundred-thousand-opq8-query-plans-v1",
        "seal_sha256": hashlib.sha256((root / "seal.json").read_bytes()).hexdigest(),
        "queries": dataclasses.asdict(FROZEN_INPUTS.queries),
        "samples": samples,
    }))


def run_evaluate(root: Path, out: Path) -> None:
    _, _, seal = _read_sealed(root)
    plans_body = (root / "plans.json").read_bytes()
    plans = json.loads(plans_body)
    if (
        plans_body != _canonical(plans)
        or plans.get("schema") != "borsuk-hundred-thousand-opq8-query-plans-v1"
        or plans.get("seal_sha256") != hashlib.sha256((root / "seal.json").read_bytes()).hexdigest()
        or plans.get("queries") != dataclasses.asdict(FROZEN_INPUTS.queries)
        or len(plans.get("samples", [])) != 1000
    ):
        raise ValueError("OPQ8 frozen plans differ")
    _authenticate(root / "historical-evidence.json", HISTORICAL_EVIDENCE)
    historical = json.loads((root / "historical-evidence.json").read_bytes())
    if hashlib.sha256(_canonical(historical["samples"])).hexdigest() != HISTORICAL_SAMPLES_SHA256:
        raise ValueError("OPQ8 historical sample list differs")
    ids, _, membership = _read_inputs(root, FROZEN_INPUTS)
    _, truth = _read_queries_truth(root, FROZEN_INPUTS)
    owner = {row.stable_id: row.page_ordinal // 4 for row in membership}
    if len(owner) != len(ids) or len(truth) != 1000:
        raise ValueError("OPQ8 truth owner map differs")
    samples = []
    for ordinal, plan in enumerate(plans["samples"]):
        if plan["query_ordinal"] != ordinal:
            raise ValueError("OPQ8 plan ordinal differs")
        chosen = set(plan["selected_groups"])
        row = truth[ordinal]
        if len(row) != 100 or len(set(row)) != 100 or any(i not in owner for i in row):
            raise ValueError("OPQ8 GT100 row differs")
        samples.append({
            **plan,
            "grouped_hits_at_10": sum(owner[i] in chosen for i in row[:10]),
            "grouped_hits_at_100": sum(owner[i] in chosen for i in row),
        })
    hits100 = sorted(sample["grouped_hits_at_100"] for sample in samples)
    hit_total = sum(hits100)
    hit10_total = sum(sample["grouped_hits_at_10"] for sample in samples)
    below90 = sum(i < 90 for i in hits100)
    metrics = {
        "query_count": 1000,
        "grouped_gt100_hits": hit_total,
        "grouped_p05_gt100_hits": hits100[49],
        "grouped_gt10_hits": hit10_total,
        "grouped_sub90_queries": below90,
        "maximum_code_gets": max(sample["code_gets"] for sample in samples),
        "maximum_code_bytes": max(sample["code_bytes"] for sample in samples),
    }
    decision = (
        "opq8-group-containment-advance"
        if hit_total >= 98_418 and hits100[49] >= 92 and hit10_total >= 9_951
        and below90 < 36 and metrics["maximum_code_gets"] <= 32
        and metrics["maximum_code_bytes"] <= 16_777_216
        else "opq8-group-containment-killed"
    )
    evidence = {
        "schema": "borsuk-hundred-thousand-opq8-containment-evidence-v1",
        "samples": samples,
        "metrics": metrics,
        "decision": decision,
        "historical_primary": {
            "gt100_hits": sum(sample["primary_hits_at_100"] for sample in historical["samples"]),
            "p05_gt100_hits": sorted(sample["primary_hits_at_100"] for sample in historical["samples"])[49],
            "gt10_hits": sum(sample["primary_hits_at_10"] for sample in historical["samples"]),
            "sub90_queries": sum(sample["primary_hits_at_100"] < 90 for sample in historical["samples"]),
        },
    }
    if evidence["historical_primary"] != {
        "gt100_hits": 98_418, "p05_gt100_hits": 91, "gt10_hits": 9_951,
        "sub90_queries": 36,
    }:
        raise ValueError("OPQ8 historical control differs")
    out.mkdir(parents=True, exist_ok=True)
    evidence_body = _canonical(evidence)
    (out / "evidence.json").write_bytes(evidence_body)
    result = {
        "schema": "borsuk-hundred-thousand-opq8-containment-result-v1",
        "claim_eligible": False,
        "decision": decision,
        "metrics": metrics,
        "source_seal_sha256": hashlib.sha256((root / "seal.json").read_bytes()).hexdigest(),
        "plans_sha256": hashlib.sha256(plans_body).hexdigest(),
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "source": dataclasses.asdict(FROZEN_INPUTS.layout.source),
        "membership": dataclasses.asdict(FROZEN_INPUTS.membership),
        "queries": dataclasses.asdict(FROZEN_INPUTS.queries),
        "truth": dataclasses.asdict(FROZEN_INPUTS.truth),
    }
    (out / "result.json").write_bytes(_canonical(result))


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
            raise ValueError("OPQ8 evidence output required")
        run_evaluate(args.root, args.out)
    else:
        if args.out is None:
            raise ValueError("OPQ8 validation output required")
        from scripts.validate_native_hundred_thousand_opq8 import validate_opq8

        validate_opq8(args.root, args.out)


if __name__ == "__main__":
    main()
