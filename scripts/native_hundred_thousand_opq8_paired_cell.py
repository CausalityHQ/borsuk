#!/usr/bin/env python3
"""Paired actual S3 two-bit group reads for the frozen 100k OPQ8 route."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    read_geometric_router_parquet,
    route_geometric_query,
)
from scripts.native_hundred_thousand_opq8_cell import (
    FROZEN_INPUTS,
    HISTORICAL_EVIDENCE,
    HISTORICAL_SAMPLES_SHA256,
    PRIOR_CODE_SEAL,
    _canonical,
    _read_sealed,
)
from scripts.native_page_microcluster_cell import (
    _authenticate,
    _read_inputs,
    _read_queries_truth,
)
from scripts.native_rotated_two_bit_cell import PRIOR_PAGES, PRIOR_TREE, read_cell_seal
from scripts.native_rotated_two_bit_codes import read_two_bit_codes
from scripts.native_rotated_two_bit_evaluation import evaluate_two_bit_query
from scripts.native_rotated_two_bit_evidence import read_two_bit_evidence
from scripts.native_rotated_two_bit_range_broker import BrokerRangeReader

OPQ_PREFIX = (
    "s3://borsuk-bench-453182569524-euc1/research/native-hundred-thousand-opq8-router/"
    "87236c5765186fb8ab977ef86f87a6b1047effb1/runs/relaion-100k-dev1000-a0001"
)
TWO_BIT_PREFIX = (
    "s3://borsuk-bench-453182569524-euc1/research/native-rotated-two-bit/"
    "80ddf40533aefd3c24b7d3ea4539887aa94f91bb/runs/relaion-100k-dev1000-a0001"
)
OPQ_SOURCE = {
    "model": ArtifactIdentity("model", OPQ_PREFIX + "/artifacts/model.bin", "e49a31fd334f4af3de7964ca6452c3f93bc6223939e29718721b3d34207733ff", 3_148_820),
    "codes": ArtifactIdentity("codes", OPQ_PREFIX + "/artifacts/codes.bin", "04c3b5c38928c225f27ca749acbbfef1a6571f9ffe0dd8782fd5b4309464e8cc", 800_000),
    "seal": ArtifactIdentity("seal", OPQ_PREFIX + "/artifacts/seal.json", "e6f1edf3d680dd60e1cb572df70347a8ac8d3c74bfaa6dbcc89dea2cc9c1faf8", 5_993),
    "plans": ArtifactIdentity("plans", OPQ_PREFIX + "/artifacts/plans.json", "b14ed50ad4b552a95de9aa1871e3ea964bb96d76e871e3979e10e7036dc77064", 301_559),
    "evidence": ArtifactIdentity("evidence", OPQ_PREFIX + "/artifacts/evidence.json", "b834a78673b3b5c8dfa30ed528ec8edb00c9e8508da3385b681026096bcc5019", 351_455),
}
TWO_BIT_SOURCE = {
    "mean": ArtifactIdentity("rotated-two-bit-mean", TWO_BIT_PREFIX + "/artifacts/mean.bin", "197a8ead172a5a41e029c8f0c9d39b981150c69fc3b0367316fb89c920eb1beb", 3_072),
    "groups": ArtifactIdentity("rotated-two-bit-groups", TWO_BIT_PREFIX + "/artifacts/groups.bin", "57f122df99a20612c7ed0afa13883709469ce6736287488593b9c411d8bbfa14", 20_000_832),
    "sealed": ArtifactIdentity("sealed", TWO_BIT_PREFIX + "/artifacts/sealed.json", "40239155cb36be4c6a41c292bb7acc31b00726ecc01ba5acef6b72e5c683da4b", 1_228),
}


def verify_recorded_outcomes(
    arm: dict[str, object], truth_ids: Sequence[bytes],
    owners: Mapping[bytes, int], page_bytes: Sequence[int],
) -> None:
    if len(truth_ids) != 100 or len(set(truth_ids)) != 100 or any(item not in owners for item in truth_ids):
        raise ValueError("OPQ8 paired validation truth differs")
    for prefix in ("exact", "primary", "diagnostic"):
        pages = arm[prefix + "_pages"]
        if (
            not pages or len(pages) > 32 or len(set(pages)) != len(pages)
            or any(type(page) is not int or not 0 <= page < len(page_bytes) for page in pages)
            or arm[prefix + "_data_bytes"] != sum(page_bytes[page] for page in pages)
            or arm[prefix + "_data_bytes"] > 16_777_216
            or arm[prefix + "_hits_at_10"] != sum(owners[item] in pages for item in truth_ids[:10])
            or arm[prefix + "_hits_at_100"] != sum(owners[item] in pages for item in truth_ids)
        ):
            raise ValueError(f"OPQ8 paired validation {prefix} outcome differs")
    grouped = arm["grouped_pages"]
    if (
        arm["grouped_hits_at_10"] != sum(owners[item] in grouped for item in truth_ids[:10])
        or arm["grouped_hits_at_100"] != sum(owners[item] in grouped for item in truth_ids)
    ):
        raise ValueError("OPQ8 paired validation group outcome differs")


def authenticate_source(root: Path) -> None:
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("OPQ8 paired source boundary differs")
    ids, vectors, membership = _read_inputs(root, FROZEN_INPUTS)
    _read_sealed(root / "opq")
    for name in ("model", "codes", "seal"):
        _authenticate(root / "opq" / {"model": "model.bin", "codes": "codes.bin", "seal": "seal.json"}[name], OPQ_SOURCE[name])
    for name, identity in TWO_BIT_SOURCE.items():
        _authenticate(root / {"mean": "mean.bin", "groups": "groups.bin", "sealed": "sealed.json"}[name], identity)
    _authenticate(root / "seal.json", PRIOR_CODE_SEAL)
    identities = read_cell_seal(
        root, TWO_BIT_PREFIX, bytes.fromhex(FROZEN_INPUTS.layout.source.sha256),
        bytes.fromhex(FROZEN_INPUTS.membership.sha256), bytes.fromhex(PRIOR_TREE.sha256),
    )
    read_two_bit_codes(
        root, identities, ids, membership,
        bytes.fromhex(FROZEN_INPUTS.layout.source.sha256),
        bytes.fromhex(FROZEN_INPUTS.membership.sha256),
        bytes.fromhex(PRIOR_TREE.sha256), dimensions=768,
        layout_seed=FROZEN_INPUTS.layout.seed, rotation_seed=20260923,
    )
    if vectors.shape != (100_000, 768):
        raise ValueError("OPQ8 paired source shape differs")
    (root / "source-seal.json").write_bytes(_canonical({
        "schema": "borsuk-hundred-thousand-opq8-paired-source-v1",
        "source": dataclasses.asdict(FROZEN_INPUTS.layout.source),
        "membership": dataclasses.asdict(FROZEN_INPUTS.membership),
        "opq": {name: dataclasses.asdict(OPQ_SOURCE[name]) for name in ("model", "codes", "seal")},
        "two_bit": {name: dataclasses.asdict(identity) for name, identity in TWO_BIT_SOURCE.items()},
        "code_seal": dataclasses.asdict(PRIOR_CODE_SEAL),
    }))


def _read_plans(root: Path) -> list[dict[str, object]]:
    path = root / "opq" / "plans.json"
    _authenticate(path, OPQ_SOURCE["plans"])
    body = path.read_bytes()
    value = json.loads(body)
    if (
        body != _canonical(value)
        or value.get("schema") != "borsuk-hundred-thousand-opq8-query-plans-v1"
        or value.get("seal_sha256") != OPQ_SOURCE["seal"].sha256
        or value.get("queries") != dataclasses.asdict(FROZEN_INPUTS.queries)
        or len(value.get("samples", [])) != 1000
    ):
        raise ValueError("OPQ8 paired plan authority differs")
    return value["samples"]


def authenticate_plans(root: Path) -> None:
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("OPQ8 paired plan boundary differs")
    plans = _read_plans(root)
    _authenticate(root / "opq" / "evidence.json", OPQ_SOURCE["evidence"])
    body = (root / "opq" / "evidence.json").read_bytes()
    evidence = json.loads(body)
    if (
        body != _canonical(evidence)
        or evidence.get("schema") != "borsuk-hundred-thousand-opq8-containment-evidence-v1"
        or len(evidence.get("samples", [])) != 1000
    ):
        raise ValueError("OPQ8 paired plan evidence differs")
    for ordinal, (plan, sample) in enumerate(zip(plans, evidence["samples"], strict=True)):
        if (
            plan["query_ordinal"] != ordinal
            or sample["query_ordinal"] != ordinal
            or any(sample[field] != plan[field] for field in ("ranked_groups", "selected_groups", "code_gets", "code_bytes"))
        ):
            raise ValueError(f"OPQ8 paired plan {ordinal} differs")
    (root / "plan-seal.json").write_bytes(_canonical({
        "schema": "borsuk-hundred-thousand-opq8-paired-plans-v1",
        "plans": dataclasses.asdict(OPQ_SOURCE["plans"]),
        "evidence": dataclasses.asdict(OPQ_SOURCE["evidence"]),
        "source_seal_sha256": hashlib.sha256((root / "source-seal.json").read_bytes()).hexdigest(),
    }))


def run_evaluate(root: Path, out: Path) -> None:
    if not (root / "plan-seal.json").exists():
        raise ValueError("OPQ8 paired plan seal missing")
    ids, vectors, membership = _read_inputs(root, FROZEN_INPUTS)
    identities = read_cell_seal(
        root, TWO_BIT_PREFIX, bytes.fromhex(FROZEN_INPUTS.layout.source.sha256),
        bytes.fromhex(FROZEN_INPUTS.membership.sha256), bytes.fromhex(PRIOR_TREE.sha256),
    )
    artifacts = read_two_bit_codes(
        root, identities, ids, membership,
        bytes.fromhex(FROZEN_INPUTS.layout.source.sha256),
        bytes.fromhex(FROZEN_INPUTS.membership.sha256),
        bytes.fromhex(PRIOR_TREE.sha256), dimensions=768,
        layout_seed=FROZEN_INPUTS.layout.seed, rotation_seed=20260923,
    )
    router = read_geometric_router_parquet(
        root / "tree.parquet", root / "pages.parquet", FROZEN_INPUTS.layout,
        membership, PRIOR_TREE, PRIOR_PAGES,
    )
    queries, truth = _read_queries_truth(root, FROZEN_INPUTS)
    plans = _read_plans(root)
    _authenticate(root / "historical-evidence.json", HISTORICAL_EVIDENCE)
    control, _ = read_two_bit_evidence(root / "historical-evidence.json", HISTORICAL_EVIDENCE)
    historical = json.loads((root / "historical-evidence.json").read_bytes())
    if hashlib.sha256(_canonical(historical["samples"])).hexdigest() != HISTORICAL_SAMPLES_SHA256:
        raise ValueError("OPQ8 paired historical samples differ")
    _authenticate(root / "opq" / "evidence.json", OPQ_SOURCE["evidence"])
    opq_evidence = json.loads((root / "opq" / "evidence.json").read_bytes())
    if len(queries) != 1000 or len(truth) != 1000 or len(plans) != 1000 or len(control) != 1000 or len(opq_evidence["samples"]) != 1000:
        raise ValueError("OPQ8 paired cohort differs")
    offsets = np.concatenate(([0], np.cumsum(artifacts.page_row_counts)))
    owners = {
        ids[artifacts.source_ordinals[position]]: page
        for page in range(len(artifacts.page_row_counts))
        for position in range(int(offsets[page]), int(offsets[page + 1]))
    }
    page_bytes = tuple(page.encoded_page_bytes for page in router.pages)
    import os

    socket_path = os.environ.get("BORSUK_RANGE_BROKER_SOCKET")
    if not socket_path:
        raise ValueError("OPQ8 paired range broker required")
    candidate_reader = BrokerRangeReader(Path(socket_path), object_bytes=identities.groups.encoded_bytes)
    control_reader = BrokerRangeReader(Path(socket_path), object_bytes=identities.groups.encoded_bytes)
    rows: list[dict[str, object]] = []
    for ordinal, query in enumerate(queries):
        baseline = control[ordinal]
        old_route = route_geometric_query(router, query, leaf_frontier=128, limits=FROZEN_INPUTS.limits)
        actual_control = evaluate_two_bit_query(
            query_ordinal=ordinal, query=query,
            retained_pages=old_route.retained_leaf_pages, artifacts=artifacts,
            stable_ids=ids, vectors=vectors, truth_ids=truth[ordinal],
            page_byte_sizes=page_bytes, limits=FROZEN_INPUTS.limits,
            read_group_range=control_reader,
            prior_exact_pages=baseline.exact_pages,
            prior_exact_hits_at_10=baseline.exact_hits_at_10,
            prior_exact_hits_at_100=baseline.exact_hits_at_100,
            prior_pq_hits_at_100=baseline.prior_pq_hits_at_100,
            prior_residual_hits_at_100=baseline.prior_residual_hits_at_100,
            owner_by_id=owners,
        )
        if actual_control != baseline:
            raise ValueError(f"OPQ8 paired historical control differs at query {ordinal}")
        fixed = plans[ordinal]
        selected = fixed["selected_groups"]
        if fixed["query_ordinal"] != ordinal or len(selected) != fixed["code_gets"]:
            raise ValueError("OPQ8 paired selected plan differs")
        retained = tuple(
            page for index in selected
            for page in range(artifacts.group_ranges[index][0], artifacts.group_ranges[index][1])
        )
        challenger = evaluate_two_bit_query(
            query_ordinal=ordinal, query=query,
            retained_pages=retained, selected_group_ordinals=selected,
            artifacts=artifacts, stable_ids=ids, vectors=vectors,
            truth_ids=truth[ordinal], page_byte_sizes=page_bytes,
            limits=FROZEN_INPUTS.limits, read_group_range=candidate_reader,
            prior_exact_pages=None,
            prior_pq_hits_at_100=baseline.prior_pq_hits_at_100,
            prior_residual_hits_at_100=baseline.prior_residual_hits_at_100,
            owner_by_id=owners,
        )
        source_sample = opq_evidence["samples"][ordinal]
        if (
            challenger.code_gets != fixed["code_gets"]
            or challenger.code_bytes != fixed["code_bytes"]
            or challenger.grouped_hits_at_100 != source_sample["grouped_hits_at_100"]
            or challenger.grouped_hits_at_10 != source_sample["grouped_hits_at_10"]
            or source_sample["selected_groups"] != selected
        ):
            raise ValueError("OPQ8 paired code plan differs")
        rows.append({
            "query_ordinal": ordinal,
            "control": dataclasses.asdict(actual_control),
            "candidate": dataclasses.asdict(challenger),
            "control_primary_hit_mask": "".join(
                "1" if owners[item] in actual_control.primary_pages else "0" for item in truth[ordinal]
            ),
            "candidate_primary_hit_mask": "".join(
                "1" if owners[item] in challenger.primary_pages else "0" for item in truth[ordinal]
            ),
        })
    hits = sorted(row["candidate"]["primary_hits_at_100"] for row in rows)
    control_sub90 = sum(row["control"]["primary_hits_at_100"] < 90 for row in rows)
    metrics = {
        "query_count": len(rows),
        "primary_gt100_hits": sum(hits),
        "primary_p05_gt100_hits": hits[49],
        "primary_gt10_hits": sum(row["candidate"]["primary_hits_at_10"] for row in rows),
        "primary_sub90_queries": sum(hit < 90 for hit in hits),
        "control_sub90_queries": control_sub90,
        "maximum_code_gets": max(row["candidate"]["code_gets"] for row in rows),
        "maximum_code_bytes": max(row["candidate"]["code_bytes"] for row in rows),
        "maximum_data_pages": max(len(row["candidate"]["primary_pages"]) for row in rows),
        "maximum_data_bytes": max(row["candidate"]["primary_data_bytes"] for row in rows),
        "candidate_actual_gets": candidate_reader.gets,
        "candidate_actual_bytes": candidate_reader.bytes,
        "control_actual_gets": control_reader.gets,
        "control_actual_bytes": control_reader.bytes,
    }
    advance = (
        metrics["primary_gt100_hits"] >= 98_418
        and metrics["primary_p05_gt100_hits"] >= 92
        and metrics["primary_gt10_hits"] >= 9_951
        and metrics["primary_sub90_queries"] < 36
        and metrics["primary_sub90_queries"] < control_sub90
        and metrics["maximum_code_gets"] <= 32
        and metrics["maximum_code_bytes"] <= 16_777_216
        and metrics["maximum_data_pages"] <= 32
        and metrics["maximum_data_bytes"] <= 16_777_216
    )
    evidence = {"schema": "borsuk-hundred-thousand-opq8-paired-evidence-v1", "samples": rows, "metrics": metrics}
    out.mkdir(parents=True, exist_ok=True)
    (out / "evidence.json").write_bytes(_canonical(evidence))
    (out / "result.json").write_bytes(_canonical({
        "schema": "borsuk-hundred-thousand-opq8-paired-result-v1",
        "claim_eligible": False,
        "decision": "opq8-paired-advance" if advance else "opq8-paired-killed",
        "metrics": metrics,
        "evidence_sha256": hashlib.sha256((out / "evidence.json").read_bytes()).hexdigest(),
        "source_plan_sha256": OPQ_SOURCE["plans"].sha256,
        "historical_samples_sha256": HISTORICAL_SAMPLES_SHA256,
    }))


def run_validate(root: Path, out: Path) -> None:
    evidence_body = (root / "evidence.json").read_bytes()
    result_body = (root / "result.json").read_bytes()
    evidence = json.loads(evidence_body)
    result = json.loads(result_body)
    if (
        evidence_body != _canonical(evidence)
        or result_body != _canonical(result)
        or evidence.get("schema") != "borsuk-hundred-thousand-opq8-paired-evidence-v1"
        or result.get("schema") != "borsuk-hundred-thousand-opq8-paired-result-v1"
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or result.get("source_plan_sha256") != OPQ_SOURCE["plans"].sha256
        or result.get("historical_samples_sha256") != HISTORICAL_SAMPLES_SHA256
    ):
        raise ValueError("OPQ8 paired result authority differs")
    _authenticate(root / "historical-evidence.json", HISTORICAL_EVIDENCE)
    _authenticate(root / "opq" / "plans.json", OPQ_SOURCE["plans"])
    _authenticate(root / "opq" / "evidence.json", OPQ_SOURCE["evidence"])
    historical = json.loads((root / "historical-evidence.json").read_bytes())["samples"]
    plans = _read_plans(root)
    source_rows = json.loads((root / "opq" / "evidence.json").read_bytes())["samples"]
    rows = evidence["samples"]
    if len(rows) != 1000 or len(historical) != 1000 or len(source_rows) != 1000:
        raise ValueError("OPQ8 paired validation cohort differs")
    ids, _, membership = _read_inputs(root, FROZEN_INPUTS)
    _, truth = _read_queries_truth(root, FROZEN_INPUTS)
    router = read_geometric_router_parquet(
        root / "tree.parquet", root / "pages.parquet", FROZEN_INPUTS.layout,
        membership, PRIOR_TREE, PRIOR_PAGES,
    )
    owners = {row.stable_id: row.page_ordinal for row in membership}
    page_bytes = tuple(page.encoded_page_bytes for page in router.pages)
    if len(ids) != len(owners):
        raise ValueError("OPQ8 paired validation ownership differs")
    for ordinal, row in enumerate(rows):
        candidate = row["candidate"]
        source = source_rows[ordinal]
        if (
            row["query_ordinal"] != ordinal
            or row["control"] != historical[ordinal]
            or candidate["query_ordinal"] != ordinal
            or candidate["code_gets"] != plans[ordinal]["code_gets"]
            or candidate["code_bytes"] != plans[ordinal]["code_bytes"]
            or candidate["grouped_hits_at_100"] != source["grouped_hits_at_100"]
            or candidate["grouped_hits_at_10"] != source["grouped_hits_at_10"]
            or [group[0] // 4 for group in candidate["group_ranges"]] != plans[ordinal]["selected_groups"]
        ):
            raise ValueError(f"OPQ8 paired validation row {ordinal} differs")
        for arm in (row["control"], candidate):
            verify_recorded_outcomes(arm, truth[ordinal], owners, page_bytes)
        for role in ("control", "candidate"):
            pages = set(row[role]["primary_pages"])
            expected_mask = "".join("1" if owners[item] in pages else "0" for item in truth[ordinal])
            if row[role + "_primary_hit_mask"] != expected_mask:
                raise ValueError(f"OPQ8 paired validation ordered hits differ at {ordinal}")
    hits = sorted(row["candidate"]["primary_hits_at_100"] for row in rows)
    metrics = result["metrics"]
    derived = {
        "query_count": 1000,
        "primary_gt100_hits": sum(hits),
        "primary_p05_gt100_hits": hits[49],
        "primary_gt10_hits": sum(row["candidate"]["primary_hits_at_10"] for row in rows),
        "primary_sub90_queries": sum(hit < 90 for hit in hits),
        "control_sub90_queries": sum(row["control"]["primary_hits_at_100"] < 90 for row in rows),
        "maximum_code_gets": max(row["candidate"]["code_gets"] for row in rows),
        "maximum_code_bytes": max(row["candidate"]["code_bytes"] for row in rows),
        "maximum_data_pages": max(len(row["candidate"]["primary_pages"]) for row in rows),
        "maximum_data_bytes": max(row["candidate"]["primary_data_bytes"] for row in rows),
        "candidate_actual_gets": sum(row["candidate"]["code_gets"] for row in rows),
        "candidate_actual_bytes": sum(row["candidate"]["code_bytes"] for row in rows),
        "control_actual_gets": sum(row["control"]["code_gets"] for row in rows),
        "control_actual_bytes": sum(row["control"]["code_bytes"] for row in rows),
    }
    if metrics != derived or evidence["metrics"] != derived:
        raise ValueError("OPQ8 paired validation metrics differ")
    advance = (
        derived["primary_gt100_hits"] >= 98_418
        and derived["primary_p05_gt100_hits"] >= 92
        and derived["primary_gt10_hits"] >= 9_951
        and derived["primary_sub90_queries"] < min(36, derived["control_sub90_queries"])
        and derived["maximum_code_gets"] <= 32
        and derived["maximum_code_bytes"] <= 16_777_216
        and derived["maximum_data_pages"] <= 32
        and derived["maximum_data_bytes"] <= 16_777_216
    )
    decision = "opq8-paired-advance" if advance else "opq8-paired-killed"
    if result["decision"] != decision or result["claim_eligible"] is not False:
        raise ValueError("OPQ8 paired validation decision differs")
    out.mkdir(parents=True, exist_ok=True)
    (out / "validation.json").write_bytes(_canonical({
        "schema": "borsuk-hundred-thousand-opq8-paired-validation-v1",
        "decision": decision, "metrics": derived,
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "result_sha256": hashlib.sha256(result_body).hexdigest(),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("source", "plans", "evaluate", "validate"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    if args.phase == "source":
        authenticate_source(args.root)
    elif args.phase == "plans":
        authenticate_plans(args.root)
    elif args.phase == "evaluate":
        if args.out is None:
            raise ValueError("OPQ8 paired output missing")
        run_evaluate(args.root, args.out)
    else:
        if args.out is None:
            raise ValueError("OPQ8 paired output missing")
        run_validate(args.root, args.out)


if __name__ == "__main__":
    main()
