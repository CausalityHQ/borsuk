"""Independently authenticate and recount a closed source-score range attempt."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from pathlib import Path

import numpy as np

from scripts.closeout_native_one_million_data_range import MARGIN, _identity, _receipt
from scripts.launch_native_one_million_selector_spot import (
    SelectorSpotPlan,
    _validate_terminal_bytes,
    artifact_names,
)
from scripts.native_one_million_page_oracle_cell import MAP_DTYPE, PRIOR
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.native_one_million_source_range_cell import SCHEMA, SCORE_ARITHMETIC
from scripts.recount_native_one_million_data_range import recount_range_masks
from scripts.v97_row_width_screen import _authenticate_object


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def closeout_receipts(root: Path, plan: SelectorSpotPlan, instance_id: str) -> dict[str, object]:
    if plan.selector_kind != "source_range":
        raise ValueError("source-range closeout plan differs")
    reservation_body = (root / "reservation.json").read_bytes()
    reservation = json.loads(reservation_body)
    if (
        reservation_body != _canonical(reservation)
        or reservation.get("schema") != "borsuk-one-million-source-range-reservation-v1"
        or reservation.get("attempt") != plan.attempt
        or reservation.get("source_commit") != plan.source_commit
        or reservation.get("source_archive") != dataclasses.asdict(plan.source_archive)
        or reservation.get("requirements_sha256") != plan.requirements_sha256
        or reservation.get("source_inputs", {}).get("source") != dataclasses.asdict(SOURCE_IDENTITIES["source"])
    ):
        raise ValueError("source-range reservation differs")
    if _identity(root / "source.tar.gz") != (
        plan.source_archive.encoded_bytes, plan.source_archive.sha256,
    ):
        raise ValueError("source-range source archive differs")
    terminal_body = (root / "terminal.json").read_bytes()
    terminal = _validate_terminal_bytes(terminal_body, plan, instance_id)
    if terminal["status"] != "complete":
        raise ValueError("source-range terminal incomplete")
    for role, filename in artifact_names("source_range").items():
        recorded = terminal["artifacts"][role]
        if _identity(root / filename) != (recorded["encoded_bytes"], recorded["sha256"]):
            raise ValueError(f"source-range {role} readback differs")
        if role.endswith("-resources"):
            _receipt(root / filename, plan.maximum_rss_bytes)
    return {
        "status": "complete",
        "terminal_sha256": hashlib.sha256(terminal_body).hexdigest(),
        "source_commit": plan.source_commit,
        "instance_id": instance_id,
        "artifact_count": len(terminal["artifacts"]),
        "resource_margin_bytes": MARGIN,
    }


def closeout_closed_attempt(
    root: Path, plan: SelectorSpotPlan, instance_id: str, *, query_count: int = 1000,
) -> dict[str, object]:
    receipt = closeout_receipts(root, plan, instance_id)
    _authenticate_object("generation", root / "generation.json", SOURCE_IDENTITIES["generation"])
    _, truth_ids = _query_truth(
        root / "queries.parquet", root / "truth.parquet",
        DEVELOPMENT_IDENTITIES, query_count=query_count,
    )
    generation = json.loads((root / "generation.json").read_bytes())
    runs = generation["runs"]
    if len(runs) != 2 or [run["kind"] for run in runs] != ["base", "delta"]:
        raise ValueError("source-range generation differs")
    lengths = {
        role: tuple(int(page["bytes"]) for page in run["pages"])
        for role, run in zip(("base", "delta"), runs, strict=True)
    }
    page_bytes = np.fromfile(root / "page-bytes.bin", dtype="<u4")
    if page_bytes.tolist() != [*lengths["base"], *lengths["delta"]]:
        raise ValueError("source-range page bytes differ")
    membership = np.fromfile(root / "page-map.bin", dtype=MAP_DTYPE)
    if not np.all(membership["id"][1:] > membership["id"][:-1]):
        raise ValueError("source-range membership order differs")
    positions = np.searchsorted(membership["id"], truth_ids)
    if np.any(positions >= len(membership)) or not np.array_equal(membership["id"][positions], truth_ids):
        raise ValueError("source-range truth owners differ")
    truth_pages = membership["page"][positions]
    plans_body = (root / "source-range-plans.json").read_bytes()
    seal_body = (root / "source-range-plan-seal.json").read_bytes()
    evidence_body = (root / "source-range-evidence.json").read_bytes()
    result_body = (root / "source-range-result.json").read_bytes()
    validation_body = (root / "source-range-validation.json").read_bytes()
    plans, seal, evidence, result, validation = (
        json.loads(body)
        for body in (plans_body, seal_body, evidence_body, result_body, validation_body)
    )
    if any(
        body != _canonical(value)
        for body, value in (
            (plans_body, plans), (seal_body, seal), (evidence_body, evidence),
            (result_body, result), (validation_body, validation),
        )
    ) or (
        plans.get("schema") != SCHEMA + "-plans"
        or plans.get("prior_plans_sha256") != PRIOR["plans"][1]
        or plans.get("source_identity") != dataclasses.asdict(SOURCE_IDENTITIES["source"])
        or plans.get("query_identity") != dataclasses.asdict(DEVELOPMENT_IDENTITIES["queries"])
        or plans.get("scorer", {}).get("arithmetic") != SCORE_ARITHMETIC
        or plans.get("scorer", {}).get("openblas_threads") != "1"
        or plans.get("scorer", {}).get("omp_threads") != "1"
        or plans.get("source_seal_sha256") != _identity(root / "range-seal.json")[1]
        or len(plans.get("samples", [])) != query_count
        or seal != {
            "schema": SCHEMA + "-plan-seal",
            "plans_sha256": hashlib.sha256(plans_body).hexdigest(),
            "source_seal_sha256": plans["source_seal_sha256"],
            "query_identity": plans["query_identity"],
            "scorer": plans["scorer"],
        }
    ):
        raise ValueError("source-range sealed plan differs")
    samples, metrics = recount_range_masks(
        plans["samples"], [[int(page) for page in row] for row in truth_pages], lengths,
    )
    candidate = metrics["candidate"]
    passes = (
        candidate["gt100_hits"] >= 98_151 and candidate["gt10_hits"] >= 9_928
        and candidate["p05_gt100_hits"] >= 90 and candidate["sub90_queries"] <= 49
        and candidate["maximum_gets"] <= 32 and candidate["maximum_bytes"] <= 16_777_216
    )
    decision = "source-score-diagnostic-pass" if passes else "source-score-diagnostic-fail"
    if (
        evidence.get("schema") != SCHEMA + "-evidence"
        or evidence.get("samples") != samples or evidence.get("metrics") != metrics
        or evidence.get("plans_sha256") != hashlib.sha256(plans_body).hexdigest()
        or result.get("schema") != SCHEMA + "-result"
        or result.get("metrics") != metrics or result.get("decision") != decision
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or result.get("claim_eligible") is not False
        or validation.get("schema") != SCHEMA + "-validation"
        or validation.get("decision") != decision or validation.get("metrics") != metrics
        or validation.get("source_identity") != {
            "bytes": SOURCE_IDENTITIES["source"].bytes,
            "sha256": SOURCE_IDENTITIES["source"].sha256,
        }
        or validation.get("plans_sha256") != hashlib.sha256(plans_body).hexdigest()
        or validation.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or validation.get("result_sha256") != hashlib.sha256(result_body).hexdigest()
    ):
        raise ValueError("source-range independent result differs")
    return {**receipt, "decision": decision, "metrics": metrics}
