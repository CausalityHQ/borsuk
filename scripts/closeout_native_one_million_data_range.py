"""Authenticate terminal closure and resource receipts before accepting range evidence."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np

from scripts.launch_native_one_million_selector_spot import (
    SelectorSpotPlan,
    _validate_terminal_bytes,
    artifact_names,
)
from scripts.native_one_million_page_oracle_cell import MAP_DTYPE
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.recount_native_one_million_data_range import recount_range_masks
from scripts.v97_row_width_screen import _authenticate_object

MARGIN = 64 * 1024 * 1024


def _identity(path: Path) -> tuple[int, str]:
    body = path.read_bytes()
    return len(body), hashlib.sha256(body).hexdigest()


def _receipt(path: Path, maximum_rss_bytes: int) -> None:
    values = {}
    for line in path.read_text().splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            values[key.strip()] = value.strip()
    try:
        maximum_kib = int(values["Maximum resident set size (kbytes)"])
        tree_bytes = int(values["Maximum sampled process-tree RSS (bytes)"])
        samples = int(values["Process-tree RSS samples"])
        swaps = int(values["Swaps"])
    except (KeyError, ValueError) as error:
        raise ValueError("data-range resource receipt differs") from error
    if (
        maximum_kib < 0 or tree_bytes < 0 or samples <= 0 or swaps != 0
        or maximum_kib * 1024 + MARGIN > maximum_rss_bytes
        or tree_bytes + MARGIN > maximum_rss_bytes
    ):
        raise ValueError("data-range resource cap differs")


def closeout_receipts(root: Path, plan: SelectorSpotPlan, instance_id: str) -> dict[str, object]:
    """Verify a complete immutable attempt, not merely computational output."""
    if plan.selector_kind != "data_range":
        raise ValueError("data-range closeout plan differs")
    reservation_body = (root / "reservation.json").read_bytes()
    reservation = json.loads(reservation_body)
    if (
        reservation_body != (json.dumps(reservation, sort_keys=True, separators=(",", ":")) + "\n").encode()
        or reservation.get("schema") != "borsuk-one-million-opq8-data-range-reservation-v1"
        or reservation.get("attempt") != plan.attempt
        or reservation.get("source_commit") != plan.source_commit
        or reservation.get("source_archive") != {
            "uri": plan.source_archive.uri,
            "sha256": plan.source_archive.sha256,
            "encoded_bytes": plan.source_archive.encoded_bytes,
        }
        or reservation.get("requirements_sha256") != plan.requirements_sha256
    ):
        raise ValueError("data-range reservation differs")
    if _identity(root / "source.tar.gz") != (
        plan.source_archive.encoded_bytes, plan.source_archive.sha256
    ):
        raise ValueError("data-range source archive differs")
    terminal_body = (root / "terminal.json").read_bytes()
    terminal = _validate_terminal_bytes(terminal_body, plan, instance_id)
    if terminal["status"] != "complete":
        raise ValueError("data-range terminal incomplete")
    for role, filename in artifact_names("data_range").items():
        recorded = terminal["artifacts"][role]
        if _identity(root / filename) != (
            recorded["encoded_bytes"], recorded["sha256"]
        ):
            raise ValueError(f"data-range {role} readback differs")
    for role, filename in artifact_names("data_range").items():
        if role.endswith("-resources"):
            _receipt(root / filename, plan.maximum_rss_bytes)
    return {
        "status": "complete",
        "terminal_sha256": hashlib.sha256(terminal_body).hexdigest(),
        "source_commit": plan.source_commit,
        "instance_id": instance_id,
        "artifact_count": len(terminal["artifacts"]),
    }


def closeout_closed_attempt(
    root: Path, plan: SelectorSpotPlan, instance_id: str, *, query_count: int = 1000,
) -> dict[str, object]:
    """Require receipt closure and an independent all-query geometry recount."""
    receipt = closeout_receipts(root, plan, instance_id)
    _authenticate_object("generation", root / "generation.json", SOURCE_IDENTITIES["generation"])
    _, truth_ids = _query_truth(
        root / "queries.parquet", root / "truth.parquet",
        DEVELOPMENT_IDENTITIES, query_count=query_count,
    )
    generation = json.loads((root / "generation.json").read_bytes())
    runs = generation["runs"]
    if len(runs) != 2 or [run["kind"] for run in runs] != ["base", "delta"]:
        raise ValueError("data-range closeout generation differs")
    lengths = {
        role: tuple(int(page["bytes"]) for page in run["pages"])
        for role, run in zip(("base", "delta"), runs, strict=True)
    }
    page_bytes = np.fromfile(root / "page-bytes.bin", dtype="<u4")
    if page_bytes.tolist() != [*lengths["base"], *lengths["delta"]]:
        raise ValueError("data-range closeout page bytes differ")
    membership = np.fromfile(root / "page-map.bin", dtype=MAP_DTYPE)
    if not np.all(membership["id"][1:] > membership["id"][:-1]):
        raise ValueError("data-range closeout membership order differs")
    positions = np.searchsorted(membership["id"], truth_ids)
    if np.any(positions >= len(membership)) or not np.array_equal(membership["id"][positions], truth_ids):
        raise ValueError("data-range closeout truth owners differ")
    truth_pages = membership["page"][positions]
    plans_body = (root / "range-plans.json").read_bytes()
    evidence_body = (root / "range-evidence.json").read_bytes()
    result_body = (root / "range-result.json").read_bytes()
    plans = json.loads(plans_body)
    evidence = json.loads(evidence_body)
    result = json.loads(result_body)
    if any(
        body != (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        for body, value in (
            (plans_body, plans), (evidence_body, evidence), (result_body, result)
        )
    ) or len(plans.get("samples", [])) != query_count:
        raise ValueError("data-range closeout canonical output differs")
    samples, metrics = recount_range_masks(
        plans["samples"],
        [[int(page) for page in row] for row in truth_pages], lengths,
    )
    candidate = metrics["candidate"]
    advances = (
        candidate["gt100_hits"] >= 98_151 and candidate["gt10_hits"] >= 9_928
        and candidate["p05_gt100_hits"] >= 90 and candidate["sub90_queries"] <= 49
        and candidate["maximum_gets"] <= 32 and candidate["maximum_bytes"] <= 16_777_216
    )
    decision = "data-range-selector-advance" if advances else "data-range-selector-killed"
    if (
        evidence.get("samples") != samples or evidence.get("metrics") != metrics
        or evidence.get("plans_sha256") != hashlib.sha256(plans_body).hexdigest()
        or result.get("metrics") != metrics or result.get("decision") != decision
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or result.get("claim_eligible") is not False
    ):
        raise ValueError("data-range closeout independent result differs")
    return {**receipt, "decision": decision, "metrics": metrics}
