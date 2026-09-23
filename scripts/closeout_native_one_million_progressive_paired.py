"""Read back and authenticate a terminal-closed progressive paired cell."""

from __future__ import annotations

import dataclasses
import hashlib
from pathlib import Path

import numpy as np

from scripts.closeout_native_one_million_data_range import _receipt
from scripts.launch_native_one_million_selector_spot import (
    SelectorSpotPlan,
    _validate_terminal_bytes,
    artifact_names,
)
from scripts.native_one_million_page_oracle_cell import _read_map
from scripts.native_one_million_progressive_paired_cell import (
    EVIDENCE_FILE,
    PLAN_FILE,
    RESULT_FILE,
    ROTATION_SEED,
    SCHEMA,
    _prior_plans,
    _read_json,
)
from scripts.native_one_million_selector_cell import SOURCE_IDENTITIES
from scripts.native_progressive_page_objects import read_authenticated_page


def _identity(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            size += len(block)
            digest.update(block)
    return size, digest.hexdigest()


def closeout_receipts(root: Path, plan: SelectorSpotPlan, instance_id: str) -> dict[str, object]:
    """Verify terminal, full artifact roster, exact source and split record digest."""
    if plan.selector_kind != "progressive_paired":
        raise ValueError("progressive paired closeout kind differs")
    reservation = _read_json(root / "reservation.json")
    if (
        reservation.get("schema") != "borsuk-one-million-progressive_paired-selector-reservation-v1"
        or reservation.get("attempt") != plan.attempt
        or reservation.get("source_commit") != plan.source_commit
        or reservation.get("source_archive") != dataclasses.asdict(plan.source_archive)
        or reservation.get("requirements_sha256") != plan.requirements_sha256
    ):
        raise ValueError("progressive paired reservation differs")
    if _identity(root / "source.tar.gz") != (
        plan.source_archive.encoded_bytes, plan.source_archive.sha256,
    ):
        raise ValueError("progressive paired source archive differs")
    terminal_body = (root / "terminal.json").read_bytes()
    terminal = _validate_terminal_bytes(terminal_body, plan, instance_id)
    if terminal["status"] != "complete":
        raise ValueError("progressive paired terminal incomplete")
    for role, filename in artifact_names(plan.selector_kind).items():
        artifact = terminal["artifacts"][role]
        if _identity(root / filename) != (artifact["encoded_bytes"], artifact["sha256"]):
            raise ValueError(f"progressive paired {role} readback differs")
        if role.endswith("-resources"):
            _receipt(root / filename, plan.maximum_rss_bytes)
    _prior_plans(root)
    code = _read_json(root / "progressive-code-seal.json")
    build = _read_json(root / "progressive-build.json")
    mean_bytes, mean_sha = _identity(root / "mean.bin")
    _, _, membership, page_groups, _ = _read_map(root)
    generation = _read_json(root / "generation.json")
    counts = np.bincount(membership["page"].astype(np.intp), minlength=len(page_groups))
    if (
        code.get("schema") != "borsuk-progressive-two-bit-pages-v1"
        or code.get("rows") != 1_000_000
        or mean_bytes != 768 * 4 or mean_sha != code.get("mean_sha256")
        or build.get("code_seal_sha256") != _identity(root / "progressive-code-seal.json")[1]
        or build.get("records_sha256") != code.get("records_sha256")
        or code.get("source_sha256") != SOURCE_IDENTITIES["source"].sha256
        or code.get("layout_sha256") != _identity(root / "seal.json")[1]
        or code.get("rotation_seed") != ROTATION_SEED
        or code.get("base_pages") != len(generation["runs"][0]["pages"])
        or code.get("page_row_counts") != [int(count) for count in counts]
        or code.get("row_bytes") != {"sign": 104, "magnitude": 96}
        or build.get("prior_plans_sha256") !=
        "ee68310ef3084e2f08a7f26bb832251f8ab420a6abb80eb8d4c75adfe947a2ef"
    ):
        raise ValueError("progressive paired code seal differs")
    record_digest = hashlib.sha256()
    for page in range(len(code["page_row_counts"])):
        record_digest.update(read_authenticated_page(
            root, code, page, authenticated_seal=True,
        ).tobytes(order="C"))
    if record_digest.hexdigest() != code["records_sha256"]:
        raise ValueError("progressive paired rejoined record digest differs")
    result = _read_json(root / RESULT_FILE)
    validation = _read_json(root / "progressive-paired-validation.json")
    if (
        result.get("schema") != SCHEMA + "-result"
        or validation.get("schema") != SCHEMA + "-validation"
        or validation.get("valid") is not True
        or validation.get("plans_sha256") != _identity(root / PLAN_FILE)[1]
        or validation.get("evidence_sha256") != _identity(root / EVIDENCE_FILE)[1]
        or validation.get("result_sha256") != _identity(root / RESULT_FILE)[1]
        or validation.get("metrics") != result.get("metrics")
    ):
        raise ValueError("progressive paired closed result differs")
    return {
        "schema": "borsuk-one-million-progressive-paired-closeout-v1",
        "status": "complete", "instance_id": instance_id,
        "source_commit": plan.source_commit,
        "terminal_sha256": hashlib.sha256(terminal_body).hexdigest(),
        "artifact_count": len(terminal["artifacts"]),
        "advance_candidate": result["advance_candidate"],
        "metrics": result["metrics"],
    }
