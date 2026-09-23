"""Read back a terminal-closed page-code projection and replay its evidence."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from pathlib import Path

from scripts.closeout_native_one_million_data_range import _receipt
from scripts.launch_native_one_million_selector_spot import (
    SelectorSpotPlan,
    _validate_terminal_bytes,
    artifact_names,
)
from scripts.native_one_million_progressive_code_projection_cell import (
    EVIDENCE_FILE,
    PLAN_FILE,
    RESULT_FILE,
    _prior_authority,
    _source_geometry,
)
from scripts.validate_native_one_million_progressive_code_projection import (
    validate_closed,
)


def _identity(path: Path) -> tuple[int, str]:
    body = path.read_bytes()
    return len(body), hashlib.sha256(body).hexdigest()


def closeout_receipts(root: Path, plan: SelectorSpotPlan, instance_id: str) -> dict[str, object]:
    """Authenticate the exact archive, terminal roster and zero-swap phases."""
    if plan.selector_kind != "progressive_code_wave":
        raise ValueError("code-wave closeout selector differs")
    reservation_body = (root / "reservation.json").read_bytes()
    reservation = json.loads(reservation_body)
    if (
        reservation_body != (json.dumps(reservation, sort_keys=True, separators=(",", ":")) + "\n").encode()
        or reservation.get("schema") != "borsuk-one-million-progressive_code_wave-selector-reservation-v1"
        or reservation.get("attempt") != plan.attempt
        or reservation.get("source_commit") != plan.source_commit
        or reservation.get("source_archive") != dataclasses.asdict(plan.source_archive)
        or reservation.get("requirements_sha256") != plan.requirements_sha256
    ):
        raise ValueError("code-wave reservation differs")
    if _identity(root / "source.tar.gz") != (
        plan.source_archive.encoded_bytes, plan.source_archive.sha256,
    ):
        raise ValueError("code-wave source archive differs")
    terminal_body = (root / "terminal.json").read_bytes()
    terminal = _validate_terminal_bytes(terminal_body, plan, instance_id)
    if terminal["status"] != "complete":
        raise ValueError("code-wave terminal incomplete")
    for role, filename in artifact_names(plan.selector_kind).items():
        receipt = terminal["artifacts"][role]
        if _identity(root / filename) != (receipt["encoded_bytes"], receipt["sha256"]):
            raise ValueError(f"code-wave {role} readback differs")
        if role.endswith("-resources"):
            _receipt(root / filename, plan.maximum_rss_bytes)
    _prior_authority(root)
    _source_geometry(root)
    result = json.loads((root / RESULT_FILE).read_bytes())
    validation = json.loads((root / "progressive-code-wave-validation.json").read_bytes())
    if (
        validation.get("valid") is not True
        or validation.get("plans_sha256") != _identity(root / PLAN_FILE)[1]
        or validation.get("evidence_sha256") != _identity(root / EVIDENCE_FILE)[1]
        or validation.get("result_sha256") != _identity(root / RESULT_FILE)[1]
        or validation.get("metrics") != result.get("metrics")
    ):
        raise ValueError("code-wave closed validation differs")
    return {
        "schema": "borsuk-one-million-progressive-code-wave-closeout-v1",
        "status": "complete",
        "source_commit": plan.source_commit,
        "instance_id": instance_id,
        "terminal_sha256": hashlib.sha256(terminal_body).hexdigest(),
        "artifact_count": len(terminal["artifacts"]),
        "quality_advance_candidate": result["quality_advance_candidate"],
        "metrics": result["metrics"],
    }


def closeout_closed_attempt(root: Path, plan: SelectorSpotPlan, instance_id: str) -> dict[str, object]:
    """Require all-query independent replay after terminal and input readback."""
    receipt = closeout_receipts(root, plan, instance_id)
    path = root / "progressive-code-wave-validation.json"
    original = path.read_bytes()
    replay = validate_closed(root, root)
    if path.read_bytes() != original:
        raise ValueError("code-wave independent validation differs")
    return {**receipt, "validation": replay}
