"""Read back and independently recount a closed sign96/source Spot attempt."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
from pathlib import Path

from scripts.launch_native_geometric_layout_spot import SpotLayoutPlan
from scripts.launch_native_hundred_thousand_sign96_spot import validate_terminal_bytes
from scripts.native_hundred_thousand_sign96_worker import ARTIFACT_FILES
from scripts.validate_native_hundred_thousand_sign96_cell import validate_closed


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _identity(path: Path) -> tuple[int, str]:
    body = path.read_bytes()
    return len(body), hashlib.sha256(body).hexdigest()


def closeout_receipts(root: Path, plan: SpotLayoutPlan, instance_id: str) -> dict[str, object]:
    """Authenticate only a terminal attempt and every byte named by its roster."""
    reservation_body = (root / "reservation.json").read_bytes()
    reservation = json.loads(reservation_body)
    if reservation_body != _canonical(reservation) or reservation != {
        "schema": "borsuk-hundred-thousand-sign96-reservation-v1",
        "attempt": plan.attempt,
        "source_commit": plan.source_commit,
        "source_archive": dataclasses.asdict(plan.source_archive),
        "requirements_sha256": plan.requirements_sha256,
    }:
        raise ValueError("sign96 reservation differs")
    if _identity(root / "source.tar.gz") != (
        plan.source_archive.encoded_bytes, plan.source_archive.sha256,
    ):
        raise ValueError("sign96 source archive differs")
    terminal_body = (root / "terminal.json").read_bytes()
    terminal = validate_terminal_bytes(terminal_body, plan, instance_id)
    if terminal["status"] != "complete":
        raise ValueError("sign96 terminal incomplete")
    for role, path in ARTIFACT_FILES.items():
        recorded = terminal["artifacts"][role]
        if _identity(root / path) != (recorded["encoded_bytes"], recorded["sha256"]):
            raise ValueError(f"sign96 {role} readback differs")
    decision_body = (root / ARTIFACT_FILES["decision"]).read_bytes()
    decision = json.loads(decision_body)
    result_body = (root / ARTIFACT_FILES["result"]).read_bytes()
    validation_body = (root / ARTIFACT_FILES["validation"]).read_bytes()
    if (
        decision_body != _canonical(decision)
        or decision.get("schema") != "borsuk-hundred-thousand-sign96-decision-v1"
        or decision.get("decision") != terminal["decision"]
        or decision.get("result_sha256") != hashlib.sha256(result_body).hexdigest()
        or decision.get("validation_sha256") != hashlib.sha256(validation_body).hexdigest()
        or decision.get("resource_cap_bytes") != 3 * 1024**3 - 64 * 1024**2
        or set(decision.get("resources", {})) != {"construct", "plan", "evaluate", "validate"}
    ):
        raise ValueError("sign96 decision receipt differs")
    cap = decision["resource_cap_bytes"]
    resource_pass = True
    for phase, values in decision["resources"].items():
        resource_text = (root / ARTIFACT_FILES[f"{phase}-resources"]).read_text()
        rss = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", resource_text)
        swaps = re.search(r"Swaps:\s*(\d+)", resource_text)
        peak = int((root / ARTIFACT_FILES[f"{phase}-peak"]).read_text().strip())
        if rss is None or swaps is None or values != {
            "maximum_rss_bytes": int(rss.group(1)) * 1024,
            "tree_peak_bytes": peak,
            "swaps": int(swaps.group(1)),
        }:
            raise ValueError("sign96 resource readback differs")
        resource_pass &= peak <= cap and int(rss.group(1)) * 1024 <= cap and int(swaps.group(1)) == 0
    result = json.loads(result_body)
    validation = json.loads(validation_body)
    expected_decision = "advance" if result.get("quality_advance_candidate") and resource_pass else "reject"
    if (
        validation.get("valid") is not True
        or decision.get("quality_advance_candidate") is not result.get("quality_advance_candidate")
        or decision.get("resource_pass") is not resource_pass
        or decision.get("decision") != expected_decision
    ):
        raise ValueError("sign96 decision or resource gate differs")
    return {
        "schema": "borsuk-hundred-thousand-sign96-closeout-v1",
        "source_commit": plan.source_commit,
        "instance_id": instance_id,
        "terminal_sha256": hashlib.sha256(terminal_body).hexdigest(),
        "artifact_count": len(terminal["artifacts"]),
        "decision": expected_decision,
        "resources": decision["resources"],
    }


def closeout_closed_attempt(
    root: Path, plan: SpotLayoutPlan, instance_id: str,
) -> dict[str, object]:
    receipt = closeout_receipts(root, plan, instance_id)
    validation_path = root / ARTIFACT_FILES["validation"]
    original_validation = validation_path.read_bytes()
    replay = validate_closed(root, validation_path.parent)
    if validation_path.read_bytes() != original_validation:
        raise ValueError("sign96 independent replay differs")
    return {**receipt, "validation": replay}
