"""A complete computational replay needs a valid terminal and resource receipts."""

import dataclasses
import hashlib
import json

import numpy as np
import pytest

from scripts import closeout_native_one_million_data_range as closeout
from scripts.closeout_native_one_million_data_range import closeout_receipts
from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import artifact_names, build_plan
from scripts.native_one_million_page_oracle_cell import MAP_DTYPE
from scripts.recount_native_one_million_data_range import recount_range_masks


def _body(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def test_closeout_rejects_resource_excess_after_complete_terminal(tmp_path) -> None:
    commit = "12" * 20
    archive = b"frozen"
    plan = build_plan(
        source_commit=commit,
        source_archive=SourceArchiveIdentity(
            "s3://frozen/source.tar.gz", hashlib.sha256(archive).hexdigest(), len(archive)
        ),
        requirements_sha256="56" * 32,
        output_prefix=(
            "s3://borsuk-bench-453182569524-euc1/research/"
            f"native-one-million-data-range-selector/{commit}/runs/relaion-1m-dev1000-a0001"
        ),
        selector_kind="data_range",
    )
    (tmp_path / "source.tar.gz").write_bytes(archive)
    artifacts = {}
    for role, filename in artifact_names("data_range").items():
        body = (
            b"Maximum resident set size (kbytes): 100\n"
            b"Maximum sampled process-tree RSS (bytes): 100000\n"
            b"Process-tree RSS samples: 1\nSwaps: 0\n"
            if role.endswith("-resources") else b"sealed"
        )
        (tmp_path / filename).write_bytes(body)
        artifacts[role] = {
            "encoded_bytes": len(body), "role": role,
            "sha256": hashlib.sha256(body).hexdigest(),
            "uri": plan.output_prefix + "/artifacts/" + filename,
        }
    terminal = {
        "artifacts": artifacts, "attempt": 1, "claim_eligible": False,
        "elapsed_seconds": 10, "exit_code": 0, "instance_id": "i-0123456789abcdef0",
        "phase": "complete", "schema": "borsuk-one-million-opq8-data-range-terminal-v1",
        "source_commit": commit, "source_archive": dataclasses.asdict(plan.source_archive),
        "requirements_sha256": plan.requirements_sha256, "status": "complete",
    }
    (tmp_path / "terminal.json").write_bytes(_body(terminal))
    (tmp_path / "reservation.json").write_bytes(_body({
        "schema": "borsuk-one-million-opq8-data-range-reservation-v1",
        "attempt": 1, "source_commit": commit,
        "source_archive": dataclasses.asdict(plan.source_archive),
        "requirements_sha256": plan.requirements_sha256,
        "source_inputs": {}, "development_inputs": {},
    }))
    assert closeout_receipts(tmp_path, plan, terminal["instance_id"])["status"] == "complete"
    (tmp_path / "plan-resources.txt").write_bytes(
        b"Maximum resident set size (kbytes): 100\n"
        b"Maximum sampled process-tree RSS (bytes): 100000\n"
        b"Process-tree RSS samples: 1\nSwaps: 1\n"
    )
    modified = (tmp_path / "plan-resources.txt").read_bytes()
    terminal["artifacts"]["plan-resources"]["encoded_bytes"] = len(modified)
    terminal["artifacts"]["plan-resources"]["sha256"] = hashlib.sha256(modified).hexdigest()
    (tmp_path / "terminal.json").write_bytes(_body(terminal))
    with pytest.raises(ValueError, match="resource cap"):
        closeout_receipts(tmp_path, plan, terminal["instance_id"])


def test_closed_attempt_recounts_all_masks(tmp_path, monkeypatch) -> None:
    plan = {
        "priority_pages": [["base", 0], ["base", 2]],
        "target_pages": [["base", 0], ["base", 2]],
        "ranges": [["base", 0, 3]],
        "included_pages": [["base", 0], ["base", 1], ["base", 2]],
        "gets": 1, "encoded_bytes": 22,
    }
    samples = [{"query_ordinal": 0, "candidate": plan, "control": plan}]
    evidence_samples, metrics = recount_range_masks(
        samples, [[1, 2, 3]], {"base": (10, 2, 10), "delta": (8,)}
    )
    (tmp_path / "generation.json").write_bytes(_body({"runs": [
        {"kind": "base", "pages": [{"bytes": 10}, {"bytes": 2}, {"bytes": 10}]},
        {"kind": "delta", "pages": [{"bytes": 8}]},
    ]}))
    np.array([10, 2, 10, 8], dtype="<u4").tofile(tmp_path / "page-bytes.bin")
    members = np.empty(3, dtype=MAP_DTYPE)
    members["id"] = [0, 1, 2]
    members["page"] = [1, 2, 3]
    members.tofile(tmp_path / "page-map.bin")
    plans_body = _body({"samples": samples})
    (tmp_path / "range-plans.json").write_bytes(plans_body)
    evidence = {
        "samples": evidence_samples, "metrics": metrics,
        "plans_sha256": hashlib.sha256(plans_body).hexdigest(),
    }
    evidence_body = _body(evidence)
    (tmp_path / "range-evidence.json").write_bytes(evidence_body)
    (tmp_path / "range-result.json").write_bytes(_body({
        "claim_eligible": False, "decision": "data-range-selector-killed",
        "metrics": metrics, "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
    }))
    monkeypatch.setattr(closeout, "closeout_receipts", lambda root, plan, instance: {"status": "complete"})
    monkeypatch.setattr(closeout, "_authenticate_object", lambda *args: None)
    monkeypatch.setattr(
        closeout, "_query_truth",
        lambda *args, **kwargs: (None, np.array([[0, 1, 2]], dtype=np.int64)),
    )
    assert closeout.closeout_closed_attempt(tmp_path, object(), "i-test", query_count=1)["metrics"] == metrics
    evidence["samples"][0]["candidate"]["hit_mask"] = "000"
    (tmp_path / "range-evidence.json").write_bytes(_body(evidence))
    with pytest.raises(ValueError, match="independent result"):
        closeout.closeout_closed_attempt(tmp_path, object(), "i-test", query_count=1)
