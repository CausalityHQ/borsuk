"""Closed source-score evidence requires receipts and independent truth recount."""

import dataclasses
import hashlib
import json

import numpy as np
import pytest

from scripts import closeout_native_one_million_source_range as closeout
from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import artifact_names, build_plan
from scripts.native_one_million_page_oracle_cell import MAP_DTYPE, PRIOR
from scripts.native_one_million_selector_cell import SOURCE_IDENTITIES
from scripts.native_one_million_source_range_cell import SCHEMA, SCORE_ARITHMETIC
from scripts.recount_native_one_million_data_range import recount_range_masks


def _body(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def test_source_range_receipts_reject_swap_after_complete_terminal(tmp_path) -> None:
    commit = "12" * 20
    archive = b"frozen"
    plan = build_plan(
        source_commit=commit,
        source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", hashlib.sha256(archive).hexdigest(), len(archive)),
        requirements_sha256="56" * 32,
        output_prefix=(
            "s3://borsuk-bench-453182569524-euc1/research/"
            f"native-one-million-source-range-diagnostic/{commit}/runs/relaion-1m-dev1000-a0001"
        ),
        selector_kind="source_range",
    )
    (tmp_path / "source.tar.gz").write_bytes(archive)
    artifacts = {}
    for role, filename in artifact_names("source_range").items():
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
        "phase": "complete", "schema": "borsuk-one-million-source-range-terminal-v1",
        "source_commit": commit, "source_archive": dataclasses.asdict(plan.source_archive),
        "requirements_sha256": plan.requirements_sha256, "status": "complete",
    }
    (tmp_path / "terminal.json").write_bytes(_body(terminal))
    (tmp_path / "reservation.json").write_bytes(_body({
        "schema": "borsuk-one-million-source-range-reservation-v1",
        "attempt": 1, "source_commit": commit,
        "source_archive": dataclasses.asdict(plan.source_archive),
        "requirements_sha256": plan.requirements_sha256,
        "source_inputs": {"source": dataclasses.asdict(SOURCE_IDENTITIES["source"])},
    }))
    assert closeout.closeout_receipts(tmp_path, plan, terminal["instance_id"])["status"] == "complete"
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
        closeout.closeout_receipts(tmp_path, plan, terminal["instance_id"])


def test_source_range_closeout_recounts_truth_masks(tmp_path, monkeypatch) -> None:
    range_plan = {
        "priority_pages": [["base", 0], ["base", 2]],
        "target_pages": [["base", 0], ["base", 2]],
        "ranges": [["base", 0, 3]],
        "included_pages": [["base", 0], ["base", 1], ["base", 2]],
        "gets": 1, "encoded_bytes": 22,
    }
    samples = [{"query_ordinal": 0, "candidate": range_plan, "control": range_plan}]
    evidence_samples, metrics = recount_range_masks(
        samples, [[1, 2, 3]], {"base": (10, 2, 10), "delta": (8,)}
    )
    (tmp_path / "generation.json").write_bytes(_body({"runs": [
        {"kind": "base", "pages": [{"bytes": 10}, {"bytes": 2}, {"bytes": 10}]},
        {"kind": "delta", "pages": [{"bytes": 8}]},
    ]}))
    np.array([10, 2, 10, 8], dtype="<u4").tofile(tmp_path / "page-bytes.bin")
    membership = np.empty(3, dtype=MAP_DTYPE)
    membership["id"] = [0, 1, 2]
    membership["page"] = [1, 2, 3]
    membership.tofile(tmp_path / "page-map.bin")
    (tmp_path / "range-seal.json").write_bytes(b"sealed")
    plans = {
        "schema": SCHEMA + "-plans", "samples": samples,
        "prior_plans_sha256": PRIOR["plans"][1],
        "source_identity": dataclasses.asdict(SOURCE_IDENTITIES["source"]),
        "query_identity": dataclasses.asdict(closeout.DEVELOPMENT_IDENTITIES["queries"]),
        "source_seal_sha256": hashlib.sha256(b"sealed").hexdigest(),
        "scorer": {"arithmetic": SCORE_ARITHMETIC, "openblas_threads": "1", "omp_threads": "1"},
    }
    plans_body = _body(plans)
    (tmp_path / "source-range-plans.json").write_bytes(plans_body)
    (tmp_path / "source-range-plan-seal.json").write_bytes(_body({
        "schema": SCHEMA + "-plan-seal",
        "plans_sha256": hashlib.sha256(plans_body).hexdigest(),
        "source_seal_sha256": plans["source_seal_sha256"],
        "query_identity": plans["query_identity"], "scorer": plans["scorer"],
    }))
    evidence = {
        "schema": SCHEMA + "-evidence", "samples": evidence_samples,
        "metrics": metrics, "plans_sha256": hashlib.sha256(plans_body).hexdigest(),
    }
    evidence_body = _body(evidence)
    (tmp_path / "source-range-evidence.json").write_bytes(evidence_body)
    result = {
        "schema": SCHEMA + "-result", "claim_eligible": False,
        "decision": "source-score-diagnostic-fail", "metrics": metrics,
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
    }
    result_body = _body(result)
    (tmp_path / "source-range-result.json").write_bytes(result_body)
    (tmp_path / "source-range-validation.json").write_bytes(_body({
        "schema": SCHEMA + "-validation", "decision": result["decision"],
        "metrics": metrics,
        "source_identity": {
            "bytes": SOURCE_IDENTITIES["source"].bytes,
            "sha256": SOURCE_IDENTITIES["source"].sha256,
        },
        "plans_sha256": hashlib.sha256(plans_body).hexdigest(),
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "result_sha256": hashlib.sha256(result_body).hexdigest(),
    }))
    monkeypatch.setattr(closeout, "closeout_receipts", lambda root, plan, instance: {"status": "complete"})
    monkeypatch.setattr(closeout, "_authenticate_object", lambda *args: None)
    monkeypatch.setattr(
        closeout, "_query_truth",
        lambda *args, **kwargs: (None, np.array([[0, 1, 2]], dtype=np.int64)),
    )
    assert closeout.closeout_closed_attempt(tmp_path, object(), "i-test", query_count=1)["metrics"] == metrics
    evidence["samples"][0]["candidate"]["hit_mask"] = "000"
    (tmp_path / "source-range-evidence.json").write_bytes(_body(evidence))
    with pytest.raises(ValueError, match="independent result"):
        closeout.closeout_closed_attempt(tmp_path, object(), "i-test", query_count=1)
