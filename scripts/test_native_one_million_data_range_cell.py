"""Phase boundaries and plan-seal authority for the 1M range cell."""

import dataclasses
import hashlib
import json

import numpy as np
import pytest

from scripts import native_one_million_data_range_cell as cell
from scripts.native_one_million_data_range_query import plan_query
from scripts.native_one_million_group_selector import MEMBERSHIP_DTYPE, Group
from scripts.native_one_million_page_oracle_cell import MAP_DTYPE
from scripts.native_one_million_range_selector_evaluation import plan_group_ranges


def test_construct_rejects_query_material_before_any_source_work(tmp_path) -> None:
    (tmp_path / "queries.parquet").write_bytes(b"unavailable in source phase")
    with pytest.raises(ValueError, match="source phase"):
        cell.run_construct(tmp_path)


def test_prior_code_authority_rejects_terminal_mismatch(tmp_path, monkeypatch) -> None:
    identities = {
        "codes.bin": cell.PRIOR_CODES,
        "model.bin": cell.PRIOR_MODEL,
        "prior-membership.bin": cell.PRIOR_MEMBERSHIP,
    }
    monkeypatch.setattr(
        cell, "_identity",
        lambda path: {"sha256": identities[path.name][0], "bytes": identities[path.name][1]},
    )
    monkeypatch.setattr(cell, "_prior", lambda root, role: {"artifacts": {}})
    source = {
        "codes": {"sha256": cell.PRIOR_CODES[0], "bytes": cell.PRIOR_CODES[1]},
        "membership": {"sha256": cell.PRIOR_MEMBERSHIP[0], "bytes": cell.PRIOR_MEMBERSHIP[1]},
        "model_identity": dataclasses.asdict(cell.MODEL_IDENTITY),
    }
    with pytest.raises(ValueError, match="terminal identity"):
        cell._check_code_authority(tmp_path, source)


def test_page_lengths_rejects_noncontiguous_object_offsets(tmp_path) -> None:
    generation = {
        "runs": [
            {"kind": "base", "object": {"bytes": 10}, "pages": [{"page": 0, "offset": 0, "bytes": 10}]},
            {"kind": "delta", "object": {"bytes": 8}, "pages": [{"page": 0, "offset": 1, "bytes": 8}]},
        ]
    }
    (tmp_path / "generation.json").write_text(json.dumps(generation))
    with pytest.raises(ValueError, match="page span"):
        cell._page_lengths(tmp_path, np.array([10, 8], dtype=np.uint32))


def test_construct_binds_physical_code_ordinal_to_page(tmp_path, monkeypatch) -> None:
    membership = np.empty(4, dtype=MAP_DTYPE)
    membership["id"] = [5, 7, 9, 11]
    membership["page"] = [0, 1, 0, 1]
    prior_membership = np.empty(4, dtype=MEMBERSHIP_DTYPE)
    prior_membership["id"] = membership["id"]
    prior_membership["group"] = [0, 1, 0, 1]
    (tmp_path / "prior-membership.bin").write_bytes(prior_membership.tobytes())
    (tmp_path / "seal.json").write_bytes(b"{}\n")
    monkeypatch.setattr(
        cell, "_prior_authority",
        lambda root: ({"page_order_sha256": "d" * 64}, {}),
    )
    monkeypatch.setattr(cell, "_check_code_authority", lambda root, source: None)
    monkeypatch.setattr(cell, "construct_page_map", lambda root, **kwargs: None)
    monkeypatch.setattr(
        cell, "_read_map",
        lambda root: (
            {"page_order_sha256": "d" * 64}, (), membership,
            np.array([0, 1], dtype=np.uint32), np.array([10, 8], dtype=np.uint32),
        ),
    )
    monkeypatch.setattr(
        cell, "_physical_ordinals",
        lambda root, identities: ({7: 2, 5: 0, 11: 3, 9: 1}, "d" * 64),
    )
    monkeypatch.setattr(cell, "_page_lengths", lambda root, page_bytes: None)
    cell.run_construct(tmp_path, expected_rows=4, expected_pages=2)
    assert np.fromfile(tmp_path / "row-pages.bin", dtype="<u4").tolist() == [0, 0, 1, 1]


def test_historical_group_plan_mismatch_fails_before_page_ranking() -> None:
    groups = (Group("base", 0, 0, 1, 5, 488),)
    selected, intervals, gets, used = plan_group_ranges(groups, [0])
    prior = {
        "ranked_groups": [0], "selected_groups": list(selected),
        "intervals": [list(item) for item in intervals],
        "projected_code_gets": gets, "projected_code_bytes": used,
    }
    assert cell._checked_group_plan(groups, prior) == selected
    with pytest.raises(ValueError, match="prior group plan"):
        cell._checked_group_plan(groups, {**prior, "selected_groups": []})


def test_plan_rejects_truth_material_before_any_source_work(tmp_path) -> None:
    (tmp_path / "truth.parquet").write_bytes(b"unavailable in plan phase")
    with pytest.raises(ValueError, match="query-only phase"):
        cell.run_plan(tmp_path, tmp_path)


def test_sealed_plans_rejects_tampering(tmp_path, monkeypatch) -> None:
    source_digest = "a" * 64
    monkeypatch.setattr(cell, "_identity", lambda path: {"sha256": source_digest})
    plans = {
        "schema": cell.SCHEMA + "-plans",
        "source_seal_sha256": source_digest,
        "prior_plans_sha256": cell.PRIOR["plans"][1],
        "query_identity": dataclasses.asdict(cell.DEVELOPMENT_IDENTITIES["queries"]),
        "samples": [{"query_ordinal": 0}],
    }
    body = cell._canonical(plans)
    (tmp_path / "range-plans.json").write_bytes(body)
    seal = {
        "schema": cell.SCHEMA + "-plan-seal",
        "plans_sha256": hashlib.sha256(body).hexdigest(),
        "source_seal_sha256": source_digest,
        "query_identity": plans["query_identity"],
    }
    (tmp_path / "range-plan-seal.json").write_bytes(cell._canonical(seal))
    assert cell._sealed_plans(tmp_path, 1) == plans
    (tmp_path / "range-plans.json").write_bytes(body + b" ")
    with pytest.raises(ValueError, match="plan seal"):
        cell._sealed_plans(tmp_path, 1)


def test_evaluate_recounts_a_complete_sealed_query(tmp_path, monkeypatch) -> None:
    membership = np.empty(100, dtype=MAP_DTYPE)
    membership["id"] = np.arange(100)
    membership["page"] = np.repeat(np.arange(2), 50)
    lengths = {"base": (10, 10), "delta": (8,)}
    plan = plan_query(
        np.arange(100, dtype=np.float32), membership["page"],
        np.array([0, 0, 1], dtype=np.uint32), lengths, (0,),
    )
    plans = {"samples": [{"query_ordinal": 0, "candidate": plan, "control": plan}]}
    historical = {"samples": [{"query_ordinal": 0}]}
    monkeypatch.setattr(
        cell, "_read_source",
        lambda root: ((), historical, lengths, np.array([], dtype=np.uint32), np.array([], dtype=np.uint32), membership),
    )
    monkeypatch.setattr(cell, "_sealed_plans", lambda root, query_count: plans)
    monkeypatch.setattr(
        cell, "_query_truth",
        lambda queries, truth, identities, query_count: (
            None, np.arange(100, dtype=np.int64).reshape(1, 100)
        ),
    )
    monkeypatch.setattr(cell, "_identity", lambda path: {"sha256": "a" * 64})
    result = cell.run_evaluate(tmp_path, tmp_path, query_count=1)
    assert result["metrics"]["candidate"]["gt100_hits"] == 100
    assert result["metrics"]["candidate"]["maximum_gets"] == 1
    assert result["decision"] == "data-range-selector-killed"
