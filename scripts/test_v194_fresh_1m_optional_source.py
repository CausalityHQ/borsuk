"""Frozen V194 decision and tail accounting checks."""

from scripts.v194_fresh_1m_optional_source import (
    _qualifies, _summary, decide, panel,
)
from scripts.v166_surrogate_probe import select_pseudoqueries

import numpy as np


def _cell(hits=50_979, bytes_read=5_700_611_604, gets=11_328):
    return {"hits": hits, "p05_hits": 98, "coverage": hits,
            "bytes": bytes_read, "gets": gets, "infeasible": 0}


def test_preregistered_policy_frontier_decision():
    optional = _cell(bytes_read=4_000_000_000, gets=8_000)
    full = _cell(hits=51_005)
    assert _qualifies(optional)
    assert decide({"optional_risk": optional,
                   "full_rank": full}) == "advance-optional-to-live-s3"
    full["hits"] += 1
    assert decide({"optional_risk": optional,
                   "full_rank": full}) == "advance-full-rank-to-live-s3"
    optional["hits"] = 50_978
    assert decide({"optional_risk": optional,
                   "full_rank": full}) == "advance-full-rank-to-live-s3"
    full["gets"] = 11_329
    assert decide({"optional_risk": optional,
                   "full_rank": full}) == "revise-representation-allocation-or-serving"


def test_nearest_rank_tail_and_hash_panel_are_fixed():
    values = {"hits": [97] * 25 + [98] * 487,
              "coverage": [100] * 512,
              "bytes": [24_960] * 512, "gets": [1] * 512}
    result = _summary(values, 0)
    assert result["p05_hits"] == 98
    assert result["below_98"] == 25
    assert 0 < result["below_98_wilson95"][0] < result["below_98_wilson95"][1] < 1
    source_ids = np.arange(10_000, dtype=np.int64)
    selected = panel(source_ids)
    assert len(selected) == 512 and len(set(selected)) == 512
    ranked = select_pseudoqueries(source_ids, 3_456)
    assert selected == ranked[2_944:]
    assert not set(selected) & set(ranked[2_688:2_944])
