#!/usr/bin/env python3
"""Independent semantic reducer for V105 confirmation evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from types import SimpleNamespace
from typing import Literal

from scripts.v97_row_width_screen import ScreenAuthority, ScreenInputs
from scripts.v98_hierarchical_row_router import (
    _hierarchy_evidence,
    build_hierarchy,
    route_hierarchy,
)
from scripts.v99_ranked_gap_range_router_rescore import (
    RescoreAggregate,
    _range_aggregate,
    _range_sample,
)
from scripts.v104_exact_retention_ladder import V104Config, _arm_fence


@dataclass(frozen=True, slots=True)
class V105Rescore:
    """Authenticated independent V105 decision summary."""

    schema: str
    result_sha256: str
    query_count: int
    classification: str
    retained_pages: int
    maximum_scanned_rows: int
    maximum_observed_scanned_rows: int
    aggregate: RescoreAggregate
    status: Literal["verified"]


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def _normalized(value: object) -> object:
    return json.loads(json.dumps(value, allow_nan=False, sort_keys=True))


def _fail(label: str) -> None:
    raise ValueError(f"V105 {label} differs")


def rescore_v105_result(
    body: bytes,
    *,
    expected_inputs: ScreenInputs,
    expected_authority: ScreenAuthority,
    expected_config: V104Config,
    expected_result_sha256: str,
) -> V105Rescore:
    """Authenticate bytes and independently recompute the 768-page arm."""

    if (
        len(expected_result_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_result_sha256)
        or hashlib.sha256(body).hexdigest() != expected_result_sha256
    ):
        _fail("result identity")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V105 result JSON differs") from error
    if type(value) is not dict or _canonical(value) != body:
        _fail("canonical result")
    if set(value) != {
        "schema",
        "authority",
        "config",
        "query_count",
        "hierarchy",
        "classification",
        "arm",
    }:
        _fail("result schema")
    query_count = value["query_count"]
    if (
        value["schema"] != "borsuk-v105-exact-retention-confirmation-v1"
        or value["authority"] != _normalized(asdict(expected_authority))
        or value["config"] != _normalized(asdict(expected_config))
        or type(query_count) is not int
        or query_count != len(expected_inputs.queries)
    ):
        _fail("result authority")

    baseline = expected_config.baseline()
    hierarchy = build_hierarchy(expected_inputs, baseline)
    if value["hierarchy"] != _normalized(asdict(_hierarchy_evidence(hierarchy))):
        _fail("hierarchy authority")
    try:
        registered = next(
            arm for arm in expected_config.arms if arm.retained_pages == 768
        )
    except StopIteration as error:
        raise ValueError("V105 retained arm differs") from error
    arm_value = value["arm"]
    if type(arm_value) is not dict or set(arm_value) != {
        "retained_pages",
        "maximum_scanned_rows",
        "maximum_observed_scanned_rows",
        "aggregate",
        "samples",
    }:
        _fail("arm schema")
    if (
        arm_value["retained_pages"] != 768
        or arm_value["maximum_scanned_rows"] != 196_608
        or type(arm_value["samples"]) is not list
        or len(arm_value["samples"]) != query_count
    ):
        _fail("arm authority")

    baseline_fences = tuple(
        route_hierarchy(query, hierarchy, baseline) for query in expected_inputs.queries
    )
    directory = {
        (key.object_role, key.ordinal): (page.offset, page.encoded_bytes)
        for key, page in expected_inputs.pages.items()
    }
    cap = SimpleNamespace(
        maximum_root_groups=expected_config.maximum_root_groups,
        maximum_exposed_pages=expected_config.maximum_exposed_pages,
        maximum_scanned_rows=registered.maximum_scanned_rows,
        maximum_gets=expected_config.maximum_gets,
        maximum_bytes=expected_config.maximum_bytes,
    )
    samples = []
    for ordinal, (sample_value, baseline_fence) in enumerate(
        zip(arm_value["samples"], baseline_fences, strict=True)
    ):
        fence = _arm_fence(baseline_fence, expected_inputs, registered)
        truth = tuple(int(row_id) for row_id in expected_inputs.truth_ids[ordinal])
        truth_pages = tuple(
            (
                expected_inputs.page_by_id[row_id].object_role,
                expected_inputs.page_by_id[row_id].ordinal,
            )
            for row_id in truth
        )
        retained = tuple(
            (page.object_role, page.ordinal) for page in fence.retained_pages
        )
        try:
            sample = _range_sample(
                sample_value,
                query_ordinal=ordinal,
                config=cap,
                directory=directory,
                expected_truth=(truth, truth_pages),
                expected_fence=(
                    fence.root_evaluations,
                    fence.page_evaluations,
                    fence.scanned_rows,
                ),
                expected_retained_pages=retained,
            )
        except ValueError as error:
            raise ValueError(str(error).replace("V99", "V105", 1)) from error
        samples.append(sample)
    sample_tuple = tuple(samples)
    aggregate = _range_aggregate(sample_tuple, cap)
    maximum_observed = max(sample.scanned_rows for sample in sample_tuple)
    classification = (
        "exact-retention-confirmed"
        if aggregate.quality_gate_passed and aggregate.resource_gate_passed
        else "exact-retention-rejected"
    )
    if (
        arm_value["aggregate"] != _normalized(asdict(aggregate))
        or arm_value["maximum_observed_scanned_rows"] != maximum_observed
        or value["classification"] != classification
    ):
        _fail("decision")
    return V105Rescore(
        schema="borsuk-v105-exact-retention-confirmation-rescore-v1",
        result_sha256=expected_result_sha256,
        query_count=query_count,
        classification=classification,
        retained_pages=768,
        maximum_scanned_rows=196_608,
        maximum_observed_scanned_rows=maximum_observed,
        aggregate=aggregate,
        status="verified",
    )


def canonical_v105_rescore_bytes(receipt: V105Rescore) -> bytes:
    """Serialize one typed verified V105 reducer receipt."""

    if (
        not isinstance(receipt, V105Rescore)
        or receipt.schema != "borsuk-v105-exact-retention-confirmation-rescore-v1"
        or receipt.status != "verified"
        or receipt.retained_pages != 768
        or receipt.maximum_scanned_rows != 196_608
        or len(receipt.result_sha256) != 64
    ):
        _fail("rescore summary")
    return _canonical(asdict(receipt))
