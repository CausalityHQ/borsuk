#!/usr/bin/env python3
"""Independent semantic reducer for canonical V100 result bytes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Literal, Mapping, Sequence

import numpy as np

from scripts.v97_row_width_screen import ScreenAuthority
from scripts.v99_ranked_gap_range_router import RankedGapConfig
from scripts.v100_page_residual_range_screen import (
    project_v100_resident_bytes_100m,
)


@dataclass(frozen=True, slots=True)
class V100Rescore:
    """Authenticated independently recomputed V100 decision summary."""

    schema: str
    result_sha256: str
    query_count: int
    classification: str
    control_aggregate: Mapping[str, object]
    residual_aggregate: Mapping[str, object]
    paired: Mapping[str, object]
    projection_total_bytes: int
    status: Literal["verified"]


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )


def _valid_sha256(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _page_tuple(value: object) -> tuple[str, int]:
    if (
        type(value) is not dict
        or set(value) != {"object_role", "ordinal"}
        or value.get("object_role") not in ("base", "delta")
        or type(value.get("ordinal")) is not int
        or value["ordinal"] < 0
    ):
        raise ValueError("V100 page evidence differs")
    return value["object_role"], value["ordinal"]


def _validate_sample(
    sample: object, *, query_ordinal: int, neighbors: int, config: RankedGapConfig
) -> tuple[int, int]:
    keys = {
        "query_ordinal",
        "truth_ids",
        "truth_pages",
        "selected_ranges",
        "selected_pages",
        "hit10_ids",
        "hit_ids",
        "hits10",
        "hits",
        "recall10_ppm",
        "recall100_ppm",
        "gets",
        "bytes",
        "root_evaluations",
        "page_evaluations",
        "scanned_rows",
    }
    if type(sample) is not dict or set(sample) != keys:
        raise ValueError("V100 sample schema differs")
    truth_ids = sample["truth_ids"]
    truth_pages_raw = sample["truth_pages"]
    selected_ranges = sample["selected_ranges"]
    selected_pages_raw = sample["selected_pages"]
    if (
        sample["query_ordinal"] != query_ordinal
        or type(truth_ids) is not list
        or len(truth_ids) != neighbors
        or any(type(row_id) is not int for row_id in truth_ids)
        or len(set(truth_ids)) != neighbors
        or type(truth_pages_raw) is not list
        or len(truth_pages_raw) != neighbors
        or type(selected_ranges) is not list
        or not selected_ranges
        or type(selected_pages_raw) is not list
    ):
        raise ValueError("V100 sample evidence differs")
    truth_pages = tuple(_page_tuple(page) for page in truth_pages_raw)
    selected_pages = tuple(_page_tuple(page) for page in selected_pages_raw)
    if len(set(selected_pages)) != len(selected_pages):
        raise ValueError("V100 selected pages differ")
    derived_pages: list[tuple[str, int]] = []
    encoded_bytes = 0
    previous: tuple[str, int, int] | None = None
    range_keys = {"object_role", "first_page", "last_page", "offset", "bytes"}
    for item in selected_ranges:
        if (
            type(item) is not dict
            or set(item) != range_keys
            or item.get("object_role") not in ("base", "delta")
            or any(
                type(item.get(name)) is not int
                for name in ("first_page", "last_page", "offset", "bytes")
            )
            or item["first_page"] < 0
            or item["last_page"] < item["first_page"]
            or item["offset"] < 0
            or item["bytes"] <= 0
        ):
            raise ValueError("V100 selected range differs")
        current = (item["object_role"], item["first_page"], item["last_page"])
        if previous is not None and previous >= current:
            raise ValueError("V100 selected range order differs")
        previous = current
        derived_pages.extend(
            (item["object_role"], ordinal)
            for ordinal in range(item["first_page"], item["last_page"] + 1)
        )
        encoded_bytes += item["bytes"]
    if tuple(derived_pages) != selected_pages:
        raise ValueError("V100 selected page union differs")
    selected = set(selected_pages)
    hit_ids = tuple(
        row_id
        for row_id, truth_page in zip(truth_ids, truth_pages, strict=True)
        if truth_page in selected
    )
    cutoff = min(10, neighbors)
    hit10_ids = tuple(
        row_id
        for row_id, truth_page in zip(
            truth_ids[:cutoff], truth_pages[:cutoff], strict=True
        )
        if truth_page in selected
    )
    hits = len(hit_ids)
    hits10 = len(hit10_ids)
    if (
        sample["hit_ids"] != list(hit_ids)
        or sample["hit10_ids"] != list(hit10_ids)
        or sample["hits"] != hits
        or sample["hits10"] != hits10
        or sample["recall10_ppm"] != hits10 * 1_000_000 // cutoff
        or sample["recall100_ppm"] != hits * 1_000_000 // neighbors
        or sample["gets"] != len(selected_ranges)
        or sample["bytes"] != encoded_bytes
        or sample["gets"] > config.maximum_gets
        or sample["bytes"] > config.maximum_bytes
        or type(sample["root_evaluations"]) is not int
        or sample["root_evaluations"] <= 0
        or type(sample["page_evaluations"]) is not int
        or sample["page_evaluations"] <= 0
        or type(sample["scanned_rows"]) is not int
        or not 0 < sample["scanned_rows"] <= config.maximum_scanned_rows
    ):
        raise ValueError("V100 sample derivation differs")
    return sample["recall10_ppm"], sample["recall100_ppm"]


def _aggregate(
    recalls: Sequence[tuple[int, int]],
    samples: Sequence[Mapping[str, Any]],
    config: RankedGapConfig,
) -> dict[str, object]:
    if not recalls or len(recalls) != len(samples):
        raise ValueError("V100 aggregate input differs")
    recall10 = [item[0] for item in recalls]
    recall100 = sorted(item[1] for item in recalls)
    p05_index = max(0, (len(recalls) * 5 + 99) // 100 - 1)
    average10 = sum(recall10) // len(recalls)
    average100 = sum(recall100) // len(recalls)
    p05 = recall100[p05_index]
    maximum_gets = max(sample["gets"] for sample in samples)
    maximum_bytes = max(sample["bytes"] for sample in samples)
    return {
        "average_recall10_ppm": average10,
        "average_recall100_ppm": average100,
        "p05_recall100_ppm": p05,
        "maximum_gets": maximum_gets,
        "maximum_bytes": maximum_bytes,
        "quality_gate_passed": average10 >= 960_000
        and average100 >= 975_000
        and p05 >= 900_000,
        "resource_gate_passed": maximum_gets <= config.maximum_gets
        and maximum_bytes <= config.maximum_bytes,
    }


def _bootstrap_matrix(query_count: int, seed: int) -> np.ndarray:
    return np.random.default_rng(seed).integers(
        0, query_count, size=(10_000, query_count), dtype=np.int32
    )


def _paired_interval(
    challenger: Sequence[int], control: Sequence[int], matrix: np.ndarray, *, p05: bool
) -> tuple[int, int]:
    left = np.asarray(challenger, dtype=np.int64)
    right = np.asarray(control, dtype=np.int64)
    differences = np.empty(matrix.shape[0], dtype=np.float64)
    p05_index = max(0, (left.size * 5 + 99) // 100 - 1)
    for start in range(0, matrix.shape[0], 256):
        stop = min(start + 256, matrix.shape[0])
        draws = matrix[start:stop]
        left_draws = left[draws]
        right_draws = right[draws]
        if p05:
            differences[start:stop] = (
                np.partition(left_draws, p05_index, axis=1)[:, p05_index]
                - np.partition(right_draws, p05_index, axis=1)[:, p05_index]
            )
        else:
            differences[start:stop] = (left_draws - right_draws).mean(axis=1)
    lower, upper = np.quantile(differences, [0.025, 0.975], method="nearest")
    return int(np.rint(lower)), int(np.rint(upper))


def rescore_v100_result(
    body: bytes,
    *,
    expected_authority: ScreenAuthority,
    expected_config: RankedGapConfig,
    expected_result_sha256: str,
) -> V100Rescore:
    """Authenticate canonical bytes and independently recompute the V100 decision."""

    if (
        not _valid_sha256(expected_result_sha256)
        or hashlib.sha256(body).hexdigest() != expected_result_sha256
    ):
        raise ValueError("V100 result identity differs")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V100 result JSON differs") from error
    if type(value) is not dict or _canonical_json_bytes(value) != body:
        raise ValueError("V100 result canonical bytes differ")
    expected_keys = {
        "schema",
        "authority",
        "config",
        "query_count",
        "bootstrap_seed",
        "bootstrap_resamples",
        "bootstrap_matrix_sha256",
        "classification",
        "artifact_sha256",
        "page_means_identity",
        "residual_books_identity",
        "residual_codes_identity",
        "control_aggregate",
        "control_samples",
        "residual_aggregate",
        "residual_samples",
        "paired",
        "projection",
    }
    if (
        set(value) != expected_keys
        or value["schema"] != "borsuk-v100-page-residual-range-v1"
        or value["authority"] != asdict(expected_authority)
        or value["config"] != asdict(expected_config)
        or type(value["query_count"]) is not int
        or value["query_count"] <= 0
        or value["bootstrap_seed"] != expected_authority.seed
        or value["bootstrap_resamples"] != 10_000
        or not _valid_sha256(value["artifact_sha256"])
    ):
        raise ValueError("V100 result authority differs")
    query_count = value["query_count"]
    control_samples = value["control_samples"]
    residual_samples = value["residual_samples"]
    if (
        type(control_samples) is not list
        or type(residual_samples) is not list
        or len(control_samples) != query_count
        or len(residual_samples) != query_count
    ):
        raise ValueError("V100 result sample cohort differs")
    neighbors = (
        len(control_samples[0].get("truth_ids", []))
        if type(control_samples[0]) is dict
        else 0
    )
    if neighbors <= 0:
        raise ValueError("V100 result neighbor count differs")
    control_recalls = tuple(
        _validate_sample(
            sample, query_ordinal=index, neighbors=neighbors, config=expected_config
        )
        for index, sample in enumerate(control_samples)
    )
    residual_recalls = tuple(
        _validate_sample(
            sample, query_ordinal=index, neighbors=neighbors, config=expected_config
        )
        for index, sample in enumerate(residual_samples)
    )
    control_aggregate = _aggregate(control_recalls, control_samples, expected_config)
    residual_aggregate = _aggregate(residual_recalls, residual_samples, expected_config)
    if (
        value["control_aggregate"] != control_aggregate
        or value["residual_aggregate"] != residual_aggregate
    ):
        raise ValueError("V100 aggregate differs")
    matrix = _bootstrap_matrix(query_count, expected_authority.seed)
    if (
        hashlib.sha256(matrix.tobytes(order="C")).hexdigest()
        != value["bootstrap_matrix_sha256"]
    ):
        raise ValueError("V100 bootstrap authority differs")
    control10 = tuple(item[0] for item in control_recalls)
    control100 = tuple(item[1] for item in control_recalls)
    residual10 = tuple(item[0] for item in residual_recalls)
    residual100 = tuple(item[1] for item in residual_recalls)
    paired = {
        "name": "page-residual-pq16x8",
        "average_recall10_ppm": list(
            _paired_interval(residual10, control10, matrix, p05=False)
        ),
        "average_recall100_ppm": list(
            _paired_interval(residual100, control100, matrix, p05=False)
        ),
        "p05_recall100_ppm": list(
            _paired_interval(residual100, control100, matrix, p05=True)
        ),
    }
    if value["paired"] != paired:
        raise ValueError("V100 paired evidence differs")
    projection = asdict(
        project_v100_resident_bytes_100m(
            expected_config, dimensions=expected_authority.dimensions
        )
    )
    if value["projection"] != projection:
        raise ValueError("V100 projection differs")
    qualified = (
        residual_aggregate["quality_gate_passed"]
        and residual_aggregate["resource_gate_passed"]
        and projection["eligible"]
        and paired["average_recall100_ppm"][0] >= 0
    )
    classification = (
        "page-residual-qualified" if qualified else "page-residual-rejected"
    )
    if value["classification"] != classification:
        raise ValueError("V100 classification differs")
    return V100Rescore(
        schema="borsuk-v100-page-residual-range-rescore-v1",
        result_sha256=expected_result_sha256,
        query_count=query_count,
        classification=classification,
        control_aggregate=control_aggregate,
        residual_aggregate=residual_aggregate,
        paired=paired,
        projection_total_bytes=projection["total_bytes"],
        status="verified",
    )


def canonical_v100_rescore_bytes(summary: V100Rescore) -> bytes:
    """Serialize a verified V100 reducer receipt canonically."""

    if not isinstance(summary, V100Rescore) or summary.status != "verified":
        raise ValueError("V100 rescore differs")
    return _canonical_json_bytes(asdict(summary))
