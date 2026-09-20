#!/usr/bin/env python3
"""Independent hostile validator and reducer for V98 evidence."""

from __future__ import annotations

import hashlib
import json
import pathlib
from dataclasses import asdict, dataclass
from typing import Literal

import numpy as np

from scripts.v97_row_width_screen import ScreenAuthority
from scripts.v98_hierarchical_row_router import HierarchyConfig

_ARM_SPECS = {
    "pq16x8": (16, 8, 16, True),
    "pq24x8": (24, 8, 24, True),
    "pq32x8": (32, 8, 32, True),
    "pq32x4": (32, 4, 16, True),
    "summary-only-pq16x8": (16, 8, 0, False),
}
_ARM_ORDER = tuple(_ARM_SPECS)
_HEX = frozenset("0123456789abcdef")


@dataclass(frozen=True, slots=True)
class RescoreAggregate:
    average_recall10_ppm: int
    average_recall100_ppm: int
    p05_recall100_ppm: int
    maximum_gets: int
    maximum_bytes: int
    quality_gate_passed: bool
    resource_gate_passed: bool


@dataclass(frozen=True, slots=True)
class V98RescoreArm:
    name: str
    row_bytes: int
    aggregate: RescoreAggregate
    projection_total_bytes: int
    average_recall10_ppm: tuple[int, int]
    average_recall100_ppm: tuple[int, int]
    p05_recall100_ppm: tuple[int, int]
    absolute_quality: bool
    resource: bool
    memory: bool
    paired_noninferior: bool
    eligible: bool


@dataclass(frozen=True, slots=True)
class V98Rescore:
    schema: str
    result_sha256: str
    query_count: int
    classification: str
    bootstrap_seed: int
    bootstrap_resamples: int
    bootstrap_matrix_sha256: str
    containment_aggregate: RescoreAggregate
    exact_aggregate: RescoreAggregate | None
    arms: tuple[V98RescoreArm, ...]
    winner: str | None


@dataclass(frozen=True, slots=True)
class ValidatedV98Result:
    result_sha256: str
    query_count: int
    classification: str


@dataclass(frozen=True, slots=True)
class _Sample:
    query_ordinal: int
    truth_ids: tuple[int, ...]
    truth_pages: tuple[tuple[str, int], ...]
    selected_pages: tuple[tuple[str, int], ...]
    selected_page_bytes: tuple[int, ...]
    recall10_ppm: int
    recall100_ppm: int
    gets: int
    encoded_bytes: int
    root_evaluations: int
    page_evaluations: int
    scanned_rows: int


def _fail(label: str) -> None:
    raise ValueError(f"V98 {label} differs")


def _mapping(value: object, keys: tuple[str, ...], label: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != set(keys):
        _fail(label)
    return value  # type: ignore[return-value]


def _list(value: object, label: str) -> list[object]:
    if type(value) is not list:
        _fail(label)
    return value  # type: ignore[return-value]


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        _fail(label)
    return value


def _boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        _fail(label)
    return value


def _string(value: object, label: str) -> str:
    if type(value) is not str or not value:
        _fail(label)
    return value


def _digest(value: object, label: str) -> str:
    text = _string(value, label)
    if (
        len(text) != 64
        or any(character not in _HEX for character in text)
        or set(text) == {"0"}
    ):
        _fail(label)
    return text


def _page(value: object, label: str) -> tuple[str, int]:
    item = _mapping(value, ("object_role", "ordinal"), label)
    role = _string(item["object_role"], label)
    ordinal = _integer(item["ordinal"], label)
    if role not in ("base", "delta"):
        _fail(label)
    return role, ordinal


def _pages(value: object, label: str) -> tuple[tuple[str, int], ...]:
    pages = tuple(_page(item, label) for item in _list(value, label))
    if len(set(pages)) != len(pages):
        _fail(label)
    return pages


def _identity(
    value: object,
    label: str,
    *,
    expected_dtype: str | None = None,
    expected_shape: tuple[int, ...] | None = None,
) -> tuple[int, str, tuple[int, ...], str]:
    item = _mapping(value, ("bytes", "dtype", "shape", "sha256"), label)
    byte_count = _integer(item["bytes"], label, minimum=1)
    dtype = _string(item["dtype"], label)
    shape = tuple(
        _integer(part, label, minimum=1) for part in _list(item["shape"], label)
    )
    digest = _digest(item["sha256"], label)
    widths = {"uint8": 1, "uint16": 2, "uint32": 4, "int64": 8, "float32": 4}
    if (
        dtype not in widths
        or byte_count != int(np.prod(shape, dtype=np.int64)) * widths[dtype]
    ):
        _fail(label)
    if expected_dtype is not None and dtype != expected_dtype:
        _fail(label)
    if expected_shape is not None and shape != expected_shape:
        _fail(label)
    return byte_count, dtype, shape, digest


def _aggregate(
    samples: tuple[_Sample, ...], *, resources: bool, config: HierarchyConfig
) -> RescoreAggregate:
    if not samples:
        _fail("samples")
    recall10 = [sample.recall10_ppm for sample in samples]
    recall100 = sorted(sample.recall100_ppm for sample in samples)
    p05_index = max(0, (len(samples) * 5 + 99) // 100 - 1)
    maximum_gets = max(sample.gets for sample in samples)
    maximum_bytes = max(sample.encoded_bytes for sample in samples)
    average10 = sum(recall10) // len(samples)
    average100 = sum(recall100) // len(samples)
    p05 = recall100[p05_index]
    return RescoreAggregate(
        average_recall10_ppm=average10,
        average_recall100_ppm=average100,
        p05_recall100_ppm=p05,
        maximum_gets=maximum_gets,
        maximum_bytes=maximum_bytes,
        quality_gate_passed=average10 >= 960_000
        and average100 >= 975_000
        and p05 >= 900_000,
        resource_gate_passed=(
            not resources
            or (
                maximum_gets <= config.maximum_gets
                and maximum_bytes <= config.maximum_bytes
            )
        ),
    )


def _aggregate_claim(value: object, expected: RescoreAggregate, label: str) -> None:
    item = _mapping(
        value,
        (
            "average_recall10_ppm",
            "average_recall100_ppm",
            "p05_recall100_ppm",
            "maximum_gets",
            "maximum_bytes",
            "quality_gate_passed",
            "resource_gate_passed",
        ),
        label,
    )
    actual = RescoreAggregate(
        average_recall10_ppm=_integer(item["average_recall10_ppm"], label),
        average_recall100_ppm=_integer(item["average_recall100_ppm"], label),
        p05_recall100_ppm=_integer(item["p05_recall100_ppm"], label),
        maximum_gets=_integer(item["maximum_gets"], label),
        maximum_bytes=_integer(item["maximum_bytes"], label),
        quality_gate_passed=_boolean(item["quality_gate_passed"], label),
        resource_gate_passed=_boolean(item["resource_gate_passed"], label),
    )
    if actual != expected:
        _fail(label)


def _sample(
    value: object,
    *,
    query_ordinal: int,
    config: HierarchyConfig,
    containment: bool,
    expected_truth: tuple[tuple[int, ...], tuple[tuple[str, int], ...]] | None,
    expected_fence: tuple[int, int, int] | None,
) -> _Sample:
    common = (
        "query_ordinal",
        "truth_ids",
        "truth_pages",
        "hit_ids",
        "hits10",
        "hits",
        "recall10_ppm",
        "recall100_ppm",
        "root_evaluations",
        "page_evaluations",
        "scanned_rows",
    )
    keys = common + (
        ("retained_pages",)
        if containment
        else ("selected_pages", "selected_page_bytes", "hit10_ids", "gets", "bytes")
    )
    item = _mapping(value, keys, "sample")
    if _integer(item["query_ordinal"], "sample") != query_ordinal:
        _fail("query order")
    truth_ids = tuple(
        _integer(row_id, "truth ids")
        for row_id in _list(item["truth_ids"], "truth ids")
    )
    truth_pages = tuple(
        _page(page, "truth pages") for page in _list(item["truth_pages"], "truth pages")
    )
    if (
        not truth_ids
        or len(truth_ids) != len(truth_pages)
        or len(set(truth_ids)) != len(truth_ids)
    ):
        _fail("truth evidence")
    if expected_truth is not None and (truth_ids, truth_pages) != expected_truth:
        _fail("truth evidence")
    selected_key = "retained_pages" if containment else "selected_pages"
    selected_pages = _pages(item[selected_key], "selected pages")
    selected = set(selected_pages)
    hit_ids = tuple(
        row_id
        for row_id, page in zip(truth_ids, truth_pages, strict=True)
        if page in selected
    )
    cutoff = min(10, len(truth_ids))
    hit10_ids = tuple(
        row_id
        for row_id, page in zip(truth_ids[:cutoff], truth_pages[:cutoff], strict=True)
        if page in selected
    )
    claimed_hits = tuple(
        _integer(row_id, "hit ids") for row_id in _list(item["hit_ids"], "hit ids")
    )
    if claimed_hits != hit_ids or _integer(item["hits"], "hits") != len(hit_ids):
        _fail("hit evidence")
    if _integer(item["hits10"], "hits") != len(hit10_ids):
        _fail("hit evidence")
    if not containment:
        claimed_hit10 = tuple(
            _integer(row_id, "hit ids")
            for row_id in _list(item["hit10_ids"], "hit ids")
        )
        if claimed_hit10 != hit10_ids:
            _fail("hit evidence")
    recall10 = len(hit10_ids) * 1_000_000 // cutoff
    recall100 = len(hit_ids) * 1_000_000 // len(truth_ids)
    if (
        _integer(item["recall10_ppm"], "recall") != recall10
        or _integer(item["recall100_ppm"], "recall") != recall100
    ):
        _fail("recall")
    root_evaluations = _integer(item["root_evaluations"], "work", minimum=1)
    page_evaluations = _integer(item["page_evaluations"], "work", minimum=1)
    scanned_rows = _integer(item["scanned_rows"], "work", minimum=1)
    if (
        root_evaluations > config.maximum_root_groups
        or page_evaluations > config.maximum_exposed_pages
        or scanned_rows > config.maximum_scanned_rows
    ):
        _fail("work caps")
    fence = (root_evaluations, page_evaluations, scanned_rows)
    if expected_fence is not None and fence != expected_fence:
        _fail("shared hierarchy fence")
    if containment:
        selected_page_bytes: tuple[int, ...] = ()
        gets = 0
        encoded_bytes = 0
    else:
        selected_page_bytes = tuple(
            _integer(size, "page bytes", minimum=1)
            for size in _list(item["selected_page_bytes"], "page bytes")
        )
        if len(selected_page_bytes) != len(selected_pages):
            _fail("page bytes")
        gets = len(selected_pages)
        encoded_bytes = sum(selected_page_bytes)
        if (
            _integer(item["gets"], "resources") != gets
            or _integer(item["bytes"], "resources") != encoded_bytes
        ):
            _fail("resources")
        if gets > config.maximum_gets or encoded_bytes > config.maximum_bytes:
            _fail("resource caps")
    return _Sample(
        query_ordinal=query_ordinal,
        truth_ids=truth_ids,
        truth_pages=truth_pages,
        selected_pages=selected_pages,
        selected_page_bytes=selected_page_bytes,
        recall10_ppm=recall10,
        recall100_ppm=recall100,
        gets=gets,
        encoded_bytes=encoded_bytes,
        root_evaluations=root_evaluations,
        page_evaluations=page_evaluations,
        scanned_rows=scanned_rows,
    )


def _base_projection(
    row_bytes: int, *, row_codes_present: bool, subspaces: int, bits: int
) -> dict[str, object]:
    rows = 100_000_000
    pages = 390_625
    dimensions = 768
    values: dict[str, object] = {
        "rows": rows,
        "pages": pages,
        "row_codes_bytes": rows * row_bytes,
        "summary_codes_bytes": pages * 2 * 16,
        "row_codebook_bytes": (1 << bits) * dimensions * 4 if row_codes_present else 0,
        "summary_codebook_bytes": 256 * dimensions * 4,
        "sq8_parameter_bytes": 2 * dimensions * 4,
        "mutation_entries": 1_000_000,
        "mutation_directory_bytes": 1_000_000 * 96,
        "resident_delta_rows": 100_000,
        "resident_delta_bytes": 100_000 * (dimensions + 72),
        "page_directory_reserve_bytes": 128 * 1024**2,
        "response_buffers_bytes": 16 * 16 * 1024**2,
        "planner_workspace_bytes": 128_000_000,
        "runtime_reserve_bytes": 536_870_912,
    }
    values["total_bytes"] = sum(
        value
        for key, value in values.items()
        if key.endswith("_bytes") and key not in ("total_bytes", "budget_bytes")
    )
    values["budget_bytes"] = 3 * 1024**3
    values["eligible"] = values["total_bytes"] < values["budget_bytes"]
    if subspaces <= 0:
        _fail("projection")
    return values


def _projection(name: str, config: HierarchyConfig) -> dict[str, object]:
    subspaces, bits, row_bytes, present = _ARM_SPECS[name]
    base = _base_projection(
        row_bytes, row_codes_present=present, subspaces=subspaces, bits=bits
    )
    pages = int(base["pages"])
    roots = (pages + config.pages_per_root - 1) // config.pages_per_root
    page_summary_bytes = pages * 2 * 16
    extra = {
        "root_summary_bytes": roots * 2 * 16,
        "page_to_root_bytes": pages * 4,
        "root_child_bytes": roots * 6,
        "row_code_offsets_bytes": (pages + 2) * 8,
        "root_scores_workspace_bytes": roots * 4,
        "page_scores_workspace_bytes": config.maximum_exposed_pages * 4,
        "row_scores_workspace_bytes": config.maximum_scanned_rows * 4,
        "shortlist_workspace_bytes": config.shortlist_rows * 12,
    }
    hierarchy_additional = sum(extra.values())
    total = int(base["total_bytes"]) + hierarchy_additional
    return {
        "base": base,
        "root_groups": roots,
        "page_summary_bytes": page_summary_bytes,
        **extra,
        "hierarchy_additional_bytes": hierarchy_additional,
        "total_bytes": total,
        "budget_bytes": int(base["budget_bytes"]),
        "eligible": total < int(base["budget_bytes"]),
    }


def _projection_claim(value: object, expected: dict[str, object]) -> None:
    item = _mapping(value, tuple(expected), "projection")
    base_expected = expected["base"]
    assert isinstance(base_expected, dict)
    base = _mapping(item["base"], tuple(base_expected), "projection")
    for key, wanted in base_expected.items():
        actual = (
            _boolean(base[key], "projection")
            if type(wanted) is bool
            else _integer(base[key], "projection")
        )
        if actual != wanted:
            _fail("projection")
    for key, wanted in expected.items():
        if key == "base":
            continue
        actual = (
            _boolean(item[key], "projection")
            if type(wanted) is bool
            else _integer(item[key], "projection")
        )
        if actual != wanted:
            _fail("projection")


def _paired_interval(
    left: tuple[int, ...],
    right: tuple[int, ...],
    matrix: np.ndarray,
    *,
    statistic: Literal["mean", "p05"],
) -> tuple[int, int]:
    challenger = np.asarray(left, dtype=np.int64)
    control = np.asarray(right, dtype=np.int64)
    differences = np.empty(matrix.shape[0], dtype=np.float64)
    p05_index = max(0, (challenger.size * 5 + 99) // 100 - 1)
    for start in range(0, matrix.shape[0], 256):
        stop = min(start + 256, matrix.shape[0])
        draws = matrix[start:stop]
        left_draws = challenger[draws]
        right_draws = control[draws]
        if statistic == "mean":
            differences[start:stop] = (left_draws - right_draws).mean(axis=1)
        else:
            differences[start:stop] = (
                np.partition(left_draws, p05_index, axis=1)[:, p05_index]
                - np.partition(right_draws, p05_index, axis=1)[:, p05_index]
            )
    lower, upper = np.quantile(differences, [0.025, 0.975], method="nearest")
    return int(np.rint(lower)), int(np.rint(upper))


def _interval_claim(value: object, expected: tuple[int, int], label: str) -> None:
    values = _list(value, label)
    if (
        len(values) != 2
        or tuple(_integer(item, label, minimum=-1_000_000) for item in values)
        != expected
    ):
        _fail(label)


def _load_and_rescore(
    path: pathlib.Path,
    registered_sha256: str,
    *,
    expected_authority: ScreenAuthority,
    expected_config: HierarchyConfig,
) -> V98Rescore:
    body = pathlib.Path(path).read_bytes()
    if _digest(registered_sha256, "registered SHA-256") != hashlib.sha256(
        body
    ).hexdigest() or not body.endswith(b"\n"):
        _fail("result bytes")
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V98 result bytes differ") from error
    if (
        json.dumps(
            document, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
        != body
    ):
        _fail("canonical result")
    top = _mapping(
        document,
        (
            "schema",
            "authority",
            "config",
            "query_count",
            "bootstrap_seed",
            "bootstrap_resamples",
            "bootstrap_matrix_sha256",
            "classification",
            "hierarchy",
            "containment_aggregate",
            "containment_samples",
            "exact_aggregate",
            "exact_samples",
            "arms",
            "paired_intervals",
            "eligibility",
            "winner",
        ),
        "result schema",
    )
    if top["schema"] != "borsuk-v98-hierarchical-row-router-v1":
        _fail("result schema")
    if top["authority"] != json.loads(
        json.dumps(asdict(expected_authority), sort_keys=True)
    ):
        _fail("authority")
    if top["config"] != json.loads(json.dumps(asdict(expected_config), sort_keys=True)):
        _fail("configuration")
    query_count = _integer(top["query_count"], "query count", minimum=1)
    seed = _integer(top["bootstrap_seed"], "bootstrap", minimum=1)
    resamples = _integer(top["bootstrap_resamples"], "bootstrap", minimum=1)
    if seed != expected_authority.seed or resamples != 10_000:
        _fail("bootstrap")
    matrix = np.random.default_rng(seed).integers(
        0, query_count, size=(resamples, query_count), dtype=np.int32
    )
    matrix_sha = hashlib.sha256(matrix.tobytes(order="C")).hexdigest()
    if _digest(top["bootstrap_matrix_sha256"], "bootstrap matrix") != matrix_sha:
        _fail("bootstrap matrix")

    hierarchy = _mapping(
        top["hierarchy"],
        (
            "roots",
            "pages",
            "summary_books",
            "page_summary_codes",
            "root_summary_codes",
            "page_row_counts",
            "ipc_sha256",
            "ipc_bytes",
        ),
        "hierarchy",
    )
    roots = _integer(hierarchy["roots"], "hierarchy", minimum=1)
    pages = _integer(hierarchy["pages"], "hierarchy", minimum=1)
    if (
        roots > expected_config.maximum_root_groups
        or pages > roots * expected_config.pages_per_root
    ):
        _fail("hierarchy")
    dimensions = expected_authority.dimensions
    _identity(
        hierarchy["summary_books"],
        "hierarchy identity",
        expected_dtype="float32",
        expected_shape=(16, 256, dimensions // 16),
    )
    _identity(
        hierarchy["page_summary_codes"],
        "hierarchy identity",
        expected_dtype="uint8",
        expected_shape=(pages * 2, 16),
    )
    _identity(
        hierarchy["root_summary_codes"],
        "hierarchy identity",
        expected_dtype="uint8",
        expected_shape=(roots * 2, 16),
    )
    _identity(
        hierarchy["page_row_counts"],
        "hierarchy identity",
        expected_dtype="uint16",
        expected_shape=(pages,),
    )
    _digest(hierarchy["ipc_sha256"], "hierarchy IPC")
    _integer(hierarchy["ipc_bytes"], "hierarchy IPC", minimum=1)

    containment_values = _list(top["containment_samples"], "containment samples")
    if len(containment_values) != query_count:
        _fail("query cardinality")
    containment: list[_Sample] = []
    truths: list[tuple[tuple[int, ...], tuple[tuple[str, int], ...]]] = []
    fences: list[tuple[int, int, int]] = []
    for ordinal, value in enumerate(containment_values):
        sample = _sample(
            value,
            query_ordinal=ordinal,
            config=expected_config,
            containment=True,
            expected_truth=None,
            expected_fence=None,
        )
        containment.append(sample)
        truths.append((sample.truth_ids, sample.truth_pages))
        fences.append(
            (sample.root_evaluations, sample.page_evaluations, sample.scanned_rows)
        )
    containment_tuple = tuple(containment)
    containment_aggregate = _aggregate(
        containment_tuple, resources=False, config=expected_config
    )
    _aggregate_claim(
        top["containment_aggregate"], containment_aggregate, "containment aggregate"
    )

    exact_values = _list(top["exact_samples"], "exact samples")
    exact_samples: tuple[_Sample, ...] = ()
    exact_aggregate: RescoreAggregate | None = None
    if exact_values:
        if len(exact_values) != query_count:
            _fail("query cardinality")
        exact_samples = tuple(
            _sample(
                value,
                query_ordinal=ordinal,
                config=expected_config,
                containment=False,
                expected_truth=truths[ordinal],
                expected_fence=fences[ordinal],
            )
            for ordinal, value in enumerate(exact_values)
        )
        exact_aggregate = _aggregate(
            exact_samples, resources=True, config=expected_config
        )
        _aggregate_claim(top["exact_aggregate"], exact_aggregate, "exact aggregate")
    elif top["exact_aggregate"] is not None:
        _fail("exact aggregate")

    classification = _string(top["classification"], "classification")
    arm_values = _list(top["arms"], "arms")
    if not containment_aggregate.quality_gate_passed:
        expected_classification = "hierarchy-containment-rejected"
        if exact_values or arm_values:
            _fail("fail-fast classification")
    elif exact_aggregate is None or not (
        exact_aggregate.quality_gate_passed and exact_aggregate.resource_gate_passed
    ):
        expected_classification = "hierarchy-exact-ceiling-rejected"
        if exact_aggregate is None or arm_values:
            _fail("fail-fast classification")
    else:
        expected_classification = "widths-evaluated"
        if len(arm_values) != len(_ARM_ORDER):
            _fail("arms")
    if classification != expected_classification:
        _fail("classification")

    arm_samples: dict[str, tuple[_Sample, ...]] = {}
    arm_aggregates: dict[str, RescoreAggregate] = {}
    projection_by_name: dict[str, dict[str, object]] = {}
    row_counts: int | None = None
    for arm_index, value in enumerate(arm_values):
        arm = _mapping(
            value,
            (
                "name",
                "row_bytes",
                "codebook_identity",
                "codes_identity",
                "projection",
                "aggregate",
                "samples",
            ),
            "arm",
        )
        name = _string(arm["name"], "arm")
        if name != _ARM_ORDER[arm_index]:
            _fail("arm order")
        subspaces, bits, row_bytes, present = _ARM_SPECS[name]
        if _integer(arm["row_bytes"], "arm") != row_bytes:
            _fail("arm")
        if present:
            centroids = 1 << bits
            _identity(
                arm["codebook_identity"],
                "codebook identity",
                expected_dtype="float32",
                expected_shape=(subspaces, centroids, dimensions // subspaces),
            )
            _, _, code_shape, _ = _identity(
                arm["codes_identity"], "codes identity", expected_dtype="uint8"
            )
            if len(code_shape) != 2 or code_shape[1] != row_bytes:
                _fail("codes identity")
            if row_counts is None:
                row_counts = code_shape[0]
            elif row_counts != code_shape[0]:
                _fail("codes identity")
        elif arm["codebook_identity"] is not None or arm["codes_identity"] is not None:
            _fail("summary-only identity")
        expected_projection = _projection(name, expected_config)
        _projection_claim(arm["projection"], expected_projection)
        projection_by_name[name] = expected_projection
        sample_values = _list(arm["samples"], "arm samples")
        if len(sample_values) != query_count:
            _fail("query cardinality")
        samples = tuple(
            _sample(
                sample_value,
                query_ordinal=ordinal,
                config=expected_config,
                containment=False,
                expected_truth=truths[ordinal],
                expected_fence=fences[ordinal],
            )
            for ordinal, sample_value in enumerate(sample_values)
        )
        for ordinal, sample in enumerate(samples):
            retained = set(containment_tuple[ordinal].selected_pages)
            if not set(sample.selected_pages).issubset(retained):
                _fail("shared hierarchy fence")
        aggregate = _aggregate(samples, resources=True, config=expected_config)
        _aggregate_claim(arm["aggregate"], aggregate, "arm aggregate")
        arm_samples[name] = samples
        arm_aggregates[name] = aggregate

    paired_values = _list(top["paired_intervals"], "paired intervals")
    eligibility_values = _list(top["eligibility"], "eligibility")
    rescored_arms: list[V98RescoreArm] = []
    if arm_values:
        if len(paired_values) != len(_ARM_ORDER) or len(eligibility_values) != len(
            _ARM_ORDER
        ):
            _fail("decision cardinality")
        control = arm_samples["pq16x8"]
        control10 = tuple(sample.recall10_ppm for sample in control)
        control100 = tuple(sample.recall100_ppm for sample in control)
        assert exact_aggregate is not None
        for index, name in enumerate(_ARM_ORDER):
            samples = arm_samples[name]
            recall10 = tuple(sample.recall10_ppm for sample in samples)
            recall100 = tuple(sample.recall100_ppm for sample in samples)
            average10_interval = _paired_interval(
                recall10, control10, matrix, statistic="mean"
            )
            average100_interval = _paired_interval(
                recall100, control100, matrix, statistic="mean"
            )
            p05_interval = _paired_interval(
                recall100, control100, matrix, statistic="p05"
            )
            paired_claim = _mapping(
                paired_values[index],
                (
                    "name",
                    "average_recall10_ppm",
                    "average_recall100_ppm",
                    "p05_recall100_ppm",
                ),
                "paired interval",
            )
            if paired_claim["name"] != name:
                _fail("paired interval")
            _interval_claim(
                paired_claim["average_recall10_ppm"],
                average10_interval,
                "paired interval",
            )
            _interval_claim(
                paired_claim["average_recall100_ppm"],
                average100_interval,
                "paired interval",
            )
            _interval_claim(
                paired_claim["p05_recall100_ppm"], p05_interval, "paired interval"
            )
            aggregate = arm_aggregates[name]
            absolute_quality = (
                exact_aggregate.quality_gate_passed and aggregate.quality_gate_passed
            )
            resource = (
                exact_aggregate.resource_gate_passed and aggregate.resource_gate_passed
            )
            memory = bool(projection_by_name[name]["eligible"])
            paired_noninferior = average100_interval[1] >= 0
            eligible = (
                name != "summary-only-pq16x8"
                and absolute_quality
                and resource
                and memory
                and paired_noninferior
            )
            eligibility_claim = _mapping(
                eligibility_values[index],
                (
                    "name",
                    "absolute_quality",
                    "resource",
                    "memory",
                    "paired_noninferior",
                    "eligible",
                ),
                "eligibility",
            )
            claimed = (
                _string(eligibility_claim["name"], "eligibility"),
                _boolean(eligibility_claim["absolute_quality"], "eligibility"),
                _boolean(eligibility_claim["resource"], "eligibility"),
                _boolean(eligibility_claim["memory"], "eligibility"),
                _boolean(eligibility_claim["paired_noninferior"], "eligibility"),
                _boolean(eligibility_claim["eligible"], "eligibility"),
            )
            if claimed != (
                name,
                absolute_quality,
                resource,
                memory,
                paired_noninferior,
                eligible,
            ):
                _fail("eligibility")
            rescored_arms.append(
                V98RescoreArm(
                    name=name,
                    row_bytes=_ARM_SPECS[name][2],
                    aggregate=aggregate,
                    projection_total_bytes=int(projection_by_name[name]["total_bytes"]),
                    average_recall10_ppm=average10_interval,
                    average_recall100_ppm=average100_interval,
                    p05_recall100_ppm=p05_interval,
                    absolute_quality=absolute_quality,
                    resource=resource,
                    memory=memory,
                    paired_noninferior=paired_noninferior,
                    eligible=eligible,
                )
            )
    elif paired_values or eligibility_values:
        _fail("fail-fast decisions")

    candidates = [arm for arm in rescored_arms if arm.eligible]
    winner = (
        min(
            candidates,
            key=lambda arm: (
                arm.row_bytes,
                -arm.aggregate.p05_recall100_ppm,
                -arm.aggregate.average_recall10_ppm,
                arm.name,
            ),
        ).name
        if candidates
        else None
    )
    if top["winner"] != winner:
        _fail("winner")
    return V98Rescore(
        schema="borsuk-v98-hierarchical-row-router-rescore-v1",
        result_sha256=registered_sha256,
        query_count=query_count,
        classification=classification,
        bootstrap_seed=seed,
        bootstrap_resamples=resamples,
        bootstrap_matrix_sha256=matrix_sha,
        containment_aggregate=containment_aggregate,
        exact_aggregate=exact_aggregate,
        arms=tuple(rescored_arms),
        winner=winner,
    )


def validate_v98_result(
    path: pathlib.Path,
    registered_sha256: str,
    *,
    expected_authority: ScreenAuthority,
    expected_config: HierarchyConfig,
) -> ValidatedV98Result:
    """Authenticate and independently validate one hostile V98 result."""

    summary = _load_and_rescore(
        path,
        registered_sha256,
        expected_authority=expected_authority,
        expected_config=expected_config,
    )
    return ValidatedV98Result(
        result_sha256=summary.result_sha256,
        query_count=summary.query_count,
        classification=summary.classification,
    )


def rescore_v98_result(
    path: pathlib.Path,
    registered_sha256: str,
    *,
    expected_authority: ScreenAuthority,
    expected_config: HierarchyConfig,
) -> V98Rescore:
    """Return independently recomputed V98 gates, intervals, and winner."""

    return _load_and_rescore(
        path,
        registered_sha256,
        expected_authority=expected_authority,
        expected_config=expected_config,
    )


def canonical_v98_rescore_bytes(summary: V98Rescore) -> bytes:
    """Serialize an independently produced rescore receipt canonically."""

    if (
        not isinstance(summary, V98Rescore)
        or summary.schema != "borsuk-v98-hierarchical-row-router-rescore-v1"
    ):
        _fail("rescore")
    return (
        json.dumps(
            asdict(summary), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
