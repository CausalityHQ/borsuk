#!/usr/bin/env python3
"""Independent hostile validator and reducer for V99 range evidence."""

from __future__ import annotations

import hashlib
import json
import pathlib
from dataclasses import asdict, dataclass

import numpy as np

from scripts.v97_row_width_screen import ScreenAuthority
from scripts.v98_hierarchical_row_router_rescore import (
    RescoreAggregate,
    _aggregate,
    _aggregate_claim,
    _boolean,
    _digest,
    _identity,
    _integer,
    _interval_claim,
    _list,
    _mapping,
    _page,
    _paired_interval,
    _projection,
    _projection_claim,
    _sample,
    _string,
)

_ARM_SPECS = {
    "pq16x8": (16, 8, 16, True),
    "pq24x8": (24, 8, 24, True),
    "pq32x8": (32, 8, 32, True),
    "pq32x4": (32, 4, 16, True),
    "summary-only-pq16x8": (16, 8, 0, False),
}
_ARM_ORDER = tuple(_ARM_SPECS)


@dataclass(frozen=True, slots=True)
class V99RescoreArm:
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
class V99Rescore:
    schema: str
    result_sha256: str
    query_count: int
    classification: str
    bootstrap_seed: int
    bootstrap_resamples: int
    bootstrap_matrix_sha256: str
    containment_aggregate: RescoreAggregate
    exact_aggregate: RescoreAggregate | None
    arms: tuple[V99RescoreArm, ...]
    winner: str | None


@dataclass(frozen=True, slots=True)
class ValidatedV99Result:
    result_sha256: str
    query_count: int
    classification: str


@dataclass(frozen=True, slots=True)
class _RangeSample:
    query_ordinal: int
    truth_ids: tuple[int, ...]
    truth_pages: tuple[tuple[str, int], ...]
    selected_pages: tuple[tuple[str, int], ...]
    recall10_ppm: int
    recall100_ppm: int
    gets: int
    encoded_bytes: int
    root_evaluations: int
    page_evaluations: int
    scanned_rows: int


def _fail(label: str) -> None:
    raise ValueError(f"V99 {label} differs")


def _page_directory(value: object) -> dict[tuple[str, int], tuple[int, int]]:
    entries = _list(value, "page directory")
    directory: dict[tuple[str, int], tuple[int, int]] = {}
    previous: tuple[str, int] | None = None
    for value in entries:
        item = _mapping(
            value, ("object_role", "ordinal", "offset", "bytes"), "page directory"
        )
        key = _page(
            {"object_role": item["object_role"], "ordinal": item["ordinal"]},
            "page directory",
        )
        offset = _integer(item["offset"], "page directory")
        byte_count = _integer(item["bytes"], "page directory", minimum=1)
        if key in directory or (previous is not None and key <= previous):
            _fail("page directory")
        if key[1] and (key[0], key[1] - 1) not in directory:
            _fail("page directory")
        if key[1]:
            left_offset, left_bytes = directory[(key[0], key[1] - 1)]
            if offset != left_offset + left_bytes:
                _fail("page directory contiguity")
        directory[key] = (offset, byte_count)
        previous = key
    if not directory:
        _fail("page directory")
    return directory


def _range_sample(
    value: object,
    *,
    query_ordinal: int,
    config: object,
    directory: dict[tuple[str, int], tuple[int, int]],
    expected_truth: tuple[tuple[int, ...], tuple[tuple[str, int], ...]],
    expected_fence: tuple[int, int, int],
    expected_retained_pages: tuple[tuple[str, int], ...],
) -> _RangeSample:
    item = _mapping(
        value,
        (
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
        ),
        "range sample",
    )
    if _integer(item["query_ordinal"], "query order") != query_ordinal:
        _fail("query order")
    truth_ids = tuple(
        _integer(row_id, "truth ids")
        for row_id in _list(item["truth_ids"], "truth ids")
    )
    truth_pages = tuple(
        _page(page, "truth pages") for page in _list(item["truth_pages"], "truth pages")
    )
    if (truth_ids, truth_pages) != expected_truth:
        _fail("truth evidence")

    selected_pages: list[tuple[str, int]] = []
    retained_pages = set(expected_retained_pages)
    ranges = _list(item["selected_ranges"], "selected ranges")
    previous: tuple[str, int, int] | None = None
    total_bytes = 0
    for value in ranges:
        entry = _mapping(
            value,
            ("object_role", "first_page", "last_page", "offset", "bytes"),
            "selected range",
        )
        role = _string(entry["object_role"], "selected range")
        first = _integer(entry["first_page"], "selected range")
        last = _integer(entry["last_page"], "selected range")
        if role not in ("base", "delta") or last < first:
            _fail("selected range")
        order = (role, first, last)
        if previous is not None and (
            order <= previous or (role == previous[0] and first <= previous[2])
        ):
            _fail("selected range order")
        pages = [(role, ordinal) for ordinal in range(first, last + 1)]
        if (
            any(page not in directory for page in pages)
            or pages[0] not in retained_pages
            or pages[-1] not in retained_pages
        ):
            if pages[0] not in retained_pages or pages[-1] not in retained_pages:
                _fail("shared hierarchy fence")
            _fail("selected range page")
        offset = directory[pages[0]][0]
        final_offset, final_bytes = directory[pages[-1]]
        byte_count = final_offset + final_bytes - offset
        if (
            _integer(entry["offset"], "selected range") != offset
            or _integer(entry["bytes"], "selected range", minimum=1) != byte_count
        ):
            _fail("selected range bytes")
        selected_pages.extend(pages)
        total_bytes += byte_count
        previous = order
    claimed_pages = tuple(
        _page(page, "selected pages")
        for page in _list(item["selected_pages"], "selected pages")
    )
    if not ranges or claimed_pages != tuple(selected_pages):
        _fail("selected page union")
    selected = set(selected_pages)
    cutoff = min(10, len(truth_ids))
    hit10_ids = tuple(
        row_id
        for row_id, page in zip(truth_ids[:cutoff], truth_pages[:cutoff], strict=True)
        if page in selected
    )
    hit_ids = tuple(
        row_id
        for row_id, page in zip(truth_ids, truth_pages, strict=True)
        if page in selected
    )
    claimed_hit10 = tuple(
        _integer(row_id, "hit ids") for row_id in _list(item["hit10_ids"], "hit ids")
    )
    claimed_hits = tuple(
        _integer(row_id, "hit ids") for row_id in _list(item["hit_ids"], "hit ids")
    )
    recall10 = len(hit10_ids) * 1_000_000 // cutoff
    recall100 = len(hit_ids) * 1_000_000 // len(truth_ids)
    gets = len(ranges)
    if (
        claimed_hit10 != hit10_ids
        or claimed_hits != hit_ids
        or _integer(item["hits10"], "hits") != len(hit10_ids)
        or _integer(item["hits"], "hits") != len(hit_ids)
        or _integer(item["recall10_ppm"], "recall") != recall10
        or _integer(item["recall100_ppm"], "recall") != recall100
        or _integer(item["gets"], "resources") != gets
        or _integer(item["bytes"], "resources") != total_bytes
    ):
        _fail("range sample evidence")
    root_evaluations = _integer(item["root_evaluations"], "work", minimum=1)
    page_evaluations = _integer(item["page_evaluations"], "work", minimum=1)
    scanned_rows = _integer(item["scanned_rows"], "work", minimum=1)
    if (
        (root_evaluations, page_evaluations, scanned_rows) != expected_fence
        or root_evaluations > config.maximum_root_groups
        or page_evaluations > config.maximum_exposed_pages
        or scanned_rows > config.maximum_scanned_rows
        or gets > config.maximum_gets
        or total_bytes > config.maximum_bytes
    ):
        _fail("work or resource caps")
    return _RangeSample(
        query_ordinal=query_ordinal,
        truth_ids=truth_ids,
        truth_pages=truth_pages,
        selected_pages=tuple(selected_pages),
        recall10_ppm=recall10,
        recall100_ppm=recall100,
        gets=gets,
        encoded_bytes=total_bytes,
        root_evaluations=root_evaluations,
        page_evaluations=page_evaluations,
        scanned_rows=scanned_rows,
    )


def _range_aggregate(
    samples: tuple[_RangeSample, ...], config: object
) -> RescoreAggregate:
    if not samples:
        _fail("samples")
    recall10 = [sample.recall10_ppm for sample in samples]
    recall100 = sorted(sample.recall100_ppm for sample in samples)
    p05_index = max(0, (len(samples) * 5 + 99) // 100 - 1)
    average10 = sum(recall10) // len(samples)
    average100 = sum(recall100) // len(samples)
    p05 = recall100[p05_index]
    maximum_gets = max(sample.gets for sample in samples)
    maximum_bytes = max(sample.encoded_bytes for sample in samples)
    return RescoreAggregate(
        average_recall10_ppm=average10,
        average_recall100_ppm=average100,
        p05_recall100_ppm=p05,
        maximum_gets=maximum_gets,
        maximum_bytes=maximum_bytes,
        quality_gate_passed=average10 >= 960_000
        and average100 >= 975_000
        and p05 >= 900_000,
        resource_gate_passed=maximum_gets <= config.maximum_gets
        and maximum_bytes <= config.maximum_bytes,
    )


def _load_and_rescore(
    path: pathlib.Path,
    registered_sha256: str,
    *,
    expected_authority: ScreenAuthority,
    expected_config: object,
) -> V99Rescore:
    body = pathlib.Path(path).read_bytes()
    if _digest(registered_sha256, "registered SHA-256") != hashlib.sha256(
        body
    ).hexdigest() or not body.endswith(b"\n"):
        _fail("result bytes")
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V99 result bytes differ") from error
    canonical = (
        json.dumps(
            document, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
    if canonical != body:
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
            "page_directory",
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
    if top["schema"] != "borsuk-v99-ranked-gap-range-router-v1":
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
    directory = _page_directory(top["page_directory"])
    if len(directory) != pages:
        _fail("page directory cardinality")

    containment_values = _list(top["containment_samples"], "containment samples")
    if len(containment_values) != query_count:
        _fail("query cardinality")
    containment = []
    truths = []
    fences = []
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
    exact_samples: tuple[_RangeSample, ...] = ()
    exact_aggregate: RescoreAggregate | None = None
    if exact_values:
        if len(exact_values) != query_count:
            _fail("query cardinality")
        exact_samples = tuple(
            _range_sample(
                value,
                query_ordinal=ordinal,
                config=expected_config,
                directory=directory,
                expected_truth=truths[ordinal],
                expected_fence=fences[ordinal],
                expected_retained_pages=containment_tuple[ordinal].selected_pages,
            )
            for ordinal, value in enumerate(exact_values)
        )
        exact_aggregate = _range_aggregate(exact_samples, expected_config)
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
        expected_classification = "range-exact-ceiling-rejected"
        if exact_aggregate is None or arm_values:
            _fail("fail-fast classification")
    else:
        expected_classification = "widths-evaluated"
        if len(arm_values) != len(_ARM_ORDER):
            _fail("arms")
    if classification != expected_classification:
        _fail("classification")

    arm_samples: dict[str, tuple[_RangeSample, ...]] = {}
    arm_aggregates: dict[str, RescoreAggregate] = {}
    projection_by_name: dict[str, dict[str, object]] = {}
    row_count: int | None = None
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
            if row_count is None:
                row_count = code_shape[0]
            elif row_count != code_shape[0]:
                _fail("codes identity")
        elif arm["codebook_identity"] is not None or arm["codes_identity"] is not None:
            _fail("summary-only identity")
        projection = _projection(name, expected_config)
        _projection_claim(arm["projection"], projection)
        projection_by_name[name] = projection
        samples_values = _list(arm["samples"], "arm samples")
        if len(samples_values) != query_count:
            _fail("query cardinality")
        samples = tuple(
            _range_sample(
                sample_value,
                query_ordinal=ordinal,
                config=expected_config,
                directory=directory,
                expected_truth=truths[ordinal],
                expected_fence=fences[ordinal],
                expected_retained_pages=containment_tuple[ordinal].selected_pages,
            )
            for ordinal, sample_value in enumerate(samples_values)
        )
        aggregate = _range_aggregate(samples, expected_config)
        _aggregate_claim(arm["aggregate"], aggregate, "arm aggregate")
        arm_samples[name] = samples
        arm_aggregates[name] = aggregate

    paired_values = _list(top["paired_intervals"], "paired intervals")
    eligibility_values = _list(top["eligibility"], "eligibility")
    rescored_arms: list[V99RescoreArm] = []
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
            intervals = (
                _paired_interval(recall10, control10, matrix, statistic="mean"),
                _paired_interval(recall100, control100, matrix, statistic="mean"),
                _paired_interval(recall100, control100, matrix, statistic="p05"),
            )
            claim = _mapping(
                paired_values[index],
                (
                    "name",
                    "average_recall10_ppm",
                    "average_recall100_ppm",
                    "p05_recall100_ppm",
                ),
                "paired interval",
            )
            if claim["name"] != name:
                _fail("paired interval")
            for key, interval in zip(
                ("average_recall10_ppm", "average_recall100_ppm", "p05_recall100_ppm"),
                intervals,
                strict=True,
            ):
                _interval_claim(claim[key], interval, "paired interval")
            aggregate = arm_aggregates[name]
            absolute_quality = (
                exact_aggregate.quality_gate_passed and aggregate.quality_gate_passed
            )
            resource = (
                exact_aggregate.resource_gate_passed and aggregate.resource_gate_passed
            )
            memory = bool(projection_by_name[name]["eligible"])
            paired_noninferior = intervals[1][1] >= 0
            eligible = (
                name != "summary-only-pq16x8"
                and absolute_quality
                and resource
                and memory
                and paired_noninferior
            )
            eligibility = _mapping(
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
                _string(eligibility["name"], "eligibility"),
                _boolean(eligibility["absolute_quality"], "eligibility"),
                _boolean(eligibility["resource"], "eligibility"),
                _boolean(eligibility["memory"], "eligibility"),
                _boolean(eligibility["paired_noninferior"], "eligibility"),
                _boolean(eligibility["eligible"], "eligibility"),
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
                V99RescoreArm(
                    name=name,
                    row_bytes=_ARM_SPECS[name][2],
                    aggregate=aggregate,
                    projection_total_bytes=int(projection_by_name[name]["total_bytes"]),
                    average_recall10_ppm=intervals[0],
                    average_recall100_ppm=intervals[1],
                    p05_recall100_ppm=intervals[2],
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
    return V99Rescore(
        schema="borsuk-v99-ranked-gap-range-router-rescore-v1",
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


def rescore_v99_result(
    path: pathlib.Path,
    registered_sha256: str,
    *,
    expected_authority: ScreenAuthority,
    expected_config: object,
) -> V99Rescore:
    """Return independently recomputed V99 gates, intervals, and winner."""

    try:
        return _load_and_rescore(
            path,
            registered_sha256,
            expected_authority=expected_authority,
            expected_config=expected_config,
        )
    except ValueError as error:
        if str(error).startswith("V99 "):
            raise
        raise ValueError(f"V99 result differs: {error}") from error


def validate_v99_result(
    path: pathlib.Path,
    registered_sha256: str,
    *,
    expected_authority: ScreenAuthority,
    expected_config: object,
) -> ValidatedV99Result:
    """Authenticate and independently validate one hostile V99 result."""

    summary = rescore_v99_result(
        path,
        registered_sha256,
        expected_authority=expected_authority,
        expected_config=expected_config,
    )
    return ValidatedV99Result(
        result_sha256=summary.result_sha256,
        query_count=summary.query_count,
        classification=summary.classification,
    )


def canonical_v99_rescore_bytes(summary: V99Rescore) -> bytes:
    """Serialize the independent V99 receipt as canonical typed JSON."""

    if (
        not isinstance(summary, V99Rescore)
        or summary.schema != "borsuk-v99-ranked-gap-range-router-rescore-v1"
    ):
        _fail("rescore")
    return (
        json.dumps(
            asdict(summary), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
