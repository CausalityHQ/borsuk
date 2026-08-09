#!/usr/bin/env python3
"""Fail-closed validator for terminal group-commit scalability campaigns."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from pathlib import Path


class ValidationError(RuntimeError):
    pass


PHASE_MARKERS = (
    "INGEST_COMPLETE",
    "ACTIVE_TAIL_READ_QUALIFICATION_COMPLETE",
    "DRAIN_COMPLETE",
    "POINT_VISIBILITY_COMPLETE",
    "READ_QUALIFICATION_COMPLETE",
)

EXACT_BOUND_SHADOW_V1_FIELDS = (
    "global_exact_bound_candidates",
    "global_exact_bound_survivors",
    "global_exact_bound_fail_open",
    "global_exact_bound_containment_failures",
    "global_exact_bound_predicted_reads",
    "global_exact_bound_predicted_bytes",
    "global_exact_bound_cpu_us",
)
EXACT_BOUND_SHADOW_BASELINE_FIELDS = (
    "global_exact_bound_baseline_reads",
    "global_exact_bound_baseline_bytes",
)
EXACT_BOUND_SHADOW_FIELDS = (
    *EXACT_BOUND_SHADOW_V1_FIELDS,
    *EXACT_BOUND_SHADOW_BASELINE_FIELDS,
)
EXACT_BOUND_SHADOW_V8_FIELDS = (
    "global_exact_bound_certificate_kind",
    "global_exact_bound_exact_backing_reads",
    "global_exact_bound_exact_backing_bytes",
    "global_exact_bound_residual_bytes",
    "global_exact_bound_residual_scan_bytes",
)
EXACT_BOUND_SHADOW_RESIDUAL_PQ_FIELDS = (
    "global_exact_bound_predicted_waves",
    "global_exact_bound_certificate_scratch_allocations",
)

RESIDUAL_PQ_CAMPAIGN_ID = "group-commit-residual-pq-local-v1"
RESIDUAL_PQ_REPORTING_FIELDS = [
    "scan_waves",
    "dynamic_program_minimum_exact_ranges",
    "dynamic_program_minimum_exact_bytes",
    "certificate_decode_cpu_p50_p95_p99_us",
    "certificate_allocation_count",
    "read_latency_p50_p95_p99_ms",
    "structural_and_empirical_floor_gaps",
]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def validate_residual_pq_manifest(manifest: dict[str, object]) -> None:
    """Validate the exact preregistered V8 local decision contract."""
    require(
        manifest.get("campaign_id") == RESIDUAL_PQ_CAMPAIGN_ID,
        "residual-PQ campaign identity drift",
    )
    exact = manifest.get("exact_bound_shadow")
    require(isinstance(exact, dict), "missing residual-PQ exact-bound contract")
    optimization = manifest.get("optimization_contract")
    require(isinstance(optimization, dict), "missing latency optimization contract")
    requirements = (
        (manifest.get("protocol_kind") == "local", "protocol kind"),
        (manifest.get("dataset") == "cohere-medium-1M", "dataset"),
        (manifest.get("dimensions") == 768, "dimensions"),
        (manifest.get("metric") == "cosine", "metric"),
        (manifest.get("cell_counts") == [2_000], "cell count"),
        (manifest.get("writers") == [32], "writer count"),
        (manifest.get("worker_lanes") == [1], "worker lane"),
        (manifest.get("repetitions") == 1, "repetition count"),
        (manifest.get("operations_per_writer") == 32, "operation count"),
        (manifest.get("records_per_operation") == 16, "records per operation"),
        (manifest.get("writer_process_cpu_threads") == 1, "writer CPU threads"),
        (manifest.get("writer_process_io_threads") == 2, "writer I/O threads"),
        (manifest.get("read_queries_per_cell") == 20, "read query count"),
        (manifest.get("min_inserted_id_recall_at_10") == 1.0, "recall gate"),
        (
            exact.get("candidate_configuration") == "residual-pq64-f32-error-shadow",
            "candidate configuration",
        ),
        (exact.get("residual_code_bytes") == 64, "residual code width"),
        (exact.get("residual_error_bytes") == 4, "residual error width"),
        (exact.get("exact_vector_bytes") == 3_072, "exact vector width"),
        (exact.get("require_zero_containment_failures") is True, "containment gate"),
        (exact.get("max_survivor_p95") == 11, "survivor gate"),
        (exact.get("min_read_reduction_fraction") == 0.30, "read reduction gate"),
        (exact.get("min_byte_reduction_fraction") == 0.30, "byte reduction gate"),
        (exact.get("max_cpu_p95_us") == 2_000, "CPU gate"),
        (exact.get("max_cpu_fraction_of_read_p95") == 0.05, "CPU fraction gate"),
        (exact.get("max_residual_bytes_per_vector") == 68, "residual byte gate"),
        (exact.get("max_total_backing_byte_ratio") == 2.0, "backing byte gate"),
        (exact.get("max_drain_regression_fraction") == 0.10, "drain regression gate"),
        (
            exact.get("max_physical_write_amplification_regression_fraction") == 0.10,
            "physical write amplification regression gate",
        ),
        (
            exact.get("non_read_regression_control")
            == "same-cell-shared-ingest-and-drain",
            "non-read regression control",
        ),
        (
            exact.get("mandatory_reporting") == RESIDUAL_PQ_REPORTING_FIELDS,
            "mandatory reporting contract",
        ),
        (optimization.get("hard_read_p95_ms") == 200, "latency optimization contract"),
        (
            optimization.get("selection_rule")
            == "pareto-minimize-latency-requests-bytes-cpu-allocations-at-fixed-correctness-recall",
            "Pareto selection contract",
        ),
        (
            optimization.get("gate_passing_freezes_production_default") is False,
            "production freeze contract",
        ),
    )
    for valid, label in requirements:
        require(valid, f"residual-PQ {label} drift")
    total_records = (
        int(manifest["writers"][0])
        * int(manifest["operations_per_writer"])
        * int(manifest["records_per_operation"])
    )
    require(total_records == 16_384, "residual-PQ total record count drift")


def validate_residual_pq_storage_trace(
    events: list[dict[str, str]], context: str
) -> None:
    exact_reads = [
        event
        for event in events
        if event.get("operation") == "read"
        and event.get("object_role") == "exact_vectors"
        and event.get("status") == "ok"
    ]
    require(bool(exact_reads), f"missing exact-path trace in {context}")
    require(
        all(event.get("cache_state") != "hit" for event in exact_reads),
        f"exact-path trace contains a cache hit in {context}",
    )
    require(
        all(
            event.get("cache_state") == "backing"
            and integer(event["request_count"], "exact trace request count") > 0
            and integer(event["bytes_fetched"], "exact trace bytes fetched") > 0
            for event in exact_reads
        ),
        f"exact-path trace lacks uncached backing evidence in {context}",
    )


def rows(path: Path) -> list[dict[str, str]]:
    require(path.is_file(), f"missing {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        require(reader.fieldnames is not None, f"missing CSV header in {path}")
        return list(reader)


def expected_aggregate_rows(
    root: Path,
    cells: set[tuple[int, int, int, int]],
    name: str,
) -> list[dict[str, str]]:
    expected: list[dict[str, str]] = []
    for cell_count, repetition, lanes, writers in sorted(cells):
        cell = (
            root
            / "cells"
            / f"c{cell_count}"
            / f"r{repetition:02d}"
            / f"l{lanes}"
            / f"w{writers}"
        )
        for row in rows(cell / name):
            identity = {
                "cell_count": str(cell_count),
                "repetition": str(repetition),
            }
            if name != "summary.csv":
                identity["worker_lanes"] = str(lanes)
            expected.append({**identity, **row})
    return expected


def finite(value: str, label: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise ValidationError(f"invalid {label}: {value!r}") from error
    require(math.isfinite(parsed), f"non-finite {label}")
    return parsed


def integer(value: str, label: str) -> int:
    try:
        return int(value)
    except ValueError as error:
        raise ValidationError(f"invalid {label}: {value!r}") from error


def exact_bound_shadow_has_baseline(read: dict[str, str], context: str) -> bool | None:
    v1_count = sum(field in read for field in EXACT_BOUND_SHADOW_V1_FIELDS)
    baseline_count = sum(field in read for field in EXACT_BOUND_SHADOW_BASELINE_FIELDS)
    if v1_count == 0 and baseline_count == 0:
        return None
    require(
        v1_count == len(EXACT_BOUND_SHADOW_V1_FIELDS),
        f"incomplete exact-bound shadow telemetry in {context}",
    )
    require(
        baseline_count in {0, len(EXACT_BOUND_SHADOW_BASELINE_FIELDS)},
        f"incomplete exact-bound shadow baseline in {context}",
    )
    return baseline_count > 0


def validate_exact_bound_shadow_row(
    read: dict[str, str],
    context: str,
    has_baseline: bool,
    require_residual_pq: bool = False,
) -> None:
    fields = EXACT_BOUND_SHADOW_FIELDS if has_baseline else EXACT_BOUND_SHADOW_V1_FIELDS
    values = {field: integer(read[field], f"{context} {field}") for field in fields}
    require(
        all(value >= 0 for value in values.values()),
        f"negative exact-bound shadow telemetry in {context}",
    )
    candidates = values["global_exact_bound_candidates"]
    survivors = values["global_exact_bound_survivors"]
    fail_open = values["global_exact_bound_fail_open"]
    failures = values["global_exact_bound_containment_failures"]
    predicted_reads = values["global_exact_bound_predicted_reads"]
    predicted_bytes = values["global_exact_bound_predicted_bytes"]
    require(
        survivors <= candidates, f"exact-bound survivors exceed candidates in {context}"
    )
    require(
        fail_open <= survivors,
        f"exact-bound fail-open rows exceed survivors in {context}",
    )
    require(
        failures <= candidates, f"exact-bound failures exceed candidates in {context}"
    )
    require(
        survivors >= min(10, candidates),
        f"exact-bound shadow retained fewer than top-k candidates in {context}",
    )
    require(
        survivors == 0 or (predicted_reads > 0 and predicted_bytes > 0),
        f"exact-bound physical prediction is empty in {context}",
    )
    if has_baseline:
        baseline_reads = values["global_exact_bound_baseline_reads"]
        baseline_bytes = values["global_exact_bound_baseline_bytes"]
        require(
            candidates == 0 or (baseline_reads > 0 and baseline_bytes > 0),
            f"exact-bound physical baseline is empty in {context}",
        )
        require(
            predicted_reads <= baseline_reads and predicted_bytes <= baseline_bytes,
            f"exact-bound prediction exceeds exact-plan baseline in {context}",
        )
    if failures > 0:
        require(
            survivors == candidates and fail_open == candidates,
            f"containment failure did not fail open in {context}",
        )
    v8_count = sum(field in read for field in EXACT_BOUND_SHADOW_V8_FIELDS)
    require(
        v8_count in {0, len(EXACT_BOUND_SHADOW_V8_FIELDS)},
        f"incomplete V8 exact-bound telemetry in {context}",
    )
    if v8_count:
        require(
            read["global_exact_bound_certificate_kind"] == "residual-pq-v8",
            f"unexpected exact-bound certificate kind in {context}",
        )
        v8_values = {
            field: integer(read[field], f"{context} {field}")
            for field in EXACT_BOUND_SHADOW_V8_FIELDS
            if field != "global_exact_bound_certificate_kind"
        }
        require(
            all(value >= 0 for value in v8_values.values()),
            f"negative V8 exact-bound telemetry in {context}",
        )
        require(
            v8_values["global_exact_bound_residual_bytes"] == candidates * 68,
            f"V8 residual candidate bytes disagree with the frozen width in {context}",
        )
        require(
            v8_values["global_exact_bound_residual_scan_bytes"]
            >= v8_values["global_exact_bound_residual_bytes"],
            f"V8 residual scan bytes are smaller than retained bytes in {context}",
        )
        require(
            candidates == 0
            or (
                v8_values["global_exact_bound_exact_backing_reads"] > 0
                and v8_values["global_exact_bound_exact_backing_bytes"] > 0
            ),
            f"V8 uncached exact backing evidence is empty in {context}",
        )
    residual_count = sum(
        field in read for field in EXACT_BOUND_SHADOW_RESIDUAL_PQ_FIELDS
    )
    require(
        residual_count in {0, len(EXACT_BOUND_SHADOW_RESIDUAL_PQ_FIELDS)},
        f"incomplete residual-PQ telemetry in {context}",
    )
    if require_residual_pq:
        require(
            v8_count == len(EXACT_BOUND_SHADOW_V8_FIELDS)
            and residual_count == len(EXACT_BOUND_SHADOW_RESIDUAL_PQ_FIELDS),
            f"incomplete residual-PQ telemetry in {context}",
        )
    if residual_count:
        predicted_waves = integer(
            read["global_exact_bound_predicted_waves"],
            f"{context} global_exact_bound_predicted_waves",
        )
        scratch_allocations = integer(
            read["global_exact_bound_certificate_scratch_allocations"],
            f"{context} global_exact_bound_certificate_scratch_allocations",
        )
        require(
            predicted_waves >= 0 and scratch_allocations >= 0,
            f"negative residual-PQ physical telemetry in {context}",
        )
        require(
            survivors == 0 or predicted_waves > 0,
            f"residual-PQ request-wave evidence is empty in {context}",
        )
        require(
            predicted_waves <= predicted_reads,
            f"residual-PQ request waves exceed requests in {context}",
        )


def validate_process_identity(
    samples: list[dict[str, str]], writers: int, cell: Path
) -> None:
    """Require one stable, distinct operating-system process per writer."""
    writer_processes: dict[int, set[int]] = {}
    process_writers: dict[int, set[int]] = {}
    for sample in samples:
        try:
            writer = integer(sample["writer"], "sample writer")
            process_id = integer(sample["process_id"], "sample process identity")
        except KeyError as error:
            raise ValidationError(
                f"missing process identity evidence in {cell}"
            ) from error
        require(process_id > 0, f"invalid process identity in {cell}")
        writer_processes.setdefault(writer, set()).add(process_id)
        process_writers.setdefault(process_id, set()).add(writer)
    require(
        set(writer_processes) == set(range(writers))
        and all(len(processes) == 1 for processes in writer_processes.values())
        and len(process_writers) == writers
        and all(len(owners) == 1 for owners in process_writers.values()),
        f"one-process-per-writer process identity violation in {cell}",
    )


def percentile(values: list[float], quantile: float) -> float:
    require(bool(values), "cannot compute a percentile from no samples")
    ordered = sorted(values)
    # Match Rust f64::round for the non-negative index used by the benchmark.
    index = math.floor((len(ordered) - 1) * quantile + 0.5)
    return ordered[index]


def require_close(observed: float, expected: float, message: str) -> None:
    require(
        math.isclose(observed, expected, rel_tol=1e-9, abs_tol=1e-9),
        f"{message}: observed {observed}, expected {expected}",
    )


def lane_receipt_evidence(sample: dict[str, str]) -> list[tuple[int | str, ...]]:
    """Return authenticated per-lane direct-mutation evidence."""
    encoded = sample.get("lane_receipts")
    require(encoded is not None, "missing authenticated lane receipt evidence")
    require(bool(encoded), "empty lane receipt evidence")
    evidence = []
    for item in encoded.split(";"):
        fields = item.split(":")
        require(len(fields) == 13, "invalid lane receipt evidence")
        numeric = tuple(integer(value, "lane receipt field") for value in fields[:11])
        extent_checksum, published_head_checksum = fields[11:]
        require(
            re.fullmatch(r"[0-9a-f]{64}", extent_checksum) is not None,
            "invalid extent checksum evidence",
        )
        require(
            re.fullmatch(r"[0-9a-f]{64}", published_head_checksum) is not None,
            "invalid published-head checksum evidence",
        )
        evidence.append((*numeric, extent_checksum, published_head_checksum))
    return evidence


def direct_acknowledgement_request_contract(protocol_kind: str) -> tuple[int, ...]:
    """Return the exact healthy request tuple for the frozen storage backend."""
    if protocol_kind in {"smoke", "local", "bounded-diagnostic"}:
        # The claim-ineligible local filesystem adapter verifies create-only
        # extents with one HEAD and publishes via an extra staging PUT.
        return (4, 0, 3, 0, 1, 0)
    if protocol_kind in {"production", "architecture-qualification"}:
        return (2, 0, 2, 0, 0, 0)
    raise ValidationError(f"unknown group-commit protocol kind {protocol_kind!r}")


def environment(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        require(
            bool(separator) and key not in values, f"invalid environment line {line!r}"
        )
        values[key] = value
    return values


def validate(
    root: Path,
    manifest_path: Path,
    terminal_cell: tuple[int, int, int, int] | None = None,
    failed_terminal_cell: tuple[int, int, int, int] | None = None,
    completed_cell_after_root_failure: tuple[int, int, int, int] | None = None,
    preterminal_root: bool = False,
) -> None:
    require(
        sum(
            mode is not None
            for mode in (
                terminal_cell,
                failed_terminal_cell,
                completed_cell_after_root_failure,
            )
        )
        <= 1,
        "terminal-cell modes are mutually exclusive",
    )
    require(
        not preterminal_root
        or (
            terminal_cell is None
            and failed_terminal_cell is None
            and completed_cell_after_root_failure is None
        ),
        "preterminal root and terminal-cell modes are mutually exclusive",
    )
    if preterminal_root:
        require(
            not (root / "GROUP_COMMIT_SCALABILITY_COMPLETE").exists()
            and not (root / "GROUP_COMMIT_SCALABILITY_FAILED").exists(),
            "preterminal root must not have a terminal marker",
        )
    elif (
        failed_terminal_cell is not None
        or completed_cell_after_root_failure is not None
    ):
        require(
            not (root / "GROUP_COMMIT_SCALABILITY_COMPLETE").exists(),
            "root-failure recovery requires no completion marker",
        )
        require(
            (root / "GROUP_COMMIT_SCALABILITY_FAILED").is_file(),
            "root-failure recovery requires the terminal failure marker",
        )
    elif terminal_cell is None:
        require(
            (root / "GROUP_COMMIT_SCALABILITY_COMPLETE").is_file(),
            "campaign is incomplete",
        )
    if (
        failed_terminal_cell is None
        and completed_cell_after_root_failure is None
        and not preterminal_root
    ):
        require(
            not (root / "GROUP_COMMIT_SCALABILITY_FAILED").exists(),
            "campaign has a failure marker",
        )
    require(manifest_path.is_file(), f"missing manifest {manifest_path}")
    frozen = manifest_path.read_bytes()
    copied = (root / "manifest.json").read_bytes()
    require(copied == frozen, "copied manifest differs from the frozen manifest")
    manifest = json.loads(frozen)
    exact_contract = manifest.get("exact_bound_shadow")
    require_residual_pq = manifest.get("campaign_id") == RESIDUAL_PQ_CAMPAIGN_ID or (
        isinstance(exact_contract, dict)
        and exact_contract.get("candidate_configuration")
        == "residual-pq64-f32-error-shadow"
    )
    if require_residual_pq:
        validate_residual_pq_manifest(manifest)
    direct_request_contract = direct_acknowledgement_request_contract(
        manifest["protocol_kind"]
    )
    manifest_sha = hashlib.sha256(frozen).hexdigest()
    identity = environment(root / "environment.txt")
    require(
        identity.get("manifest_sha256") == manifest_sha, "manifest SHA-256 mismatch"
    )
    source_sha = identity.get("source_sha256", "")
    require(
        re.fullmatch(r"[0-9a-f]{64}", source_sha) is not None, "invalid source SHA-256"
    )
    require(
        identity.get("architecture") == manifest["architecture"],
        "architecture mismatch",
    )
    require(
        identity.get("instance_type") == manifest["instance_type"],
        "instance type mismatch",
    )
    dataset_sha = manifest.get("dataset_sha256")
    if dataset_sha is not None:
        require(
            re.fullmatch(r"[0-9a-f]{64}", dataset_sha) is not None,
            "invalid frozen dataset SHA-256",
        )
        require(identity.get("dataset_sha256") == dataset_sha, "dataset identity drift")
        descriptor = root / "dataset.json"
        require(descriptor.is_file(), f"missing dataset descriptor {descriptor}")
        require(
            hashlib.sha256(descriptor.read_bytes()).hexdigest() == dataset_sha,
            "dataset SHA-256 mismatch",
        )

    worker_lanes = manifest.get("worker_lanes", [1])
    if isinstance(worker_lanes, int):
        worker_lanes = [worker_lanes]
    throughput_gate_writers = {
        int(writers)
        for writers in manifest.get("throughput_gate_writers", manifest["writers"])
    }
    require(
        throughput_gate_writers <= {int(writers) for writers in manifest["writers"]},
        "throughput gate writers are outside the frozen matrix",
    )
    require(
        manifest.get("writer_instance_policy") == "one-per-writer",
        "campaign must require one independent writer instance per writer",
    )
    require(
        manifest.get("writer_process_policy") == "one-process-per-writer",
        "campaign must require one-process-per-writer process identity evidence",
    )

    frozen_cells = {
        (int(cell_count), repetition, int(lanes), int(writers))
        for cell_count in manifest["cell_counts"]
        for repetition in range(1, int(manifest["repetitions"]) + 1)
        for lanes in worker_lanes
        for writers in manifest["writers"]
    }
    selected_cell = next(
        (
            mode
            for mode in (
                terminal_cell,
                failed_terminal_cell,
                completed_cell_after_root_failure,
            )
            if mode is not None
        ),
        None,
    )
    if selected_cell is None:
        expected_cells = frozen_cells
    else:
        require(
            selected_cell in frozen_cells, "terminal cell is outside the frozen matrix"
        )
        expected_cells = {selected_cell}
    observed_cells: set[tuple[int, int, int, int]] = set()
    expected_sample_total = 0
    for cell_count, repetition, lanes, writers in sorted(expected_cells):
        cell = (
            root
            / "cells"
            / f"c{cell_count}"
            / f"r{repetition:02d}"
            / f"l{lanes}"
            / f"w{writers}"
        )
        if failed_terminal_cell is not None:
            require(
                not (cell / "CELL_COMPLETE").exists(),
                f"failed cell has a completion marker: {cell}",
            )
            require(
                (cell / "CELL_FAILED").is_file(), f"missing failed-cell marker: {cell}"
            )
        else:
            require((cell / "CELL_COMPLETE").is_file(), f"cell is incomplete: {cell}")
            require(
                not (cell / "CELL_FAILED").exists(),
                f"cell has a failure marker: {cell}",
            )
        for marker in PHASE_MARKERS:
            require(
                (cell / marker).is_file(), f"missing phase marker {marker} in {cell}"
            )
        summary_rows = rows(cell / "summary.csv")
        require(len(summary_rows) == 1, f"{cell} must contain one summary row")
        summary = summary_rows[0]
        require(
            summary["source_sha256"] == source_sha, f"source identity drift in {cell}"
        )
        if dataset_sha is not None:
            require(
                summary["dataset_sha256"] == dataset_sha,
                f"dataset identity drift in {cell}",
            )
        require(
            summary["manifest_sha256"] == manifest_sha,
            f"manifest identity drift in {cell}",
        )
        require(
            integer(summary["writers"], "writers") == writers, f"writer drift in {cell}"
        )
        require(
            integer(summary["writer_instances"], "writer instances") == writers,
            f"writer instance drift in {cell}",
        )
        operations = int(manifest["operations_per_writer"])
        records_per_operation = int(manifest.get("records_per_operation", 1))
        expected_operations = writers * operations
        expected_records = expected_operations * records_per_operation
        require(
            integer(summary["operations"], "operations") == operations,
            f"operation drift in {cell}",
        )
        if "records_per_operation" in summary:
            require(
                integer(summary["records_per_operation"], "records per operation")
                == records_per_operation,
                f"records-per-operation drift in {cell}",
            )
        require(
            integer(summary["pipeline_depth"], "pipeline depth")
            == int(manifest.get("pipeline_depth_per_writer", 1)),
            f"pipeline depth drift in {cell}",
        )
        require(
            integer(summary["worker_lanes"], "worker lanes") == lanes,
            f"worker lane drift in {cell}",
        )
        require(
            integer(summary["records"], "records") == expected_records,
            f"record drift in {cell}",
        )
        require(
            integer(summary["visible_records"], "visible records") == expected_records,
            f"visibility failure in {cell}",
        )
        require(
            integer(summary["recall_queries"], "recall queries")
            == int(manifest["read_queries_per_cell"]),
            f"recall query count drift in {cell}",
        )
        require(
            integer(summary["max_read_segments"], "max read segments")
            == int(manifest["max_read_segments"]),
            f"read segment budget drift in {cell}",
        )
        require(
            finite(summary["inserted_id_recall_at_10"], "inserted-ID recall at 10")
            >= float(manifest["min_inserted_id_recall_at_10"]),
            f"inserted-ID recall failure in {cell}",
        )
        read_request_fields = (
            "read_storage_gets",
            "read_storage_puts",
            "read_storage_deletes",
            "read_storage_heads",
            "read_storage_lists",
        )
        try:
            read_request_parts = [
                integer(summary[field], field) for field in read_request_fields
            ]
            read_request_total = integer(
                summary["read_storage_requests"], "read storage requests"
            )
            read_bytes = integer(summary["read_bytes"], "read bytes")
            read_segments = integer(
                summary["read_segments_searched"], "read segments searched"
            )
        except KeyError as error:
            raise ValidationError("missing read path telemetry") from error
        require(
            all(value >= 0 for value in read_request_parts)
            and read_request_total >= 0
            and read_bytes >= 0
            and read_segments >= 0,
            f"negative read path telemetry in {cell}",
        )
        require(
            sum(read_request_parts) == read_request_total,
            f"read request reconciliation failed in {cell}",
        )
        for field in (
            "elapsed_ms",
            "drain_ms",
            "end_to_end_records_per_second",
            "p50_ms",
            "p95_ms",
            "records_per_second",
            "vector_mib_per_second",
            "requests_per_record",
            "read_p50_ms",
            "read_p95_ms",
            "active_tail_read_p50_ms",
            "active_tail_read_p95_ms",
        ):
            require(finite(summary[field], field) >= 0.0, f"negative {field} in {cell}")
        if manifest.get("protocol_kind") in {
            "production",
            "architecture-qualification",
        }:
            expected_failures: set[str] = set()
            if finite(summary["p95_ms"], "p95_ms") >= float(
                manifest["max_write_p95_ms"]
            ):
                expected_failures.add("PRODUCTION_WRITE_P95_FAILED")
            if writers in throughput_gate_writers:
                if finite(summary["records_per_second"], "records_per_second") < float(
                    manifest["min_records_per_second"]
                ):
                    expected_failures.add("PRODUCTION_WRITE_THROUGHPUT_FAILED")
                if finite(
                    summary["end_to_end_records_per_second"],
                    "end_to_end_records_per_second",
                ) < float(manifest["min_end_to_end_records_per_second"]):
                    expected_failures.add("PRODUCTION_END_TO_END_THROUGHPUT_FAILED")
            if finite(summary["read_p95_ms"], "read_p95_ms") >= float(
                manifest["max_read_p95_ms"]
            ):
                expected_failures.add("PRODUCTION_READ_P95_FAILED")
            if finite(
                summary["active_tail_read_p95_ms"], "active_tail_read_p95_ms"
            ) >= float(manifest["max_read_p95_ms"]):
                expected_failures.add("PRODUCTION_ACTIVE_TAIL_READ_P95_FAILED")
            if finite(
                summary["inserted_id_recall_at_10"],
                "inserted_id_recall_at_10",
            ) < float(manifest["min_inserted_id_recall_at_10"]):
                expected_failures.add("PRODUCTION_INSERTED_ID_RECALL_AT_10_FAILED")
            subgate_markers = {
                "PRODUCTION_WRITE_P95_FAILED",
                "PRODUCTION_WRITE_THROUGHPUT_FAILED",
                "PRODUCTION_END_TO_END_THROUGHPUT_FAILED",
                "PRODUCTION_READ_P95_FAILED",
                "PRODUCTION_ACTIVE_TAIL_READ_P95_FAILED",
                "PRODUCTION_INSERTED_ID_RECALL_AT_10_FAILED",
            }
            observed_failures = {
                marker for marker in subgate_markers if (cell / marker).exists()
            }
            if failed_terminal_cell is not None:
                require(
                    bool(expected_failures),
                    f"failed cell passes every production threshold: {cell}",
                )
                require(
                    observed_failures == expected_failures,
                    f"production failure markers do not match measured thresholds in {cell}",
                )
                require(
                    (cell / "PRODUCTION_PERFORMANCE_GATE_FAILED").is_file(),
                    f"missing production performance failure marker in {cell}",
                )
                require(
                    not (cell / "PRODUCTION_PERFORMANCE_GATE_COMPLETE").exists(),
                    f"failed cell has a production completion marker: {cell}",
                )
            else:
                failure_messages = (
                    ("PRODUCTION_WRITE_P95_FAILED", "production p95 gate failed"),
                    (
                        "PRODUCTION_WRITE_THROUGHPUT_FAILED",
                        "production throughput gate failed",
                    ),
                    (
                        "PRODUCTION_END_TO_END_THROUGHPUT_FAILED",
                        "production end-to-end throughput gate failed",
                    ),
                    ("PRODUCTION_READ_P95_FAILED", "production read p95 gate failed"),
                    (
                        "PRODUCTION_ACTIVE_TAIL_READ_P95_FAILED",
                        "production active-tail read p95 gate failed",
                    ),
                    (
                        "PRODUCTION_INSERTED_ID_RECALL_AT_10_FAILED",
                        "production inserted-ID recall gate failed",
                    ),
                )
                for marker, message in failure_messages:
                    require(marker not in expected_failures, f"{message} in {cell}")
                require(
                    (cell / "PRODUCTION_PERFORMANCE_GATE_COMPLETE").is_file(),
                    f"missing production performance gate marker in {cell}",
                )
                require(
                    not (cell / "PRODUCTION_PERFORMANCE_GATE_FAILED").exists(),
                    f"production performance failure marker in {cell}",
                )
                require(
                    not observed_failures,
                    f"production sub-gate failure marker in {cell}",
                )

        samples = rows(cell / "samples.csv")
        require(
            len(samples) == expected_operations, f"raw sample count mismatch in {cell}"
        )
        validate_process_identity(samples, writers, cell)
        ids: set[str] = set()
        groups: dict[tuple[int, int], tuple[int, int, int, int, int]] = {}
        writer_operations: set[tuple[int, int]] = set()
        write_latencies: list[float] = []
        for sample in samples:
            writer = integer(sample["writer"], "sample writer")
            writer_instance = integer(
                sample["writer_instance"], "sample writer instance"
            )
            operation = integer(sample["operation"], "sample operation")
            require(
                0 <= writer < writers and 0 <= operation < operations,
                f"sample coordinate out of range in {cell}",
            )
            require(
                writer_instance == writer,
                f"sample writer instance drift in {cell}",
            )
            require(
                (writer, operation) not in writer_operations,
                f"duplicate sample coordinate in {cell}",
            )
            writer_operations.add((writer, operation))
            if "batch_records" in sample:
                require(
                    integer(sample["batch_records"], "sample batch records")
                    == records_per_operation,
                    f"sample batch length drift in {cell}",
                )
                record_ids = sample["record_ids"].split("|")
                require(
                    len(record_ids) == records_per_operation,
                    f"sample record identity count drift in {cell}",
                )
                require(
                    sample["first_record_id"] == record_ids[0],
                    f"sample first ID drift in {cell}",
                )
            else:
                require(
                    records_per_operation == 1,
                    f"legacy sample cannot represent bulk cell {cell}",
                )
                record_ids = [sample["record_id"]]
            require(not ids.intersection(record_ids), f"duplicate record id in {cell}")
            ids.update(record_ids)
            sample_latency = finite(sample["latency_ms"], "sample latency")
            require(sample_latency >= 0.0, f"negative sample latency in {cell}")
            write_latencies.append(sample_latency)
            for evidence in lane_receipt_evidence(sample):
                group = (evidence[0], evidence[2], evidence[1])
                normalized = evidence[3:]
                require(
                    normalized[2:8] == direct_request_contract,
                    f"direct acknowledgement request contract drift in {cell}",
                )
                require(
                    group not in groups or groups[group] == normalized,
                    f"inconsistent shared group evidence in {cell}",
                )
                groups[group] = normalized
        require(
            len({evidence[8] for evidence in groups.values()}) == len(groups),
            f"extent checksum identity collision in {cell}",
        )
        require(
            len({evidence[9] for evidence in groups.values()}) == len(groups),
            f"published-head checksum identity collision in {cell}",
        )
        require(
            sum(evidence[0] for evidence in groups.values()) == expected_records,
            f"group record reconciliation failed in {cell}",
        )
        total_acknowledgement_bytes = sum(evidence[1] for evidence in groups.values())
        max_acknowledgement_bytes = max(evidence[1] for evidence in groups.values())
        require(
            total_acknowledgement_bytes
            == integer(
                summary["total_acknowledgement_bytes"], "total acknowledgement bytes"
            ),
            f"acknowledgement byte total drift in {cell}",
        )
        require(
            max_acknowledgement_bytes
            == integer(
                summary["max_acknowledgement_bytes"], "maximum acknowledgement bytes"
            ),
            f"acknowledgement byte maximum drift in {cell}",
        )
        require(
            max_acknowledgement_bytes <= int(manifest["max_acknowledgement_bytes"]),
            f"acknowledgement byte bound exceeded in {cell}",
        )
        storage_events = rows(cell / "storage-access.csv")
        require(bool(storage_events), f"missing physical storage evidence in {cell}")
        if require_residual_pq:
            validate_residual_pq_storage_trace(storage_events, str(cell))
        physical_write_bytes = 0
        for event in storage_events:
            object_bytes = integer(event["object_bytes"], "physical object bytes")
            request_count = integer(event["request_count"], "physical request count")
            require(
                object_bytes >= 0 and request_count >= 0,
                f"negative physical storage evidence in {cell}",
            )
            if event["operation"] == "write" and event["status"] == "ok":
                physical_write_bytes += object_bytes
        input_vector_bytes = expected_records * int(manifest["dimensions"]) * 4
        require(
            physical_write_bytes
            <= input_vector_bytes * float(manifest["max_physical_write_amplification"]),
            f"physical write amplification exceeded in {cell}",
        )
        total_requests = sum(evidence[2] for evidence in groups.values())
        require(
            total_requests == integer(summary["storage_requests"], "storage requests"),
            f"request reconciliation failed in {cell}",
        )
        require(
            len(groups) == integer(summary["groups"], "groups"),
            f"group count mismatch in {cell}",
        )
        require_close(
            finite(summary["p50_ms"], "p50_ms"),
            percentile(write_latencies, 0.50),
            f"write p50 does not match raw samples in {cell}",
        )
        require_close(
            finite(summary["p95_ms"], "p95_ms"),
            percentile(write_latencies, 0.95),
            f"write p95 does not match raw samples in {cell}",
        )
        elapsed_ms = finite(summary["elapsed_ms"], "elapsed_ms")
        require(elapsed_ms > 0.0, f"elapsed time must be positive in {cell}")
        expected_records_per_second = expected_records / (elapsed_ms / 1_000.0)
        observed_records_per_second = finite(
            summary["records_per_second"], "records_per_second"
        )
        require_close(
            observed_records_per_second,
            expected_records_per_second,
            f"throughput does not match records and elapsed time in {cell}",
        )
        if "operations_per_second" in summary:
            require_close(
                finite(summary["operations_per_second"], "operations_per_second"),
                expected_operations / (elapsed_ms / 1_000.0),
                f"operation throughput does not match operations and elapsed time in {cell}",
            )
        drain_ms = finite(summary["drain_ms"], "drain_ms")
        expected_end_to_end_records_per_second = expected_records / (
            (elapsed_ms + drain_ms) / 1_000.0
        )
        require_close(
            finite(
                summary["end_to_end_records_per_second"],
                "end_to_end_records_per_second",
            ),
            expected_end_to_end_records_per_second,
            f"end-to-end throughput does not include drain in {cell}",
        )
        expected_vector_mib_per_second = (
            expected_records_per_second
            * int(manifest["dimensions"])
            * 4
            / (1024 * 1024)
        )
        require_close(
            finite(summary["vector_mib_per_second"], "vector_mib_per_second"),
            expected_vector_mib_per_second,
            f"vector throughput does not reconcile in {cell}",
        )
        require_close(
            finite(summary["mean_group_records"], "mean_group_records"),
            expected_records / len(groups),
            f"mean group records does not reconcile in {cell}",
        )
        require_close(
            finite(summary["requests_per_record"], "requests_per_record"),
            total_requests / expected_records,
            f"requests per record does not reconcile in {cell}",
        )
        for summary_field, evidence_index in (
            ("storage_gets", 3),
            ("storage_puts", 4),
            ("storage_heads", 6),
        ):
            require(
                integer(summary[summary_field], summary_field)
                == sum(evidence[evidence_index] for evidence in groups.values()),
                f"{summary_field} reconciliation failed in {cell}",
            )

        try:
            read_samples = rows(cell / "reads.csv")
        except ValidationError as error:
            raise ValidationError(
                f"missing raw read sample telemetry in {cell}"
            ) from error
        require(
            len(read_samples) == int(manifest["read_queries_per_cell"]),
            f"raw read sample count mismatch in {cell}",
        )
        global_phase_fields = (
            "global_base_approximate_us",
            "global_base_exact_rerank_us",
            "global_delta_approximate_us",
            "global_delta_exact_rerank_us",
            "global_delta_wait_us",
        )
        emits_global_phases = any(
            field in read_samples[0] for field in global_phase_fields
        )
        require(
            not emits_global_phases
            or all(field in read_samples[0] for field in global_phase_fields),
            f"incomplete global phase telemetry in {cell}",
        )
        exact_bound_has_baseline = exact_bound_shadow_has_baseline(
            read_samples[0], str(cell)
        )
        observed_read_requests = 0
        observed_read_bytes = 0
        observed_read_segments = 0
        read_latencies: list[float] = []
        recall_hits = 0
        for query, read in enumerate(read_samples):
            require(
                integer(read["query"], "read query") == query,
                f"read query order drift in {cell}",
            )
            contains_record_id = read["contains_record_id"] == "true"
            require(contains_record_id, f"raw inserted-ID recall failure in {cell}")
            recall_hits += int(contains_record_id)
            read_latency = finite(read["latency_ms"], "read latency")
            require(read_latency >= 0.0, f"negative read latency in {cell}")
            read_latencies.append(read_latency)
            parts = [
                integer(read[field], f"read {field}")
                for field in ("gets", "puts", "deletes", "heads", "lists")
            ]
            request_total = integer(read["requests"], "read requests")
            require(
                sum(parts) == request_total,
                f"raw read request reconciliation failed in {cell}",
            )
            observed_read_requests += request_total
            observed_read_bytes += integer(read["bytes_read"], "raw read bytes")
            observed_read_segments += integer(
                read["segments_searched"], "raw read segments"
            )
            if emits_global_phases:
                require(
                    all(
                        integer(read[field], field) >= 0
                        for field in global_phase_fields
                    ),
                    f"negative global phase telemetry in {cell}",
                )
            if exact_bound_has_baseline is not None:
                require(
                    exact_bound_shadow_has_baseline(read, f"{cell} read {query}")
                    == exact_bound_has_baseline,
                    f"exact-bound shadow schema drift in {cell}",
                )
                validate_exact_bound_shadow_row(
                    read,
                    f"{cell} read {query}",
                    exact_bound_has_baseline,
                    require_residual_pq=require_residual_pq,
                )
        require(
            observed_read_requests == read_request_total,
            f"read request total drift in {cell}",
        )
        require(observed_read_bytes == read_bytes, f"read byte total drift in {cell}")
        require(
            observed_read_segments == read_segments,
            f"read segment total drift in {cell}",
        )
        require_close(
            finite(summary["read_p50_ms"], "read_p50_ms"),
            percentile(read_latencies, 0.50),
            f"read p50 does not match raw samples in {cell}",
        )
        require_close(
            finite(summary["read_p95_ms"], "read_p95_ms"),
            percentile(read_latencies, 0.95),
            f"read p95 does not match raw samples in {cell}",
        )
        require_close(
            finite(summary["inserted_id_recall_at_10"], "inserted-ID recall at 10"),
            recall_hits / len(read_samples),
            f"inserted-ID recall does not match raw samples in {cell}",
        )
        active_tail_reads = rows(cell / "active-tail-reads.csv")
        require(
            len(active_tail_reads) == int(manifest["read_queries_per_cell"]),
            f"active-tail raw read sample count mismatch in {cell}",
        )
        active_tail_latencies: list[float] = []
        active_exact_bound_has_baseline = exact_bound_shadow_has_baseline(
            active_tail_reads[0], f"{cell} active-tail reads"
        )
        for query, read in enumerate(active_tail_reads):
            require(
                integer(read["query"], "active-tail read query") == query,
                f"active-tail read query order drift in {cell}",
            )
            require(
                read["contains_record_id"] == "true",
                f"active-tail inserted-ID recall failure in {cell}",
            )
            active_tail_latencies.append(
                finite(read["latency_ms"], "active-tail read latency")
            )
            if active_exact_bound_has_baseline is not None:
                require(
                    exact_bound_shadow_has_baseline(
                        read, f"{cell} active-tail read {query}"
                    )
                    == active_exact_bound_has_baseline,
                    f"exact-bound shadow schema drift in {cell} active-tail reads",
                )
                validate_exact_bound_shadow_row(
                    read,
                    f"{cell} active-tail read {query}",
                    active_exact_bound_has_baseline,
                    require_residual_pq=require_residual_pq,
                )
        require_close(
            finite(summary["active_tail_read_p50_ms"], "active_tail_read_p50_ms"),
            percentile(active_tail_latencies, 0.50),
            f"active-tail read p50 does not match raw samples in {cell}",
        )
        require_close(
            finite(summary["active_tail_read_p95_ms"], "active_tail_read_p95_ms"),
            percentile(active_tail_latencies, 0.95),
            f"active-tail read p95 does not match raw samples in {cell}",
        )

        resource_path = cell / "resources.csv"
        require(resource_path.is_file(), f"missing resource telemetry {resource_path}")
        resource_rows = rows(resource_path)
        require(
            len(resource_rows) >= 2,
            f"resource telemetry must contain initial and terminal samples in {cell}",
        )
        for field in ("elapsed_ms", "cpu_percent", "rss_bytes"):
            require(
                all(
                    finite(row[field], f"resource {field}") >= 0.0
                    for row in resource_rows
                ),
                f"negative resource telemetry in {cell}",
            )
        resource_elapsed = [
            finite(row["elapsed_ms"], "resource elapsed_ms") for row in resource_rows
        ]
        require(
            resource_elapsed == sorted(resource_elapsed),
            f"resource timestamps are not monotonic in {cell}",
        )
        require(
            resource_elapsed[-1] >= elapsed_ms + drain_ms,
            f"resource telemetry does not bracket ingest and drain in {cell}",
        )
        expected_exit = "1" if failed_terminal_cell is not None else "0"
        require(
            (cell / "process_exit.txt").read_text(encoding="utf-8").strip()
            == expected_exit,
            f"unexpected resource exit in {cell}",
        )
        observed_cells.add((cell_count, repetition, lanes, writers))
        expected_sample_total += expected_operations

    require(observed_cells == expected_cells, "matrix coverage mismatch")
    if selected_cell is not None:
        return
    aggregate_summary = rows(root / "summary.csv")
    aggregate_samples = rows(root / "samples.csv")
    aggregate_reads = rows(root / "reads.csv")
    aggregate_active_tail_reads = rows(root / "active-tail-reads.csv")
    require(
        len(aggregate_summary) == len(expected_cells),
        "aggregate summary count mismatch",
    )
    require(
        len(aggregate_samples) == expected_sample_total,
        "aggregate sample count mismatch",
    )
    require(
        len(aggregate_reads)
        == len(expected_cells) * int(manifest["read_queries_per_cell"]),
        "aggregate read sample count mismatch",
    )
    require(
        len(aggregate_active_tail_reads)
        == len(expected_cells) * int(manifest["read_queries_per_cell"]),
        "aggregate active-tail read sample count mismatch",
    )
    aggregate_keys = {
        (
            integer(row["cell_count"], "aggregate cell count"),
            integer(row["repetition"], "aggregate repetition"),
            integer(row["worker_lanes"], "aggregate worker lanes"),
            integer(row["writers"], "aggregate writers"),
        )
        for row in aggregate_summary
    }
    require(aggregate_keys == expected_cells, "aggregate matrix coverage mismatch")
    aggregate_contracts = (
        ("summary.csv", aggregate_summary, "aggregate summary content mismatch"),
        ("samples.csv", aggregate_samples, "aggregate samples content mismatch"),
        ("reads.csv", aggregate_reads, "aggregate reads content mismatch"),
        (
            "active-tail-reads.csv",
            aggregate_active_tail_reads,
            "aggregate active-tail reads content mismatch",
        ),
    )
    for name, observed, message in aggregate_contracts:
        require(
            observed == expected_aggregate_rows(root, expected_cells, name), message
        )

    correctness = rows(root / "correctness.csv")
    expected_gates = set(manifest["correctness_gates"])
    require(
        {row["gate"] for row in correctness} == expected_gates,
        "correctness gate coverage mismatch",
    )
    require(
        all(row["status"] == "pass" for row in correctness), "correctness gate failure"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    cell_modes = parser.add_mutually_exclusive_group()
    cell_modes.add_argument(
        "--terminal-cell",
        metavar="cCELLS/rREPETITION/lLANES/wWRITERS",
        help="validate one completed cell without reading an incomplete campaign aggregate",
    )
    cell_modes.add_argument(
        "--failed-terminal-cell",
        metavar="cCELLS/rREPETITION/lLANES/wWRITERS",
        help="reconcile one terminal production-gate failure after root failure",
    )
    cell_modes.add_argument(
        "--completed-cell-after-root-failure",
        metavar="cCELLS/rREPETITION/lLANES/wWRITERS",
        help="reconcile one completed cell after a later root-level failure",
    )
    parser.add_argument(
        "--preterminal-root",
        action="store_true",
        help="validate a complete root before writing its terminal marker",
    )
    args = parser.parse_args()
    terminal_cell = None
    failed_terminal_cell = None
    completed_cell_after_root_failure = None
    if args.terminal_cell is not None:
        match = re.fullmatch(r"c(\d+)/r(\d+)/l(\d+)/w(\d+)", args.terminal_cell)
        if match is None:
            parser.error(
                "--terminal-cell must match cCELLS/rREPETITION/lLANES/wWRITERS"
            )
        terminal_cell = tuple(int(value) for value in match.groups())
    if args.failed_terminal_cell is not None:
        match = re.fullmatch(r"c(\d+)/r(\d+)/l(\d+)/w(\d+)", args.failed_terminal_cell)
        if match is None:
            parser.error(
                "--failed-terminal-cell must match cCELLS/rREPETITION/lLANES/wWRITERS"
            )
        failed_terminal_cell = tuple(int(value) for value in match.groups())
    if args.completed_cell_after_root_failure is not None:
        match = re.fullmatch(
            r"c(\d+)/r(\d+)/l(\d+)/w(\d+)",
            args.completed_cell_after_root_failure,
        )
        if match is None:
            parser.error(
                "--completed-cell-after-root-failure must match "
                "cCELLS/rREPETITION/lLANES/wWRITERS"
            )
        completed_cell_after_root_failure = tuple(
            int(value) for value in match.groups()
        )
    try:
        validate(
            args.root,
            args.manifest,
            terminal_cell=terminal_cell,
            failed_terminal_cell=failed_terminal_cell,
            completed_cell_after_root_failure=completed_cell_after_root_failure,
            preterminal_root=args.preterminal_root,
        )
    except (
        OSError,
        KeyError,
        TypeError,
        json.JSONDecodeError,
        ValidationError,
    ) as error:
        print(f"group-commit scalability validation failed: {error}")
        return 1
    if args.failed_terminal_cell:
        scope = f"terminal failed cell {args.failed_terminal_cell}"
    elif args.completed_cell_after_root_failure:
        scope = (
            "completed cell after root failure "
            f"{args.completed_cell_after_root_failure}"
        )
    elif args.terminal_cell:
        scope = f"terminal cell {args.terminal_cell}"
    else:
        scope = "artifacts"
    print(f"group-commit scalability {scope} are structurally valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
