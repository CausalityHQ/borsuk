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
    "DRAIN_COMPLETE",
    "POINT_VISIBILITY_COMPLETE",
    "READ_QUALIFICATION_COMPLETE",
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def rows(path: Path) -> list[dict[str, str]]:
    require(path.is_file(), f"missing {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        require(reader.fieldnames is not None, f"missing CSV header in {path}")
        return list(reader)


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


def environment(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        require(bool(separator) and key not in values, f"invalid environment line {line!r}")
        values[key] = value
    return values


def validate(
    root: Path,
    manifest_path: Path,
    terminal_cell: tuple[int, int, int, int] | None = None,
) -> None:
    if terminal_cell is None:
        require((root / "GROUP_COMMIT_SCALABILITY_COMPLETE").is_file(), "campaign is incomplete")
    require(not (root / "GROUP_COMMIT_SCALABILITY_FAILED").exists(), "campaign has a failure marker")
    require(manifest_path.is_file(), f"missing manifest {manifest_path}")
    frozen = manifest_path.read_bytes()
    copied = (root / "manifest.json").read_bytes()
    require(copied == frozen, "copied manifest differs from the frozen manifest")
    manifest = json.loads(frozen)
    manifest_sha = hashlib.sha256(frozen).hexdigest()
    identity = environment(root / "environment.txt")
    require(identity.get("manifest_sha256") == manifest_sha, "manifest SHA-256 mismatch")
    source_sha = identity.get("source_sha256", "")
    require(re.fullmatch(r"[0-9a-f]{64}", source_sha) is not None, "invalid source SHA-256")
    require(identity.get("architecture") == manifest["architecture"], "architecture mismatch")
    require(identity.get("instance_type") == manifest["instance_type"], "instance type mismatch")
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
        int(writers) for writers in manifest.get("throughput_gate_writers", manifest["writers"])
    }
    require(
        throughput_gate_writers <= {int(writers) for writers in manifest["writers"]},
        "throughput gate writers are outside the frozen matrix",
    )

    frozen_cells = {
        (int(cell_count), repetition, int(lanes), int(writers))
        for cell_count in manifest["cell_counts"]
        for repetition in range(1, int(manifest["repetitions"]) + 1)
        for lanes in worker_lanes
        for writers in manifest["writers"]
    }
    if terminal_cell is None:
        expected_cells = frozen_cells
    else:
        require(terminal_cell in frozen_cells, "terminal cell is outside the frozen matrix")
        expected_cells = {terminal_cell}
    observed_cells: set[tuple[int, int, int, int]] = set()
    expected_sample_total = 0
    for cell_count, repetition, lanes, writers in sorted(expected_cells):
        cell = root / "cells" / f"c{cell_count}" / f"r{repetition:02d}" / f"l{lanes}" / f"w{writers}"
        require((cell / "CELL_COMPLETE").is_file(), f"cell is incomplete: {cell}")
        require(not (cell / "CELL_FAILED").exists(), f"cell has a failure marker: {cell}")
        for marker in PHASE_MARKERS:
            require((cell / marker).is_file(), f"missing phase marker {marker} in {cell}")
        summary_rows = rows(cell / "summary.csv")
        require(len(summary_rows) == 1, f"{cell} must contain one summary row")
        summary = summary_rows[0]
        require(summary["source_sha256"] == source_sha, f"source identity drift in {cell}")
        if dataset_sha is not None:
            require(summary["dataset_sha256"] == dataset_sha, f"dataset identity drift in {cell}")
        require(summary["manifest_sha256"] == manifest_sha, f"manifest identity drift in {cell}")
        require(integer(summary["writers"], "writers") == writers, f"writer drift in {cell}")
        operations = int(manifest["operations_per_writer"])
        expected_records = writers * operations
        require(integer(summary["operations"], "operations") == operations, f"operation drift in {cell}")
        require(
            integer(summary["pipeline_depth"], "pipeline depth")
            == int(manifest.get("pipeline_depth_per_writer", 1)),
            f"pipeline depth drift in {cell}",
        )
        require(
            integer(summary["worker_lanes"], "worker lanes")
            == lanes,
            f"worker lane drift in {cell}",
        )
        require(integer(summary["records"], "records") == expected_records, f"record drift in {cell}")
        require(integer(summary["visible_records"], "visible records") == expected_records, f"visibility failure in {cell}")
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
        ):
            require(finite(summary[field], field) >= 0.0, f"negative {field} in {cell}")
        if manifest.get("protocol_kind") in {"production", "architecture-qualification"}:
            require(
                finite(summary["p95_ms"], "p95_ms") < float(manifest["max_write_p95_ms"]),
                f"production p95 gate failed in {cell}",
            )
            if writers in throughput_gate_writers:
                require(
                    finite(summary["records_per_second"], "records_per_second")
                    >= float(manifest["min_records_per_second"]),
                    f"production throughput gate failed in {cell}",
                )
                require(
                    finite(
                        summary["end_to_end_records_per_second"],
                        "end_to_end_records_per_second",
                    )
                    >= float(manifest["min_end_to_end_records_per_second"]),
                    f"production end-to-end throughput gate failed in {cell}",
                )
            require(
                finite(summary["read_p95_ms"], "read_p95_ms")
                < float(manifest["max_read_p95_ms"]),
                f"production read p95 gate failed in {cell}",
            )
            require(
                (cell / "PRODUCTION_PERFORMANCE_GATE_COMPLETE").is_file(),
                f"missing production performance gate marker in {cell}",
            )
            require(
                not (cell / "PRODUCTION_PERFORMANCE_GATE_FAILED").exists(),
                f"production performance failure marker in {cell}",
            )
            subgate_failures = (
                "PRODUCTION_WRITE_P95_FAILED",
                "PRODUCTION_WRITE_THROUGHPUT_FAILED",
                "PRODUCTION_END_TO_END_THROUGHPUT_FAILED",
                "PRODUCTION_READ_P95_FAILED",
                "PRODUCTION_INSERTED_ID_RECALL_AT_10_FAILED",
            )
            require(
                not any((cell / marker).exists() for marker in subgate_failures),
                f"production sub-gate failure marker in {cell}",
            )

        samples = rows(cell / "samples.csv")
        require(len(samples) == expected_records, f"raw sample count mismatch in {cell}")
        ids: set[str] = set()
        groups: dict[tuple[int, int], tuple[int, int, int, int, int]] = {}
        writer_operations: set[tuple[int, int]] = set()
        write_latencies: list[float] = []
        for sample in samples:
            writer = integer(sample["writer"], "sample writer")
            operation = integer(sample["operation"], "sample operation")
            require(0 <= writer < writers and 0 <= operation < operations, f"sample coordinate out of range in {cell}")
            require((writer, operation) not in writer_operations, f"duplicate sample coordinate in {cell}")
            writer_operations.add((writer, operation))
            require(sample["record_id"] not in ids, f"duplicate record id in {cell}")
            ids.add(sample["record_id"])
            sample_latency = finite(sample["latency_ms"], "sample latency")
            require(sample_latency >= 0.0, f"negative sample latency in {cell}")
            write_latencies.append(sample_latency)
            group = (
                integer(sample["commit_lane"], "commit lane"),
                integer(sample["commit_sequence"], "commit sequence"),
            )
            evidence = tuple(
                integer(sample[field], field)
                for field in (
                    "committed_records",
                    "acknowledgement_bytes",
                    "group_requests",
                    "group_gets",
                    "group_puts",
                    "group_heads",
                )
            )
            require(group not in groups or groups[group] == evidence, f"inconsistent shared group evidence in {cell}")
            groups[group] = evidence
        require(sum(evidence[0] for evidence in groups.values()) == expected_records, f"group record reconciliation failed in {cell}")
        total_acknowledgement_bytes = sum(evidence[1] for evidence in groups.values())
        max_acknowledgement_bytes = max(evidence[1] for evidence in groups.values())
        require(
            total_acknowledgement_bytes
            == integer(summary["total_acknowledgement_bytes"], "total acknowledgement bytes"),
            f"acknowledgement byte total drift in {cell}",
        )
        require(
            max_acknowledgement_bytes
            == integer(summary["max_acknowledgement_bytes"], "maximum acknowledgement bytes"),
            f"acknowledgement byte maximum drift in {cell}",
        )
        require(
            max_acknowledgement_bytes <= int(manifest["max_acknowledgement_bytes"]),
            f"acknowledgement byte bound exceeded in {cell}",
        )
        storage_events = rows(cell / "storage-access.csv")
        require(bool(storage_events), f"missing physical storage evidence in {cell}")
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
        require(total_requests == integer(summary["storage_requests"], "storage requests"), f"request reconciliation failed in {cell}")
        require(len(groups) == integer(summary["groups"], "groups"), f"group count mismatch in {cell}")
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
            ("storage_heads", 5),
        ):
            require(
                integer(summary[summary_field], summary_field)
                == sum(evidence[evidence_index] for evidence in groups.values()),
                f"{summary_field} reconciliation failed in {cell}",
            )

        try:
            read_samples = rows(cell / "reads.csv")
        except ValidationError as error:
            raise ValidationError(f"missing raw read sample telemetry in {cell}") from error
        require(
            len(read_samples) == int(manifest["read_queries_per_cell"]),
            f"raw read sample count mismatch in {cell}",
        )
        observed_read_requests = 0
        observed_read_bytes = 0
        observed_read_segments = 0
        read_latencies: list[float] = []
        recall_hits = 0
        for query, read in enumerate(read_samples):
            require(integer(read["query"], "read query") == query, f"read query order drift in {cell}")
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
            require(sum(parts) == request_total, f"raw read request reconciliation failed in {cell}")
            observed_read_requests += request_total
            observed_read_bytes += integer(read["bytes_read"], "raw read bytes")
            observed_read_segments += integer(read["segments_searched"], "raw read segments")
        require(observed_read_requests == read_request_total, f"read request total drift in {cell}")
        require(observed_read_bytes == read_bytes, f"read byte total drift in {cell}")
        require(observed_read_segments == read_segments, f"read segment total drift in {cell}")
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

        resource_path = cell / "resources.csv"
        require(resource_path.is_file(), f"missing resource telemetry {resource_path}")
        resource_rows = rows(resource_path)
        require(
            len(resource_rows) >= 2,
            f"resource telemetry must contain initial and terminal samples in {cell}",
        )
        for field in ("elapsed_ms", "cpu_percent", "rss_bytes"):
            require(
                all(finite(row[field], f"resource {field}") >= 0.0 for row in resource_rows),
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
        require(
            (cell / "process_exit.txt").read_text(encoding="utf-8").strip() == "0",
            f"nonzero resource exit in {cell}",
        )
        observed_cells.add((cell_count, repetition, lanes, writers))
        expected_sample_total += expected_records

    require(observed_cells == expected_cells, "matrix coverage mismatch")
    if terminal_cell is not None:
        return
    aggregate_summary = rows(root / "summary.csv")
    aggregate_samples = rows(root / "samples.csv")
    aggregate_reads = rows(root / "reads.csv")
    require(len(aggregate_summary) == len(expected_cells), "aggregate summary count mismatch")
    require(len(aggregate_samples) == expected_sample_total, "aggregate sample count mismatch")
    require(
        len(aggregate_reads) == len(expected_cells) * int(manifest["read_queries_per_cell"]),
        "aggregate read sample count mismatch",
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

    correctness = rows(root / "correctness.csv")
    expected_gates = set(manifest["correctness_gates"])
    require({row["gate"] for row in correctness} == expected_gates, "correctness gate coverage mismatch")
    require(all(row["status"] == "pass" for row in correctness), "correctness gate failure")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument(
        "--terminal-cell",
        metavar="cCELLS/rREPETITION/lLANES/wWRITERS",
        help="validate one completed cell without reading an incomplete campaign aggregate",
    )
    args = parser.parse_args()
    terminal_cell = None
    if args.terminal_cell is not None:
        match = re.fullmatch(r"c(\d+)/r(\d+)/l(\d+)/w(\d+)", args.terminal_cell)
        if match is None:
            parser.error("--terminal-cell must match cCELLS/rREPETITION/lLANES/wWRITERS")
        terminal_cell = tuple(int(value) for value in match.groups())
    try:
        validate(args.root, args.manifest, terminal_cell=terminal_cell)
    except (OSError, KeyError, TypeError, json.JSONDecodeError, ValidationError) as error:
        print(f"group-commit scalability validation failed: {error}")
        return 1
    scope = f"terminal cell {args.terminal_cell}" if args.terminal_cell else "artifacts"
    print(f"group-commit scalability {scope} are structurally valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
