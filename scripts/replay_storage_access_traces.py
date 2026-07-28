#!/usr/bin/env python3
"""Build and enforce a checked replay contract for BORSUK storage traces.

The benchmark executor may evolve independently, but a replay is publishable
only when this gate proves that its immutable inputs, operations, cache state,
materialization boundary, repetitions, and logical results are paired.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Sequence

SCHEMA_VERSION = 1
TRACE_REQUIRED_FIELDS = {
    "operation",
    "object_role",
    "path",
    "physical_format",
    "object_bytes",
    "logical_projection",
    "row_selection",
    "logical_rows_requested",
    "logical_rows_decoded",
    "cache_state",
    "status",
}
REPLAY_REQUIRED_FIELDS = {
    "trace_id",
    "format",
    "repetition",
    "elapsed_ms",
    "requests",
    "bytes_fetched",
    "decode_cpu_ns",
    "peak_rss_bytes",
    "logical_checksum",
    "materialized",
    "cache_state",
    "status",
}
REPLAYABLE_OBJECT_ROLES = {"normal_segment"}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _positive_int(value: Any, field: str, *, allow_zero: bool = False) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field} must be an integer") from error
    minimum = 0 if allow_zero else 1
    if parsed < minimum:
        qualifier = "nonnegative" if allow_zero else "positive"
        raise ValueError(f"{field} must be {qualifier}")
    return parsed


def _nonnegative_float(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field} must be numeric") from error
    if parsed < 0:
        raise ValueError(f"{field} must be nonnegative")
    return parsed


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"materialized must be boolean, got {value!r}")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path}")
        return list(reader)


def _trace_id(operation: dict[str, Any]) -> str:
    canonical = json.dumps(
        operation, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def build_manifest(
    trace_path: Path,
    source_root: Path,
    *,
    formats: Sequence[str],
    minimum_samples: int = 30,
) -> dict[str, Any]:
    """Normalize real decode events and checksum every referenced source."""
    if len(formats) < 2 or len(set(formats)) != len(formats):
        raise ValueError("replay requires at least two unique formats")
    minimum_samples = _positive_int(minimum_samples, "minimum_samples")
    rows = read_csv(trace_path)
    if not rows:
        raise ValueError("storage trace is empty")
    missing = TRACE_REQUIRED_FIELDS.difference(rows[0])
    if missing:
        raise ValueError(f"storage trace missing fields: {', '.join(sorted(missing))}")

    operations_by_id: dict[str, dict[str, Any]] = {}
    source_objects: dict[str, str] = {}
    for row in rows:
        if row["operation"] != "decode" or row["status"] != "ok":
            continue
        if row["object_role"] not in REPLAYABLE_OBJECT_ROLES:
            continue
        relative = row["path"].strip().replace("\\", "/").lstrip("/")
        if not relative or relative.startswith("../") or "/../" in relative:
            raise ValueError(f"unsafe traced source path: {row['path']!r}")
        source = source_root / relative
        if not source.is_file():
            raise ValueError(f"traced source object is missing: {relative}")
        actual_bytes = source.stat().st_size
        traced_bytes = _positive_int(row["object_bytes"], "object_bytes")
        if actual_bytes != traced_bytes:
            raise ValueError(
                f"traced object size changed for {relative}: "
                f"trace={traced_bytes}, actual={actual_bytes}"
            )
        requested = _positive_int(
            row["logical_rows_requested"],
            "logical_rows_requested",
            allow_zero=True,
        )
        decoded = _positive_int(
            row["logical_rows_decoded"],
            "logical_rows_decoded",
            allow_zero=True,
        )
        if requested > decoded:
            raise ValueError(f"trace requests more rows than it decoded: {relative}")
        operation = {
            "object_role": row["object_role"],
            "path": relative,
            "source_format": row["physical_format"],
            "object_bytes": actual_bytes,
            "projection": [
                item for item in row["logical_projection"].split("|") if item
            ],
            "row_selection": row["row_selection"],
            "logical_rows_requested": requested,
            "logical_rows_decoded": decoded,
            "cache_state": row["cache_state"],
        }
        trace_id = _trace_id(operation)
        operation["trace_id"] = trace_id
        operations_by_id[trace_id] = operation
        source_objects[relative] = sha256_file(source)
    if not operations_by_id:
        raise ValueError("storage trace contains no successful decode operation")
    return {
        "schema_version": SCHEMA_VERSION,
        "execution_contract": "materialized_arrow",
        "trace_sha256": sha256_file(trace_path),
        "formats": list(formats),
        "minimum_samples": minimum_samples,
        "source_objects": dict(sorted(source_objects.items())),
        "operations": sorted(
            operations_by_id.values(), key=lambda item: item["trace_id"]
        ),
    }


def _verify_sources(
    manifest: dict[str, Any],
    source_root: Path,
) -> None:
    for relative, expected in manifest["source_objects"].items():
        path = source_root / relative
        actual = sha256_file(path) if path.is_file() else "missing"
        if actual != expected:
            raise ValueError(
                f"source checksum changed for {relative}: "
                f"expected {expected}, got {actual}"
            )


def validate_replay(
    manifest: dict[str, Any],
    rows: Sequence[dict[str, Any]],
    source_root: Path,
) -> None:
    """Reject any replay that cannot support a paired correctness claim."""
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported replay manifest schema version")
    if manifest.get("execution_contract") != "materialized_arrow":
        raise ValueError("replay must use the materialized_arrow contract")
    _verify_sources(manifest, source_root)
    if not rows:
        raise ValueError("replay contains no samples")
    missing = REPLAY_REQUIRED_FIELDS.difference(rows[0])
    if missing:
        raise ValueError(f"replay missing fields: {', '.join(sorted(missing))}")

    operations = {
        operation["trace_id"]: operation for operation in manifest["operations"]
    }
    formats = tuple(manifest["formats"])
    minimum_samples = _positive_int(manifest["minimum_samples"], "minimum_samples")
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        trace_id = str(row["trace_id"])
        format_name = str(row["format"])
        if trace_id not in operations:
            raise ValueError(f"replay has unknown trace_id: {trace_id}")
        if format_name not in formats:
            raise ValueError(f"replay has unexpected format: {format_name}")
        if row["status"] != "complete":
            raise ValueError("replay contains incomplete or blocked samples")
        if not _bool(row["materialized"]):
            raise ValueError("every replay sample must be materialized")
        if row["cache_state"] != operations[trace_id]["cache_state"]:
            raise ValueError(f"replay cache state drifted for {trace_id}")
        if not str(row["logical_checksum"]):
            raise ValueError("replay logical checksum is missing")
        _positive_int(row["repetition"], "repetition")
        _nonnegative_float(row["elapsed_ms"], "elapsed_ms")
        _positive_int(row["requests"], "requests", allow_zero=True)
        _positive_int(row["bytes_fetched"], "bytes_fetched", allow_zero=True)
        _positive_int(row["decode_cpu_ns"], "decode_cpu_ns", allow_zero=True)
        _positive_int(row["peak_rss_bytes"], "peak_rss_bytes", allow_zero=True)
        groups.setdefault((trace_id, format_name), []).append(row)

    expected_groups = {
        (trace_id, format_name) for trace_id in operations for format_name in formats
    }
    if set(groups) != expected_groups:
        missing_groups = expected_groups.difference(groups)
        raise ValueError(f"replay has unpaired format groups: {sorted(missing_groups)}")

    for trace_id in operations:
        repetition_sets = []
        for format_name in formats:
            group = groups[(trace_id, format_name)]
            repetitions = [int(row["repetition"]) for row in group]
            if len(repetitions) != len(set(repetitions)):
                raise ValueError(f"duplicate repetition for {trace_id}/{format_name}")
            repetition_sets.append(set(repetitions))
        if any(current != repetition_sets[0] for current in repetition_sets[1:]):
            raise ValueError(f"replay lacks paired repetitions for {trace_id}")
        if len(repetition_sets[0]) < minimum_samples:
            raise ValueError(
                f"replay requires at least {minimum_samples} samples per cell"
            )
        checksums = {
            str(row["logical_checksum"])
            for format_name in formats
            for row in groups[(trace_id, format_name)]
        }
        if len(checksums) != 1:
            raise ValueError(f"logical checksum differs across replay for {trace_id}")


def write_json(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _benchmark_module() -> Any:
    try:
        from scripts import benchmark_borsuk_table_formats as benchmark
    except ImportError:
        import benchmark_borsuk_table_formats as benchmark
    return benchmark


def _physical_projection(
    requested: Sequence[str],
    available: set[str],
) -> tuple[str, ...]:
    aliases = {
        "record_id": ("record_id",),
        "generation": ("generation",),
        "metadata": ("metadata",),
        "text": ("text_term_ids", "text_term_freqs"),
        "routing_codes": ("routing_code",),
        "pq_codes": ("pq_code",),
    }
    optional_when_absent = {"generation", "metadata", "text"}
    if "*" in requested:
        exclusions = {
            item.removeprefix("-") for item in requested if item.startswith("-")
        }
        return tuple(sorted(column for column in available if column not in exclusions))
    selected: list[str] = []
    for logical in requested:
        if logical.startswith("-"):
            continue
        physical = aliases.get(logical, (logical,))
        found = [column for column in physical if column in available]
        if not found and logical in optional_when_absent:
            continue
        if not found:
            raise ValueError(
                f"traced projection `{logical}` is absent from the source schema"
            )
        selected.extend(found)
    return tuple(dict.fromkeys(selected))


def _write_replay_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite {path}")
    fields = (
        "trace_id",
        "format",
        "repetition",
        "elapsed_ms",
        "requests",
        "bytes_fetched",
        "decode_cpu_ns",
        "peak_rss_bytes",
        "logical_checksum",
        "materialized",
        "cache_state",
        "status",
    )
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def run_manifest_replay(
    manifest: dict[str, Any],
    source_root: Path,
    output_dir: Path,
    *,
    repetitions: int | None = None,
    warmups: int = 3,
) -> Path:
    """Re-encode traced normal segments and execute their exact projection."""
    benchmark = _benchmark_module()
    output_dir.mkdir(parents=True, exist_ok=False)
    materialized_root = output_dir / "materialized"
    required_repetitions = (
        manifest["minimum_samples"] if repetitions is None else repetitions
    )
    required_repetitions = _positive_int(required_repetitions, "repetitions")
    if required_repetitions < int(manifest["minimum_samples"]):
        raise ValueError(
            f"replay requires at least {manifest['minimum_samples']} repetitions"
        )
    selected = benchmark.parse_formats(",".join(manifest["formats"]))
    rows: list[dict[str, Any]] = []
    object_cache: dict[str, tuple[Any, Any, list[Any]]] = {}
    for operation in manifest["operations"]:
        if operation["row_selection"] != "all":
            raise ValueError(
                "checked executor currently requires row_selection=all; "
                "it will not silently replace traced selection semantics"
            )
        relative = operation["path"]
        if relative not in object_cache:
            source_path = source_root / relative
            source = benchmark.ObjectRef(
                backend="local_disk",
                storage_path=str(source_path),
                relative_path=relative,
                family="segments",
                bytes=source_path.stat().st_size,
            )
            table = benchmark.read_source_table(source)
            profiles = benchmark.profile_table(table)
            variants = benchmark.materialize_variants(
                source,
                table,
                materialized_root,
                selected=selected,
            )
            object_cache[relative] = (source, profiles, variants)
        source, profiles, variants = object_cache[relative]
        projection = _physical_projection(
            operation["projection"], {profile.name for profile in profiles}
        )
        trace = benchmark.TraceSpec(
            operation["trace_id"],
            "trace",
            projection,
            None,
        )
        for variant in variants:
            if variant.status != "ready":
                raise ValueError(
                    f"format materialization failed for {relative}: {variant.blocker}"
                )
            prepare_impl = (
                benchmark.prepare_parquet
                if variant.format == "parquet"
                else benchmark.prepare_vortex
            )
            samples = benchmark.replay_variant(
                object_ref=source,
                variant=variant,
                traces=(trace,),
                repetitions=required_repetitions,
                warmups=warmups,
                prepare=lambda item, impl=prepare_impl, current_profiles=profiles: impl(
                    item,
                    current_profiles,
                    execution_mode="materialized_arrow",
                    without_segment_cache=True,
                ),
                execution_mode="materialized_arrow",
                cache_state=operation["cache_state"],
            )
            format_name = (
                "parquet" if variant.format == "parquet" else f"vortex-{variant.layout}"
            )
            for sample in samples:
                rows.append(
                    {
                        "trace_id": operation["trace_id"],
                        "format": format_name,
                        "repetition": sample["repetition"],
                        "elapsed_ms": sample["elapsed_ms"],
                        "requests": sample["requests"],
                        "bytes_fetched": sample["bytes_fetched"],
                        "decode_cpu_ns": sample["decode_cpu_ns"],
                        "peak_rss_bytes": sample["peak_rss_bytes"],
                        "logical_checksum": sample["logical_checksum"],
                        "materialized": sample["materialized"],
                        "cache_state": sample["cache_state"],
                        "status": sample["status"],
                    }
                )
    validate_replay(manifest, rows, source_root)
    samples_path = output_dir / "samples.csv"
    _write_replay_csv(samples_path, rows)
    return samples_path


def parse_formats(value: str) -> tuple[str, ...]:
    formats = tuple(item.strip() for item in value.split(",") if item.strip())
    if not formats:
        raise ValueError("at least one replay format is required")
    return formats


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--trace", type=Path, required=True)
    plan.add_argument("--source-root", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--formats", default="parquet,vortex-default,vortex-compact")
    plan.add_argument("--minimum-samples", type=int, default=30)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--manifest", type=Path, required=True)
    validate.add_argument("--samples", type=Path, required=True)
    validate.add_argument("--source-root", type=Path, required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--source-root", type=Path, required=True)
    run.add_argument("--output-dir", type=Path, required=True)
    run.add_argument("--repetitions", type=int)
    run.add_argument("--warmups", type=int, default=3)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "plan":
        manifest = build_manifest(
            args.trace,
            args.source_root,
            formats=parse_formats(args.formats),
            minimum_samples=args.minimum_samples,
        )
        write_json(args.output, manifest)
        print(
            f"planned {len(manifest['operations'])} checked operations "
            f"over {len(manifest['source_objects'])} immutable objects"
        )
        return 0
    with args.manifest.open() as handle:
        manifest = json.load(handle)
    if args.command == "run":
        samples = run_manifest_replay(
            manifest,
            args.source_root,
            args.output_dir,
            repetitions=args.repetitions,
            warmups=args.warmups,
        )
        print(f"checked replay passed: {samples}")
        return 0
    rows: Iterable[dict[str, Any]] = read_csv(args.samples)
    validate_replay(manifest, list(rows), args.source_root)
    print("replay validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
