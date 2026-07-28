#!/usr/bin/env python3
"""Replay real BORSUK Parquet objects as Parquet and Vortex table formats.

The input is an existing local index directory or ``s3://bucket/prefix``.  This
script never creates AWS infrastructure.  For S3 inputs, source Parquet is read
through PyArrow's native S3 filesystem; converted Vortex files stay local unless
an explicit ``--s3-materialized-prefix`` is supplied.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import resource
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Sequence
from urllib.parse import urlparse

FAMILIES = (
    "wal",
    "tombstones",
    "segments",
    "graphs",
    "routing",
    "manifests",
    "global-pq-descriptors",
    "quantizer",
    "lexical",
)
FORMAT_LAYOUTS = (
    ("parquet", "source"),
    ("vortex", "default"),
    ("vortex", "compact"),
)


@dataclass(frozen=True)
class SourceSpec:
    backend: str
    root: str
    bucket: str = ""
    prefix: str = ""


@dataclass(frozen=True)
class ObjectRef:
    backend: str
    storage_path: str
    relative_path: str
    family: str
    bytes: int
    filesystem: Any | None = field(default=None, compare=False, repr=False)
    bucket: str = ""
    key: str = ""
    region: str = ""


@dataclass(frozen=True)
class ColumnProfile:
    name: str
    kind: str
    sample: Any
    minimum: Any | None = None
    maximum: Any | None = None
    arrow_type: Any | None = field(default=None, compare=False, repr=False)


@dataclass(frozen=True)
class PredicateSpec:
    kind: str
    column: str
    value: Any
    upper: Any | None = None


@dataclass(frozen=True)
class TraceSpec:
    name: str
    operation: str
    columns: tuple[str, ...] | None
    predicate: PredicateSpec | None
    blocker: str = ""


@dataclass(frozen=True)
class Variant:
    format: str
    layout: str
    path: str
    bytes: int
    status: str
    blocker: str
    backend: str = "local_disk"
    filesystem: Any | None = field(default=None, compare=False, repr=False)
    bucket: str = ""
    key: str = ""
    region: str = ""


@dataclass(frozen=True)
class ReplayResult:
    """Fully materialized logical result returned inside the timed boundary."""

    rows: int
    logical_checksum: str
    materialized: bool = True


def peak_rss_bytes() -> int:
    """Normalize getrusage's platform-specific maximum-RSS units to bytes."""
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


FAMILY_HINTS: dict[str, dict[str, tuple[str, ...]]] = {
    "wal": {
        "projection": ("record_id", "id", "sequence", "operation"),
        "point": ("record_id", "id"),
        "range": ("sequence", "seq", "version", "created_at_ms"),
        "filter": ("operation", "deleted", "level"),
    },
    "segments": {
        "projection": ("record_id", "id", "routing_code", "pq_codes"),
        "point": ("record_id", "id"),
        "range": ("sequence", "seq", "created_at_ms", "row_id"),
        "filter": ("routing_code", "level", "deleted"),
    },
    "tombstones": {
        "projection": ("record_id", "min_visible_generation"),
        "point": ("record_id",),
        "range": ("min_visible_generation",),
        "filter": ("min_visible_generation",),
    },
    "graphs": {
        "projection": ("source_record_index", "neighbor_record_index"),
        "point": ("source_record_index",),
        "range": ("source_record_index", "neighbor_record_index"),
        "filter": ("level", "neighbor_distance"),
    },
    "routing": {
        "projection": ("segment_path", "path", "level", "row_start"),
        "point": ("segment_path", "path", "segment_id", "id"),
        "range": ("level", "row_start", "row_end", "version"),
        "filter": ("level", "kind", "radius"),
    },
    "manifests": {
        "projection": ("version", "dimensions", "metric", "segment_count"),
        "point": ("version", "manifest_version"),
        "range": ("version", "created_at_ms", "dimensions"),
        "filter": ("metric", "leaf_capability", "quantizer"),
    },
    "global-pq-descriptors": {
        "projection": ("path", "rows", "code_width", "location_width"),
        "point": ("path", "checksum", "segment_id"),
        "range": ("rows", "row_count", "code_width"),
        "filter": ("vector_element_type", "quantizer_kind", "location_width"),
    },
    "quantizer": {
        "projection": ("quantizer_json", "format_version"),
        "point": ("format_version",),
        "range": ("format_version",),
        "filter": ("quantizer_json",),
    },
    "lexical": {
        "projection": ("term", "document_id", "row_id", "frequency"),
        "point": ("term", "document_id", "row_id"),
        "range": ("term", "document_id", "row_id", "row_start"),
        "filter": ("kind", "frequency", "document_frequency_delta"),
    },
}


def classify_object_family(path: str) -> str | None:
    """Return the BORSUK durable-table family for a relative Parquet path."""
    normalized = path.replace("\\", "/").lstrip("/")
    if not normalized.lower().endswith(".parquet"):
        return None
    if normalized.startswith("cells/") and "/wal/" in normalized:
        if "/runs/records/" in normalized:
            return "wal"
        if "/runs/tombstones/" in normalized:
            return "tombstones"
        return None
    if normalized.startswith("global-pq/descriptors/"):
        return "global-pq-descriptors"
    for family in (
        "segments",
        "graphs",
        "routing",
        "manifests",
        "quantizer",
        "lexical",
    ):
        if normalized.startswith(f"{family}/"):
            return family
    return None


def parse_source(value: str) -> SourceSpec:
    parsed = urlparse(value)
    if parsed.scheme:
        if parsed.scheme != "s3":
            raise ValueError("source must be a local directory or an s3:// prefix")
        prefix = parsed.path.strip("/")
        if not parsed.netloc:
            raise ValueError("S3 source requires a bucket")
        if not prefix:
            raise ValueError("S3 source requires a non-empty prefix")
        return SourceSpec("s3", value, parsed.netloc, prefix)
    root = Path(value).expanduser()
    if not root.is_dir():
        raise ValueError(f"local source is not a directory: {root}")
    return SourceSpec("local_disk", str(root.resolve()))


def _load_pyarrow() -> tuple[Any, Any, Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.dataset as ds
        import pyarrow.parquet as pq
    except ImportError as error:
        raise RuntimeError(
            "this benchmark requires scripts/requirements-format-bench.txt"
        ) from error
    return pa, pc, ds, pq


def _load_vortex() -> Any:
    try:
        import vortex as vx
    except ImportError as error:
        raise RuntimeError(
            "Vortex layouts require scripts/requirements-format-bench.txt"
        ) from error
    return vx


def discover_objects(source_value: str, *, region: str = "") -> list[ObjectRef]:
    source = parse_source(source_value)
    if source.backend == "local_disk":
        root = Path(source.root)
        objects = []
        for path in root.rglob("*.parquet"):
            if not path.is_file():
                continue
            relative = path.relative_to(root).as_posix()
            family = classify_object_family(relative)
            if family is not None:
                objects.append(
                    ObjectRef(
                        "local_disk",
                        str(path),
                        relative,
                        family,
                        path.stat().st_size,
                    )
                )
        return sorted(objects, key=lambda item: item.relative_path)

    try:
        import pyarrow.fs as pafs
    except ImportError as error:
        raise RuntimeError(
            "native S3 discovery requires scripts/requirements-format-bench.txt"
        ) from error
    filesystem = pafs.S3FileSystem(region=region or None)
    root = f"{source.bucket}/{source.prefix}"
    infos = filesystem.get_file_info(pafs.FileSelector(root, recursive=True))
    objects = []
    relative_prefix = f"{root.rstrip('/')}/"
    for info in infos:
        if info.type != pafs.FileType.File:
            continue
        relative = info.path.removeprefix(relative_prefix)
        family = classify_object_family(relative)
        if family is None:
            continue
        key = info.path.removeprefix(f"{source.bucket}/")
        objects.append(
            ObjectRef(
                "s3",
                info.path,
                relative,
                family,
                info.size,
                filesystem,
                source.bucket,
                key,
                region,
            )
        )
    return sorted(objects, key=lambda item: item.relative_path)


def _first_matching(
    columns: Sequence[ColumnProfile],
    hints: Sequence[str],
    allowed: set[str],
) -> ColumnProfile | None:
    by_name = {column.name: column for column in columns if column.kind in allowed}
    for name in hints:
        if name in by_name and by_name[name].sample is not None:
            return by_name[name]
    return next(
        (
            column
            for column in columns
            if column.kind in allowed and column.sample is not None
        ),
        None,
    )


def _projection_columns(
    columns: Sequence[ColumnProfile], hints: Sequence[str]
) -> tuple[str, ...]:
    by_name = {column.name for column in columns}
    selected = [name for name in hints if name in by_name][:2]
    if not selected:
        selected = [column.name for column in columns[:2]]
    return tuple(selected)


def plan_traces(
    family: str, columns: Sequence[ColumnProfile], rows: int
) -> tuple[TraceSpec, ...]:
    """Build schema-safe, family-guided traces without casting any column."""
    if family not in FAMILIES:
        raise ValueError(f"unknown BORSUK object family: {family}")
    if rows < 0:
        raise ValueError("rows must be nonnegative")
    hints = FAMILY_HINTS[family]
    scalar = {
        "binary",
        "string",
        "integer",
        "floating",
        "boolean",
        "temporal",
        "decimal",
    }
    ordered = {"integer", "floating", "temporal", "decimal"}
    projection = _projection_columns(columns, hints["projection"])
    traces: list[TraceSpec] = [TraceSpec("projection", "projection", projection, None)]

    point = _first_matching(columns, hints["point"], scalar)
    if point is not None:
        traces.append(
            TraceSpec(
                "point_lookup",
                "point",
                projection,
                PredicateSpec("equal", point.name, point.sample),
            )
        )
    else:
        traces.append(
            TraceSpec(
                "point_lookup",
                "point",
                projection,
                None,
                "schema has no non-null scalar point key; coercion is forbidden",
            )
        )

    ranged = _first_matching(columns, hints["range"], ordered)
    if ranged is not None:
        lower = ranged.minimum if ranged.minimum is not None else ranged.sample
        upper = ranged.maximum if ranged.maximum is not None else ranged.sample
        traces.append(
            TraceSpec(
                "range_lookup",
                "range",
                projection,
                PredicateSpec("range", ranged.name, lower, upper),
            )
        )
    else:
        traces.append(
            TraceSpec(
                "range_lookup",
                "range",
                projection,
                None,
                "schema has no non-null ordered range key; coercion is forbidden",
            )
        )

    filtered = _first_matching(columns, hints["filter"], scalar)
    if filtered is not None:
        traces.append(
            TraceSpec(
                "filtered_scan",
                "filtered_scan",
                projection,
                PredicateSpec("equal", filtered.name, filtered.sample),
            )
        )
    else:
        traces.append(
            TraceSpec(
                "filtered_scan",
                "filtered_scan",
                projection,
                None,
                "schema has no non-null scalar filter key; coercion is forbidden",
            )
        )
    traces.append(TraceSpec("full_scan", "full_scan", None, None))
    return tuple(traces)


def _exception_text(error: BaseException) -> str:
    return f"{type(error).__name__}: {str(error).replace(chr(10), ' ')}"


def _default_vortex_writer(layout: str, table: Any, path: Path) -> None:
    vx = _load_vortex()
    options = (
        vx.io.VortexWriteOptions.compact()
        if layout == "compact"
        else vx.io.VortexWriteOptions.default()
    )
    options.write(table, str(path))


def _upload_file(
    path: Path, filesystem: Any, destination: str, *, refuse_overwrite: bool = True
) -> None:
    if refuse_overwrite:
        info = filesystem.get_file_info(destination)
        try:
            import pyarrow.fs as pafs
        except ImportError as error:
            raise RuntimeError("PyArrow is required for S3 upload") from error
        if info.type != pafs.FileType.NotFound:
            raise FileExistsError(f"refusing to overwrite s3://{destination}")
    with path.open("rb") as source, filesystem.open_output_stream(destination) as sink:
        while chunk := source.read(8 * 1024 * 1024):
            sink.write(chunk)


def materialize_variants(
    source: ObjectRef,
    table: Any,
    output_dir: Path,
    *,
    vortex_writer: Callable[[str, Any, Path], None] = _default_vortex_writer,
    selected: Sequence[tuple[str, str]] = FORMAT_LAYOUTS,
    s3_materialized_prefix: str = "",
) -> list[Variant]:
    """Write Vortex layouts from the exact Arrow table; never cast or coerce."""
    variants: list[Variant] = []
    selected_set = set(selected)
    if ("parquet", "source") in selected_set:
        variants.append(
            Variant(
                "parquet",
                "source",
                source.storage_path,
                source.bytes,
                "ready",
                "",
                source.backend,
                source.filesystem,
                source.bucket,
                source.key,
                source.region,
            )
        )

    digest = hashlib.sha256(source.relative_path.encode()).hexdigest()[:16]
    target_dir = output_dir / source.family / digest
    target_dir.mkdir(parents=True, exist_ok=True)
    stem = PurePosixPath(source.relative_path).name.removesuffix(".parquet")
    for format_name, layout in selected:
        if format_name != "vortex":
            continue
        path = target_dir / f"{stem}.{layout}.vortex"
        try:
            if path.exists():
                raise FileExistsError(f"refusing to overwrite {path}")
            vortex_writer(layout, table, path)
            backend = "local_disk"
            storage_path = str(path)
            filesystem = None
            bucket = ""
            key = ""
            if source.backend == "s3" and s3_materialized_prefix:
                key = (
                    f"{s3_materialized_prefix.strip('/')}/"
                    f"{source.family}/{digest}/{path.name}"
                )
                storage_path = f"{source.bucket}/{key}"
                _upload_file(path, source.filesystem, storage_path)
                backend = "s3"
                filesystem = source.filesystem
                bucket = source.bucket
            variants.append(
                Variant(
                    "vortex",
                    layout,
                    storage_path,
                    path.stat().st_size,
                    "ready",
                    "",
                    backend,
                    filesystem,
                    bucket,
                    key,
                    source.region,
                )
            )
        except Exception as error:
            variants.append(
                Variant(
                    "vortex",
                    layout,
                    str(path),
                    0,
                    "blocked",
                    f"schema-incompatible: {_exception_text(error)}",
                )
            )
    return variants


def _arrow_kind(pa: Any, data_type: Any) -> str:
    types = pa.types
    if types.is_integer(data_type):
        return "integer"
    if types.is_floating(data_type):
        return "floating"
    if types.is_boolean(data_type):
        return "boolean"
    if types.is_string(data_type) or types.is_large_string(data_type):
        return "string"
    if (
        types.is_binary(data_type)
        or types.is_large_binary(data_type)
        or types.is_fixed_size_binary(data_type)
    ):
        return "binary"
    if (
        types.is_date(data_type)
        or types.is_time(data_type)
        or types.is_timestamp(data_type)
        or types.is_duration(data_type)
    ):
        return "temporal"
    if types.is_decimal(data_type):
        return "decimal"
    return "nested"


def profile_table(table: Any) -> tuple[ColumnProfile, ...]:
    pa, pc, _, _ = _load_pyarrow()
    profiles = []
    for arrow_field, chunked in zip(table.schema, table.columns, strict=True):
        kind = _arrow_kind(pa, arrow_field.type)
        sample = None
        for chunk in chunked.chunks:
            if chunk.null_count == len(chunk):
                continue
            first = pc.drop_null(chunk)
            if len(first):
                sample = first[0].as_py()
                break
        minimum = maximum = None
        if (
            kind in {"integer", "floating", "temporal", "decimal"}
            and sample is not None
        ):
            try:
                min_max = pc.min_max(chunked).as_py()
                minimum = min_max["min"]
                maximum = min_max["max"]
            except (TypeError, ValueError, NotImplementedError):
                pass
        profiles.append(
            ColumnProfile(
                arrow_field.name,
                kind,
                sample,
                minimum,
                maximum,
                arrow_field.type,
            )
        )
    return tuple(profiles)


def read_source_table(source: ObjectRef) -> Any:
    _, _, _, pq = _load_pyarrow()
    return pq.read_table(source.storage_path, filesystem=source.filesystem)


def _arrow_filter(ds: Any, predicate: PredicateSpec | None) -> Any | None:
    if predicate is None:
        return None
    column = ds.field(predicate.column)
    if predicate.kind == "equal":
        return column == predicate.value
    if predicate.kind == "range":
        if predicate.upper == predicate.value:
            return column == predicate.value
        return (column >= predicate.value) & (column <= predicate.upper)
    raise ValueError(f"unknown predicate kind: {predicate.kind}")


def materialized_table_result(table: Any) -> ReplayResult:
    """Hash an Arrow table after decoding values, schema, order, and nulls."""
    pa, _, _, _ = _load_pyarrow()
    combined = table.combine_chunks()
    canonical_fields = []
    for arrow_field in combined.schema:
        data_type = arrow_field.type
        if hasattr(pa.types, "is_binary_view") and pa.types.is_binary_view(data_type):
            data_type = pa.binary()
        elif hasattr(pa.types, "is_string_view") and pa.types.is_string_view(data_type):
            data_type = pa.string()
        canonical_fields.append(
            pa.field(arrow_field.name, data_type, nullable=arrow_field.nullable)
        )
    canonical_schema = pa.schema(canonical_fields)
    if not combined.schema.equals(canonical_schema, check_metadata=True):
        combined = combined.cast(canonical_schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, combined.schema) as writer:
        writer.write_table(combined)
    checksum = hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()
    return ReplayResult(combined.num_rows, checksum)


def prepare_parquet(
    variant: Variant,
    _profiles: Sequence[ColumnProfile],
    *,
    execution_mode: str = "materialized_arrow",
    without_segment_cache: bool = False,
) -> Callable[[TraceSpec], ReplayResult]:
    del without_segment_cache
    _, _, ds, pq = _load_pyarrow()
    parquet_file = pq.ParquetFile(variant.path, filesystem=variant.filesystem)
    _ = parquet_file.metadata
    _ = parquet_file.schema_arrow
    dataset = ds.dataset(variant.path, format="parquet", filesystem=variant.filesystem)

    def execute(trace: TraceSpec) -> ReplayResult:
        validate_execution_contract(
            format_name="parquet",
            execution_mode=execution_mode,
            trace=trace,
        )
        scanner = dataset.scanner(
            columns=list(trace.columns) if trace.columns is not None else None,
            filter=_arrow_filter(ds, trace.predicate),
            use_threads=True,
        )
        return materialized_table_result(scanner.to_table())

    return execute


def _vortex_filter(
    vx: Any,
    ve: Any,
    profiles: Sequence[ColumnProfile],
    predicate: PredicateSpec | None,
) -> Any | None:
    if predicate is None:
        return None
    pa, _, _, _ = _load_pyarrow()
    profiles_by_name = {profile.name: profile for profile in profiles}
    if predicate.column not in profiles_by_name:
        raise ValueError(f"predicate column is absent: {predicate.column}")
    # Preserve the Arrow scalar's inferred logical type.  The table was passed
    # directly to Vortex at write time, so this constructs an expression; it
    # does not cast or rewrite stored data.
    profile = profiles_by_name[predicate.column]
    scalar = pa.scalar(predicate.value, type=profile.arrow_type)
    dtype = vx.DType.from_arrow(profile.arrow_type or scalar.type)
    lower = ve.literal(dtype, predicate.value)
    column = ve.column(predicate.column)
    if predicate.kind == "equal":
        return column == lower
    if predicate.kind == "range":
        if predicate.upper == predicate.value:
            return column == lower
        upper_scalar = pa.scalar(predicate.upper, type=profile.arrow_type)
        upper = ve.literal(
            vx.DType.from_arrow(profile.arrow_type or upper_scalar.type),
            predicate.upper,
        )
        return (column >= lower) & (column <= upper)
    raise ValueError(f"unknown predicate kind: {predicate.kind}")


def prepare_vortex(
    variant: Variant,
    profiles: Sequence[ColumnProfile],
    *,
    execution_mode: str = "materialized_arrow",
    without_segment_cache: bool = False,
) -> Callable[[TraceSpec], ReplayResult]:
    vx = _load_vortex()
    import vortex.expr as ve

    if variant.backend == "s3":
        if not variant.bucket or not variant.key:
            raise ValueError("native S3 Vortex variant is missing bucket/key")
        parent, filename = variant.key.rsplit("/", 1)
        store = vx.store.S3Store(
            bucket=variant.bucket,
            prefix=parent,
            region=variant.region,
        )
        reader = vx.open(
            filename,
            store=store,
            without_segment_cache=without_segment_cache,
        )
    else:
        reader = vx.open(
            variant.path,
            without_segment_cache=without_segment_cache,
        )
    _ = getattr(reader, "dtype", None)

    def execute(trace: TraceSpec) -> ReplayResult:
        validate_execution_contract(
            format_name="vortex",
            execution_mode=execution_mode,
            trace=trace,
        )
        result = reader.scan(
            list(trace.columns) if trace.columns is not None else None,
            expr=_vortex_filter(vx, ve, profiles, trace.predicate),
        ).read_all()
        return vortex_materialized_result(result, execution_mode)

    return execute


def validate_execution_contract(
    *, format_name: str, execution_mode: str, trace: TraceSpec
) -> None:
    """Reject timings that would compare metadata work with decoded Arrow."""
    if execution_mode not in {"materialized_arrow", "compressed_native"}:
        raise ValueError(f"unknown execution mode: {execution_mode}")
    if format_name == "parquet" and execution_mode != "materialized_arrow":
        raise ValueError("Parquet is only reported under materialized_arrow")
    if format_name == "vortex" and execution_mode == "compressed_native":
        raise ValueError(
            "compressed_native is blocked until a verified value-consuming "
            f"Vortex operation exists for {trace.operation}; len(result) is not timed"
        )


def vortex_result_rows(result: Any, execution_mode: str) -> int:
    """Finish the declared Vortex contract before stopping the timer."""
    if execution_mode == "materialized_arrow":
        arrow_table = result.to_arrow_table()
        if hasattr(arrow_table, "num_rows"):
            return int(arrow_table.num_rows)
        return len(arrow_table)
    if execution_mode == "compressed_native":
        raise ValueError(
            "compressed_native requires a verified value-consuming Vortex operation"
        )
    raise ValueError(f"unknown execution mode: {execution_mode}")


def vortex_materialized_result(result: Any, execution_mode: str) -> ReplayResult:
    """Finish Vortex decode and checksum its materialized Arrow values."""
    if execution_mode == "materialized_arrow":
        arrow_table = result.to_arrow_table()
        return materialized_table_result(arrow_table)
    if execution_mode == "compressed_native":
        raise ValueError(
            "compressed_native requires a verified value-consuming Vortex operation"
        )
    raise ValueError(f"unknown execution mode: {execution_mode}")


def replay_variant(
    *,
    object_ref: ObjectRef,
    variant: Variant,
    traces: Sequence[TraceSpec],
    repetitions: int,
    warmups: int,
    prepare: Callable[[Variant], Callable[[TraceSpec], ReplayResult | int]],
    execution_mode: str,
    cache_state: str = "warm",
    timer: Callable[[], float] = time.perf_counter,
    cpu_timer: Callable[[], float] = time.process_time,
) -> list[dict[str, Any]]:
    if repetitions <= 0:
        raise ValueError("repetitions must be positive")
    if warmups < 0:
        raise ValueError("warmups must be nonnegative")
    # prepare() opens the file/footer and constructs native readers.  It is
    # intentionally outside the timed region.
    execute = prepare(variant)
    rows: list[dict[str, Any]] = []
    for trace in traces:
        if trace.blocker:
            rows.append(
                {
                    "object": object_ref.relative_path,
                    "backend": variant.backend,
                    "family": object_ref.family,
                    "format": variant.format,
                    "layout": variant.layout,
                    "execution_mode": execution_mode,
                    "workload": trace.name,
                    "repetition": "",
                    "elapsed_ms": "",
                    "bytes": variant.bytes,
                    "rows": "",
                    "status": "blocked",
                    "blocker": trace.blocker,
                }
            )
            continue
        try:
            for _ in range(warmups):
                execute(trace)
            for repetition in range(1, repetitions + 1):
                started = timer()
                cpu_started = cpu_timer()
                result = execute(trace)
                decode_cpu_ns = round((cpu_timer() - cpu_started) * 1_000_000_000)
                elapsed_ms = round((timer() - started) * 1_000, 12)
                if isinstance(result, ReplayResult):
                    result_rows = result.rows
                    logical_checksum = result.logical_checksum
                    materialized = result.materialized
                else:
                    result_rows = int(result)
                    logical_checksum = ""
                    materialized = execution_mode == "materialized_arrow"
                rows.append(
                    {
                        "object": object_ref.relative_path,
                        "backend": variant.backend,
                        "family": object_ref.family,
                        "format": variant.format,
                        "layout": variant.layout,
                        "execution_mode": execution_mode,
                        "workload": trace.name,
                        "repetition": repetition,
                        "elapsed_ms": elapsed_ms,
                        "bytes": variant.bytes,
                        "rows": result_rows,
                        "logical_checksum": logical_checksum,
                        "materialized": materialized,
                        "cache_state": cache_state,
                        "requests": 1,
                        "bytes_fetched": variant.bytes,
                        "decode_cpu_ns": decode_cpu_ns,
                        "peak_rss_bytes": peak_rss_bytes(),
                        "status": "complete",
                        "blocker": "",
                    }
                )
        except Exception as error:
            rows.append(
                {
                    "object": object_ref.relative_path,
                    "backend": variant.backend,
                    "family": object_ref.family,
                    "format": variant.format,
                    "layout": variant.layout,
                    "execution_mode": execution_mode,
                    "workload": trace.name,
                    "repetition": "",
                    "elapsed_ms": "",
                    "bytes": variant.bytes,
                    "rows": "",
                    "status": "blocked",
                    "blocker": _exception_text(error),
                }
            )
    return rows


def _percentile(values: Sequence[float], quantile: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def summarize_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = (
        "object",
        "backend",
        "family",
        "format",
        "layout",
        "execution_mode",
        "workload",
        "status",
        "blocker",
    )
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(tuple(row.get(key, "") for key in keys), []).append(row)
    summaries = []
    for group_key, group in groups.items():
        base = dict(zip(keys, group_key, strict=True))
        values = [
            float(row["elapsed_ms"])
            for row in group
            if row.get("status") == "complete" and row.get("elapsed_ms") != ""
        ]
        base["samples"] = len(values)
        if values:
            base.update(
                {
                    "mean_ms": statistics.fmean(values),
                    "stddev_ms": statistics.stdev(values) if len(values) > 1 else 0.0,
                    "p50_ms": _percentile(values, 0.50),
                    "p95_ms": _percentile(values, 0.95),
                    "p99_ms": _percentile(values, 0.99),
                }
            )
        else:
            base.update(
                {
                    "mean_ms": "",
                    "stddev_ms": "",
                    "p50_ms": "",
                    "p95_ms": "",
                    "p99_ms": "",
                }
            )
        base["bytes"] = group[0].get("bytes", "")
        base["rows"] = group[0].get("rows", "")
        summaries.append(base)
    return sorted(
        summaries,
        key=lambda row: (
            row["object"],
            row["format"],
            row["layout"],
            row["workload"],
            row["status"],
        ),
    )


def validate_logical_results(rows: Sequence[dict[str, Any]]) -> None:
    """Fail closed unless every completed format materializes identical values."""
    groups: dict[tuple[str, str, str, str], set[tuple[int, str]]] = {}
    for row in rows:
        if row.get("status") != "complete":
            continue
        materialized = row.get("materialized")
        if materialized is not True and str(materialized).lower() not in {
            "true",
            "1",
            "yes",
        }:
            raise ValueError(
                "completed replay row is not materialized: "
                f"{row.get('object', 'unknown')} {row.get('workload', 'unknown')}"
            )
        checksum = str(row.get("logical_checksum", "")).strip()
        if not checksum:
            raise ValueError(
                "completed replay row has no logical checksum: "
                f"{row.get('object', 'unknown')} {row.get('workload', 'unknown')}"
            )
        key = (
            str(row.get("object", "")),
            str(row.get("family", "")),
            str(row.get("execution_mode", "")),
            str(row.get("workload", "")),
        )
        groups.setdefault(key, set()).add((int(row["rows"]), checksum))
    for key, results in groups.items():
        if len(results) != 1:
            raise ValueError(
                "logical result mismatch across physical formats for "
                f"object={key[0]} family={key[1]} "
                f"execution_mode={key[2]} workload={key[3]}: {sorted(results)}"
            )


def _write_csv(
    path: Path, fieldnames: Sequence[str], rows: Iterable[dict[str, Any]]
) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite {path}")
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: f"{value:.6f}" if isinstance(value, float) else value
                    for key, value in row.items()
                }
            )


def parse_formats(value: str) -> tuple[tuple[str, str], ...]:
    aliases = {
        "parquet": ("parquet", "source"),
        "vortex-default": ("vortex", "default"),
        "vortex-compact": ("vortex", "compact"),
    }
    names = [item.strip() for item in value.split(",") if item.strip()]
    if not names:
        raise ValueError("at least one format is required")
    unsupported = [name for name in names if name not in aliases]
    if unsupported:
        raise ValueError(f"unsupported format(s): {', '.join(unsupported)}")
    if len(names) != len(set(names)):
        raise ValueError("formats must not contain duplicates")
    return tuple(aliases[name] for name in names)


def parse_families(value: str) -> tuple[str, ...]:
    families = tuple(item.strip() for item in value.split(",") if item.strip())
    unsupported = sorted(set(families).difference(FAMILIES))
    if not families:
        raise ValueError("at least one family is required")
    if unsupported:
        raise ValueError(f"unsupported family/families: {', '.join(unsupported)}")
    return families


def parse_execution_modes(value: str) -> tuple[str, ...]:
    modes = tuple(item.strip() for item in value.split(",") if item.strip())
    supported = {"materialized_arrow", "compressed_native"}
    unsupported = sorted(set(modes).difference(supported))
    if not modes:
        raise ValueError("at least one execution mode is required")
    if unsupported:
        raise ValueError(f"unsupported execution mode(s): {', '.join(unsupported)}")
    if len(modes) != len(set(modes)):
        raise ValueError("execution modes must not contain duplicates")
    return modes


def _blocked_variant_rows(
    source: ObjectRef,
    variant: Variant,
    traces: Sequence[TraceSpec],
    *,
    rows: int | str,
    execution_mode: str,
) -> list[dict[str, Any]]:
    workloads = [trace.name for trace in traces] or ["all"]
    return [
        {
            "object": source.relative_path,
            "backend": variant.backend,
            "family": source.family,
            "format": variant.format,
            "layout": variant.layout,
            "execution_mode": execution_mode,
            "workload": workload,
            "repetition": "",
            "elapsed_ms": "",
            "bytes": variant.bytes,
            "rows": rows,
            "status": "blocked",
            "blocker": variant.blocker,
        }
        for workload in workloads
    ]


def run_benchmark(args: argparse.Namespace) -> int:
    if args.repetitions <= 0 or args.warmups < 0:
        raise ValueError("repetitions must be positive and warmups nonnegative")
    selected = parse_formats(args.formats)
    families = set(parse_families(args.families))
    execution_modes = parse_execution_modes(args.execution_modes)
    objects = [
        item
        for item in discover_objects(args.source, region=args.aws_region)
        if item.family in families
    ]
    if not objects:
        raise ValueError("source contains no classified BORSUK Parquet objects")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    materialized = args.output_dir / "materialized"
    raw_rows: list[dict[str, Any]] = []

    for source in objects:
        try:
            table = read_source_table(source)
            profiles = profile_table(table)
            traces = plan_traces(source.family, profiles, table.num_rows)
            variants = materialize_variants(
                source,
                table,
                materialized,
                selected=selected,
                s3_materialized_prefix=args.s3_materialized_prefix,
            )
        except Exception as error:
            blocker = f"source-incompatible: {_exception_text(error)}"
            for format_name, layout in selected:
                for execution_mode in execution_modes:
                    raw_rows.extend(
                        _blocked_variant_rows(
                            source,
                            Variant(
                                format_name,
                                layout,
                                source.storage_path,
                                source.bytes if format_name == "parquet" else 0,
                                "blocked",
                                blocker,
                                source.backend,
                            ),
                            (),
                            rows="",
                            execution_mode=execution_mode,
                        )
                    )
            continue

        for variant in variants:
            for execution_mode in execution_modes:
                if variant.status == "blocked":
                    raw_rows.extend(
                        _blocked_variant_rows(
                            source,
                            variant,
                            traces,
                            rows=table.num_rows,
                            execution_mode=execution_mode,
                        )
                    )
                    continue
                prepare_impl = (
                    prepare_parquet if variant.format == "parquet" else prepare_vortex
                )
                try:
                    raw_rows.extend(
                        replay_variant(
                            object_ref=source,
                            variant=variant,
                            traces=traces,
                            repetitions=args.repetitions,
                            warmups=args.warmups,
                            prepare=lambda item, impl=prepare_impl, mode=execution_mode, current_profiles=profiles: (
                                impl(
                                    item,
                                    current_profiles,
                                    execution_mode=mode,
                                    without_segment_cache=args.vortex_without_segment_cache,
                                )
                            ),
                            execution_mode=execution_mode,
                        )
                    )
                except Exception as error:
                    failed = Variant(
                        variant.format,
                        variant.layout,
                        variant.path,
                        variant.bytes,
                        "blocked",
                        f"open-metadata: {_exception_text(error)}",
                        variant.backend,
                    )
                    raw_rows.extend(
                        _blocked_variant_rows(
                            source,
                            failed,
                            traces,
                            rows=table.num_rows,
                            execution_mode=execution_mode,
                        )
                    )

    validate_logical_results(raw_rows)
    sample_fields = (
        "object",
        "backend",
        "family",
        "format",
        "layout",
        "execution_mode",
        "workload",
        "repetition",
        "elapsed_ms",
        "bytes",
        "rows",
        "logical_checksum",
        "materialized",
        "cache_state",
        "requests",
        "bytes_fetched",
        "decode_cpu_ns",
        "peak_rss_bytes",
        "status",
        "blocker",
    )
    summary_fields = (
        "object",
        "backend",
        "family",
        "format",
        "layout",
        "execution_mode",
        "workload",
        "status",
        "blocker",
        "samples",
        "mean_ms",
        "stddev_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "bytes",
        "rows",
    )
    _write_csv(args.output_dir / "samples.csv", sample_fields, raw_rows)
    summaries = summarize_rows(raw_rows)
    _write_csv(args.output_dir / "summary.csv", summary_fields, summaries)
    return 2 if any(row["status"] == "blocked" for row in raw_rows) else 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay actual BORSUK Parquet artifacts as Parquet and Vortex."
    )
    parser.add_argument(
        "source",
        help="Existing local BORSUK directory or s3://bucket/prefix.",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--formats",
        default="parquet,vortex-default,vortex-compact",
    )
    parser.add_argument("--families", default=",".join(FAMILIES))
    parser.add_argument("--repetitions", type=int, default=30)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument(
        "--execution-modes",
        default="materialized_arrow",
        help=(
            "Comma-separated execution contracts. materialized_arrow is the "
            "fair default. compressed_native currently emits blocked cells "
            "because no verified value-consuming native operation is defined."
        ),
    )
    parser.add_argument("--aws-region", default="eu-central-1")
    parser.add_argument(
        "--s3-materialized-prefix",
        default="",
        help=(
            "Explicit existing-bucket prefix for native-S3 Vortex replay. "
            "If omitted, converted Vortex artifacts remain on local disk."
        ),
    )
    parser.add_argument(
        "--vortex-without-segment-cache",
        action="store_true",
        help="Disable the Vortex reader's in-process segment cache.",
    )
    return parser.parse_args(argv)


def main() -> int:
    return run_benchmark(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
