#!/usr/bin/env python3
"""Compare exact-vector containers with identical rows and access traces.

This is deliberately a physical-format benchmark, not an ANN benchmark.  It
writes the same Arrow table as Arrow IPC, Parquet, Vortex default, and Vortex
compact, prepares each reader once, then measures clustered and scattered
selected-row retrieval.  Linux CPU/RSS/physical-I/O traces are supplied by
``benchmark_with_resources.py`` around this process.
"""

from __future__ import annotations

import argparse
import csv
import math
import mmap
import os
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

KNOWN_FORMATS = (
    "arrow-ipc",
    "parquet",
    "vortex-default",
    "vortex-compact",
)
KNOWN_ELEMENT_TYPES = ("float32", "float16", "bfloat16", "int8", "binary")


def parse_formats(value: str) -> tuple[str, ...]:
    formats = tuple(item.strip() for item in value.split(",") if item.strip())
    if not formats:
        raise ValueError("at least one format is required")
    unsupported = sorted(set(formats).difference(KNOWN_FORMATS))
    if unsupported:
        raise ValueError(f"unsupported format(s): {', '.join(unsupported)}")
    if len(set(formats)) != len(formats):
        raise ValueError("formats must not contain duplicates")
    return formats


def validate_backend(backend: str, s3_bucket: str, s3_prefix: str) -> None:
    if backend not in {"local_disk", "s3"}:
        raise ValueError("backend must be local_disk or s3")
    if backend == "s3" and (not s3_bucket or not s3_prefix.strip("/")):
        raise ValueError("S3 backend requires an explicit bucket and prefix")


def validate_arrow_io_options(
    max_gap_bytes: int,
    max_parallel: int,
    max_range_bytes: int = 0,
) -> None:
    if max_gap_bytes < 0:
        raise ValueError("Arrow maximum coalescing gap must be nonnegative")
    if max_parallel <= 0:
        raise ValueError("Arrow maximum parallel reads must be positive")
    if max_range_bytes < 0:
        raise ValueError("Arrow maximum physical range must be nonnegative")


def parse_positive_ints(value: str, label: str) -> tuple[int, ...]:
    try:
        values = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as error:
        raise ValueError(f"{label} must be a comma-separated integer list") from error
    if not values or any(item <= 0 for item in values):
        raise ValueError(f"{label} must contain positive integers")
    return values


def make_query_indices(
    *,
    rows: int,
    selected_rows: int,
    repetitions: int,
    pattern: str,
    seed: int,
) -> list[list[int]]:
    if rows <= 0:
        raise ValueError("rows must be positive")
    if selected_rows <= 0 or selected_rows > rows:
        raise ValueError("selected_rows must be between one and rows")
    if repetitions <= 0:
        raise ValueError("repetitions must be positive")
    if pattern not in {"clustered", "scattered"}:
        raise ValueError("pattern must be clustered or scattered")
    rng = random.Random(seed)
    queries: list[list[int]] = []
    for _ in range(repetitions):
        if pattern == "clustered":
            start = rng.randrange(rows - selected_rows + 1)
            indices = list(range(start, start + selected_rows))
        else:
            indices = sorted(rng.sample(range(rows), selected_rows))
        queries.append(indices)
    return queries


def percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        raise ValueError("cannot calculate a percentile of no samples")
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def summarize_samples(values: Sequence[float]) -> dict[str, float | int]:
    if not values:
        raise ValueError("cannot summarize no samples")
    return {
        "samples": len(values),
        "mean_ms": statistics.fmean(values),
        "stddev_ms": statistics.stdev(values) if len(values) > 1 else 0.0,
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "min_ms": min(values),
        "max_ms": max(values),
    }


def load_dependencies() -> tuple[Any, Any, Any]:
    try:
        import numpy as np
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.ipc as ipc
        import pyarrow.parquet as pq
    except ImportError as error:
        raise RuntimeError(
            "format benchmark requires scripts/requirements-format-bench.txt"
        ) from error
    return np, pa, (pc, ipc, pq)


def load_vortex() -> Any:
    try:
        import vortex as vx
    except ImportError as error:
        raise RuntimeError(
            "Vortex formats require vortex-data from "
            "scripts/requirements-format-bench.txt"
        ) from error
    return vx


def _fixed_size_list(pa: Any, values: Any, value_type: Any, dimensions: int) -> Any:
    primitive = pa.array(values.reshape(-1), type=value_type)
    return pa.FixedSizeListArray.from_arrays(primitive, dimensions)


def create_table(
    *,
    rows: int,
    dimensions: int,
    element_type: str,
    seed: int,
    input_npy: Path | None,
) -> tuple[Any, int]:
    np, pa, _ = load_dependencies()
    if element_type not in KNOWN_ELEMENT_TYPES:
        raise ValueError(f"unsupported element type {element_type!r}")
    if dimensions <= 0 or rows <= 0:
        raise ValueError("rows and dimensions must be positive")
    if input_npy is not None:
        values = np.load(input_npy, mmap_mode="r")
        if values.ndim != 2 or values.shape != (rows, dimensions):
            raise ValueError(
                f"{input_npy} has shape {values.shape}, expected {(rows, dimensions)}"
            )
        source = np.asarray(values)
    else:
        rng = np.random.default_rng(seed)
        source = rng.standard_normal((rows, dimensions), dtype=np.float32)

    if element_type == "float32":
        physical = np.asarray(source, dtype=np.float32)
        vectors = _fixed_size_list(pa, physical, pa.float32(), dimensions)
        useful_bytes = dimensions * 4
    elif element_type == "float16":
        physical = np.asarray(source, dtype=np.float16)
        vectors = _fixed_size_list(pa, physical, pa.float16(), dimensions)
        useful_bytes = dimensions * 2
    elif element_type == "bfloat16":
        as_f32 = np.asarray(source, dtype=np.float32)
        physical = (as_f32.view(np.uint32) >> 16).astype(np.uint16)
        vectors = _fixed_size_list(pa, physical, pa.uint16(), dimensions)
        useful_bytes = dimensions * 2
    elif element_type == "int8":
        physical = np.clip(np.rint(source), -128, 127).astype(np.int8)
        vectors = _fixed_size_list(pa, physical, pa.int8(), dimensions)
        useful_bytes = dimensions
    else:
        bits = np.asarray(source >= 0, dtype=np.uint8)
        packed = np.packbits(bits, axis=1, bitorder="little")
        width = (dimensions + 7) // 8
        vectors = pa.array(
            [row[:width].tobytes() for row in packed],
            type=pa.binary(width),
        )
        useful_bytes = width

    table = pa.table(
        {
            "row_id": pa.array(np.arange(rows, dtype=np.uint64)),
            "vector": vectors,
        }
    )
    return table, useful_bytes


@dataclass(frozen=True)
class ArrowBatchLayout:
    row_start: int
    rows: int
    vector_offset: int
    vector_length: int


@dataclass(frozen=True)
class ArrowReadRange:
    offset: int
    length: int
    rows: int


def _locate_vector_buffer(
    mapped: mmap.mmap,
    *,
    search_start: int,
    search_end: int,
    source: memoryview,
) -> int:
    if len(source) == 0:
        raise ValueError("Arrow vector buffer must not be empty")
    probe_length = min(64, len(source))
    probe_offsets = sorted(
        {0, max(0, (len(source) - probe_length) // 2), len(source) - probe_length}
    )
    first_probe = bytes(source[:probe_length])
    candidates: list[int] = []
    cursor = search_start
    while True:
        candidate = mapped.find(first_probe, cursor, search_end)
        if candidate < 0:
            break
        if candidate + len(source) <= search_end and all(
            mapped[candidate + offset : candidate + offset + probe_length]
            == bytes(source[offset : offset + probe_length])
            for offset in probe_offsets
        ):
            candidates.append(candidate)
        cursor = candidate + 1
    if len(candidates) != 1:
        raise RuntimeError(
            "could not resolve one unambiguous Arrow vector-buffer range "
            f"inside record-batch envelope: found {len(candidates)}"
        )
    return candidates[0]


def write_arrow_ipc(
    table: Any,
    path: Path,
    batch_rows: int,
    row_bytes: int,
) -> tuple[ArrowBatchLayout, ...]:
    _, pa, modules = load_dependencies()
    _, ipc, _ = modules
    pending: list[tuple[int, Any, int, int]] = []
    row_start = 0
    with pa.OSFile(str(path), "wb") as sink:
        with ipc.new_file(sink, table.schema) as writer:
            for batch in table.to_batches(max_chunksize=batch_rows):
                envelope_start = sink.tell()
                writer.write_batch(batch)
                sink.flush()
                envelope_end = sink.tell()
                pending.append(
                    (row_start, batch.column(1), envelope_start, envelope_end)
                )
                row_start += batch.num_rows
    layouts: list[ArrowBatchLayout] = []
    with path.open("rb") as handle:
        with mmap.mmap(handle.fileno(), length=0, access=mmap.ACCESS_READ) as mapped:
            for batch_row_start, vector, envelope_start, envelope_end in pending:
                source_buffer = vector.buffers()[-1]
                if source_buffer is None:
                    raise RuntimeError("Arrow vector column has no values buffer")
                source_start = vector.offset * row_bytes
                source_length = len(vector) * row_bytes
                source = memoryview(source_buffer)[
                    source_start : source_start + source_length
                ]
                vector_offset = _locate_vector_buffer(
                    mapped,
                    search_start=envelope_start,
                    search_end=envelope_end,
                    source=source,
                )
                layouts.append(
                    ArrowBatchLayout(
                        row_start=batch_row_start,
                        rows=len(vector),
                        vector_offset=vector_offset,
                        vector_length=source_length,
                    )
                )
                source.release()
    return tuple(layouts)


def write_parquet(table: Any, path: Path, batch_rows: int) -> None:
    _, _, modules = load_dependencies()
    _, _, pq = modules
    pq.write_table(
        table,
        path,
        compression="zstd",
        row_group_size=batch_rows,
        data_page_version="2.0",
        write_page_index=True,
        use_dictionary=False,
    )


def write_vortex(table: Any, path: Path, compact: bool) -> None:
    vx = load_vortex()
    options = (
        vx.io.VortexWriteOptions.compact()
        if compact
        else vx.io.VortexWriteOptions.default()
    )
    options.write(table, str(path))


@dataclass
class PreparedReader:
    select: Callable[[Sequence[int]], int]
    close: Callable[[], None]


def arrow_exact_ranges(
    indices: Sequence[int],
    layouts: Sequence[ArrowBatchLayout],
    *,
    row_bytes: int,
) -> tuple[ArrowReadRange, ...]:
    if row_bytes <= 0:
        raise ValueError("row_bytes must be positive")
    ordered = sorted(indices)
    if ordered != list(indices) or len(set(ordered)) != len(ordered):
        raise ValueError("Arrow ranged-read indices must be sorted and unique")
    ranges: list[ArrowReadRange] = []
    cursor = 0
    for layout in layouts:
        if layout.vector_length != layout.rows * row_bytes:
            raise ValueError("Arrow descriptor vector length does not match its rows")
        stop = layout.row_start + layout.rows
        while cursor < len(ordered) and ordered[cursor] < stop:
            row = ordered[cursor]
            if row < layout.row_start:
                raise IndexError("selected row is outside the Arrow range descriptor")
            run_start = row
            run_rows = 1
            cursor += 1
            while (
                cursor < len(ordered)
                and ordered[cursor] < stop
                and ordered[cursor] == run_start + run_rows
            ):
                run_rows += 1
                cursor += 1
            offset = layout.vector_offset + (run_start - layout.row_start) * row_bytes
            length = run_rows * row_bytes
            if offset + length > layout.vector_offset + layout.vector_length:
                raise IndexError("Arrow exact range exceeds the descriptor")
            ranges.append(ArrowReadRange(offset=offset, length=length, rows=run_rows))
    if cursor != len(ordered):
        raise IndexError("selected row is outside the Arrow range descriptor")
    return tuple(ranges)


def coalesce_arrow_ranges(
    ranges: Sequence[ArrowReadRange],
    *,
    max_gap_bytes: int,
    max_range_bytes: int = 0,
) -> tuple[ArrowReadRange, ...]:
    if max_gap_bytes < 0:
        raise ValueError("max_gap_bytes must be nonnegative")
    if max_range_bytes < 0:
        raise ValueError("max_range_bytes must be nonnegative")
    if not ranges:
        return ()
    ordered = sorted(ranges, key=lambda item: item.offset)
    merged: list[ArrowReadRange] = []
    start = ordered[0].offset
    end = start + ordered[0].length
    rows = ordered[0].rows
    for requested in ordered[1:]:
        requested_end = requested.offset + requested.length
        merged_end = max(end, requested_end)
        within_range_cap = max_range_bytes == 0 or merged_end - start <= max_range_bytes
        if requested.offset - end <= max_gap_bytes and within_range_cap:
            end = max(end, requested_end)
            rows += requested.rows
            continue
        merged.append(ArrowReadRange(offset=start, length=end - start, rows=rows))
        start = requested.offset
        end = requested_end
        rows = requested.rows
    merged.append(ArrowReadRange(offset=start, length=end - start, rows=rows))
    return tuple(merged)


def _grouped_local_indices(
    indices: Sequence[int], offsets: Sequence[int], lengths: Sequence[int]
) -> list[tuple[int, list[int]]]:
    grouped: list[tuple[int, list[int]]] = []
    cursor = 0
    for ordinal, (start, length) in enumerate(zip(offsets, lengths, strict=True)):
        stop = start + length
        local: list[int] = []
        while cursor < len(indices) and indices[cursor] < stop:
            if indices[cursor] >= start:
                local.append(indices[cursor] - start)
            cursor += 1
        if local:
            grouped.append((ordinal, local))
    if cursor != len(indices):
        raise IndexError("selected row exceeds the physical file row count")
    return grouped


def prepare_arrow_ipc(
    path: Path | str,
    *,
    filesystem: Any | None = None,
    layouts: Sequence[ArrowBatchLayout],
    row_bytes: int,
    max_gap_bytes: int = 64 * 1024,
    max_parallel: int = 10,
    max_range_bytes: int = 0,
) -> PreparedReader:
    if not layouts:
        raise ValueError("Arrow ranged reader requires its in-memory descriptor")
    validate_arrow_io_options(max_gap_bytes, max_parallel, max_range_bytes)
    if filesystem is not None:
        source = filesystem.open_input_file(str(path))

        def read_at(length: int, offset: int) -> bytes:
            return source.read_at(length, offset)

        close = source.close
    else:
        descriptor = os.open(path, os.O_RDONLY)

        def read_at(length: int, offset: int) -> bytes:
            return os.pread(descriptor, length, offset)

        def close() -> None:
            os.close(descriptor)

    executor = ThreadPoolExecutor(
        max_workers=max_parallel,
        thread_name_prefix="arrow-ranges",
    )

    def select(indices: Sequence[int]) -> int:
        logical = arrow_exact_ranges(indices, layouts, row_bytes=row_bytes)
        physical = coalesce_arrow_ranges(
            logical,
            max_gap_bytes=max_gap_bytes,
            max_range_bytes=max_range_bytes,
        )

        def fetch(requested: ArrowReadRange) -> int:
            payload = read_at(requested.length, requested.offset)
            if len(payload) != requested.length:
                raise RuntimeError("Arrow exact-vector range is truncated")
            return requested.rows

        return sum(executor.map(fetch, physical))

    def close_all() -> None:
        executor.shutdown(wait=True)
        close()

    return PreparedReader(select=select, close=close_all)


def prepare_parquet(
    path: Path | str,
    *,
    filesystem: Any | None = None,
) -> PreparedReader:
    _, pa, modules = load_dependencies()
    _, _, pq = modules
    reader = pq.ParquetFile(path, filesystem=filesystem)
    lengths = [
        reader.metadata.row_group(index).num_rows
        for index in range(reader.metadata.num_row_groups)
    ]
    offsets: list[int] = []
    total = 0
    for length in lengths:
        offsets.append(total)
        total += length

    def select(indices: Sequence[int]) -> int:
        selected = []
        for group_index, local in _grouped_local_indices(indices, offsets, lengths):
            table = reader.read_row_group(group_index, columns=["row_id", "vector"])
            selected.append(table.take(pa.array(local, type=pa.int64())))
        return sum(table.num_rows for table in selected)

    return PreparedReader(select=select, close=lambda: None)


def prepare_vortex(
    path: Path | str,
    *,
    store: Any | None = None,
    without_segment_cache: bool = False,
) -> PreparedReader:
    _, pa, _ = load_dependencies()
    vx = load_vortex()
    reader = vx.open(
        str(path),
        store=store,
        without_segment_cache=without_segment_cache,
    )

    def select(indices: Sequence[int]) -> int:
        selected = vx.array(pa.array(indices, type=pa.uint64()))
        result = reader.scan(["row_id", "vector"], indices=selected).read_all()
        return materialized_vortex_row_count(result)

    return PreparedReader(select=select, close=lambda: None)


def materialized_vortex_row_count(result: Any) -> int:
    """Finish Vortex-to-Arrow decode before reporting a timed row count."""
    return result.to_arrow_table().num_rows


def write_format(
    format_name: str,
    table: Any,
    path: Path,
    batch_rows: int,
    row_bytes: int,
) -> tuple[ArrowBatchLayout, ...] | None:
    if format_name == "arrow-ipc":
        return write_arrow_ipc(table, path, batch_rows, row_bytes)
    elif format_name == "parquet":
        write_parquet(table, path, batch_rows)
    elif format_name == "vortex-default":
        write_vortex(table, path, compact=False)
    elif format_name == "vortex-compact":
        write_vortex(table, path, compact=True)
    else:
        raise ValueError(f"unsupported format {format_name!r}")
    return None


def prepare_format(
    format_name: str,
    path: Path | str,
    *,
    filesystem: Any | None = None,
    store: Any | None = None,
    without_segment_cache: bool = False,
    arrow_layouts: Sequence[ArrowBatchLayout] | None = None,
    row_bytes: int = 0,
    arrow_max_gap_bytes: int = 64 * 1024,
    arrow_max_parallel: int = 10,
    arrow_max_range_bytes: int = 0,
) -> PreparedReader:
    if format_name == "arrow-ipc":
        return prepare_arrow_ipc(
            path,
            filesystem=filesystem,
            layouts=arrow_layouts or (),
            row_bytes=row_bytes,
            max_gap_bytes=arrow_max_gap_bytes,
            max_parallel=arrow_max_parallel,
            max_range_bytes=arrow_max_range_bytes,
        )
    if format_name == "parquet":
        return prepare_parquet(path, filesystem=filesystem)
    if format_name in {"vortex-default", "vortex-compact"}:
        return prepare_vortex(
            path,
            store=store,
            without_segment_cache=without_segment_cache,
        )
    raise ValueError(f"unsupported format {format_name!r}")


def write_csv(
    path: Path, fieldnames: Sequence[str], rows: Iterable[dict[str, Any]]
) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite {path}")
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def format_path(output_dir: Path, format_name: str) -> Path:
    suffix = {
        "arrow-ipc": "arrow",
        "parquet": "parquet",
        "vortex-default": "vortex",
        "vortex-compact": "compact.vortex",
    }[format_name]
    return output_dir / f"vectors.{suffix}"


def access_method(
    format_name: str,
    without_segment_cache: bool,
    *,
    arrow_max_gap_bytes: int = 64 * 1024,
    arrow_max_parallel: int = 10,
    arrow_max_range_bytes: int = 0,
) -> str:
    if format_name == "arrow-ipc":
        validate_arrow_io_options(
            arrow_max_gap_bytes,
            arrow_max_parallel,
            arrow_max_range_bytes,
        )
        gap = (
            f"{arrow_max_gap_bytes // 1024}k"
            if arrow_max_gap_bytes % 1024 == 0
            else f"{arrow_max_gap_bytes}b"
        )
        cap = (
            ""
            if arrow_max_range_bytes == 0
            else (
                f"-{arrow_max_range_bytes // 1024}k-cap"
                if arrow_max_range_bytes % 1024 == 0
                else f"-{arrow_max_range_bytes}b-cap"
            )
        )
        return f"borsuk-range-{gap}-gap{cap}-{arrow_max_parallel}-parallel"
    if format_name == "parquet":
        return "native-row-group-take"
    cache = "no-segment-cache" if without_segment_cache else "segment-cache"
    return f"native-indices-{cache}"


def upload_to_s3(path: Path, bucket: str, key: str, region: str) -> Any:
    import pyarrow.fs as pafs

    filesystem = pafs.S3FileSystem(region=region)
    destination = f"{bucket}/{key}"
    with path.open("rb") as source, filesystem.open_output_stream(destination) as sink:
        while chunk := source.read(8 * 1024 * 1024):
            sink.write(chunk)
    return filesystem


def run_benchmark(args: argparse.Namespace) -> int:
    formats = parse_formats(args.formats)
    validate_backend(args.backend, args.s3_bucket, args.s3_prefix)
    selected_widths = parse_positive_ints(args.selected_rows, "selected_rows")
    patterns = tuple(item.strip() for item in args.patterns.split(",") if item.strip())
    if not patterns or any(item not in {"clustered", "scattered"} for item in patterns):
        raise ValueError("patterns must contain clustered and/or scattered")
    if args.element_type not in KNOWN_ELEMENT_TYPES:
        raise ValueError(f"unsupported element type {args.element_type!r}")
    if any(width > args.rows for width in selected_widths):
        raise ValueError("selected_rows must not exceed rows")
    if args.repetitions <= 0 or args.warmups < 0 or args.batch_rows <= 0:
        raise ValueError(
            "repetitions/batch_rows must be positive and warmups nonnegative"
        )
    validate_arrow_io_options(
        args.arrow_max_gap_bytes,
        args.arrow_max_parallel,
        args.arrow_max_range_bytes,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    table, useful_bytes_per_vector = create_table(
        rows=args.rows,
        dimensions=args.dimensions,
        element_type=args.element_type,
        seed=args.seed,
        input_npy=args.input_npy,
    )
    build_rows: list[dict[str, Any]] = []
    open_rows: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    status_rows: list[dict[str, str]] = []
    completed_formats: list[str] = []

    for format_name in formats:
        path = format_path(args.output_dir, format_name)
        if path.exists():
            raise FileExistsError(f"refusing to overwrite {path}")
        started = time.perf_counter()
        arrow_layouts: tuple[ArrowBatchLayout, ...] | None = None
        try:
            arrow_layouts = write_format(
                format_name,
                table,
                path,
                args.batch_rows,
                useful_bytes_per_vector,
            )
        except Exception as error:
            if path.exists():
                path.unlink()
            status_rows.append(
                {
                    "format": format_name,
                    "element_type": args.element_type,
                    "status": "blocked",
                    "blocker": f"{type(error).__name__}: {str(error).replace(chr(10), ' ')}",
                }
            )
            continue
        build_ms = (time.perf_counter() - started) * 1000
        file_bytes = path.stat().st_size
        build_rows.append(
            {
                "format": format_name,
                "element_type": args.element_type,
                "rows": args.rows,
                "dimensions": args.dimensions,
                "batch_rows": args.batch_rows,
                "elapsed_ms": f"{build_ms:.6f}",
                "vectors_per_s": f"{args.rows / max(build_ms / 1000, 1e-12):.6f}",
                "file_bytes": file_bytes,
                "bytes_per_vector": f"{file_bytes / args.rows:.6f}",
            }
        )
        filesystem = None
        store = None
        reader_path: Path | str = path
        if args.backend == "s3":
            remote_key = f"{args.s3_prefix.strip('/')}/{format_name}/{path.name}"
            filesystem = upload_to_s3(
                path,
                args.s3_bucket,
                remote_key,
                args.aws_region,
            )
            if format_name in {"vortex-default", "vortex-compact"}:
                vx = load_vortex()
                parent, filename = remote_key.rsplit("/", 1)
                store = vx.store.S3Store(
                    bucket=args.s3_bucket,
                    prefix=parent,
                    region=args.aws_region,
                )
                reader_path = filename
            else:
                reader_path = f"{args.s3_bucket}/{remote_key}"
        started = time.perf_counter()
        try:
            reader = prepare_format(
                format_name,
                reader_path,
                filesystem=filesystem,
                store=store,
                without_segment_cache=args.vortex_without_segment_cache,
                arrow_layouts=arrow_layouts,
                row_bytes=useful_bytes_per_vector,
                arrow_max_gap_bytes=args.arrow_max_gap_bytes,
                arrow_max_parallel=args.arrow_max_parallel,
                arrow_max_range_bytes=args.arrow_max_range_bytes,
            )
        except Exception as error:
            status_rows.append(
                {
                    "format": format_name,
                    "element_type": args.element_type,
                    "status": "blocked",
                    "blocker": f"open {type(error).__name__}: {str(error).replace(chr(10), ' ')}",
                }
            )
            continue
        open_ms = (time.perf_counter() - started) * 1000
        completed_formats.append(format_name)
        status_rows.append(
            {
                "format": format_name,
                "element_type": args.element_type,
                "status": "complete",
                "blocker": "",
            }
        )
        open_rows.append(
            {
                "format": format_name,
                "element_type": args.element_type,
                "backend": args.backend,
                "access_method": access_method(
                    format_name,
                    args.vortex_without_segment_cache,
                    arrow_max_gap_bytes=args.arrow_max_gap_bytes,
                    arrow_max_parallel=args.arrow_max_parallel,
                    arrow_max_range_bytes=args.arrow_max_range_bytes,
                ),
                "elapsed_ms": f"{open_ms:.6f}",
                "file_bytes": file_bytes,
            }
        )
        try:
            for pattern in patterns:
                for width in selected_widths:
                    queries = make_query_indices(
                        rows=args.rows,
                        selected_rows=width,
                        repetitions=args.warmups + args.repetitions,
                        pattern=pattern,
                        seed=args.seed
                        ^ width
                        ^ (0 if pattern == "clustered" else 0x5A17),
                    )
                    for indices in queries[: args.warmups]:
                        result_rows = reader.select(indices)
                        if result_rows != width:
                            raise RuntimeError(
                                f"{format_name} returned {result_rows} rows, expected {width}"
                            )
                    for repetition, indices in enumerate(
                        queries[args.warmups :], start=1
                    ):
                        started = time.perf_counter()
                        result_rows = reader.select(indices)
                        elapsed_ms = (time.perf_counter() - started) * 1000
                        if result_rows != width:
                            raise RuntimeError(
                                f"{format_name} returned {result_rows} rows, expected {width}"
                            )
                        sample_rows.append(
                            {
                                "format": format_name,
                                "element_type": args.element_type,
                                "backend": args.backend,
                                "access_method": access_method(
                                    format_name,
                                    args.vortex_without_segment_cache,
                                    arrow_max_gap_bytes=args.arrow_max_gap_bytes,
                                    arrow_max_parallel=args.arrow_max_parallel,
                                    arrow_max_range_bytes=args.arrow_max_range_bytes,
                                ),
                                "cache_profile": args.cache_profile,
                                "pattern": pattern,
                                "selected_rows": width,
                                "repetition": repetition,
                                "elapsed_ms": f"{elapsed_ms:.6f}",
                                "result_rows": result_rows,
                                "useful_bytes": result_rows * useful_bytes_per_vector,
                                "file_bytes": file_bytes,
                            }
                        )
        finally:
            reader.close()

    summary_rows: list[dict[str, Any]] = []
    for format_name in completed_formats:
        file_bytes = format_path(args.output_dir, format_name).stat().st_size
        for pattern in patterns:
            for width in selected_widths:
                elapsed = [
                    float(row["elapsed_ms"])
                    for row in sample_rows
                    if row["format"] == format_name
                    and row["pattern"] == pattern
                    and row["selected_rows"] == width
                ]
                summary = summarize_samples(elapsed)
                useful_bytes = width * useful_bytes_per_vector
                summary_rows.append(
                    {
                        "format": format_name,
                        "element_type": args.element_type,
                        "backend": args.backend,
                        "access_method": access_method(
                            format_name,
                            args.vortex_without_segment_cache,
                            arrow_max_gap_bytes=args.arrow_max_gap_bytes,
                            arrow_max_parallel=args.arrow_max_parallel,
                            arrow_max_range_bytes=args.arrow_max_range_bytes,
                        ),
                        "cache_profile": args.cache_profile,
                        "pattern": pattern,
                        "selected_rows": width,
                        **{
                            key: f"{value:.6f}" if isinstance(value, float) else value
                            for key, value in summary.items()
                        },
                        "useful_bytes": useful_bytes,
                        "file_bytes": file_bytes,
                        "bytes_per_vector": f"{file_bytes / args.rows:.6f}",
                    }
                )

    write_csv(
        args.output_dir / "build.csv",
        (
            "format",
            "element_type",
            "rows",
            "dimensions",
            "batch_rows",
            "elapsed_ms",
            "vectors_per_s",
            "file_bytes",
            "bytes_per_vector",
        ),
        build_rows,
    )
    write_csv(
        args.output_dir / "open.csv",
        (
            "format",
            "element_type",
            "backend",
            "access_method",
            "elapsed_ms",
            "file_bytes",
        ),
        open_rows,
    )
    write_csv(
        args.output_dir / "samples.csv",
        (
            "format",
            "element_type",
            "backend",
            "access_method",
            "cache_profile",
            "pattern",
            "selected_rows",
            "repetition",
            "elapsed_ms",
            "result_rows",
            "useful_bytes",
            "file_bytes",
        ),
        sample_rows,
    )
    write_csv(
        args.output_dir / "summary.csv",
        (
            "format",
            "element_type",
            "backend",
            "access_method",
            "cache_profile",
            "pattern",
            "selected_rows",
            "samples",
            "mean_ms",
            "stddev_ms",
            "p50_ms",
            "p95_ms",
            "p99_ms",
            "min_ms",
            "max_ms",
            "useful_bytes",
            "file_bytes",
            "bytes_per_vector",
        ),
        summary_rows,
    )
    write_csv(
        args.output_dir / "status.csv",
        ("format", "element_type", "status", "blocker"),
        status_rows,
    )
    return 2 if len(completed_formats) != len(formats) else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument(
        "--element-type", choices=KNOWN_ELEMENT_TYPES, default="float32"
    )
    parser.add_argument(
        "--formats",
        default="arrow-ipc,parquet,vortex-default,vortex-compact",
    )
    parser.add_argument("--batch-rows", type=int, default=8192)
    parser.add_argument(
        "--backend",
        choices=("local_disk", "s3"),
        default="local_disk",
    )
    parser.add_argument("--s3-bucket", default="")
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument("--aws-region", default="eu-central-1")
    parser.add_argument(
        "--vortex-without-segment-cache",
        action="store_true",
        help="Disable Vortex's in-process segment cache for native-S3 request tests.",
    )
    parser.add_argument(
        "--arrow-max-gap-bytes",
        type=int,
        default=1024 * 1024,
        help="Maximum unrequested byte gap merged into one Arrow physical read.",
    )
    parser.add_argument(
        "--arrow-max-parallel",
        type=int,
        default=10,
        help="Maximum simultaneous Arrow physical range reads.",
    )
    parser.add_argument(
        "--arrow-max-range-bytes",
        type=int,
        default=4 * 1024 * 1024,
        help="Maximum merged Arrow physical range.",
    )
    parser.add_argument("--selected-rows", default="10,100,1000")
    parser.add_argument("--patterns", default="clustered,scattered")
    parser.add_argument("--repetitions", type=int, default=30)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--seed", type=int, default=0xB05)
    parser.add_argument("--input-npy", type=Path)
    parser.add_argument(
        "--cache-profile",
        choices=("disk_cached", "uncached"),
        default="disk_cached",
        help="Label only; uncached runs require external page-cache eviction.",
    )
    return parser.parse_args()


def main() -> int:
    return run_benchmark(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
