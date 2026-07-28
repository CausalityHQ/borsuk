#!/usr/bin/env python3
"""Compare Parquet and Vortex on BORSUK-like durable table workloads."""

from __future__ import annotations

import argparse
import csv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from benchmark_vector_formats import summarize_samples

KNOWN_FORMATS = ("parquet", "vortex-default", "vortex-compact")


def parse_formats(value: str) -> tuple[str, ...]:
    formats = tuple(item.strip() for item in value.split(",") if item.strip())
    unsupported = sorted(set(formats).difference(KNOWN_FORMATS))
    if not formats:
        raise ValueError("at least one format is required")
    if unsupported:
        raise ValueError(f"unsupported table format(s): {', '.join(unsupported)}")
    if len(set(formats)) != len(formats):
        raise ValueError("formats must not contain duplicates")
    return formats


def validate_backend(backend: str, s3_bucket: str, s3_prefix: str) -> None:
    if backend not in {"local_disk", "s3"}:
        raise ValueError("backend must be local_disk or s3")
    if backend == "s3" and (not s3_bucket or not s3_prefix.strip("/")):
        raise ValueError("S3 backend requires an explicit bucket and prefix")


@dataclass(frozen=True)
class WorkloadSpec:
    name: str
    columns: tuple[str, ...] | None
    predicate: str | None
    expected_rows: int


def workload_specs(rows: int) -> tuple[WorkloadSpec, ...]:
    if rows < 100:
        raise ValueError("table benchmark requires at least 100 rows")
    tenant = 17
    tenant_rows = 0 if rows <= tenant else ((rows - 1 - tenant) // 100) + 1
    range_rows = rows // 100
    return (
        WorkloadSpec("narrow_projection", ("row_id", "score"), None, rows),
        WorkloadSpec(
            "tenant_filter_1pct",
            ("row_id", "score"),
            f"tenant:{tenant}",
            tenant_rows,
        ),
        WorkloadSpec(
            "row_range_1pct",
            ("row_id", "term_hash", "score"),
            f"range:{rows // 3}:{rows // 3 + range_rows}",
            range_rows,
        ),
        WorkloadSpec(
            "point_lookup",
            ("row_id", "generation", "code"),
            f"point:{rows // 2}",
            1,
        ),
        WorkloadSpec("full_table_scan", None, None, rows),
    )


def load_dependencies() -> tuple[Any, Any, Any, Any]:
    try:
        import numpy as np
        import pyarrow as pa
        import pyarrow.dataset as ds
        import pyarrow.parquet as pq
    except ImportError as error:
        raise RuntimeError(
            "table format benchmark requires scripts/requirements-format-bench.txt"
        ) from error
    return np, pa, ds, pq


def load_vortex() -> Any:
    try:
        import vortex as vx
    except ImportError as error:
        raise RuntimeError(
            "Vortex table formats require scripts/requirements-format-bench.txt"
        ) from error
    return vx


def create_table(rows: int, code_width: int, seed: int, code_type: str) -> Any:
    if rows < 100 or code_width <= 0:
        raise ValueError("rows must be at least 100 and code_width must be positive")
    if code_type not in {"variable", "fixed"}:
        raise ValueError("code_type must be variable or fixed")
    np, pa, _, _ = load_dependencies()
    rng = np.random.default_rng(seed)
    row_id = np.arange(rows, dtype=np.uint64)
    code_bytes = rng.integers(0, 256, size=(rows, code_width), dtype=np.uint8)
    if code_type == "fixed":
        code = pa.Array.from_buffers(
            pa.binary(code_width),
            rows,
            [None, pa.py_buffer(code_bytes)],
        )
    else:
        offsets = np.arange(rows + 1, dtype=np.int32) * np.int32(code_width)
        code = pa.Array.from_buffers(
            pa.binary(),
            rows,
            [None, pa.py_buffer(offsets), pa.py_buffer(code_bytes)],
        )
    return pa.table(
        {
            "row_id": pa.array(row_id),
            "tenant_id": pa.array((row_id % 100).astype(np.uint32)),
            "generation": pa.array((row_id // 1_000).astype(np.uint64)),
            "term_hash": pa.array(
                (row_id * np.uint64(0x9E3779B97F4A7C15)) ^ np.uint64(0xB05),
            ),
            "score": pa.array(rng.standard_normal(rows, dtype=np.float32)),
            "active": pa.array((row_id % 11) != 0),
            "code": code,
        }
    )


def write_format(
    format_name: str,
    table: Any,
    path: Path,
    row_group_rows: int,
) -> None:
    if format_name == "parquet":
        _, _, _, pq = load_dependencies()
        pq.write_table(
            table,
            path,
            compression="zstd",
            row_group_size=row_group_rows,
            data_page_version="2.0",
            write_page_index=True,
            use_dictionary=["tenant_id", "active"],
        )
        return
    vx = load_vortex()
    options = (
        vx.io.VortexWriteOptions.compact()
        if format_name == "vortex-compact"
        else vx.io.VortexWriteOptions.default()
    )
    options.write(table, str(path))


def _predicate_parts(predicate: str | None) -> tuple[str, tuple[int, ...]]:
    if predicate is None:
        return "all", ()
    parts = predicate.split(":")
    return parts[0], tuple(int(value) for value in parts[1:])


def materialized_vortex_row_count(result: Any) -> int:
    """Finish Vortex-to-Arrow decode before reporting a timed row count."""
    return result.to_arrow_table().num_rows


def prepare_parquet(
    path: Path | str,
    *,
    filesystem: Any | None = None,
) -> Callable[[WorkloadSpec], int]:
    _, _, ds, _ = load_dependencies()
    dataset = ds.dataset(path, format="parquet", filesystem=filesystem)

    def execute(workload: WorkloadSpec) -> int:
        kind, values = _predicate_parts(workload.predicate)
        predicate = None
        if kind == "tenant":
            predicate = ds.field("tenant_id") == values[0]
        elif kind == "range":
            predicate = (ds.field("row_id") >= values[0]) & (
                ds.field("row_id") < values[1]
            )
        elif kind == "point":
            predicate = ds.field("row_id") == values[0]
        table = dataset.scanner(
            columns=list(workload.columns) if workload.columns is not None else None,
            filter=predicate,
            use_threads=True,
        ).to_table()
        return table.num_rows

    return execute


def prepare_vortex(
    path: Path | str,
    *,
    store: Any | None = None,
    without_segment_cache: bool = False,
) -> Callable[[WorkloadSpec], int]:
    vx = load_vortex()
    _, pa, _, _ = load_dependencies()
    import vortex.expr as ve

    reader = vx.open(
        str(path),
        store=store,
        without_segment_cache=without_segment_cache,
    )
    u32 = vx.DType.from_arrow(pa.uint32())
    u64 = vx.DType.from_arrow(pa.uint64())

    def execute(workload: WorkloadSpec) -> int:
        kind, values = _predicate_parts(workload.predicate)
        predicate = None
        if kind == "tenant":
            predicate = ve.column("tenant_id") == ve.literal(u32, values[0])
        elif kind == "range":
            predicate = (ve.column("row_id") >= ve.literal(u64, values[0])) & (
                ve.column("row_id") < ve.literal(u64, values[1])
            )
        elif kind == "point":
            predicate = ve.column("row_id") == ve.literal(u64, values[0])
        result = reader.scan(
            list(workload.columns) if workload.columns is not None else None,
            expr=predicate,
        ).read_all()
        return materialized_vortex_row_count(result)

    return execute


def format_path(output_dir: Path, format_name: str) -> Path:
    suffix = (
        "parquet"
        if format_name == "parquet"
        else ("compact.vortex" if format_name == "vortex-compact" else "vortex")
    )
    return output_dir / f"table.{suffix}"


def upload_to_s3(path: Path, bucket: str, key: str, region: str) -> Any:
    import pyarrow.fs as pafs

    filesystem = pafs.S3FileSystem(region=region)
    destination = f"{bucket}/{key}"
    with path.open("rb") as source, filesystem.open_output_stream(destination) as sink:
        while chunk := source.read(8 * 1024 * 1024):
            sink.write(chunk)
    return filesystem


def write_csv(
    path: Path, fieldnames: Sequence[str], rows: Iterable[dict[str, Any]]
) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite {path}")
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_benchmark(args: argparse.Namespace) -> int:
    formats = parse_formats(args.formats)
    validate_backend(args.backend, args.s3_bucket, args.s3_prefix)
    if args.repetitions <= 0 or args.warmups < 0 or args.row_group_rows <= 0:
        raise ValueError(
            "repetitions/row_group_rows must be positive and warmups nonnegative"
        )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    table = create_table(args.rows, args.code_width, args.seed, args.code_type)
    workloads = workload_specs(args.rows)
    build_rows: list[dict[str, Any]] = []
    open_rows: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    status_rows: list[dict[str, str]] = []

    for format_name in formats:
        path = format_path(args.output_dir, format_name)
        if path.exists():
            raise FileExistsError(f"refusing to overwrite {path}")
        started = time.perf_counter()
        try:
            write_format(format_name, table, path, args.row_group_rows)
            build_ms = (time.perf_counter() - started) * 1_000
            file_bytes = path.stat().st_size
            build_rows.append(
                {
                    "format": format_name,
                    "rows": args.rows,
                    "code_width": args.code_width,
                    "code_type": args.code_type,
                    "row_group_rows": args.row_group_rows,
                    "elapsed_ms": f"{build_ms:.6f}",
                    "rows_per_s": f"{args.rows / max(build_ms / 1_000, 1e-12):.6f}",
                    "file_bytes": file_bytes,
                    "bytes_per_row": f"{file_bytes / args.rows:.6f}",
                }
            )
            filesystem = None
            remote_key = ""
            if args.backend == "s3":
                remote_key = f"{args.s3_prefix.strip('/')}/{format_name}/{path.name}"
                filesystem = upload_to_s3(
                    path,
                    args.s3_bucket,
                    remote_key,
                    args.aws_region,
                )
            opened = time.perf_counter()
            if format_name == "parquet":
                execute = prepare_parquet(
                    (
                        f"{args.s3_bucket}/{remote_key}"
                        if args.backend == "s3"
                        else path
                    ),
                    filesystem=filesystem,
                )
            elif args.backend == "s3":
                vx = load_vortex()
                parent, filename = remote_key.rsplit("/", 1)
                store = vx.store.S3Store(
                    bucket=args.s3_bucket,
                    prefix=parent,
                    region=args.aws_region,
                )
                execute = prepare_vortex(
                    filename,
                    store=store,
                    without_segment_cache=args.vortex_without_segment_cache,
                )
            else:
                execute = prepare_vortex(
                    path,
                    without_segment_cache=args.vortex_without_segment_cache,
                )
            open_rows.append(
                {
                    "format": format_name,
                    "backend": args.backend,
                    "elapsed_ms": f"{(time.perf_counter() - opened) * 1_000:.6f}",
                    "file_bytes": file_bytes,
                }
            )
            for workload in workloads:
                for _ in range(args.warmups):
                    if execute(workload) != workload.expected_rows:
                        raise RuntimeError(
                            f"{format_name} returned the wrong warmup row count"
                        )
                for repetition in range(1, args.repetitions + 1):
                    query_started = time.perf_counter()
                    result_rows = execute(workload)
                    elapsed_ms = (time.perf_counter() - query_started) * 1_000
                    if result_rows != workload.expected_rows:
                        raise RuntimeError(
                            f"{format_name}/{workload.name} returned {result_rows}, "
                            f"expected {workload.expected_rows}"
                        )
                    sample_rows.append(
                        {
                            "format": format_name,
                            "backend": args.backend,
                            "workload": workload.name,
                            "cache_profile": args.cache_profile,
                            "repetition": repetition,
                            "elapsed_ms": f"{elapsed_ms:.6f}",
                            "result_rows": result_rows,
                            "file_bytes": file_bytes,
                        }
                    )
            status_rows.append(
                {"format": format_name, "status": "complete", "blocker": ""}
            )
        except Exception as error:
            status_rows.append(
                {
                    "format": format_name,
                    "status": "blocked",
                    "blocker": f"{type(error).__name__}: {str(error).replace(chr(10), ' ')}",
                }
            )

    summary_rows: list[dict[str, Any]] = []
    for format_name in formats:
        for workload in workloads:
            values = [
                float(row["elapsed_ms"])
                for row in sample_rows
                if row["format"] == format_name and row["workload"] == workload.name
            ]
            if not values:
                continue
            summary = summarize_samples(values)
            summary_rows.append(
                {
                    "format": format_name,
                    "backend": args.backend,
                    "workload": workload.name,
                    "cache_profile": args.cache_profile,
                    **{
                        key: f"{value:.6f}" if isinstance(value, float) else value
                        for key, value in summary.items()
                    },
                    "result_rows": workload.expected_rows,
                    "file_bytes": format_path(args.output_dir, format_name)
                    .stat()
                    .st_size,
                }
            )

    write_csv(
        args.output_dir / "build.csv",
        (
            "format",
            "rows",
            "code_width",
            "code_type",
            "row_group_rows",
            "elapsed_ms",
            "rows_per_s",
            "file_bytes",
            "bytes_per_row",
        ),
        build_rows,
    )
    write_csv(
        args.output_dir / "open.csv",
        ("format", "backend", "elapsed_ms", "file_bytes"),
        open_rows,
    )
    write_csv(
        args.output_dir / "samples.csv",
        (
            "format",
            "backend",
            "workload",
            "cache_profile",
            "repetition",
            "elapsed_ms",
            "result_rows",
            "file_bytes",
        ),
        sample_rows,
    )
    write_csv(
        args.output_dir / "summary.csv",
        (
            "format",
            "backend",
            "workload",
            "cache_profile",
            "samples",
            "mean_ms",
            "stddev_ms",
            "p50_ms",
            "p95_ms",
            "p99_ms",
            "min_ms",
            "max_ms",
            "result_rows",
            "file_bytes",
        ),
        summary_rows,
    )
    write_csv(
        args.output_dir / "status.csv",
        ("format", "status", "blocker"),
        status_rows,
    )
    return 0 if all(row["status"] == "complete" for row in status_rows) else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--code-width", type=int, default=64)
    parser.add_argument(
        "--code-type",
        choices=("variable", "fixed"),
        default="variable",
        help="Run common Binary performance cells or the FixedSizeBinary compatibility gate.",
    )
    parser.add_argument("--row-group-rows", type=int, default=8_192)
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
        "--formats",
        default="parquet,vortex-default,vortex-compact",
    )
    parser.add_argument("--repetitions", type=int, default=30)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--seed", type=int, default=0xB05)
    parser.add_argument(
        "--cache-profile",
        choices=("disk_cached", "uncached"),
        default="disk_cached",
    )
    return parser.parse_args()


def main() -> int:
    return run_benchmark(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
