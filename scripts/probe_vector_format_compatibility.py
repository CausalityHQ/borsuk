#!/usr/bin/env python3
"""Probe exact typed-vector round trips through Arrow IPC and Vortex."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from benchmark_vector_formats import create_table, load_dependencies, load_vortex

KNOWN_FORMATS = ("arrow-ipc", "vortex-default", "vortex-compact")
VECTOR_TYPES = ("float32", "float16", "bfloat16", "int8", "binary")


def parse_formats(value: str) -> tuple[str, ...]:
    formats = tuple(item.strip() for item in value.split(",") if item.strip())
    unsupported = sorted(set(formats).difference(KNOWN_FORMATS))
    if not formats:
        raise ValueError("at least one format is required")
    if unsupported:
        raise ValueError(f"unsupported vector format(s): {', '.join(unsupported)}")
    if len(formats) != len(set(formats)):
        raise ValueError("formats must not contain duplicates")
    return formats


@dataclass(frozen=True)
class CompatibilityCase:
    name: str
    table: Any


def compatibility_cases(rows: int, dimensions: int) -> tuple[CompatibilityCase, ...]:
    if rows < 8:
        raise ValueError("compatibility probe requires at least eight rows")
    if dimensions <= 0:
        raise ValueError("dimensions must be positive")
    return tuple(
        CompatibilityCase(
            name=element_type,
            table=create_table(
                rows=rows,
                dimensions=dimensions,
                element_type=element_type,
                seed=0xB05,
                input_npy=None,
            )[0],
        )
        for element_type in VECTOR_TYPES
    )


def write_and_read(format_name: str, table: Any, path: Path) -> Any:
    _, pa, modules = load_dependencies()
    _, ipc, _ = modules
    if format_name == "arrow-ipc":
        with pa.OSFile(str(path), "wb") as sink:
            with ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)
        with pa.memory_map(str(path), "r") as source:
            return ipc.open_file(source).read_all()

    vx = load_vortex()
    options = (
        vx.io.VortexWriteOptions.compact()
        if format_name == "vortex-compact"
        else vx.io.VortexWriteOptions.default()
    )
    options.write(table, str(path))
    return vx.open(str(path)).scan().read_all().to_arrow_table()


def run(
    output_dir: Path,
    formats: Sequence[str],
    rows: int,
    dimensions: int,
) -> None:
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite {output_dir}")
    output_dir.mkdir(parents=True)
    results: list[dict[str, Any]] = []
    for case in compatibility_cases(rows, dimensions):
        source_type = str(case.table.schema.field("vector").type)
        for format_name in formats:
            suffix = "arrow" if format_name == "arrow-ipc" else "vortex"
            path = output_dir / f"{case.name}-{format_name}.{suffix}"
            status = "complete"
            blocker = ""
            decoded_type = ""
            try:
                decoded = write_and_read(format_name, case.table, path)
                decoded_type = str(decoded.schema.field("vector").type)
                if (
                    decoded.schema.field("vector").type
                    != case.table.schema.field("vector").type
                ):
                    status = "type_changed"
                    blocker = f"decoded type {decoded_type}, expected {source_type}"
                elif not decoded.equals(case.table):
                    status = "data_mismatch"
                    blocker = "decoded values differ from the source Arrow table"
            except Exception as error:
                status = "blocked"
                blocker = f"{type(error).__name__}: {str(error).replace(chr(10), ' ')}"
            results.append(
                {
                    "format": format_name,
                    "case": case.name,
                    "source_arrow_type": source_type,
                    "decoded_arrow_type": decoded_type,
                    "rows": rows,
                    "dimensions": dimensions,
                    "status": status,
                    "blocker": blocker,
                }
            )

    with (output_dir / "compatibility.csv").open("x", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "format",
                "case",
                "source_arrow_type",
                "decoded_arrow_type",
                "rows",
                "dimensions",
                "status",
                "blocker",
            ),
        )
        writer.writeheader()
        writer.writerows(results)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--formats",
        default="arrow-ipc,vortex-default,vortex-compact",
    )
    parser.add_argument("--rows", type=int, default=128)
    parser.add_argument("--dimensions", type=int, default=64)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        args.output_dir,
        parse_formats(args.formats),
        args.rows,
        args.dimensions,
    )


if __name__ == "__main__":
    main()
