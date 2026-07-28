#!/usr/bin/env python3
"""Probe exact Arrow-type round trips through Parquet and Vortex writers."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from benchmark_table_formats import load_dependencies, load_vortex

KNOWN_FORMATS = ("parquet", "vortex-default", "vortex-compact")


def parse_formats(value: str) -> tuple[str, ...]:
    formats = tuple(item.strip() for item in value.split(",") if item.strip())
    unsupported = sorted(set(formats).difference(KNOWN_FORMATS))
    if not formats:
        raise ValueError("at least one format is required")
    if unsupported:
        raise ValueError(f"unsupported table format(s): {', '.join(unsupported)}")
    if len(formats) != len(set(formats)):
        raise ValueError("formats must not contain duplicates")
    return formats


@dataclass(frozen=True)
class CompatibilityCase:
    name: str
    table: Any


def compatibility_cases(rows: int) -> tuple[CompatibilityCase, ...]:
    if rows < 8:
        raise ValueError("compatibility probe requires at least eight rows")
    np, pa, _, _ = load_dependencies()
    row_ids = pa.array(np.arange(rows, dtype=np.uint64))

    def table(values: Any) -> Any:
        return pa.table({"row_id": row_ids, "value": values})

    nullable_mask = np.arange(rows) % 7 == 0

    def primitive(values: Any, arrow_type: Any) -> Any:
        return pa.array(values, type=arrow_type, mask=nullable_mask)

    fixed_f32 = pa.FixedSizeListArray.from_arrays(
        pa.array(np.arange(rows * 16, dtype=np.float32)),
        16,
    )
    fixed_u8 = pa.FixedSizeListArray.from_arrays(
        pa.array(np.arange(rows * 16, dtype=np.uint8)),
        16,
    )
    fixed_binary_values = [
        None if nullable_mask[row] else bytes([row % 251]) * 64 for row in range(rows)
    ]
    utf8_values = [
        None if nullable_mask[row] else f"tenant-{row % 13}" for row in range(rows)
    ]
    binary_values = [
        None if nullable_mask[row] else row.to_bytes(8, "little") * (1 + row % 3)
        for row in range(rows)
    ]
    list_u32_values = [
        None if nullable_mask[row] else [row, row + 1, row + 3] for row in range(rows)
    ]
    list_f32_values = [
        None if nullable_mask[row] else [row / 3.0, row / 5.0] for row in range(rows)
    ]

    return (
        CompatibilityCase("uint8", table(primitive(np.arange(rows), pa.uint8()))),
        CompatibilityCase("uint16", table(primitive(np.arange(rows), pa.uint16()))),
        CompatibilityCase("uint32", table(primitive(np.arange(rows), pa.uint32()))),
        CompatibilityCase("uint64", table(primitive(np.arange(rows), pa.uint64()))),
        CompatibilityCase("int64", table(primitive(np.arange(rows), pa.int64()))),
        CompatibilityCase(
            "float16",
            table(primitive(np.arange(rows, dtype=np.float16) / 7, pa.float16())),
        ),
        CompatibilityCase(
            "float32",
            table(primitive(np.arange(rows, dtype=np.float32) / 11, pa.float32())),
        ),
        CompatibilityCase(
            "boolean",
            table(
                pa.array(
                    [
                        None if nullable_mask[row] else row % 2 == 0
                        for row in range(rows)
                    ]
                )
            ),
        ),
        CompatibilityCase("utf8", table(pa.array(utf8_values, type=pa.utf8()))),
        CompatibilityCase("binary", table(pa.array(binary_values, type=pa.binary()))),
        CompatibilityCase(
            "fixed_size_binary_64",
            table(pa.array(fixed_binary_values, type=pa.binary(64))),
        ),
        CompatibilityCase("fixed_list_f32_16", table(fixed_f32)),
        CompatibilityCase("fixed_list_u8_16", table(fixed_u8)),
        CompatibilityCase(
            "list_u32",
            table(pa.array(list_u32_values, type=pa.list_(pa.uint32()))),
        ),
        CompatibilityCase(
            "list_f32",
            table(pa.array(list_f32_values, type=pa.list_(pa.float32()))),
        ),
    )


def write_and_read(format_name: str, table: Any, path: Path) -> Any:
    _, _, _, parquet = load_dependencies()
    if format_name == "parquet":
        parquet.write_table(table, path, compression="zstd")
        return parquet.read_table(path)
    vx = load_vortex()
    options = (
        vx.io.VortexWriteOptions.compact()
        if format_name == "vortex-compact"
        else vx.io.VortexWriteOptions.default()
    )
    options.write(table, str(path))
    return vx.open(str(path)).scan().read_all().to_arrow_table()


def run(output_dir: Path, formats: Sequence[str], rows: int) -> None:
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite {output_dir}")
    output_dir.mkdir(parents=True)
    results: list[dict[str, Any]] = []
    for case in compatibility_cases(rows):
        source_type = str(case.table.schema.field("value").type)
        for format_name in formats:
            suffix = "parquet" if format_name == "parquet" else "vortex"
            path = output_dir / f"{case.name}-{format_name}.{suffix}"
            status = "complete"
            blocker = ""
            read_type = ""
            try:
                decoded = write_and_read(format_name, case.table, path)
                read_type = str(decoded.schema.field("value").type)
                if (
                    decoded.schema.field("value").type
                    != case.table.schema.field("value").type
                ):
                    status = "type_changed"
                    blocker = f"decoded type {read_type}, expected {source_type}"
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
                    "decoded_arrow_type": read_type,
                    "rows": rows,
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
        default="parquet,vortex-default,vortex-compact",
    )
    parser.add_argument("--rows", type=int, default=128)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(args.output_dir, parse_formats(args.formats), args.rows)


if __name__ == "__main__":
    main()
