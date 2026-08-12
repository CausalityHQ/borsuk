#!/usr/bin/env python3
"""Shared Publication V3 measurement and physical-object admission."""

from __future__ import annotations


OBJECT_FIELDS = frozenset({"role", "path", "format", "bytes", "rows", "checksum"})
OBJECT_ROLES = frozenset({"data-bundle", "query-page", "directory", "control"})
FORMATS = {
    "data-bundle": frozenset({"parquet", "arrow-ipc"}),
    "query-page": frozenset({"arrow-ipc", "parquet"}),
    "directory": frozenset({"arrow-ipc", "parquet"}),
    "control": frozenset({"json"}),
}
MAX_DATA_OBJECT_BYTES = 128 * 1024 * 1024
MAX_CONTROL_OBJECT_BYTES = 256 * 1024
MAX_CONTROL_OBJECTS = 256
MAX_DATA_OBJECTS = 8192


def _positive_integer(value: object, role: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{role} must be a positive integer")
    return value


def _checksum(value: object) -> str:
    digest = str(value)
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("object checksum must be lowercase SHA-256")
    return digest


def validate_object_roster(
    value: list[dict[str, object]], *, logical_rows: int, logical_cells: int | None = None
) -> dict[str, int]:
    logical_rows = _positive_integer(logical_rows, "logical rows")
    if logical_cells is not None:
        logical_cells = _positive_integer(logical_cells, "logical cells")
    if not isinstance(value, list) or not value:
        raise ValueError("object roster must be a nonempty list")
    paths: set[str] = set()
    represented_rows = 0
    data_bundles = 0
    data_objects = 0
    control_objects = 0
    total_bytes = 0
    maximum_bytes = 0
    for item in value:
        if not isinstance(item, dict) or frozenset(item) != OBJECT_FIELDS:
            raise ValueError("object roster fields differ")
        role = str(item["role"])
        path = str(item["path"])
        if role not in OBJECT_ROLES or str(item["format"]) not in FORMATS[role]:
            raise ValueError("object roster role or format is invalid")
        if not path or path.startswith("/") or ".." in path.split("/") or path in paths:
            raise ValueError("object roster paths must be relative and unique")
        paths.add(path)
        byte_count = _positive_integer(item["bytes"], "object bytes")
        rows = item["rows"]
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            raise ValueError("object rows must be a nonnegative integer")
        _checksum(item["checksum"])
        cap = MAX_CONTROL_OBJECT_BYTES if role == "control" else MAX_DATA_OBJECT_BYTES
        if byte_count > cap:
            raise ValueError("object exceeds its format byte cap")
        if role == "control":
            control_objects += 1
            if rows != 0:
                raise ValueError("control objects cannot represent data rows")
        else:
            data_objects += 1
        if role == "data-bundle":
            if rows <= 0:
                raise ValueError("data bundles must represent rows")
            data_bundles += 1
            represented_rows += rows
        total_bytes += byte_count
        maximum_bytes = max(maximum_bytes, byte_count)
    if represented_rows != logical_rows:
        raise ValueError("object roster logical row total differs from dataset")
    if logical_rows >= 10_000_000 and data_bundles < 2:
        raise ValueError("large-scale results require multiple data bundles")
    if (data_objects > 1 and data_objects * 1024 > logical_rows) or data_objects > MAX_DATA_OBJECTS or (
        logical_cells is not None
        and logical_cells > MAX_DATA_OBJECTS
        and data_objects >= logical_cells
    ):
        raise ValueError("object-count amplification is proportional to rows or cells")
    if control_objects > MAX_CONTROL_OBJECTS:
        raise ValueError("control-object amplification exceeds its fixed cap")
    return {
        "objects": len(value),
        "data_objects": data_objects,
        "data_bundles": data_bundles,
        "control_objects": control_objects,
        "represented_rows": represented_rows,
        "total_object_bytes": total_bytes,
        "maximum_object_bytes": maximum_bytes,
    }
