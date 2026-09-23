"""Create-only page objects for the mirrored rotated two-bit format."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_progressive_two_bit_codes import join_planes, split_records

SCHEMA = "borsuk-progressive-two-bit-pages-v1"
NAMES = {
    "sign": {"base": "sign-base.bin", "delta": "sign-delta.bin"},
    "magnitude": {"base": "magnitude-base.bin", "delta": "magnitude-delta.bin"},
}


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _hex(value: object) -> bool:
    return type(value) is str and len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def write_page_objects(
    root: Path, records: np.ndarray, page_row_counts: Sequence[int],
    *, base_pages: int, source_sha256: str, layout_sha256: str,
    mean_sha256: str, rotation_seed: int,
) -> dict[str, object]:
    """Seal page offsets/digests; publish the manifest only after all objects."""
    counts = tuple(page_row_counts)
    if (
        not isinstance(records, np.ndarray)
        or records.dtype != np.uint8 or records.ndim != 2
        or records.shape != (sum(counts), 200)
        or not counts or type(base_pages) is not int
        or not 0 < base_pages <= len(counts)
        or any(type(count) is not int or count <= 0 for count in counts)
        or any(not _hex(value) for value in (
            source_sha256, layout_sha256, mean_sha256,
        ))
        or type(rotation_seed) is not int or not 0 <= rotation_seed < 2**32
    ):
        raise ValueError("progressive page source differs")
    root.mkdir(parents=True, exist_ok=True)
    roles = ("base", "delta") if base_pages < len(counts) else ("base",)
    names = tuple(NAMES[plane][role] for plane in NAMES for role in roles)
    if any((root / name).exists() for name in (*names, "progressive-code-seal.json")):
        raise ValueError("progressive page object already exists")
    with tempfile.TemporaryDirectory(prefix=".progressive-", dir=root) as directory:
        scratch = Path(directory)
        handles = {name: (scratch / name).open("wb") for name in names}
        spans: dict[str, list[list[object]]] = {plane: [] for plane in NAMES}
        record_digest = hashlib.sha256()
        first = 0
        try:
            for page, count in enumerate(counts):
                last = first + count
                source = records[first:last]
                sign, magnitude = split_records(source)
                if not np.array_equal(join_planes(sign, magnitude), source):
                    raise ValueError("progressive page bit rejoin differs")
                record_digest.update(source.tobytes(order="C"))
                role = "base" if page < base_pages else "delta"
                for plane, payload in (("sign", sign), ("magnitude", magnitude)):
                    handle = handles[NAMES[plane][role]]
                    body = payload.tobytes(order="C")
                    offset = handle.tell()
                    handle.write(body)
                    spans[plane].append([page, role, offset, len(body),
                                         hashlib.sha256(body).hexdigest()])
                first = last
        finally:
            for handle in handles.values():
                handle.close()
        if first != len(records):
            raise ValueError("progressive page row count differs")
        objects = {
            name: {"bytes": (scratch / name).stat().st_size, "sha256": _sha(scratch / name)}
            for name in names
        }
        seal: dict[str, object] = {
            "schema": SCHEMA, "rows": len(records), "page_row_counts": list(counts),
            "base_pages": base_pages, "row_bytes": {"sign": 104, "magnitude": 96},
            "source_sha256": source_sha256, "layout_sha256": layout_sha256,
            "mean_sha256": mean_sha256, "rotation_seed": rotation_seed,
            "records_sha256": record_digest.hexdigest(),
            "objects": objects, "pages": spans,
        }
        (scratch / "progressive-code-seal.json").write_bytes(_canonical(seal))
        for name in names:
            os.link(scratch / name, root / name)
        os.link(scratch / "progressive-code-seal.json", root / "progressive-code-seal.json")
    return seal


def read_authenticated_page(root: Path, seal: dict[str, object], page: int) -> np.ndarray:
    """Read matching local page ranges after authenticating each payload."""
    if (
        type(seal) is not dict or seal.get("schema") != SCHEMA
        or (root / "progressive-code-seal.json").read_bytes() != _canonical(seal)
        or type(page) is not int or not 0 <= page < len(seal["page_row_counts"])
    ):
        raise ValueError("progressive page seal differs")
    rows = seal["page_row_counts"][page]
    pieces = {}
    for plane, width in (("sign", 104), ("magnitude", 96)):
        recorded_page, role, offset, length, digest = seal["pages"][plane][page]
        name = NAMES[plane][role]
        if (
            recorded_page != page or length != rows * width
            or (root / name).stat().st_size != seal["objects"][name]["bytes"]
        ):
            raise ValueError("progressive page range differs")
        with (root / name).open("rb") as stream:
            stream.seek(offset)
            body = stream.read(length)
        if len(body) != length or hashlib.sha256(body).hexdigest() != digest:
            raise ValueError("progressive page digest differs")
        pieces[plane] = np.frombuffer(body, dtype=np.uint8).reshape(rows, width)
    return join_planes(pieces["sign"], pieces["magnitude"])
