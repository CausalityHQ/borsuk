"""Sealed 96-byte product-quantized records in four-page physical groups."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import struct
import tempfile
from collections.abc import Iterable, Sequence
from pathlib import Path

import numpy as np

from scripts.v97_row_width_screen import PqSpec, adc_scores, encode_pq, fit_pq

SCHEMA = "borsuk-pq96-group-codes-v1"
SPEC = PqSpec("pq96x8", 96, 8, 96)
TRAINING_SEED = 20260923
TRAINING_ROWS = 100_000
TRAINING_ITERATIONS = 10
GROUP_PAGES = 4
RECORD_BYTES = 96
MODEL_HEADER = struct.Struct("<16sIIII")
MODEL_MAGIC = b"BORSUK-PQ96-V1\0\0"


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _hex(value: object) -> bool:
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def source_rows_digest(vectors: np.ndarray, *, batch_rows: int = 4096) -> str:
    if (
        type(vectors) is not np.ndarray or vectors.dtype != np.float32
        or vectors.ndim != 2 or vectors.shape[1] != 768
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("PQ96 source rows differ")
    digest = hashlib.sha256()
    for first in range(0, len(vectors), batch_rows):
        digest.update(vectors[first:first + batch_rows].tobytes(order="C"))
    return digest.hexdigest()


def fit_source_pq96(
    vectors: np.ndarray, *, expected_rows: int = TRAINING_ROWS,
) -> np.ndarray:
    """Fit the frozen source-only codebook before query or truth access."""
    if (
        type(vectors) is not np.ndarray or vectors.dtype != np.float32
        or vectors.shape != (expected_rows, 768) or expected_rows != TRAINING_ROWS
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("PQ96 frozen training population differs")
    return fit_pq(
        vectors, SPEC, seed=TRAINING_SEED,
        sample_rows=TRAINING_ROWS, iterations=TRAINING_ITERATIONS,
    )


def _model_bytes(books: np.ndarray) -> bytes:
    if (
        type(books) is not np.ndarray or books.dtype != np.float32
        or books.shape != (96, 256, 8) or not np.isfinite(books).all()
    ):
        raise ValueError("PQ96 model differs")
    return MODEL_HEADER.pack(MODEL_MAGIC, 768, 96, 256, 8) + np.asarray(
        books, dtype="<f4",
    ).tobytes(order="C")


@dataclasses.dataclass(frozen=True, slots=True)
class Pq96WriteResult:
    seal: dict[str, object]
    seal_sha256: str


def write_pq96_groups(
    root: Path, books: np.ndarray, page_row_counts: Sequence[int],
    batches: Iterable[tuple[np.ndarray, np.ndarray]],
    *, source_sha256: str, layout_sha256: str, physical_order_sha256: str,
    source_rows_sha256: str,
) -> Pq96WriteResult:
    """Encode source-order batches and place each row at its physical ordinal."""
    root.mkdir(parents=True, exist_ok=True)
    if any((root / name).exists() for name in ("model.bin", "groups.bin", "seal.json")):
        raise ValueError("PQ96 output already exists")
    counts = tuple(page_row_counts)
    rows = sum(counts)
    if (
        not counts or rows <= 0
        or any(type(count) is not int or not 0 < count < 2**32 for count in counts)
        or any(not _hex(value) for value in (
            source_sha256, layout_sha256, physical_order_sha256, source_rows_sha256,
        ))
    ):
        raise ValueError("PQ96 physical layout differs")
    model_body = _model_bytes(books)
    with tempfile.TemporaryDirectory(prefix=".pq96-", dir=root) as temporary:
        temporary_root = Path(temporary)
        records = np.memmap(
            temporary_root / "records.bin", dtype=np.uint8, mode="w+",
            shape=(rows, RECORD_BYTES),
        )
        seen = np.zeros(rows, dtype=np.bool_)
        digest = hashlib.sha256()
        observed = 0
        short_batch = False
        for ordinals, vectors in batches:
            if (
                type(ordinals) is not np.ndarray or ordinals.ndim != 1
                or ordinals.dtype.kind not in "iu"
                or type(vectors) is not np.ndarray or vectors.dtype != np.float32
                or vectors.shape != (len(ordinals), 768)
                or not 1 <= len(ordinals) <= 4096
                or not np.isfinite(vectors).all()
                or np.any(ordinals < 0) or np.any(ordinals >= rows)
                or len(np.unique(ordinals)) != len(ordinals)
                or np.any(seen[ordinals]) or short_batch
            ):
                raise ValueError("PQ96 source batch differs")
            short_batch = len(ordinals) < 4096
            observed += len(ordinals)
            digest.update(vectors.tobytes(order="C"))
            records[ordinals] = encode_pq(vectors, books, SPEC)
            seen[ordinals] = True
        if observed != rows or not np.all(seen) or digest.hexdigest() != source_rows_sha256:
            raise ValueError("PQ96 source stream differs")
        records.flush()
        (root / "model.bin").write_bytes(model_body)
        ranges: list[list[object]] = []
        row_first = 0
        with (root / "groups.bin").open("wb") as output:
            for first in range(0, len(counts), GROUP_PAGES):
                end = min(first + GROUP_PAGES, len(counts))
                group_rows = sum(counts[first:end])
                header = struct.pack("<I", end - first) + b"".join(
                    struct.pack("<I", count) for count in counts[first:end]
                )
                body = header + records[row_first:row_first + group_rows].tobytes(order="C")
                offset = output.tell()
                output.write(body)
                ranges.append([first, end, offset, len(body), hashlib.sha256(body).hexdigest()])
                row_first += group_rows
        if row_first != rows:
            raise ValueError("PQ96 group row count differs")
    seal: dict[str, object] = {
        "schema": SCHEMA,
        "dimensions": 768, "row_bytes": RECORD_BYTES,
        "group_pages": GROUP_PAGES, "page_row_counts": list(counts),
        "rows": rows, "ranges": ranges,
        "source_sha256": source_sha256,
        "source_rows_sha256": source_rows_sha256,
        "layout_sha256": layout_sha256,
        "physical_order_sha256": physical_order_sha256,
        "model_sha256": hashlib.sha256(model_body).hexdigest(),
        "groups_sha256": _hash_file(root / "groups.bin"),
        "training": {
            "method": "v97-fit-pq-contiguous-eight-dim-float32-v1",
            "sample_rows": TRAINING_ROWS, "seed": TRAINING_SEED,
            "iterations": TRAINING_ITERATIONS,
            "centroid_dtype": "float32", "scoring": "ascending-subspace-float32-adc-v1",
        },
    }
    body = _canonical(seal)
    (root / "seal.json").write_bytes(body)
    return Pq96WriteResult(seal, hashlib.sha256(body).hexdigest())


@dataclasses.dataclass(frozen=True, slots=True)
class Pq96GroupReader:
    path: Path
    books: np.ndarray
    page_row_counts: tuple[int, ...]
    ranges: tuple[tuple[int, int, int, int, str], ...]

    def group_records(self, index: int) -> np.ndarray:
        if type(index) is not int or not 0 <= index < len(self.ranges):
            raise ValueError("PQ96 group index differs")
        first, end, offset, length, digest = self.ranges[index]
        with self.path.open("rb") as handle:
            handle.seek(offset)
            body = handle.read(length)
        counts = self.page_row_counts[first:end]
        header = 4 + 4 * len(counts)
        if (
            len(body) != length or hashlib.sha256(body).hexdigest() != digest
            or body[:header] != struct.pack("<I", len(counts))
            + b"".join(struct.pack("<I", count) for count in counts)
        ):
            raise ValueError("PQ96 group identity differs")
        return np.frombuffer(body, dtype=np.uint8, offset=header).reshape(sum(counts), RECORD_BYTES)


def open_pq96_groups(
    root: Path, *, source_sha256: str, layout_sha256: str,
    physical_order_sha256: str, seal_sha256: str,
) -> Pq96GroupReader:
    """Open only a group set bound to an independently authenticated seal."""
    seal_body = (root / "seal.json").read_bytes()
    seal = json.loads(seal_body)
    if (
        seal_body != _canonical(seal)
        or hashlib.sha256(seal_body).hexdigest() != seal_sha256
        or seal.get("schema") != SCHEMA
        or seal.get("dimensions") != 768
        or seal.get("row_bytes") != RECORD_BYTES
        or seal.get("group_pages") != GROUP_PAGES
        or seal.get("source_sha256") != source_sha256
        or seal.get("layout_sha256") != layout_sha256
        or seal.get("physical_order_sha256") != physical_order_sha256
        or seal.get("training") != {
            "method": "v97-fit-pq-contiguous-eight-dim-float32-v1",
            "sample_rows": TRAINING_ROWS, "seed": TRAINING_SEED,
            "iterations": TRAINING_ITERATIONS,
            "centroid_dtype": "float32", "scoring": "ascending-subspace-float32-adc-v1",
        }
    ):
        raise ValueError("PQ96 seal authority differs")
    model_body = (root / "model.bin").read_bytes()
    header = MODEL_HEADER.size
    if (
        hashlib.sha256(model_body).hexdigest() != seal.get("model_sha256")
        or len(model_body) != header + 96 * 256 * 8 * 4
        or MODEL_HEADER.unpack_from(model_body) != (MODEL_MAGIC, 768, 96, 256, 8)
        or _hash_file(root / "groups.bin") != seal.get("groups_sha256")
    ):
        raise ValueError("PQ96 model or group digest differs")
    books = np.frombuffer(model_body, dtype="<f4", offset=header).reshape(96, 256, 8)
    counts = tuple(seal["page_row_counts"])
    if (
        not np.isfinite(books).all() or sum(counts) != seal.get("rows")
        or any(type(count) is not int or count <= 0 for count in counts)
    ):
        raise ValueError("PQ96 decoded model or counts differ")
    ranges = tuple(tuple(item) for item in seal["ranges"])
    offset = 0
    for ordinal, item in enumerate(ranges):
        first, end, actual_offset, length, digest = item
        if (
            first != ordinal * GROUP_PAGES or end != min(first + GROUP_PAGES, len(counts))
            or actual_offset != offset or length != 4 + 4 * (end - first)
            + RECORD_BYTES * sum(counts[first:end]) or not _hex(digest)
        ):
            raise ValueError("PQ96 group ranges differ")
        offset += length
    if offset != os.stat(root / "groups.bin").st_size:
        raise ValueError("PQ96 group file length differs")
    books.setflags(write=False)
    return Pq96GroupReader(root / "groups.bin", books, counts, ranges)


def score_pq96(query: np.ndarray, reader: Pq96GroupReader, records: np.ndarray) -> np.ndarray:
    return adc_scores(query, reader.books, records, SPEC)
