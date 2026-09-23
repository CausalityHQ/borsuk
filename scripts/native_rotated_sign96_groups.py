"""Source-only, bounded construction and authentication of sign96 groups."""

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

from scripts.native_rotated_sign96 import (
    KEPT_COORDINATES,
    MAX_ENCODE_ROWS,
    RECORD_BYTES,
    encode_records,
)

SCHEMA = "borsuk-rotated-sign96-groups-v1"
GROUP_PAGES = 4
GroupRange = tuple[int, int, int, int, str]


@dataclasses.dataclass(frozen=True, slots=True)
class MeanReceipt:
    mean: np.ndarray
    mean_sha256: str
    source_rows_sha256: str
    batch_rows: int
    rows: int


@dataclasses.dataclass(frozen=True, slots=True)
class Sign96WriteResult:
    seal: dict[str, object]
    seal_sha256: str


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _hex_digest(value: str) -> bool:
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _encoding_rule_sha256() -> str:
    rule = {
        "rotation": "sq2-sha256-sign-three-256-hadamard-div16-v1",
        "coordinates": KEPT_COORDINATES.tolist(),
        "bit_order": "little-one-is-positive",
        "scale": "mean-absolute-kept-then-little-f16-v1",
        "score": "full-centered-query-norm-plus-coded-norm-minus-two-dot-byte-table-v1",
    }
    return hashlib.sha256(_canonical(rule)).hexdigest()


@dataclasses.dataclass(frozen=True, slots=True)
class Sign96GroupReader:
    path: Path
    mean: np.ndarray
    page_row_counts: tuple[int, ...]
    ranges: tuple[GroupRange, ...]
    rotation_seed: int

    def group_records(self, index: int) -> np.ndarray:
        """Read one complete group and verify its own digest and page counts."""
        if type(index) is not int or not 0 <= index < len(self.ranges):
            raise ValueError("sign96 group index differs")
        first, end, offset, length, digest = self.ranges[index]
        with self.path.open("rb") as handle:
            handle.seek(offset)
            body = handle.read(length)
        header = 4 + 4 * (end - first)
        counts = self.page_row_counts[first:end]
        if (
            len(body) != length
            or hashlib.sha256(body).hexdigest() != digest
            or body[:header] != struct.pack("<I", end - first)
            + b"".join(struct.pack("<I", count) for count in counts)
        ):
            raise ValueError("sign96 group identity differs")
        return np.frombuffer(body, dtype=np.uint8, offset=header).reshape(sum(counts), RECORD_BYTES)


def mean_from_source_batches(
    batches: Iterable[np.ndarray], *, expected_rows: int, batch_rows: int = 4096,
) -> MeanReceipt:
    """Fit a float32 mean from bounded source-order float64 batch sums."""
    if (
        type(expected_rows) is not int or expected_rows <= 0
        or type(batch_rows) is not int or not 1 <= batch_rows <= MAX_ENCODE_ROWS
    ):
        raise ValueError("sign96 mean population differs")
    total = np.zeros(768, dtype=np.float64)
    observed = 0
    short_batch = False
    source_digest = hashlib.sha256()
    for rows in batches:
        if (
            type(rows) is not np.ndarray
            or rows.dtype != np.float32
            or rows.ndim != 2
            or rows.shape[1] != 768
            or not 1 <= len(rows) <= batch_rows
            or not np.isfinite(rows).all()
        ):
            raise ValueError("sign96 mean source batch differs")
        if short_batch:
            raise ValueError("sign96 mean batch boundary differs")
        short_batch = len(rows) < batch_rows
        observed += len(rows)
        if observed > expected_rows:
            raise ValueError("sign96 mean population differs")
        total += np.sum(rows.astype(np.float64), axis=0, dtype=np.float64)
        source_digest.update(rows.tobytes(order="C"))
    if observed != expected_rows:
        raise ValueError("sign96 mean population differs")
    mean = (total / expected_rows).astype(np.float32)
    if not np.isfinite(mean).all():
        raise ValueError("sign96 mean is nonfinite")
    mean_sha = hashlib.sha256(np.asarray(mean, dtype="<f4").tobytes(order="C")).hexdigest()
    mean.setflags(write=False)
    return MeanReceipt(mean, mean_sha, source_digest.hexdigest(), batch_rows, observed)


def write_sign96_groups(
    root: Path,
    mean_receipt: MeanReceipt,
    page_row_counts: Sequence[int],
    batches: Iterable[tuple[np.ndarray, np.ndarray]],
    *,
    source_sha256: str,
    layout_sha256: str,
    physical_order_sha256: str,
    rotation_seed: int,
) -> Sign96WriteResult:
    """Place streamed source batches by sealed ordinal, then frame four-page groups."""
    root.mkdir(parents=True, exist_ok=True)
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("sign96 construction must be source-only")
    if any((root / name).exists() for name in ("mean.bin", "groups.bin", "seal.json")):
        raise ValueError("sign96 output already exists")
    counts = tuple(page_row_counts)
    if type(mean_receipt) is not MeanReceipt:
        raise ValueError("sign96 mean receipt differs")
    mean = mean_receipt.mean
    if (
        type(mean) is not np.ndarray
        or mean.dtype != np.float32
        or mean.shape != (768,)
        or not np.isfinite(mean).all()
        or not counts
        or any(type(count) is not int or not 0 < count < 2**32 for count in counts)
        or any(not _hex_digest(value) for value in
               (source_sha256, layout_sha256, physical_order_sha256))
        or not _hex_digest(mean_receipt.source_rows_sha256)
        or type(mean_receipt.batch_rows) is not int
        or not 1 <= mean_receipt.batch_rows <= MAX_ENCODE_ROWS
        or type(mean_receipt.rows) is not int
    ):
        raise ValueError("sign96 source authority differs")
    if (
        hashlib.sha256(np.asarray(mean, dtype="<f4").tobytes(order="C")).hexdigest()
        != mean_receipt.mean_sha256
    ):
        raise ValueError("sign96 mean receipt differs")
    rows = sum(counts)
    if rows == 0 or rows != mean_receipt.rows:
        raise ValueError("sign96 source population differs")
    with tempfile.TemporaryDirectory(prefix=".sign96-", dir=root) as temporary:
        temporary_root = Path(temporary)
        records = np.memmap(
            temporary_root / "records.bin", dtype=np.uint8, mode="w+",
            shape=(rows, RECORD_BYTES),
        )
        seen = np.zeros(rows, dtype=np.bool_)
        source_digest = hashlib.sha256()
        short_batch = False
        for ordinals, vectors in batches:
            if (
                type(ordinals) is not np.ndarray
                or ordinals.ndim != 1
                or ordinals.dtype.kind not in "iu"
                or len(ordinals) == 0
                or len(ordinals) > mean_receipt.batch_rows
                or type(vectors) is not np.ndarray
                or vectors.shape != (len(ordinals), 768)
                or np.any(ordinals < 0)
                or np.any(ordinals >= rows)
                or len(np.unique(ordinals)) != len(ordinals)
            ):
                raise ValueError("sign96 source batch differs")
            if short_batch:
                raise ValueError("sign96 source batch boundary differs")
            short_batch = len(ordinals) < mean_receipt.batch_rows
            if np.any(seen[ordinals]):
                raise ValueError("sign96 duplicate physical ordinal")
            seen[ordinals] = True
            source_digest.update(vectors.tobytes(order="C"))
            records[ordinals] = encode_records(vectors, mean, rotation_seed=rotation_seed)
        if not np.all(seen):
            raise ValueError("sign96 source population incomplete")
        if source_digest.hexdigest() != mean_receipt.source_rows_sha256:
            raise ValueError("sign96 source rows differ from mean pass")
        records.flush()
        ranges: list[GroupRange] = []
        offsets = np.concatenate(([0], np.cumsum(counts, dtype=np.int64)))
        whole_digest = hashlib.sha256()
        groups_path = temporary_root / "groups.bin"
        with groups_path.open("wb") as handle:
            for first in range(0, len(counts), GROUP_PAGES):
                end = min(first + GROUP_PAGES, len(counts))
                header = struct.pack("<I", end - first) + b"".join(
                    struct.pack("<I", count) for count in counts[first:end]
                )
                body = header + records[int(offsets[first]) : int(offsets[end])].tobytes()
                offset = handle.tell()
                handle.write(body)
                whole_digest.update(body)
                ranges.append((first, end, offset, len(body), hashlib.sha256(body).hexdigest()))
        del records
        mean_body = np.asarray(mean, dtype="<f4").tobytes(order="C")
        seal: dict[str, object] = {
            "schema": SCHEMA,
            "source_sha256": source_sha256,
            "layout_sha256": layout_sha256,
            "physical_order_sha256": physical_order_sha256,
            "source_rows_sha256": mean_receipt.source_rows_sha256,
            "mean_batch_rows": mean_receipt.batch_rows,
            "encoding_rule_sha256": _encoding_rule_sha256(),
            "rotation_seed": rotation_seed,
            "rows": rows,
            "dimensions": 768,
            "row_bytes": RECORD_BYTES,
            "group_pages": GROUP_PAGES,
            "page_row_counts": list(counts),
            "mean_sha256": hashlib.sha256(mean_body).hexdigest(),
            "groups_sha256": whole_digest.hexdigest(),
            "group_ranges": [list(group) for group in ranges],
        }
        os.replace(groups_path, root / "groups.bin")
        (root / "mean.bin").write_bytes(mean_body)
        seal_body = _canonical(seal)
        (root / "seal.json").write_bytes(seal_body)
    return Sign96WriteResult(seal, hashlib.sha256(seal_body).hexdigest())


def open_sign96_groups(
    root: Path, *, source_sha256: str, layout_sha256: str,
    physical_order_sha256: str, seal_sha256: str, rotation_seed: int,
) -> Sign96GroupReader:
    """Authenticate the exact new format and all group framing before use."""
    try:
        body = (root / "seal.json").read_bytes()
        if hashlib.sha256(body).hexdigest() != seal_sha256:
            raise ValueError("sign96 seal identity differs")
        seal = json.loads(body)
        counts = tuple(seal["page_row_counts"])
        ranges = tuple(tuple(group) for group in seal["group_ranges"])
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, KeyError) as error:
        raise ValueError("sign96 seal identity differs") from error
    expected_keys = {
        "schema", "source_sha256", "layout_sha256", "physical_order_sha256",
        "source_rows_sha256", "mean_batch_rows", "encoding_rule_sha256",
        "rotation_seed", "rows", "dimensions", "row_bytes", "group_pages",
        "page_row_counts", "mean_sha256", "groups_sha256", "group_ranges",
    }
    if (
        type(seal) is not dict or set(seal) != expected_keys or body != _canonical(seal)
        or seal["schema"] != SCHEMA
        or seal["source_sha256"] != source_sha256
        or seal["layout_sha256"] != layout_sha256
        or seal["physical_order_sha256"] != physical_order_sha256
        or not _hex_digest(seal["source_rows_sha256"])
        or type(seal["mean_batch_rows"]) is not int
        or not 1 <= seal["mean_batch_rows"] <= MAX_ENCODE_ROWS
        or seal["encoding_rule_sha256"] != _encoding_rule_sha256()
        or seal["rotation_seed"] != rotation_seed
        or type(seal["rows"]) is not int
        or type(seal["dimensions"]) is not int
        or seal["dimensions"] != 768
        or type(seal["row_bytes"]) is not int
        or seal["row_bytes"] != RECORD_BYTES
        or type(seal["group_pages"]) is not int
        or seal["group_pages"] != GROUP_PAGES
        or type(seal["rotation_seed"]) is not int
        or not counts
        or any(type(count) is not int or not 0 < count < 2**32 for count in counts)
        or seal["rows"] != sum(counts)
        or len(ranges) != (len(counts) + GROUP_PAGES - 1) // GROUP_PAGES
    ):
        raise ValueError("sign96 seal identity differs")
    mean_path = root / "mean.bin"
    groups_path = root / "groups.bin"
    try:
        mean_body = mean_path.read_bytes()
        if (
            len(mean_body) != 768 * 4
            or hashlib.sha256(mean_body).hexdigest() != seal["mean_sha256"]
            or _digest(groups_path) != seal["groups_sha256"]
        ):
            raise ValueError("sign96 object identity differs")
    except OSError as error:
        raise ValueError("sign96 object identity differs") from error
    mean = np.frombuffer(mean_body, dtype="<f4").copy()
    if not np.isfinite(mean).all():
        raise ValueError("sign96 mean identity differs")
    offset = 0
    for index, group in enumerate(ranges):
        first = index * GROUP_PAGES
        end = min(first + GROUP_PAGES, len(counts))
        length = 4 + 4 * (end - first) + RECORD_BYTES * sum(counts[first:end])
        if (
            len(group) != 5
            or any(type(value) is not int for value in group[:4])
            or group[:4] != (first, end, offset, length)
            or not _hex_digest(group[4])
        ):
            raise ValueError("sign96 group framing differs")
        offset += length
    if groups_path.stat().st_size != offset:
        raise ValueError("sign96 group framing differs")
    reader = Sign96GroupReader(groups_path, mean, counts, ranges, seal["rotation_seed"])
    for index in range(len(ranges)):
        reader.group_records(index)
    return reader
