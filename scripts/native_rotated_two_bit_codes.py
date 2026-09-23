#!/usr/bin/env python3
"""Source-only 200-byte rotated scalar codes in authenticated four-page groups."""

from __future__ import annotations

import dataclasses
import functools
import hashlib
import json
import struct
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import ArtifactIdentity, MembershipRow
from scripts.native_row_score_code_artifacts import (
    _physical_order,
    _physical_order_sha256,
)

SCHEMA = "borsuk-rotated-two-bit-group-codes-v1"
GROUP_PAGES = 4
DIMENSIONS = 768
PACKED_BYTES = 192
ROW_BYTES = 200
BATCH_ROWS = 4096
GroupRange = tuple[int, int, int, int, str]


@dataclasses.dataclass(frozen=True, slots=True)
class TwoBitCodes:
    mean: np.ndarray
    records: np.ndarray
    source_ordinals: tuple[int, ...]
    page_row_counts: tuple[int, ...]
    group_ranges: tuple[GroupRange, ...]
    layout_seed: int
    rotation_seed: int


@dataclasses.dataclass(frozen=True, slots=True)
class TwoBitIdentities:
    mean: ArtifactIdentity
    groups: ArtifactIdentity
    seal: ArtifactIdentity


@functools.lru_cache(maxsize=8)
def rotation_signs(rotation_seed: int) -> np.ndarray:
    """SHA-256-derived immutable signs; independent of NumPy RNG versions."""
    if type(rotation_seed) is not int or not 0 <= rotation_seed < 2**32:
        raise ValueError("two-bit rotation seed differs")
    prefix = b"borsuk-sq2-sign-v1" + struct.pack("<I", rotation_seed)
    signs = np.fromiter(
        (
            1.0
            if hashlib.sha256(prefix + struct.pack("<I", index)).digest()[0] & 1 == 0
            else -1.0
            for index in range(DIMENSIONS)
        ),
        dtype=np.float64,
        count=DIMENSIONS,
    )
    signs.setflags(write=False)
    return signs


def rotate_rows(rows: np.ndarray, *, rotation_seed: int) -> np.ndarray:
    """Apply signed, normalized 256-coordinate Hadamard to three blocks."""
    if type(rows) is not np.ndarray or rows.ndim != 2 or rows.shape[1] != DIMENSIONS:
        raise ValueError("two-bit rotation shape differs")
    out = rows.astype(np.float64) * rotation_signs(rotation_seed)
    for start in range(0, DIMENSIONS, 256):
        block = out[:, start : start + 256]
        width = 1
        while width < 256:
            pairs = block.reshape(len(out), 256 // (2 * width), 2, width)
            left = pairs[:, :, 0, :].copy()
            right = pairs[:, :, 1, :].copy()
            pairs[:, :, 0, :] = left + right
            pairs[:, :, 1, :] = left - right
            width *= 2
        block /= 16.0
    return out


def decode_levels(packed: np.ndarray) -> np.ndarray:
    """Unpack 2-bit symbols to the four signed integer reconstruction levels."""
    if (
        type(packed) is not np.ndarray
        or packed.dtype != np.uint8
        or packed.ndim != 2
        or packed.shape[1] != PACKED_BYTES
    ):
        raise ValueError("two-bit packed shape differs")
    symbols = np.empty((len(packed), DIMENSIONS), dtype=np.uint8)
    for shift in range(4):
        symbols[:, shift::4] = (packed >> (2 * shift)) & 3
    return (symbols.astype(np.int8) * 2 - 3).astype(np.int8)


def _fit_records(centered: np.ndarray, *, rotation_seed: int) -> np.ndarray:
    rotated = rotate_rows(centered, rotation_seed=rotation_seed)
    absolute = np.abs(rotated)
    scales = np.mean(absolute, axis=1, dtype=np.float64) / 1.6
    levels = np.empty(rotated.shape, dtype=np.int8)
    for _ in range(8):
        levels[:] = np.where(rotated >= 0, 1, -1) * np.where(
            absolute > 2 * scales[:, None], 3, 1
        )
        numerator = np.sum(levels * rotated, axis=1, dtype=np.float64)
        denominator = np.sum(levels.astype(np.float64) ** 2, axis=1)
        scales = numerator / denominator
    symbols = ((levels.astype(np.int16) + 3) // 2).astype(np.uint8)
    parts = symbols.reshape(len(symbols), PACKED_BYTES, 4)
    packed = parts[:, :, 0] | (parts[:, :, 1] << 2) | (parts[:, :, 2] << 4) | (parts[:, :, 3] << 6)
    records = np.empty((len(centered), ROW_BYTES), dtype=np.uint8)
    records[:, :PACKED_BYTES] = packed
    records[:, PACKED_BYTES : PACKED_BYTES + 4] = np.frombuffer(
        np.asarray(scales, dtype="<f4").tobytes(), dtype=np.uint8
    ).reshape(-1, 4)
    norms = np.sum(centered * centered, axis=1, dtype=np.float64)
    records[:, PACKED_BYTES + 4 :] = np.frombuffer(
        np.asarray(norms, dtype="<f4").tobytes(), dtype=np.uint8
    ).reshape(-1, 4)
    return records


def _group_payloads(
    records: np.ndarray, counts: Sequence[int]
) -> tuple[bytes, tuple[GroupRange, ...]]:
    if (
        type(records) is not np.ndarray
        or records.dtype != np.uint8
        or records.ndim != 2
        or records.shape[1] != ROW_BYTES
        or sum(counts) != len(records)
        or any(type(count) is not int or count <= 0 for count in counts)
    ):
        raise ValueError("two-bit group shape differs")
    offsets = [0]
    for count in counts:
        offsets.append(offsets[-1] + count)
    payloads: list[bytes] = []
    ranges: list[GroupRange] = []
    next_offset = 0
    for first in range(0, len(counts), GROUP_PAGES):
        end = min(first + GROUP_PAGES, len(counts))
        header = struct.pack("<I", end - first) + b"".join(
            struct.pack("<I", counts[page]) for page in range(first, end)
        )
        body = header + records[offsets[first] : offsets[end]].tobytes(order="C")
        ranges.append((first, end, next_offset, len(body), hashlib.sha256(body).hexdigest()))
        next_offset += len(body)
        payloads.append(body)
    return b"".join(payloads), tuple(ranges)


def construct_two_bit_codes(
    ids: Sequence[bytes],
    vectors: np.ndarray,
    membership: Sequence[MembershipRow],
    *,
    layout_seed: int,
    rotation_seed: int,
) -> TwoBitCodes:
    """Fit only source rows, writing records in sealed physical page order."""
    ordinals, counts, _ = _physical_order(ids, membership, layout_seed)
    if (
        type(vectors) is not np.ndarray
        or vectors.dtype != np.float32
        or vectors.shape != (len(ids), DIMENSIONS)
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("two-bit source authority differs")
    rotation_signs(rotation_seed)
    mean = np.mean(vectors, axis=0, dtype=np.float64).astype(np.float32)
    records = np.empty((len(ids), ROW_BYTES), dtype=np.uint8)
    for first in range(0, len(ids), BATCH_ROWS):
        last = min(first + BATCH_ROWS, len(ids))
        selected = np.asarray(ordinals[first:last], dtype=np.int64)
        centered = vectors[selected].astype(np.float64) - mean.astype(np.float64)
        records[first:last] = _fit_records(centered, rotation_seed=rotation_seed)
    _, ranges = _group_payloads(records, counts)
    return TwoBitCodes(mean, records, ordinals, counts, ranges, layout_seed, rotation_seed)


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _identity(path: Path, role: str) -> ArtifactIdentity:
    body = path.read_bytes()
    return ArtifactIdentity(role, path.resolve().as_uri(), hashlib.sha256(body).hexdigest(), len(body))


def write_two_bit_codes(
    root: Path,
    artifacts: TwoBitCodes,
    source_sha: bytes,
    membership_sha: bytes,
    tree_sha: bytes,
) -> TwoBitIdentities:
    """Seal a distinct incompatible mean/group representation."""
    if (
        any(type(digest) is not bytes or len(digest) != 32 for digest in (source_sha, membership_sha, tree_sha))
        or artifacts.mean.dtype != np.float32
        or artifacts.mean.shape != (DIMENSIONS,)
        or not np.isfinite(artifacts.mean).all()
    ):
        raise ValueError("two-bit artifact authority differs")
    groups_body, ranges = _group_payloads(artifacts.records, artifacts.page_row_counts)
    if ranges != artifacts.group_ranges:
        raise ValueError("two-bit ranges differ")
    root.mkdir(parents=True, exist_ok=True)
    (root / "mean.bin").write_bytes(np.asarray(artifacts.mean, dtype="<f4").tobytes())
    (root / "groups.bin").write_bytes(groups_body)
    mean = _identity(root / "mean.bin", "rotated-two-bit-mean")
    groups = _identity(root / "groups.bin", "rotated-two-bit-groups")
    seal = {
        "schema": SCHEMA,
        "source_sha256": source_sha.hex(),
        "membership_sha256": membership_sha.hex(),
        "tree_sha256": tree_sha.hex(),
        "layout_seed": artifacts.layout_seed,
        "rotation_seed": artifacts.rotation_seed,
        "rotation_signs_sha256": hashlib.sha256(
            rotation_signs(artifacts.rotation_seed).astype(np.int8).tobytes()
        ).hexdigest(),
        "rows": len(artifacts.source_ordinals),
        "dimensions": DIMENSIONS,
        "row_bytes": ROW_BYTES,
        "group_pages": GROUP_PAGES,
        "page_row_counts": list(artifacts.page_row_counts),
        "physical_order_sha256": _physical_order_sha256(artifacts.source_ordinals),
        "mean_sha256": mean.sha256,
        "groups_sha256": groups.sha256,
        "group_ranges": [list(group) for group in ranges],
    }
    (root / "seal.json").write_bytes(_canonical(seal))
    return TwoBitIdentities(mean, groups, _identity(root / "seal.json", "rotated-two-bit-seal"))


def read_two_bit_codes(
    root: Path,
    identities: TwoBitIdentities,
    ids: Sequence[bytes],
    membership: Sequence[MembershipRow],
    source_sha: bytes,
    membership_sha: bytes,
    tree_sha: bytes,
    *,
    dimensions: int,
    layout_seed: int,
    rotation_seed: int,
) -> TwoBitCodes:
    """Authenticate sealed source/order and every complete physical group."""
    ordinals, counts, member_source_sha = _physical_order(ids, membership, layout_seed)
    if member_source_sha != source_sha or dimensions != DIMENSIONS:
        raise ValueError("two-bit source binding differs")
    for identity, filename, role in (
        (identities.mean, "mean.bin", "rotated-two-bit-mean"),
        (identities.groups, "groups.bin", "rotated-two-bit-groups"),
        (identities.seal, "seal.json", "rotated-two-bit-seal"),
    ):
        body = (root / filename).read_bytes()
        if (
            identity.role != role
            or identity.encoded_bytes != len(body)
            or identity.sha256 != hashlib.sha256(body).hexdigest()
        ):
            raise ValueError("two-bit identity differs")
    mean = np.frombuffer((root / "mean.bin").read_bytes(), dtype="<f4")
    if mean.shape != (DIMENSIONS,) or not np.isfinite(mean).all():
        raise ValueError("two-bit mean differs")
    groups_body = (root / "groups.bin").read_bytes()
    records = np.empty((len(ids), ROW_BYTES), dtype=np.uint8)
    ranges: list[GroupRange] = []
    byte_offset = 0
    row_offset = 0
    for first in range(0, len(counts), GROUP_PAGES):
        end = min(first + GROUP_PAGES, len(counts))
        row_count = sum(counts[first:end])
        header_bytes = 4 + 4 * (end - first)
        length = header_bytes + row_count * ROW_BYTES
        group = groups_body[byte_offset : byte_offset + length]
        if len(group) != length or struct.unpack_from("<I", group)[0] != end - first:
            raise ValueError("two-bit group payload differs")
        for page in range(first, end):
            if struct.unpack_from("<I", group, 4 + 4 * (page - first))[0] != counts[page]:
                raise ValueError("two-bit group count differs")
        records[row_offset : row_offset + row_count] = np.frombuffer(
            group, dtype=np.uint8, count=row_count * ROW_BYTES, offset=header_bytes
        ).reshape(row_count, ROW_BYTES)
        ranges.append((first, end, byte_offset, length, hashlib.sha256(group).hexdigest()))
        byte_offset += length
        row_offset += row_count
    if byte_offset != len(groups_body):
        raise ValueError("two-bit group payload differs")
    seal_body = (root / "seal.json").read_bytes()
    try:
        seal = json.loads(seal_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("two-bit seal differs") from error
    expected = {
        "schema": SCHEMA,
        "source_sha256": source_sha.hex(),
        "membership_sha256": membership_sha.hex(),
        "tree_sha256": tree_sha.hex(),
        "layout_seed": layout_seed,
        "rotation_seed": rotation_seed,
        "rotation_signs_sha256": hashlib.sha256(
            rotation_signs(rotation_seed).astype(np.int8).tobytes()
        ).hexdigest(),
        "rows": len(ids),
        "dimensions": DIMENSIONS,
        "row_bytes": ROW_BYTES,
        "group_pages": GROUP_PAGES,
        "page_row_counts": list(counts),
        "physical_order_sha256": _physical_order_sha256(ordinals),
        "mean_sha256": identities.mean.sha256,
        "groups_sha256": identities.groups.sha256,
        "group_ranges": [list(group) for group in ranges],
    }
    if seal != expected or seal_body != _canonical(expected):
        raise ValueError("two-bit seal differs")
    return TwoBitCodes(mean.copy(), records, ordinals, counts, tuple(ranges), layout_seed, rotation_seed)
