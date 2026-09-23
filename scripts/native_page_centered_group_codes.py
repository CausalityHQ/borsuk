#!/usr/bin/env python3
"""Source-only page-centered PQ48 codes in authenticated four-page groups."""

from __future__ import annotations

import dataclasses
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
from scripts.v97_row_width_screen import encode_pq, fit_pq
from scripts.v102_two_wave_pq48_refinement import PQ48X8

SCHEMA = "borsuk-page-centered-group-codes-v1"
GROUP_PAGES = 4
ROW_BYTES = 48
GroupRange = tuple[int, int, int, int, str]


@dataclasses.dataclass(frozen=True, slots=True)
class GroupCodes:
    books: np.ndarray
    page_means: np.ndarray
    codes: np.ndarray
    source_ordinals: tuple[int, ...]
    page_row_counts: tuple[int, ...]
    group_ranges: tuple[GroupRange, ...]
    seed: int


@dataclasses.dataclass(frozen=True, slots=True)
class GroupCodeIdentities:
    books: ArtifactIdentity
    groups: ArtifactIdentity
    seal: ArtifactIdentity


def _group_payloads(
    means: np.ndarray, codes: np.ndarray, counts: Sequence[int]
) -> tuple[bytes, tuple[GroupRange, ...]]:
    if (
        means.dtype != np.float32
        or means.ndim != 2
        or means.shape[0] != len(counts)
        or means.shape[1] <= 0
        or means.shape[1] % ROW_BYTES
        or codes.dtype != np.uint8
        or codes.ndim != 2
        or codes.shape[1] != ROW_BYTES
        or sum(counts) != len(codes)
        or any(type(count) is not int or count <= 0 for count in counts)
        or not np.isfinite(means).all()
    ):
        raise ValueError("group code shape differs")
    offsets = [0]
    for count in counts:
        offsets.append(offsets[-1] + count)
    payloads: list[bytes] = []
    ranges: list[GroupRange] = []
    next_offset = 0
    for first in range(0, len(counts), GROUP_PAGES):
        end = min(first + GROUP_PAGES, len(counts))
        header = bytearray(struct.pack("<I", end - first))
        for page in range(first, end):
            header.extend(struct.pack("<I", counts[page]))
            header.extend(np.asarray(means[page], dtype="<f4").tobytes())
        body = bytes(header) + codes[offsets[first] : offsets[end]].tobytes(order="C")
        ranges.append((first, end, next_offset, len(body), hashlib.sha256(body).hexdigest()))
        next_offset += len(body)
        payloads.append(body)
    return b"".join(payloads), tuple(ranges)


def construct_group_codes(
    ids: Sequence[bytes],
    vectors: np.ndarray,
    membership: Sequence[MembershipRow],
    *,
    seed: int,
    sample_rows: int = 100_000,
    iterations: int = 10,
) -> GroupCodes:
    """Fit one PQ48 book over source rows centered by their sealed page."""
    ordinals, counts, _ = _physical_order(ids, membership, seed)
    if (
        type(vectors) is not np.ndarray
        or vectors.dtype != np.float32
        or vectors.ndim != 2
        or vectors.shape[0] != len(ids)
        or vectors.shape[1] % ROW_BYTES
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("group code source authority differs")
    physical = np.asarray(ordinals, dtype=np.int64)
    page_means = np.empty((len(counts), vectors.shape[1]), dtype=np.float32)
    source_residuals = np.empty_like(vectors)
    start = 0
    for page, count in enumerate(counts):
        selected = physical[start : start + count]
        mean = vectors[selected].mean(axis=0, dtype=np.float64).astype(np.float32)
        page_means[page] = mean
        source_residuals[selected] = vectors[selected] - mean
        start += count
    books = fit_pq(
        source_residuals,
        PQ48X8,
        seed=seed,
        sample_rows=sample_rows,
        iterations=iterations,
    )
    codes = encode_pq(
        np.ascontiguousarray(source_residuals[physical]), books, PQ48X8
    )
    _, ranges = _group_payloads(page_means, codes, counts)
    return GroupCodes(books, page_means, codes, ordinals, counts, ranges, seed)


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _identity(path: Path, role: str) -> ArtifactIdentity:
    data = path.read_bytes()
    return ArtifactIdentity(role, path.resolve().as_uri(), hashlib.sha256(data).hexdigest(), len(data))


def write_group_codes(
    root: Path,
    artifacts: GroupCodes,
    source_sha: bytes,
    membership_sha: bytes,
    tree_sha: bytes,
) -> GroupCodeIdentities:
    """Write one immutable group plane, books and canonical authority seal."""
    if (
        any(len(digest) != 32 for digest in (source_sha, membership_sha, tree_sha))
        or artifacts.books.dtype != np.float32
        or artifacts.books.shape != (48, 256, artifacts.page_means.shape[1] // 48)
        or artifacts.seed < 0
    ):
        raise ValueError("group code artifact authority differs")
    body, ranges = _group_payloads(
        artifacts.page_means, artifacts.codes, artifacts.page_row_counts
    )
    if ranges != artifacts.group_ranges:
        raise ValueError("group code ranges differ")
    root.mkdir(parents=True, exist_ok=True)
    (root / "books.bin").write_bytes(np.asarray(artifacts.books, dtype="<f4").tobytes())
    (root / "groups.bin").write_bytes(body)
    books = _identity(root / "books.bin", "page-centered-pq48-books")
    groups = _identity(root / "groups.bin", "page-centered-groups")
    seal = {
        "schema": SCHEMA,
        "source_sha256": source_sha.hex(),
        "membership_sha256": membership_sha.hex(),
        "tree_sha256": tree_sha.hex(),
        "seed": artifacts.seed,
        "rows": len(artifacts.source_ordinals),
        "dimensions": artifacts.page_means.shape[1],
        "group_pages": GROUP_PAGES,
        "page_row_counts": list(artifacts.page_row_counts),
        "physical_order_sha256": _physical_order_sha256(artifacts.source_ordinals),
        "books_sha256": books.sha256,
        "groups_sha256": groups.sha256,
        "group_ranges": [list(group) for group in ranges],
    }
    (root / "seal.json").write_bytes(_canonical(seal))
    return GroupCodeIdentities(
        books, groups, _identity(root / "seal.json", "page-centered-group-seal")
    )


def read_group_codes(
    root: Path,
    identities: GroupCodeIdentities,
    ids: Sequence[bytes],
    membership: Sequence[MembershipRow],
    source_sha: bytes,
    membership_sha: bytes,
    tree_sha: bytes,
    *,
    dimensions: int,
    seed: int,
) -> GroupCodes:
    """Authenticate the physical order and every group before decoding."""
    ordinals, counts, member_source_sha = _physical_order(ids, membership, seed)
    if member_source_sha != source_sha or dimensions <= 0 or dimensions % ROW_BYTES:
        raise ValueError("group code source binding differs")
    for identity, filename, role in (
        (identities.books, "books.bin", "page-centered-pq48-books"),
        (identities.groups, "groups.bin", "page-centered-groups"),
        (identities.seal, "seal.json", "page-centered-group-seal"),
    ):
        data = (root / filename).read_bytes()
        if (
            identity.role != role
            or identity.encoded_bytes != len(data)
            or identity.sha256 != hashlib.sha256(data).hexdigest()
        ):
            raise ValueError("group code identity differs")
    seal_bytes = (root / "seal.json").read_bytes()
    try:
        seal = json.loads(seal_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("group code seal differs") from error
    if (
        type(seal) is not dict
        or seal.get("physical_order_sha256") != _physical_order_sha256(ordinals)
    ):
        raise ValueError("group code physical order differs")
    body = (root / "groups.bin").read_bytes()
    means = np.empty((len(counts), dimensions), dtype=np.float32)
    codes = np.empty((len(ids), ROW_BYTES), dtype=np.uint8)
    ranges: list[GroupRange] = []
    row_offset = 0
    byte_offset = 0
    for first in range(0, len(counts), GROUP_PAGES):
        end = min(first + GROUP_PAGES, len(counts))
        header_bytes = 4 + (end - first) * (4 + dimensions * 4)
        row_count = sum(counts[first:end])
        length = header_bytes + row_count * ROW_BYTES
        group = body[byte_offset : byte_offset + length]
        if len(group) != length or struct.unpack_from("<I", group)[0] != end - first:
            raise ValueError("group code payload differs")
        for page in range(first, end):
            local = 4 + (page - first) * (4 + dimensions * 4)
            if struct.unpack_from("<I", group, local)[0] != counts[page]:
                raise ValueError("group code page count differs")
            means[page] = np.frombuffer(
                group, dtype="<f4", count=dimensions, offset=local + 4
            )
        codes[row_offset : row_offset + row_count] = np.frombuffer(
            group, dtype=np.uint8, count=row_count * ROW_BYTES, offset=header_bytes
        ).reshape(row_count, ROW_BYTES)
        ranges.append(
            (first, end, byte_offset, length, hashlib.sha256(group).hexdigest())
        )
        row_offset += row_count
        byte_offset += length
    if byte_offset != len(body) or not np.isfinite(means).all():
        raise ValueError("group code payload differs")
    expected = {
        "schema": SCHEMA,
        "source_sha256": source_sha.hex(),
        "membership_sha256": membership_sha.hex(),
        "tree_sha256": tree_sha.hex(),
        "seed": seed,
        "rows": len(ids),
        "dimensions": dimensions,
        "group_pages": GROUP_PAGES,
        "page_row_counts": list(counts),
        "physical_order_sha256": _physical_order_sha256(ordinals),
        "books_sha256": identities.books.sha256,
        "groups_sha256": identities.groups.sha256,
        "group_ranges": [list(group) for group in ranges],
    }
    if seal != expected or seal_bytes != _canonical(expected):
        raise ValueError("group code seal differs")
    books = np.frombuffer((root / "books.bin").read_bytes(), dtype="<f4")
    if books.size != 48 * 256 * (dimensions // 48):
        raise ValueError("group code books shape differs")
    return GroupCodes(
        books.reshape(48, 256, dimensions // 48).copy(),
        means,
        codes,
        ordinals,
        counts,
        tuple(ranges),
        seed,
    )
