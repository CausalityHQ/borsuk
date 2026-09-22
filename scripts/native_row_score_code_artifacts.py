#!/usr/bin/env python3
"""Source-only, page-ordered PQ48 artifacts for the 100k routing falsifier."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    LayoutMethod,
    MembershipRow,
)
from scripts.v97_row_width_screen import encode_pq, fit_pq
from scripts.v102_two_wave_pq48_refinement import PQ48X8

SCHEMA = "borsuk-row-score-code-artifacts-v1"


@dataclasses.dataclass(frozen=True, slots=True)
class CodeArtifacts:
    books: np.ndarray
    codes: np.ndarray
    source_ordinals: tuple[int, ...]
    page_row_counts: tuple[int, ...]
    seed: int


@dataclasses.dataclass(frozen=True, slots=True)
class CodeIdentities:
    books: ArtifactIdentity
    codes: ArtifactIdentity
    seal: ArtifactIdentity


def _physical_order(
    ids: Sequence[bytes], membership: Sequence[MembershipRow], seed: int
) -> tuple[tuple[int, ...], tuple[int, ...], bytes]:
    if (
        not ids
        or len(ids) != len(membership)
        or len(set(ids)) != len(ids)
        or type(seed) is not int
        or seed < 0
    ):
        raise ValueError("code membership authority differs")
    rows = sorted(membership, key=lambda row: (row.page_ordinal, row.in_page_ordinal))
    ordinals = tuple(row.source_ordinal for row in rows)
    page_count = max(row.page_ordinal for row in rows) + 1
    counts = [0] * page_count
    source_shas = {row.source_sha256 for row in rows}
    for row in rows:
        if (
            type(row) is not MembershipRow
            or row.method is not LayoutMethod.TWO_MEANS_480K
            or row.seed != seed
            or type(row.source_ordinal) is not int
            or not 0 <= row.source_ordinal < len(ids)
            or row.stable_id != ids[row.source_ordinal]
            or type(row.page_ordinal) is not int
            or not 0 <= row.page_ordinal < page_count
            or type(row.in_page_ordinal) is not int
            or row.in_page_ordinal != counts[row.page_ordinal]
        ):
            raise ValueError("code membership authority differs")
        counts[row.page_ordinal] += 1
    if (
        set(ordinals) != set(range(len(ids)))
        or any(count == 0 for count in counts)
        or any(row.page_rows != counts[row.page_ordinal] for row in rows)
        or len(source_shas) != 1
        or len(next(iter(source_shas))) != 32
    ):
        raise ValueError("code membership authority differs")
    return ordinals, tuple(counts), next(iter(source_shas))


def construct_code_artifacts(
    ids: Sequence[bytes],
    vectors: np.ndarray,
    membership: Sequence[MembershipRow],
    *,
    seed: int,
    sample_rows: int = 100_000,
    iterations: int = 10,
) -> CodeArtifacts:
    """Train on source rows, then encode them in sealed physical page order."""
    ordinals, counts, _ = _physical_order(ids, membership, seed)
    if (
        type(vectors) is not np.ndarray
        or vectors.dtype != np.float32
        or vectors.ndim != 2
        or vectors.shape[0] != len(ids)
        or vectors.shape[1] % PQ48X8.subspaces
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("code source authority differs")
    books = fit_pq(
        vectors, PQ48X8, seed=seed, sample_rows=sample_rows, iterations=iterations
    )
    codes = encode_pq(np.ascontiguousarray(vectors[list(ordinals)]), books, PQ48X8)
    return CodeArtifacts(books, codes, ordinals, counts, seed)


def _identity(path: Path, role: str) -> ArtifactIdentity:
    data = path.read_bytes()
    return ArtifactIdentity(role, path.resolve().as_uri(), hashlib.sha256(data).hexdigest(), len(data))


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _physical_order_sha256(ordinals: Sequence[int]) -> str:
    if any(type(ordinal) is not int or not 0 <= ordinal < 1 << 32 for ordinal in ordinals):
        raise ValueError("code physical order differs")
    return hashlib.sha256(np.asarray(ordinals, dtype="<u4").tobytes(order="C")).hexdigest()


def write_code_artifacts(
    root: Path, artifacts: CodeArtifacts, source_sha: bytes, membership_sha: bytes
) -> CodeIdentities:
    """Write binary codebooks and codes plus a digest-bound seal."""
    if (
        len(source_sha) != 32
        or len(membership_sha) != 32
        or artifacts.books.dtype != np.float32
        or artifacts.books.shape[0:2] != (48, 256)
        or artifacts.codes.dtype != np.uint8
        or artifacts.codes.shape != (len(artifacts.source_ordinals), 48)
        or sum(artifacts.page_row_counts) != len(artifacts.source_ordinals)
    ):
        raise ValueError("code artifact authority differs")
    root.mkdir(parents=True, exist_ok=True)
    (root / "books.bin").write_bytes(np.asarray(artifacts.books, dtype="<f4").tobytes(order="C"))
    (root / "codes.bin").write_bytes(artifacts.codes.tobytes(order="C"))
    books = _identity(root / "books.bin", "row-score-pq48-books")
    codes = _identity(root / "codes.bin", "row-score-pq48-codes")
    seal = {
        "schema": SCHEMA,
        "source_sha256": source_sha.hex(),
        "membership_sha256": membership_sha.hex(),
        "seed": artifacts.seed,
        "rows": len(artifacts.source_ordinals),
        "dimensions": artifacts.books.shape[0] * artifacts.books.shape[2],
        "page_row_counts": list(artifacts.page_row_counts),
        "physical_order_sha256": _physical_order_sha256(artifacts.source_ordinals),
        "books_sha256": books.sha256,
        "codes_sha256": codes.sha256,
    }
    (root / "seal.json").write_bytes(_canonical(seal))
    return CodeIdentities(books, codes, _identity(root / "seal.json", "row-score-pq48-seal"))


def read_code_artifacts(
    root: Path,
    identities: CodeIdentities,
    ids: Sequence[bytes],
    membership: Sequence[MembershipRow],
    source_sha: bytes,
    membership_sha: bytes,
    *,
    dimensions: int,
    seed: int,
) -> CodeArtifacts:
    """Authenticate and decode the sealed page-ordered binary artifacts."""
    ordinals, counts, membership_source_sha = _physical_order(ids, membership, seed)
    if membership_source_sha != source_sha or dimensions % 48:
        raise ValueError("code source binding differs")
    for identity, filename, role in (
        (identities.books, "books.bin", "row-score-pq48-books"),
        (identities.codes, "codes.bin", "row-score-pq48-codes"),
        (identities.seal, "seal.json", "row-score-pq48-seal"),
    ):
        data = (root / filename).read_bytes()
        if (
            identity.role != role
            or identity.encoded_bytes != len(data)
            or identity.sha256 != hashlib.sha256(data).hexdigest()
        ):
            raise ValueError("code identity differs")
    seal_bytes = (root / "seal.json").read_bytes()
    try:
        seal = json.loads(seal_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("code seal differs") from error
    if type(seal) is not dict or seal.get("physical_order_sha256") != _physical_order_sha256(ordinals):
        raise ValueError("code physical order differs")
    expected = {
        "schema": SCHEMA,
        "source_sha256": source_sha.hex(),
        "membership_sha256": membership_sha.hex(),
        "seed": seed,
        "rows": len(ids),
        "dimensions": dimensions,
        "page_row_counts": list(counts),
        "physical_order_sha256": _physical_order_sha256(ordinals),
        "books_sha256": identities.books.sha256,
        "codes_sha256": identities.codes.sha256,
    }
    if seal != expected or seal_bytes != _canonical(expected):
        raise ValueError("code seal differs")
    books = np.frombuffer((root / "books.bin").read_bytes(), dtype="<f4")
    codes = np.frombuffer((root / "codes.bin").read_bytes(), dtype=np.uint8)
    if books.size != 48 * 256 * (dimensions // 48) or codes.size != len(ids) * 48:
        raise ValueError("code artifact shape differs")
    return CodeArtifacts(
        books.reshape(48, 256, dimensions // 48).copy(),
        codes.reshape(len(ids), 48).copy(),
        ordinals,
        counts,
        seed,
    )
