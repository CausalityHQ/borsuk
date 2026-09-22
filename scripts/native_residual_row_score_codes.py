#!/usr/bin/env python3
"""Source-only two-stage residual PQ codes in sealed physical page order."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import ArtifactIdentity, MembershipRow
from scripts.native_row_score_code_artifacts import (
    CodeIdentities,
    _physical_order,
    _physical_order_sha256,
)
from scripts.v97_row_width_screen import PQ24X8, encode_pq, fit_pq
from scripts.v102_two_wave_pq48_refinement import PQ48X8

SCHEMA = "borsuk-residual-row-score-code-artifacts-v1"


@dataclasses.dataclass(frozen=True, slots=True)
class ResidualCodes:
    first_books: np.ndarray
    residual_books: np.ndarray
    codes: np.ndarray
    source_ordinals: tuple[int, ...]
    page_row_counts: tuple[int, ...]
    seed: int


def _reconstruct_first(books: np.ndarray, codes: np.ndarray) -> np.ndarray:
    """Decode source-order first-stage rows without query or truth access."""
    count, subspaces = codes.shape
    width = books.shape[2]
    reconstructed = np.empty((count, subspaces * width), dtype=np.float32)
    for subspace in range(subspaces):
        lo = subspace * width
        reconstructed[:, lo : lo + width] = books[subspace, codes[:, subspace]]
    return reconstructed


def construct_residual_codes(
    ids: Sequence[bytes],
    vectors: np.ndarray,
    membership: Sequence[MembershipRow],
    *,
    seed: int,
    sample_rows: int = 100_000,
    iterations: int = 10,
) -> ResidualCodes:
    """Fit first and residual stages using source vectors only."""
    ordinals, counts, _ = _physical_order(ids, membership, seed)
    if (
        type(vectors) is not np.ndarray
        or vectors.dtype != np.float32
        or vectors.ndim != 2
        or vectors.shape[0] != len(ids)
        or vectors.shape[1] % 48
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("residual code source authority differs")
    first_books = fit_pq(
        vectors, PQ48X8, seed=seed, sample_rows=sample_rows, iterations=iterations
    )
    first_source_codes = encode_pq(vectors, first_books, PQ48X8)
    residuals = np.ascontiguousarray(
        vectors - _reconstruct_first(first_books, first_source_codes),
        dtype=np.float32,
    )
    residual_books = fit_pq(
        residuals, PQ24X8, seed=seed, sample_rows=sample_rows, iterations=iterations
    )
    residual_source_codes = encode_pq(residuals, residual_books, PQ24X8)
    physical = np.asarray(ordinals, dtype=np.int64)
    codes = np.empty((len(ids), 72), dtype=np.uint8)
    codes[:, :48] = first_source_codes[physical]
    codes[:, 48:] = residual_source_codes[physical]
    return ResidualCodes(first_books, residual_books, codes, ordinals, counts, seed)


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _identity(path: Path, role: str) -> ArtifactIdentity:
    body = path.read_bytes()
    return ArtifactIdentity(role, path.resolve().as_uri(), hashlib.sha256(body).hexdigest(), len(body))


def write_residual_codes(
    root: Path, artifacts: ResidualCodes, source_sha: bytes, membership_sha: bytes
) -> CodeIdentities:
    """Persist books and interleaved codes with a canonical source-bound seal."""
    dimensions = artifacts.first_books.shape[0] * artifacts.first_books.shape[2]
    if (
        len(source_sha) != 32
        or len(membership_sha) != 32
        or dimensions <= 0
        or artifacts.first_books.dtype != np.float32
        or artifacts.first_books.shape != (48, 256, dimensions // 48)
        or artifacts.residual_books.dtype != np.float32
        or artifacts.residual_books.shape != (24, 256, dimensions // 24)
        or artifacts.codes.dtype != np.uint8
        or artifacts.codes.shape != (len(artifacts.source_ordinals), 72)
        or sum(artifacts.page_row_counts) != len(artifacts.source_ordinals)
    ):
        raise ValueError("residual code artifact authority differs")
    root.mkdir(parents=True, exist_ok=True)
    (root / "books.bin").write_bytes(
        np.asarray(artifacts.first_books, dtype="<f4").tobytes(order="C")
        + np.asarray(artifacts.residual_books, dtype="<f4").tobytes(order="C")
    )
    (root / "codes.bin").write_bytes(artifacts.codes.tobytes(order="C"))
    books = _identity(root / "books.bin", "residual-row-score-books")
    codes = _identity(root / "codes.bin", "residual-row-score-codes")
    seal = {
        "schema": SCHEMA,
        "source_sha256": source_sha.hex(),
        "membership_sha256": membership_sha.hex(),
        "seed": artifacts.seed,
        "rows": len(artifacts.source_ordinals),
        "dimensions": dimensions,
        "page_row_counts": list(artifacts.page_row_counts),
        "physical_order_sha256": _physical_order_sha256(artifacts.source_ordinals),
        "books_sha256": books.sha256,
        "codes_sha256": codes.sha256,
    }
    (root / "seal.json").write_bytes(_canonical(seal))
    return CodeIdentities(
        books, codes, _identity(root / "seal.json", "residual-row-score-seal")
    )


def read_residual_codes(
    root: Path,
    identities: CodeIdentities,
    ids: Sequence[bytes],
    membership: Sequence[MembershipRow],
    source_sha: bytes,
    membership_sha: bytes,
    *,
    dimensions: int,
    seed: int,
) -> ResidualCodes:
    """Authenticate the full code plane and its source/physical-order seal."""
    ordinals, counts, membership_source_sha = _physical_order(ids, membership, seed)
    if membership_source_sha != source_sha or dimensions <= 0 or dimensions % 48:
        raise ValueError("residual code source binding differs")
    for identity, name, role in (
        (identities.books, "books.bin", "residual-row-score-books"),
        (identities.codes, "codes.bin", "residual-row-score-codes"),
        (identities.seal, "seal.json", "residual-row-score-seal"),
    ):
        body = (root / name).read_bytes()
        if (
            identity.role != role
            or identity.encoded_bytes != len(body)
            or identity.sha256 != hashlib.sha256(body).hexdigest()
        ):
            raise ValueError("residual code identity differs")
    seal_bytes = (root / "seal.json").read_bytes()
    try:
        seal = json.loads(seal_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("residual code seal differs") from error
    if type(seal) is not dict or seal.get("physical_order_sha256") != _physical_order_sha256(ordinals):
        raise ValueError("residual code physical order differs")
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
        raise ValueError("residual code seal differs")
    book_values = np.frombuffer((root / "books.bin").read_bytes(), dtype="<f4")
    codes = np.frombuffer((root / "codes.bin").read_bytes(), dtype=np.uint8)
    first_size = 48 * 256 * (dimensions // 48)
    second_size = 24 * 256 * (dimensions // 24)
    if book_values.size != first_size + second_size or codes.size != len(ids) * 72:
        raise ValueError("residual code shape differs")
    return ResidualCodes(
        book_values[:first_size].reshape(48, 256, dimensions // 48).copy(),
        book_values[first_size:].reshape(24, 256, dimensions // 24).copy(),
        codes.reshape(len(ids), 72).copy(),
        ordinals,
        counts,
        seed,
    )
