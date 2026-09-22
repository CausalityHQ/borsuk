#!/usr/bin/env python3
"""Phase-separated source-only artifact seal for the row-score 100k cell."""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable, Sequence
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    EvaluationLimits,
    MembershipRow,
)
from scripts.native_row_score_code_artifacts import (
    CodeArtifacts,
    CodeIdentities,
    construct_code_artifacts,
    write_code_artifacts,
)
from scripts.native_row_score_evaluation import (
    RowScoreSample,
    aggregate_samples,
    evaluate_row_score_query,
)

SEAL_SCHEMA = "borsuk-row-score-cell-seal-v1"


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _remote(identity: ArtifactIdentity, prefix: str, name: str) -> ArtifactIdentity:
    return dataclasses.replace(
        identity, uri=prefix.rstrip("/") + "/artifacts/" + name
    )


def construct_cell(
    root: Path,
    prefix: str,
    ids: Sequence[bytes],
    vectors: np.ndarray,
    membership: Sequence[MembershipRow],
    source_sha: bytes,
    membership_sha: bytes,
    *,
    seed: int,
    iterations: int = 10,
) -> CodeIdentities:
    """Construct and seal source-only codes before any query capability."""
    if (
        not prefix.startswith("s3://")
        or len(source_sha) != 32
        or len(membership_sha) != 32
        or any(row.source_sha256 != source_sha for row in membership)
    ):
        raise ValueError("row-score construction authority differs")
    artifacts = construct_code_artifacts(
        ids, vectors, membership, seed=seed, iterations=iterations
    )
    local = write_code_artifacts(root, artifacts, source_sha, membership_sha)
    identities = CodeIdentities(
        _remote(local.books, prefix, "books.bin"),
        _remote(local.codes, prefix, "codes.bin"),
        _remote(local.seal, prefix, "seal.json"),
    )
    seal = {
        "schema": SEAL_SCHEMA,
        "source_sha256": source_sha.hex(),
        "membership_sha256": membership_sha.hex(),
        "books": dataclasses.asdict(identities.books),
        "codes": dataclasses.asdict(identities.codes),
        "code_seal": dataclasses.asdict(identities.seal),
    }
    (root / "sealed.json").write_bytes(_canonical(seal))
    return identities


def read_cell_seal(
    root: Path, prefix: str, source_sha: bytes, membership_sha: bytes
) -> CodeIdentities:
    """Read the exact code artifact identities expected at this attempt."""
    payload = (root / "sealed.json").read_bytes()
    try:
        seal = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("row-score cell seal differs") from error
    if (
        type(seal) is not dict
        or set(seal)
        != {"schema", "source_sha256", "membership_sha256", "books", "codes", "code_seal"}
        or seal["schema"] != SEAL_SCHEMA
        or seal["source_sha256"] != source_sha.hex()
        or seal["membership_sha256"] != membership_sha.hex()
        or payload != _canonical(seal)
    ):
        raise ValueError("row-score cell seal differs")
    try:
        identities = CodeIdentities(
            ArtifactIdentity(**seal["books"]),
            ArtifactIdentity(**seal["codes"]),
            ArtifactIdentity(**seal["code_seal"]),
        )
    except (TypeError, ValueError) as error:
        raise ValueError("row-score cell identities differ") from error
    for identity, name, role in (
        (identities.books, "books.bin", "row-score-pq48-books"),
        (identities.codes, "codes.bin", "row-score-pq48-codes"),
        (identities.seal, "seal.json", "row-score-pq48-seal"),
    ):
        if (
            identity.role != role
            or identity.uri != prefix.rstrip("/") + "/artifacts/" + name
        ):
            raise ValueError("row-score cell artifact URI differs")
    return identities


def evaluate_routes(
    *,
    queries: np.ndarray,
    truth: Sequence[Sequence[bytes]],
    retained_by_query: Sequence[Sequence[int]],
    artifacts: CodeArtifacts,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
    read_code_range: Callable[[int, int], bytes],
) -> tuple[tuple[RowScoreSample, ...], dict[str, int | str]]:
    """Evaluate the same sealed rows and route shortlist for every query."""
    if (
        type(queries) is not np.ndarray
        or queries.dtype != np.float32
        or queries.ndim != 2
        or queries.shape[0] == 0
        or queries.shape[0] != len(truth)
        or queries.shape[0] != len(retained_by_query)
        or not np.isfinite(queries).all()
    ):
        raise ValueError("row-score query cohort differs")
    owner_by_id: dict[bytes, int] = {}
    offset = 0
    for page, count in enumerate(artifacts.page_row_counts):
        for source_ordinal in artifacts.source_ordinals[offset : offset + count]:
            stable_id = stable_ids[source_ordinal]
            if stable_id in owner_by_id:
                raise ValueError("row-score owner authority differs")
            owner_by_id[stable_id] = page
        offset += count
    if len(owner_by_id) != len(stable_ids):
        raise ValueError("row-score owner authority differs")
    samples = tuple(
        evaluate_row_score_query(
            query_ordinal=ordinal,
            query=query,
            retained_pages=retained_by_query[ordinal],
            artifacts=artifacts,
            stable_ids=stable_ids,
            vectors=vectors,
            truth_ids=truth[ordinal],
            page_byte_sizes=page_byte_sizes,
            limits=limits,
            read_code_range=read_code_range,
            owner_by_id=owner_by_id,
        )
        for ordinal, query in enumerate(queries)
    )
    return samples, aggregate_samples(samples)
