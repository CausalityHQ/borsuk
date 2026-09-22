#!/usr/bin/env python3
"""Phase-separated source-only artifact seal for the residual row-score 100k cell."""

from __future__ import annotations

import argparse
import dataclasses
import json
from collections.abc import Callable, Sequence
from pathlib import Path

import numpy as np

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    EvaluationLimits,
    MembershipRow,
    read_geometric_router_parquet,
    route_geometric_query,
)
from scripts.native_page_microcluster_cell import (
    FROZEN_INPUTS,
    CellInputs,
    _read_inputs,
    _read_queries_truth,
)
from scripts.native_residual_row_score_codes import (
    CodeIdentities,
    ResidualCodes,
    construct_residual_codes,
    read_residual_codes,
    write_residual_codes,
)
from scripts.native_residual_row_score_evaluation import (
    ResidualSample,
    aggregate_residual_samples,
    build_cross_terms,
    evaluate_residual_query,
)
from scripts.native_residual_row_score_evidence import (
    read_residual_evidence,
    write_residual_evidence,
)
from scripts.native_row_score_s3 import S3CodeRangeReader
from scripts.validate_native_geometric_layout_result import _independent_route
from scripts.validate_native_residual_row_score_result import validate_residual_samples

SEAL_SCHEMA = "borsuk-residual-row-score-cell-seal-v1"
RESULT_SCHEMA = "borsuk-residual-row-score-cell-result-v1"
PRIOR_TREE = ArtifactIdentity(
    "geometric-router-tree",
    "s3://borsuk-bench-453182569524-euc1/research/native-geometric-router/"
    "67c88488fb17a9f02715c6d262a22225cf950de5/"
    "runs/relaion-100k-dev1000-a0001/artifacts/tree.parquet",
    "e38f7e7385147eef0bd101aa510fb15579edbd07d7516505d70eb17b7326c974",
    123_743,
)
PRIOR_PAGES = ArtifactIdentity(
    "geometric-page-representatives",
    "s3://borsuk-bench-453182569524-euc1/research/native-geometric-router/"
    "67c88488fb17a9f02715c6d262a22225cf950de5/"
    "runs/relaion-100k-dev1000-a0001/artifacts/pages.parquet",
    "f7c3c5b39a6a0e7c7a3fd05082599283d94c3960e26a33d90c862303506a7476",
    476_403,
)


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
        raise ValueError("residual row-score construction authority differs")
    artifacts = construct_residual_codes(
        ids, vectors, membership, seed=seed, iterations=iterations
    )
    local = write_residual_codes(root, artifacts, source_sha, membership_sha)
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
        raise ValueError("residual row-score cell seal differs") from error
    if (
        type(seal) is not dict
        or set(seal)
        != {"schema", "source_sha256", "membership_sha256", "books", "codes", "code_seal"}
        or seal["schema"] != SEAL_SCHEMA
        or seal["source_sha256"] != source_sha.hex()
        or seal["membership_sha256"] != membership_sha.hex()
        or payload != _canonical(seal)
    ):
        raise ValueError("residual row-score cell seal differs")
    try:
        identities = CodeIdentities(
            ArtifactIdentity(**seal["books"]),
            ArtifactIdentity(**seal["codes"]),
            ArtifactIdentity(**seal["code_seal"]),
        )
    except (TypeError, ValueError) as error:
        raise ValueError("residual row-score cell identities differ") from error
    for identity, name, role in (
        (identities.books, "books.bin", "residual-row-score-books"),
        (identities.codes, "codes.bin", "residual-row-score-codes"),
        (identities.seal, "seal.json", "residual-row-score-seal"),
    ):
        if (
            identity.role != role
            or identity.uri != prefix.rstrip("/") + "/artifacts/" + name
        ):
            raise ValueError("residual row-score cell artifact URI differs")
    return identities


def evaluate_routes(
    *,
    queries: np.ndarray,
    truth: Sequence[Sequence[bytes]],
    retained_by_query: Sequence[Sequence[int]],
    artifacts: ResidualCodes,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
    read_code_range: Callable[[int, int], bytes],
) -> tuple[tuple[ResidualSample, ...], dict[str, int | str]]:
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
        raise ValueError("residual row-score query cohort differs")
    owner_by_id: dict[bytes, int] = {}
    offset = 0
    for page, count in enumerate(artifacts.page_row_counts):
        for source_ordinal in artifacts.source_ordinals[offset : offset + count]:
            stable_id = stable_ids[source_ordinal]
            if stable_id in owner_by_id:
                raise ValueError("residual row-score owner authority differs")
            owner_by_id[stable_id] = page
        offset += count
    if len(owner_by_id) != len(stable_ids):
        raise ValueError("residual row-score owner authority differs")
    cross_terms = build_cross_terms(artifacts.first_books, artifacts.residual_books)
    samples = tuple(
        evaluate_residual_query(
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
            cross_terms=cross_terms,
            owner_by_id=owner_by_id,
        )
        for ordinal, query in enumerate(queries)
    )
    return samples, aggregate_residual_samples(samples)


def run_construct_phase(
    root: Path, prefix: str, inputs: CellInputs = FROZEN_INPUTS
) -> CodeIdentities:
    """Authenticate source and membership, then publish only source-only codes."""
    ids, vectors, membership = _read_inputs(root, inputs)
    return construct_cell(
        root,
        prefix,
        ids,
        vectors,
        membership,
        bytes.fromhex(inputs.layout.source.sha256),
        bytes.fromhex(inputs.membership.sha256),
        seed=inputs.layout.seed,
    )


def run_evaluate_phase(
    root: Path,
    out: Path,
    prefix: str,
    inputs: CellInputs = FROZEN_INPUTS,
    *,
    code_reader: Callable[[int, int], bytes] | None = None,
    source_archive: SourceArchiveIdentity | None = None,
    requirements_sha256: str | None = None,
) -> dict[str, object]:
    """Route frozen queries using sealed codes and verified S3 code ranges."""
    ids, vectors, membership = _read_inputs(root, inputs)
    source_sha = bytes.fromhex(inputs.layout.source.sha256)
    membership_sha = bytes.fromhex(inputs.membership.sha256)
    identities = read_cell_seal(root, prefix, source_sha, membership_sha)
    artifacts = read_residual_codes(
        root, identities, ids, membership, source_sha, membership_sha,
        dimensions=inputs.layout.dimensions, seed=inputs.layout.seed,
    )
    router = read_geometric_router_parquet(
        root / "tree.parquet", root / "pages.parquet", inputs.layout,
        membership, PRIOR_TREE, PRIOR_PAGES,
    )
    queries, truth = _read_queries_truth(root, inputs)
    if code_reader is None:
        import boto3
        from botocore.config import Config

        code_reader = S3CodeRangeReader(
            boto3.client(
                "s3",
                region_name="eu-central-1",
                config=Config(retries={"mode": "standard", "total_max_attempts": 1}),
            ),
            identities.codes.uri,
            object_bytes=identities.codes.encoded_bytes,
        )
    retained = tuple(
        route_geometric_query(
            router, query, leaf_frontier=128, limits=inputs.limits
        ).retained_leaf_pages
        for query in queries
    )
    samples, metrics = evaluate_routes(
        queries=queries,
        truth=truth,
        retained_by_query=retained,
        artifacts=artifacts,
        stable_ids=ids,
        vectors=vectors,
        page_byte_sizes=tuple(page.encoded_page_bytes for page in router.pages),
        limits=inputs.limits,
        read_code_range=code_reader,
    )
    out.mkdir(parents=True, exist_ok=True)
    evidence = _remote(write_residual_evidence(out / "evidence.json", samples), prefix, "evidence.json")
    result: dict[str, object] = {
        "schema": RESULT_SCHEMA,
        "claim_eligible": False,
        "source": dataclasses.asdict(inputs.layout.source),
        "membership": dataclasses.asdict(inputs.membership),
        "tree": dataclasses.asdict(PRIOR_TREE),
        "pages": dataclasses.asdict(PRIOR_PAGES),
        "books": dataclasses.asdict(identities.books),
        "codes": dataclasses.asdict(identities.codes),
        "code_seal": dataclasses.asdict(identities.seal),
        "queries": dataclasses.asdict(inputs.queries),
        "truth": dataclasses.asdict(inputs.truth),
        "evidence": dataclasses.asdict(evidence),
        "limits": dataclasses.asdict(inputs.limits),
        "metrics": metrics,
        "output_prefix": prefix.rstrip("/"),
        "source_archive": dataclasses.asdict(source_archive) if source_archive else None,
        "requirements_sha256": requirements_sha256,
    }
    (out / "result.json").write_bytes(_canonical(result))
    return result


def run_validate_phase(
    root: Path,
    out: Path,
    prefix: str,
    source_commit: str,
    inputs: CellInputs = FROZEN_INPUTS,
    *,
    source_archive: SourceArchiveIdentity | None = None,
    requirements_sha256: str | None = None,
) -> dict[str, object]:
    """Rebuild codes and independently replay every frozen query and hit."""
    if (
        len(source_commit) != 40
        or any(character not in "0123456789abcdef" for character in source_commit)
    ):
        raise ValueError("residual row-score source revision differs")
    payload = (out / "result.json").read_bytes()
    try:
        result = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("residual row-score result differs") from error
    source_sha = bytes.fromhex(inputs.layout.source.sha256)
    membership_sha = bytes.fromhex(inputs.membership.sha256)
    identities = read_cell_seal(root, prefix, source_sha, membership_sha)
    expected_keys = {
        "schema", "claim_eligible", "source", "membership", "tree", "pages",
        "books", "codes", "code_seal", "queries", "truth", "evidence",
        "limits", "metrics", "output_prefix",
        "source_archive", "requirements_sha256",
    }
    if (
        type(result) is not dict
        or set(result) != expected_keys
        or payload != _canonical(result)
        or result["schema"] != RESULT_SCHEMA
        or result["claim_eligible"] is not False
        or result["source"] != dataclasses.asdict(inputs.layout.source)
        or result["membership"] != dataclasses.asdict(inputs.membership)
        or result["tree"] != dataclasses.asdict(PRIOR_TREE)
        or result["pages"] != dataclasses.asdict(PRIOR_PAGES)
        or result["books"] != dataclasses.asdict(identities.books)
        or result["codes"] != dataclasses.asdict(identities.codes)
        or result["code_seal"] != dataclasses.asdict(identities.seal)
        or result["queries"] != dataclasses.asdict(inputs.queries)
        or result["truth"] != dataclasses.asdict(inputs.truth)
        or result["limits"] != dataclasses.asdict(inputs.limits)
        or result["output_prefix"] != prefix.rstrip("/")
        or result["source_archive"] != (
            dataclasses.asdict(source_archive) if source_archive else None
        )
        or result["requirements_sha256"] != requirements_sha256
    ):
        raise ValueError("residual row-score result authority differs")
    try:
        evidence = ArtifactIdentity(**result["evidence"])
    except (TypeError, ValueError) as error:
        raise ValueError("residual row-score evidence reference differs") from error
    if evidence.uri != prefix.rstrip("/") + "/artifacts/evidence.json":
        raise ValueError("residual row-score evidence URI differs")
    samples, metrics = read_residual_evidence(out / "evidence.json", evidence)
    if result["metrics"] != metrics:
        raise ValueError("residual row-score result aggregate differs")
    ids, vectors, membership = _read_inputs(root, inputs)
    artifacts = read_residual_codes(
        root, identities, ids, membership, source_sha, membership_sha,
        dimensions=inputs.layout.dimensions, seed=inputs.layout.seed,
    )
    reconstructed = construct_residual_codes(
        ids, vectors, membership, seed=inputs.layout.seed
    )
    if (
        reconstructed.source_ordinals != artifacts.source_ordinals
        or reconstructed.page_row_counts != artifacts.page_row_counts
        or not np.array_equal(reconstructed.first_books, artifacts.first_books)
        or not np.array_equal(reconstructed.residual_books, artifacts.residual_books)
        or not np.array_equal(reconstructed.codes, artifacts.codes)
    ):
        raise ValueError("residual row-score sealed construction differs")
    router = read_geometric_router_parquet(
        root / "tree.parquet", root / "pages.parquet", inputs.layout,
        membership, PRIOR_TREE, PRIOR_PAGES,
    )
    queries, truth = _read_queries_truth(root, inputs)
    retained = tuple(
        _independent_route(router, query, leaf_frontier=128, limits=inputs.limits)[1]
        for query in queries
    )
    replay = validate_residual_samples(
        samples, metrics, queries, truth, retained, artifacts, ids, vectors,
        tuple(page.encoded_page_bytes for page in router.pages), inputs.limits,
    )
    validation: dict[str, object] = {
        "schema": "borsuk-residual-row-score-validation-v1",
        "claim_eligible": False,
        "source_commit": source_commit,
        "decision": replay["decision"],
        "metrics": replay,
        "source_archive": dataclasses.asdict(source_archive) if source_archive else None,
        "requirements_sha256": requirements_sha256,
    }
    (root / "validation.json").write_bytes(_canonical(validation))
    return validation


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("construct", "evaluate", "validate"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--source-commit")
    parser.add_argument("--source-archive-uri")
    parser.add_argument("--source-archive-sha256")
    parser.add_argument("--source-archive-bytes", type=int)
    parser.add_argument("--requirements-sha256")
    args = parser.parse_args()
    archive_values = (
        args.source_archive_uri,
        args.source_archive_sha256,
        args.source_archive_bytes,
        args.requirements_sha256,
    )
    if any(value is None for value in archive_values) and any(
        value is not None for value in archive_values
    ):
        parser.error("complete source archive authority is required")
    source_archive = (
        SourceArchiveIdentity(*archive_values[:3]) if archive_values[0] else None
    )
    if args.phase == "construct":
        run_construct_phase(args.root, args.output_prefix)
    elif args.phase == "evaluate":
        if args.out is None:
            parser.error("--out is required for evaluate")
        run_evaluate_phase(
            args.root, args.out, args.output_prefix,
            source_archive=source_archive,
            requirements_sha256=args.requirements_sha256,
        )
    else:
        if args.out is None or args.source_commit is None:
            parser.error("--out and --source-commit are required for validate")
        run_validate_phase(
            args.root, args.out, args.output_prefix, args.source_commit,
            source_archive=source_archive,
            requirements_sha256=args.requirements_sha256,
        )


if __name__ == "__main__":
    main()
