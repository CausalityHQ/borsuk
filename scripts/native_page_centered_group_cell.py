#!/usr/bin/env python3
"""Phase-separated source seal and 100k grouped row-code decision cell."""

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
    MembershipRow,
    read_geometric_router_parquet,
    route_geometric_query,
)
from scripts.native_page_centered_group_codes import (
    GroupCodeIdentities,
    GroupCodes,
    construct_group_codes,
    read_group_codes,
    write_group_codes,
)
from scripts.native_page_centered_group_evaluation import (
    GroupSample,
    aggregate_group_samples,
    evaluate_group_query,
)
from scripts.native_page_centered_group_evidence import (
    read_group_evidence,
    write_group_evidence,
)
from scripts.native_page_microcluster_cell import (
    FROZEN_INPUTS,
    CellInputs,
    _read_inputs,
    _read_queries_truth,
)
from scripts.native_residual_row_score_evidence import read_residual_evidence
from scripts.native_row_score_s3 import S3CodeRangeReader
from scripts.validate_native_geometric_layout_result import _independent_route
from scripts.validate_native_page_centered_group_result import validate_group_samples

SEAL_SCHEMA = "borsuk-page-centered-group-cell-seal-v1"
RESULT_SCHEMA = "borsuk-page-centered-group-cell-result-v1"
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
PRIOR_EVIDENCE = ArtifactIdentity(
    "residual-row-score-evidence",
    "s3://borsuk-bench-453182569524-euc1/research/native-residual-row-score/"
    "8fc9c497a85dbdbfc016346e8fc1cd3100a089a6/"
    "runs/relaion-100k-dev1000-a0001/artifacts/evidence.json",
    "d91c726afb584826c24a3fd1c0d804cd8d71d95c4967cf16f3cdceb924c53f1f",
    1_903_426,
)


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _remote(identity: ArtifactIdentity, prefix: str, name: str) -> ArtifactIdentity:
    return dataclasses.replace(identity, uri=prefix.rstrip("/") + "/artifacts/" + name)


def construct_cell(
    root: Path,
    prefix: str,
    ids: Sequence[bytes],
    vectors: np.ndarray,
    membership: Sequence[MembershipRow],
    source_sha: bytes,
    membership_sha: bytes,
    tree_sha: bytes,
    *,
    seed: int,
    iterations: int = 10,
) -> GroupCodeIdentities:
    """Construct and bind source-only group codes before query capability."""
    if (
        not prefix.startswith("s3://")
        or any(len(value) != 32 for value in (source_sha, membership_sha, tree_sha))
        or any(row.source_sha256 != source_sha for row in membership)
    ):
        raise ValueError("group source construction authority differs")
    artifacts = construct_group_codes(
        ids, vectors, membership, seed=seed, iterations=iterations
    )
    local = write_group_codes(root, artifacts, source_sha, membership_sha, tree_sha)
    identities = GroupCodeIdentities(
        _remote(local.books, prefix, "books.bin"),
        _remote(local.groups, prefix, "groups.bin"),
        _remote(local.seal, prefix, "seal.json"),
    )
    sealed = {
        "schema": SEAL_SCHEMA,
        "source_sha256": source_sha.hex(),
        "membership_sha256": membership_sha.hex(),
        "tree_sha256": tree_sha.hex(),
        "books": dataclasses.asdict(identities.books),
        "groups": dataclasses.asdict(identities.groups),
        "code_seal": dataclasses.asdict(identities.seal),
    }
    (root / "sealed.json").write_bytes(_canonical(sealed))
    return identities


def read_cell_seal(
    root: Path,
    prefix: str,
    source_sha: bytes,
    membership_sha: bytes,
    tree_sha: bytes,
) -> GroupCodeIdentities:
    body = (root / "sealed.json").read_bytes()
    try:
        value = json.loads(body)
        identities = GroupCodeIdentities(
            ArtifactIdentity(**value["books"]),
            ArtifactIdentity(**value["groups"]),
            ArtifactIdentity(**value["code_seal"]),
        )
    except (TypeError, ValueError, KeyError, json.JSONDecodeError) as error:
        raise ValueError("group cell seal differs") from error
    expected = {
        "schema": SEAL_SCHEMA,
        "source_sha256": source_sha.hex(),
        "membership_sha256": membership_sha.hex(),
        "tree_sha256": tree_sha.hex(),
        "books": dataclasses.asdict(identities.books),
        "groups": dataclasses.asdict(identities.groups),
        "code_seal": dataclasses.asdict(identities.seal),
    }
    if (
        value != expected
        or body != _canonical(expected)
        or identities.books.uri != prefix.rstrip("/") + "/artifacts/books.bin"
        or identities.groups.uri != prefix.rstrip("/") + "/artifacts/groups.bin"
        or identities.seal.uri != prefix.rstrip("/") + "/artifacts/seal.json"
    ):
        raise ValueError("group cell seal authority differs")
    return identities


def _load_prior_hits(root: Path) -> tuple[tuple[int, int], ...]:
    samples, metrics = read_residual_evidence(root / "prior-evidence.json", PRIOR_EVIDENCE)
    if (
        metrics["query_count"] != len(samples)
        or len(samples) != 1000
        or any(sample.query_ordinal != index for index, sample in enumerate(samples))
    ):
        raise ValueError("group prior evidence count differs")
    return tuple(
        (sample.baseline.pq_hits_at_100, sample.residual_hits_at_100)
        for sample in samples
    )


def evaluate_routes(
    *,
    queries: np.ndarray,
    truth: Sequence[Sequence[bytes]],
    retained_by_query: Sequence[Sequence[int]],
    prior_hits: Sequence[tuple[int, int]],
    artifacts: GroupCodes,
    ids: Sequence[bytes],
    vectors: np.ndarray,
    page_byte_sizes: Sequence[int],
    limits,
    read_group_range: Callable[[int, int], bytes],
) -> tuple[tuple[GroupSample, ...], dict[str, int | str]]:
    if (
        len(queries) != len(truth)
        or len(queries) != len(retained_by_query)
        or len(queries) != len(prior_hits)
    ):
        raise ValueError("group paired query count differs")
    offsets = np.concatenate(([0], np.cumsum(artifacts.page_row_counts, dtype=np.int64)))
    owners = {
        ids[artifacts.source_ordinals[position]]: page
        for page in range(len(artifacts.page_row_counts))
        for position in range(int(offsets[page]), int(offsets[page + 1]))
    }
    samples = tuple(
        evaluate_group_query(
            query_ordinal=ordinal,
            query=queries[ordinal],
            retained_pages=retained_by_query[ordinal],
            artifacts=artifacts,
            stable_ids=ids,
            vectors=vectors,
            truth_ids=truth[ordinal],
            page_byte_sizes=page_byte_sizes,
            limits=limits,
            read_group_range=read_group_range,
            prior_pq_hits_at_100=prior_hits[ordinal][0],
            prior_residual_hits_at_100=prior_hits[ordinal][1],
            owner_by_id=owners,
        )
        for ordinal in range(len(queries))
    )
    return samples, aggregate_group_samples(samples)


def run_construct_phase(
    root: Path,
    prefix: str,
    inputs: CellInputs = FROZEN_INPUTS,
    *,
    iterations: int = 10,
) -> GroupCodeIdentities:
    ids, vectors, membership = _read_inputs(root, inputs)
    return construct_cell(
        root,
        prefix,
        ids,
        vectors,
        membership,
        bytes.fromhex(inputs.layout.source.sha256),
        bytes.fromhex(inputs.membership.sha256),
        bytes.fromhex(PRIOR_TREE.sha256),
        seed=inputs.layout.seed,
        iterations=iterations,
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
    ids, vectors, membership = _read_inputs(root, inputs)
    source_sha = bytes.fromhex(inputs.layout.source.sha256)
    member_sha = bytes.fromhex(inputs.membership.sha256)
    tree_sha = bytes.fromhex(PRIOR_TREE.sha256)
    identities = read_cell_seal(root, prefix, source_sha, member_sha, tree_sha)
    artifacts = read_group_codes(
        root, identities, ids, membership, source_sha, member_sha, tree_sha,
        dimensions=inputs.layout.dimensions, seed=inputs.layout.seed,
    )
    router = read_geometric_router_parquet(
        root / "tree.parquet", root / "pages.parquet", inputs.layout,
        membership, PRIOR_TREE, PRIOR_PAGES,
    )
    queries, truth = _read_queries_truth(root, inputs)
    prior_hits = _load_prior_hits(root)
    if len(prior_hits) != len(queries):
        raise ValueError("group prior evidence query count differs")
    if code_reader is None:
        import boto3
        from botocore.config import Config

        code_reader = S3CodeRangeReader(
            boto3.client(
                "s3",
                region_name="eu-central-1",
                config=Config(retries={"mode": "standard", "total_max_attempts": 1}),
            ),
            identities.groups.uri,
            object_bytes=identities.groups.encoded_bytes,
        )
    retained = tuple(
        route_geometric_query(router, query, leaf_frontier=128, limits=inputs.limits).retained_leaf_pages
        for query in queries
    )
    samples, metrics = evaluate_routes(
        queries=queries,
        truth=truth,
        retained_by_query=retained,
        prior_hits=prior_hits,
        artifacts=artifacts,
        ids=ids,
        vectors=vectors,
        page_byte_sizes=tuple(page.encoded_page_bytes for page in router.pages),
        limits=inputs.limits,
        read_group_range=code_reader,
    )
    out.mkdir(parents=True, exist_ok=True)
    evidence = _remote(write_group_evidence(out / "evidence.json", samples), prefix, "evidence.json")
    result: dict[str, object] = {
        "schema": RESULT_SCHEMA,
        "claim_eligible": False,
        "source": dataclasses.asdict(inputs.layout.source),
        "membership": dataclasses.asdict(inputs.membership),
        "tree": dataclasses.asdict(PRIOR_TREE),
        "pages": dataclasses.asdict(PRIOR_PAGES),
        "prior_evidence": dataclasses.asdict(PRIOR_EVIDENCE),
        "books": dataclasses.asdict(identities.books),
        "groups": dataclasses.asdict(identities.groups),
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
    if len(source_commit) != 40 or any(character not in "0123456789abcdef" for character in source_commit):
        raise ValueError("group source revision differs")
    body = (out / "result.json").read_bytes()
    try:
        result = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("group result differs") from error
    source_sha = bytes.fromhex(inputs.layout.source.sha256)
    member_sha = bytes.fromhex(inputs.membership.sha256)
    tree_sha = bytes.fromhex(PRIOR_TREE.sha256)
    identities = read_cell_seal(root, prefix, source_sha, member_sha, tree_sha)
    expected = {
        "schema": RESULT_SCHEMA,
        "claim_eligible": False,
        "source": dataclasses.asdict(inputs.layout.source),
        "membership": dataclasses.asdict(inputs.membership),
        "tree": dataclasses.asdict(PRIOR_TREE),
        "pages": dataclasses.asdict(PRIOR_PAGES),
        "prior_evidence": dataclasses.asdict(PRIOR_EVIDENCE),
        "books": dataclasses.asdict(identities.books),
        "groups": dataclasses.asdict(identities.groups),
        "code_seal": dataclasses.asdict(identities.seal),
        "queries": dataclasses.asdict(inputs.queries),
        "truth": dataclasses.asdict(inputs.truth),
        "evidence": result.get("evidence"),
        "limits": dataclasses.asdict(inputs.limits),
        "metrics": result.get("metrics"),
        "output_prefix": prefix.rstrip("/"),
        "source_archive": dataclasses.asdict(source_archive) if source_archive else None,
        "requirements_sha256": requirements_sha256,
    }
    if type(result) is not dict or result != expected or body != _canonical(expected):
        raise ValueError("group result authority differs")
    try:
        evidence = ArtifactIdentity(**result["evidence"])
    except (TypeError, ValueError) as error:
        raise ValueError("group evidence reference differs") from error
    if evidence.uri != prefix.rstrip("/") + "/artifacts/evidence.json":
        raise ValueError("group evidence URI differs")
    samples, metrics = read_group_evidence(out / "evidence.json", evidence)
    if result["metrics"] != metrics:
        raise ValueError("group result aggregate differs")
    ids, vectors, membership = _read_inputs(root, inputs)
    artifacts = read_group_codes(
        root, identities, ids, membership, source_sha, member_sha, tree_sha,
        dimensions=inputs.layout.dimensions, seed=inputs.layout.seed,
    )
    reconstructed = construct_group_codes(ids, vectors, membership, seed=inputs.layout.seed)
    if (
        reconstructed.source_ordinals != artifacts.source_ordinals
        or reconstructed.page_row_counts != artifacts.page_row_counts
        or reconstructed.group_ranges != artifacts.group_ranges
        or not np.array_equal(reconstructed.books, artifacts.books)
        or not np.array_equal(reconstructed.page_means, artifacts.page_means)
        or not np.array_equal(reconstructed.codes, artifacts.codes)
    ):
        raise ValueError("group sealed construction differs")
    router = read_geometric_router_parquet(
        root / "tree.parquet", root / "pages.parquet", inputs.layout,
        membership, PRIOR_TREE, PRIOR_PAGES,
    )
    queries, truth = _read_queries_truth(root, inputs)
    prior_hits = _load_prior_hits(root)
    retained = tuple(
        _independent_route(router, query, leaf_frontier=128, limits=inputs.limits)[1]
        for query in queries
    )
    replay = validate_group_samples(
        samples,
        metrics,
        queries=queries,
        truth=truth,
        retained_routes=retained,
        ids=ids,
        vectors=vectors,
        membership=membership,
        artifacts=artifacts,
        group_bytes=(root / "groups.bin").read_bytes(),
        page_byte_sizes=tuple(page.encoded_page_bytes for page in router.pages),
        limits=inputs.limits,
        prior_hits=prior_hits,
    )
    validation: dict[str, object] = {
        "schema": "borsuk-page-centered-group-validation-v1",
        "claim_eligible": False,
        "source_commit": source_commit,
        "decision": replay["decision"],
        "metrics": replay["metrics"],
        "source_archive": dataclasses.asdict(source_archive) if source_archive else None,
        "requirements_sha256": requirements_sha256,
    }
    (root / "validation.json").write_bytes(_canonical(validation))
    return validation


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="phase", required=True)
    for phase in ("construct", "evaluate", "validate"):
        command = sub.add_parser(phase)
        command.add_argument("--root", type=Path, required=True)
        command.add_argument("--output-prefix", required=True)
        if phase != "construct":
            command.add_argument("--out", type=Path)
            command.add_argument("--source-archive-uri", required=True)
            command.add_argument("--source-archive-sha256", required=True)
            command.add_argument("--source-archive-bytes", type=int, required=True)
            command.add_argument("--requirements-sha256", required=True)
        if phase == "validate":
            command.add_argument("--source-commit", required=True)
    args = parser.parse_args()
    if args.phase == "construct":
        run_construct_phase(args.root, args.output_prefix)
        return
    if args.out is None:
        parser.error("--out is required")
    archive = SourceArchiveIdentity(
        args.source_archive_uri, args.source_archive_sha256, args.source_archive_bytes
    )
    if args.phase == "evaluate":
        run_evaluate_phase(
            args.root, args.out, args.output_prefix,
            source_archive=archive, requirements_sha256=args.requirements_sha256,
        )
    else:
        run_validate_phase(
            args.root, args.out, args.output_prefix, args.source_commit,
            source_archive=archive, requirements_sha256=args.requirements_sha256,
        )


if __name__ == "__main__":
    main()
