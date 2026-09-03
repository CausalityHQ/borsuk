#!/usr/bin/env python3
"""Exact fail-fast quality gate for a bounded V27 construction sample."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import numpy as np

QUERY_COUNT = 32
DIMENSIONS = 96
RECALL_K = 10
MAX_GETS = 10
MAX_ENCODED_BYTES = 4_587_520


@dataclass(frozen=True)
class V27SearchObservation:
    """One real V27 search result and its bounded S3 work."""

    source_ordinals: tuple[int, ...]
    get_count: int
    encoded_bytes: int
    decoded_rows: int
    unique_rows: int


@dataclass(frozen=True)
class V27Artifact:
    """One locally staged immutable artifact."""

    path: Path
    sha256: str
    encoded_bytes: int


@dataclass(frozen=True)
class V27QualifierPlan:
    """Explicit invocation authority for the real V27 S3 qualifier."""

    command: tuple[str, ...]
    roots: V27Artifact
    leaves: V27Artifact
    postings: V27Artifact
    modes: V27Artifact
    manifest: V27Artifact
    query: V27Artifact
    s3_page_prefix: str
    root_beam: int
    leaf_beam: int
    page_count: int


@dataclass(frozen=True)
class V27QualityPlan:
    """Complete bounded quality invocation."""

    train: V27Artifact
    query: V27Artifact
    qualifier: V27QualifierPlan


def _take(values: dict[str, str], name: str) -> str:
    try:
        return values.pop(name)
    except KeyError as error:
        raise ValueError(f"V27 quality missing --{name}") from error


def _positive(values: dict[str, str], name: str) -> int:
    try:
        value = int(_take(values, name))
    except ValueError as error:
        raise ValueError(f"V27 quality --{name} type differs") from error
    if value <= 0:
        raise ValueError(f"V27 quality --{name} bound differs")
    return value


def _parse_artifact(values: dict[str, str], role: str) -> V27Artifact:
    path_name = f"{role}-parquet" if role in {"train", "query"} else role
    artifact = V27Artifact(
        path=Path(_take(values, path_name)),
        sha256=_take(values, f"{role}-sha256"),
        encoded_bytes=_positive(values, f"{role}-bytes"),
    )
    if (
        not artifact.path.is_absolute()
        or len(artifact.sha256) != 64
        or any(character not in "0123456789abcdef" for character in artifact.sha256)
    ):
        raise ValueError(f"V27 quality --{role} authority differs")
    return artifact


def parse_args(arguments: list[str]) -> V27QualityPlan:
    """Parse an explicit bounded quality command with no discovery surface."""

    iterator = iter(arguments)
    if next(iterator, None) is None:
        raise ValueError("V27 quality program is missing")
    execute = False
    values: dict[str, str] = {}
    for flag in iterator:
        if flag == "--execute":
            if execute:
                raise ValueError("V27 quality duplicate --execute")
            execute = True
            continue
        if not flag.startswith("--"):
            raise ValueError("V27 quality flag differs")
        name = flag.removeprefix("--")
        value = next(iterator, None)
        if value is None:
            raise ValueError(f"V27 quality --{name} value is missing")
        if name in values:
            raise ValueError(f"V27 quality duplicate --{name}")
        values[name] = value
    if not execute:
        raise ValueError("V27 quality --execute is required")
    train = _parse_artifact(values, "train")
    query = _parse_artifact(values, "query")
    artifacts = {
        role: _parse_artifact(values, role)
        for role in ("roots", "leaves", "postings", "modes", "manifest")
    }
    qualifier = V27QualifierPlan(
        command=(_take(values, "qualifier-binary"),),
        roots=artifacts["roots"],
        leaves=artifacts["leaves"],
        postings=artifacts["postings"],
        modes=artifacts["modes"],
        manifest=artifacts["manifest"],
        query=query,
        s3_page_prefix=_take(values, "s3-page-prefix"),
        root_beam=_positive(values, "root-beam"),
        leaf_beam=_positive(values, "leaf-beam"),
        page_count=_positive(values, "page-count"),
    )
    if values:
        raise ValueError("V27 quality unknown flag")
    return V27QualityPlan(train=train, query=query, qualifier=qualifier)


def _artifact_args(role: str, artifact: V27Artifact) -> list[str]:
    path_flag = "query-parquet" if role == "query" else role
    return [
        f"--{path_flag}",
        str(artifact.path),
        f"--{role}-sha256",
        artifact.sha256,
        f"--{role}-bytes",
        str(artifact.encoded_bytes),
    ]


def run_v27_qualifier(plan: V27QualifierPlan, query_index: int) -> V27SearchObservation:
    """Run one query through the exact explicit S3 qualification binary."""

    if (
        type(plan.command) is not tuple
        or not plan.command
        or any(type(value) is not str or not value for value in plan.command)
        or type(query_index) is not int
        or not 0 <= query_index < QUERY_COUNT
        or not plan.s3_page_prefix.startswith("s3://")
        or plan.s3_page_prefix.endswith("/")
        or type(plan.root_beam) is not int
        or plan.root_beam <= 0
        or type(plan.leaf_beam) is not int
        or plan.leaf_beam <= 0
        or type(plan.page_count) is not int
        or not 1 <= plan.page_count <= MAX_GETS
    ):
        raise ValueError("V27 qualifier plan differs")
    command = [*plan.command, "--execute"]
    for role in ("roots", "leaves", "postings", "modes", "manifest", "query"):
        command.extend(_artifact_args(role, getattr(plan, role)))
    command.extend(
        [
            "--query-row",
            str(query_index),
            "--root-beam",
            str(plan.root_beam),
            "--leaf-beam",
            str(plan.leaf_beam),
            "--page-count",
            str(plan.page_count),
            "--k",
            str(RECALL_K),
            "--s3-page-prefix",
            plan.s3_page_prefix,
        ]
    )
    completed = subprocess.run(command, capture_output=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError("V27 S3 qualifier failed: " + completed.stderr.decode().strip())
    try:
        value = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V27 qualifier result JSON differs") from error
    canonical = (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )
    if completed.stdout != canonical or set(value) != {
        "claim_eligible",
        "matches",
        "schema_version",
        "work",
    }:
        raise ValueError("V27 qualifier result authority differs")
    matches = value["matches"]
    work = value["work"]
    if (
        value["claim_eligible"] is not False
        or value["schema_version"] != 1
        or type(matches) is not list
        or len(matches) != RECALL_K
        or type(work) is not dict
        or any(
            type(item) is not dict
            or set(item) != {"source_ordinal", "squared_distance"}
            or type(item["source_ordinal"]) is not int
            or type(item["squared_distance"]) not in {int, float}
            or not np.isfinite(item["squared_distance"])
            for item in matches
        )
    ):
        raise ValueError("V27 qualifier result schema differs")
    return V27SearchObservation(
        source_ordinals=tuple(item["source_ordinal"] for item in matches),
        get_count=work["get_count"],
        encoded_bytes=work["encoded_bytes"],
        decoded_rows=work["decoded_rows"],
        unique_rows=work["unique_rows"],
    )


def load_v27_vectors(
    path: Path,
    expected_sha256: str,
    expected_bytes: int,
    *,
    column: str,
    row_limit: int,
) -> np.ndarray:
    """Authenticate one Parquet object and materialize only its bounded row prefix."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    if (
        type(expected_sha256) is not str
        or len(expected_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_sha256)
        or type(expected_bytes) is not int
        or expected_bytes <= 0
        or type(row_limit) is not int
        or row_limit <= 0
        or column not in {"emb", "vector"}
    ):
        raise ValueError("V27 reduced Parquet authority differs")
    digest = hashlib.sha256()
    observed_bytes = 0
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
            observed_bytes += len(block)
    if observed_bytes != expected_bytes or digest.hexdigest() != expected_sha256:
        raise ValueError("V27 reduced Parquet byte authority differs")

    child = pa.field("element", pa.float32(), nullable=False)
    expected_schema = pa.schema(
        [pa.field(column, pa.list_(child, DIMENSIONS), nullable=False)]
    )
    parquet = pq.ParquetFile(path)
    if parquet.schema_arrow != expected_schema:
        raise ValueError("V27 reduced Parquet schema differs")
    chunks: list[np.ndarray] = []
    remaining = row_limit
    for batch in parquet.iter_batches(batch_size=min(8_192, remaining), columns=[column]):
        if batch.num_rows == 0 or batch.column(0).null_count != 0:
            raise ValueError("V27 reduced Parquet nullability differs")
        take = min(remaining, batch.num_rows)
        values = batch.column(0).values.to_numpy(zero_copy_only=False)
        chunks.append(np.asarray(values[: take * DIMENSIONS], dtype=np.float32).reshape(take, DIMENSIONS))
        remaining -= take
        if remaining == 0:
            break
    if remaining != 0:
        raise ValueError("V27 reduced Parquet row count differs")
    result = np.concatenate(chunks)
    if not np.isfinite(result).all():
        raise ValueError("V27 reduced Parquet value differs")
    return result


def _normalized(vectors: np.ndarray) -> np.ndarray:
    if (
        not isinstance(vectors, np.ndarray)
        or vectors.dtype != np.float32
        or vectors.ndim != 2
        or vectors.shape[1] != DIMENSIONS
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("V27 reduced vector authority differs")
    norms = np.linalg.norm(vectors.astype(np.float64), axis=1)
    if not np.isfinite(norms).all() or np.any(norms <= 0.0):
        raise ValueError("V27 reduced vector norm differs")
    return (vectors.astype(np.float64) / norms[:, None]).astype(np.float32)


def _exact_top_ten(train: np.ndarray, query: np.ndarray) -> tuple[int, ...]:
    delta = train - query
    distances = np.einsum("ij,ij->i", delta, delta, dtype=np.float32)
    ordinals = np.arange(train.shape[0], dtype=np.int64)
    return tuple(int(value) for value in np.lexsort((ordinals, distances))[:RECALL_K])


def evaluate_v27_reduced_quality(
    train_vectors: np.ndarray,
    query_vectors: np.ndarray,
    search: Callable[[int, np.ndarray], V27SearchObservation],
) -> bytes:
    """Compute independent subset truth and compare 32 real page searches."""

    train = _normalized(train_vectors)
    queries = _normalized(query_vectors)
    if train.shape[0] < RECALL_K or queries.shape[0] != QUERY_COUNT:
        raise ValueError("V27 reduced row count differs")

    samples: list[dict[str, object]] = []
    failed: list[int] = []
    total_hits = 0
    minimum_hits = RECALL_K
    maximum_gets = 0
    maximum_bytes = 0
    for query_index, query in enumerate(queries):
        truth = _exact_top_ten(train, query)
        observed = search(query_index, query)
        ids = observed.source_ordinals
        if (
            type(ids) is not tuple
            or len(ids) != RECALL_K
            or len(set(ids)) != RECALL_K
            or any(type(value) is not int or value < 0 or value >= train.shape[0] for value in ids)
            or type(observed.get_count) is not int
            or observed.get_count <= 0
            or type(observed.encoded_bytes) is not int
            or observed.encoded_bytes <= 0
            or type(observed.decoded_rows) is not int
            or observed.decoded_rows <= 0
            or type(observed.unique_rows) is not int
            or observed.unique_rows <= 0
            or observed.unique_rows > observed.decoded_rows
        ):
            raise ValueError("V27 reduced search evidence differs")
        hits = len(set(truth).intersection(ids))
        total_hits += hits
        minimum_hits = min(minimum_hits, hits)
        maximum_gets = max(maximum_gets, observed.get_count)
        maximum_bytes = max(maximum_bytes, observed.encoded_bytes)
        if hits != RECALL_K:
            failed.append(query_index)
        samples.append(
            {
                "encoded_bytes": observed.encoded_bytes,
                "get_count": observed.get_count,
                "hits": hits,
                "observed_source_ordinals": list(ids),
                "query_ordinal": query_index,
                "recall_ppm": hits * 100_000,
                "truth_source_ordinals": list(truth),
            }
        )

    aggregate = total_hits * 1_000_000 // (QUERY_COUNT * RECALL_K)
    minimum = minimum_hits * 100_000
    passed = (
        aggregate == 1_000_000
        and minimum == 1_000_000
        and maximum_gets <= MAX_GETS
        and maximum_bytes <= MAX_ENCODED_BYTES
    )
    value = {
        "aggregate_recall_ppm": aggregate,
        "claim_eligible": False,
        "failed_query_ordinals": failed,
        "maximum_encoded_bytes": maximum_bytes,
        "maximum_get_count": maximum_gets,
        "minimum_recall_ppm": minimum,
        "queries": QUERY_COUNT,
        "recall_k": RECALL_K,
        "samples": samples,
        "schema": "borsuk-v27-reduced-quality-v1",
        "status": "passed" if passed else "failed",
    }
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def execute_plan(plan: V27QualityPlan) -> bytes:
    """Load one bounded shard and run its queries against immutable S3 pages."""

    train = load_v27_vectors(
        plan.train.path,
        plan.train.sha256,
        plan.train.encoded_bytes,
        column="emb",
        row_limit=100_000,
    )
    queries = load_v27_vectors(
        plan.query.path,
        plan.query.sha256,
        plan.query.encoded_bytes,
        column="emb",
        row_limit=QUERY_COUNT,
    )
    return evaluate_v27_reduced_quality(
        train,
        queries,
        lambda query_index, _query: run_v27_qualifier(plan.qualifier, query_index),
    )


def main() -> int:
    try:
        receipt = execute_plan(parse_args(sys.argv))
        sys.stdout.buffer.write(receipt)
        return 0 if json.loads(receipt)["status"] == "passed" else 1
    except (OSError, RuntimeError, ValueError) as error:
        print(f"run_v27_reduced_quality: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
