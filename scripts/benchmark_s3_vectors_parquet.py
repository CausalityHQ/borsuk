#!/usr/bin/env python3
"""Run one matched Amazon S3 Vectors comparison from authenticated Parquet.

The benchmark streams the source corpus in bounded batches, preserves feature
row identifiers as service keys, and emits cross-language per-query evidence.
S3 Vectors' internal cache state remains vendor-managed and opaque; the two
passes are therefore named by observable execution order only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True, slots=True)
class ObjectIdentity:
    role: str
    uri: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        if (
            not self.role
            or not self.uri.startswith("s3://")
            or len(self.sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.sha256)
            or type(self.bytes) is not int
            or self.bytes <= 0
        ):
            raise ValueError("object identity differs")


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    source: Path
    queries: Path
    truth: Path
    output_dir: Path
    inputs: Mapping[str, ObjectIdentity]
    vector_bucket: str
    index_name: str
    dimensions: int
    source_rows: int
    query_count: int
    neighbors: int
    metric: str
    upload_workers: int
    query_seed: int
    source_commit: str
    settle_seconds: float = 0.0

    def __post_init__(self) -> None:
        if (
            set(self.inputs) != {"source", "queries", "truth"}
            or any(identity.role != role for role, identity in self.inputs.items())
            or not self.vector_bucket
            or not self.index_name
            or not 1 <= self.dimensions <= 4_096
            or self.source_rows <= 0
            or self.query_count <= 0
            or self.neighbors != 100
            or self.metric not in {"euclidean", "cosine"}
            or not 1 <= self.upload_workers <= 16
            or len(self.source_commit) != 40
            or any(
                character not in "0123456789abcdef" for character in self.source_commit
            )
            or not math.isfinite(self.settle_seconds)
            or self.settle_seconds < 0
        ):
            raise ValueError("benchmark configuration differs")


@dataclass(frozen=True, slots=True)
class QuerySample:
    pass_label: str
    query_position: int
    query_ordinal: int
    latency_ns: int
    recall10_ppm: int
    recall100_ppm: int
    pages: int
    response_bytes: int
    returned_feature_row_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class PassAggregate:
    label: str
    queries: int
    average_recall10_ppm: int
    average_recall100_ppm: int
    p05_recall100_ppm: int
    worst_recall100_ppm: int
    latency_p50_ns: int
    latency_p95_ns: int
    latency_p99_ns: int
    response_bytes: int


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    schema: str
    source_commit: str
    inputs: Mapping[str, ObjectIdentity]
    vector_bucket: str
    index_name: str
    vectors: int
    dimensions: int
    metric: str
    top_k: int
    upload_seconds: float
    upload_vectors_per_second: float
    put_requests: int
    upload_logical_bytes: int
    passes: tuple[PassAggregate, ...]
    samples_sha256: str
    samples_bytes: int
    service_cache_state: str
    cleanup_completed: bool


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _authenticate_inputs(config: BenchmarkConfig) -> None:
    for role, path in (
        ("source", config.source),
        ("queries", config.queries),
        ("truth", config.truth),
    ):
        identity = config.inputs[role]
        try:
            observed_bytes = path.stat().st_size
        except FileNotFoundError as error:
            raise ValueError(f"{role} identity differs") from error
        if observed_bytes != identity.bytes or _sha256(path) != identity.sha256:
            raise ValueError(f"{role} identity differs")


def _embedding_field(dimensions: int) -> pa.Field:
    return pa.field(
        "embedding",
        pa.list_(pa.field("item", pa.float32(), nullable=False), dimensions),
        nullable=False,
    )


def _source_schema(dimensions: int) -> pa.Schema:
    return pa.schema(
        [
            pa.field("feature_row_id", pa.uint64(), nullable=False),
            _embedding_field(dimensions),
        ]
    )


def _query_schema(dimensions: int) -> pa.Schema:
    return pa.schema(
        [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field("feature_row_id", pa.uint64(), nullable=False),
            _embedding_field(dimensions),
        ]
    )


def _truth_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field("rank", pa.uint16(), nullable=False),
            pa.field("feature_row_id", pa.uint64(), nullable=False),
            pa.field("squared_distance", pa.float64(), nullable=False),
        ]
    )


def _vectors_from_list_array(array: pa.Array, dimensions: int) -> list[list[float]]:
    if not isinstance(array, pa.FixedSizeListArray) or array.null_count:
        raise ValueError("embedding values differ")
    values = array.values
    if values.null_count:
        raise ValueError("embedding values differ")
    rows = values.to_numpy(zero_copy_only=False).reshape(-1, dimensions)
    if not math.isfinite(float(rows.min())) or not math.isfinite(float(rows.max())):
        raise ValueError("embedding values differ")
    return rows.tolist()


def _source_batches(
    config: BenchmarkConfig,
) -> Iterator[tuple[list[dict[str, object]], int]]:
    parquet = pq.ParquetFile(config.source)
    if parquet.schema_arrow != _source_schema(config.dimensions):
        raise ValueError("source schema differs")
    observed_rows = 0
    for record_batch in parquet.iter_batches(batch_size=500):
        ids = record_batch.column(0).to_pylist()
        vectors = _vectors_from_list_array(record_batch.column(1), config.dimensions)
        if len(ids) != len(vectors) or len(ids) > 500:
            raise ValueError("source batch differs")
        batch = [
            {"key": str(row_id), "data": {"float32": vector}}
            for row_id, vector in zip(ids, vectors, strict=True)
        ]
        observed_rows += len(batch)
        logical_bytes = sum(
            config.dimensions * 4 + len(str(vector["key"]).encode())
            for vector in batch
        )
        yield batch, logical_bytes
    if observed_rows != config.source_rows:
        raise ValueError("source row count differs")


def _read_queries_and_truth(
    config: BenchmarkConfig,
) -> tuple[list[list[float]], list[list[str]]]:
    query_table = pq.read_table(config.queries)
    if (
        query_table.schema != _query_schema(config.dimensions)
        or query_table.num_rows != config.query_count
    ):
        raise ValueError("query schema differs")
    ordinals = query_table.column("query_ordinal").combine_chunks().to_pylist()
    if ordinals != list(range(config.query_count)):
        raise ValueError("query ordinals differ")
    queries = _vectors_from_list_array(
        query_table.column("embedding").combine_chunks(), config.dimensions
    )

    truth_table = pq.read_table(config.truth)
    if (
        truth_table.schema != _truth_schema()
        or truth_table.num_rows != config.query_count * config.neighbors
    ):
        raise ValueError("truth schema differs")
    truth_ordinals = truth_table.column("query_ordinal").combine_chunks().to_pylist()
    ranks = truth_table.column("rank").combine_chunks().to_pylist()
    ids = truth_table.column("feature_row_id").combine_chunks().to_pylist()
    distances = truth_table.column("squared_distance").combine_chunks().to_pylist()
    truth: list[list[str]] = []
    for query_ordinal in range(config.query_count):
        start = query_ordinal * config.neighbors
        end = start + config.neighbors
        if (
            truth_ordinals[start:end] != [query_ordinal] * config.neighbors
            or ranks[start:end] != list(range(config.neighbors))
            or any(not math.isfinite(value) or value < 0 for value in distances[start:end])
            or any(
                right < left
                for left, right in zip(
                    distances[start : end - 1], distances[start + 1 : end], strict=True
                )
            )
            or len(set(ids[start:end])) != config.neighbors
        ):
            raise ValueError("truth authority differs")
        truth.append([str(value) for value in ids[start:end]])
    return queries, truth


def _permutation(count: int, seed: int) -> list[int]:
    positions = list(range(count))
    mask = (1 << 64) - 1
    state = (seed ^ 0x9E3779B97F4A7C15) & mask
    for upper in range(count - 1, 0, -1):
        state = (state + 0x9E3779B97F4A7C15) & mask
        mixed = state
        mixed = ((mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9) & mask
        mixed = ((mixed ^ (mixed >> 27)) * 0x94D049BB133111EB) & mask
        mixed ^= mixed >> 31
        other = mixed % (upper + 1)
        positions[upper], positions[other] = positions[other], positions[upper]
    return positions


def _response_bytes(response: Mapping[str, object]) -> int:
    metadata = response.get("ResponseMetadata")
    if not isinstance(metadata, Mapping):
        return 0
    headers = metadata.get("HTTPHeaders")
    if not isinstance(headers, Mapping):
        return 0
    value = headers.get("content-length")
    try:
        return max(0, int(str(value)))
    except (TypeError, ValueError):
        return 0


def _query_one(
    client: object,
    config: BenchmarkConfig,
    query: list[float],
    truth: list[str],
    *,
    label: str,
    query_position: int,
    query_ordinal: int,
) -> QuerySample:
    started = time.perf_counter_ns()
    response = client.query_vectors(
        vectorBucketName=config.vector_bucket,
        indexName=config.index_name,
        queryVector={"float32": query},
        topK=config.neighbors,
        returnDistance=True,
    )
    vectors = response.get("vectors")
    if not isinstance(vectors, list) or response.get("nextToken") is not None:
        raise ValueError("query response differs")
    keys: list[str] = []
    for vector in vectors:
        if not isinstance(vector, Mapping) or type(vector.get("key")) is not str:
            raise ValueError("query response differs")
        keys.append(vector["key"])
    latency_ns = time.perf_counter_ns() - started
    if len(keys) != config.neighbors or len(set(keys)) != config.neighbors:
        raise ValueError("query result count differs")
    try:
        returned_ids = tuple(int(key) for key in keys)
    except ValueError as error:
        raise ValueError("query feature identifiers differ") from error
    if any(value < 0 or value > (1 << 64) - 1 for value in returned_ids) or any(
        str(value) != key for value, key in zip(returned_ids, keys, strict=True)
    ):
        raise ValueError("query feature identifiers differ")
    recall10 = len(set(keys[:10]).intersection(truth[:10])) * 100_000
    recall100 = len(set(keys).intersection(truth)) * 10_000
    return QuerySample(
        pass_label=label,
        query_position=query_position,
        query_ordinal=query_ordinal,
        latency_ns=latency_ns,
        recall10_ppm=recall10,
        recall100_ppm=recall100,
        pages=1,
        response_bytes=_response_bytes(response),
        returned_feature_row_ids=returned_ids,
    )


def _nearest_rank(values: list[int], fraction: float) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def _aggregate(label: str, samples: list[QuerySample]) -> PassAggregate:
    recall10 = [sample.recall10_ppm for sample in samples]
    recall100 = [sample.recall100_ppm for sample in samples]
    latencies = [sample.latency_ns for sample in samples]
    return PassAggregate(
        label=label,
        queries=len(samples),
        average_recall10_ppm=sum(recall10) // len(recall10),
        average_recall100_ppm=sum(recall100) // len(recall100),
        p05_recall100_ppm=_nearest_rank(recall100, 0.05),
        worst_recall100_ppm=min(recall100),
        latency_p50_ns=_nearest_rank(latencies, 0.50),
        latency_p95_ns=_nearest_rank(latencies, 0.95),
        latency_p99_ns=_nearest_rank(latencies, 0.99),
        response_bytes=sum(sample.response_bytes for sample in samples),
    )


def _write_samples(path: Path, samples: list[QuerySample]) -> None:
    table = pa.Table.from_pylist(
        [asdict(sample) for sample in samples],
        schema=pa.schema(
            [
                pa.field("pass_label", pa.string(), nullable=False),
                pa.field("query_position", pa.uint32(), nullable=False),
                pa.field("query_ordinal", pa.uint32(), nullable=False),
                pa.field("latency_ns", pa.uint64(), nullable=False),
                pa.field("recall10_ppm", pa.uint32(), nullable=False),
                pa.field("recall100_ppm", pa.uint32(), nullable=False),
                pa.field("pages", pa.uint16(), nullable=False),
                pa.field("response_bytes", pa.uint64(), nullable=False),
                pa.field(
                    "returned_feature_row_ids",
                    pa.list_(pa.field("item", pa.uint64(), nullable=False), 100),
                    nullable=False,
                ),
            ]
        ),
    )
    pq.write_table(table, path, compression="zstd")


def _wait_for_index(client: object, config: BenchmarkConfig) -> None:
    deadline = time.monotonic() + 300
    while True:
        try:
            client.get_index(
                vectorBucketName=config.vector_bucket,
                indexName=config.index_name,
            )
            return
        except client.exceptions.NotFoundException as error:
            if time.monotonic() >= deadline:
                raise TimeoutError("S3 Vectors index was not ready") from error
            time.sleep(2)


def _service_error_code(error: Exception) -> str:
    return str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))


def delete_service_resources(client: object, bucket: str, index: str) -> None:
    """Attempt both service deletions and report the first real failure."""

    first_error: Exception | None = None
    try:
        client.delete_index(vectorBucketName=bucket, indexName=index)
    except Exception as error:
        if _service_error_code(error) not in {"404", "NotFound", "NotFoundException"}:
            first_error = error
    deadline = time.monotonic() + 300
    while True:
        try:
            client.delete_vector_bucket(vectorBucketName=bucket)
            break
        except Exception as error:
            code = _service_error_code(error)
            if code in {"404", "NotFound", "NotFoundException"}:
                break
            if code in {"Conflict", "ConflictException"} and time.monotonic() < deadline:
                time.sleep(2)
                continue
            if first_error is None:
                first_error = error
            break
    if first_error is not None:
        raise first_error


def run_matched_benchmark(config: BenchmarkConfig, client: object) -> BenchmarkResult:
    """Execute one authenticated, bounded-memory matched S3 Vectors run."""

    _authenticate_inputs(config)
    queries, truth = _read_queries_and_truth(config)
    config.output_dir.mkdir(parents=True, exist_ok=False)
    created_bucket = False
    created_index = False
    samples: list[QuerySample] = []
    upload_seconds = 0.0
    put_requests = 0
    upload_logical_bytes = 0
    try:
        client.create_vector_bucket(vectorBucketName=config.vector_bucket)
        created_bucket = True
        client.create_index(
            vectorBucketName=config.vector_bucket,
            indexName=config.index_name,
            dataType="float32",
            dimension=config.dimensions,
            distanceMetric=config.metric,
        )
        created_index = True
        _wait_for_index(client, config)
        upload_started = time.monotonic()
        with ThreadPoolExecutor(max_workers=config.upload_workers) as executor:
            futures = []
            for batch, logical_bytes in _source_batches(config):
                futures.append(
                    executor.submit(
                        client.put_vectors,
                        vectorBucketName=config.vector_bucket,
                        indexName=config.index_name,
                        vectors=batch,
                    )
                )
                put_requests += 1
                upload_logical_bytes += logical_bytes
                if len(futures) == config.upload_workers:
                    for future in futures:
                        future.result()
                    futures.clear()
            for future in futures:
                future.result()
        upload_seconds = time.monotonic() - upload_started
        if config.settle_seconds:
            time.sleep(config.settle_seconds)
        positions = _permutation(config.query_count, config.query_seed)
        for label in ("fresh_index_first_pass", "immediate_repeated_pass"):
            for query_position, query_ordinal in enumerate(positions):
                samples.append(
                    _query_one(
                        client,
                        config,
                        queries[query_ordinal],
                        truth[query_ordinal],
                        label=label,
                        query_position=query_position,
                        query_ordinal=query_ordinal,
                    )
                )
    finally:
        if created_index or created_bucket:
            delete_service_resources(
                client,
                config.vector_bucket,
                config.index_name,
            )

    samples_path = config.output_dir / "samples.parquet"
    _write_samples(samples_path, samples)
    aggregates = tuple(
        _aggregate(label, [sample for sample in samples if sample.pass_label == label])
        for label in ("fresh_index_first_pass", "immediate_repeated_pass")
    )
    result = BenchmarkResult(
        schema="borsuk-matched-s3-vectors-relaion-1m-v1",
        source_commit=config.source_commit,
        inputs=dict(sorted(config.inputs.items())),
        vector_bucket=config.vector_bucket,
        index_name=config.index_name,
        vectors=config.source_rows,
        dimensions=config.dimensions,
        metric=config.metric,
        top_k=config.neighbors,
        upload_seconds=upload_seconds,
        upload_vectors_per_second=config.source_rows / upload_seconds,
        put_requests=put_requests,
        upload_logical_bytes=upload_logical_bytes,
        passes=aggregates,
        samples_sha256=_sha256(samples_path),
        samples_bytes=samples_path.stat().st_size,
        service_cache_state="vendor-managed-opaque",
        cleanup_completed=True,
    )
    (config.output_dir / "result.json").write_bytes(
        json.dumps(
            asdict(result), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
    return result


def _identity_argument(parser: argparse.ArgumentParser, role: str) -> None:
    parser.add_argument(f"--{role}", type=Path, required=True)
    parser.add_argument(f"--{role}-uri", required=True)
    parser.add_argument(f"--{role}-sha256", required=True)
    parser.add_argument(f"--{role}-bytes", type=int, required=True)


def parse_args(argv: Sequence[str] | None = None) -> tuple[BenchmarkConfig, str]:
    parser = argparse.ArgumentParser()
    for role in ("source", "queries", "truth"):
        _identity_argument(parser, role)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--vector-bucket", required=True)
    parser.add_argument("--index-name", default="vectors")
    parser.add_argument("--region", default="eu-central-1")
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument("--source-rows", type=int, default=1_000_000)
    parser.add_argument("--query-count", type=int, default=1_000)
    parser.add_argument("--neighbors", type=int, default=100)
    parser.add_argument("--metric", default="euclidean")
    parser.add_argument("--upload-workers", type=int, default=5)
    parser.add_argument("--query-seed", type=int, default=20260921)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--settle-seconds", type=float, default=60.0)
    args = parser.parse_args(argv)
    paths = {role: getattr(args, role) for role in ("source", "queries", "truth")}
    identities = {
        role: ObjectIdentity(
            role=role,
            uri=getattr(args, f"{role}_uri"),
            sha256=getattr(args, f"{role}_sha256"),
            bytes=getattr(args, f"{role}_bytes"),
        )
        for role in paths
    }
    return (
        BenchmarkConfig(
            source=paths["source"],
            queries=paths["queries"],
            truth=paths["truth"],
            output_dir=args.output_dir,
            inputs=identities,
            vector_bucket=args.vector_bucket,
            index_name=args.index_name,
            dimensions=args.dimensions,
            source_rows=args.source_rows,
            query_count=args.query_count,
            neighbors=args.neighbors,
            metric=args.metric,
            upload_workers=args.upload_workers,
            query_seed=args.query_seed,
            source_commit=args.source_commit,
            settle_seconds=args.settle_seconds,
        ),
        args.region,
    )


def main(argv: Sequence[str] | None = None) -> int:
    import boto3
    from botocore.config import Config

    config, region = parse_args(argv)
    client = boto3.client(
        "s3vectors",
        region_name=region,
        config=Config(retries={"mode": "adaptive", "max_attempts": 10}),
    )
    result = run_matched_benchmark(config, client)
    print(
        json.dumps(
            {
                "result": str(config.output_dir / "result.json"),
                "samples_sha256": result.samples_sha256,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
