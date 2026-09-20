#!/usr/bin/env python3
"""Replay frozen page plans through real S3 range GETs.

This module is the small serving boundary for the V95 fail-fast probe.  It
does not choose pages and it never receives the base corpus.  It reads only
the registered code and SQ8 page ranges, reranks the decoded SQ8 rows together
with the resident delta, and reports physical S3 work and stage timings.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import pathlib
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Protocol, Sequence

import numpy as np

_HEADER_BYTES = 64
_MAGIC = b"B95SQ8P\0"
_PQ_MAGIC = b"B95PQPG\0"
_VERSION = 1


def _digest(value: object, length: int) -> bool:
    return (
        type(value) is str
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def validate_replay_plan(plan: object, *, expected_queries: int) -> dict[str, Any]:
    """Validate the immutable handoff from offline planning to S3 replay."""

    if type(plan) is not dict or set(plan) != {
        "bucket",
        "code_object",
        "code_page_bytes",
        "data_page_bytes",
        "dimensions",
        "neighbors",
        "page_rows",
        "queries",
        "schema",
        "source_commit",
        "sq8_object",
    }:
        raise ValueError("V95 replay plan differs")
    code_object = plan["code_object"]
    sq8_object = plan["sq8_object"]
    if (
        plan["schema"] != "borsuk-v95-real-s3-plan-v1"
        or not _digest(plan["source_commit"], 40)
        or type(plan["bucket"]) is not str
        or not plan["bucket"]
        or type(plan["code_page_bytes"]) is not int
        or plan["code_page_bytes"] <= 0
        or type(plan["data_page_bytes"]) is not int
        or plan["data_page_bytes"] <= 0
        or type(plan["dimensions"]) is not int
        or plan["dimensions"] <= 0
        or type(plan["neighbors"]) is not int
        or plan["neighbors"] <= 0
        or type(plan["page_rows"]) is not int
        or plan["page_rows"] <= 0
        or type(expected_queries) is not int
        or expected_queries <= 0
    ):
        raise ValueError("V95 replay plan differs")
    for identity, page_bytes in (
        (code_object, plan["code_page_bytes"]),
        (sq8_object, plan["data_page_bytes"]),
    ):
        if (
            type(identity) is not dict
            or set(identity) != {"bytes", "key", "sha256"}
            or type(identity["bytes"]) is not int
            or identity["bytes"] <= 0
            or identity["bytes"] % page_bytes
            or type(identity["key"]) is not str
            or not identity["key"]
            or not _digest(identity["sha256"], 64)
        ):
            raise ValueError("V95 replay plan differs")
    queries = plan["queries"]
    if type(queries) is not list or len(queries) != expected_queries:
        raise ValueError("V95 replay plan differs")
    expected_ordinals = list(range(328, 328 + expected_queries))
    if [
        query.get("ordinal") for query in queries if type(query) is dict
    ] != expected_ordinals:
        raise ValueError("V95 replay plan differs")
    for query in queries:
        if type(query) is not dict or set(query) != {
            "code_ranges",
            "data_ranges",
            "expected_hits",
            "ordinal",
        }:
            raise ValueError("V95 replay plan differs")
        code_ranges = query["code_ranges"]
        data_ranges = query["data_ranges"]
        if (
            type(query["expected_hits"]) is not int
            or not 0 <= query["expected_hits"] <= plan["neighbors"]
            or type(code_ranges) is not list
            or type(data_ranges) is not list
            or not code_ranges
            or not data_ranges
        ):
            raise ValueError("V95 replay plan differs")
        try:
            _validate_ranges([tuple(pair) for pair in code_ranges])
            _validate_ranges([tuple(pair) for pair in data_ranges])
        except (TypeError, ValueError):
            raise ValueError("V95 replay plan differs") from None
        if (
            max(pair[1] for pair in code_ranges)
            >= code_object["bytes"] // plan["code_page_bytes"]
            or max(pair[1] for pair in data_ranges)
            >= sq8_object["bytes"] // plan["data_page_bytes"]
        ):
            raise ValueError("V95 replay plan differs")
    return plan


class S3RangeClient(Protocol):
    def get_object(self, *, Bucket: str, Key: str, Range: str) -> dict[str, Any]: ...


@dataclass(frozen=True)
class PqCodePageCodec:
    """Fixed-width PQ code page used by the frozen wave-one range plan."""

    subspaces: int
    page_rows: int

    def __post_init__(self) -> None:
        if self.subspaces <= 0 or self.page_rows <= 0:
            raise ValueError("V95 PQ page shape differs")

    @property
    def encoded_bytes(self) -> int:
        return _HEADER_BYTES + self.page_rows * self.subspaces

    def encode(self, codes: np.ndarray) -> bytes:
        codes = np.asarray(codes)
        rows = codes.shape[0] if codes.ndim == 2 else 0
        if (
            codes.dtype != np.uint8
            or codes.shape != (rows, self.subspaces)
            or not 0 < rows <= self.page_rows
        ):
            raise ValueError("V95 PQ page codes differ")
        body = bytearray(self.encoded_bytes)
        struct.pack_into(
            "<8sIIII40x",
            body,
            0,
            _PQ_MAGIC,
            _VERSION,
            rows,
            self.subspaces,
            self.page_rows,
        )
        body[_HEADER_BYTES : _HEADER_BYTES + codes.nbytes] = codes.tobytes()
        return bytes(body)

    def decode(self, payload: bytes) -> np.ndarray:
        if len(payload) != self.encoded_bytes:
            raise ValueError("V95 S3 range length differs")
        magic, version, rows, subspaces, page_rows = struct.unpack_from(
            "<8sIIII40x", payload, 0
        )
        if (
            magic != _PQ_MAGIC
            or version != _VERSION
            or subspaces != self.subspaces
            or page_rows != self.page_rows
            or not 0 < rows <= self.page_rows
        ):
            raise ValueError("V95 PQ page authority differs")
        return np.frombuffer(
            payload,
            dtype=np.uint8,
            count=rows * self.subspaces,
            offset=_HEADER_BYTES,
        ).reshape(rows, self.subspaces)


@dataclass(frozen=True)
class Sq8PageCodec:
    """Fixed-width, independently decodable SQ8 page representation."""

    dimensions: int
    page_rows: int

    def __post_init__(self) -> None:
        if self.dimensions <= 0 or self.page_rows <= 0:
            raise ValueError("V95 SQ8 page shape differs")

    @property
    def record_bytes(self) -> int:
        return 12 + self.dimensions

    @property
    def encoded_bytes(self) -> int:
        return (
            _HEADER_BYTES
            + 2 * self.dimensions * np.dtype("<f4").itemsize
            + self.page_rows * self.record_bytes
        )

    def encode(self, identifiers: np.ndarray, vectors: np.ndarray) -> bytes:
        identifiers = np.asarray(identifiers, dtype=np.int64)
        vectors = np.asarray(vectors, dtype=np.float32)
        rows = identifiers.size
        if (
            identifiers.shape != (rows,)
            or vectors.shape != (rows, self.dimensions)
            or not 0 < rows <= self.page_rows
            or np.unique(identifiers).size != rows
            or not np.isfinite(vectors).all()
        ):
            raise ValueError("V95 SQ8 page values differ")
        low = np.min(vectors, axis=0).astype("<f4")
        high = np.max(vectors, axis=0).astype("<f4")
        step = ((high - low) / np.float32(255.0)).astype("<f4")
        step[step == 0.0] = np.float32(1.0)
        codes = np.clip(
            np.rint((vectors - low[None, :]) / step[None, :]), 0, 255
        ).astype(np.uint8)
        decoded = low[None, :] + codes.astype(np.float32) * step[None, :]
        norms = np.einsum("ij,ij->i", decoded, decoded).astype("<f4")

        body = bytearray(self.encoded_bytes)
        struct.pack_into(
            "<8sIIII40x",
            body,
            0,
            _MAGIC,
            _VERSION,
            rows,
            self.dimensions,
            self.page_rows,
        )
        cursor = _HEADER_BYTES
        body[cursor : cursor + low.nbytes] = low.tobytes()
        cursor += low.nbytes
        body[cursor : cursor + step.nbytes] = step.tobytes()
        cursor += step.nbytes
        for row in range(rows):
            struct.pack_into("<qf", body, cursor, int(identifiers[row]), norms[row])
            cursor += 12
            body[cursor : cursor + self.dimensions] = codes[row].tobytes()
            cursor += self.dimensions
        return bytes(body)

    def decode(self, payload: bytes) -> tuple[np.ndarray, np.ndarray]:
        if len(payload) != self.encoded_bytes:
            raise ValueError("V95 S3 range length differs")
        magic, version, rows, dimensions, page_rows = struct.unpack_from(
            "<8sIIII40x", payload, 0
        )
        if (
            magic != _MAGIC
            or version != _VERSION
            or dimensions != self.dimensions
            or page_rows != self.page_rows
            or not 0 < rows <= self.page_rows
        ):
            raise ValueError("V95 SQ8 page authority differs")
        cursor = _HEADER_BYTES
        width = self.dimensions * np.dtype("<f4").itemsize
        low = np.frombuffer(payload, dtype="<f4", count=self.dimensions, offset=cursor)
        cursor += width
        step = np.frombuffer(payload, dtype="<f4", count=self.dimensions, offset=cursor)
        cursor += width
        if (
            not np.isfinite(low).all()
            or not np.isfinite(step).all()
            or np.any(step <= 0)
        ):
            raise ValueError("V95 SQ8 quantizer differs")
        records = np.frombuffer(
            payload,
            dtype=np.dtype(
                [
                    ("identifier", "<i8"),
                    ("norm", "<f4"),
                    ("codes", "u1", (self.dimensions,)),
                ],
                align=False,
            ),
            count=rows,
            offset=cursor,
        )
        identifiers = np.asarray(records["identifier"], dtype=np.int64).copy()
        vectors = low[None, :] + records["codes"].astype(np.float32) * step[None, :]
        computed_norms = np.einsum("ij,ij->i", vectors, vectors)
        if not np.isfinite(records["norm"]).all() or not np.allclose(
            records["norm"], computed_norms, rtol=2e-5, atol=2e-3
        ):
            raise ValueError("V95 SQ8 row norm differs")
        if np.unique(identifiers).size != rows:
            raise ValueError("V95 SQ8 identifiers differ")
        return identifiers, vectors


def _validate_ranges(ranges: Sequence[tuple[int, int]]) -> None:
    previous = -1
    for first, last in ranges:
        if first < 0 or last < first or first <= previous:
            raise ValueError("V95 registered ranges differ")
        previous = last


def _fetch_ranges(
    client: S3RangeClient,
    *,
    bucket: str,
    key: str,
    ranges: Sequence[tuple[int, int]],
    page_bytes: int,
    read_threads: int,
) -> tuple[list[bytes], int]:
    _validate_ranges(ranges)
    if not bucket or not key or page_bytes <= 0 or read_threads <= 0:
        raise ValueError("V95 S3 request authority differs")

    def fetch(bounds: tuple[int, int]) -> bytes:
        first, last = bounds
        start = first * page_bytes
        stop = (last + 1) * page_bytes
        payload = client.get_object(
            Bucket=bucket,
            Key=key,
            Range=f"bytes={start}-{stop - 1}",
        )["Body"].read()
        if len(payload) != stop - start:
            raise ValueError("V95 S3 range length differs")
        return payload

    with ThreadPoolExecutor(max_workers=min(read_threads, len(ranges))) as pool:
        payloads = list(pool.map(fetch, ranges))
    return payloads, sum(len(payload) for payload in payloads)


def warm_range_connections(
    client: S3RangeClient,
    *,
    bucket: str,
    code_key: str,
    sq8_key: str,
    code_page_bytes: int,
    data_page_bytes: int,
    read_threads: int,
) -> dict[str, int]:
    """Establish each bounded HTTP connection before the timed cohort."""

    ranges = tuple((index, index) for index in range(read_threads))
    _, code_bytes = _fetch_ranges(
        client,
        bucket=bucket,
        key=code_key,
        ranges=ranges,
        page_bytes=code_page_bytes,
        read_threads=read_threads,
    )
    _, data_bytes = _fetch_ranges(
        client,
        bucket=bucket,
        key=sq8_key,
        ranges=ranges,
        page_bytes=data_page_bytes,
        read_threads=read_threads,
    )
    return {"bytes": code_bytes + data_bytes, "requests": read_threads * 2}


def replay_query(
    client: S3RangeClient,
    *,
    bucket: str,
    code_key: str,
    sq8_key: str,
    code_ranges: Sequence[tuple[int, int]],
    data_ranges: Sequence[tuple[int, int]],
    code_page_bytes: int,
    codec: Sq8PageCodec,
    query: np.ndarray,
    delta_ids: np.ndarray,
    delta_vectors: np.ndarray,
    truth_ids: np.ndarray,
    neighbors: int,
    read_threads: int,
) -> dict[str, Any]:
    """Execute one frozen plan without access to base-corpus vectors."""

    query = np.asarray(query, dtype=np.float32)
    delta_ids = np.asarray(delta_ids, dtype=np.int64)
    delta_vectors = np.asarray(delta_vectors, dtype=np.float32)
    truth_ids = np.asarray(truth_ids, dtype=np.int64)
    if (
        query.shape != (codec.dimensions,)
        or delta_vectors.shape != (delta_ids.size, codec.dimensions)
        or truth_ids.ndim != 1
        or truth_ids.size == 0
        or neighbors <= 0
        or not np.isfinite(query).all()
        or not np.isfinite(delta_vectors).all()
    ):
        raise ValueError("V95 replay inputs differ")

    code_started = time.perf_counter_ns()
    _, code_bytes = _fetch_ranges(
        client,
        bucket=bucket,
        key=code_key,
        ranges=code_ranges,
        page_bytes=code_page_bytes,
        read_threads=read_threads,
    )
    code_io_ns = time.perf_counter_ns() - code_started

    data_started = time.perf_counter_ns()
    payloads, data_bytes = _fetch_ranges(
        client,
        bucket=bucket,
        key=sq8_key,
        ranges=data_ranges,
        page_bytes=codec.encoded_bytes,
        read_threads=read_threads,
    )
    data_io_ns = time.perf_counter_ns() - data_started

    decode_started = time.perf_counter_ns()
    base_ids: list[np.ndarray] = []
    base_vectors: list[np.ndarray] = []
    for range_index, payload in enumerate(payloads):
        first, last = data_ranges[range_index]
        for offset in range(last - first + 1):
            start = offset * codec.encoded_bytes
            ids, vectors = codec.decode(payload[start : start + codec.encoded_bytes])
            base_ids.append(ids)
            base_vectors.append(vectors)
    candidate_ids = np.concatenate((*base_ids, delta_ids))
    candidate_vectors = np.concatenate((*base_vectors, delta_vectors), axis=0)
    decode_ns = time.perf_counter_ns() - decode_started

    rerank_started = time.perf_counter_ns()
    delta = candidate_vectors - query[None, :]
    scores = np.einsum("ij,ij->i", delta, delta)
    take = min(neighbors, scores.size)
    order = np.lexsort((candidate_ids, scores))[:take]
    result_ids = [int(candidate_ids[index]) for index in order]
    rerank_ns = time.perf_counter_ns() - rerank_started
    expected = {int(value) for value in truth_ids}
    return {
        "bytes": code_bytes + data_bytes,
        "code_bytes": code_bytes,
        "code_io_ns": code_io_ns,
        "code_requests": len(code_ranges),
        "data_bytes": data_bytes,
        "data_io_ns": data_io_ns,
        "data_requests": len(data_ranges),
        "decode_ns": decode_ns,
        "requests": len(code_ranges) + len(data_ranges),
        "rerank_ns": rerank_ns,
        "result_ids": result_ids,
        "storage_io_ns": code_io_ns + data_io_ns,
        "truth_hits": len(expected.intersection(result_ids)),
    }


def _nearest_rank_ms(values: list[int], numerator: int, denominator: int) -> float:
    ordered = sorted(values)
    index = (len(ordered) * numerator - 1) // denominator
    return round(ordered[index] / 1_000_000, 3)


def summarize_replays(
    samples: list[dict[str, Any]], *, neighbors: int
) -> dict[str, Any]:
    """Summarize a bounded cohort without making unsupported p99 claims."""

    required = {
        "bytes",
        "code_io_ns",
        "data_io_ns",
        "decode_ns",
        "requests",
        "rerank_ns",
        "storage_io_ns",
        "total_ns",
        "truth_hits",
    }
    if (
        not samples
        or neighbors <= 0
        or any(
            set(sample) < required
            or any(
                type(sample[field]) is not int or sample[field] < 0
                for field in required
            )
            or sample["truth_hits"] > neighbors
            for sample in samples
        )
    ):
        raise ValueError("V95 replay samples differ")

    def latency(field: str) -> dict[str, float]:
        values = [sample[field] for sample in samples]
        return {
            "mean": round(sum(values) / len(values) / 1_000_000, 3),
            "p50": _nearest_rank_ms(values, 50, 100),
            "p95": _nearest_rank_ms(values, 95, 100),
            "maximum": round(max(values) / 1_000_000, 3),
        }

    hits = sum(sample["truth_hits"] for sample in samples)
    requests = [sample["requests"] for sample in samples]
    transferred = [sample["bytes"] for sample in samples]
    return {
        "bytes": {
            "mean": sum(transferred) / len(transferred),
            "maximum": max(transferred),
        },
        "code_io_ms": latency("code_io_ns"),
        "data_io_ms": latency("data_io_ns"),
        "decode_ms": latency("decode_ns"),
        "percentile_note": "p95-is-maximum-of-16-or-fewer",
        "queries": len(samples),
        "recall_ppm": round(hits * 1_000_000 / (len(samples) * neighbors)),
        "requests": {"mean": sum(requests) / len(requests), "maximum": max(requests)},
        "rerank_ms": latency("rerank_ns"),
        "storage_io_ms": latency("storage_io_ns"),
        "total_ms": latency("total_ns"),
        "worst_query_hits": min(sample["truth_hits"] for sample in samples),
    }


def _sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _write_delta_parquet(
    path: pathlib.Path, identifiers: np.ndarray, vectors: np.ndarray
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    values = pa.array(
        np.asarray(vectors, dtype=np.float32).reshape(-1), type=pa.float32()
    )
    embeddings = pa.FixedSizeListArray.from_arrays(values, vectors.shape[1])
    table = pa.Table.from_arrays(
        [pa.array(np.asarray(identifiers, dtype=np.uint64)), embeddings],
        names=["feature_row_id", "embedding"],
    )
    pq.write_table(table, path, compression="zstd", version="2.6")


def _put_authenticated_file(
    client: Any, *, path: pathlib.Path, bucket: str, key: str, sha256: str
) -> None:
    checksum = base64.b64encode(bytes.fromhex(sha256)).decode("ascii")
    with path.open("rb") as stream:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=stream,
            ChecksumSHA256=checksum,
            StorageClass="STANDARD",
        )


def prepare(args: argparse.Namespace) -> None:
    """Freeze 16 V94 plans and publish their base-page payloads once."""

    import boto3
    from botocore.config import Config

    import scripts.v93_protected_rescue_screen as v93
    import scripts.v94_protected_rescue_confirmation as v94
    from scripts.v85_shared_overlay_screen import _fixed_list, _scalar, _sq8
    from scripts.v86_coarse_to_fine_screen import (
        build_coarse_to_fine_artifact,
        validate_layout_order,
        validate_truth_rows,
    )
    from scripts.v87_summary_capacity_screen import _REGISTERED_CONTROL_ARTIFACT_SHA256
    from scripts.v90_residual_row_sketch_screen import (
        _BASE_ROWS,
        _CLUSTERS,
        _CONTROL_SEED,
        _CONTROL_SUBSPACES,
        _DIMENSIONS,
        _NEIGHBORS,
        _PAGE_ROWS,
        _ROWS,
        _SKETCH_SEED,
        _SKETCH_SUBSPACES,
        _TRAINING_ITERATIONS,
        _TRAINING_SAMPLE_ROWS,
        build_residual_row_sketch,
    )

    measured_queries = 16
    loaded_queries = 328 + measured_queries
    source_ids = _scalar(args.source, "feature_row_id", _ROWS)
    if np.unique(source_ids).size != _ROWS:
        raise ValueError("V95 source identifiers differ")
    loaded_truth = validate_truth_rows(
        _scalar(args.ground_truth, "query_ordinal", loaded_queries * _NEIGHBORS),
        _scalar(args.ground_truth, "rank", loaded_queries * _NEIGHBORS),
        _scalar(args.ground_truth, "feature_row_id", loaded_queries * _NEIGHBORS),
        queries=loaded_queries,
        neighbors=_NEIGHBORS,
    )
    full_order = np.asarray(np.load(args.layout_order), dtype=np.int64)
    if (
        full_order.shape != (_ROWS,)
        or np.any(full_order < 0)
        or np.any(full_order >= _ROWS)
        or np.unique(full_order).size != _ROWS
    ):
        raise ValueError("V95 full layout order differs")
    base_order = validate_layout_order(
        full_order[full_order < _BASE_ROWS], rows=_ROWS, base_rows=_BASE_ROWS
    )
    source = _fixed_list(args.source, "embedding", _ROWS)
    query_vectors = _fixed_list(args.queries, "embedding", loaded_queries)
    queries, truth, ordinals = v94.select_confirmation_rows(
        query_vectors,
        loaded_truth,
        confirmation_start=328,
        confirmation_queries=measured_queries,
    )
    base = np.ascontiguousarray(source[base_order])
    base_ids = np.ascontiguousarray(source_ids[base_order])
    delta = np.ascontiguousarray(source[_BASE_ROWS:])
    delta_ids = np.ascontiguousarray(source_ids[_BASE_ROWS:])
    control = build_coarse_to_fine_artifact(
        base,
        page_rows=_PAGE_ROWS,
        subspaces=_CONTROL_SUBSPACES,
        clusters=_CLUSTERS,
        sample_rows=_TRAINING_SAMPLE_ROWS,
        seed=_CONTROL_SEED,
        iterations=_TRAINING_ITERATIONS,
    )
    if control.digest() != _REGISTERED_CONTROL_ARTIFACT_SHA256:
        raise ValueError("V95 registered control artifact differs")
    sketch = build_residual_row_sketch(
        base,
        control,
        subspaces=_SKETCH_SUBSPACES,
        clusters=_CLUSTERS,
        sample_rows=_TRAINING_SAMPLE_ROWS,
        seed=_SKETCH_SEED,
        iterations=_TRAINING_ITERATIONS,
    )
    if sketch.digest() != v93._REGISTERED_RESIDUAL_SKETCH_SHA256:
        raise ValueError("V95 registered residual sketch differs")
    offline = v94.evaluate_confirmation_arms(
        base,
        delta,
        queries,
        control_artifact=control,
        sketch_artifact=sketch,
        base_ids=base_ids,
        delta_ids=delta_ids,
        truth_ids=truth,
        query_ordinals=ordinals,
    )
    samples = offline["arms"]["direct_rescue"]["result"]["samples"]

    args.scratch.mkdir(parents=True, exist_ok=False)
    code_codec = PqCodePageCodec(_CONTROL_SUBSPACES, _PAGE_ROWS)
    sq8_codec = Sq8PageCodec(_DIMENSIONS, _PAGE_ROWS)
    code_path = args.scratch / "codes.bin"
    sq8_path = args.scratch / "sq8.bin"
    with code_path.open("wb") as stream:
        for start in range(0, _BASE_ROWS, _PAGE_ROWS):
            stream.write(
                code_codec.encode(control.row_codes[start : start + _PAGE_ROWS])
            )
    with sq8_path.open("wb") as stream:
        for start in range(0, _BASE_ROWS, _PAGE_ROWS):
            stream.write(
                sq8_codec.encode(
                    base_ids[start : start + _PAGE_ROWS],
                    base[start : start + _PAGE_ROWS],
                )
            )
    code_sha256 = _sha256_file(code_path)
    sq8_sha256 = _sha256_file(sq8_path)
    code_key = f"{args.index_prefix}/codes.bin"
    sq8_key = f"{args.index_prefix}/sq8.bin"
    client = boto3.client(
        "s3",
        config=Config(
            connect_timeout=5,
            max_pool_connections=32,
            read_timeout=120,
            retries={"max_attempts": 3, "mode": "standard"},
            tcp_keepalive=True,
        ),
    )
    _put_authenticated_file(
        client,
        path=code_path,
        bucket=args.bucket,
        key=code_key,
        sha256=code_sha256,
    )
    _put_authenticated_file(
        client,
        path=sq8_path,
        bucket=args.bucket,
        key=sq8_key,
        sha256=sq8_sha256,
    )
    code_path.unlink()
    sq8_path.unlink()

    delta_sq8 = _sq8(delta)
    _write_delta_parquet(args.delta_sq8, delta_ids, delta_sq8)
    plan = {
        "bucket": args.bucket,
        "code_object": {
            "bytes": code_codec.encoded_bytes
            * ((_BASE_ROWS + _PAGE_ROWS - 1) // _PAGE_ROWS),
            "key": code_key,
            "sha256": code_sha256,
        },
        "code_page_bytes": code_codec.encoded_bytes,
        "data_page_bytes": sq8_codec.encoded_bytes,
        "dimensions": _DIMENSIONS,
        "neighbors": _NEIGHBORS,
        "page_rows": _PAGE_ROWS,
        "queries": [
            {
                "code_ranges": sample["wave1_ranges"],
                "data_ranges": sample["wave2_ranges"],
                "expected_hits": int(sample["page_sq8_hits"]),
                "ordinal": int(sample["query"]),
            }
            for sample in samples
        ],
        "schema": "borsuk-v95-real-s3-plan-v1",
        "source_commit": args.source_commit,
        "sq8_object": {
            "bytes": sq8_codec.encoded_bytes
            * ((_BASE_ROWS + _PAGE_ROWS - 1) // _PAGE_ROWS),
            "key": sq8_key,
            "sha256": sq8_sha256,
        },
    }
    validate_replay_plan(plan, expected_queries=measured_queries)
    args.plan.write_text(
        json.dumps(plan, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n"
    )
    build_receipt = {
        "claim_eligible": False,
        "delta_sq8_bytes": args.delta_sq8.stat().st_size,
        "offline_expected_hits": sum(
            query["expected_hits"] for query in plan["queries"]
        ),
        "offline_expected_worst": min(
            query["expected_hits"] for query in plan["queries"]
        ),
        "plan_sha256": _sha256_file(args.plan),
        "schema": "borsuk-v95-real-s3-build-receipt-v1",
        "source_commit": args.source_commit,
    }
    args.build_receipt.write_text(
        json.dumps(
            build_receipt, allow_nan=False, separators=(",", ":"), sort_keys=True
        )
        + "\n"
    )
    args.scratch.rmdir()


def replay(args: argparse.Namespace) -> None:
    """Replay the frozen plan in a fresh process with no base corpus file."""

    import boto3
    from botocore.config import Config

    from scripts.v85_shared_overlay_screen import _fixed_list, _scalar
    from scripts.v86_coarse_to_fine_screen import validate_truth_rows

    expected_queries = 16
    plan = validate_replay_plan(
        json.loads(args.plan.read_text()), expected_queries=expected_queries
    )
    client = boto3.client(
        "s3",
        config=Config(
            connect_timeout=5,
            max_pool_connections=args.read_threads * 2,
            read_timeout=120,
            retries={"max_attempts": 3, "mode": "standard"},
            tcp_keepalive=True,
        ),
    )
    for identity in (plan["code_object"], plan["sq8_object"]):
        head = client.head_object(
            Bucket=plan["bucket"], Key=identity["key"], ChecksumMode="ENABLED"
        )
        if head.get("ContentLength") != identity["bytes"] or head.get(
            "ChecksumSHA256"
        ) != base64.b64encode(bytes.fromhex(identity["sha256"])).decode("ascii"):
            raise ValueError("V95 S3 object identity differs")
    preflight = warm_range_connections(
        client,
        bucket=plan["bucket"],
        code_key=plan["code_object"]["key"],
        sq8_key=plan["sq8_object"]["key"],
        code_page_bytes=plan["code_page_bytes"],
        data_page_bytes=plan["data_page_bytes"],
        read_threads=args.read_threads,
    )
    loaded_queries = 328 + expected_queries
    query_vectors = _fixed_list(args.queries, "embedding", loaded_queries)
    truth = validate_truth_rows(
        _scalar(args.ground_truth, "query_ordinal", loaded_queries * plan["neighbors"]),
        _scalar(args.ground_truth, "rank", loaded_queries * plan["neighbors"]),
        _scalar(
            args.ground_truth, "feature_row_id", loaded_queries * plan["neighbors"]
        ),
        queries=loaded_queries,
        neighbors=plan["neighbors"],
    )
    delta_ids = _scalar(args.delta_sq8, "feature_row_id", 100_000).astype(
        np.int64, copy=False
    )
    delta_vectors = _fixed_list(args.delta_sq8, "embedding", 100_000)
    codec = Sq8PageCodec(plan["dimensions"], plan["page_rows"])
    if codec.encoded_bytes != plan["data_page_bytes"]:
        raise ValueError("V95 data page bytes differ")
    samples: list[dict[str, Any]] = []
    for query_plan in plan["queries"]:
        ordinal = query_plan["ordinal"]
        started = time.perf_counter_ns()
        sample = replay_query(
            client,
            bucket=plan["bucket"],
            code_key=plan["code_object"]["key"],
            sq8_key=plan["sq8_object"]["key"],
            code_ranges=tuple(tuple(pair) for pair in query_plan["code_ranges"]),
            data_ranges=tuple(tuple(pair) for pair in query_plan["data_ranges"]),
            code_page_bytes=plan["code_page_bytes"],
            codec=codec,
            query=query_vectors[ordinal],
            delta_ids=delta_ids,
            delta_vectors=delta_vectors,
            truth_ids=truth[ordinal],
            neighbors=plan["neighbors"],
            read_threads=args.read_threads,
        )
        sample["total_ns"] = time.perf_counter_ns() - started
        sample["ordinal"] = ordinal
        if sample["truth_hits"] != query_plan["expected_hits"]:
            raise ValueError("V95 S3 replay quality differs")
        samples.append(sample)
        print(
            json.dumps(
                {
                    "bytes": sample["bytes"],
                    "ordinal": ordinal,
                    "requests": sample["requests"],
                    "total_ms": round(sample["total_ns"] / 1_000_000, 3),
                    "truth_hits": sample["truth_hits"],
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            flush=True,
        )
    summary = summarize_replays(samples, neighbors=plan["neighbors"])
    latency_passed = bool(
        summary["storage_io_ms"]["p50"] <= 250.0
        and summary["storage_io_ms"]["p95"] <= 500.0
    )
    result = {
        "actual_s3_requests": preflight["requests"]
        + sum(sample["requests"] for sample in samples),
        "claim_eligible": False,
        "gate": {
            "p50_ms": 250.0,
            "p95_ms": 500.0,
            "passed": latency_passed,
            "quality_exactly_matches_frozen_plan": True,
            "stage": "storage-io-only",
        },
        "index": {
            "bucket": plan["bucket"],
            "code_object": plan["code_object"],
            "sq8_object": plan["sq8_object"],
        },
        "measurement": "in-region-real-s3-range-get-plan-replay",
        "no_base_corpus_local": True,
        "plan_replay_not_full_planner_latency": True,
        "preflight": preflight,
        "read_threads": args.read_threads,
        "samples": samples,
        "schema": "borsuk-v95-real-s3-plan-replay-result-v1",
        "source_commit": plan["source_commit"],
        "summary": summary,
        "wave_one_code_pages_transferred_not_decoded": True,
    }
    args.output.write_text(
        json.dumps(result, allow_nan=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare_parser = subparsers.add_parser("prepare")
    for name in ("source", "queries", "ground_truth", "layout_order"):
        prepare_parser.add_argument(
            f"--{name.replace('_', '-')}", type=pathlib.Path, required=True
        )
    prepare_parser.add_argument("--bucket", required=True)
    prepare_parser.add_argument("--index-prefix", required=True)
    prepare_parser.add_argument("--source-commit", required=True)
    prepare_parser.add_argument("--scratch", type=pathlib.Path, required=True)
    prepare_parser.add_argument("--plan", type=pathlib.Path, required=True)
    prepare_parser.add_argument("--delta-sq8", type=pathlib.Path, required=True)
    prepare_parser.add_argument("--build-receipt", type=pathlib.Path, required=True)
    replay_parser = subparsers.add_parser("replay")
    for name in ("plan", "queries", "ground_truth", "delta_sq8", "output"):
        replay_parser.add_argument(
            f"--{name.replace('_', '-')}", type=pathlib.Path, required=True
        )
    replay_parser.add_argument("--read-threads", type=int, default=16)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "prepare":
        prepare(args)
    else:
        replay(args)


if __name__ == "__main__":
    main()
