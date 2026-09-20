#!/usr/bin/env python3
"""Replay the immutable V95 S3 ranges in reverse query order.

This transport-only diagnostic downloads no corpus, query, truth, or vector
artifact.  It authenticates the frozen V95 plan and its two object identities,
opens a fresh S3 client, performs the registered untimed connection preflight,
then reads the same ranges in reverse query order without decoding them.
"""

from __future__ import annotations

import argparse
import base64
import json
import pathlib
import time
from typing import Any

from scripts.v95_real_s3_plan_replay import (
    S3RangeClient,
    _fetch_ranges,
    _sha256_file,
    validate_replay_plan,
    warm_range_connections,
)


def replay_transport_order(
    client: S3RangeClient,
    *,
    plan: dict[str, Any],
    read_threads: int,
    order: str,
) -> list[dict[str, Any]]:
    """Read every frozen range once in the requested query order."""

    validated = validate_replay_plan(plan, expected_queries=len(plan["queries"]))
    if order not in {"forward", "reverse"} or read_threads <= 0:
        raise ValueError("V96 transport order differs")
    queries = list(validated["queries"])
    if order == "reverse":
        queries.reverse()

    samples: list[dict[str, Any]] = []
    for position, query in enumerate(queries):
        code_ranges = tuple(tuple(pair) for pair in query["code_ranges"])
        data_ranges = tuple(tuple(pair) for pair in query["data_ranges"])
        code_started = time.perf_counter_ns()
        _, code_bytes = _fetch_ranges(
            client,
            bucket=validated["bucket"],
            key=validated["code_object"]["key"],
            ranges=code_ranges,
            page_bytes=validated["code_page_bytes"],
            read_threads=read_threads,
        )
        code_io_ns = time.perf_counter_ns() - code_started
        data_started = time.perf_counter_ns()
        _, data_bytes = _fetch_ranges(
            client,
            bucket=validated["bucket"],
            key=validated["sq8_object"]["key"],
            ranges=data_ranges,
            page_bytes=validated["data_page_bytes"],
            read_threads=read_threads,
        )
        data_io_ns = time.perf_counter_ns() - data_started
        samples.append(
            {
                "bytes": code_bytes + data_bytes,
                "code_io_ns": code_io_ns,
                "code_requests": len(code_ranges),
                "data_io_ns": data_io_ns,
                "data_requests": len(data_ranges),
                "expected_hits": query["expected_hits"],
                "ordinal": query["ordinal"],
                "position": position,
                "requests": len(code_ranges) + len(data_ranges),
                "storage_io_ns": code_io_ns + data_io_ns,
            }
        )
    return samples


def classify_transport_tail(
    samples: list[dict[str, Any]], *, threshold_ms: int
) -> str:
    """Classify whether the registered tail follows position or ordinal 328."""

    if (
        not samples
        or threshold_ms <= 0
        or samples[0].get("ordinal") == 328
        or not any(sample.get("ordinal") == 328 for sample in samples)
    ):
        raise ValueError("V96 transport samples differ")
    threshold_ns = threshold_ms * 1_000_000
    first_slow = samples[0]["storage_io_ns"] > threshold_ns
    ordinal_328_slow = next(
        sample["storage_io_ns"] > threshold_ns
        for sample in samples
        if sample["ordinal"] == 328
    )
    if first_slow and not ordinal_328_slow:
        return "position-dependent-tail-supported"
    if ordinal_328_slow and not first_slow:
        return "ordinal-328-tail-supported"
    if first_slow:
        return "tail-cause-unresolved"
    return "original-tail-not-reproduced"


def _latency(values: list[int]) -> dict[str, float]:
    ordered = sorted(values)
    return {
        "maximum": round(max(values) / 1_000_000, 3),
        "mean": round(sum(values) / len(values) / 1_000_000, 3),
        "p50": round(ordered[(len(values) * 50 - 1) // 100] / 1_000_000, 3),
        "p95": round(ordered[(len(values) * 95 - 1) // 100] / 1_000_000, 3),
    }


def run(args: argparse.Namespace) -> None:
    """Authenticate and execute one reverse-order transport diagnostic."""

    import boto3
    from botocore.config import Config

    if _sha256_file(args.plan) != args.plan_sha256:
        raise ValueError("V96 plan identity differs")
    plan = validate_replay_plan(json.loads(args.plan.read_text()), expected_queries=16)
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
        expected_checksum = base64.b64encode(
            bytes.fromhex(identity["sha256"])
        ).decode("ascii")
        if (
            head.get("ContentLength") != identity["bytes"]
            or head.get("ChecksumSHA256") != expected_checksum
        ):
            raise ValueError("V96 S3 object identity differs")
    preflight = warm_range_connections(
        client,
        bucket=plan["bucket"],
        code_key=plan["code_object"]["key"],
        sq8_key=plan["sq8_object"]["key"],
        code_page_bytes=plan["code_page_bytes"],
        data_page_bytes=plan["data_page_bytes"],
        read_threads=args.read_threads,
    )
    samples = replay_transport_order(
        client,
        plan=plan,
        read_threads=args.read_threads,
        order="reverse",
    )
    for sample in samples:
        print(
            json.dumps(
                {
                    "bytes": sample["bytes"],
                    "ordinal": sample["ordinal"],
                    "position": sample["position"],
                    "requests": sample["requests"],
                    "storage_io_ms": round(sample["storage_io_ns"] / 1_000_000, 3),
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            flush=True,
        )
    result = {
        "actual_s3_requests": preflight["requests"]
        + sum(sample["requests"] for sample in samples),
        "claim_eligible": False,
        "classification": classify_transport_tail(samples, threshold_ms=500),
        "frozen_quality_hits_not_recomputed": True,
        "gate_ms": 500,
        "measurement": "in-region-real-s3-reverse-order-range-get",
        "no_corpus_query_truth_or_vector_download": True,
        "order": "reverse",
        "plan_sha256": args.plan_sha256,
        "preflight": preflight,
        "read_threads": args.read_threads,
        "samples": samples,
        "schema": "borsuk-v96-s3-transport-order-result-v1",
        "source_commit": args.source_commit,
        "storage_io_ms": _latency(
            [sample["storage_io_ns"] for sample in samples]
        ),
    }
    args.output.write_text(
        json.dumps(result, allow_nan=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=pathlib.Path, required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--read-threads", type=int, default=16)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    return parser.parse_args()


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
