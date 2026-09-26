#!/usr/bin/env python3
"""Frozen eight-connection HTTP gate for V219's selected 1M graph arm."""

import argparse
import concurrent.futures
import hashlib
import http.client
import json
import math
import time
from pathlib import Path

COUNT = 1000
WORKERS = 8


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def percentile(values: list[int], percent: int) -> int:
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * percent / 100) - 1]


def run(args: argparse.Namespace) -> None:
    prep = json.loads(args.prep.read_text())
    cohere = prep["schema"] == "borsuk-v248-source-preparation-v1"
    valid_requests = (prep.get("requests_sha256") == digest(args.requests)
                      if not cohere else
                      prep.get("dataset_id") == "cohere-large-10m-768"
                      and prep.get("rows") == 1_000_000
                      and digest(args.requests) ==
                      "86d9406486a2bb27aa2e603f019e078dd3ecaed47f79ec685558ba3536433812")
    if (prep["schema"] not in ("borsuk-v217-graph-1m-preparation-v1",
                               "borsuk-v248-source-preparation-v1")
            or not valid_requests):
        raise ValueError("V219 request panel identity differs")
    requests = [json.loads(line) for line in args.requests.read_text().splitlines()]
    if (len(requests) != COUNT or any(
            row["query_ordinal"] != index or len(row["query"]) != 768
            or not all(math.isfinite(value) for value in row["query"])
            for index, row in enumerate(requests))):
        raise ValueError("V219 request panel geometry differs")
    def worker(offset: int) -> list[dict]:
        connection = http.client.HTTPConnection(args.host, args.port, timeout=30)
        rows = []
        try:
            for index in range(offset, COUNT, WORKERS):
                request_start = time.perf_counter_ns()
                payload = json.dumps({"query": requests[index]["query"], "k": 100},
                                     separators=(",", ":")).encode()
                connection.request("POST", "/search", body=payload,
                                   headers={"content-type": "application/json"})
                response = connection.getresponse()
                body = response.read()
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status} at {index}: {body[:200]!r}")
                result = json.loads(body)
                ids = result["ids"]
                if (len(ids) != 100 or len(set(ids)) != 100
                        or result["vector_body_gets"] != 0):
                    raise ValueError(f"HTTP result geometry differs at {index}")
                request_end = time.perf_counter_ns()
                rows.append({"ordinal": index, "returned_ids": ids,
                             "whole_ns": request_end - request_start,
                             "start_ns": request_start - started,
                             "end_ns": request_end - started, "worker": offset,
                             "base_visits": result["base_visits"],
                             "request_bytes": len(payload),
                             "response_bytes": len(body), "vector_body_gets": 0})
        finally:
            connection.close()
        return rows

    started = time.perf_counter_ns()
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = [pool.submit(worker, offset) for offset in range(WORKERS)]
        rows = sorted((row for result in results for row in result.result()),
                      key=lambda row: row["ordinal"])
    wall_ns = time.perf_counter_ns() - started
    if [row["ordinal"] for row in rows] != list(range(COUNT)):
        raise ValueError("HTTP response panel differs")
    with args.raw.open("w") as output:
        for row in rows:
            output.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    latencies = [row["whole_ns"] for row in rows]
    args.summary.write_text(json.dumps({
        "schema": ("borsuk-v262-cohere-dual-http-client-1m-v1" if cohere else
                   "borsuk-v220-graph-http-1m-v1" if args.host in {"127.0.0.1", "localhost"}
                   else "borsuk-v222-graph-http-client-1m-v1"),
        "requests_sha256": digest(args.requests),
        "raw_sha256": digest(args.raw), "queries": COUNT, "concurrency": WORKERS,
        "transport": ("persistent HTTP/1.1 loopback" if args.host in {"127.0.0.1", "localhost"}
                      else "persistent HTTP/1.1 VPC peer"),
        "timing_scope": "client JSON encode, HTTP exchange, and JSON decode",
        "cache_state": "resident after authenticated hydration",
        "comparator": "not measured in this cell",
        "pass_label": args.pass_label,
        "vector_body_gets_basis": "zero by construction; resident plane, no object client",
        "p50_ns": percentile(latencies, 50), "p90_ns": percentile(latencies, 90),
        "p95_ns": percentile(latencies, 95), "p99_ns": percentile(latencies, 99),
        "wall_ns": wall_ns, "qps": COUNT * 1e9 / wall_ns,
        "request_bytes": sum(row["request_bytes"] for row in rows),
        "response_bytes": sum(row["response_bytes"] for row in rows),
        "vector_body_gets": 0,
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--pass-label", choices=("single_pass", "first_pass", "immediate_repeat"),
                        default="single_pass")
    for name in ("prep", "requests", "raw", "summary"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
