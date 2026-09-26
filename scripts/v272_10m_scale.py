#!/usr/bin/env python3
"""Authenticated CoHere 10M source, exact truth, and one frozen Rust API score."""

import argparse
import hashlib
import importlib.metadata
import json
import shutil
from pathlib import Path
from urllib.parse import urlparse

import numpy as np

from scripts.v271_fresh_frontier import D, digest, percentile, unit

BUCKET = "borsuk-bench-453182569524-euc1"
RECEIPT_SHA = "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
TEST_SHA = "5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e"
ROWS = 10_000_000
QUERIES = 1000


def source_roster(receipt):
    if (
        receipt["dataset_id"] != "cohere-large-10m-768"
        or receipt["dataset_content_sha256"]
        != "fa8ccb38e5c761388e0c2ac211cc219438cd79e802e69debd6197d74f83f11ad"
        or receipt["object_count"] != len(receipt["objects"])
    ):
        raise ValueError("source authority differs")
    shards = sorted(
        (row for row in receipt["objects"] if row["role"] == "train"),
        key=lambda row: row["uri"],
    )
    if len(shards) != 458 or sum(row["rows"] for row in shards) != ROWS:
        raise ValueError("canonical train roster differs")
    return shards


def prepare(args):
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq

    if digest(args.receipt) != RECEIPT_SHA or digest(args.test) != TEST_SHA:
        raise ValueError("source receipt or test object differs")
    shards = source_roster(json.loads(args.receipt.read_text()))
    table = pq.read_table(args.test, columns=["emb"])
    field = table.schema.field("emb")
    if (
        table.num_rows != QUERIES
        or not pa.types.is_fixed_size_list(field.type)
        or field.type.list_size != D
        or field.type.value_type != pa.float32()
        or table.column("emb").null_count
        or table.column("emb").combine_chunks().values.null_count
    ):
        raise ValueError("test geometry differs")
    values = table.column("emb").combine_chunks().values.to_numpy()
    unit(np.asarray(values, dtype="<f4").reshape(QUERIES, D)).tofile(args.queries)
    client = boto3.client("s3", region_name="eu-central-1")
    raw_hash = hashlib.sha256()
    rows = 0
    source_bytes = 0
    with args.source.open("xb") as output:
        for ordinal, identity in enumerate(shards):
            uri = urlparse(identity["uri"])
            if uri.scheme != "s3" or uri.netloc != BUCKET:
                raise ValueError("shard URI differs")
            temporary = args.source.with_suffix(".parquet.part")
            with temporary.open("wb") as target:
                shutil.copyfileobj(
                    client.get_object(Bucket=BUCKET, Key=uri.path.lstrip("/"))["Body"],
                    target,
                )
            if (
                temporary.stat().st_size != identity["bytes"]
                or digest(temporary) != identity["sha256"]
            ):
                raise ValueError(f"train shard {ordinal} differs")
            source_bytes += identity["bytes"]
            part = pq.read_table(temporary, columns=["emb"])
            column = part.schema.field("emb")
            if (
                part.num_rows != identity["rows"]
                or not pa.types.is_fixed_size_list(column.type)
                or column.type.list_size != D
                or column.type.value_type != pa.float32()
                or part.column("emb").null_count
                or part.column("emb").combine_chunks().values.null_count
            ):
                raise ValueError(f"train shard {ordinal} geometry differs")
            values = part.column("emb").combine_chunks().values.to_numpy()
            block = np.asarray(values, dtype="<f4").reshape(part.num_rows, D)
            if not np.isfinite(block).all() or not (block != 0).any(axis=1).all():
                raise ValueError(f"train shard {ordinal} values differ")
            chunk = np.ascontiguousarray(block).tobytes()
            output.write(chunk)
            raw_hash.update(chunk)
            rows += part.num_rows
            temporary.unlink()
        output.flush()
    if rows != ROWS or args.source.stat().st_size != ROWS * D * 4:
        raise ValueError("materialized corpus geometry differs")
    args.output.write_text(
        json.dumps(
            {
                "schema": "borsuk-v272-10m-source-v1",
                "rows": rows,
                "dimensions": D,
                "receipt_sha256": RECEIPT_SHA,
                "test_sha256": TEST_SHA,
                "raw_sha256": raw_hash.hexdigest(),
                "queries_sha256": digest(args.queries),
                "source_gets": len(shards),
                "source_response_bytes": source_bytes,
            },
            sort_keys=True,
        )
        + "\n"
    )


def truth(args):
    import faiss

    prepared = json.loads(args.prepared.read_text())
    if (
        prepared["schema"] != "borsuk-v272-10m-source-v1"
        or args.source.stat().st_size != ROWS * D * 4
        or digest(args.source) != prepared["raw_sha256"]
        or digest(args.queries) != prepared["queries_sha256"]
    ):
        raise ValueError("prepared source differs")
    data = np.memmap(args.source, dtype="<f4", mode="r", shape=(ROWS, D))
    queries = np.fromfile(args.queries, dtype="<f4").reshape(QUERIES, D)
    index = faiss.IndexFlatIP(D)
    faiss.omp_set_num_threads(16)
    for start in range(0, ROWS, 65_536):
        index.add(unit(data[start : start + 65_536]))
    _, ids = index.search(queries, 100)
    if ids.shape != (QUERIES, 100) or (ids < 0).any():
        raise ValueError("exact truth incomplete")
    np.asarray(ids, dtype="<u4").tofile(args.truth)
    args.output.write_text(
        json.dumps(
            {
                "schema": "borsuk-v272-10m-truth-v1",
                "rows": index.ntotal,
                "queries": QUERIES,
                "k": 100,
                "faiss_version": importlib.metadata.version("faiss-cpu"),
                "truth_sha256": digest(args.truth),
            },
            sort_keys=True,
        )
        + "\n"
    )


def score(args):
    truth = np.fromfile(args.truth, dtype="<u4").reshape(QUERIES, 100)
    latency = []
    hits = []
    raw = args.raw.read_text().splitlines()
    if len(raw) != QUERIES:
        raise ValueError("query count differs")
    for ordinal, line in enumerate(raw):
        sample = json.loads(line)
        ids = sample["ids"]
        if (
            sample["ordinal"] != ordinal
            or len(ids) != 100
            or len(set(ids)) != 100
            or min(ids) < 0
            or max(ids) >= ROWS
            or sample["latency_ns"] <= 0
        ):
            raise ValueError("query identity or IDs differ")
        hits.append(len(set(ids) & set(map(int, truth[ordinal]))))
        latency.append(sample["latency_ns"] / 1_000_000)

    def split(part):
        return {
            "hits": sum(part),
            "possible_hits": len(part) * 100,
            "recall_at_100": sum(part) / (len(part) * 100),
            "p05_hits": percentile(part, 5),
        }

    result = {
        "schema": "borsuk-v272-10m-quality-v1",
        "combined": split(hits),
        "development_first256_prior_used": split(hits[:256]),
        "validation_remaining744_prior_used": split(hits[256:]),
        "p50_ms": percentile(latency, 50),
        "p90_ms": percentile(latency, 90),
        "p95_ms": percentile(latency, 95),
        "p99_ms": percentile(latency, 99),
        "raw_sha256": digest(args.raw),
        "truth_sha256": digest(args.truth),
    }
    args.output.write_text(json.dumps(result, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    specs = {
        "prepare": ("receipt", "test", "source", "queries", "output"),
        "truth": ("prepared", "source", "queries", "truth", "output"),
        "score": ("raw", "truth", "output"),
    }
    for command, fields in specs.items():
        branch = commands.add_parser(command)
        for field in fields:
            branch.add_argument("--" + field, type=Path, required=True)
    args = parser.parse_args()
    {"prepare": prepare, "truth": truth, "score": score}[args.command](args)


if __name__ == "__main__":
    main()
