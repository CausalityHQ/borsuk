#!/usr/bin/env python3
"""Prepare CoHere queries, then score sealed IDs against exact 100k cosine truth."""

import argparse
import json
import math
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v248_prepare_cohere_graph import DIMS, digest

TEST_SHA = "5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e"
ARMS = ("2048-2048", "4096-4096", "8192-8192")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def requests(test: Path, output: Path):
    if test.stat().st_size != 3_073_101 or digest(test) != TEST_SHA:
        raise ValueError("CoHere test object differs")
    table = pq.read_table(test, columns=["emb"])
    column = table.schema.field("emb")
    if (table.num_rows != 1000 or not pa.types.is_fixed_size_list(column.type)
            or column.type.list_size != DIMS or column.type.value_type != pa.float32()
            or table.column("emb").null_count
            or table.column("emb").combine_chunks().values.null_count):
        raise ValueError("CoHere test geometry differs")
    data = np.asarray(table.column("emb").combine_chunks().values.to_numpy(
        zero_copy_only=False), dtype="<f4").reshape(1000, DIMS)
    if not np.isfinite(data).all() or not (data != 0).any(axis=1).all():
        raise ValueError("CoHere test values differ")
    with output.open("x") as destination:
        for ordinal, query in enumerate(data):
            destination.write(canonical({"query_ordinal": ordinal,
                                         "query": query.astype(float).tolist()}))
    return {"schema": "borsuk-v248-cohere-requests-v1", "test_sha256": TEST_SHA,
            "requests_sha256": digest(output), "queries": 1000}


def percentile(values, pct):
    return sorted(values)[math.ceil(len(values) * pct / 100) - 1]


def score(source: Path, prep: Path, request_file: Path, raw_file: Path,
          serving_file: Path, truth_output: Path, output: Path):
    prepared = json.loads(prep.read_text())
    serving = json.loads(serving_file.read_text())
    if (prepared["schema"] != "borsuk-v248-source-preparation-v1"
            or prepared["rows"] != 100_000
            or prepared["source_sha256"] != digest(source)
            or serving["schema"] != "borsuk-v248-cohere-graph-100k-serving-v1"
            or serving["raw_sha256"] != digest(raw_file)
            or serving["requests_sha256"] != digest(request_file)):
        raise ValueError("CoHere source or pretruth identity differs")
    rows = [json.loads(line) for line in raw_file.read_text().splitlines()]
    queries = [json.loads(line) for line in request_file.read_text().splitlines()]
    if (len(rows) != 1000 or len(queries) != 1000
            or any(row["ordinal"] != i or set(row["arms"]) != set(ARMS)
                   or row["vector_body_gets"] != 0
                   or queries[i]["query_ordinal"] != i
                   for i, row in enumerate(rows))):
        raise ValueError("CoHere query or returned ID panel differs")
    data = np.memmap(source, dtype="<f4", mode="r", shape=(100_000, DIMS))
    corpus = np.asarray(data, dtype=np.float64)
    corpus /= np.linalg.norm(corpus, axis=1)[:, None]
    q = np.asarray([row["query"] for row in queries], dtype=np.float64)
    q /= np.linalg.norm(q, axis=1)[:, None]
    ids = np.arange(100_000)
    hits = {arm: [] for arm in ARMS}
    with truth_output.open("xb") as truth:
        for first in range(0, 1000, 16):
            scores = q[first:first + 16] @ corpus.T
            for offset, vector in enumerate(scores):
                top = np.argpartition(-vector, 99)[:100]
                threshold = vector[top].min()
                candidates = ids[vector >= threshold]
                exact = candidates[np.lexsort((candidates, -vector[candidates]))][:100]
                truth.write(np.asarray(exact, dtype="<u4").tobytes())
                ordinal = first + offset
                for arm in ARMS:
                    returned = rows[ordinal]["arms"][arm]["returned_ids"]
                    if (len(returned) != 100 or len(set(returned)) != 100
                            or min(returned) < 0 or max(returned) >= 100_000):
                        raise ValueError("CoHere returned IDs differ")
                    hits[arm].append(len(set(returned) & set(exact.tolist())))
    summary = {}
    selected = None
    for arm in ARMS:
        def split(start, stop):
            values = hits[arm][start:stop]
            return {"queries": len(values), "gt100_hits": sum(values),
                    "mean_r100": sum(values) / (100 * len(values)),
                    "p05_hits": percentile(values, 5)}
        dev, validation = split(0, 256), split(256, 1000)
        summary[arm] = {"development_first_256_prior_used": dev,
                        "validation_remaining_744_prior_used": validation,
                        "combined": split(0, 1000)}
        if selected is None and dev["mean_r100"] >= .995 and dev["p05_hits"] >= 98:
            selected = arm
    passed = selected is not None and (
        summary[selected]["validation_remaining_744_prior_used"]["mean_r100"] >= .995
        and summary[selected]["validation_remaining_744_prior_used"]["p05_hits"] >= 98)
    output.write_text(canonical({"schema": "borsuk-v248-cohere-quality-v1",
        "dataset": "CoHere-large-10M first 100k D768 cosine", "k": 100,
        "source_sha256": digest(source), "test_sha256": TEST_SHA,
        "requests_sha256": digest(request_file), "raw_sha256": digest(raw_file),
        "serving_sha256": digest(serving_file),
        "exact_gt100_ids_le_u32_sha256": digest(truth_output),
        "arms": summary, "selected_arm": selected, "quality_gate_pass": passed}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("requests", "score"))
    for name in ("test", "source", "prep", "request_file", "raw_file",
                 "serving_file", "truth_output", "output"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path)
    args = parser.parse_args()
    if args.mode == "requests":
        args.output.write_text(canonical(requests(args.test, args.request_file)))
    else:
        score(args.source, args.prep, args.request_file, args.raw_file,
              args.serving_file, args.truth_output, args.output)
