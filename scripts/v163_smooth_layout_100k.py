#!/usr/bin/env python3
"""V163 source-only smooth k-means relayout and paired 100k return gate."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from pathlib import Path

import numpy as np

from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v114_exact_local_100k import SOURCE_SHA256, route_reference
from scripts.v120_source_layout import _fit_order, cluster_count
from scripts.v158_pq_primary_returned import (
    CAP_BYTES, CAP_GETS, DIMS, QUERIES, ROWS, authenticate, jsonl,
    load_truth, sha256, spread,
)
from scripts.v160_geometric_relayout_primary import (
    DTYPE, ROW_BYTES, page_rows, route,
)

SCHEMA = "borsuk-v163-smooth-geometric-layout-100k-v1"
SOURCE_BYTES = 145_121_661
V160_TERMINAL_SHA = "026bf6792ae140f8cba83bc4452d4db0e54cb31facfdd84394309117931f0c52"
V160_RAW_SHA = "c2d0676ba3f0003a6f90681ce28143d509847786a3714ce98f0ffca02d1eab67"


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def source_vectors(source_path: Path, old_sq8: np.memmap) -> np.ndarray:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if source_path.stat().st_size != SOURCE_BYTES or sha256(source_path) != SOURCE_SHA256:
        raise ValueError("V163 source Parquet identity differs")
    table = pq.read_table(source_path, columns=["feature_row_id", "embedding"])
    ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    embedding = table.schema.field("embedding").type
    if (table.num_rows != ROWS or not pa.types.is_fixed_size_list(embedding)
            or embedding.list_size != DIMS or embedding.value_type != pa.float32()
            or not np.array_equal(ids, old_sq8["id"])):
        raise ValueError("V163 source geometry or stable-ID mapping differs")
    values = table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False)
    vectors = np.asarray(values, dtype=np.float32).reshape(ROWS, DIMS)
    if not np.isfinite(vectors).all():
        raise ValueError("V163 source vector is nonfinite")
    return vectors


def construct(source_path: Path, old_sq8_path: Path, old_manifest: Path,
              order_path: Path, new_sq8_path: Path, layout_seal: Path) -> None:
    authenticate(old_sq8_path, "sq8")
    authenticate(old_manifest, "manifest")
    old = np.memmap(old_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    vectors = source_vectors(source_path, old)
    clusters = cluster_count(ROWS)
    if clusters != 3803:
        raise ValueError("V163 smooth cluster rule differs")
    order = _fit_order(vectors, clusters)
    if (order.shape != (ROWS,) or order.dtype != np.int64
            or not np.array_equal(np.sort(order), np.arange(ROWS))):
        raise ValueError("V163 k-means chain order is not a permutation")
    with order_path.open("xb") as output:
        np.save(output, order, allow_pickle=False)
    with new_sq8_path.open("xb") as output:
        for start in range(0, ROWS, 4096):
            output.write(old[order[start:start + 4096]].tobytes())
    if new_sq8_path.stat().st_size != ROWS * ROW_BYTES:
        raise ValueError("V163 relaid SQ8 length differs")
    layout_seal.write_text(canonical({
        "schema": SCHEMA + "-layout-seal", "gt_opened": False,
        "source_sha256": SOURCE_SHA256, "old_sq8_sha256": sha256(old_sq8_path),
        "old_manifest_sha256": sha256(old_manifest),
        "order_file_sha256": sha256(order_path),
        "order_sha256": hashlib.sha256(order.astype("<i8").tobytes()).hexdigest(),
        "new_sq8_sha256": sha256(new_sq8_path),
        "layout_method": "v120-lloyd-centroid-chain-l2",
        "cluster_rule": "min(N,ceil(8192*(N/1000000)^(1/3)))",
        "clusters": clusters, "lloyd_iterations": 12,
        "seed_sample": 8201, "seed_lloyd": 8202,
        "page_rows": page_rows(), "rows": ROWS, "dimensions": DIMS,
        "row_bytes": ROW_BYTES, "cap_gets": CAP_GETS, "cap_bytes": CAP_BYTES,
    }))


def plan(requests: Path, reference: Path, old_sq8_path: Path,
         order_path: Path, new_sq8_path: Path, layout_seal: Path,
         plans_path: Path, plan_seal: Path) -> None:
    for role, path in (("requests", requests), ("reference", reference),
                       ("sq8", old_sq8_path)):
        authenticate(path, role)
    seal = json.loads(layout_seal.read_text())
    if (seal.get("gt_opened") is not False
            or seal.get("order_file_sha256") != sha256(order_path)
            or seal.get("new_sq8_sha256") != sha256(new_sq8_path)
            or seal.get("page_rows") != page_rows()):
        raise ValueError("V163 GT-blind layout seal differs")
    order = np.load(order_path, allow_pickle=False)
    if (order.shape != (ROWS,) or not np.array_equal(np.sort(order), np.arange(ROWS))):
        raise ValueError("V163 order file differs")
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[order] = np.arange(ROWS, dtype=np.int64)
    count = 0
    with plans_path.open("x") as output:
        for request, ref in itertools.zip_longest(jsonl(requests), jsonl(reference)):
            if (request is None or ref is None
                    or request.get("query_ordinal") != count
                    or ref.get("query_ordinal") != count):
                raise ValueError("V163 frozen roster order differs")
            nominees, primary = request["nominees"], ref["primary"]
            _, old_ranges, old_amount, old_score = route_reference(
                primary, nominees, rows=ROWS, dimensions=DIMS,
            )
            if (old_ranges != ref["ranges"] or old_amount != ref["plan_bytes"]
                    or old_score != ref["plan_score"]):
                raise ValueError("V114 source-order control differs")
            ranges, amount, distinct, score = route(primary, nominees, inverse,
                                                    page_rows())
            output.write(canonical({"query_ordinal": count,
                                    "ranges": ranges, "bytes": amount,
                                    "distinct_primary_pages": distinct,
                                    "plan_score": score}))
            count += 1
    if count != QUERIES:
        raise ValueError("V163 request count differs")
    plan_seal.write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "gt_opened": False,
        "queries": count, "layout_seal_sha256": sha256(layout_seal),
        "requests_sha256": sha256(requests),
        "reference_sha256": sha256(reference),
        "plans_sha256": sha256(plans_path),
    }))


def reduce(requests: Path, plans_path: Path, plan_seal: Path,
           layout_seal: Path, new_sq8_path: Path, old_manifest: Path,
           truth_path: Path, v160_terminal: Path, v160_raw: Path,
           raw_path: Path, summary_path: Path) -> None:
    authenticate(requests, "requests")
    authenticate(old_manifest, "manifest")
    authenticate(truth_path, "truth")
    if (sha256(v160_terminal) != V160_TERMINAL_SHA
            or sha256(v160_raw) != V160_RAW_SHA):
        raise ValueError("closed V160 control identity differs")
    terminal = json.loads(v160_terminal.read_text())
    if (terminal.get("status") != "complete"
            or terminal.get("artifacts", {}).get("raw.jsonl", {}).get("sha256")
                != V160_RAW_SHA):
        raise ValueError("V160 terminal binding differs")
    seal = json.loads(plan_seal.read_text())
    layout = json.loads(layout_seal.read_text())
    if (seal.get("gt_opened") is not False
            or seal.get("plans_sha256") != sha256(plans_path)
            or seal.get("layout_seal_sha256") != sha256(layout_seal)
            or layout.get("new_sq8_sha256") != sha256(new_sq8_path)):
        raise ValueError("V163 GT-blind plan seal differs")
    manifest = json.loads(old_manifest.read_text())
    low = np.asarray(manifest["low"], dtype=np.float32)
    step = np.asarray(manifest["step"], dtype=np.float32)
    if (low.shape != (DIMS,) or step.shape != (DIMS,)
            or not np.isfinite(low).all() or not np.isfinite(step).all()
            or (step <= 0).any()):
        raise ValueError("V163 SQ8 quantizer differs")
    sq8 = np.memmap(new_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    positions = {int(identifier): i for i, identifier in enumerate(sq8["id"])}
    if len(positions) != ROWS:
        raise ValueError("V163 stable IDs differ")
    gold = load_truth(truth_path)
    fields = ("hits100", "hits10", "physical_coverage", "bytes", "gets",
              "distinct_primary_pages", "control_hits100", "control_hits10",
              "control_bytes", "control_gets")
    metrics = {field: [] for field in fields}
    wins = ties = losses = 0
    count = 0
    with raw_path.open("x") as output:
        for request, planned, control in itertools.zip_longest(
                jsonl(requests), jsonl(plans_path), jsonl(v160_raw)):
            if any(row is None or row.get("query_ordinal") != count
                   for row in (request, planned, control)):
                raise ValueError("V163 reduction row order differs")
            ranges = planned["ranges"]
            if (not 1 <= len(ranges) <= CAP_GETS
                    or planned["bytes"] != sum(end - start for start, end in ranges)
                    or planned["bytes"] > CAP_BYTES):
                raise ValueError("V163 range cap differs")
            query = np.asarray(request["query"], dtype=np.float32)
            returned = score_sq8_ranges(sq8, query, low, step, ranges, top_k=100)
            if len(returned) != 100 or len(set(returned)) != 100:
                raise ValueError("V163 returned ID width differs")
            truth100 = set(map(int, gold[count]))
            truth10 = set(map(int, gold[count, :10]))
            hits100 = len(set(returned) & truth100)
            hits10 = len(set(returned[:10]) & truth10)
            physical = sum(any(start <= positions[identifier] * ROW_BYTES < end
                               for start, end in ranges) for identifier in truth100)
            if hits100 > physical:
                raise ValueError("V163 returned hits exceed physical coverage")
            values = {
                "hits100": hits100, "hits10": hits10,
                "physical_coverage": physical, "bytes": planned["bytes"],
                "gets": len(ranges),
                "distinct_primary_pages": planned["distinct_primary_pages"],
                "control_hits100": control["hits100"],
                "control_hits10": control["hits10"],
                "control_bytes": control["bytes"],
                "control_gets": control["gets"],
            }
            for field, value in values.items():
                metrics[field].append(value)
            wins += hits100 > control["hits100"]
            ties += hits100 == control["hits100"]
            losses += hits100 < control["hits100"]
            output.write(canonical({"query_ordinal": count,
                                    "returned_ids": returned, **values}))
            count += 1
    if count != QUERIES:
        raise ValueError("V163 reduction query count differs")
    if (sum(metrics["control_hits100"]) != 99_170
            or sum(metrics["control_hits10"]) != 9_945):
        raise ValueError("V160 control totals differ")
    candidate = metrics["hits100"]
    passed = (sum(candidate) >= 97_500 and sorted(candidate)[49] >= 90
              and sum(metrics["hits10"]) >= 9_600)
    summary_path.write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "ReLAION-100k",
        "split": "development-1000-used", "queries": QUERIES,
        "decision": "candidate-advance" if passed else "killed",
        "paired": {"wins": wins, "ties": ties, "losses": losses},
        "metrics": {field: spread(values) for field, values in metrics.items()},
        "raw_sha256": sha256(raw_path),
        "plan_seal_sha256": sha256(plan_seal),
        "v160_terminal_sha256": V160_TERMINAL_SHA,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("construct", "plan", "reduce"))
    for name in ("source", "old_sq8", "old_manifest", "order", "new_sq8",
                 "layout_seal", "requests", "reference", "plans", "plan_seal",
                 "truth", "v160_terminal", "v160_raw", "raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path)
    args = parser.parse_args()
    if args.phase == "construct":
        construct(args.source, args.old_sq8, args.old_manifest,
                  args.order, args.new_sq8, args.layout_seal)
    elif args.phase == "plan":
        plan(args.requests, args.reference, args.old_sq8, args.order,
             args.new_sq8, args.layout_seal, args.plans, args.plan_seal)
    else:
        reduce(args.requests, args.plans, args.plan_seal, args.layout_seal,
               args.new_sq8, args.old_manifest, args.truth, args.v160_terminal,
               args.v160_raw, args.raw, args.summary)
