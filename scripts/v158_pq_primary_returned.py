#!/usr/bin/env python3
"""GT-blind paired plans followed by frozen-GT returned-quality reduction."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from pathlib import Path

import numpy as np

from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v114_exact_local_100k import route_reference

ROWS = 100_000
DIMS = 768
QUERIES = 1_000
CAP_BYTES = 16_777_216
CAP_GETS = 32
HASHES = {
    "requests": "b2485629b919614bf46877a779b16d678cd1690d1872b7d4f9c9cbe6ddd94eb0",
    "reference": "fa42050d6610630576f3f00232aecb8c43e6a0af350bf0ac90094aaba00c9b7b",
    "sq8": "5d215d5983da54015038083cd3f1cda16983a2660e7dd66be74bba96debe3375",
    "manifest": "14a12fa3f7a571a99bf4e4411fea0a9f4a2180579decc3e539db558accda5be5",
    "truth": "ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7",
}
SIZES = {"requests": 17_714_558, "reference": 9_374_356,
         "sq8": 78_000_000, "manifest": 30_000, "truth": 512_093}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def authenticate(path: Path, role: str) -> None:
    if path.stat().st_size != SIZES[role] or sha256(path) != HASHES[role]:
        raise ValueError(f"{role} frozen identity differs")


def jsonl(path: Path):
    with path.open() as source:
        for line in source:
            yield json.loads(line)


def check_plan(ranges: list[list[int]], byte_count: int) -> None:
    object_bytes = ROWS * (DIMS + 12)
    full_page_bytes = 256 * (DIMS + 12)
    if (not 1 <= len(ranges) <= CAP_GETS
            or byte_count != sum(end - start for start, end in ranges)
            or byte_count > CAP_BYTES or byte_count <= 0
            or any(start < 0 or start >= end or start % full_page_bytes
                   or (end != object_bytes and end % full_page_bytes)
                   or end > object_bytes
                   for start, end in ranges)
            or any(left[1] >= right[0] for left, right in zip(ranges, ranges[1:]))):
        raise ValueError("physical plan exceeds cap or geometry")


def plan(requests: Path, reference: Path, sq8: Path, manifest: Path,
         plans: Path, seal: Path) -> None:
    for role, path in (("requests", requests), ("reference", reference),
                       ("sq8", sq8), ("manifest", manifest)):
        authenticate(path, role)
    authority = json.loads(manifest.read_text())
    if (authority.get("geometry") != {"rows": ROWS, "dimensions": DIMS}
            or authority.get("object_sha256") != HASHES["sq8"]
            or authority.get("max_nominees") != 512):
        raise ValueError("SQ8 generation authority differs")
    count = 0
    with plans.open("x") as output:
        for request, exact in itertools.zip_longest(jsonl(requests), jsonl(reference)):
            if request is None or exact is None:
                raise ValueError("request/reference count differs")
            nominees = request.get("nominees")
            primary = exact.get("primary")
            query = request.get("query")
            if (request.get("query_ordinal") != count or exact.get("query_ordinal") != count
                    or not isinstance(nominees, list) or len(nominees) != 512
                    or not isinstance(primary, list) or len(primary) != 100
                    or not isinstance(query, list) or len(query) != DIMS
                    or any(type(value) is not int or not 0 <= value < ROWS
                           for value in nominees + primary)
                    or len(set(nominees)) != 512 or len(set(primary)) != 100
                    or not set(primary).issubset(nominees)
                    or not np.isfinite(np.asarray(query, dtype=np.float32)).all()):
                raise ValueError(f"paired query {count} differs")
            old_votes, old_ranges, old_bytes, old_score = route_reference(
                primary, nominees, rows=ROWS, dimensions=DIMS,
            )
            if (old_votes != exact.get("page_votes")
                    or old_ranges != exact.get("ranges")
                    or old_bytes != exact.get("plan_bytes")
                    or old_score != exact.get("plan_score")):
                raise ValueError(f"exact-primary reproduction differs at {count}")
            _, pq_ranges, pq_bytes, _ = route_reference(
                nominees[:100], nominees, rows=ROWS, dimensions=DIMS,
            )
            check_plan(old_ranges, old_bytes)
            check_plan(pq_ranges, pq_bytes)
            row = {"query_ordinal": count, "exact_ranges": old_ranges,
                   "exact_bytes": old_bytes, "pq_ranges": pq_ranges,
                   "pq_bytes": pq_bytes,
                   "primary_overlap": len(set(primary) & set(nominees[:100]))}
            output.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
            count += 1
    if count != QUERIES:
        raise ValueError("query count differs")
    seal.write_text(json.dumps({
        "schema": "borsuk-v158-plan-seal-v1", "queries": count,
        "gt_opened": False, "input_sha256": {name: HASHES[name] for name in
            ("requests", "reference", "sq8", "manifest")},
        "plans_sha256": sha256(plans)}, sort_keys=True, separators=(",", ":")) + "\n")


def load_truth(path: Path) -> np.ndarray:
    import pyarrow as pa
    import pyarrow.parquet as pq

    authenticate(path, "truth")
    schema = pa.schema([
        pa.field("query", pa.uint32(), nullable=False),
        pa.field("neighbors", pa.list_(pa.field("element", pa.int64(), nullable=False),
                                        100), nullable=False),
    ])
    if pq.read_schema(path) != schema:
        raise ValueError("GT schema differs")
    table = pq.read_table(path)
    if table.num_rows != QUERIES or table["query"].to_pylist() != list(range(QUERIES)):
        raise ValueError("GT query order differs")
    result = np.asarray(table["neighbors"].to_pylist(), dtype=np.int64)
    if (result.shape != (QUERIES, 100)
            or any(len(set(map(int, row))) != 100 for row in result)):
        raise ValueError("GT IDs differ")
    return result


def spread(values: list[int]) -> dict:
    ordered = sorted(values)
    return {"min": ordered[0], "p05": ordered[49], "p50": ordered[499],
            "p95": ordered[949], "p99": ordered[989], "max": ordered[-1],
            "sum": sum(ordered)}


def reduce(requests: Path, plans: Path, seal: Path, sq8_path: Path,
           manifest_path: Path, truth_path: Path, raw: Path, summary: Path) -> None:
    for role, path in (("requests", requests), ("sq8", sq8_path),
                       ("manifest", manifest_path)):
        authenticate(path, role)
    sealed = json.loads(seal.read_text())
    if (sealed.get("schema") != "borsuk-v158-plan-seal-v1"
            or sealed.get("queries") != QUERIES or sealed.get("gt_opened") is not False
            or sealed.get("plans_sha256") != sha256(plans)
            or sealed.get("input_sha256") != {name: HASHES[name] for name in
                ("requests", "reference", "sq8", "manifest")}):
        raise ValueError("GT-blind plan seal differs")
    authority = json.loads(manifest_path.read_text())
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"),
                      ("code", "u1", (DIMS,))])
    sq8 = np.memmap(sq8_path, mode="r", dtype=dtype, shape=(ROWS,))
    ids = np.asarray(sq8["id"], dtype=np.int64)
    physical = {int(identifier): position for position, identifier in enumerate(ids)}
    if len(physical) != ROWS:
        raise ValueError("SQ8 IDs are not unique")
    low = np.asarray(authority["low"], dtype=np.float32)
    step = np.asarray(authority["step"], dtype=np.float32)
    if (low.shape != (DIMS,) or step.shape != (DIMS,)
            or not np.isfinite(low).all() or not np.isfinite(step).all()
            or (step <= 0).any()):
        raise ValueError("SQ8 quantizer differs")
    truth = load_truth(truth_path)
    if any(int(identifier) not in physical for row in truth for identifier in row):
        raise ValueError("GT IDs are absent from SQ8 source")
    hits = {"exact": [], "pq": []}
    coverage = {"exact": [], "pq": []}
    bytes_per = {"exact": [], "pq": []}
    gets = {"exact": [], "pq": []}
    overlap = []
    wins = ties = losses = 0
    with raw.open("x") as output:
        for ordinal, (request, planned) in enumerate(itertools.zip_longest(
                jsonl(requests), jsonl(plans))):
            if request is None or planned is None or any(
                    row["query_ordinal"] != ordinal for row in (request, planned)):
                raise ValueError("replay query order differs")
            query = np.asarray(request["query"], dtype=np.float32)
            if query.shape != (DIMS,) or not np.isfinite(query).all():
                raise ValueError("query vector differs")
            truth_set = set(map(int, truth[ordinal]))
            record = {"query_ordinal": ordinal,
                      "primary_overlap": planned["primary_overlap"], "arms": {}}
            overlap.append(planned["primary_overlap"])
            for arm in ("exact", "pq"):
                ranges = planned[f"{arm}_ranges"]
                amount = planned[f"{arm}_bytes"]
                check_plan(ranges, amount)
                returned = score_sq8_ranges(
                    sq8, query, low, step, ranges, top_k=100,
                )
                if len(returned) != 100 or len(set(returned)) != 100:
                    raise ValueError("returned-ID width differs")
                hit = len(set(returned) & truth_set)
                covered = sum(any(start <= physical[identifier] * dtype.itemsize < end
                                  for start, end in ranges) for identifier in truth_set)
                if hit > covered:
                    raise ValueError("returned hits exceed physical coverage")
                hits[arm].append(hit)
                coverage[arm].append(covered)
                bytes_per[arm].append(amount)
                gets[arm].append(len(ranges))
                record["arms"][arm] = {"returned_ids": returned,
                                       "hits": hit, "physical_coverage": covered,
                                       "bytes": amount, "gets": len(ranges)}
            left, right = hits["pq"][-1], hits["exact"][-1]
            wins += left > right
            ties += left == right
            losses += left < right
            output.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    if len(overlap) != QUERIES:
        raise ValueError("replay query count differs")
    arms = {arm: {"hits": spread(hits[arm]),
                  "physical_coverage": spread(coverage[arm]),
                  "bytes": spread(bytes_per[arm]),
                  "gets": spread(gets[arm])} for arm in hits}
    passes = (arms["pq"]["hits"]["sum"] >= 99_000
              and arms["pq"]["hits"]["sum"] >= arms["exact"]["hits"]["sum"] - 100
              and arms["pq"]["hits"]["p05"] >= 98
              and arms["pq"]["hits"]["p05"] >= arms["exact"]["hits"]["p05"] - 1
              and sum(value < 90 for value in hits["pq"])
                  <= sum(value < 90 for value in hits["exact"]))
    summary.write_text(json.dumps({
        "schema": "borsuk-v158-pq-primary-returned-summary-v1",
        "dataset": "ReLAION-100k", "split": "development-1000-used",
        "queries": QUERIES, "truth_sha256": HASHES["truth"],
        "plan_seal_sha256": sha256(seal), "raw_sha256": sha256(raw),
        "primary_overlap": spread(overlap), "arms": arms,
        "paired": {"pq_wins": wins, "ties": ties, "pq_losses": losses},
        "below_90": {arm: sum(value < 90 for value in hits[arm]) for arm in hits},
        "passes_100k_gate": passes,
    }, sort_keys=True, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("plan", "reduce"))
    for name in ("requests", "reference", "sq8", "manifest", "truth",
                 "plans", "seal", "raw", "summary"):
        parser.add_argument("--" + name, type=Path)
    args = parser.parse_args()
    if args.phase == "plan":
        plan(args.requests, args.reference, args.sq8, args.manifest,
             args.plans, args.seal)
    else:
        reduce(args.requests, args.plans, args.seal, args.sq8,
               args.manifest, args.truth, args.raw, args.summary)


if __name__ == "__main__":
    main()
