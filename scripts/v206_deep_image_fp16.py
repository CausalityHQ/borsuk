#!/usr/bin/env python3
"""Frozen same-range FP16 versus source-float32 Deep-Image diagnostic."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np

from scripts.v121_deep_image_paired import (
    BYTE_CAP, DIMENSIONS, GET_CAP, QUERY_COUNT, ROWS, _valid_ranges,
)

SCHEMA = "borsuk-v206-deep-image-fp16-v1"
ROW_BYTES = DIMENSIONS + 12


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def source_vectors(path: Path) -> np.ndarray:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(path, columns=["feature_row_id", "embedding"])
    kind = table.schema.field("embedding").type
    if (table.num_rows != ROWS or not pa.types.is_fixed_size_list(kind)
            or kind.list_size != DIMENSIONS or kind.value_type != pa.float32()
            or not pa.types.is_integer(table.schema.field("feature_row_id").type)):
        raise ValueError("V206 source schema differs")
    ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if not np.array_equal(ids, np.arange(ROWS, dtype=ids.dtype)):
        raise ValueError("V206 source train IDs differ")
    values = table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False)
    vectors = np.asarray(values, dtype=np.float32).reshape(ROWS, DIMENSIONS)
    if not np.isfinite(vectors).all():
        raise ValueError("V206 source vector is nonfinite")
    return vectors


def top100(vectors: np.ndarray, ids: np.ndarray, query: np.ndarray,
           *, fp16: bool) -> list[int]:
    if ids.size < 100 or np.unique(ids).size != ids.size:
        raise ValueError("V206 fetched ID geometry differs")
    query64 = query.astype(np.float64)
    query64 /= np.linalg.norm(query64)
    scores = np.empty(ids.size, dtype=np.float64)
    for start in range(0, ids.size, 16_384):
        end = min(start + 16_384, ids.size)
        block = vectors[ids[start:end]]
        payload = block.astype(np.float16).astype(np.float64) if fp16 else block.astype(np.float64)
        norms = np.linalg.norm(payload, axis=1)
        if not np.isfinite(norms).all() or (norms <= 0).any():
            raise ValueError("V206 fetched vector norm differs")
        scores[start:end] = (payload @ query64) / norms
    if not np.isfinite(scores).all():
        raise ValueError("V206 score is nonfinite")
    order = np.lexsort((ids, -scores))[:100]
    return [int(item) for item in ids[order]]


def returned(args: argparse.Namespace) -> None:
    import pyarrow.parquet as pq

    if args.raw.exists() or args.seal.exists():
        raise ValueError("V206 return output exists")
    layout = np.load(args.layout, mmap_mode="r", allow_pickle=False)
    if (layout.shape != (ROWS,) or layout.dtype != np.dtype("int64")
            or not np.array_equal(np.sort(layout), np.arange(ROWS, dtype=np.int64))):
        raise ValueError("V206 physical layout differs")
    vectors = source_vectors(args.source)
    queries, plans = records(args.queries), records(args.replay)
    if len(queries) != QUERY_COUNT or len(plans) != QUERY_COUNT:
        raise ValueError("V206 query or plan count differs")
    with args.raw.open("x") as output:
        for ordinal, (request, plan) in enumerate(zip(queries, plans, strict=True)):
            if request.get("query_ordinal") != ordinal or plan.get("query_ordinal") != ordinal:
                raise ValueError(f"V206 query identity differs: {ordinal}")
            query = np.asarray(request["query"], dtype=np.float32)
            ranges, charged = plan["ranges"], plan["plan_bytes"]
            if (query.shape != (DIMENSIONS,) or not np.isfinite(query).all()
                    or np.linalg.norm(query) <= 0
                    or not _valid_ranges(ranges, charged)
                    or len(ranges) > GET_CAP or charged > BYTE_CAP
                    or any(start % ROW_BYTES or end % ROW_BYTES for start, end in ranges)):
                raise ValueError(f"V206 query or range differs: {ordinal}")
            physical = np.concatenate([
                np.arange(start // ROW_BYTES, end // ROW_BYTES, dtype=np.int64)
                for start, end in ranges
            ])
            ids = np.asarray(layout[physical], dtype=np.int64)
            fp16_ids = top100(vectors, ids, query, fp16=True)
            f32_ids = top100(vectors, ids, query, fp16=False)
            sq8_ids = plan["returned_ids"]
            if (len(sq8_ids) != 100 or len(set(sq8_ids)) != 100
                    or not set(sq8_ids).issubset(set(map(int, ids)))):
                raise ValueError(f"V206 V121 SQ8 witness differs: {ordinal}")
            output.write(canonical({"ordinal": ordinal,
                                    "fp16_ids": fp16_ids,
                                    "f32_ids": f32_ids,
                                    "sq8_ids": sq8_ids,
                                    "ranges": ranges,
                                    "gets": len(ranges), "bytes": charged}))
    args.seal.write_text(canonical({
        "schema": SCHEMA + "-pretruth-seal",
        "source_truth_opened": False,
        "dataset": "deep-image-96-angular",
        "split": "publication-test-first-1000-already-used",
        "source_sha256": sha256(args.source),
        "layout_sha256": sha256(args.layout),
        "queries_sha256": sha256(args.queries),
        "replay_sha256": sha256(args.replay),
        "raw_sha256": sha256(args.raw),
    }))


def score(args: argparse.Namespace) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if args.evidence.exists() or args.summary.exists():
        raise ValueError("V206 score output exists")
    seal = json.loads(args.seal.read_text())
    if (seal.get("schema") != SCHEMA + "-pretruth-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("raw_sha256") != sha256(args.raw)):
        raise ValueError("V206 pretruth seal differs")
    rows = records(args.raw)
    table = pq.read_table(args.truth, columns=["neighbors_id"])
    kind = table.schema.field("neighbors_id").type
    if table.num_rows < QUERY_COUNT or not (pa.types.is_list(kind)
                                           or pa.types.is_fixed_size_list(kind)):
        raise ValueError("V206 truth schema differs")
    truth = table["neighbors_id"].slice(0, QUERY_COUNT).to_pylist()
    if len(rows) != QUERY_COUNT:
        raise ValueError("V206 raw count differs")
    hits = {arm: [] for arm in ("fp16", "f32", "sq8")}
    with args.evidence.open("x") as output:
        for ordinal, (row, neighbors) in enumerate(zip(rows, truth, strict=True)):
            if (row["ordinal"] != ordinal or len(neighbors) < 100
                    or len(set(neighbors[:100])) != 100):
                raise ValueError(f"V206 truth identity differs: {ordinal}")
            gold = set(neighbors[:100])
            record = {"ordinal": ordinal}
            for arm in hits:
                ids = row[f"{arm}_ids"]
                if len(ids) != 100 or len(set(ids)) != 100:
                    raise ValueError(f"V206 returned IDs differ: {ordinal} {arm}")
                count = len(gold.intersection(ids))
                hits[arm].append(count)
                record[f"{arm}_hits"] = count
            output.write(canonical(record))
    totals = {arm: sum(values) for arm, values in hits.items()}
    p05 = {arm: sorted(values)[49] for arm, values in hits.items()}
    sub90 = {arm: sum(hit < 90 for hit in values) for arm, values in hits.items()}
    fp16, sq8 = hits["fp16"], hits["sq8"]
    verdict = (totals["fp16"] >= 99_000 and p05["fp16"] >= 90
               and totals["fp16"] > totals["sq8"]
               and sum(row["gets"] for row in rows) == sum(len(row["ranges"]) for row in rows)
               and all(row["gets"] <= GET_CAP and row["bytes"] <= BYTE_CAP for row in rows))
    args.summary.write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "deep-image-96-angular",
        "split": "publication-test-first-1000-already-used",
        "queries": QUERY_COUNT, "hits": totals, "p05_hits": p05,
        "below_90": sub90,
        "fp16_vs_sq8_wins": sum(a > b for a, b in zip(fp16, sq8, strict=True)),
        "fp16_vs_sq8_ties": sum(a == b for a, b in zip(fp16, sq8, strict=True)),
        "fp16_vs_sq8_losses": sum(a < b for a, b in zip(fp16, sq8, strict=True)),
        "planned_gets": sum(row["gets"] for row in rows),
        "planned_bytes": sum(row["bytes"] for row in rows),
        "raw_sha256": sha256(args.raw), "truth_sha256": sha256(args.truth),
        "passes_representation_screen": verdict,
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("return", "score"))
    for name in ("source", "layout", "queries", "replay", "raw", "seal",
                 "truth", "evidence", "summary"):
        parser.add_argument(f"--{name}", type=Path)
    args = parser.parse_args()
    if args.phase == "return":
        returned(args)
    else:
        score(args)


if __name__ == "__main__":
    main()
