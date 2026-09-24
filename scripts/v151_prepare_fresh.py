#!/usr/bin/env python3
"""Seal fresh D96 queries, GT100 and source-only primary witnesses."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v114_1m_paired import prepare_paired_query
from scripts.v115_source_router import load_source_router
from scripts.v122_subset_screen import exact_subset_truth

FIRST = 3000
COUNT = 1000
SOURCE_SHA = "da3ad1295d6031818b7ccb817529c6102e9f0ec93bb4ad0792e2b7ab3cd21e69"
TEST_SHA = "296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4"
SQ8_SHA = "c20dcb8058d2409791c6c584d9f078491d4c239acbe7c19350be7757d533e8df"
LAYOUT_SHA = "8b23fb6d2f76704394733f5540f25e36db961386b568495b7bfd73fc52d60aea"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--test", type=Path, required=True)
    parser.add_argument("--built", type=Path, required=True)
    parser.add_argument("--router", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (args.output.exists() or sha256(args.source) != SOURCE_SHA
            or sha256(args.test) != TEST_SHA
            or sha256(args.built / "sq8.bin") != SQ8_SHA
            or sha256(args.built / "layout.npy") != LAYOUT_SHA):
        raise ValueError("fresh input identity differs")
    built = json.loads((args.built / "manifest.json").read_text())
    router, planes = load_source_router(args.router)
    if (built["source_sha256"] != SOURCE_SHA
            or built["sq8_sha256"] != SQ8_SHA
            or built["layout_sha256"] != LAYOUT_SHA
            or router["source_sha256"] != SOURCE_SHA
            or router["sq8_sha256"] != SQ8_SHA
            or router["layout_sha256"] != LAYOUT_SHA):
        raise ValueError("source, layout or router binding differs")
    source_table = pq.read_table(args.source, columns=["feature_row_id", "embedding"])
    test_table = pq.read_table(args.test, columns=["emb"])
    if (source_table.num_rows != 100_000 or test_table.num_rows != 10_000
            or not pa.types.is_fixed_size_list(source_table.schema.field("embedding").type)
            or source_table.schema.field("embedding").type.list_size != 96
            or not pa.types.is_fixed_size_list(test_table.schema.field("emb").type)
            or test_table.schema.field("emb").type.list_size != 96):
        raise ValueError("fresh source/query geometry differs")
    ids = source_table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if not np.array_equal(ids, np.arange(100_000, dtype=ids.dtype)):
        raise ValueError("subset source IDs differ")
    corpus = np.asarray(
        source_table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False),
        np.float32,
    ).reshape(100_000, 96)
    if (not np.isfinite(corpus).all()
            or not np.allclose(np.linalg.norm(corpus, axis=1), 1.0,
                               atol=1e-4, rtol=0)):
        raise ValueError("subset source unit vectors differ")
    all_queries = np.asarray(
        test_table["emb"].combine_chunks().values.to_numpy(zero_copy_only=False),
        np.float32,
    ).reshape(10_000, 96)
    selected = all_queries[FIRST:FIRST + COUNT]
    if not np.isfinite(selected).all():
        raise ValueError("fresh query is nonfinite")
    norms = np.linalg.norm(selected, axis=1)
    if (norms <= 0).any():
        raise ValueError("fresh query contains zero vector")
    queries = np.ascontiguousarray(selected / norms[:, None])
    truth = exact_subset_truth(corpus, queries, 100)
    layout = np.load(args.built / "layout.npy", allow_pickle=False)
    if (layout.shape != (100_000,)
            or not np.array_equal(np.sort(layout), np.arange(100_000))):
        raise ValueError("layout permutation differs")
    record = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (96,))])
    sq8 = np.memmap(args.built / "sq8.bin", dtype=record, mode="r", shape=(100_000,))
    if not np.array_equal(sq8["id"], layout):
        raise ValueError("SQ8 row layout differs")
    regions = (((100_000 + 255) // 256) * 1024 + 3906) // 3907
    with tempfile.TemporaryDirectory(prefix=args.output.name + ".tmp-",
                                     dir=args.output.parent) as temporary:
        pending = Path(temporary)
        np.save(pending / "truth.npy", truth, allow_pickle=False)
        with (pending / "queries.jsonl").open("w") as query_file, \
                (pending / "routing.jsonl").open("w") as route_file:
            for ordinal, query in enumerate(queries):
                request, reference = prepare_paired_query(
                    ordinal, query, planes["summaries"], planes["books"], planes["codes"],
                    sq8, planes["low"], planes["step"], page_rows=256,
                    blocks_per_page=2, regions=regions, shortlist=512, primary_count=100,
                )
                query_file.write(json.dumps({
                    "query_ordinal": ordinal, "source_query_ordinal": FIRST + ordinal,
                    "query": query.astype(float).tolist(),
                }, sort_keys=True, separators=(",", ":")) + "\n")
                route_file.write(json.dumps({
                    "query_ordinal": ordinal, "source_query_ordinal": FIRST + ordinal,
                    "primary": reference["primary"], "nominees": request["nominees"],
                }, sort_keys=True, separators=(",", ":")) + "\n")
        manifest = {
            "schema": "borsuk-v151-fresh-inputs-v1",
            "source_query_start": FIRST, "query_count": COUNT,
            "source_sha256": SOURCE_SHA, "test_sha256": TEST_SHA,
            "sq8_sha256": SQ8_SHA, "layout_sha256": LAYOUT_SHA,
            "queries_sha256": sha256(pending / "queries.jsonl"),
            "routing_sha256": sha256(pending / "routing.jsonl"),
            "truth_sha256": sha256(pending / "truth.npy"),
            "truth_read_by_router": False,
        }
        (pending / "manifest.json").write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
        )
        pending.rename(args.output)
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
