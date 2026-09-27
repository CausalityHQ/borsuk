"""Prepare query-blind 100k source Parquet, then separate exact GT100."""

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROWS = 100_000
DIMS = 768


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def topk_ids(scores, k):
    cutoff = np.partition(scores, len(scores) - k)[len(scores) - k]
    better = np.flatnonzero(scores > cutoff)
    tied = np.flatnonzero(scores == cutoff)[:k - len(better)]
    selected = np.concatenate((better, tied))
    return selected[np.lexsort((selected, -scores[selected]))]


def vectors_from_input(path, kind):
    if kind == "raw":
        if path.stat().st_size != ROWS * DIMS * 4:
            raise ValueError("raw source geometry differs")
        vectors = np.fromfile(path, dtype="<f4").reshape(ROWS, DIMS)
    else:
        source = pq.ParquetFile(path)
        blocks = []
        remaining = ROWS
        for batch in source.iter_batches(batch_size=8192, columns=["feature_row_id", "embedding"]):
            table = pa.Table.from_batches([batch])
            embedding = table.schema.field("embedding").type
            if (not pa.types.is_fixed_size_list(embedding) or embedding.list_size != DIMS
                    or embedding.value_type != pa.float32()):
                raise ValueError("Parquet source geometry differs")
            count = min(remaining, table.num_rows)
            ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)[:count]
            if not np.array_equal(ids, np.arange(ROWS - remaining, ROWS - remaining + count)):
                raise ValueError("Parquet source IDs differ")
            values = table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False)
            blocks.append(np.asarray(values, np.float32).reshape(table.num_rows, DIMS)[:count])
            remaining -= count
            if not remaining:
                break
        if remaining:
            raise ValueError("Parquet source shorter than 100k")
        vectors = np.concatenate(blocks)
    norms = np.linalg.norm(vectors, axis=1)
    if not np.isfinite(vectors).all() or not (norms > 0).all():
        raise ValueError("source vectors invalid")
    return np.ascontiguousarray(vectors / norms[:, None], dtype=np.float32)


def source(input_path, input_sha, kind, output):
    if output.exists() or digest(input_path) != input_sha:
        raise ValueError("source identity or output differs")
    vectors = vectors_from_input(input_path, kind)
    with tempfile.TemporaryDirectory(prefix=output.name + ".tmp-", dir=output.parent) as temporary:
        pending = Path(temporary)
        ids = pa.array(np.arange(ROWS, dtype=np.int64))
        data = pa.FixedSizeListArray.from_arrays(pa.array(vectors.ravel()), DIMS)
        path = pending / "source.parquet"
        pq.write_table(pa.table({"feature_row_id": ids, "embedding": data}), path,
                       compression="zstd")
        provenance = {
            "schema": "borsuk-source-shards-v1", "metric": "cosine",
            "rows": ROWS, "dimensions": DIMS, "source_bytes": path.stat().st_size,
            "source_sha256": digest(path), "query_or_truth_used": False,
            "input_sha256": input_sha, "input_kind": kind,
        }
        (pending / "provenance.json").write_text(
            json.dumps(provenance, sort_keys=True, separators=(",", ":")) + "\n"
        )
        pending.rename(output)
    path = output / "source.parquet"
    print(json.dumps({"source_sha256": digest(path),
                      "provenance_sha256": digest(output / "provenance.json")}, sort_keys=True))


def truth(source_path, source_sha, requests, requests_sha, output):
    if (output.exists() or digest(source_path) != source_sha
            or digest(requests) != requests_sha):
        raise ValueError("truth input identity or output differs")
    table = pq.read_table(source_path, columns=["embedding"])
    corpus = np.asarray(table["embedding"].combine_chunks().values.to_numpy(
        zero_copy_only=False), np.float64).reshape(ROWS, DIMS)
    corpus /= np.linalg.norm(corpus, axis=1)[:, None]
    queries = []
    for ordinal, line in enumerate(requests.read_text().splitlines()):
        row = json.loads(line)
        if row.get("query_ordinal", row.get("ordinal")) != ordinal:
            raise ValueError("query ordinal differs")
        query = np.asarray(row["query"], np.float64)
        if query.shape != (DIMS,) or not np.isfinite(query).all() or not np.any(query):
            raise ValueError("query geometry differs")
        queries.append(query / np.linalg.norm(query))
    if len(queries) != 1000:
        raise ValueError("expected 1,000 queries")
    with output.open("xb") as target:
        for first in range(0, len(queries), 16):
            scores = corpus @ np.asarray(queries[first:first + 16], np.float64).T
            for column in range(scores.shape[1]):
                ordered = topk_ids(scores[:, column], 100)
                target.write(ordered.astype("<u4").tobytes())
    print(json.dumps({"truth_sha256": digest(output), "bytes": output.stat().st_size},
                     sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    build = actions.add_parser("source")
    build.add_argument("--input", type=Path, required=True)
    build.add_argument("--input-sha", required=True)
    build.add_argument("--kind", choices=("raw", "parquet"), required=True)
    build.add_argument("--output", type=Path, required=True)
    score = actions.add_parser("truth")
    score.add_argument("--source", type=Path, required=True)
    score.add_argument("--source-sha", required=True)
    score.add_argument("--requests", type=Path, required=True)
    score.add_argument("--requests-sha", required=True)
    score.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "source":
        source(args.input, args.input_sha, args.kind, args.output)
    else:
        truth(args.source, args.source_sha, args.requests, args.requests_sha, args.output)
