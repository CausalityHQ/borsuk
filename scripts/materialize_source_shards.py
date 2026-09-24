"""Stream authenticated embedding shards into a source-only ANN build table."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_shards(staging: dict, root: Path, *, dimensions: int,
                  rows: int) -> list[tuple[Path, dict]]:
    if (staging.get("dataset_id") is None or type(staging.get("objects")) is not list
            or dimensions <= 0 or rows <= 0):
        raise ValueError("source staging identity or geometry differs")
    objects = [item for item in staging["objects"] if item.get("role") == "train"]
    if not objects or sum(item.get("rows", -1) for item in objects) != rows:
        raise ValueError("source shard row count differs")
    objects.sort(key=lambda item: item["uri"])
    names = [item["uri"].rsplit("/", 1)[-1] for item in objects]
    if (len(set(names)) != len(names)
            or any(name != f"train-{index:08d}.parquet"
                   for index, name in enumerate(names))):
        raise ValueError("source shard sequence differs")
    shards = []
    for name, item in zip(names, objects):
        if (type(item.get("rows")) is not int or item["rows"] <= 0
                or type(item.get("bytes")) is not int or item["bytes"] <= 0
                or type(item.get("sha256")) is not str
                or len(item["sha256"]) != 64):
            raise ValueError("source shard authority differs")
        path = root / name
        if path.stat().st_size != item["bytes"] or sha256_file(path) != item["sha256"]:
            raise ValueError(f"source shard digest differs: {name}")
        shards.append((path, item))
    return shards


def materialize(staging_path: Path, root: Path, output: Path,
                provenance_path: Path, *, dimensions: int, rows: int,
                metric: str, batch_rows: int = 16_384) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if (metric not in {"l2", "cosine"} or batch_rows <= 0
            or output.exists() or provenance_path.exists()
            or output == provenance_path):
        raise ValueError("source materialization request differs")
    staging = json.loads(staging_path.read_text())
    shards = source_shards(staging, root, dimensions=dimensions, rows=rows)
    schema = pa.schema([
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("embedding", pa.list_(pa.field("item", pa.float32(), nullable=False),
                                       dimensions), nullable=False),
    ])
    pending = output.with_name(output.name + ".pending")
    if pending.exists():
        raise ValueError("source materialization pending path exists")
    next_id = 0
    try:
        with pq.ParquetWriter(pending, schema, compression="zstd",
                              use_dictionary=False) as writer:
            for path, item in shards:
                parquet = pq.ParquetFile(path)
                if parquet.metadata.num_rows != item["rows"]:
                    raise ValueError(f"source shard row metadata differs: {path.name}")
                field = parquet.schema_arrow.field("emb")
                if (parquet.schema_arrow.names != ["emb"] or field.nullable
                        or not pa.types.is_fixed_size_list(field.type)
                        or field.type.list_size != dimensions
                        or field.type.value_type != pa.float32()
                        or field.type.value_field.nullable):
                    raise ValueError(f"source shard physical schema differs: {path.name}")
                seen = 0
                for batch in parquet.iter_batches(batch_size=batch_rows, columns=["emb"]):
                    embedding = batch.column(0)
                    if embedding.null_count or embedding.values.null_count:
                        raise ValueError("source shard contains null vectors")
                    values = embedding.values.to_numpy(zero_copy_only=False)
                    vectors = np.asarray(values, np.float32).reshape(batch.num_rows, dimensions)
                    if not np.isfinite(vectors).all():
                        raise ValueError("source shard contains nonfinite vectors")
                    if metric == "cosine":
                        norms = np.linalg.norm(vectors, axis=1)
                        if not np.isfinite(norms).all() or (norms <= 0).any():
                            raise ValueError("cosine source contains zero or nonfinite norm")
                        vectors = np.asarray(vectors / norms[:, None], np.float32)
                    start = next_id
                    next_id += batch.num_rows
                    ids = pa.array(np.arange(start, next_id, dtype=np.uint64))
                    flat = pa.array(np.ascontiguousarray(vectors).reshape(-1), type=pa.float32())
                    written = pa.RecordBatch.from_arrays(
                        [ids, pa.FixedSizeListArray.from_arrays(flat, dimensions)],
                        schema=schema,
                    )
                    writer.write_batch(written)
                    seen += batch.num_rows
                if seen != item["rows"]:
                    raise ValueError(f"source shard decoded row count differs: {path.name}")
        if next_id != rows:
            raise ValueError("source materialization total rows differ")
        digest = sha256_file(pending)
        result = {
            "schema": "borsuk-source-shards-v1", "dataset_id": staging["dataset_id"],
            "metric": metric, "dimensions": dimensions, "rows": rows,
            "source_sha256": digest, "source_bytes": pending.stat().st_size,
            "staging_sha256": sha256_file(staging_path),
            "shards": [{"name": path.name, "rows": item["rows"],
                        "sha256": item["sha256"]} for path, item in shards],
            "query_or_truth_used": False,
        }
        pending.replace(output)
        provenance_path.write_text(json.dumps(result, sort_keys=True,
                                              separators=(",", ":")) + "\n")
        return result
    finally:
        pending.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging", required=True, type=Path)
    parser.add_argument("--shard-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--provenance", required=True, type=Path)
    parser.add_argument("--dimensions", required=True, type=int)
    parser.add_argument("--rows", required=True, type=int)
    parser.add_argument("--metric", required=True, choices=("l2", "cosine"))
    args = parser.parse_args()
    print(json.dumps(materialize(
        args.staging, args.shard_root, args.output, args.provenance,
        dimensions=args.dimensions, rows=args.rows, metric=args.metric,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
