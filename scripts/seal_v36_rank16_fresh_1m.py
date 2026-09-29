"""AWS-only construction of the fixed rank16 query panel and exact 1M GT."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.audit_v36_ranked_physical_ids import acquire, digest
from scripts.run_native_source_frontier_1m import oracle, self_check


def checked_s3(bucket, ident, path):
    subprocess.run(["aws", "s3", "cp", f"s3://{bucket}/{ident['key']}", str(path),
                    "--only-show-errors"], check=True)
    if digest(path) != (ident["bytes"], ident["sha256"]):
        raise ValueError(f"source identity differs: {path}")


def selected_vectors(panel, registry, scratch):
    ranked = sorted(registry, key=lambda shard: (
        hashlib.sha256(b"borsuk-v36-screen-object-v1" + shard["path"].encode()
                       + shard["encoded_bytes"].to_bytes(8, "little")).digest(),
        shard["path"].encode()))
    assert len(ranked) == 2298
    wanted = {}
    for row in panel["selected"]:
        assert row["source_rank"] in range(16, 32)
        wanted.setdefault(row["source_rank"], {})[row["source_row_offset"]] = row
    vectors = np.empty((1000, 768), dtype="<f4")
    found = set()
    sources = []
    for rank, offsets in sorted(wanted.items()):
        shard = ranked[rank]
        path = acquire(shard, scratch)
        sources.append({"rank": rank, "path": shard["path"],
                        "bytes": shard["encoded_bytes"], "sha256": shard["sha256"]})
        parquet = pq.ParquetFile(path)
        if parquet.schema_arrow.field("feature_row_id").type != pa.int64():
            raise ValueError("candidate feature ID type differs")
        start = 0
        for batch in parquet.iter_batches(columns=["feature_row_id", "embedding"], batch_size=8192):
            for offset in sorted(k for k in offsets if start <= k < start + len(batch)):
                row = offsets[offset]
                if batch.column(0)[offset - start].as_py() != row["feature_row_id"]:
                    raise ValueError("selected feature ID/locator differs")
                vector = np.asarray(batch.column(1)[offset - start].as_py(), dtype="<f4")
                if vector.shape != (768,) or not np.isfinite(vector).all() or not vector.any():
                    raise ValueError("selected query geometry differs")
                vectors[row["query_ordinal"]] = vector
                found.add(row["query_ordinal"])
            start += len(batch)
        if start != parquet.metadata.num_rows or max(offsets) >= start:
            raise ValueError("selected source row coverage differs")
        path.unlink()
    if found != set(range(1000)):
        raise ValueError("selected query roster differs")
    return vectors, sources


def source_raw(parquet_path, output, expected_sha):
    parquet = pq.ParquetFile(parquet_path)
    if parquet.metadata.num_rows != 1_000_000:
        raise ValueError("indexed source row count differs")
    ids = set()
    with output.open("xb") as target:
        for batch in parquet.iter_batches(columns=["feature_row_id", "embedding"], batch_size=8192):
            values = batch.column(0).to_pylist()
            if any(type(v) is not int or v < 0 for v in values):
                raise ValueError("indexed source feature ID differs")
            ids.update(values)
            column = batch.column(1)
            if (not pa.types.is_fixed_size_list(column.type) or column.type.list_size != 768
                    or column.type.value_type != pa.float32() or column.null_count
                    or column.values.null_count):
                raise ValueError("indexed source vector schema differs")
            data = np.asarray(column.values.to_numpy(zero_copy_only=False),
                              dtype="<f4").reshape(len(batch), 768)
            if not np.isfinite(data).all() or not (data != 0).any(axis=1).all():
                raise ValueError("indexed source vector differs")
            target.write(data.tobytes())
    if len(ids) != 1_000_000 or digest(output) != (3_072_000_000, expected_sha):
        raise ValueError("indexed source raw parity differs")


def seal(bucket, prefix, path):
    ident = {"key": f"{prefix}/sealed/{path.name}", "bytes": path.stat().st_size,
             "sha256": digest(path)[1]}
    subprocess.run(["aws", "s3api", "put-object", "--bucket", bucket,
                    "--key", ident["key"], "--body", str(path), "--if-none-match", "*",
                    "--metadata", "sha256=" + ident["sha256"]], check=True,
                   stdout=subprocess.DEVNULL)
    return ident


def main():
    config_path, expected_config_sha, repo_arg, out_arg, prefix = sys.argv[1:]
    repo, out = Path(repo_arg), Path(out_arg)
    if digest(Path(config_path))[1] != expected_config_sha:
        raise ValueError("configuration identity differs")
    config = json.loads(Path(config_path).read_text())
    if (config["schema"] != "borsuk-v36-rank16-fresh-1m-seal-v1"
            or config["quality_peek_allowed"] is not False
            or np.__version__ != "2.3.3" or pa.__version__ != "24.0.0"):
        raise ValueError("frozen construction scope differs")
    for name, expected in config["code_sha256"].items():
        if digest(repo / name)[1] != expected:
            raise ValueError(f"frozen scorer or input code differs: {name}")
    panel_path = repo / config["panel_path"]
    registry_path = repo / config["registry_path"]
    if digest(panel_path)[1] != config["panel_sha256"] or digest(registry_path)[1] != config["registry_sha256"]:
        raise ValueError("frozen panel or registry differs")
    panel, registry = json.loads(panel_path.read_text()), json.loads(registry_path.read_text())
    if (len(panel["selected"]) != 1000 or panel["development_ordinals"] != [0, 63]
            or panel["sealed_ordinals"] != [64, 999]
            or [r["query_ordinal"] for r in panel["selected"]] != list(range(1000))
            or len({r["feature_row_id"] for r in panel["selected"]}) != 1000):
        raise ValueError("frozen selected query panel differs")
    out.mkdir()
    started = time.monotonic()
    checked_s3(config["bucket"], config["source_parquet"], out / "source.parquet")
    source_raw(out / "source.parquet", out / "source.raw", config["source_raw_sha256"])
    queries, sources = selected_vectors(panel, registry, out)
    queries.tofile(out / "queries.raw")
    with (out / "requests.jsonl").open("x") as stream:
        for q, vector in enumerate(queries):
            stream.write(json.dumps({"query_ordinal": q, "query": vector.tolist()},
                                    separators=(",", ":"), allow_nan=False) + "\n")
    self_check()
    truth = oracle(out / "source.raw", queries, 1_000_000)
    if truth.shape != (1000, 100) or truth.dtype != np.dtype("<u4"):
        raise ValueError("exact ground truth geometry differs")
    truth.tofile(out / "truth.u32")
    artifacts = {name: seal(config["bucket"], prefix, out / name)
                 for name in ("queries.raw", "requests.jsonl", "truth.u32")}
    result = {"decision": "PASS sealed query and exact-GT construction only",
              "qualification": False, "ann_quality_measured": False,
              "dataset": "ReLAION FIRST1M D768 cosine",
              "query_split": "rank16 quality-blind0-999; development0-63, prospective64-999",
              "indexed_source_sha256": config["source_parquet"]["sha256"],
              "indexed_raw_sha256": config["source_raw_sha256"],
              "panel_sha256": config["panel_sha256"],
              "authenticated_query_sources": sources,
              "oracle": "existing exhaustive f64 cosine block-sort merge; signed source ordinal ties",
              "oracle_self_check": True, "queries": 1000, "gt_k": 100,
              "artifacts": artifacts, "wall_seconds": time.monotonic() - started}
    (out / "decision.json").write_text(json.dumps(result, sort_keys=True) + "\n")


if __name__ == "__main__":
    try:
        main()
    finally:
        if len(sys.argv) == 6 and Path(sys.argv[4]).is_dir():
            group = Path("/sys/fs/cgroup") / Path("/proc/self/cgroup").read_text().strip().split("0::")[-1].lstrip("/")
            (Path(sys.argv[4]) / "cgroup.json").write_text(json.dumps({
                name: (group / name).read_text() for name in
                ("memory.max", "memory.peak", "memory.swap.peak", "memory.events", "cpu.stat")
                if (group / name).exists()}, sort_keys=True) + "\n")
