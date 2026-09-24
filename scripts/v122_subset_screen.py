"""Source-only 100k D96 subset and exact held-out development GT helpers."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

import numpy as np


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(4 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def make_subset(
    source: Path, output_dir: Path, *, source_sha256: str,
    sample_rows: int = 100_000, seed: int = 122001,
) -> dict:
    """Select train ordinals only; queries and GT are absent from this API."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if (output_dir.exists() or type(sample_rows) is not int or sample_rows <= 0
            or type(seed) is not int or seed < 0
            or _sha256_file(source) != source_sha256):
        raise ValueError("subset source identity or configuration differs")
    table = pq.read_table(source, columns=["feature_row_id", "embedding"])
    rows = table.num_rows
    field = table.schema.field("embedding")
    if (sample_rows > rows or not pa.types.is_fixed_size_list(field.type)
            or field.type.list_size != 96
            or field.type.value_type != pa.float32()):
        raise ValueError("subset source geometry differs")
    ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if not np.array_equal(ids, np.arange(rows, dtype=ids.dtype)):
        raise ValueError("subset source IDs differ")
    selection = np.sort(np.random.default_rng(seed).choice(
        rows, sample_rows, replace=False,
    )).astype(np.int64)
    values = table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False)
    corpus = np.asarray(values, np.float32).reshape(rows, 96)
    chosen = np.ascontiguousarray(corpus[selection])
    if (not np.isfinite(chosen).all()
            or not np.allclose(np.linalg.norm(chosen, axis=1), 1.0,
                               atol=1e-4, rtol=0)):
        raise ValueError("subset source unit vectors differ")
    with tempfile.TemporaryDirectory(prefix=output_dir.name + ".tmp-",
                                     dir=output_dir.parent) as temporary:
        pending = Path(temporary)
        np.save(pending / "original_ids.npy", selection, allow_pickle=False)
        subset = pa.table({
            "feature_row_id": pa.array(np.arange(sample_rows, dtype=np.uint64)),
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array(chosen.ravel()), 96,
            ),
        })
        pq.write_table(subset, pending / "source.parquet", compression="zstd")
        subset_sha = _sha256_file(pending / "source.parquet")
        mapping_sha = _sha256_file(pending / "original_ids.npy")
        provenance = {
            "schema": "borsuk-source-shards-v1",
            "dataset_id": "deep-image-96-development-subset",
            "metric": "cosine", "rows": sample_rows, "dimensions": 96,
            "source_bytes": (pending / "source.parquet").stat().st_size,
            "source_sha256": subset_sha,
            "parent_source_sha256": source_sha256,
            "original_ids_sha256": mapping_sha,
            "query_or_truth_used": False,
        }
        (pending / "source.json").write_text(
            json.dumps(provenance, sort_keys=True, separators=(",", ":")) + "\n"
        )
        manifest = {
            "schema": "borsuk-v122-source-subset-v1",
            "source_rows": rows, "subset_rows": sample_rows,
            "sample_seed": seed, "parent_source_sha256": source_sha256,
            "subset_source_sha256": subset_sha,
            "subset_provenance_sha256": _sha256_file(pending / "source.json"),
            "original_ids_sha256": mapping_sha,
            "query_or_truth_used": False,
        }
        (pending / "manifest.json").write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
        )
        pending.rename(output_dir)
    return manifest


def exact_subset_truth(
    corpus: np.ndarray, queries: np.ndarray, k: int = 100,
) -> np.ndarray:
    """Float32 cosine top-k, deterministic score/ID ties within a sealed subset."""
    if (corpus.ndim != 2 or queries.ndim != 2
            or corpus.dtype != np.float32 or queries.dtype != np.float32
            or corpus.shape[1] != queries.shape[1]
            or not 0 < k <= corpus.shape[0]
            or not np.isfinite(corpus).all() or not np.isfinite(queries).all()):
        raise ValueError("subset exact GT geometry differs")
    truth = np.empty((queries.shape[0], k), dtype=np.int64)
    for start in range(0, queries.shape[0], 32):
        scores = queries[start:start + 32] @ corpus.T
        for offset, row in enumerate(scores):
            threshold = np.partition(row, -k)[-k]
            above = np.flatnonzero(row > threshold)
            tied = np.flatnonzero(row == threshold)[:k - above.size]
            admitted = np.concatenate((above, tied))
            ordered = np.lexsort((admitted, -row[admitted]))
            truth[start + offset] = admitted[ordered]
    return truth


def covered_truth_count(
    truth_ids: np.ndarray, source_to_physical: np.ndarray,
    ranges: list[list[int]], *, row_bytes: int,
) -> int:
    """Count exact GT IDs whose physical SQ8 records lie in fetched ranges."""
    if (truth_ids.ndim != 1 or source_to_physical.ndim != 1
            or row_bytes <= 0 or (truth_ids < 0).any()
            or (truth_ids >= source_to_physical.size).any()):
        raise ValueError("subset truth or layout geometry differs")
    offsets = source_to_physical[truth_ids] * row_bytes
    covered = np.zeros(truth_ids.size, dtype=bool)
    for start, end in ranges:
        if start < 0 or start >= end or end > source_to_physical.size * row_bytes:
            raise ValueError("subset physical range differs")
        covered |= (offsets >= start) & (offsets < end)
    return int(covered.sum())


def prepare_development(
    subset_source: Path, queries: Path, output_queries: Path,
    output_truth: Path, *, subset_source_sha256: str,
) -> None:
    """Use only held-out ordinals 9000..9999 after the subset index is sealed."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if _sha256_file(subset_source) != subset_source_sha256:
        raise ValueError("subset source SHA-256 differs")
    source = pq.read_table(subset_source, columns=["feature_row_id", "embedding"])
    test = pq.read_table(queries, columns=["emb"])
    if (source.num_rows != 100_000 or test.num_rows != 10_000
            or not pa.types.is_fixed_size_list(source.schema.field("embedding").type)
            or source.schema.field("embedding").type.list_size != 96
            or not pa.types.is_fixed_size_list(test.schema.field("emb").type)
            or test.schema.field("emb").type.list_size != 96):
        raise ValueError("subset or development query geometry differs")
    ids = source["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if not np.array_equal(ids, np.arange(100_000, dtype=ids.dtype)):
        raise ValueError("subset source IDs differ")
    corpus = np.asarray(
        source["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False),
        np.float32,
    ).reshape(100_000, 96)
    all_queries = np.asarray(
        test["emb"].combine_chunks().values.to_numpy(zero_copy_only=False),
        np.float32,
    ).reshape(10_000, 96)
    selected = all_queries[9_000:10_000]
    if not np.isfinite(selected).all():
        raise ValueError("development query is nonfinite")
    norms = np.linalg.norm(selected, axis=1)
    if (norms <= 0).any():
        raise ValueError("development query contains zero vector")
    normalized = np.ascontiguousarray(selected / norms[:, None])
    truth = exact_subset_truth(corpus, normalized, 100)
    with output_queries.open("x") as dest:
        for ordinal, vector in enumerate(normalized):
            dest.write(json.dumps({"query_ordinal": ordinal,
                "source_query_ordinal": 9_000 + ordinal,
                "query": vector.astype(float).tolist()},
                sort_keys=True, separators=(",", ":")) + "\n")
    if output_truth.exists():
        raise ValueError("development GT output already exists")
    np.save(output_truth, truth, allow_pickle=False)


def evaluate_development(
    built_dir: Path, router_dir: Path, queries: Path, truth: Path,
    evidence: Path, summary: Path,
) -> None:
    """Run one paired SQ8 returned-quality screen without shipped GT."""
    from scripts.v114_1m_paired import prepare_paired_query, score_sq8_ranges
    from scripts.v115_source_router import load_source_router

    built = json.loads((built_dir / "manifest.json").read_text())
    router, planes = load_source_router(router_dir)
    rows, dimensions = built["rows"], built["dimensions"]
    if (rows != 100_000 or dimensions != 96
            or router["geometry"] != {"rows": rows, "dimensions": dimensions,
                "page_rows": 256, "blocks_per_page": 2, "subspaces": 64,
                "pq_width": 2, "pq_partition": "balanced_floor_v1"}
            or router["source_sha256"] != built["source_sha256"]
            or router["layout_sha256"] != built["layout_sha256"]
            or router["sq8_sha256"] != built["sq8_sha256"]
            or _sha256_file(built_dir / "layout.npy") != built["layout_sha256"]
            or _sha256_file(built_dir / "sq8.bin") != built["sq8_sha256"]):
        raise ValueError("development index binding differs")
    layout = np.load(built_dir / "layout.npy", allow_pickle=False)
    if (layout.shape != (rows,)
            or not np.array_equal(np.sort(layout), np.arange(rows))):
        raise ValueError("development layout permutation differs")
    source_to_physical = np.empty(rows, dtype=np.int64)
    source_to_physical[layout] = np.arange(rows)
    record = np.dtype([("id", "<i8"), ("norm", "<f4"),
                       ("code", "u1", (dimensions,))])
    sq8 = np.memmap(built_dir / "sq8.bin", dtype=record, mode="r", shape=(rows,))
    if not np.array_equal(sq8["id"], layout):
        raise ValueError("development SQ8 row IDs differ")
    gold = np.load(truth, allow_pickle=False)
    requests = [json.loads(line) for line in queries.read_text().splitlines()]
    if (gold.shape != (1_000, 100) or len(requests) != 1_000
            or gold.min() < 0 or gold.max() >= rows):
        raise ValueError("development query or GT count differs")
    regions = (((rows + 255) // 256) * 1024 + 3906) // 3907
    hits = {"candidate": [], "baseline": []}
    cover = {"candidate": [], "baseline": []}
    maximum_gets = {"candidate": 0, "baseline": 0}
    maximum_bytes = {"candidate": 0, "baseline": 0}
    with evidence.open("x") as dest:
        for ordinal, (request, truth_ids) in enumerate(zip(requests, gold)):
            if (request.get("query_ordinal") != ordinal
                    or request.get("source_query_ordinal") != 9_000 + ordinal
                    or len(set(map(int, truth_ids))) != 100):
                raise ValueError(f"development query/GT identity differs at {ordinal}")
            query = np.asarray(request["query"], dtype=np.float32)
            if query.shape != (dimensions,) or not np.isfinite(query).all():
                raise ValueError(f"development query geometry differs at {ordinal}")
            prepared, reference = prepare_paired_query(
                ordinal, query, planes["summaries"], planes["books"],
                planes["codes"], sq8, planes["low"], planes["step"],
                page_rows=256, blocks_per_page=2, regions=regions,
                shortlist=512, primary_count=100,
            )
            row = {"query_ordinal": ordinal,
                   "source_query_ordinal": 9_000 + ordinal,
                   "truth_ids": truth_ids.astype(int).tolist(),
                   "nominees": prepared["nominees"],
                   "primary": reference["primary"]}
            for arm, ranges_key, bytes_key in (
                ("candidate", "ranges", "plan_bytes"),
                ("baseline", "baseline_ranges", "baseline_bytes"),
            ):
                ranges = reference[ranges_key]
                nbytes = reference[bytes_key]
                if (not 1 <= len(ranges) <= 32
                        or nbytes > 16_777_216
                        or nbytes != sum(end - start for start, end in ranges)):
                    raise ValueError(f"development {arm} cap differs at {ordinal}")
                returned = score_sq8_ranges(
                    sq8, query, planes["low"], planes["step"], ranges, top_k=100,
                )
                if len(returned) != 100 or len(set(returned)) != 100:
                    raise ValueError(f"development {arm} returned IDs differ at {ordinal}")
                hit = len(set(returned).intersection(map(int, truth_ids)))
                coverage = covered_truth_count(
                    truth_ids, source_to_physical, ranges, row_bytes=record.itemsize,
                )
                if hit > coverage:
                    raise ValueError(f"development {arm} exceeds physical upper bound")
                hits[arm].append(hit)
                cover[arm].append(coverage)
                maximum_gets[arm] = max(maximum_gets[arm], len(ranges))
                maximum_bytes[arm] = max(maximum_bytes[arm], nbytes)
                row[f"{arm}_ranges"] = ranges
                row[f"{arm}_returned_ids"] = returned
                row[f"{arm}_hits"] = hit
                row[f"{arm}_physical_coverage"] = coverage
                row[f"{arm}_gets"] = len(ranges)
                row[f"{arm}_bytes"] = nbytes
            dest.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    total = {arm: sum(values) for arm, values in hits.items()}
    p05 = {arm: sorted(values)[49] for arm, values in hits.items()}
    sub90 = {arm: sum(value < 90 for value in values) for arm, values in hits.items()}
    qualifies = (total["candidate"] >= 99_000
                 and total["candidate"] >= total["baseline"]
                 and p05["candidate"] >= 90
                 and p05["candidate"] >= p05["baseline"]
                 and sub90["candidate"] <= sub90["baseline"])
    summary.write_text(json.dumps({
        "schema": "borsuk-v122-deep-image-100k-development-v1",
        "dataset": "deep-image-96-angular-random-100k",
        "split": "test-ordinals-9000-through-9999",
        "query_count": 1_000, "rows": rows, "dimensions": dimensions,
        "regions": regions, "returned_hits": total,
        "recall_at_100": {arm: value / 100_000 for arm, value in total.items()},
        "physical_coverage_hits": {arm: sum(values) for arm, values in cover.items()},
        "p05_hits": p05, "sub90_queries": sub90,
        "maximum_gets": maximum_gets, "maximum_bytes": maximum_bytes,
        "paired_candidate_better": sum(a > b for a, b in zip(hits["candidate"], hits["baseline"])),
        "paired_equal": sum(a == b for a, b in zip(hits["candidate"], hits["baseline"])),
        "paired_baseline_better": sum(a < b for a, b in zip(hits["candidate"], hits["baseline"])),
        "queries_sha256": _sha256_file(queries),
        "truth_sha256": _sha256_file(truth),
        "evidence_sha256": _sha256_file(evidence),
        "qualifies_100k_screen": qualifies,
        "live_s3_measured": False,
    }, sort_keys=True, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("subset", "prepare", "evaluate"))
    for name in ("source", "source_sha256", "output", "subset_source",
                 "subset_source_sha256", "queries", "truth", "built",
                 "router", "evidence", "summary"):
        parser.add_argument(f"--{name.replace('_','-')}")
    args = parser.parse_args()
    if args.phase == "subset":
        make_subset(Path(args.source), Path(args.output),
                    source_sha256=args.source_sha256)
    elif args.phase == "prepare":
        prepare_development(
            Path(args.subset_source), Path(args.queries),
            Path(args.output), Path(args.truth),
            subset_source_sha256=args.subset_source_sha256,
        )
    else:
        evaluate_development(
            Path(args.built), Path(args.router), Path(args.queries),
            Path(args.truth), Path(args.evidence), Path(args.summary),
        )


if __name__ == "__main__":
    main()
