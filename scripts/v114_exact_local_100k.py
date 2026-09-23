"""Source-only SQ8 mirror construction for the V114 correctness gate."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np

from scripts.v114_weighted_interval_plan import optimal_weighted_intervals
from scripts.v113_100k_score_screen import nominate_pq64

BLOCK_BYTES = 4096
SOURCE_SHA256 = "a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d"
QUERY_SHA256 = "4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac"
V113_SEAL_SHA256 = "803b00d9366bc8feb0e58e4900e27b1973c5d9578c33a0d6007fe4c28722c7b2"


def _canonical(value: dict[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def compare_records(
    reference: dict[str, object], actual: dict[str, object],
) -> list[str]:
    """Name every diverging exact score, primary or physical-route field."""
    mismatches = []
    for field in ("query_ordinal", "generation", "score_bits", "page_votes",
                  "ranges", "plan_bytes", "plan_score"):
        if reference[field] != actual.get(field):
            mismatches.append(field)
    for field in ("ram_primary", "file_primary"):
        if reference["primary"] != actual.get(field):
            mismatches.append(field)
    return mismatches


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def read_queries(
    path: Path, expected_sha256: str, *, query_count: int, dimensions: int,
) -> np.ndarray:
    """Read only ordered development vectors from the pinned query object."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if _sha256_file(path) != expected_sha256:
        raise ValueError("V114 query object identity differs")
    table = pq.read_table(path, columns=["query", "vector"])
    query_type = table.schema.field("query").type
    vector_type = table.schema.field("vector").type
    if (
        table.num_rows != query_count
        or not pa.types.is_integer(query_type)
        or not pa.types.is_fixed_size_list(vector_type)
        or vector_type.list_size != dimensions
        or vector_type.value_type != pa.float32()
    ):
        raise ValueError("V114 query schema differs")
    identifiers = table["query"].combine_chunks().to_numpy(zero_copy_only=False)
    if not np.array_equal(identifiers, np.arange(query_count)):
        raise ValueError("V114 query IDs are not ordered")
    values = table["vector"].combine_chunks().values.to_numpy(zero_copy_only=False)
    vectors = np.asarray(values, dtype=np.float32).reshape(query_count, dimensions).copy()
    if not np.isfinite(vectors).all():
        raise ValueError("V114 query vector is nonfinite")
    return vectors


def prepare_one(
    query_ordinal: int, query: np.ndarray, built: object, *,
    shortlist: int, primary_count: int,
) -> tuple[dict[str, object], dict[str, object]]:
    """Freeze one PQ64 roster and independent SQ8 primary/physical oracle."""
    nominees = nominate_pq64(
        query, built.pq_books, built.pq_codes, shortlist,
    )
    primary, scores = score_reference(
        query=query, nominees=nominees, ids=built.ids,
        norms=built.sq8_norm, codes=built.sq8_codes,
        low=built.low, step=built.span_step,
        primary_count=primary_count,
    )
    votes, ranges, byte_count, plan_score = route_reference(
        primary, nominees.astype(int).tolist(),
        rows=int(built.ids.size), dimensions=int(query.size),
    )
    request: dict[str, object] = {
        "query_ordinal": query_ordinal,
        "query": query.astype(float).tolist(),
        "nominees": nominees.astype(int).tolist(),
        "primary_count": primary_count,
    }
    reference: dict[str, object] = {
        "query_ordinal": query_ordinal,
        "generation": 1,
        "primary": primary,
        "score_bits": scores.view(np.uint32).astype(int).tolist(),
        "page_votes": votes,
        "ranges": ranges,
        "plan_bytes": byte_count,
        "plan_score": plan_score,
    }
    return request, reference


def route_reference(
    primary: list[int], nominees: list[int], *, rows: int, dimensions: int,
) -> tuple[list[list[int]], list[list[int]], int, int]:
    """Plan exact bytes with V112's 513/1 primary/secondary page votes."""
    final_rows = rows % 256 or 256
    primary_set = set(primary)
    if (
        rows <= 0 or dimensions <= 0 or final_rows % 32
        or len(primary_set) != len(primary)
        or len(set(nominees)) != len(nominees)
        or not primary_set.issubset(nominees)
        or any(ordinal < 0 or ordinal >= rows for ordinal in nominees)
    ):
        raise ValueError("V114 physical row geometry differs")
    votes: dict[int, int] = {}
    for ordinal in nominees:
        page = ordinal // 256
        votes[page] = votes.get(page, 0) + (513 if ordinal in primary_set else 1)
    unit_bytes = 32 * (dimensions + 12)
    score, pages = optimal_weighted_intervals(
        votes, page_count=(rows + 255) // 256, max_gets=32,
        max_units=16_777_216 // unit_bytes,
        full_page_units=8, last_page_units=final_rows // 32,
    )
    full_bytes = 256 * (dimensions + 12)
    object_bytes = rows * (dimensions + 12)
    ranges = [
        [start * full_bytes,
         object_bytes if end == (rows - 1) // 256 else (end + 1) * full_bytes]
        for start, end in pages
    ]
    return (
        [[page, weight] for page, weight in sorted(votes.items())],
        ranges, sum(end - start for start, end in ranges), score,
    )


def score_reference(
    *, query: np.ndarray, nominees: np.ndarray, ids: np.ndarray,
    norms: np.ndarray, codes: np.ndarray, low: np.ndarray, step: np.ndarray,
    primary_count: int,
) -> tuple[list[int], np.ndarray]:
    """Independent vectorized-row implementation of scalar f32 SQ8 order."""
    if (
        query.ndim != 1 or query.dtype != np.float32
        or codes.ndim != 2 or codes.dtype != np.uint8
        or codes.shape[1] != query.size
        or ids.shape != (codes.shape[0],) or ids.dtype != np.int64
        or norms.shape != ids.shape or norms.dtype != np.float32
        or low.shape != query.shape or low.dtype != np.float32
        or step.shape != query.shape or step.dtype != np.float32
        or nominees.ndim != 1 or not np.issubdtype(nominees.dtype, np.integer)
        or not 0 < primary_count <= nominees.size
        or (nominees < 0).any() or (nominees >= ids.size).any()
        or np.unique(nominees).size != nominees.size
        or not np.isfinite(query).all() or not np.isfinite(norms[nominees]).all()
        or not np.isfinite(low).all() or not np.isfinite(step).all()
        or (step <= 0).any()
    ):
        raise ValueError("V114 SQ8 reference inputs differ")
    shift = np.float32(0.0)
    qnorm = np.float32(0.0)
    for coordinate in range(query.size):
        shift += query[coordinate] * low[coordinate]
        qnorm += query[coordinate] * query[coordinate]
    shift -= qnorm / np.float32(2.0)
    weights = query * step
    inner = np.zeros(nominees.size, dtype=np.float32)
    for coordinate in range(query.size):
        inner += codes[nominees, coordinate].astype(np.float32) * weights[coordinate]
    scores = norms[nominees] - np.float32(2.0) * (inner + shift)
    order = np.lexsort((ids[nominees], scores))[:primary_count]
    return nominees[order].astype(int).tolist(), scores


def write_mirror(
    root: Path, *, ids: np.ndarray, norms: np.ndarray, codes: np.ndarray,
    low: np.ndarray, step: np.ndarray, generation: int, max_nominees: int,
) -> dict[str, object]:
    """Write little-endian ID/norm/code rows and SHA-256 block sidecar."""
    if (
        ids.ndim != 1 or ids.dtype != np.int64
        or norms.shape != ids.shape or norms.dtype != np.float32
        or codes.ndim != 2 or codes.shape[0] != ids.size
        or codes.dtype != np.uint8 or codes.shape[1] == 0
        or low.shape != (codes.shape[1],) or low.dtype != np.float32
        or step.shape != low.shape or step.dtype != np.float32
        or not 0 < max_nominees <= ids.size or generation <= 0
        or np.unique(ids).size != ids.size
        or not np.isfinite(norms).all() or not np.isfinite(low).all()
        or not np.isfinite(step).all() or (step <= 0).any()
    ):
        raise ValueError("V114 SQ8 mirror source differs")
    root.mkdir(parents=True, exist_ok=False)
    object_sha = hashlib.sha256()
    digest_sha = hashlib.sha256()
    pending = b""
    row_type = np.dtype([
        ("id", "<i8"), ("norm", "<f4"), ("code", "u1", (codes.shape[1],)),
    ], align=False)
    with (root / "sq8.bin").open("wb") as object_file, \
            (root / "blocks.sha256").open("wb") as sidecar_file:
        for start in range(0, ids.size, 4096):
            stop = min(start + 4096, ids.size)
            rows = np.empty(stop - start, dtype=row_type)
            rows["id"] = ids[start:stop]
            rows["norm"] = norms[start:stop]
            rows["code"] = codes[start:stop]
            encoded = rows.tobytes()
            object_file.write(encoded)
            object_sha.update(encoded)
            pending += encoded
            complete = len(pending) // BLOCK_BYTES * BLOCK_BYTES
            for offset in range(0, complete, BLOCK_BYTES):
                block_digest = hashlib.sha256(
                    pending[offset:offset + BLOCK_BYTES],
                ).digest()
                sidecar_file.write(block_digest)
                digest_sha.update(block_digest)
            pending = pending[complete:]
        if pending:
            block_digest = hashlib.sha256(pending).digest()
            sidecar_file.write(block_digest)
            digest_sha.update(block_digest)
    manifest: dict[str, object] = {
        "format_version": 1,
        "generation": generation,
        "max_nominees": max_nominees,
        "geometry": {"rows": int(ids.size), "dimensions": int(codes.shape[1])},
        "object_sha256": object_sha.hexdigest(),
        "block_digest_sha256": digest_sha.hexdigest(),
        "low": [float(value) for value in low],
        "step": [float(value) for value in step],
    }
    (root / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
    )
    return manifest


def build_from_sealed_artifact(artifact: Path, mirror: Path) -> None:
    """Derive the local plane from the completed, source-only V113 build."""
    from scripts.v113_score_artifact import read_artifact

    if _sha256_file(artifact / "seal.json") != V113_SEAL_SHA256:
        raise ValueError("V113 completed source-only artifact seal differs")
    built = read_artifact(artifact, expected_source_sha256=SOURCE_SHA256)
    if built.ids.size != 100_000 or built.low.size != 768:
        raise ValueError("V114 100k source geometry differs")
    write_mirror(
        mirror, ids=built.ids, norms=built.sq8_norm, codes=built.sq8_codes,
        low=built.low, step=built.span_step, generation=1, max_nominees=512,
    )
    (mirror / "source.json").write_text(_canonical({
        "schema": "borsuk-v114-source-mirror-v1",
        "source_sha256": SOURCE_SHA256,
        "artifact_seal_sha256": V113_SEAL_SHA256,
        "manifest_sha256": _sha256_file(mirror / "manifest.json"),
    }))


def prepare_frozen_queries(
    artifact: Path, mirror: Path, queries: Path,
    requests: Path, reference: Path,
) -> None:
    """Publish the 1,000 exact-roster requests after source mirror sealing."""
    from scripts.v113_score_artifact import read_artifact

    source = json.loads((mirror / "source.json").read_text())
    if (
        source.get("schema") != "borsuk-v114-source-mirror-v1"
        or source.get("source_sha256") != SOURCE_SHA256
        or source.get("artifact_seal_sha256") != V113_SEAL_SHA256
        or source.get("manifest_sha256") != _sha256_file(mirror / "manifest.json")
        or _sha256_file(artifact / "seal.json") != V113_SEAL_SHA256
    ):
        raise ValueError("V114 source-only mirror provenance differs")
    built = read_artifact(artifact, expected_source_sha256=SOURCE_SHA256)
    query_vectors = read_queries(
        queries, QUERY_SHA256, query_count=1000, dimensions=768,
    )
    with requests.open("w") as request_file, reference.open("w") as reference_file:
        for ordinal, query in enumerate(query_vectors):
            request, oracle = prepare_one(
                ordinal, query, built, shortlist=512, primary_count=100,
            )
            request_file.write(_canonical(request))
            reference_file.write(_canonical(oracle))


def compare_frozen_results(reference: Path, actual: Path, summary: Path) -> None:
    """Require full score/route equality and exact physical caps."""
    reference_rows = reference.read_text().splitlines()
    actual_rows = actual.read_text().splitlines()
    if len(reference_rows) != 1000 or len(actual_rows) != 1000:
        raise ValueError("V114 query result count differs")
    first_mismatches: list[dict[str, object]] = []
    maximum_gets = 0
    maximum_bytes = 0
    matched = 0
    for ordinal, (expected_line, actual_line) in enumerate(zip(reference_rows, actual_rows)):
        expected = json.loads(expected_line)
        observed = json.loads(actual_line)
        differs = compare_records(expected, observed)
        if expected["query_ordinal"] != ordinal or observed["query_ordinal"] != ordinal:
            differs.append("ordered_query_ids")
        gets, byte_count = len(observed["ranges"]), observed["plan_bytes"]
        maximum_gets = max(maximum_gets, gets)
        maximum_bytes = max(maximum_bytes, byte_count)
        if gets > 32 or byte_count > 16_777_216:
            differs.append("physical_cap")
        if differs:
            if len(first_mismatches) < 10:
                first_mismatches.append({"query_ordinal": ordinal, "fields": differs})
        else:
            matched += 1
    reduction: dict[str, object] = {
        "schema": "borsuk-v114-exact-local-100k-v1",
        "dataset": "ReLAION-100k",
        "split": "development-1000",
        "source_sha256": SOURCE_SHA256,
        "query_sha256": QUERY_SHA256,
        "artifact_seal_sha256": V113_SEAL_SHA256,
        "reference_sha256": _sha256_file(reference),
        "rust_result_sha256": _sha256_file(actual),
        "query_count": 1000,
        "exact_query_matches": matched,
        "first_mismatches": first_mismatches,
        "maximum_gets": maximum_gets,
        "maximum_bytes": maximum_bytes,
        "ground_truth_used": False,
        "live_s3_measured": False,
    }
    summary.write_text(_canonical(reduction))
    if matched != 1000:
        raise ValueError(f"V114 exact scorer/plan parity failed: {matched}/1000")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="phase", required=True)
    build = subcommands.add_parser("build")
    build.add_argument("--artifact", type=Path, required=True)
    build.add_argument("--mirror", type=Path, required=True)
    prepare = subcommands.add_parser("prepare")
    prepare.add_argument("--artifact", type=Path, required=True)
    prepare.add_argument("--mirror", type=Path, required=True)
    prepare.add_argument("--queries", type=Path, required=True)
    prepare.add_argument("--requests", type=Path, required=True)
    prepare.add_argument("--reference", type=Path, required=True)
    compare = subcommands.add_parser("compare")
    compare.add_argument("--reference", type=Path, required=True)
    compare.add_argument("--actual", type=Path, required=True)
    compare.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    if args.phase == "build":
        build_from_sealed_artifact(args.artifact, args.mirror)
    elif args.phase == "prepare":
        prepare_frozen_queries(
            args.artifact, args.mirror, args.queries,
            args.requests, args.reference,
        )
    else:
        compare_frozen_results(args.reference, args.actual, args.summary)


if __name__ == "__main__":
    main()
