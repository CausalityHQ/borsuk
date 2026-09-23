"""Recompute V114 exact local 100k evidence after its terminal marker."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

import numpy as np

from scripts.v113_score_artifact import read_artifact
from scripts.v114_exact_local_100k import (
    QUERY_SHA256, SOURCE_SHA256, V113_SEAL_SHA256, _canonical,
    _sha256_file, compare_frozen_results, compare_records, prepare_one,
    read_queries,
)


def validate_mirror_rows(
    mirror: Path, ids: np.ndarray, norms: np.ndarray, codes: np.ndarray,
) -> None:
    """Check every physical row against the source-only sealed arrays."""
    manifest = json.loads((mirror / "manifest.json").read_text())
    rows, dimensions = ids.size, codes.shape[1]
    if (
        manifest.get("format_version") != 1
        or manifest.get("geometry") != {"rows": rows, "dimensions": dimensions}
        or norms.shape != (rows,) or codes.shape != (rows, dimensions)
        or manifest.get("object_sha256") != _sha256_file(mirror / "sq8.bin")
        or manifest.get("block_digest_sha256") != _sha256_file(mirror / "blocks.sha256")
    ):
        raise ValueError("V114 mirror geometry or identity differs")
    object_path = mirror / "sq8.bin"
    if object_path.stat().st_size != rows * (dimensions + 12):
        raise ValueError("V114 SQ8 object size differs")
    row_type = np.dtype([
        ("id", "<i8"), ("norm", "<f4"), ("code", "u1", (dimensions,)),
    ], align=False)
    records = np.memmap(object_path, dtype=row_type, mode="r", shape=(rows,))
    if (
        not np.array_equal(records["id"], ids)
        or not np.array_equal(records["norm"].view(np.uint32), norms.view(np.uint32))
        or not np.array_equal(records["code"], codes)
    ):
        raise ValueError("V114 mirror row differs from sealed source")
    expected_blocks = (object_path.stat().st_size + 4095) // 4096
    sidecar = (mirror / "blocks.sha256").read_bytes()
    if len(sidecar) != expected_blocks * 32:
        raise ValueError("V114 block sidecar size differs")
    with object_path.open("rb") as handle:
        for offset in range(expected_blocks):
            if hashlib.sha256(handle.read(4096)).digest() != sidecar[offset * 32:(offset + 1) * 32]:
                raise ValueError("V114 block digest differs from object")


def validate(
    artifact: Path, mirror: Path, queries: Path,
    requests: Path, reference: Path, rust: Path, summary: Path,
) -> None:
    """Independently reload the frozen cohort and rederive every decision."""
    if _sha256_file(artifact / "seal.json") != V113_SEAL_SHA256:
        raise ValueError("V114 upstream artifact seal differs")
    built = read_artifact(artifact, expected_source_sha256=SOURCE_SHA256)
    source = json.loads((mirror / "source.json").read_text())
    if (
        source.get("source_sha256") != SOURCE_SHA256
        or source.get("artifact_seal_sha256") != V113_SEAL_SHA256
        or source.get("manifest_sha256") != _sha256_file(mirror / "manifest.json")
    ):
        raise ValueError("V114 source provenance differs")
    validate_mirror_rows(mirror, built.ids, built.sq8_norm, built.sq8_codes)
    vectors = read_queries(queries, QUERY_SHA256, query_count=1000, dimensions=768)
    request_rows = requests.read_text().splitlines()
    oracle_rows = reference.read_text().splitlines()
    rust_rows = rust.read_text().splitlines()
    if any(len(rows) != 1000 for rows in (request_rows, oracle_rows, rust_rows)):
        raise ValueError("V114 evidence row count differs")
    for ordinal, query in enumerate(vectors):
        expected_request, expected_reference = prepare_one(
            ordinal, query, built, shortlist=512, primary_count=100,
        )
        if (
            json.loads(request_rows[ordinal]) != expected_request
            or json.loads(oracle_rows[ordinal]) != expected_reference
            or compare_records(expected_reference, json.loads(rust_rows[ordinal]))
        ):
            raise ValueError(f"V114 query {ordinal} evidence differs")
    with tempfile.TemporaryDirectory() as directory:
        calculated = Path(directory) / "reduction.json"
        compare_frozen_results(reference, rust, calculated)
        if summary.read_bytes() != calculated.read_bytes():
            raise ValueError("V114 reduction differs")
    print(_canonical({
        "schema": "borsuk-v114-validation-v1",
        "queries_verified": 1000,
        "mirror_rows_verified": int(built.ids.size),
        "ground_truth_used": False,
    }), end="")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("artifact", "mirror", "queries", "requests", "reference", "rust", "summary"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    validate(args.artifact, args.mirror, args.queries, args.requests,
             args.reference, args.rust, args.summary)


if __name__ == "__main__":
    main()
