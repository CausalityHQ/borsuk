#!/usr/bin/env python3
"""Build deterministic source-only coarse postings for the V254 PQ gate."""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np


SEED = 254
ITERATIONS = 6
ROWS_PER_CELL = 256
COPIES = 2


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def fit_and_assign(data, k):
    if data.ndim != 2 or len(data) < k or k < 2 or not np.isfinite(data).all():
        raise ValueError("coarse training geometry differs")
    vectors = np.asarray(data, dtype=np.float32).copy()
    norms = np.linalg.norm(vectors, axis=1)
    if not np.isfinite(norms).all() or (norms <= 0).any():
        raise ValueError("coarse source norm differs")
    vectors /= norms[:, None]
    rng = np.random.default_rng(SEED)
    centroids = vectors[rng.choice(len(vectors), size=k, replace=False)].copy()
    labels = np.empty(len(vectors), dtype=np.int32)
    for _ in range(ITERATIONS):
        for first in range(0, len(vectors), 4096):
            block = vectors[first:first + 4096] @ centroids.T
            labels[first:first + len(block)] = np.argmax(block, axis=1)
        for cell in range(k):
            members = vectors[labels == cell]
            if len(members):
                mean = members.mean(axis=0)
                norm = np.linalg.norm(mean)
                if norm > 0 and np.isfinite(norm):
                    centroids[cell] = mean / norm
    nearest = np.empty((len(vectors), COPIES), dtype=np.int32)
    for first in range(0, len(vectors), 4096):
        scores = vectors[first:first + 4096] @ centroids.T
        top = np.argsort(-scores, axis=1, kind="stable")[:, :COPIES]
        nearest[first:first + len(top)] = top
    return centroids, nearest


def prepare(source: Path, prep: Path, codes: Path, output: Path):
    authority = json.loads(prep.read_text())
    rows, dims = authority["rows"], authority["dimensions"]
    if (authority["schema"] != "borsuk-v248-source-preparation-v1"
            or not 2 <= rows <= 10_000_000 or dims <= 0
            or source.stat().st_size != rows * dims * 4
            or codes.stat().st_size != rows * 64
            or digest(source) != authority["source_sha256"]
            or digest(codes) != authority["artifacts"]["codes.bin"]["sha256"]
            or output.exists()):
        raise ValueError("coarse source/PQ identity differs")
    k = (rows + ROWS_PER_CELL - 1) // ROWS_PER_CELL
    data = np.memmap(source, dtype="<f4", mode="r", shape=(rows, dims))
    centroids, nearest = fit_and_assign(data, k)
    cells = nearest.T.reshape(-1)
    ordinals = np.tile(np.arange(rows, dtype=np.uint32), COPIES)
    order = np.lexsort((ordinals, cells))
    cells, ordinals = cells[order], ordinals[order]
    offsets = np.searchsorted(cells, np.arange(k + 1)).astype("<u4")
    sizes = np.diff(offsets.astype(np.int64))
    if (len(ordinals) != rows * COPIES or sizes.max() > 8 * len(ordinals) / k
            or (nearest[:, 0] == nearest[:, 1]).any()):
        raise ValueError("coarse posting structure differs")
    output.mkdir(parents=True)
    np.asarray(centroids, dtype="<f4").tofile(output / "centroids.f32")
    offsets.tofile(output / "offsets.u32")
    np.asarray(ordinals, dtype="<u4").tofile(output / "postings.u32")
    artifacts = {name: {"bytes": (output / name).stat().st_size,
                        "sha256": digest(output / name)}
                 for name in ("centroids.f32", "offsets.u32", "postings.u32")}
    (output / "coarse.json").write_text(json.dumps({
        "schema": "borsuk-v254-coarse-pq-v1", "rows": rows, "dimensions": dims,
        "centroids": k, "rows_per_cell": ROWS_PER_CELL, "copies": COPIES,
        "seed": SEED, "lloyd_iterations": ITERATIONS,
        "source_sha256": authority["source_sha256"],
        "codes_sha256": authority["artifacts"]["codes.bin"]["sha256"],
        "max_list_rows": int(sizes.max()), "min_list_rows": int(sizes.min()),
        "artifacts": artifacts,
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--prep", required=True, type=Path)
    parser.add_argument("--codes", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    prepare(args.source, args.prep, args.codes, args.output)
