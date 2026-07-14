#!/usr/bin/env python3
"""Fetch a public ann-benchmarks dataset and convert it to flat binaries.

ann-benchmarks distributes standard ANN datasets as HDF5 with real vectors and
precomputed ground-truth neighbors — the right basis for honest recall-vs-latency
curves. HDF5 is awkward from Rust, so this script downloads the HDF5 once and
rewrites it as three headerless little-endian binaries plus a JSON sidecar the
Rust benchmark harness (`production_bench`) can mmap directly:

    <out>/train.f32       n_train * dim   float32   (the corpus vectors)
    <out>/test.f32        n_test  * dim   float32   (the query vectors)
    <out>/neighbors.i32   n_test  * k     int32     (ground-truth ids into train)
    <out>/meta.json       { name, metric, dim, n_train, n_test, k }

Usage:
    uv run --with h5py --with numpy python scripts/fetch_ann_dataset.py \
        --dataset glove-100-angular --out /tmp/borsuk-datasets/glove-100

Common datasets (name -> dim, metric):
    sift-128-euclidean (128, euclidean)   glove-100-angular (100, angular/cosine)
    gist-960-euclidean (960, euclidean)   nytimes-256-angular (256, angular)
    deep-image-96-angular (96, angular)   fashion-mnist-784-euclidean (784, euclidean)
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

BASE_URL = "http://ann-benchmarks.com"
# ann-benchmarks stores angular datasets pre-normalized; BORSUK's cosine metric
# handles normalization itself, so we map angular -> cosine.
METRIC_MAP = {"angular": "cosine", "euclidean": "euclidean"}


def dataset_metric(name: str) -> str:
    suffix = name.rsplit("-", 1)[-1]
    return METRIC_MAP.get(suffix, suffix)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, help="ann-benchmarks dataset name")
    parser.add_argument("--out", required=True, type=Path, help="output directory")
    parser.add_argument(
        "--limit-train",
        type=int,
        default=0,
        help="cap the corpus size for a quick run (0 = full)",
    )
    args = parser.parse_args()

    import h5py  # noqa: PLC0415 — optional heavy dep, imported on demand
    import numpy as np  # noqa: PLC0415

    args.out.mkdir(parents=True, exist_ok=True)
    hdf5_path = args.out / f"{args.dataset}.hdf5"
    if not hdf5_path.exists():
        url = f"{BASE_URL}/{args.dataset}.hdf5"
        print(f"downloading {url} -> {hdf5_path}", file=sys.stderr)
        # ann-benchmarks.com rejects the default urllib User-Agent with 403.
        request = urllib.request.Request(  # noqa: S310 — trusted host
            url, headers={"User-Agent": "curl/8"}
        )
        with (
            urllib.request.urlopen(request) as response,  # noqa: S310
            hdf5_path.open("wb") as handle,
        ):
            while chunk := response.read(1 << 20):
                handle.write(chunk)

    with h5py.File(hdf5_path, "r") as handle:
        train = np.asarray(handle["train"], dtype="<f4")
        test = np.asarray(handle["test"], dtype="<f4")
        neighbors = np.asarray(handle["neighbors"], dtype="<i4")
        metric = handle.attrs.get("distance", args.dataset.rsplit("-", 1)[-1])

    if args.limit_train and args.limit_train < train.shape[0]:
        train = train[: args.limit_train]

    dim = int(train.shape[1])
    train.tofile(args.out / "train.f32")
    test.tofile(args.out / "test.f32")
    neighbors.tofile(args.out / "neighbors.i32")

    meta = {
        "name": args.dataset,
        "metric": dataset_metric(str(metric)),
        "dim": dim,
        "n_train": int(train.shape[0]),
        "n_test": int(test.shape[0]),
        "k": int(neighbors.shape[1]),
    }
    (args.out / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(json.dumps(meta), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
