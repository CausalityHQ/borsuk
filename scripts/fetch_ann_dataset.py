#!/usr/bin/env python3
"""Fetch ann-benchmarks HDF5 and stream it into bounded stock Parquet.

The publication dataset directory contains only the shared BORSUK protocol:
ordered ``train-NNNNNNNN.parquet`` shards, ``test.parquet``,
``neighbors.parquet``, and ``meta.json``. Training shards target 64 MiB of
uncompressed vectors and may never exceed the protocol's 128 MiB object cap.
Row order is preserved so shipped ground-truth ids remain authoritative.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import urllib.request
from pathlib import Path

BASE_URL = "https://ann-benchmarks.com"
METRIC_MAP = {"angular": "cosine", "euclidean": "euclidean"}
TRAIN_SHARD_TARGET_BYTES = 64 * 1024 * 1024
MAX_OBJECT_BYTES = 128 * 1024 * 1024
ROW_GROUP_ROWS = 8_192


def dataset_metric(name: str) -> str:
    suffix = name.rsplit("-", 1)[-1]
    return METRIC_MAP.get(suffix, suffix)


def publication_dataset_id(name: str) -> str:
    suffix = name.rsplit("-", 1)[-1]
    if suffix not in {"angular", "euclidean"}:
        raise ValueError("ann-benchmarks dataset name must end in its metric")
    return name.rsplit("-", 1)[0]


def _require_format_dependencies():
    try:
        import h5py
        import numpy as np
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        raise RuntimeError(
            "dataset conversion requires scripts/requirements-format-bench.txt"
        ) from error
    return h5py, np, pa, pq


def _fixed_list_table(values, dimensions: int, column: str, value_type):
    _, np, pa, _ = _require_format_dependencies()
    contiguous = np.ascontiguousarray(values)
    flat = pa.array(contiguous.reshape(-1), type=value_type)
    array = pa.FixedSizeListArray.from_arrays(
        flat,
        type=pa.list_(pa.field("item", value_type, nullable=False), dimensions),
    )
    return pa.Table.from_arrays(
        [array], schema=pa.schema([pa.field(column, array.type, nullable=False)])
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_bounded_parquet(path: Path, table) -> None:
    _, _, _, pq = _require_format_dependencies()
    pq.write_table(
        table,
        path,
        compression="snappy",
        row_group_size=ROW_GROUP_ROWS,
        use_dictionary=False,
    )
    encoded_bytes = path.stat().st_size
    if encoded_bytes <= 8 or encoded_bytes > MAX_OBJECT_BYTES:
        raise ValueError(f"{path.name} exceeds the bounded Parquet object contract")


def convert_hdf5_dataset(
    source: Path,
    output: Path,
    *,
    dataset_name: str,
    publication_id: str,
    shard_target_bytes: int = TRAIN_SHARD_TARGET_BYTES,
) -> dict[str, object]:
    h5py, np, pa, _ = _require_format_dependencies()
    if output.exists():
        if not output.is_dir() or any(output.iterdir()):
            raise FileExistsError(
                f"refusing to reuse nonempty dataset directory {output}"
            )
    else:
        output.mkdir(parents=True)

    with h5py.File(source, "r") as handle:
        train = handle["train"]
        test = handle["test"]
        neighbors = handle["neighbors"]
        if (
            train.ndim != 2
            or test.ndim != 2
            or neighbors.ndim != 2
            or train.shape[1] != test.shape[1]
            or test.shape[0] != neighbors.shape[0]
        ):
            raise ValueError("ann-benchmarks HDF5 matrices have inconsistent shapes")
        train_rows, dimensions = map(int, train.shape)
        test_rows = int(test.shape[0])
        k = int(neighbors.shape[1])
        if train_rows <= 0 or dimensions <= 0 or test_rows <= 0 or k < 10:
            raise ValueError("ann-benchmarks HDF5 matrices are empty or truth k < 10")
        rows_per_shard = max(1, shard_target_bytes // (dimensions * 4))
        for shard, first in enumerate(range(0, train_rows, rows_per_shard)):
            last = min(train_rows, first + rows_per_shard)
            values = np.asarray(train[first:last], dtype="<f4")
            if not np.isfinite(values).all():
                raise ValueError("training vectors contain a non-finite value")
            _write_bounded_parquet(
                output / f"train-{shard:08}.parquet",
                _fixed_list_table(values, dimensions, "emb", pa.float32()),
            )
        test_values = np.asarray(test, dtype="<f4")
        if not np.isfinite(test_values).all():
            raise ValueError("query vectors contain a non-finite value")
        _write_bounded_parquet(
            output / "test.parquet",
            _fixed_list_table(test_values, dimensions, "emb", pa.float32()),
        )
        neighbor_values = np.asarray(neighbors)
        if (
            not np.issubdtype(neighbor_values.dtype, np.integer)
            or neighbor_values.min() < 0
            or neighbor_values.max() >= train_rows
            or neighbor_values.max() > np.iinfo(np.int32).max
        ):
            raise ValueError("ground-truth ids are outside the train/i32 range")
        _write_bounded_parquet(
            output / "neighbors.parquet",
            _fixed_list_table(
                neighbor_values.astype("<i4"), k, "neighbors_id", pa.int32()
            ),
        )
        metric = handle.attrs.get("distance", dataset_name.rsplit("-", 1)[-1])

    metadata = {
        "name": publication_id,
        "metric": dataset_metric(str(metric)),
        "dim": dimensions,
        "n_train": train_rows,
        "n_test": test_rows,
        "k": k,
    }
    (output / "meta.json").write_text(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n"
    )
    if __package__:
        from scripts.publication_v3_datasets import dataset_materialization_sha256
    else:
        from publication_v3_datasets import dataset_materialization_sha256

    provenance = {
        "schema_version": 1,
        "dataset": publication_id,
        "source": f"{BASE_URL}/{dataset_name}.hdf5",
        "source_sha256": _sha256(source),
        "materialization_sha256": dataset_materialization_sha256(
            output, kind="standard-ann"
        ),
    }
    (output.parent / f"{output.name}.provenance.json").write_text(
        json.dumps(provenance, sort_keys=True, separators=(",", ":")) + "\n"
    )
    return metadata


def _download(source: Path, dataset: str) -> None:
    url = f"{BASE_URL}/{dataset}.hdf5"
    print(f"downloading {url} -> {source}", file=sys.stderr)
    request = urllib.request.Request(url, headers={"User-Agent": "curl/8"})  # noqa: S310
    source.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = None
    try:
        with (
            urllib.request.urlopen(request) as response,  # noqa: S310
            tempfile.NamedTemporaryFile(
                dir=source.parent,
                prefix=f".{source.name}.",
                suffix=".partial",
                delete=False,
            ) as handle,
        ):
            temporary_name = handle.name
            while chunk := response.read(8 * 1024 * 1024):
                handle.write(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, source)
    except BaseException:
        if temporary_name is not None:
            Path(temporary_name).unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, help="ann-benchmarks dataset name")
    parser.add_argument(
        "--out", required=True, type=Path, help="empty output directory"
    )
    parser.add_argument("--publication-id", help="manifest dataset id")
    parser.add_argument("--source-cache", type=Path, help="downloaded HDF5 path")
    parser.add_argument("--limit-train", type=int, default=0, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.limit_train:
        raise ValueError(
            "partial corpus conversion cannot preserve shipped ground truth"
        )
    source = args.source_cache or args.out.parent / f".{args.dataset}.source.hdf5"
    if not source.exists():
        _download(source, args.dataset)
    metadata = convert_hdf5_dataset(
        source,
        args.out,
        dataset_name=args.dataset,
        publication_id=args.publication_id or publication_dataset_id(args.dataset),
    )
    print(json.dumps(metadata), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
