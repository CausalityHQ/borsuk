#!/usr/bin/env python3
"""Fetch checksum-pinned official VectorDBBench Parquet inputs.

The script selects only unshuffled train shards so BORSUK's monotonic generated
ids remain aligned with the shipped ground-truth ids. It never converts the
corpus to a private/raw vector container.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PUBLIC_BUCKET = "assets.zilliz.com"
PUBLIC_REGION = "us-west-2"
TRAIN_FILE = re.compile(r"^train(?:-\d+-of-\d+)?\.parquet$")


@dataclass(frozen=True)
class DatasetContract:
    remote_prefix: str
    rows: int
    dimensions: int
    train_files: int
    metric: str
    workload: str
    adapter: str
    license: str
    license_source: str


DATASETS = {
    "cohere-medium-1M": DatasetContract(
        remote_prefix="cohere_medium_1m",
        rows=1_000_000,
        dimensions=768,
        train_files=1,
        metric="cosine",
        workload="dense_ann",
        adapter="borsuk_dense_ann",
        license="benchmark-use; Wikipedia/Cohere embedding provenance",
        license_source="https://docs.cohere.com/v2/page/wikipedia-semantic-search",
    ),
    "cohere-large-10M": DatasetContract(
        remote_prefix="cohere_large_10m",
        rows=10_000_000,
        dimensions=768,
        train_files=10,
        metric="cosine",
        workload="dense_ann",
        adapter="borsuk_dense_ann",
        license="benchmark-use; Wikipedia/Cohere embedding provenance",
        license_source="https://docs.cohere.com/v2/page/wikipedia-semantic-search",
    ),
    "laion-100M": DatasetContract(
        remote_prefix="laion_large_100m",
        rows=100_000_000,
        dimensions=768,
        train_files=100,
        metric="euclidean",
        workload="insert_search_under_load",
        adapter="borsuk_lifecycle",
        license="CC-BY-4.0 metadata; derived embedding benchmark artifact",
        license_source="https://laion.ai/blog/laion-5b/",
    ),
}


def parse_s3_listing(value: str) -> list[str]:
    files: list[str] = []
    for line in value.splitlines():
        fields = line.split(maxsplit=3)
        if len(fields) == 4 and fields[2].isdigit():
            files.append(fields[3])
    return files


def select_files(files: list[str], expected_train_files: int) -> list[str]:
    train = sorted(name for name in files if TRAIN_FILE.fullmatch(name))
    required = {"test.parquet", "neighbors.parquet"}
    missing = sorted(required.difference(files))
    if missing:
        raise ValueError(f"remote dataset is missing: {', '.join(missing)}")
    if len(train) != expected_train_files:
        raise ValueError(
            f"expected {expected_train_files} unshuffled train Parquet file(s), "
            f"found {len(train)}"
        )
    return ["neighbors.parquet", "test.parquet", *train]


def validate_local_files(dataset_dir: Path, expected: list[str]) -> None:
    missing = [name for name in expected if not (dataset_dir / name).is_file()]
    if missing:
        raise ValueError(f"missing downloaded source: {', '.join(missing)}")
    empty = [name for name in expected if (dataset_dir / name).stat().st_size == 0]
    if empty:
        raise ValueError(f"empty downloaded source: {', '.join(empty)}")
    expected_set = set(expected)
    unexpected = sorted(
        path.name
        for path in dataset_dir.iterdir()
        if ".parquet" in path.name and path.name not in expected_set
    )
    if unexpected:
        raise ValueError(f"unexpected downloaded Parquet source: {', '.join(unexpected)}")


def source_uri(contract: DatasetContract) -> str:
    return f"s3://{PUBLIC_BUCKET}/benchmark/{contract.remote_prefix}"


def list_remote(contract: DatasetContract) -> list[str]:
    result = subprocess.run(
        [
            "aws",
            "s3",
            "ls",
            source_uri(contract) + "/",
            "--no-sign-request",
            "--region",
            PUBLIC_REGION,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return parse_s3_listing(result.stdout)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def metadata_document(
    dataset: str,
    contract: DatasetContract,
    *,
    n_test: int,
    k: int,
) -> dict[str, Any]:
    return {
        "name": dataset,
        "metric": contract.metric,
        "dim": contract.dimensions,
        "n_train": contract.rows,
        "n_test": n_test,
        "k": k,
    }


def descriptor_document(
    dataset: str,
    dataset_dir: Path,
    remote_files: list[str],
    contract: DatasetContract,
) -> dict[str, Any]:
    meta_path = dataset_dir / "meta.json"
    prepared_files = [dataset_dir / name for name in remote_files]
    file_rows: list[dict[str, Any]] = []
    source_digest = hashlib.sha256()
    for path in [*prepared_files, meta_path]:
        digest = sha256_file(path)
        file_rows.append(
            {"path": path.name, "bytes": path.stat().st_size, "sha256": digest}
        )
        if path != meta_path:
            source_digest.update(path.name.encode())
            source_digest.update(b"\0")
            source_digest.update(digest.encode())
            source_digest.update(b"\n")
    return {
        "schema_version": 1,
        "dataset": dataset,
        "workload": contract.workload,
        "dimensions": contract.dimensions,
        "scale": str(contract.rows),
        "source": source_uri(contract),
        "source_sha256": source_digest.hexdigest(),
        "license": contract.license,
        "license_source": contract.license_source,
        "adapter": contract.adapter,
        "physical_format": "Parquet",
        "files": file_rows,
    }


def parquet_contract(dataset_dir: Path, contract: DatasetContract) -> tuple[int, int]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        raise RuntimeError(
            "dataset validation requires pyarrow from "
            "scripts/requirements-format-bench.txt"
        ) from error

    train_paths = sorted(dataset_dir.glob("train*.parquet"))
    train_rows = sum(pq.ParquetFile(path).metadata.num_rows for path in train_paths)
    if train_rows != contract.rows:
        raise ValueError(
            f"downloaded train Parquet has {train_rows} rows, expected {contract.rows}"
        )
    test = pq.ParquetFile(dataset_dir / "test.parquet")
    test_schema = test.schema_arrow
    if test_schema.get_field_index("emb") < 0:
        raise ValueError("test.parquet has no emb column")
    emb_type = test_schema.field("emb").type
    if not (
        pa.types.is_list(emb_type)
        or pa.types.is_large_list(emb_type)
        or pa.types.is_fixed_size_list(emb_type)
    ) or not pa.types.is_float32(emb_type.value_type):
        raise ValueError(f"test.parquet emb must be a float32 list, got {emb_type}")
    first_test = test.read_row_group(0, columns=["emb"]).column("emb")[0].as_py()
    if len(first_test) != contract.dimensions:
        raise ValueError(
            f"test vector has {len(first_test)} dimensions, expected {contract.dimensions}"
        )

    neighbors = pq.ParquetFile(dataset_dir / "neighbors.parquet")
    neighbor_type = neighbors.schema_arrow.field("neighbors_id").type
    if not (
        pa.types.is_list(neighbor_type)
        or pa.types.is_large_list(neighbor_type)
        or pa.types.is_fixed_size_list(neighbor_type)
    ):
        raise ValueError(
            f"neighbors.parquet neighbors_id must be a list, got {neighbor_type}"
        )
    first_neighbors = (
        neighbors.read_row_group(0, columns=["neighbors_id"])
        .column("neighbors_id")[0]
        .as_py()
    )
    if len(first_neighbors) < 10:
        raise ValueError(
            "neighbors.parquet must contain at least ten neighbors per query"
        )
    if neighbors.metadata.num_rows != test.metadata.num_rows:
        raise ValueError("test and neighbors Parquet row counts differ")
    return test.metadata.num_rows, len(first_neighbors)


def download_dataset(dataset: str, output_root: Path) -> Path:
    contract = DATASETS[dataset]
    dataset_dir = output_root / dataset
    if dataset_dir.exists():
        raise FileExistsError(
            f"refusing to reuse existing dataset directory {dataset_dir}"
        )
    remote_files = select_files(list_remote(contract), contract.train_files)
    dataset_dir.mkdir(parents=True)
    for name in remote_files:
        subprocess.run(
            [
                "aws",
                "s3",
                "cp",
                f"{source_uri(contract)}/{name}",
                str(dataset_dir / name),
                "--no-sign-request",
                "--region",
                PUBLIC_REGION,
                "--only-show-errors",
            ],
            check=True,
        )

    return finalize_dataset(dataset, dataset_dir, remote_files)


def finalize_dataset(dataset: str, dataset_dir: Path, remote_files: list[str]) -> Path:
    contract = DATASETS[dataset]
    validate_local_files(dataset_dir, remote_files)

    n_test, k = parquet_contract(dataset_dir, contract)
    write_json(
        dataset_dir / "meta.json",
        metadata_document(dataset, contract, n_test=n_test, k=k),
    )
    write_json(
        dataset_dir / "dataset.json",
        descriptor_document(dataset, dataset_dir, remote_files, contract),
    )
    return dataset_dir


def validate_existing_dataset(dataset: str, output_root: Path) -> Path:
    contract = DATASETS[dataset]
    dataset_dir = output_root / dataset
    if not dataset_dir.is_dir():
        raise FileNotFoundError(f"missing existing dataset directory {dataset_dir}")
    remote_files = select_files(list_remote(contract), contract.train_files)
    return finalize_dataset(dataset, dataset_dir, remote_files)


def check_existing_dataset(dataset: str, output_root: Path) -> Path:
    """Recompute the complete frozen contract without modifying the dataset."""
    contract = DATASETS[dataset]
    dataset_dir = output_root / dataset
    if not dataset_dir.is_dir():
        raise FileNotFoundError(f"missing existing dataset directory {dataset_dir}")
    remote_files = select_files(list_remote(contract), contract.train_files)
    validate_local_files(dataset_dir, remote_files)
    n_test, k = parquet_contract(dataset_dir, contract)
    expected_meta = metadata_document(dataset, contract, n_test=n_test, k=k)
    actual_meta = json.loads((dataset_dir / "meta.json").read_text())
    if actual_meta != expected_meta:
        raise ValueError("frozen meta.json differs from validated dataset contract")
    expected_descriptor = descriptor_document(
        dataset, dataset_dir, remote_files, contract
    )
    actual_descriptor = json.loads((dataset_dir / "dataset.json").read_text())
    if actual_descriptor != expected_descriptor:
        raise ValueError("frozen dataset.json differs from validated source bytes")
    return dataset_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=tuple(DATASETS), required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--execute-download", action="store_true")
    parser.add_argument("--validate-existing", action="store_true")
    parser.add_argument("--check-existing", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    modes = sum((args.execute_download, args.validate_existing, args.check_existing))
    if modes > 1:
        raise ValueError(
            "choose one of --execute-download, --validate-existing, or --check-existing"
        )
    contract = DATASETS[args.dataset]
    files = select_files(list_remote(contract), contract.train_files)
    if modes == 0:
        print(
            json.dumps(
                {
                    "dataset": args.dataset,
                    "source": source_uri(contract),
                    "files": files,
                    "rows": contract.rows,
                    "dimensions": contract.dimensions,
                    "output": str(args.output_root / args.dataset),
                    "status": "planned",
                },
                indent=2,
            )
        )
        return 0
    if args.check_existing:
        print(check_existing_dataset(args.dataset, args.output_root))
    elif args.validate_existing:
        print(validate_existing_dataset(args.dataset, args.output_root))
    else:
        print(download_dataset(args.dataset, args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
