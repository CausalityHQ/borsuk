#!/usr/bin/env python3
"""Prepare one BEIR dataset as bounded multimodal stock-Parquet objects."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import tempfile
from pathlib import Path

if __package__:
    from scripts.fetch_beir_dataset import fetch as fetch_beir
    from scripts.prepare_hybrid_dataset import input_rows
    from scripts.prepare_hybrid_dataset import prepare as prepare_hybrid
    from scripts.publication_v3_beir import (
        BEIR_DENSE_DIMENSIONS,
        BEIR_DENSE_MODEL,
        BEIR_DENSE_REVISION,
        BEIR_DOCUMENT_PREFIX,
        BEIR_ENCODER_PACKAGES,
        BEIR_QUERY_PREFIX,
        BEIR_ROW_GROUP_SIZE,
        BEIR_ROW_SHARD_SIZE,
        BEIR_SPARSE_MAX_FEATURES,
        expected_beir_metadata,
    )
    from scripts.publication_v3_datasets import (
        MAX_DATASET_OBJECT_BYTES,
        dataset_materialization_sha256,
    )
else:
    from fetch_beir_dataset import fetch as fetch_beir
    from prepare_hybrid_dataset import input_rows
    from prepare_hybrid_dataset import prepare as prepare_hybrid
    from publication_v3_beir import (
        BEIR_DENSE_DIMENSIONS,
        BEIR_DENSE_MODEL,
        BEIR_DENSE_REVISION,
        BEIR_DOCUMENT_PREFIX,
        BEIR_ENCODER_PACKAGES,
        BEIR_QUERY_PREFIX,
        BEIR_ROW_GROUP_SIZE,
        BEIR_ROW_SHARD_SIZE,
        BEIR_SPARSE_MAX_FEATURES,
        expected_beir_metadata,
    )
    from publication_v3_datasets import (
        MAX_DATASET_OBJECT_BYTES,
        dataset_materialization_sha256,
    )


def _dependencies():
    try:
        import numpy as np
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        raise RuntimeError(
            "BEIR publication staging requires requirements-beir-stage.txt"
        ) from error
    return np, pa, pq


def _write_parquet(path: Path, table) -> int:
    _, _, pq = _dependencies()
    pq.write_table(
        table,
        path,
        compression="zstd",
        row_group_size=BEIR_ROW_GROUP_SIZE,
        use_dictionary=False,
    )
    return path.stat().st_size


def _write_table(path: Path, table) -> None:
    size = _write_parquet(path, table)
    if size <= 8 or size > MAX_DATASET_OBJECT_BYTES:
        raise ValueError(f"{path.name} exceeds the bounded Parquet object contract")


def _write_bounded_table_shards(
    output: Path,
    prefix: str,
    table,
    *,
    first_shard: int,
    max_object_bytes: int = MAX_DATASET_OBJECT_BYTES,
) -> int:
    if table.num_rows <= 0:
        raise ValueError(f"{prefix} Parquet shard must contain rows")
    if max_object_bytes <= 8:
        raise ValueError("Parquet object byte cap must exceed its framing")
    next_shard = first_shard

    def emit(candidate) -> None:
        nonlocal next_shard
        path = output / f"{prefix}-{next_shard:08}.parquet"
        size = _write_parquet(path, candidate)
        if 8 < size <= max_object_bytes:
            next_shard += 1
            return
        path.unlink(missing_ok=True)
        if candidate.num_rows == 1:
            raise ValueError(
                f"one {prefix} row exceeds the bounded Parquet object contract"
            )
        midpoint = candidate.num_rows // 2
        emit(candidate.slice(0, midpoint))
        emit(candidate.slice(midpoint))

    emit(table)
    return next_shard


def _normalized_rows(path: Path) -> list[tuple[str, str]]:
    rows = [(str(row["id"]), str(row["text"])) for row in input_rows(path)]
    if not rows:
        raise ValueError(f"{path.name} must contain at least one row")
    return rows


def _row_table(
    rows: list[tuple[str, str]],
    dense,
    offsets,
    indices,
    values,
    dimensions: int,
):
    np, pa, _ = _dependencies()
    first_offset = int(offsets[0])
    last_offset = int(offsets[-1])
    if last_offset < first_offset or last_offset - first_offset > 2**31 - 1:
        raise ValueError("sparse payload exceeds 32-bit Arrow list offsets")
    relative_offsets = np.asarray(offsets - first_offset, dtype=np.int32)
    dense_values = pa.array(np.ascontiguousarray(dense).reshape(-1), type=pa.float32())
    embeddings = pa.FixedSizeListArray.from_arrays(
        dense_values,
        type=pa.list_(pa.field("item", pa.float32(), nullable=False), dimensions),
    )
    index_lists = pa.ListArray.from_arrays(
        pa.array(relative_offsets, type=pa.int32()),
        pa.array(indices[first_offset:last_offset], type=pa.int32()),
        type=pa.list_(pa.field("item", pa.int32(), nullable=False)),
    )
    value_lists = pa.ListArray.from_arrays(
        pa.array(relative_offsets, type=pa.int32()),
        pa.array(values[first_offset:last_offset], type=pa.float32()),
        type=pa.list_(pa.field("item", pa.float32(), nullable=False)),
    )
    return pa.Table.from_arrays(
        [
            pa.array([row[0] for row in rows], type=pa.string()),
            pa.array([row[1] for row in rows], type=pa.string()),
            embeddings,
            index_lists,
            value_lists,
        ],
        schema=pa.schema(
            [
                pa.field("id", pa.string(), nullable=False),
                pa.field("text", pa.string(), nullable=False),
                pa.field("emb", embeddings.type, nullable=False),
                pa.field("sparse_indices", index_lists.type, nullable=False),
                pa.field("sparse_values", value_lists.type, nullable=False),
            ]
        ),
    )


def _convert_rows(prepared: Path, output: Path, prefix: str, dimensions: int) -> int:
    np, _, _ = _dependencies()
    rows = _normalized_rows(prepared / f"{prefix}.jsonl")
    dense = np.memmap(
        prepared / f"{prefix}.dense.f32",
        dtype="<f4",
        mode="r",
        shape=(len(rows), dimensions),
    )
    offsets = np.fromfile(prepared / f"{prefix}.sparse.offsets.u64", dtype="<u8")
    indices = np.fromfile(prepared / f"{prefix}.sparse.indices.u32", dtype="<u4")
    values = np.fromfile(prepared / f"{prefix}.sparse.values.f32", dtype="<f4")
    if (
        len(offsets) != len(rows) + 1
        or int(offsets[-1]) != len(indices)
        or len(indices) != len(values)
    ):
        raise ValueError(f"{prefix} sparse payload cardinality differs")
    output_prefix = "corpus" if prefix == "corpus" else "queries"
    next_shard = 0
    for first in range(0, len(rows), BEIR_ROW_SHARD_SIZE):
        last = min(len(rows), first + BEIR_ROW_SHARD_SIZE)
        next_shard = _write_bounded_table_shards(
            output,
            output_prefix,
            _row_table(
                rows[first:last],
                dense[first:last],
                offsets[first : last + 1],
                indices,
                values,
                dimensions,
            ),
            first_shard=next_shard,
        )
    return len(rows)


def _convert_qrels(prepared: Path, output: Path) -> int:
    _, pa, _ = _dependencies()
    rows: list[tuple[str, str, int]] = []
    with (prepared / "qrels.tsv").open(encoding="utf-8") as handle:
        if handle.readline().rstrip("\n").split("\t") != [
            "query-id",
            "corpus-id",
            "score",
        ]:
            raise ValueError("prepared BEIR qrels header differs")
        for line in handle:
            query_id, corpus_id, score = line.rstrip("\n").split("\t")
            rows.append((query_id, corpus_id, int(score)))
    rows.sort(key=lambda row: (row[0], row[1]))
    table = pa.Table.from_arrays(
        [
            pa.array([row[0] for row in rows], type=pa.string()),
            pa.array([row[1] for row in rows], type=pa.string()),
            pa.array([row[2] for row in rows], type=pa.int32()),
        ],
        schema=pa.schema(
            [
                pa.field("query_id", pa.string(), nullable=False),
                pa.field("corpus_id", pa.string(), nullable=False),
                pa.field("score", pa.int32(), nullable=False),
            ]
        ),
    )
    _write_table(output / "qrels.parquet", table)
    return len(rows)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _source_provenance(
    source: Path, *, dataset_id: str, expected_source: str
) -> dict[str, str]:
    try:
        value = json.loads((source / ".borsuk-source.json").read_text(encoding="utf-8"))
    except (FileNotFoundError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("BEIR source provenance is missing or invalid") from error
    if (
        not isinstance(value, dict)
        or frozenset(value) != {"dataset", "url", "archive_md5", "archive_sha256"}
        or value["dataset"] != dataset_id
        or value["url"] != expected_source
        or not isinstance(value["archive_md5"], str)
        or len(value["archive_md5"]) != 32
        or any(
            character not in "0123456789abcdef" for character in value["archive_md5"]
        )
        or not isinstance(value["archive_sha256"], str)
        or len(value["archive_sha256"]) != 64
        or any(
            character not in "0123456789abcdef" for character in value["archive_sha256"]
        )
    ):
        raise ValueError("BEIR source provenance differs from publication authority")
    return value


def _validate_publication_packages() -> None:
    try:
        installed = {
            package: importlib.metadata.version(package)
            for package in BEIR_ENCODER_PACKAGES
        }
    except importlib.metadata.PackageNotFoundError as error:
        raise ValueError(
            "BEIR publication package versions differ from authority"
        ) from error
    if installed != BEIR_ENCODER_PACKAGES:
        raise ValueError("BEIR publication package versions differ from authority")


def convert_prepared_beir(
    prepared: Path,
    output: Path,
    *,
    dataset_id: str,
    expected_source: str,
    source: Path,
    publication: bool,
) -> dict[str, object]:
    legacy = json.loads((prepared / "manifest.json").read_text(encoding="utf-8"))
    dimensions = int(legacy["dense"]["dimensions"])
    output.mkdir(parents=True, exist_ok=False)
    documents = _convert_rows(prepared, output, "corpus", dimensions)
    queries = _convert_rows(prepared, output, "queries", dimensions)
    qrels = _convert_qrels(prepared, output)
    metadata = expected_beir_metadata(dataset_id)
    metadata.update({"documents": documents, "queries": queries, "qrels": qrels})
    observed_dense = {
        key: value
        for key, value in legacy["dense"].items()
        if key != "publication_valid"
    }
    if publication and observed_dense != metadata["dense"]:
        raise ValueError("BEIR publication dense authority differs")
    metadata["dense"] = observed_dense
    metadata["sparse"].update(legacy["sparse"])
    metadata["sparse"]["vocabulary_sha256"] = _sha256(
        prepared / "sparse.vocabulary.json"
    )
    metadata["text"] = legacy["text"]
    if (
        publication
        and metadata["dense"].get("package_versions") != BEIR_ENCODER_PACKAGES
    ):
        raise ValueError("BEIR publication package versions differ from authority")
    (output / "meta.json").write_text(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    source_metadata = _source_provenance(
        source, dataset_id=dataset_id, expected_source=expected_source
    )
    provenance = {
        "schema_version": 1,
        "dataset": dataset_id,
        "source": expected_source,
        "source_sha256": str(source_metadata["archive_sha256"]),
        "materialization_sha256": dataset_materialization_sha256(
            output, kind="beir-hybrid"
        ),
    }
    (output.parent / f"{output.name}.provenance.json").write_text(
        json.dumps(provenance, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return metadata


def prepare(args: argparse.Namespace) -> dict[str, object]:
    if args.publication and (
        args.dense_backend != "sentence-transformers"
        or args.dense_model != BEIR_DENSE_MODEL
        or args.dense_revision != BEIR_DENSE_REVISION
        or args.dense_query_prefix != BEIR_QUERY_PREFIX
        or args.dense_document_prefix != BEIR_DOCUMENT_PREFIX
        or args.sparse_max_features != BEIR_SPARSE_MAX_FEATURES
    ):
        raise ValueError("BEIR publication encoder arguments differ from authority")
    if args.out.exists():
        raise FileExistsError(f"refusing to reuse BEIR output {args.out}")
    source = args.source
    if source is None:
        source = fetch_beir(
            argparse.Namespace(
                dataset=args.dataset,
                output=args.out.parent / "acquired",
                url=None,
                md5=None,
            )
        )
    _source_provenance(
        source, dataset_id=args.dataset, expected_source=args.expected_source
    )
    if args.publication:
        _validate_publication_packages()
    with tempfile.TemporaryDirectory(
        prefix=".beir-parquet-", dir=args.out.parent
    ) as directory:
        prepared = Path(directory) / "prepared"
        prepare_hybrid(
            argparse.Namespace(
                source=source,
                output=prepared,
                dataset=args.dataset,
                split="test",
                dense_backend=args.dense_backend,
                dense_dimensions=BEIR_DENSE_DIMENSIONS,
                dense_model=args.dense_model,
                dense_revision=args.dense_revision,
                dense_batch_size=args.dense_batch_size,
                dense_query_prefix=args.dense_query_prefix,
                dense_document_prefix=args.dense_document_prefix,
                sparse_max_features=args.sparse_max_features,
                publication=args.publication,
            )
        )
        return convert_prepared_beir(
            prepared,
            args.out,
            dataset_id=args.dataset,
            expected_source=args.expected_source,
            source=source,
            publication=args.publication,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--expected-source", required=True)
    parser.add_argument("--source", type=Path)
    parser.add_argument(
        "--dense-backend",
        choices=("hash", "sentence-transformers"),
        default="sentence-transformers",
    )
    parser.add_argument("--dense-model", default=BEIR_DENSE_MODEL)
    parser.add_argument("--dense-revision", default=BEIR_DENSE_REVISION)
    parser.add_argument("--dense-batch-size", type=int, default=256)
    parser.add_argument("--dense-query-prefix", default=BEIR_QUERY_PREFIX)
    parser.add_argument("--dense-document-prefix", default=BEIR_DOCUMENT_PREFIX)
    parser.add_argument(
        "--sparse-max-features", type=int, default=BEIR_SPARSE_MAX_FEATURES
    )
    parser.add_argument("--publication", action="store_true")
    return parser.parse_args()


def main() -> int:
    metadata = prepare(parse_args())
    print(json.dumps(metadata, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
