#!/usr/bin/env python3
"""Prepare one BEIR corpus for dense, sparse, text, and hybrid retrieval.

The output deliberately keeps relevance judgments independent from every
retrieval representation. Dense vectors, TF-IDF sparse vectors, and raw text
therefore share the exact same document/query IDs and qrels.

The deterministic hash encoder exists only for tests and pipeline smoke runs.
Publication runs must use a revision-pinned sentence-transformers model.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import re
import struct
import sys
from collections import Counter
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator, TextIO

SCHEMA_VERSION = 1
RETRIEVAL_MODES = [
    "dense",
    "sparse",
    "text",
    "dense+sparse",
    "dense+text",
    "sparse+text",
    "dense+sparse+text",
]
TOKEN_RE = re.compile(r"\w+", flags=re.UNICODE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--split", default="test")
    parser.add_argument(
        "--dense-backend",
        choices=("hash", "sentence-transformers"),
        default="sentence-transformers",
    )
    parser.add_argument("--dense-dimensions", type=int, default=384)
    parser.add_argument(
        "--dense-model",
        default="sentence-transformers/all-MiniLM-L6-v2",
    )
    parser.add_argument("--dense-revision")
    parser.add_argument("--dense-batch-size", type=int, default=256)
    parser.add_argument("--dense-query-prefix", default="")
    parser.add_argument("--dense-document-prefix", default="")
    parser.add_argument("--sparse-max-features", type=int, default=250_000)
    parser.add_argument("--publication", action="store_true")
    return parser.parse_args()


def fail(message: str) -> None:
    raise SystemExit(message)


def tokens(text: str) -> list[str]:
    return TOKEN_RE.findall(text.casefold())


def combined_text(row: dict[str, object]) -> str:
    title = str(row.get("title") or "").strip()
    text = str(row.get("text") or "").strip()
    return f"{title}\n{text}" if title else text


def input_rows(path: Path) -> Iterator[dict[str, object]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                fail(f"{path}:{line_number}: invalid JSON: {error}")
            if not isinstance(row, dict):
                fail(f"{path}:{line_number}: row must be a JSON object")
            yield row


def normalize_id(row: dict[str, object], path: Path) -> str:
    value = row.get("_id", row.get("id"))
    if value is None or not str(value):
        fail(f"{path}: every row must have a non-empty _id or id")
    return str(value)


def load_qrels(path: Path) -> tuple[list[tuple[str, str, int]], set[str]]:
    rows: list[tuple[str, str, int]] = []
    query_ids: set[str] = set()
    with path.open(encoding="utf-8") as handle:
        header = handle.readline().rstrip("\n").split("\t")
        expected = ["query-id", "corpus-id", "score"]
        if header != expected:
            fail(f"{path}: expected TSV header {expected!r}, got {header!r}")
        for line_number, line in enumerate(handle, 2):
            if not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 3:
                fail(f"{path}:{line_number}: qrel must have three TSV fields")
            query_id, document_id, score_text = fields
            try:
                score = int(score_text)
            except ValueError:
                fail(f"{path}:{line_number}: relevance score must be an integer")
            if score <= 0:
                continue
            rows.append((query_id, document_id, score))
            query_ids.add(query_id)
    rows.sort(key=lambda row: (row[0], row[1], -row[2]))
    return rows, query_ids


def write_json_row(handle: TextIO, identifier: str, text: str) -> None:
    handle.write(
        json.dumps(
            {"id": identifier, "text": text},
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    handle.write("\n")


def write_text_lf(path: Path, value: str) -> None:
    """Write UTF-8 with stable LF newlines on Python 3.9 and newer."""
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(value)


def normalize_corpus(
    source: Path,
    output: Path,
    document_frequency: Counter[str],
) -> tuple[int, set[str]]:
    count = 0
    qrel_document_ids: set[str] = set()
    with output.open("w", encoding="utf-8", newline="\n") as target:
        for row in input_rows(source):
            identifier = normalize_id(row, source)
            text = combined_text(row)
            write_json_row(target, identifier, text)
            document_frequency.update(set(tokens(text)))
            qrel_document_ids.add(identifier)
            count += 1
    return count, qrel_document_ids


def normalize_queries(
    source: Path,
    output: Path,
    qrel_query_ids: set[str],
) -> tuple[int, set[str]]:
    found: set[str] = set()
    count = 0
    with output.open("w", encoding="utf-8", newline="\n") as target:
        for row in input_rows(source):
            identifier = normalize_id(row, source)
            if identifier not in qrel_query_ids:
                continue
            write_json_row(target, identifier, combined_text(row))
            found.add(identifier)
            count += 1
    return count, found


def selected_vocabulary(
    document_frequency: Counter[str],
    max_features: int,
) -> tuple[dict[str, int], list[int]]:
    if max_features <= 0:
        fail("--sparse-max-features must be positive")
    selected = sorted(
        document_frequency,
        key=lambda token: (-document_frequency[token], token),
    )[:max_features]
    # Lexicographic assignment remains stable if the feature cap is raised.
    selected.sort()
    vocabulary = {token: index for index, token in enumerate(selected)}
    dfs = [document_frequency[token] for token in selected]
    return vocabulary, dfs


def sparse_vector(
    text: str,
    vocabulary: dict[str, int],
    document_frequencies: list[int],
    documents: int,
) -> tuple[list[int], list[float]]:
    counts = Counter(tokens(text))
    weighted: list[tuple[int, float]] = []
    for token, frequency in counts.items():
        index = vocabulary.get(token)
        if index is None:
            continue
        inverse_document_frequency = (
            math.log((documents + 1.0) / (document_frequencies[index] + 1.0)) + 1.0
        )
        weighted.append((index, frequency * inverse_document_frequency))
    weighted.sort()
    norm = math.sqrt(sum(value * value for _, value in weighted))
    if norm == 0.0:
        return [], []
    return (
        [index for index, _ in weighted],
        [value / norm for _, value in weighted],
    )


def write_u64(handle: BinaryIO, value: int) -> None:
    handle.write(struct.pack("<Q", value))


def write_u32(handle: BinaryIO, values: Iterable[int]) -> None:
    for value in values:
        handle.write(struct.pack("<I", value))


def write_f32(handle: BinaryIO, values: Iterable[float]) -> None:
    for value in values:
        handle.write(struct.pack("<f", value))


def normalized_rows(path: Path) -> Iterator[tuple[str, str]]:
    for row in input_rows(path):
        yield str(row["id"]), str(row["text"])


def write_sparse_payload(
    rows_path: Path,
    prefix: Path,
    vocabulary: dict[str, int],
    document_frequencies: list[int],
    documents: int,
) -> int:
    non_zero = 0
    with (
        prefix.with_suffix(".sparse.offsets.u64").open("wb") as offsets,
        prefix.with_suffix(".sparse.indices.u32").open("wb") as indices_handle,
        prefix.with_suffix(".sparse.values.f32").open("wb") as values_handle,
    ):
        write_u64(offsets, 0)
        for _, text in normalized_rows(rows_path):
            indices, values = sparse_vector(
                text,
                vocabulary,
                document_frequencies,
                documents,
            )
            write_u32(indices_handle, indices)
            write_f32(values_handle, values)
            non_zero += len(indices)
            write_u64(offsets, non_zero)
    return non_zero


def hash_dense_vector(text: str, dimensions: int) -> list[float]:
    vector = [0.0] * dimensions
    for token in tokens(text):
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=16).digest()
        bucket = int.from_bytes(digest[:8], "little") % dimensions
        sign = 1.0 if digest[8] & 1 else -1.0
        vector[bucket] += sign
    norm = math.sqrt(sum(value * value for value in vector))
    if norm:
        vector = [value / norm for value in vector]
    return vector


def chunks(
    rows: Iterable[tuple[str, str]],
    size: int,
) -> Iterator[list[tuple[str, str]]]:
    batch: list[tuple[str, str]] = []
    for row in rows:
        batch.append(row)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def write_hash_dense(
    rows_path: Path,
    output: Path,
    dimensions: int,
    text_prefix: str,
) -> int:
    if dimensions <= 0:
        fail("--dense-dimensions must be positive")
    count = 0
    with output.open("wb") as handle:
        for _, text in normalized_rows(rows_path):
            write_f32(handle, hash_dense_vector(text_prefix + text, dimensions))
            count += 1
    return count


def write_sentence_transformer_dense(
    rows_path: Path,
    output: Path,
    model_name: str,
    revision: str,
    batch_size: int,
    text_prefix: str,
) -> tuple[int, int, dict[str, str]]:
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        fail(
            "sentence-transformers is required for the publication dense encoder; "
            "install the pinned research requirements"
        )
    model = SentenceTransformer(model_name, revision=revision)
    count = 0
    dimensions = int(model.get_sentence_embedding_dimension())
    with output.open("wb") as handle:
        for batch in chunks(normalized_rows(rows_path), batch_size):
            embeddings = model.encode(
                [text_prefix + text for _, text in batch],
                batch_size=batch_size,
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=False,
            )
            if embeddings.shape != (len(batch), dimensions):
                fail(
                    f"dense encoder returned shape {embeddings.shape}, "
                    f"expected {(len(batch), dimensions)}"
                )
            handle.write(embeddings.astype("<f4", copy=False).tobytes(order="C"))
            count += len(batch)
    versions = {
        package: importlib.metadata.version(package)
        for package in ("sentence-transformers", "torch", "transformers", "numpy")
    }
    return count, dimensions, versions


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def write_qrels(path: Path, rows: list[tuple[str, str, int]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write("query-id\tcorpus-id\tscore\n")
        for query_id, document_id, score in rows:
            handle.write(f"{query_id}\t{document_id}\t{score}\n")


def prepare(args: argparse.Namespace) -> None:
    if args.publication and args.dense_backend == "hash":
        fail("publication mode rejects the deterministic hash dense encoder")
    if args.publication and not args.dense_revision:
        fail("publication mode requires a pinned --dense-revision")
    if args.dense_batch_size <= 0:
        fail("--dense-batch-size must be positive")

    source = args.source
    corpus_source = source / "corpus.jsonl"
    queries_source = source / "queries.jsonl"
    qrels_source = source / "qrels" / f"{args.split}.tsv"
    for required in (corpus_source, queries_source, qrels_source):
        if not required.is_file():
            fail(f"missing BEIR input: {required}")

    output = args.output
    output.mkdir(parents=True, exist_ok=False)
    qrels, qrel_query_ids = load_qrels(qrels_source)

    document_frequency: Counter[str] = Counter()
    documents, corpus_ids = normalize_corpus(
        corpus_source,
        output / "corpus.jsonl",
        document_frequency,
    )
    queries, found_query_ids = normalize_queries(
        queries_source,
        output / "queries.jsonl",
        qrel_query_ids,
    )
    missing_queries = qrel_query_ids - found_query_ids
    missing_documents = {row[1] for row in qrels} - corpus_ids
    if missing_queries:
        fail(f"qrels reference {len(missing_queries)} missing queries")
    if missing_documents:
        fail(f"qrels reference {len(missing_documents)} missing documents")
    if documents == 0 or queries == 0 or not qrels:
        fail("dataset must contain documents, judged queries, and positive qrels")

    vocabulary, document_frequencies = selected_vocabulary(
        document_frequency,
        args.sparse_max_features,
    )
    corpus_non_zero = write_sparse_payload(
        output / "corpus.jsonl",
        output / "corpus",
        vocabulary,
        document_frequencies,
        documents,
    )
    query_non_zero = write_sparse_payload(
        output / "queries.jsonl",
        output / "queries",
        vocabulary,
        document_frequencies,
        documents,
    )
    write_text_lf(
        output / "sparse.vocabulary.json",
        json.dumps(
            {
                "tokens": [
                    token
                    for token, _ in sorted(
                        vocabulary.items(),
                        key=lambda item: item[1],
                    )
                ],
                "document_frequencies": document_frequencies,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n",
    )

    dense_metadata: dict[str, object]
    if args.dense_backend == "hash":
        corpus_dense_count = write_hash_dense(
            output / "corpus.jsonl",
            output / "corpus.dense.f32",
            args.dense_dimensions,
            args.dense_document_prefix,
        )
        query_dense_count = write_hash_dense(
            output / "queries.jsonl",
            output / "queries.dense.f32",
            args.dense_dimensions,
            args.dense_query_prefix,
        )
        dense_dimensions = args.dense_dimensions
        dense_metadata = {
            "backend": "deterministic-hash",
            "dimensions": dense_dimensions,
            "publication_valid": False,
            "algorithm": "blake2b-signed-token-hash-v1",
            "query_prefix": args.dense_query_prefix,
            "document_prefix": args.dense_document_prefix,
        }
    else:
        revision = str(args.dense_revision)
        (
            corpus_dense_count,
            dense_dimensions,
            package_versions,
        ) = write_sentence_transformer_dense(
            output / "corpus.jsonl",
            output / "corpus.dense.f32",
            args.dense_model,
            revision,
            args.dense_batch_size,
            args.dense_document_prefix,
        )
        (
            query_dense_count,
            query_dimensions,
            query_package_versions,
        ) = write_sentence_transformer_dense(
            output / "queries.jsonl",
            output / "queries.dense.f32",
            args.dense_model,
            revision,
            args.dense_batch_size,
            args.dense_query_prefix,
        )
        if (
            query_dimensions != dense_dimensions
            or query_package_versions != package_versions
        ):
            fail("query and corpus dense encoder metadata differ")
        dense_metadata = {
            "backend": "sentence-transformers",
            "dimensions": dense_dimensions,
            "publication_valid": True,
            "model": args.dense_model,
            "revision": revision,
            "package_versions": package_versions,
            "normalized": True,
            "query_prefix": args.dense_query_prefix,
            "document_prefix": args.dense_document_prefix,
        }
    if corpus_dense_count != documents or query_dense_count != queries:
        fail("dense vector counts do not match normalized row counts")

    write_qrels(output / "qrels.tsv", qrels)
    artifact_names = [
        "corpus.jsonl",
        "queries.jsonl",
        "qrels.tsv",
        "corpus.dense.f32",
        "queries.dense.f32",
        "corpus.sparse.offsets.u64",
        "corpus.sparse.indices.u32",
        "corpus.sparse.values.f32",
        "queries.sparse.offsets.u64",
        "queries.sparse.indices.u32",
        "queries.sparse.values.f32",
        "sparse.vocabulary.json",
    ]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "dataset": args.dataset,
        "split": args.split,
        "documents": documents,
        "queries": queries,
        "qrels": len(qrels),
        "retrieval_modes": RETRIEVAL_MODES,
        "dense": dense_metadata,
        "sparse": {
            "backend": "tf-idf",
            "dimensions": len(vocabulary),
            "corpus_non_zero": corpus_non_zero,
            "query_non_zero": query_non_zero,
            "l2_normalized": True,
            "tokenizer": "unicode-word-casefold-v1",
            "max_features": args.sparse_max_features,
        },
        "text": {
            "backend": "raw-title-newline-text",
            "query_backend": "raw-text",
        },
        "qrels_semantics": "shared-across-all-retrieval-modes",
        "source_sha256": {
            "corpus.jsonl": sha256(corpus_source),
            "queries.jsonl": sha256(queries_source),
            f"qrels/{args.split}.tsv": sha256(qrels_source),
        },
        "artifacts_sha256": {name: sha256(output / name) for name in artifact_names},
    }
    write_text_lf(
        output / "manifest.json",
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    )
    print(
        json.dumps(
            {
                "dataset": args.dataset,
                "documents": documents,
                "queries": queries,
                "qrels": len(qrels),
                "dense_dimensions": dense_dimensions,
                "sparse_dimensions": len(vocabulary),
            },
            sort_keys=True,
        )
    )


def main() -> None:
    prepare(parse_args())


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(1)
