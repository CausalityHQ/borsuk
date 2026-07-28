#!/usr/bin/env python3
"""Generate oracle-qrel hybrid controls with independent modality signals."""

from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path

from prepare_hybrid_dataset import (
    RETRIEVAL_MODES,
    sha256,
    write_f32,
    write_text_lf,
    write_u32,
    write_u64,
)

SCENARIOS = (
    "aligned",
    "complementary",
    "dense-sparse-complementary",
    "dense-text-complementary",
    "sparse-text-complementary",
    "dense-conflict",
    "sparse-conflict",
    "text-conflict",
)

COMPLEMENTARY_MODALITIES = {
    "complementary": ("dense", "sparse", "text"),
    "dense-sparse-complementary": ("dense", "sparse"),
    "dense-text-complementary": ("dense", "text"),
    "sparse-text-complementary": ("sparse", "text"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--scenario", choices=SCENARIOS, default="aligned")
    parser.add_argument("--documents", type=int, default=100_000)
    parser.add_argument("--queries", type=int, default=600)
    parser.add_argument("--topics", type=int, default=60)
    parser.add_argument("--dense-dimensions", type=int, default=96)
    parser.add_argument("--sparse-dimensions", type=int, default=4096)
    parser.add_argument("--seed", type=int, default=20260723)
    parser.add_argument("--dense-noise", type=float, default=0.05)
    return parser.parse_args()


def fail(message: str) -> None:
    raise SystemExit(message)


def normalize(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0.0:
        return vector
    return [value / norm for value in vector]


def dense_vector(
    prototype: list[float],
    noise: float,
    rng: random.Random,
) -> list[float]:
    return normalize([value + rng.gauss(0.0, noise) for value in prototype])


def sparse_vector(signal: int, signal_slots: int) -> tuple[list[int], list[float]]:
    # Four signal-specific coordinates plus deterministic low-weight shared terms.
    base = signal * 4
    indices = [
        base,
        base + 1,
        base + 2,
        base + 3,
        signal_slots * 4,
        signal_slots * 4 + 1,
    ]
    values = normalize([1.0, 0.8, 0.6, 0.4, 0.15, 0.1])
    return indices, values


def alphabetic_suffix(value: int) -> str:
    """Encode a non-negative integer without tokenizer-breaking punctuation."""
    encoded = []
    value += 1
    while value:
        value, remainder = divmod(value - 1, 26)
        encoded.append(chr(ord("a") + remainder))
    return "".join(reversed(encoded))


def text_payload(marker: str, ordinal: int, kind: str) -> str:
    neutral_marker = f"borsukneutral{alphabetic_suffix(ordinal % 17)}"
    return f"{marker} {marker} {kind} {neutral_marker} sharedcontrolterm"


def write_json_row(handle, identifier: str, text: str) -> None:
    handle.write(
        json.dumps(
            {"id": identifier, "text": text},
            separators=(",", ":"),
        )
    )
    handle.write("\n")


def validate(args: argparse.Namespace) -> None:
    for name in (
        "documents",
        "queries",
        "topics",
        "dense_dimensions",
        "sparse_dimensions",
    ):
        if getattr(args, name) <= 0:
            fail(f"--{name.replace('_', '-')} must be positive")
    if args.documents % args.topics or args.queries % args.topics:
        fail("--documents and --queries must each be divisible by --topics")
    signal_slots = args.queries + args.topics
    if args.sparse_dimensions < signal_slots * 4 + 2:
        fail(
            "--sparse-dimensions is too small for query-specific and background signals"
        )
    if not math.isfinite(args.dense_noise) or args.dense_noise < 0.0:
        fail("--dense-noise must be finite and non-negative")


def generate(args: argparse.Namespace) -> None:
    validate(args)
    output = args.output
    output.mkdir(parents=True, exist_ok=False)
    rng = random.Random(args.seed)
    prototypes = [
        normalize([rng.gauss(0.0, 1.0) for _ in range(args.dense_dimensions)])
        for _ in range(args.queries + args.topics)
    ]
    documents_per_topic = args.documents // args.topics
    queries_per_topic = args.queries // args.topics
    complementary = COMPLEMENTARY_MODALITIES.get(args.scenario)
    document_channels = len(complementary or ())
    relevant_documents_per_query = document_channels or 1
    qrels_by_query: list[list[int]] = [[] for _ in range(args.queries)]

    corpus_non_zero = 0
    with (
        (output / "corpus.jsonl").open(
            "w", encoding="utf-8", newline="\n"
        ) as text_handle,
        (output / "corpus.dense.f32").open("wb") as dense_handle,
        (output / "corpus.sparse.offsets.u64").open("wb") as offsets_handle,
        (output / "corpus.sparse.indices.u32").open("wb") as indices_handle,
        (output / "corpus.sparse.values.f32").open("wb") as values_handle,
    ):
        write_u64(offsets_handle, 0)
        for ordinal in range(args.documents):
            oracle_topic = ordinal // documents_per_topic
            topic_ordinal = ordinal % documents_per_topic
            designated_limit = queries_per_topic * relevant_documents_per_query
            designated_query = None
            owner = None
            if topic_ordinal < designated_limit:
                designated_query = (
                    oracle_topic * queries_per_topic
                    + topic_ordinal // relevant_documents_per_query
                )
                if complementary:
                    owner = complementary[topic_ordinal % relevant_documents_per_query]
                qrels_by_query[designated_query].append(ordinal)

            background_signal = args.queries + oracle_topic
            modality_signals = {
                modality: (
                    designated_query
                    if designated_query is not None
                    and (owner is None or owner == modality)
                    else background_signal
                )
                for modality in ("dense", "sparse", "text")
            }
            text_signal = modality_signals["text"]
            text_marker = (
                f"borsukquery{alphabetic_suffix(text_signal)}"
                if text_signal < args.queries
                else f"borsukbackground{alphabetic_suffix(oracle_topic)}"
            )
            write_json_row(
                text_handle,
                f"d{ordinal}",
                text_payload(text_marker, ordinal, "document"),
            )
            write_f32(
                dense_handle,
                dense_vector(
                    prototypes[modality_signals["dense"]],
                    args.dense_noise,
                    rng,
                ),
            )
            indices, values = sparse_vector(
                modality_signals["sparse"],
                args.queries + args.topics,
            )
            write_u32(indices_handle, indices)
            write_f32(values_handle, values)
            corpus_non_zero += len(indices)
            write_u64(offsets_handle, corpus_non_zero)

    query_non_zero = 0
    with (
        (output / "queries.jsonl").open(
            "w", encoding="utf-8", newline="\n"
        ) as text_handle,
        (output / "queries.dense.f32").open("wb") as dense_handle,
        (output / "queries.sparse.offsets.u64").open("wb") as offsets_handle,
        (output / "queries.sparse.indices.u32").open("wb") as indices_handle,
        (output / "queries.sparse.values.f32").open("wb") as values_handle,
    ):
        write_u64(offsets_handle, 0)
        for ordinal in range(args.queries):
            query_signals = {
                modality: (
                    (ordinal + 1) % args.queries
                    if args.scenario == f"{modality}-conflict"
                    else ordinal
                )
                for modality in ("dense", "sparse", "text")
            }
            text_marker = f"borsukquery{alphabetic_suffix(query_signals['text'])}"
            write_json_row(
                text_handle,
                f"q{ordinal}",
                text_payload(text_marker, ordinal, "query"),
            )
            write_f32(
                dense_handle,
                dense_vector(
                    prototypes[query_signals["dense"]],
                    args.dense_noise,
                    rng,
                ),
            )
            indices, values = sparse_vector(
                query_signals["sparse"],
                args.queries + args.topics,
            )
            write_u32(indices_handle, indices)
            write_f32(values_handle, values)
            query_non_zero += len(indices)
            write_u64(offsets_handle, query_non_zero)

    qrel_count = 0
    with (output / "qrels.tsv").open("w", encoding="utf-8", newline="\n") as handle:
        handle.write("query-id\tcorpus-id\tscore\n")
        for query_ordinal, document_ordinals in enumerate(qrels_by_query):
            for document_ordinal in document_ordinals:
                handle.write(f"q{query_ordinal}\td{document_ordinal}\t1\n")
                qrel_count += 1

    write_text_lf(
        output / "sparse.vocabulary.json",
        json.dumps(
            {
                "kind": "synthetic-coordinate-map-v1",
                "dimensions": args.sparse_dimensions,
                "topics": args.topics,
                "query_signals": args.queries,
                "background_signals": args.topics,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
    )
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
        "schema_version": 1,
        "dataset": args.dataset,
        "split": "synthetic",
        "scenario": args.scenario,
        "seed": args.seed,
        "documents": args.documents,
        "queries": args.queries,
        "qrels": qrel_count,
        "topics": args.topics,
        "complementary_modalities": list(complementary or ()),
        "document_channels": document_channels,
        "relevant_documents_per_query": relevant_documents_per_query,
        "retrieval_modes": RETRIEVAL_MODES,
        "dense": {
            "backend": "synthetic-topic-gaussian-v1",
            "dimensions": args.dense_dimensions,
            "publication_valid": True,
            "noise_standard_deviation": args.dense_noise,
        },
        "sparse": {
            "backend": "synthetic-topic-coordinates-v1",
            "dimensions": args.sparse_dimensions,
            "corpus_non_zero": corpus_non_zero,
            "query_non_zero": query_non_zero,
            "l2_normalized": True,
        },
        "text": {
            "backend": "synthetic-topic-tokens-v1",
        },
        "qrels_semantics": "shared-across-all-retrieval-modes",
        "generator": {
            "scenario": args.scenario,
            "seed": args.seed,
            "documents_per_topic": documents_per_topic,
            "queries_per_topic": queries_per_topic,
            "relevant_documents_per_query": relevant_documents_per_query,
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
                "scenario": args.scenario,
                "documents": args.documents,
                "queries": args.queries,
                "qrels": qrel_count,
            },
            sort_keys=True,
        )
    )


def main() -> None:
    generate(parse_args())


if __name__ == "__main__":
    main()
