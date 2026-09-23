"""Rebuild corpus-only PQ codes and independently rank the closed roster."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np

from scripts.native_pq192_returned import (
    MAX_CODE_BYTES,
    physical_order_sha256,
    planned_bytes,
    score_pq,
    train_pq,
)
from scripts.native_rotated_two_bit_evaluation import exact_scores
from scripts.native_two_bit_returned_replay import (
    LEGACY_TERMINAL_SHA256,
    load_closed_inputs,
    selected_positions,
)


def _canonical(path: Path) -> dict[str, object]:
    body = path.read_bytes()
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("PQ validation JSON differs") from error
    if type(value) is not dict or body != (
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode():
        raise ValueError("PQ validation canonical JSON differs")
    return value


def _reference_hits(
    scores: np.ndarray, sources: np.ndarray, stable_ids: tuple[bytes, ...],
    truth_set: set[bytes],
) -> int:
    ranks = np.lexsort((sources, scores))[:100]
    ids = {stable_ids[int(sources[index])] for index in ranks}
    if len(ids) != 100:
        raise ValueError("PQ reference ID differs")
    return len(ids & truth_set)


def _verify_assignments(
    vectors: np.ndarray, physical_ordinals: tuple[int, ...],
    books: np.ndarray, codes: np.ndarray,
) -> None:
    """Check each physical row's chosen centroid without the trainer's argmin."""
    for subspace in range(192):
        lo = 4 * subspace
        book = books[subspace].astype(np.float64)
        for first in range(0, len(physical_ordinals), 2048):
            last = min(first + 2048, len(physical_ordinals))
            sources = list(physical_ordinals[first:last])
            rows = vectors[sources, lo:lo + 4].astype(np.float64)
            delta = rows[:, None, :] - book[None, :, :]
            distances = np.einsum("ijk,ijk->ij", delta, delta)
            selected = distances[np.arange(last - first), codes[first:last, subspace]]
            minimum = distances.min(axis=1)
            if np.any(selected > minimum + 1e-6 * np.maximum(1.0, minimum)):
                raise ValueError("PQ physical code assignment differs")


def run_closed(root: Path, out: Path) -> dict[str, object]:
    queries, vectors, stable_ids, truth, old_codes, samples = load_closed_inputs(root)
    evidence_path = out / "returned-evidence.json"
    result_path = out / "returned-result.json"
    evidence = _canonical(evidence_path)
    result = _canonical(result_path)
    if (
        evidence.get("schema") != "borsuk-pq192-returned-evidence-v1"
        or result.get("schema") != "borsuk-pq192-returned-result-v1"
        or result.get("dataset") != "ReLAION-100k"
        or result.get("split") != "development"
        or result.get("legacy_terminal_sha256") != LEGACY_TERMINAL_SHA256
        or result.get("physical_order_sha256") != (
            physical_order_sha256(old_codes.source_ordinals)
        )
        or result.get("evidence_sha256") != hashlib.sha256(evidence_path.read_bytes()).hexdigest()
        or type(evidence.get("samples")) is not list
        or len(evidence["samples"]) != len(samples)
        or len(samples) != len(queries)
        or len(queries) != len(truth)
        or len(truth) != 1000
    ):
        raise ValueError("PQ validation authority differs")
    books_path = out / "pq-books.npy"
    codes_path = out / "pq-codes.npy"
    if (
        result.get("pq_books_sha256") != hashlib.sha256(books_path.read_bytes()).hexdigest()
        or result.get("pq_codes_sha256") != hashlib.sha256(codes_path.read_bytes()).hexdigest()
    ):
        raise ValueError("PQ code identity differs")
    books = np.load(books_path, allow_pickle=False)
    codes = np.load(codes_path, allow_pickle=False)
    rebuilt_books, rebuilt_codes = train_pq(vectors, old_codes.source_ordinals)
    if (
        books.shape != (192, 256, 4) or books.dtype != np.float32
        or codes.shape != (len(vectors), 192) or codes.dtype != np.uint8
        or not np.array_equal(books, rebuilt_books)
        or not np.array_equal(codes, rebuilt_codes)
    ):
        raise ValueError("PQ corpus-only training differs")
    _verify_assignments(vectors, old_codes.source_ordinals, books, codes)
    cases = evidence["samples"]
    for ordinal, sample in enumerate(samples):
        positions = selected_positions(sample, old_codes)
        sources = np.asarray(
            [old_codes.source_ordinals[position] for position in positions],
            dtype=np.int64,
        )
        grouped = {stable_ids[int(source)] for source in sources}
        grouped_hits = sum(value in grouped for value in truth[ordinal])
        if grouped_hits != sample.grouped_hits_at_100:
            raise ValueError("PQ grouped containment differs")
        pq_score = score_pq(queries[ordinal], books, codes, positions)
        reference = np.empty(len(positions), dtype=np.float32)
        for first in range(0, len(positions), 2048):
            last = min(first + 2048, len(positions))
            selected = codes[list(positions[first:last])]
            decoded = books[np.arange(192)[None, :], selected].reshape(last - first, 768)
            delta = decoded - queries[ordinal]
            reference[first:last] = np.sum(
                delta * delta, axis=1, dtype=np.float64,
            ).astype(np.float32)
        if not np.allclose(pq_score, reference, rtol=1e-6, atol=1e-6):
            raise ValueError("PQ reference score differs")
        exact = exact_scores(queries[ordinal], vectors, tuple(int(x) for x in sources))
        truth_set = set(truth[ordinal])

        pq_hits = _reference_hits(reference, sources, stable_ids, truth_set)
        exact_hits = _reference_hits(exact, sources, stable_ids, truth_set)
        if exact_hits != grouped_hits:
            raise ValueError("PQ exact/grouped control differs")
        expected = {
            "query_ordinal": ordinal, "candidate_rows": len(positions),
            "code_gets": sample.code_gets,
            "code_bytes": planned_bytes(sample.code_bytes, len(positions)),
            "grouped_hits": grouped_hits, "pq_hits": pq_hits,
            "exact_hits": exact_hits, "paired_loss": exact_hits - pq_hits,
        }
        if cases[ordinal] != expected:
            raise ValueError("PQ returned ranking differs")
    pq_values = sorted(case["pq_hits"] for case in cases)
    exact_values = sorted(case["exact_hits"] for case in cases)
    losses = sorted(case["paired_loss"] for case in cases)
    summary = {
        "query_count": len(cases), "top_k": 100,
        "pq_recall_at_100_ppm": sum(pq_values) * 10,
        "exact_recall_at_100_ppm": sum(exact_values) * 10,
        "pq_p05_hits": pq_values[49], "exact_p05_hits": exact_values[49],
        "paired_loss_hits": sum(losses), "p95_paired_loss_hits": losses[949],
        "pq_sub90_count": sum(value < 90 for value in pq_values),
        "exact_sub90_count": sum(value < 90 for value in exact_values),
        "max_code_gets": max(case["code_gets"] for case in cases),
        "max_code_bytes": max(case["code_bytes"] for case in cases),
        "mean_candidate_rows": sum(case["candidate_rows"] for case in cases) / len(cases),
    }
    decision = (
        "advance-pq-fidelity-only"
        if summary["paired_loss_hits"] <= 250
        and summary["pq_p05_hits"] >= summary["exact_p05_hits"] - 1
        else "stop-pq192-sole-scorer"
    )
    if (
        result.get("summary") != summary
        or result.get("decision") != decision
        or summary["max_code_gets"] > 32
        or summary["max_code_bytes"] > MAX_CODE_BYTES
    ):
        raise ValueError("PQ summary decision differs")
    value = {
        "schema": "borsuk-pq192-returned-validation-v1", "valid": True,
        "query_count": len(cases),
        "result_sha256": hashlib.sha256(result_path.read_bytes()).hexdigest(),
        "evidence_sha256": hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
    }
    with (out / "returned-validation.json").open("xb") as file:
        file.write((json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode())
    return value


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run_closed(args.root, args.out), sort_keys=True))


if __name__ == "__main__":
    main()
