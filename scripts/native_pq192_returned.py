"""Corpus-only PQ192 ranking on the closed 100k code-wave candidate rows."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np

from scripts.native_rotated_two_bit_evaluation import exact_scores
from scripts.native_two_bit_returned_recall import ranked_hits
from scripts.native_two_bit_returned_replay import (
    LEGACY_TERMINAL_SHA256,
    load_closed_inputs,
    selected_positions,
)
from scripts.v65_algorithm_first_two_stage import lloyd

SUBSPACES = 192
CLUSTERS = 256
ITERATIONS = 10
SEED = 6501
ROW_BYTES = 208  # 192 centroid bytes and 16 stable-ID bytes.
MAX_CODE_BYTES = 16_777_216


def write_array(path: Path, array: np.ndarray) -> str:
    with path.open("xb") as file:
        np.save(file, array, allow_pickle=False)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def physical_order_sha256(ordinals: tuple[int, ...]) -> str:
    if any(type(value) is not int or not 0 <= value < 2**32 for value in ordinals):
        raise ValueError("PQ physical order differs")
    return hashlib.sha256(np.asarray(ordinals, dtype="<u4").tobytes()).hexdigest()


def assign_codes(ordered: np.ndarray, books: np.ndarray) -> np.ndarray:
    subspaces, clusters, width = books.shape
    if ordered.shape[1] != subspaces * width or clusters > 256:
        raise ValueError("PQ assignment dimensions differ")
    codes = np.empty((len(ordered), subspaces), dtype=np.uint8)
    for subspace in range(subspaces):
        lo = subspace * width
        book = books[subspace]
        norms = np.einsum("ij,ij->i", book, book)
        for first in range(0, len(ordered), 16_384):
            last = min(first + 16_384, len(ordered))
            scores = norms[None, :] - 2.0 * (ordered[first:last, lo:lo + width] @ book.T)
            codes[first:last, subspace] = np.argmin(scores, axis=1)
    return codes


def train_pq(
    vectors: np.ndarray, physical_ordinals: tuple[int, ...], *,
    subspaces: int = SUBSPACES, clusters: int = CLUSTERS,
    iterations: int = ITERATIONS, seed: int = SEED,
) -> tuple[np.ndarray, np.ndarray]:
    if (
        type(vectors) is not np.ndarray or vectors.dtype != np.float32
        or vectors.ndim != 2 or subspaces <= 0
        or vectors.shape[1] % subspaces != 0
        or not 1 < clusters <= min(256, len(vectors)) or iterations <= 0
        or len(physical_ordinals) != len(vectors)
        or set(physical_ordinals) != set(range(len(vectors)))
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("PQ source authority differs")
    ordered = np.ascontiguousarray(vectors[list(physical_ordinals)])
    generator = np.random.default_rng(seed)
    sample = ordered[generator.choice(len(ordered), min(len(ordered), 100_000), replace=False)]
    width = vectors.shape[1] // subspaces
    books = np.empty((subspaces, clusters, width), dtype=np.float32)
    for subspace in range(subspaces):
        lo = subspace * width
        books[subspace] = lloyd(
            np.ascontiguousarray(sample[:, lo:lo + width]),
            clusters, iterations, seed + subspace,
        )
    if not np.isfinite(books).all():
        raise ValueError("PQ trained codebook differs")
    return books, assign_codes(ordered, books)


def score_pq(
    query: np.ndarray, books: np.ndarray, codes: np.ndarray,
    positions: tuple[int, ...],
) -> np.ndarray:
    subspaces, clusters, width = books.shape
    if (
        query.shape != (subspaces * width,)
        or codes.ndim != 2 or codes.shape[1] != subspaces
        or any(not 0 <= position < len(codes) for position in positions)
    ):
        raise ValueError("PQ scorer authority differs")
    difference = books - query.reshape(subspaces, width)[:, None, :]
    table = np.sum(difference * difference, axis=2, dtype=np.float64)
    scores = np.empty(len(positions), dtype=np.float32)
    for first in range(0, len(positions), 4096):
        last = min(first + 4096, len(positions))
        selected = codes[list(positions[first:last])].T.astype(np.intp)
        scores[first:last] = np.take_along_axis(table, selected, axis=1).sum(
            axis=0, dtype=np.float64,
        ).astype(np.float32)
    if not np.isfinite(scores).all():
        raise ValueError("PQ score nonfinite")
    return scores


def planned_bytes(old_bytes: int, rows: int) -> int:
    framing = old_bytes - 200 * rows
    result = framing + ROW_BYTES * rows
    if not 0 < framing <= 1024 or result > MAX_CODE_BYTES:
        raise ValueError("PQ wave byte budget differs")
    return result


def run_closed(root: Path, out: Path) -> dict[str, object]:
    queries, vectors, stable_ids, truth, old_codes, samples = load_closed_inputs(root)
    if not out.is_dir() or len(samples) != 1000 or len(queries) != 1000:
        raise ValueError("PQ closed cohort differs")
    books, codes = train_pq(vectors, old_codes.source_ordinals)
    books_sha = write_array(out / "pq-books.npy", books)
    codes_sha = write_array(out / "pq-codes.npy", codes)
    cases = []
    for ordinal, sample in enumerate(samples):
        positions = selected_positions(sample, old_codes)
        sources = tuple(old_codes.source_ordinals[position] for position in positions)
        grouped = {stable_ids[source] for source in sources}
        grouped_hits = sum(value in grouped for value in truth[ordinal])
        if grouped_hits != sample.grouped_hits_at_100:
            raise ValueError("PQ closed containment differs")
        pq_hits = ranked_hits(
            score_pq(queries[ordinal], books, codes, positions),
            sources, stable_ids, truth[ordinal], top_k=100,
        )
        exact_hits = ranked_hits(
            exact_scores(queries[ordinal], vectors, sources),
            sources, stable_ids, truth[ordinal], top_k=100,
        )
        if max(pq_hits, exact_hits) > grouped_hits:
            raise ValueError("PQ returned hit exceeds fetched rows")
        cases.append({
            "query_ordinal": ordinal, "candidate_rows": len(positions),
            "code_gets": sample.code_gets,
            "code_bytes": planned_bytes(sample.code_bytes, len(positions)),
            "grouped_hits": grouped_hits, "pq_hits": pq_hits,
            "exact_hits": exact_hits, "paired_loss": exact_hits - pq_hits,
        })
    pq_values = sorted(case["pq_hits"] for case in cases)
    exact_values = sorted(case["exact_hits"] for case in cases)
    losses = sorted(case["paired_loss"] for case in cases)
    if any(case["grouped_hits"] != case["exact_hits"] for case in cases):
        raise ValueError("PQ exact/grouped control differs")
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
    if any(not math.isfinite(value) for value in summary.values()):
        raise ValueError("PQ summary nonfinite")
    decision = (
        "advance-pq-fidelity-only"
        if summary["paired_loss_hits"] <= 250
        and summary["pq_p05_hits"] >= summary["exact_p05_hits"] - 1
        else "stop-pq192-sole-scorer"
    )
    evidence_body = (json.dumps({
        "schema": "borsuk-pq192-returned-evidence-v1", "samples": cases,
    }, sort_keys=True, separators=(",", ":")) + "\n").encode()
    result = {
        "schema": "borsuk-pq192-returned-result-v1",
        "dataset": "ReLAION-100k", "split": "development",
        "legacy_terminal_sha256": LEGACY_TERMINAL_SHA256,
        "physical_order_sha256": physical_order_sha256(old_codes.source_ordinals),
        "pq_books_sha256": books_sha, "pq_codes_sha256": codes_sha,
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "summary": summary, "decision": decision,
    }
    with (out / "returned-evidence.json").open("xb") as file:
        file.write(evidence_body)
    with (out / "returned-result.json").open("xb") as file:
        file.write((json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode())
    return result


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run_closed(args.root, args.out), sort_keys=True))


if __name__ == "__main__":
    main()
