"""Independently recompute returned IDs from the sealed 100k candidate rows."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


def _canonical(body: bytes) -> dict[str, object]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("returned validation JSON differs") from error
    if (
        type(value) is not dict
        or body != (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ):
        raise ValueError("returned validation canonical JSON differs")
    return value


def _reference_hits(
    query: object, vectors: object, stable_ids: tuple[bytes, ...],
    truth_ids: tuple[bytes, ...], codes: object, positions: tuple[int, ...],
    *, top_k: int,
) -> tuple[int, int, int]:
    import numpy as np

    from scripts.native_rotated_two_bit_codes import decode_levels, rotate_rows

    sources = np.asarray([codes.source_ordinals[index] for index in positions], dtype=np.int64)
    primary = np.empty(len(sources), dtype=np.float32)
    stored_norm = np.empty(len(sources), dtype=np.float32)
    exact = np.empty(len(sources), dtype=np.float32)
    centered = query.astype(np.float64) - codes.mean.astype(np.float64)
    rotated = rotate_rows(centered[None, :], rotation_seed=codes.rotation_seed)[0]
    query_norm = float(np.sum(centered * centered, dtype=np.float64))
    query_f64 = query.astype(np.float64)
    for first in range(0, len(sources), 2048):
        last = min(first + 2048, len(sources))
        records = codes.records[list(positions[first:last])]
        levels = decode_levels(records[:, :192]).astype(np.float64)
        scales = np.frombuffer(records[:, 192:196].tobytes(), dtype="<f4").astype(np.float64)
        norms = np.frombuffer(records[:, 196:200].tobytes(), dtype="<f4").astype(np.float64)
        dot = np.sum(levels * rotated, axis=1, dtype=np.float64)
        coded_norm = np.sum(levels * levels, axis=1, dtype=np.float64) * (scales * scales)
        primary[first:last] = (query_norm + coded_norm - 2 * scales * dot).astype(np.float32)
        stored_norm[first:last] = (query_norm + norms - 2 * scales * dot).astype(np.float32)
        delta = vectors[sources[first:last]].astype(np.float64) - query_f64
        exact[first:last] = np.einsum("ij,ij->i", delta, delta).astype(np.float32)
    if (not np.isfinite(primary).all() or not np.isfinite(stored_norm).all()
            or not np.isfinite(exact).all()):
        raise ValueError("returned reference score differs")
    truth_set = set(truth_ids)
    if len(truth_set) != len(truth_ids):
        raise ValueError("returned reference truth differs")

    def hits(scores: object) -> int:
        ranked = np.lexsort((sources, scores))[:top_k]
        selected = {stable_ids[int(sources[index])] for index in ranked}
        if len(selected) != top_k:
            raise ValueError("returned reference ID differs")
        return len(selected & truth_set)

    return hits(primary), hits(stored_norm), hits(exact)


def validate_loaded(
    queries: object, vectors: object, stable_ids: tuple[bytes, ...],
    truth: tuple[tuple[bytes, ...], ...], codes: object,
    samples: tuple[object, ...], out: Path, *, top_k: int = 100,
) -> dict[str, object]:
    """Reject any reported returned hit that disagrees with reference scoring."""
    from scripts.native_two_bit_returned_replay import (
        LEGACY_TERMINAL_SHA256,
        selected_positions,
        summarize_results,
    )

    evidence_body = (out / "returned-evidence.json").read_bytes()
    result_body = (out / "returned-result.json").read_bytes()
    evidence = _canonical(evidence_body)
    result = _canonical(result_body)
    if (
        evidence.get("schema") != "borsuk-two-bit-returned-evidence-v2"
        or result.get("schema") != "borsuk-two-bit-returned-result-v2"
        or result.get("dataset") != "ReLAION-100k"
        or result.get("split") != "development"
        or result.get("legacy_terminal_sha256") != LEGACY_TERMINAL_SHA256
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or type(evidence.get("samples")) is not list
        or len(evidence["samples"]) != len(samples)
        or len(samples) != len(queries)
        or len(queries) != len(truth)
    ):
        raise ValueError("returned replay authority differs")
    cases = []
    for ordinal, sample in enumerate(samples):
        positions = selected_positions(sample, codes)
        grouped_ids = {stable_ids[codes.source_ordinals[index]] for index in positions}
        grouped_hits = sum(value in grouped_ids for value in truth[ordinal])
        if grouped_hits != sample.grouped_hits_at_100:
            raise ValueError("returned replay closed group containment differs")
        primary, stored_norm, exact = _reference_hits(
            queries[ordinal], vectors, stable_ids, truth[ordinal], codes,
            positions, top_k=top_k,
        )
        case = {
            "query_ordinal": ordinal,
            "candidate_rows": len(positions),
            "code_gets": sample.code_gets,
            "code_bytes": sample.code_bytes,
            "grouped_hits": grouped_hits,
            "two_bit_hits": primary,
            "stored_norm_hits": stored_norm,
            "exact_hits": exact,
            "paired_loss": exact - primary,
            "stored_norm_paired_loss": exact - stored_norm,
        }
        if evidence["samples"][ordinal] != case:
            raise ValueError("returned replay hit differs")
        cases.append(case)
    summary = summarize_results(cases, expected_queries=len(samples), top_k=top_k)
    if result.get("summary") != summary:
        raise ValueError("returned replay summary differs")
    if top_k == 100 and len(samples) == 1000:
        decision = (
            "advance-fidelity-only"
            if summary["stored_norm_net_paired_loss_hits"] <= 250
            and summary["stored_norm_p05_hits"] >= summary["exact_p05_hits"] - 1
            else "stop-stored-norm-scorer"
        )
    else:
        decision = "diagnostic-only"
    if result.get("decision") != decision:
        raise ValueError("returned replay decision differs")
    validation = {
        "schema": "borsuk-two-bit-returned-validation-v2",
        "valid": True,
        "query_count": len(samples),
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "result_sha256": hashlib.sha256(result_body).hexdigest(),
    }
    with (out / "returned-validation.json").open("xb") as file:
        file.write((json.dumps(validation, sort_keys=True, separators=(",", ":")) + "\n").encode())
    return validation


def run_closed_validation(root: Path, out: Path) -> dict[str, object]:
    from scripts.native_two_bit_returned_replay import load_closed_inputs

    return validate_loaded(*load_closed_inputs(root), out)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run_closed_validation(args.root, args.out),
                     sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
