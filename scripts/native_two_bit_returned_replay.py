"""Offline replay of the terminal-closed two-bit code-wave candidate roster."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

LEGACY_TERMINAL_SHA256 = "82875585b8854d0a5f20ff4ebafe629e50ff64808ae8d0f577a1e1935e9caf19"
LEGACY_ARTIFACTS = {
    "groups": "groups.bin",
    "mean": "mean.bin",
    "code-seal": "seal.json",
    "evidence": "evidence.json",
}


def authenticate_legacy_artifacts(
    root: Path, terminal_sha256: str = LEGACY_TERMINAL_SHA256,
) -> dict[str, dict[str, object]]:
    """Bind the four input files to the complete closed terminal receipt."""
    terminal_body = (root / "terminal.json").read_bytes()
    if hashlib.sha256(terminal_body).hexdigest() != terminal_sha256:
        raise ValueError("returned legacy terminal identity differs")
    try:
        terminal = json.loads(terminal_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("returned legacy terminal JSON differs") from error
    if (
        type(terminal) is not dict
        or terminal_body != (json.dumps(terminal, sort_keys=True,
                                         separators=(",", ":")) + "\n").encode()
        or terminal.get("status") != "complete"
        or terminal.get("exit_code") != 0
        or type(terminal.get("artifacts")) is not dict
    ):
        raise ValueError("returned legacy terminal authority differs")
    receipts: dict[str, dict[str, object]] = {}
    for role, filename in LEGACY_ARTIFACTS.items():
        receipt = terminal["artifacts"].get(role)
        body = (root / filename).read_bytes()
        if (
            type(receipt) is not dict
            or receipt.get("role") != role
            or type(receipt.get("uri")) is not str
            or not receipt["uri"].startswith("s3://")
            or receipt.get("sha256") != hashlib.sha256(body).hexdigest()
            or receipt.get("encoded_bytes") != len(body)
        ):
            raise ValueError("returned legacy artifact identity differs")
        receipts[role] = receipt
    return receipts


def selected_positions(sample: object, codes: object) -> tuple[int, ...]:
    """Map each sealed selected group to physical code-record positions."""
    selected = getattr(sample, "group_ranges", None)
    sealed = getattr(codes, "group_ranges", None)
    counts = getattr(codes, "page_row_counts", None)
    if (
        type(selected) is not tuple or type(sealed) is not tuple
        or type(counts) is not tuple or not counts
        or any(type(count) is not int or count <= 0 for count in counts)
        or not selected or len(selected) > 32
    ):
        raise ValueError("returned group authority differs")
    offsets = [0]
    for count in counts:
        offsets.append(offsets[-1] + count)
    seen: set[int] = set()
    positions: list[int] = []
    encoded = 0
    for group in selected:
        if (
            type(group) is not tuple or len(group) != 5
            or group not in sealed
            or any(type(value) is not int for value in group[:4])
            or not 0 <= group[0] < group[1] <= len(counts)
        ):
            raise ValueError("returned group identity differs")
        first, end, _, length, _ = group
        if seen.intersection(range(first, end)):
            raise ValueError("returned group overlap differs")
        seen.update(range(first, end))
        encoded += length
        positions.extend(range(offsets[first], offsets[end]))
    if (
        type(getattr(sample, "code_gets", None)) is not int
        or sample.code_gets != len(selected)
        or type(getattr(sample, "code_bytes", None)) is not int
        or sample.code_bytes != encoded
        or encoded > 16_777_216
    ):
        raise ValueError("returned code-wave budget differs")
    return tuple(positions)


def replay_query(
    sample: object, codes: object, query: object, vectors: object,
    stable_ids: tuple[bytes, ...], truth_ids: tuple[bytes, ...], *,
    top_k: int = 100,
) -> dict[str, int]:
    """Score the identical closed code-wave rows in primary and exact arms."""
    from scripts.native_rotated_two_bit_evaluation import exact_scores, score_records
    from scripts.native_two_bit_returned_recall import ranked_hits

    positions = selected_positions(sample, codes)
    if (
        not 0 < top_k <= len(positions)
        or type(getattr(sample, "query_ordinal", None)) is not int
        or len(codes.source_ordinals) != len(codes.records)
    ):
        raise ValueError("returned query authority differs")
    sources = tuple(codes.source_ordinals[position] for position in positions)
    primary, _ = score_records(
        query, codes.mean, codes.records[list(positions)],
        rotation_seed=codes.rotation_seed,
    )
    exact = exact_scores(query, vectors, sources)
    two_bit_hits = ranked_hits(primary, sources, stable_ids, truth_ids, top_k=top_k)
    exact_hits = ranked_hits(exact, sources, stable_ids, truth_ids, top_k=top_k)
    return {
        "query_ordinal": sample.query_ordinal,
        "candidate_rows": len(positions),
        "code_gets": sample.code_gets,
        "code_bytes": sample.code_bytes,
        "two_bit_hits": two_bit_hits,
        "exact_hits": exact_hits,
        "paired_loss": exact_hits - two_bit_hits,
    }


def summarize_results(
    cases: list[dict[str, int]], *, expected_queries: int, top_k: int = 100,
) -> dict[str, int]:
    """Keep returned mean, marginal tail and paired loss as distinct metrics."""
    fields = {
        "query_ordinal", "candidate_rows", "code_gets", "code_bytes",
        "two_bit_hits", "exact_hits", "paired_loss",
    }
    if (
        type(expected_queries) is not int or expected_queries <= 0
        or type(top_k) is not int or top_k <= 0
        or len(cases) != expected_queries
    ):
        raise ValueError("returned cohort authority differs")
    for ordinal, case in enumerate(cases):
        if (
            type(case) is not dict or set(case) != fields
            or any(type(value) is not int for value in case.values())
            or case["query_ordinal"] != ordinal
            or case["candidate_rows"] < top_k
            or not 0 < case["code_gets"] <= 32
            or not 0 < case["code_bytes"] <= 16_777_216
            or not 0 <= case["two_bit_hits"] <= top_k
            or not 0 <= case["exact_hits"] <= top_k
            or case["paired_loss"] != case["exact_hits"] - case["two_bit_hits"]
        ):
            raise ValueError("returned per-query authority differs")
    percentile05 = math.ceil(expected_queries * 0.05) - 1
    percentile95 = math.ceil(expected_queries * 0.95) - 1
    primary = sorted(case["two_bit_hits"] for case in cases)
    exact = sorted(case["exact_hits"] for case in cases)
    losses = sorted(case["paired_loss"] for case in cases)
    return {
        "query_count": expected_queries,
        "top_k": top_k,
        "two_bit_recall_at_k_ppm": sum(primary) * 1_000_000 // (expected_queries * top_k),
        "exact_recall_at_k_ppm": sum(exact) * 1_000_000 // (expected_queries * top_k),
        "two_bit_p05_hits": primary[percentile05],
        "exact_p05_hits": exact[percentile05],
        "p95_paired_loss_hits": losses[percentile95],
        "net_paired_loss_hits": sum(losses),
        "two_bit_sub90_count": sum(value < math.ceil(top_k * 0.9) for value in primary),
        "exact_sub90_count": sum(value < math.ceil(top_k * 0.9) for value in exact),
        "maximum_code_gets": max(case["code_gets"] for case in cases),
        "maximum_code_bytes": max(case["code_bytes"] for case in cases),
    }


def replay_loaded(
    queries: object, vectors: object, stable_ids: tuple[bytes, ...],
    truth: tuple[tuple[bytes, ...], ...], codes: object,
    samples: tuple[object, ...], out: Path, *, top_k: int = 100,
) -> dict[str, int]:
    """Evaluate an already authenticated frozen roster and write canonical files."""
    if (
        len(samples) != len(queries) or len(truth) != len(queries)
        or len(stable_ids) != len(vectors)
        or len(codes.source_ordinals) != len(vectors)
        or len(set(stable_ids)) != len(stable_ids)
        or not out.is_dir()
    ):
        raise ValueError("returned replay cohort differs")
    cases = [
        replay_query(sample, codes, queries[ordinal], vectors, stable_ids,
                     truth[ordinal], top_k=top_k)
        for ordinal, sample in enumerate(samples)
    ]
    summary = summarize_results(cases, expected_queries=len(samples), top_k=top_k)
    evidence_body = (
        json.dumps({"schema": "borsuk-two-bit-returned-evidence-v1", "samples": cases},
                   sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    if top_k == 100 and len(samples) == 1000:
        decision = (
            "advance-fidelity-only"
            if summary["net_paired_loss_hits"] <= 250
            and summary["two_bit_p05_hits"] >= summary["exact_p05_hits"] - 1
            else "stop-two-bit-sole-scorer"
        )
    else:
        decision = "diagnostic-only"
    result_body = (
        json.dumps({
            "schema": "borsuk-two-bit-returned-result-v1",
            "dataset": "ReLAION-100k", "split": "development",
            "legacy_terminal_sha256": LEGACY_TERMINAL_SHA256,
            "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
            "summary": summary, "decision": decision,
        }, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    with (out / "returned-evidence.json").open("xb") as file:
        file.write(evidence_body)
    with (out / "returned-result.json").open("xb") as file:
        file.write(result_body)
    return summary


def run_closed_replay(root: Path, out: Path) -> dict[str, int]:
    """Load only pinned closed artifacts before evaluating returned recall."""
    from scripts.native_geometric_layout_screen import ArtifactIdentity
    from scripts.native_page_microcluster_cell import (
        FROZEN_INPUTS,
        _read_inputs,
        _read_queries_truth,
    )
    from scripts.native_rotated_two_bit_cell import PRIOR_TREE, ROTATION_SEED
    from scripts.native_rotated_two_bit_codes import (
        TwoBitIdentities,
        read_two_bit_codes,
    )
    from scripts.native_rotated_two_bit_evidence import read_two_bit_evidence

    receipts = authenticate_legacy_artifacts(root)

    def identity(role: str, name: str) -> ArtifactIdentity:
        receipt = receipts[role]
        return ArtifactIdentity(
            name, receipt["uri"], receipt["sha256"], receipt["encoded_bytes"]
        )

    inputs = FROZEN_INPUTS
    ids, vectors, membership = _read_inputs(root, inputs)
    codes = read_two_bit_codes(
        root,
        TwoBitIdentities(
            identity("mean", "rotated-two-bit-mean"),
            identity("groups", "rotated-two-bit-groups"),
            identity("code-seal", "rotated-two-bit-seal"),
        ),
        ids, membership,
        bytes.fromhex(inputs.layout.source.sha256),
        bytes.fromhex(inputs.membership.sha256),
        bytes.fromhex(PRIOR_TREE.sha256),
        dimensions=inputs.layout.dimensions,
        layout_seed=inputs.layout.seed,
        rotation_seed=ROTATION_SEED,
    )
    samples, _ = read_two_bit_evidence(
        root / "evidence.json", identity("evidence", "rotated-two-bit-evidence")
    )
    queries, truth = _read_queries_truth(root, inputs)
    if len(samples) != 1000 or len(queries) != 1000:
        raise ValueError("returned closed query roster differs")
    return replay_loaded(
        queries, vectors, tuple(ids), tuple(tuple(values) for values in truth),
        codes, samples, out,
    )
