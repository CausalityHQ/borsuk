#!/usr/bin/env python3
"""Bounded residual-correction primitives for the V31 100K falsifier."""

from __future__ import annotations

import hashlib
import json
import math
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass

import numpy as np

from scripts.run_v30_variable_rate_reproduction import (
    ArtifactAuthority,
    Pq8Model,
    V30ReproductionPlan,
    _encoded_page_sizes,
    _s3_getter,
    build_base_page_layout,
    encode_pq8,
    exact_truth,
    fit_pq8,
    load_frozen_reproduction,
    parse_args,
    select_high_fidelity,
    validate_reproduction_authority,
)

ARM_NAMES = (
    "none",
    "u8-error",
    "sign8",
    "sign16",
    "exact-error",
    "exact-cross-term",
)
MAX_CANDIDATE_DEPTH = 12_288
PAGE_COUNT = 10
MAX_ENCODED_BYTES = 4_587_520
MAX_SCANNED_CODES = 1_000_000
QUERY_COUNT = 32
RECALL_K = 10


@dataclass(frozen=True)
class V31ResidualObservation:
    """Raw bounded evidence for one preregistered correction arm."""

    arm: str
    hits: tuple[int, ...]
    selected_page_counts: tuple[int, ...]
    maximum_encoded_bytes: int
    maximum_scanned_codes: int
    maximum_candidates_retained: int


def _matrix(value: object, role: str) -> np.ndarray:
    if (
        not isinstance(value, np.ndarray)
        or value.dtype != np.float32
        or value.ndim != 2
        or not value.size
        or not np.isfinite(value).all()
    ):
        raise ValueError(f"V31 {role} differs")
    return value


def quantize_squared_error_u8(
    squared_errors: np.ndarray, leaf_ordinals: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Quantize squared reconstruction error with one corpus-only scale per leaf."""

    if (
        not isinstance(squared_errors, np.ndarray)
        or squared_errors.dtype != np.float32
        or squared_errors.ndim != 1
        or not len(squared_errors)
        or not np.isfinite(squared_errors).all()
        or np.any(squared_errors < 0)
        or not isinstance(leaf_ordinals, np.ndarray)
        or leaf_ordinals.ndim != 1
        or len(leaf_ordinals) != len(squared_errors)
        or not np.issubdtype(leaf_ordinals.dtype, np.integer)
        or np.any(leaf_ordinals < 0)
    ):
        raise ValueError("V31 squared error authority differs")
    leaf_count = int(np.max(leaf_ordinals)) + 1
    if set(int(value) for value in np.unique(leaf_ordinals)) != set(range(leaf_count)):
        raise ValueError("V31 squared error leaf coverage differs")
    codes = np.zeros(len(squared_errors), dtype=np.uint8)
    steps = np.zeros(leaf_count, dtype=np.float32)
    decoded = np.zeros(len(squared_errors), dtype=np.float32)
    for leaf in range(leaf_count):
        rows = np.flatnonzero(leaf_ordinals == leaf)
        maximum = float(np.max(squared_errors[rows]))
        if maximum == 0.0:
            continue
        step = np.float32(maximum / 255.0)
        if not math.isfinite(float(step)) or step <= 0:
            raise ValueError("V31 squared error scale differs")
        steps[leaf] = step
        quantized = np.rint(squared_errors[rows] / step)
        quantized = np.clip(quantized, 0, 255).astype(np.uint8)
        codes[rows] = quantized
        decoded[rows] = quantized.astype(np.float32) * step
    if not np.isfinite(decoded).all() or np.any(decoded < 0):
        raise ValueError("V31 squared error quantization differs")
    return codes, steps, decoded


def residual_projection_matrix(dimensions: int, bits: int, seed_sha256: str) -> np.ndarray:
    """Return the fixed diagnostic Gaussian matrix derived from immutable authority."""

    if (
        type(dimensions) is not int
        or dimensions <= 0
        or bits not in {8, 16, 32}
        or type(seed_sha256) is not str
        or len(seed_sha256) != 64
        or any(character not in "0123456789abcdef" for character in seed_sha256)
    ):
        raise ValueError("V31 projection authority differs")
    seed_bytes = hashlib.sha256(
        bytes.fromhex(seed_sha256) + b"borsuk-v31-residual-sketch-v1"
    ).digest()
    seed = int.from_bytes(seed_bytes[:16], "little")
    generator = np.random.Generator(np.random.PCG64(seed))
    matrix = generator.standard_normal((bits, dimensions), dtype=np.float32)
    if not np.isfinite(matrix).all():
        raise ValueError("V31 projection values differ")
    return matrix


def correct_residual_scores(
    adc_scores: np.ndarray,
    residual_errors: np.ndarray,
    reconstructions: np.ndarray,
    query_residual: np.ndarray,
    *,
    mode: str,
    projection: np.ndarray | None = None,
    squared_error_estimate: np.ndarray | None = None,
) -> np.ndarray:
    """Apply one fixed residual correction to already-bounded row candidates."""

    errors = _matrix(residual_errors, "residual errors")
    reconstructed = _matrix(reconstructions, "reconstructions")
    if (
        errors.shape != reconstructed.shape
        or not isinstance(adc_scores, np.ndarray)
        or adc_scores.dtype != np.float32
        or adc_scores.shape != (len(errors),)
        or not np.isfinite(adc_scores).all()
        or np.any(adc_scores < 0)
        or not isinstance(query_residual, np.ndarray)
        or query_residual.dtype != np.float32
        or query_residual.shape != (errors.shape[1],)
        or not np.isfinite(query_residual).all()
        or mode not in {"exact-error", "exact-cross-term", "sign8", "sign16"}
    ):
        raise ValueError("V31 residual correction input differs")
    squared_error = np.sum(errors * errors, axis=1, dtype=np.float32)
    estimated_error = squared_error
    if squared_error_estimate is not None:
        if (
            not isinstance(squared_error_estimate, np.ndarray)
            or squared_error_estimate.dtype != np.float32
            or squared_error_estimate.shape != squared_error.shape
            or not np.isfinite(squared_error_estimate).all()
            or np.any(squared_error_estimate < 0)
        ):
            raise ValueError("V31 residual error estimate differs")
        estimated_error = squared_error_estimate
    if mode == "exact-error":
        corrected = adc_scores - estimated_error
    else:
        delta = query_residual[None, :] - reconstructed
        if mode == "exact-cross-term":
            dot = np.sum(delta * errors, axis=1, dtype=np.float32)
        else:
            bits = int(mode.removeprefix("sign"))
            if (
                not isinstance(projection, np.ndarray)
                or projection.dtype != np.float32
                or projection.shape != (bits, errors.shape[1])
                or not np.isfinite(projection).all()
            ):
                raise ValueError("V31 residual projection differs")
            signs = np.where(errors @ projection.T >= 0.0, 1.0, -1.0).astype(np.float32)
            projected_delta = delta @ projection.T
            dot = (
                np.sqrt(estimated_error, dtype=np.float32)
                * np.float32(math.sqrt(math.pi / 2.0) / bits)
                * np.sum(signs * projected_delta, axis=1, dtype=np.float32)
            )
        corrected = adc_scores - np.float32(2.0) * dot + estimated_error
    if not np.isfinite(corrected).all():
        raise ValueError("V31 corrected score differs")
    return corrected.astype(np.float32, copy=False)


def select_residual_pages(
    scan_scores: np.ndarray,
    corrected_scores: np.ndarray,
    row_pages: np.ndarray,
    *,
    candidate_depth: int,
    page_count: int,
) -> tuple[int, ...]:
    """Retain a bounded scan frontier, then reduce corrected rows to unique pages."""

    if (
        not isinstance(scan_scores, np.ndarray)
        or scan_scores.dtype != np.float32
        or scan_scores.ndim != 1
        or not isinstance(corrected_scores, np.ndarray)
        or corrected_scores.dtype != np.float32
        or corrected_scores.shape != scan_scores.shape
        or not np.isfinite(scan_scores).all()
        or not np.isfinite(corrected_scores).all()
        or not isinstance(row_pages, np.ndarray)
        or row_pages.shape != scan_scores.shape
        or not np.issubdtype(row_pages.dtype, np.integer)
        or np.any(row_pages < 0)
        or type(candidate_depth) is not int
        or not 1 <= candidate_depth <= min(MAX_CANDIDATE_DEPTH, len(scan_scores))
        or type(page_count) is not int
        or not 1 <= page_count <= PAGE_COUNT
    ):
        raise ValueError("V31 residual page selection input differs")
    rows = np.arange(len(scan_scores), dtype=np.int64)
    scan_order = np.lexsort((rows, scan_scores))[:candidate_depth]
    corrected_order = scan_order[
        np.lexsort((scan_order, corrected_scores[scan_order]))
    ]
    selected: list[int] = []
    seen: set[int] = set()
    for row in corrected_order:
        page = int(row_pages[row])
        if page not in seen:
            seen.add(page)
            selected.append(page)
            if len(selected) == page_count:
                return tuple(selected)
    raise ValueError("V31 residual page cardinality differs")


def _reconstruct_pq8(model: Pq8Model, codes: np.ndarray) -> np.ndarray:
    if (
        type(model) is not Pq8Model
        or not isinstance(codes, np.ndarray)
        or codes.dtype != np.uint8
        or codes.ndim != 2
        or codes.shape[1] != model.width_bytes
    ):
        raise ValueError("V31 PQ8 reconstruction input differs")
    reconstruction = np.empty(
        (len(codes), model.width_bytes * model.dimensions_per_subquantizer),
        dtype=np.float32,
    )
    for subquantizer in range(model.width_bytes):
        start = subquantizer * model.dimensions_per_subquantizer
        reconstruction[:, start : start + model.dimensions_per_subquantizer] = (
            model.centroids[subquantizer, codes[:, subquantizer]]
        )
    if not np.isfinite(reconstruction).all():
        raise ValueError("V31 PQ8 reconstruction differs")
    return reconstruction


def evaluate_residual_correction_arms(
    primary: np.ndarray,
    primary_leaf: np.ndarray,
    leaf_centroids: np.ndarray,
    queries: np.ndarray,
    truth: tuple[tuple[int, ...], ...],
    base_model: Pq8Model,
    high_model: Pq8Model,
    *,
    page_rows: int,
    leaf_beam: int,
    candidate_depth: int,
    page_encoded_bytes: tuple[int, ...],
    projection_seed_sha256: str,
) -> tuple[V31ResidualObservation, ...]:
    """Evaluate the frozen correction ladder over one query-independent layout."""

    _matrix(primary, "primary matrix")
    _matrix(leaf_centroids, "leaf centroids")
    _matrix(queries, "queries")
    if (
        primary.shape[1] != leaf_centroids.shape[1]
        or queries.shape != (QUERY_COUNT, primary.shape[1])
        or not isinstance(primary_leaf, np.ndarray)
        or primary_leaf.shape != (len(primary),)
        or not np.issubdtype(primary_leaf.dtype, np.integer)
        or np.any(primary_leaf < 0)
        or np.any(primary_leaf >= len(leaf_centroids))
        or type(truth) is not tuple
        or len(truth) != QUERY_COUNT
        or any(
            type(neighbors) is not tuple
            or len(neighbors) != RECALL_K
            or len(set(neighbors)) != RECALL_K
            or any(type(row) is not int or not 0 <= row < len(primary) for row in neighbors)
            for neighbors in truth
        )
        or type(base_model) is not Pq8Model
        or type(high_model) is not Pq8Model
        or type(page_rows) is not int
        or page_rows <= 0
        or type(leaf_beam) is not int
        or not 1 <= leaf_beam <= len(leaf_centroids)
        or type(candidate_depth) is not int
        or not 1 <= candidate_depth <= min(MAX_CANDIDATE_DEPTH, len(primary))
    ):
        raise ValueError("V31 correction evaluation input differs")
    residuals = primary - leaf_centroids[primary_leaf]
    base_codes, base_errors = encode_pq8(base_model, residuals)
    high_codes, _ = encode_pq8(high_model, residuals)
    high_mask = np.zeros(len(primary), dtype=np.bool_)
    high_mask[list(select_high_fidelity(base_errors.tolist(), 50_000))] = True
    active_reconstruction = _reconstruct_pq8(base_model, base_codes)
    if high_mask.any():
        active_reconstruction[high_mask] = _reconstruct_pq8(
            high_model, high_codes[high_mask]
        )
    residual_errors = residuals - active_reconstruction
    exact_squared_error = np.sum(
        residual_errors * residual_errors, axis=1, dtype=np.float32
    )
    _, _, quantized_squared_error = quantize_squared_error_u8(
        exact_squared_error, primary_leaf
    )
    pages, row_page, leaf_rows = build_base_page_layout(
        primary_leaf,
        base_codes,
        leaf_count=len(leaf_centroids),
        page_rows=page_rows,
    )
    if (
        type(page_encoded_bytes) is not tuple
        or len(page_encoded_bytes) != len(pages)
        or any(type(value) is not int or value <= 0 for value in page_encoded_bytes)
    ):
        raise ValueError("V31 page byte authority differs")
    projections = {
        8: residual_projection_matrix(primary.shape[1], 8, projection_seed_sha256),
        16: residual_projection_matrix(primary.shape[1], 16, projection_seed_sha256),
    }
    leaf_ordinals = np.arange(len(leaf_centroids), dtype=np.int64)
    hits = {arm: [] for arm in ARM_NAMES}
    selected_counts = {arm: [] for arm in ARM_NAMES}
    maximum_bytes = {arm: 0 for arm in ARM_NAMES}
    maximum_scanned = 0
    maximum_retained = 0
    for query_index, query in enumerate(queries):
        leaf_distance = np.sum(
            (leaf_centroids - query) ** 2, axis=1, dtype=np.float32
        )
        selected_leaves = np.lexsort((leaf_ordinals, leaf_distance))[:leaf_beam]
        selected_rows = np.concatenate(
            [leaf_rows[int(leaf)] for leaf in selected_leaves if len(leaf_rows[int(leaf)])]
        )
        maximum_scanned = max(maximum_scanned, len(selected_rows))
        if len(selected_rows) > MAX_SCANNED_CODES:
            raise ValueError("V31 scanned-code bound differs")
        query_residuals = query[None, :] - leaf_centroids[primary_leaf[selected_rows]]
        adc = np.empty(len(selected_rows), dtype=np.float32)
        for leaf in selected_leaves:
            mask = primary_leaf[selected_rows] == leaf
            rows = selected_rows[mask]
            if not len(rows):
                continue
            query_residual = query - leaf_centroids[int(leaf)]
            base_mask = ~high_mask[rows]
            if base_mask.any():
                adc[np.flatnonzero(mask)[base_mask]] = base_model.score(
                    base_codes[rows[base_mask]], query_residual
                )
            if (~base_mask).any():
                adc[np.flatnonzero(mask)[~base_mask]] = high_model.score(
                    high_codes[rows[~base_mask]], query_residual
                )
        exact_error_score = adc - exact_squared_error[selected_rows]
        quantized_error_score = adc - quantized_squared_error[selected_rows]
        full_scores = {
            "none": adc,
            "u8-error": quantized_error_score,
            "exact-error": exact_error_score,
        }
        # The correction helper receives one common residual origin. Because
        # leaves differ, evaluate directional arms leaf-by-leaf before reducing.
        full_scores["exact-cross-term"] = np.sum(
            (
                query_residuals
                - active_reconstruction[selected_rows]
                - residual_errors[selected_rows]
            )
            * (
                query_residuals
                - active_reconstruction[selected_rows]
                - residual_errors[selected_rows]
            ),
            axis=1,
            dtype=np.float32,
        )
        for bits in (8, 16):
            name = f"sign{bits}"
            corrected = np.empty(len(selected_rows), dtype=np.float32)
            for leaf in selected_leaves:
                mask = primary_leaf[selected_rows] == leaf
                positions = np.flatnonzero(mask)
                rows = selected_rows[positions]
                if not len(rows):
                    continue
                corrected[positions] = correct_residual_scores(
                    adc[positions],
                    residual_errors[rows],
                    active_reconstruction[rows],
                    query - leaf_centroids[int(leaf)],
                    mode=name,
                    projection=projections[bits],
                    squared_error_estimate=quantized_squared_error[rows],
                )
            full_scores[name] = corrected
        scan_scores = {
            "none": adc,
            "u8-error": quantized_error_score,
            "sign8": quantized_error_score,
            "sign16": quantized_error_score,
            "exact-error": exact_error_score,
            "exact-cross-term": full_scores["exact-cross-term"],
        }
        depth = min(candidate_depth, len(selected_rows))
        maximum_retained = max(maximum_retained, depth)
        for arm in ARM_NAMES:
            selected_pages = select_residual_pages(
                scan_scores[arm],
                full_scores[arm],
                row_page[selected_rows],
                candidate_depth=depth,
                page_count=PAGE_COUNT,
            )
            exact_rows = np.concatenate([pages[page] for page in selected_pages])
            distances = np.sum(
                (primary[exact_rows] - query) ** 2, axis=1, dtype=np.float32
            )
            take = min(RECALL_K, len(exact_rows))
            local = np.argpartition(distances, take - 1)[:take]
            ordered = local[np.lexsort((exact_rows[local], distances[local]))]
            matches = set(int(exact_rows[index]) for index in ordered)
            hits[arm].append(len(matches & set(truth[query_index])))
            selected_counts[arm].append(len(selected_pages))
            maximum_bytes[arm] = max(
                maximum_bytes[arm],
                sum(page_encoded_bytes[page] for page in selected_pages),
            )
    return tuple(
        V31ResidualObservation(
            arm=arm,
            hits=tuple(hits[arm]),
            selected_page_counts=tuple(selected_counts[arm]),
            maximum_encoded_bytes=maximum_bytes[arm],
            maximum_scanned_codes=maximum_scanned,
            maximum_candidates_retained=maximum_retained,
        )
        for arm in ARM_NAMES
    )


def _arm_result(observation: V31ResidualObservation) -> dict[str, object]:
    if (
        type(observation) is not V31ResidualObservation
        or observation.arm not in ARM_NAMES
        or type(observation.hits) is not tuple
        or len(observation.hits) != QUERY_COUNT
        or any(type(hit) is not int or not 0 <= hit <= RECALL_K for hit in observation.hits)
        or observation.selected_page_counts != (PAGE_COUNT,) * QUERY_COUNT
        or type(observation.maximum_encoded_bytes) is not int
        or not 0 < observation.maximum_encoded_bytes <= MAX_ENCODED_BYTES
        or type(observation.maximum_scanned_codes) is not int
        or not 0 < observation.maximum_scanned_codes <= MAX_SCANNED_CODES
        or type(observation.maximum_candidates_retained) is not int
        or not 0 < observation.maximum_candidates_retained <= MAX_CANDIDATE_DEPTH
    ):
        raise ValueError("V31 residual arm evidence differs")
    total = sum(observation.hits)
    return {
        "aggregate_recall_ppm": total * 1_000_000 // (QUERY_COUNT * RECALL_K),
        "arm": observation.arm,
        "maximum_candidates_retained": observation.maximum_candidates_retained,
        "maximum_encoded_bytes": observation.maximum_encoded_bytes,
        "maximum_scanned_codes": observation.maximum_scanned_codes,
        "minimum_recall_ppm": min(observation.hits) * 1_000_000 // RECALL_K,
        "perfect_queries": sum(hit == RECALL_K for hit in observation.hits),
        "selected_pages": PAGE_COUNT,
    }


def build_residual_correction_result(
    observations: tuple[V31ResidualObservation, ...],
) -> bytes:
    """Reduce the frozen six-arm ladder into canonical claim-ineligible JSON."""

    if (
        type(observations) is not tuple
        or tuple(observation.arm for observation in observations) != ARM_NAMES
    ):
        raise ValueError("V31 residual arm ordering differs")
    arms = [_arm_result(observation) for observation in observations]
    passing = [
        arm
        for arm in arms
        if arm["aggregate_recall_ppm"] == 1_000_000
        and arm["minimum_recall_ppm"] == 1_000_000
        and arm["perfect_queries"] == QUERY_COUNT
    ]
    selected = passing[0]["arm"] if passing else None
    value = {
        "arms": arms,
        "claim_eligible": False,
        "schema": "borsuk-v31-residual-correction-falsifier-v1",
        "selected_arm": selected,
        "status": "perfect-arm-found" if selected is not None else "rejected",
    }
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def encode_residual_correction_evidence(
    observations: tuple[V31ResidualObservation, ...],
) -> bytes:
    """Encode every raw arm/query observation as strict Parquet evidence."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    if (
        type(observations) is not tuple
        or tuple(observation.arm for observation in observations) != ARM_NAMES
    ):
        raise ValueError("V31 residual evidence arm ordering differs")
    rows = []
    for observation in observations:
        _arm_result(observation)
        rows.extend(
            {
                "arm": observation.arm,
                "query_ordinal": query,
                "hits": observation.hits[query],
                "selected_pages": observation.selected_page_counts[query],
            }
            for query in range(QUERY_COUNT)
        )
    schema = pa.schema(
        [
            pa.field("arm", pa.string(), nullable=False),
            pa.field("query_ordinal", pa.uint16(), nullable=False),
            pa.field("hits", pa.uint8(), nullable=False),
            pa.field("selected_pages", pa.uint8(), nullable=False),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        version="2.6",
        use_dictionary=False,
        write_statistics=True,
    )
    return sink.getvalue().to_pybytes()


def finalize_residual_correction_result(
    observations: tuple[V31ResidualObservation, ...],
    artifacts: tuple[ArtifactAuthority, ...],
    *,
    construction_bytes_streamed: int,
    evidence_parquet: bytes,
    projection_seed_sha256: str,
) -> bytes:
    """Bind canonical reduction to exact frozen inputs and raw Parquet evidence."""

    validate_reproduction_authority(
        artifacts,
        source_rows=100_000,
        query_count=QUERY_COUNT,
        truth_memberships=QUERY_COUNT * RECALL_K,
    )
    expected_evidence = encode_residual_correction_evidence(observations)
    if type(evidence_parquet) is not bytes or evidence_parquet != expected_evidence:
        raise ValueError("V31 residual evidence differs")
    if type(construction_bytes_streamed) is not int or construction_bytes_streamed <= 0:
        raise ValueError("V31 construction byte evidence differs")
    if (
        type(projection_seed_sha256) is not str
        or len(projection_seed_sha256) != 64
        or any(character not in "0123456789abcdef" for character in projection_seed_sha256)
    ):
        raise ValueError("V31 projection seed authority differs")
    value = json.loads(build_residual_correction_result(observations))
    value.update(
        {
            "artifacts": [asdict(artifact) for artifact in artifacts],
            "construction_bytes_streamed": construction_bytes_streamed,
            "evidence_parquet_bytes": len(evidence_parquet),
            "evidence_parquet_sha256": hashlib.sha256(evidence_parquet).hexdigest(),
            "projection_seed_sha256": projection_seed_sha256,
        }
    )
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def run_residual_correction_falsifier(
    plan: V30ReproductionPlan, get_object: Callable[[str], bytes]
) -> tuple[bytes, bytes]:
    """Run one bounded six-arm diagnostic over the frozen authenticated fixture."""

    if type(plan) is not V30ReproductionPlan or not callable(get_object):
        raise ValueError("V31 residual falsifier plan differs")
    loaded = load_frozen_reproduction(
        plan.artifacts,
        page_prefix=plan.page_prefix,
        get_object=get_object,
    )
    truth = exact_truth(loaded.primary, loaded.queries)
    residuals = loaded.primary - loaded.leaf_centroids[loaded.primary_leaf]
    base_model = fit_pq8(residuals, width_bytes=24)
    high_model = fit_pq8(residuals, width_bytes=48)
    base_codes, _ = encode_pq8(base_model, residuals)
    pages, _, _ = build_base_page_layout(
        loaded.primary_leaf,
        base_codes,
        leaf_count=len(loaded.leaf_centroids),
        page_rows=512,
    )
    page_sizes = _encoded_page_sizes(loaded.primary, pages)
    seed = plan.artifacts[0].sha256
    observations = evaluate_residual_correction_arms(
        loaded.primary,
        loaded.primary_leaf,
        loaded.leaf_centroids,
        loaded.queries,
        truth,
        base_model,
        high_model,
        page_rows=512,
        leaf_beam=64,
        candidate_depth=MAX_CANDIDATE_DEPTH,
        page_encoded_bytes=page_sizes,
        projection_seed_sha256=seed,
    )
    evidence = encode_residual_correction_evidence(observations)
    result = finalize_residual_correction_result(
        observations,
        plan.artifacts,
        construction_bytes_streamed=loaded.construction_bytes_streamed,
        evidence_parquet=evidence,
        projection_seed_sha256=seed,
    )
    return result, evidence


def main(arguments: list[str] | None = None) -> int:
    """Execute one explicit S3-backed diagnostic and emit canonical JSON."""

    arguments = sys.argv[1:] if arguments is None else arguments
    try:
        plan = parse_args(arguments)
        result, evidence = run_residual_correction_falsifier(plan, _s3_getter())
        plan.evidence_parquet.write_bytes(evidence)
        sys.stdout.buffer.write(result)
        return 0
    except (OSError, ValueError) as error:
        print(f"run_v31_residual_correction_falsifier: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
