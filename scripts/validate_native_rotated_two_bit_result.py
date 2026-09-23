#!/usr/bin/env python3
"""Independent source, byte, score and nomination replay for two-bit codes."""

from __future__ import annotations

import hashlib
import math
import struct
from collections import Counter
from collections.abc import Mapping, Sequence

import numpy as np

from scripts.native_geometric_layout_screen import EvaluationLimits, MembershipRow
from scripts.native_rotated_two_bit_codes import TwoBitCodes
from scripts.native_rotated_two_bit_evaluation import TwoBitSample

SCHEMA = "borsuk-rotated-two-bit-validation-v1"
ROW_BYTES = 200
PACKED_BYTES = 192
DIMENSIONS = 768
BATCH_ROWS = 4096
SCORE_BATCH = 2048


def _rotate_independently(rows: np.ndarray, seed: int) -> np.ndarray:
    signs = np.empty(DIMENSIONS, dtype=np.float64)
    for coordinate in range(DIMENSIONS):
        material = b"borsuk-sq2-sign-v1" + struct.pack("<II", seed, coordinate)
        signs[coordinate] = -1.0 if hashlib.sha256(material).digest()[0] % 2 else 1.0
    values = rows.astype(np.float64) * signs
    for start in range(0, DIMENSIONS, 256):
        block = values[:, start : start + 256]
        width = 1
        while width != 256:
            for left_index in range(0, 256, 2 * width):
                a = block[:, left_index : left_index + width].copy()
                b = block[:, left_index + width : left_index + 2 * width].copy()
                block[:, left_index : left_index + width] = a + b
                block[:, left_index + width : left_index + 2 * width] = a - b
            width *= 2
        block /= 16.0
    return values


def _reconstruct_source_records(
    vectors: np.ndarray, ordinals: Sequence[int], mean: np.ndarray, seed: int
) -> np.ndarray:
    records = np.empty((len(ordinals), ROW_BYTES), dtype=np.uint8)
    mean64 = mean.astype(np.float64)
    for first in range(0, len(ordinals), BATCH_ROWS):
        last = min(first + BATCH_ROWS, len(ordinals))
        centered = vectors[list(ordinals[first:last])].astype(np.float64) - mean64
        rotated = _rotate_independently(centered, seed)
        absolute = np.abs(rotated)
        scale = np.mean(absolute, axis=1, dtype=np.float64) / 1.6
        choices = np.empty(rotated.shape, dtype=np.int8)
        for _ in range(8):
            choices[:] = np.where(rotated < 0, -1, 1) * np.where(
                absolute > 2 * scale[:, None], 3, 1
            )
            scale = np.sum(choices * rotated, axis=1, dtype=np.float64) / np.sum(
                choices.astype(np.float64) * choices.astype(np.float64), axis=1
            )
        symbols = ((choices.astype(np.int16) + 3) // 2).astype(np.uint8)
        for byte in range(PACKED_BYTES):
            records[first:last, byte] = (
                symbols[:, byte * 4]
                | (symbols[:, byte * 4 + 1] << 2)
                | (symbols[:, byte * 4 + 2] << 4)
                | (symbols[:, byte * 4 + 3] << 6)
            )
        records[first:last, PACKED_BYTES : PACKED_BYTES + 4] = np.frombuffer(
            scale.astype("<f4").tobytes(), dtype=np.uint8
        ).reshape(-1, 4)
        norm = np.sum(centered * centered, axis=1, dtype=np.float64)
        records[first:last, PACKED_BYTES + 4 :] = np.frombuffer(
            norm.astype("<f4").tobytes(), dtype=np.uint8
        ).reshape(-1, 4)
    return records


def _decode_and_verify(
    body: bytes,
    artifacts: TwoBitCodes,
    vectors: np.ndarray,
    ordinals: Sequence[int],
) -> np.ndarray:
    mean = np.mean(vectors, axis=0, dtype=np.float64).astype(np.float32)
    if not np.array_equal(mean, artifacts.mean):
        raise ValueError("two-bit independent source mean differs")
    generated = _reconstruct_source_records(vectors, ordinals, mean, artifacts.rotation_seed)
    if not np.array_equal(generated, artifacts.records):
        raise ValueError("two-bit independent source records differ")
    counts = artifacts.page_row_counts
    offsets = [0]
    for count in counts:
        offsets.append(offsets[-1] + count)
    next_byte = 0
    for ordinal, group in enumerate(artifacts.group_ranges):
        first, end, byte_offset, length, digest = group
        rows = sum(counts[first:end])
        header_bytes = 4 + 4 * (end - first)
        if (
            first != ordinal * 4
            or end != min(first + 4, len(counts))
            or byte_offset != next_byte
            or length != header_bytes + rows * ROW_BYTES
        ):
            raise ValueError("two-bit independent group coordinates differ")
        payload = body[byte_offset : byte_offset + length]
        if len(payload) != length or hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError("two-bit independent group identity differs")
        if struct.unpack_from("<I", payload)[0] != end - first:
            raise ValueError("two-bit independent group header differs")
        for page in range(first, end):
            if struct.unpack_from("<I", payload, 4 + 4 * (page - first))[0] != counts[page]:
                raise ValueError("two-bit independent group row count differs")
        stored = np.frombuffer(payload, dtype=np.uint8, offset=header_bytes).reshape(rows, ROW_BYTES)
        if not np.array_equal(stored, generated[offsets[first] : offsets[end]]):
            raise ValueError("two-bit independent group records differ")
        next_byte += length
    if next_byte != len(body) or not artifacts.group_ranges:
        raise ValueError("two-bit independent group length differs")
    return generated


def _score_independently(
    query: np.ndarray, mean: np.ndarray, records: np.ndarray, seed: int
) -> tuple[np.ndarray, np.ndarray]:
    centered = query.astype(np.float64) - mean.astype(np.float64)
    q = _rotate_independently(centered.reshape(1, -1), seed)[0]
    qnorm = float(np.sum(centered * centered))
    primary = np.empty(len(records), dtype=np.float32)
    diagnostic = np.empty(len(records), dtype=np.float32)
    for first in range(0, len(records), SCORE_BATCH):
        last = min(first + SCORE_BATCH, len(records))
        chunk = records[first:last]
        symbols = np.empty((len(chunk), DIMENSIONS), dtype=np.uint8)
        packed = chunk[:, :PACKED_BYTES]
        for part in range(4):
            symbols[:, part::4] = np.bitwise_and(packed >> (2 * part), 3)
        levels = (2 * symbols.astype(np.int8) - 3).astype(np.float64)
        scales = np.frombuffer(chunk[:, PACKED_BYTES : PACKED_BYTES + 4].tobytes(), dtype="<f4").astype(np.float64)
        norms = np.frombuffer(chunk[:, PACKED_BYTES + 4 :].tobytes(), dtype="<f4").astype(np.float64)
        if not np.isfinite(scales).all() or not np.isfinite(norms).all() or np.any(scales < 0) or np.any(norms < 0):
            raise ValueError("two-bit independent scalar differs")
        dot = np.sum(levels * q, axis=1, dtype=np.float64)
        coded_norm = np.sum(levels * levels, axis=1, dtype=np.float64) * (scales * scales)
        primary[first:last] = (qnorm + coded_norm - 2 * scales * dot).astype(np.float32)
        diagnostic[first:last] = (qnorm + norms - 2 * scales * dot).astype(np.float32)
    return primary, diagnostic


def _exact_independently(
    query: np.ndarray, vectors: np.ndarray, sources: Sequence[int]
) -> np.ndarray:
    scores = np.empty(len(sources), dtype=np.float32)
    q = query.astype(np.float64)
    for first in range(0, len(sources), SCORE_BATCH):
        last = min(first + SCORE_BATCH, len(sources))
        delta = vectors[list(sources[first:last])].astype(np.float64) - q
        scores[first:last] = np.sum(delta * delta, axis=1).astype(np.float32)
    return scores


def _nominate_independently(
    scores: np.ndarray,
    sources: Sequence[int],
    row_pages: Sequence[int],
    page_bytes: Sequence[int],
    limits: EvaluationLimits,
) -> tuple[int, ...]:
    order = sorted(range(len(scores)), key=lambda index: (float(scores[index]), sources[index]))
    top_counts = Counter(row_pages[index] for index in order[:100])
    nearest: dict[int, float] = {}
    for index in order:
        nearest.setdefault(row_pages[index], float(scores[index]))
    nominated = sorted(top_counts, key=lambda page: (-top_counts[page], nearest[page], page))
    rest = sorted((page for page in nearest if page not in top_counts), key=lambda page: (nearest[page], page))
    selected: list[int] = []
    used = 0
    for page in nominated + rest:
        if used + page_bytes[page] <= limits.maximum_bytes:
            selected.append(page)
            used += page_bytes[page]
            if len(selected) == limits.maximum_pages:
                break
    if not selected:
        raise ValueError("two-bit independent data budget differs")
    return tuple(selected)


def _independent_metrics(samples: Sequence[TwoBitSample]) -> dict[str, int | str]:
    count = len(samples)

    def mean(field: str, top: int) -> int:
        return sum(getattr(sample, field) for sample in samples) * 1_000_000 // (count * top)

    def p05(field: str) -> int:
        values = sorted(getattr(sample, field) for sample in samples)
        return values[math.ceil(count * 0.05) - 1] * 10_000

    exact_mean, exact_p05, exact_r10 = mean("exact_hits_at_100", 100), p05("exact_hits_at_100"), mean("exact_hits_at_10", 10)
    primary_mean, primary_p05, primary_r10 = mean("primary_hits_at_100", 100), p05("primary_hits_at_100"), mean("primary_hits_at_10", 10)
    diagnostic_mean, diagnostic_p05, diagnostic_r10 = mean("diagnostic_hits_at_100", 100), p05("diagnostic_hits_at_100"), mean("diagnostic_hits_at_10", 10)
    exact_pass = exact_mean >= 975_000 and exact_p05 >= 900_000 and exact_r10 >= 960_000
    primary_pass = primary_mean >= 975_000 and primary_p05 >= 900_000 and primary_r10 >= 960_000
    diagnostic_pass = diagnostic_mean >= 975_000 and diagnostic_p05 >= 900_000 and diagnostic_r10 >= 960_000
    decision = (
        "invalid-exact-control" if not exact_pass else
        "quality-advance-memory-pending" if primary_pass else
        "estimator-failed-diagnostic-pass" if diagnostic_pass else
        "representation-killed"
    )
    return {
        "query_count": count,
        "prior_pq_mean_recall_at_100_ppm": mean("prior_pq_hits_at_100", 100),
        "prior_residual_mean_recall_at_100_ppm": mean("prior_residual_hits_at_100", 100),
        "grouped_mean_recall_at_100_ppm": mean("grouped_hits_at_100", 100),
        "exact_mean_recall_at_100_ppm": exact_mean,
        "exact_p05_recall_at_100_ppm": exact_p05,
        "exact_recall_at_10_ppm": exact_r10,
        "primary_mean_recall_at_100_ppm": primary_mean,
        "primary_p05_recall_at_100_ppm": primary_p05,
        "primary_recall_at_10_ppm": primary_r10,
        "primary_worst_recall_at_100_ppm": min(sample.primary_hits_at_100 for sample in samples) * 10_000,
        "diagnostic_mean_recall_at_100_ppm": diagnostic_mean,
        "diagnostic_p05_recall_at_100_ppm": diagnostic_p05,
        "diagnostic_recall_at_10_ppm": diagnostic_r10,
        "max_code_gets": max(sample.code_gets for sample in samples),
        "max_code_bytes": max(sample.code_bytes for sample in samples),
        "max_data_pages": max(max(len(sample.exact_pages), len(sample.primary_pages), len(sample.diagnostic_pages)) for sample in samples),
        "max_data_bytes": max(max(sample.exact_data_bytes, sample.primary_data_bytes, sample.diagnostic_data_bytes) for sample in samples),
        "decision": decision,
    }


def validate_two_bit_samples(
    samples: Sequence[TwoBitSample],
    metrics: Mapping[str, int | str],
    *,
    queries: np.ndarray,
    truth: Sequence[Sequence[bytes]],
    retained_routes: Sequence[Sequence[int]],
    ids: Sequence[bytes],
    vectors: np.ndarray,
    membership: Sequence[MembershipRow],
    artifacts: TwoBitCodes,
    group_bytes: bytes,
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
    prior_group_samples: Sequence[object],
) -> dict[str, object]:
    """Independently rebuild source bytes and every paired query outcome."""
    if (
        not samples
        or type(queries) is not np.ndarray
        or queries.dtype != np.float32
        or queries.shape != (len(samples), DIMENSIONS)
        or type(vectors) is not np.ndarray
        or vectors.dtype != np.float32
        or vectors.shape != (len(ids), DIMENSIONS)
        or len(membership) != len(ids)
        or len(truth) != len(samples)
        or len(retained_routes) != len(samples)
        or len(prior_group_samples) != len(samples)
        or len(page_byte_sizes) != len(artifacts.page_row_counts)
        or type(group_bytes) is not bytes
        or type(limits) is not EvaluationLimits
        or not np.isfinite(queries).all()
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("two-bit independent authority differs")
    rows = sorted(membership, key=lambda row: (row.page_ordinal, row.in_page_ordinal))
    ordinals = tuple(row.source_ordinal for row in rows)
    counts = artifacts.page_row_counts
    offsets = [0]
    for count in counts:
        offsets.append(offsets[-1] + count)
    if (
        ordinals != artifacts.source_ordinals
        or sorted(ordinals) != list(range(len(ids)))
        or any(row.stable_id != ids[row.source_ordinal] for row in rows)
        or any(
            row.page_ordinal != page or row.in_page_ordinal != index
            for page in range(len(counts))
            for index, row in enumerate(rows[offsets[page] : offsets[page + 1]])
        )
    ):
        raise ValueError("two-bit independent physical order differs")
    records = _decode_and_verify(group_bytes, artifacts, vectors, ordinals)
    owners = {row.stable_id: row.page_ordinal for row in rows}
    if len(owners) != len(ids):
        raise ValueError("two-bit independent owners differ")
    expected_samples: list[TwoBitSample] = []
    for query_ordinal, recorded in enumerate(samples):
        route = tuple(retained_routes[query_ordinal])
        if not route or len(set(route)) != len(route):
            raise ValueError("two-bit independent retained route differs")
        selected_groups: list[int] = []
        for page in route:
            if type(page) is not int or not 0 <= page < len(counts):
                raise ValueError("two-bit independent retained route differs")
            group = page // 4
            if group not in selected_groups:
                selected_groups.append(group)
                if len(selected_groups) == 32:
                    break
        ranges = tuple(artifacts.group_ranges[group] for group in selected_groups)
        code_bytes = sum(group[3] for group in ranges)
        if len(ranges) > 32 or code_bytes > 16_777_216:
            raise ValueError("two-bit independent code wave differs")
        grouped_pages = tuple(page for group in ranges for page in range(group[0], group[1]))
        positions = np.concatenate(
            [np.arange(offsets[page], offsets[page + 1]) for page in grouped_pages]
        )
        sources = tuple(ordinals[position] for position in positions)
        row_pages = tuple(page for page in grouped_pages for _ in range(counts[page]))
        selected = records[positions]
        primary_scores, diagnostic_scores = _score_independently(
            queries[query_ordinal], artifacts.mean, selected, artifacts.rotation_seed
        )
        exact = _exact_independently(queries[query_ordinal], vectors, sources)
        exact_pages = _nominate_independently(exact, sources, row_pages, page_byte_sizes, limits)
        primary_pages = _nominate_independently(primary_scores, sources, row_pages, page_byte_sizes, limits)
        diagnostic_pages = _nominate_independently(diagnostic_scores, sources, row_pages, page_byte_sizes, limits)
        prior = prior_group_samples[query_ordinal]
        if (
            prior.query_ordinal != query_ordinal
            or tuple(prior.retained_pages) != route
            or tuple(prior.grouped_pages) != grouped_pages
            or tuple(prior.exact_pages) != exact_pages
        ):
            raise ValueError("two-bit closed exact control differs")
        truth_ids = tuple(truth[query_ordinal])
        if len(truth_ids) != 100 or len(set(truth_ids)) != 100 or any(value not in owners for value in truth_ids):
            raise ValueError("two-bit independent truth differs")
        grouped_set = set(grouped_pages)
        exact_set = set(exact_pages)
        primary_set = set(primary_pages)
        diagnostic_set = set(diagnostic_pages)
        exact_hits10 = sum(owners[value] in exact_set for value in truth_ids[:10])
        exact_hits100 = sum(owners[value] in exact_set for value in truth_ids)
        if exact_hits10 != prior.exact_hits_at_10 or exact_hits100 != prior.exact_hits_at_100:
            raise ValueError("two-bit closed exact control differs")
        expected = TwoBitSample(
            query_ordinal=query_ordinal,
            retained_pages=route,
            group_ranges=ranges,
            grouped_pages=grouped_pages,
            code_gets=len(ranges),
            code_bytes=code_bytes,
            grouped_hits_at_10=sum(owners[value] in grouped_set for value in truth_ids[:10]),
            grouped_hits_at_100=sum(owners[value] in grouped_set for value in truth_ids),
            exact_pages=exact_pages,
            exact_data_bytes=sum(page_byte_sizes[page] for page in exact_pages),
            exact_hits_at_10=exact_hits10,
            exact_hits_at_100=exact_hits100,
            primary_pages=primary_pages,
            primary_data_bytes=sum(page_byte_sizes[page] for page in primary_pages),
            primary_hits_at_10=sum(owners[value] in primary_set for value in truth_ids[:10]),
            primary_hits_at_100=sum(owners[value] in primary_set for value in truth_ids),
            diagnostic_pages=diagnostic_pages,
            diagnostic_data_bytes=sum(page_byte_sizes[page] for page in diagnostic_pages),
            diagnostic_hits_at_10=sum(owners[value] in diagnostic_set for value in truth_ids[:10]),
            diagnostic_hits_at_100=sum(owners[value] in diagnostic_set for value in truth_ids),
            prior_pq_hits_at_100=prior.prior_pq_hits_at_100,
            prior_residual_hits_at_100=prior.prior_residual_hits_at_100,
        )
        if recorded != expected:
            raise ValueError(f"two-bit independent sample {query_ordinal} differs")
        expected_samples.append(expected)
    independent = _independent_metrics(expected_samples)
    if dict(metrics) != independent:
        raise ValueError("two-bit independent aggregate differs")
    return {"schema": SCHEMA, "claim_eligible": False, "decision": independent["decision"], "metrics": independent}
