#!/usr/bin/env python3
"""Independent direct-vector replay of grouped page-centered code evidence."""

from __future__ import annotations

import hashlib
import math
import struct
from collections import Counter
from collections.abc import Mapping, Sequence

import numpy as np

from scripts.native_geometric_layout_screen import EvaluationLimits, MembershipRow
from scripts.native_page_centered_group_codes import GroupCodes
from scripts.native_page_centered_group_evaluation import GroupSample

SCHEMA = "borsuk-page-centered-group-validation-v1"


def _nominate_independently(
    scores: np.ndarray,
    source_ordinals: Sequence[int],
    row_pages: Sequence[int],
    page_bytes: Sequence[int],
    limits: EvaluationLimits,
) -> tuple[int, ...]:
    ordered = sorted(
        range(len(scores)), key=lambda position: (float(scores[position]), source_ordinals[position])
    )
    top_counts = Counter(row_pages[position] for position in ordered[:100])
    nearest: dict[int, float] = {}
    for position in ordered:
        page = row_pages[position]
        if page not in nearest:
            nearest[page] = float(scores[position])
    nominated = sorted(top_counts, key=lambda page: (-top_counts[page], nearest[page], page))
    rest = sorted(
        (page for page in nearest if page not in top_counts),
        key=lambda page: (nearest[page], page),
    )
    selected: list[int] = []
    used = 0
    for page in nominated + rest:
        if used + page_bytes[page] <= limits.maximum_bytes:
            selected.append(page)
            used += page_bytes[page]
            if len(selected) == limits.maximum_pages:
                break
    if not selected:
        raise ValueError("group independent data budget differs")
    return tuple(selected)


def _decode_and_verify(
    body: bytes,
    artifacts: GroupCodes,
    vectors: np.ndarray,
    ordinals: Sequence[int],
) -> tuple[np.ndarray, np.ndarray]:
    counts = artifacts.page_row_counts
    dimensions = vectors.shape[1]
    page_means = np.empty((len(counts), dimensions), dtype=np.float32)
    codes = np.empty((len(ordinals), 48), dtype=np.uint8)
    offsets = [0]
    for count in counts:
        offsets.append(offsets[-1] + count)
    next_offset = 0
    for group_ordinal, item in enumerate(artifacts.group_ranges):
        first, end, byte_offset, length, digest = item
        if (
            first != group_ordinal * 4
            or end != min(first + 4, len(counts))
            or byte_offset != next_offset
            or length <= 0
        ):
            raise ValueError("group independent coordinates differ")
        payload = body[byte_offset : byte_offset + length]
        if len(payload) != length or hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError("group independent range identity differs")
        header_length = 4 + (end - first) * (4 + dimensions * 4)
        if (
            length != header_length + (offsets[end] - offsets[first]) * 48
            or struct.unpack_from("<I", payload)[0] != end - first
        ):
            raise ValueError("group independent range layout differs")
        for page in range(first, end):
            local = 4 + (page - first) * (4 + dimensions * 4)
            if struct.unpack_from("<I", payload, local)[0] != counts[page]:
                raise ValueError("group independent page rows differs")
            decoded_mean = np.frombuffer(
                payload, dtype="<f4", count=dimensions, offset=local + 4
            )
            source_rows = np.asarray(ordinals[offsets[page] : offsets[page + 1]], dtype=np.int64)
            source_mean = vectors[source_rows].mean(axis=0, dtype=np.float64).astype(np.float32)
            if not np.array_equal(decoded_mean, source_mean):
                raise ValueError("group independent source mean differs")
            page_means[page] = decoded_mean
        decoded_codes = np.frombuffer(
            payload, dtype=np.uint8, offset=header_length
        ).reshape(offsets[end] - offsets[first], 48)
        codes[offsets[first] : offsets[end]] = decoded_codes
        next_offset += length
    if (
        next_offset != len(body)
        or not np.array_equal(page_means, artifacts.page_means)
        or not np.array_equal(codes, artifacts.codes)
    ):
        raise ValueError("group independent artifact differs")
    return page_means, codes


def _direct_coded_scores(
    query: np.ndarray,
    books: np.ndarray,
    means: np.ndarray,
    codes: np.ndarray,
    row_pages: Sequence[int],
) -> np.ndarray:
    dimensions = query.size
    width = dimensions // 48
    q = query.astype(np.float64)
    scores = np.empty(len(codes), dtype=np.float32)
    pages = np.asarray(row_pages, dtype=np.int64)
    for first in range(0, len(codes), 512):
        end = min(first + 512, len(codes))
        reconstruction = means[pages[first:end]].astype(np.float64)
        for subspace in range(48):
            lo = subspace * width
            reconstruction[:, lo : lo + width] += books[
                subspace, codes[first:end, subspace]
            ].astype(np.float64)
        delta = q - reconstruction
        scores[first:end] = np.sum(delta * delta, axis=1).astype(np.float32)
    return scores


def validate_group_samples(
    samples: Sequence[GroupSample],
    metrics: Mapping[str, int | str],
    *,
    queries: np.ndarray,
    truth: Sequence[Sequence[bytes]],
    retained_routes: Sequence[Sequence[int]],
    ids: Sequence[bytes],
    vectors: np.ndarray,
    membership: Sequence[MembershipRow],
    artifacts: GroupCodes,
    group_bytes: bytes,
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
    prior_hits: Sequence[tuple[int, int]],
) -> dict[str, object]:
    """Recompute source means, group ranges, direct scores and every outcome."""
    if (
        not samples
        or type(queries) is not np.ndarray
        or queries.dtype != np.float32
        or queries.shape != (len(samples), vectors.shape[1])
        or type(vectors) is not np.ndarray
        or vectors.dtype != np.float32
        or vectors.shape != (len(ids), queries.shape[1])
        or len(membership) != len(ids)
        or len(truth) != len(samples)
        or len(retained_routes) != len(samples)
        or len(prior_hits) != len(samples)
        or len(page_byte_sizes) != len(artifacts.page_row_counts)
        or type(group_bytes) is not bytes
        or type(limits) is not EvaluationLimits
        or not np.isfinite(queries).all()
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("group independent authority differs")
    rows = sorted(membership, key=lambda row: (row.page_ordinal, row.in_page_ordinal))
    ordinals = tuple(row.source_ordinal for row in rows)
    counts = artifacts.page_row_counts
    if (
        ordinals != artifacts.source_ordinals
        or sorted(ordinals) != list(range(len(ids)))
        or any(row.stable_id != ids[row.source_ordinal] for row in rows)
        or any(row.page_ordinal != page or row.in_page_ordinal != index
               for page in range(len(counts))
               for index, row in enumerate(
                   rows[sum(counts[:page]) : sum(counts[:page + 1])]
               ))
    ):
        raise ValueError("group independent physical order differs")
    means, codes = _decode_and_verify(group_bytes, artifacts, vectors, ordinals)
    offsets = np.concatenate(([0], np.cumsum(counts, dtype=np.int64)))
    owner = {row.stable_id: row.page_ordinal for row in rows}
    if len(owner) != len(ids):
        raise ValueError("group independent owner mapping differs")
    expected_samples: list[GroupSample] = []
    for query_ordinal, recorded in enumerate(samples):
        retained = tuple(retained_routes[query_ordinal])
        if len(set(retained)) != len(retained):
            raise ValueError("group independent retained route differs")
        selected_groups: list[int] = []
        for page in retained:
            if type(page) is not int or not 0 <= page < len(counts):
                raise ValueError("group independent retained route differs")
            group = page // 4
            if group not in selected_groups:
                selected_groups.append(group)
                if len(selected_groups) == 32:
                    break
        ranges = tuple(artifacts.group_ranges[group] for group in selected_groups)
        code_bytes = sum(group[3] for group in ranges)
        if len(ranges) > 32 or code_bytes > 16_777_216:
            raise ValueError("group independent code wave differs")
        grouped_pages = tuple(
            page for group in ranges for page in range(group[0], group[1])
        )
        positions = np.concatenate(
            [np.arange(offsets[page], offsets[page + 1]) for page in grouped_pages]
        )
        row_sources = tuple(ordinals[position] for position in positions)
        row_pages = tuple(
            page for page in grouped_pages for _ in range(counts[page])
        )
        code_scores = _direct_coded_scores(
            queries[query_ordinal], artifacts.books, means, codes[positions], row_pages
        )
        exact_delta = vectors[list(row_sources)].astype(np.float64) - queries[
            query_ordinal
        ].astype(np.float64)
        exact_scores = np.sum(exact_delta * exact_delta, axis=1)
        coded_pages = _nominate_independently(
            code_scores, row_sources, row_pages, page_byte_sizes, limits
        )
        exact_pages = _nominate_independently(
            exact_scores, row_sources, row_pages, page_byte_sizes, limits
        )
        truth_ids = tuple(truth[query_ordinal])
        if len(truth_ids) != 100 or len(set(truth_ids)) != 100 or any(value not in owner for value in truth_ids):
            raise ValueError("group independent truth differs")
        grouped_set = set(grouped_pages)
        per_page = Counter(owner[value] for value in truth_ids if owner[value] in grouped_set)
        oracle_pages = tuple(
            sorted(grouped_set, key=lambda page: (-per_page[page], page))[
                : limits.maximum_pages
            ]
        )
        if sum(page_byte_sizes[page] for page in oracle_pages) > limits.maximum_bytes:
            raise ValueError("group independent oracle bytes differ")
        oracle_set = set(oracle_pages)
        coded_set = set(coded_pages)
        exact_set = set(exact_pages)
        expected = GroupSample(
            query_ordinal=query_ordinal,
            retained_pages=retained,
            group_ranges=ranges,
            grouped_pages=grouped_pages,
            code_gets=len(ranges),
            code_bytes=code_bytes,
            grouped_hits_at_10=sum(owner[value] in grouped_set for value in truth_ids[:10]),
            grouped_hits_at_100=sum(owner[value] in grouped_set for value in truth_ids),
            oracle_pages=oracle_pages,
            oracle_hits_at_10=sum(owner[value] in oracle_set for value in truth_ids[:10]),
            oracle_hits_at_100=sum(owner[value] in oracle_set for value in truth_ids),
            exact_pages=exact_pages,
            exact_data_bytes=sum(page_byte_sizes[page] for page in exact_pages),
            exact_hits_at_10=sum(owner[value] in exact_set for value in truth_ids[:10]),
            exact_hits_at_100=sum(owner[value] in exact_set for value in truth_ids),
            coded_pages=coded_pages,
            coded_data_bytes=sum(page_byte_sizes[page] for page in coded_pages),
            coded_hits_at_10=sum(owner[value] in coded_set for value in truth_ids[:10]),
            coded_hits_at_100=sum(owner[value] in coded_set for value in truth_ids),
            prior_pq_hits_at_100=prior_hits[query_ordinal][0],
            prior_residual_hits_at_100=prior_hits[query_ordinal][1],
        )
        if recorded != expected:
            raise ValueError(f"group independent sample {query_ordinal} differs")
        expected_samples.append(expected)
    count = len(expected_samples)

    def average(hits: Sequence[int], top: int) -> int:
        return 1_000_000 * sum(hits) // (count * top)

    exact100 = [sample.exact_hits_at_100 for sample in expected_samples]
    coded100 = [sample.coded_hits_at_100 for sample in expected_samples]
    exact_mean = average(exact100, 100)
    exact_p05 = sorted(exact100)[math.ceil(count * 0.05) - 1] * 10_000
    exact_r10 = average([sample.exact_hits_at_10 for sample in expected_samples], 10)
    coded_mean = average(coded100, 100)
    coded_p05 = sorted(coded100)[math.ceil(count * 0.05) - 1] * 10_000
    coded_r10 = average([sample.coded_hits_at_10 for sample in expected_samples], 10)
    exact_pass = exact_mean >= 975_000 and exact_p05 >= 900_000 and exact_r10 >= 960_000
    coded_pass = coded_mean >= 975_000 and coded_p05 >= 900_000 and coded_r10 >= 960_000
    decision = "grouping-killed" if not exact_pass else "representation-killed" if not coded_pass else "quality-advance-memory-pending"
    independent = {
        "query_count": count,
        "prior_pq_mean_recall_at_100_ppm": average([sample.prior_pq_hits_at_100 for sample in expected_samples], 100),
        "prior_residual_mean_recall_at_100_ppm": average([sample.prior_residual_hits_at_100 for sample in expected_samples], 100),
        "grouped_mean_recall_at_100_ppm": average([sample.grouped_hits_at_100 for sample in expected_samples], 100),
        "oracle_mean_recall_at_100_ppm": average([sample.oracle_hits_at_100 for sample in expected_samples], 100),
        "exact_mean_recall_at_100_ppm": exact_mean,
        "exact_p05_recall_at_100_ppm": exact_p05,
        "exact_recall_at_10_ppm": exact_r10,
        "coded_mean_recall_at_100_ppm": coded_mean,
        "coded_p05_recall_at_100_ppm": coded_p05,
        "coded_recall_at_10_ppm": coded_r10,
        "coded_worst_recall_at_100_ppm": min(coded100) * 10_000,
        "max_code_gets": max(sample.code_gets for sample in expected_samples),
        "max_code_bytes": max(sample.code_bytes for sample in expected_samples),
        "max_data_pages": max(max(len(sample.exact_pages), len(sample.coded_pages)) for sample in expected_samples),
        "max_data_bytes": max(max(sample.exact_data_bytes, sample.coded_data_bytes) for sample in expected_samples),
        "decision": decision,
    }
    if dict(metrics) != independent:
        raise ValueError("group independent aggregate differs")
    return {
        "schema": SCHEMA,
        "claim_eligible": False,
        "decision": decision,
        "metrics": independent,
    }
