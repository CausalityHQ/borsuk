#!/usr/bin/env python3
"""Authenticated grouped code reads and paired row-score page nomination."""

from __future__ import annotations

import dataclasses
import hashlib
import math
import struct
from collections.abc import Callable, Mapping, Sequence

import numpy as np

from scripts.native_geometric_layout_screen import EvaluationLimits
from scripts.native_page_centered_group_codes import GroupCodes, GroupRange
from scripts.native_row_score_nomination import nominate_pages


@dataclasses.dataclass(frozen=True, slots=True)
class GroupPlan:
    group_ordinals: tuple[int, ...]
    ranges: tuple[GroupRange, ...]
    pages: tuple[int, ...]
    gets: int
    bytes: int


@dataclasses.dataclass(frozen=True, slots=True)
class GroupSample:
    query_ordinal: int
    retained_pages: tuple[int, ...]
    group_ranges: tuple[GroupRange, ...]
    grouped_pages: tuple[int, ...]
    code_gets: int
    code_bytes: int
    grouped_hits_at_10: int
    grouped_hits_at_100: int
    oracle_pages: tuple[int, ...]
    oracle_hits_at_10: int
    oracle_hits_at_100: int
    exact_pages: tuple[int, ...]
    exact_data_bytes: int
    exact_hits_at_10: int
    exact_hits_at_100: int
    coded_pages: tuple[int, ...]
    coded_data_bytes: int
    coded_hits_at_10: int
    coded_hits_at_100: int
    prior_pq_hits_at_100: int
    prior_residual_hits_at_100: int


def plan_groups(
    retained_pages: Sequence[int],
    group_manifest: Sequence[GroupRange],
    *,
    maximum_groups: int = 32,
    maximum_bytes: int = 16_777_216,
) -> GroupPlan:
    """Take the first distinct fixed four-page groups in tree leaf order."""
    if (
        not group_manifest
        or not retained_pages
        or len(set(retained_pages)) != len(retained_pages)
        or type(maximum_groups) is not int
        or maximum_groups <= 0
        or type(maximum_bytes) is not int
        or maximum_bytes <= 0
    ):
        raise ValueError("group retained authority differs")
    page_count = group_manifest[-1][1]
    if any(type(page) is not int or not 0 <= page < page_count for page in retained_pages):
        raise ValueError("group retained authority differs")
    offset = 0
    for ordinal, group in enumerate(group_manifest):
        if (
            len(group) != 5
            or group[0] != 4 * ordinal
            or group[1] != min(4 * ordinal + 4, page_count)
            or group[2] != offset
            or type(group[3]) is not int
            or group[3] <= 0
            or type(group[4]) is not str
            or len(group[4]) != 64
            or any(character not in "0123456789abcdef" for character in group[4])
        ):
            raise ValueError("group manifest authority differs")
        offset += group[3]
    selected: list[int] = []
    seen: set[int] = set()
    for page in retained_pages:
        group = page // 4
        if group not in seen:
            selected.append(group)
            seen.add(group)
            if len(selected) == maximum_groups:
                break
    ranges = tuple(group_manifest[index] for index in selected)
    total = sum(group[3] for group in ranges)
    if total > maximum_bytes:
        raise ValueError("group code wave budget exceeded")
    pages = tuple(page for group in ranges for page in range(group[0], group[1]))
    return GroupPlan(tuple(selected), ranges, pages, len(ranges), total)


def centered_scores(
    query: np.ndarray,
    books: np.ndarray,
    page_means: np.ndarray,
    codes: np.ndarray,
    row_pages: Sequence[int],
) -> np.ndarray:
    """Lookup squared distances to page mean plus source-residual PQ code."""
    if (
        type(query) is not np.ndarray
        or query.dtype != np.float32
        or query.ndim != 1
        or query.size % 48
        or type(books) is not np.ndarray
        or books.dtype != np.float32
        or books.shape != (48, 256, query.size // 48)
        or type(page_means) is not np.ndarray
        or page_means.dtype != np.float32
        or page_means.ndim != 2
        or page_means.shape[1] != query.size
        or type(codes) is not np.ndarray
        or codes.dtype != np.uint8
        or codes.shape != (len(row_pages), 48)
        or not np.isfinite(query).all()
        or not np.isfinite(books).all()
        or not np.isfinite(page_means).all()
        or any(type(page) is not int or not 0 <= page < len(page_means) for page in row_pages)
    ):
        raise ValueError("page-centered score authority differs")
    pages = np.asarray(row_pages, dtype=np.int64)
    scores = np.zeros(len(row_pages), dtype=np.float64)
    width = query.size // 48
    q = query.astype(np.float64)
    for page in sorted(set(row_pages)):
        positions = np.flatnonzero(pages == page)
        local = q - page_means[page].astype(np.float64)
        page_scores = np.zeros(len(positions), dtype=np.float64)
        for subspace in range(48):
            lo = subspace * width
            delta = books[subspace].astype(np.float64) - local[lo : lo + width]
            table = np.einsum("ij,ij->i", delta, delta)
            page_scores += table[codes[positions, subspace]]
        scores[positions] = page_scores
    return scores.astype(np.float32)


def _exact_scores(query: np.ndarray, selected_vectors: np.ndarray) -> np.ndarray:
    delta = selected_vectors.astype(np.float64) - query.astype(np.float64)
    return np.einsum("ij,ij->i", delta, delta).astype(np.float32)


def _decode_group(
    payload: bytes,
    group: GroupRange,
    counts: Sequence[int],
    dimensions: int,
) -> tuple[np.ndarray, np.ndarray]:
    first, end, _, length, digest = group
    if (
        type(payload) is not bytes
        or len(payload) != length
        or hashlib.sha256(payload).hexdigest() != digest
    ):
        raise ValueError("group range identity differs")
    header_bytes = 4 + (end - first) * (4 + dimensions * 4)
    rows = sum(counts[first:end])
    if (
        len(payload) != header_bytes + rows * 48
        or struct.unpack_from("<I", payload)[0] != end - first
    ):
        raise ValueError("group range payload differs")
    means = np.empty((end - first, dimensions), dtype=np.float32)
    for page in range(first, end):
        offset = 4 + (page - first) * (4 + dimensions * 4)
        if struct.unpack_from("<I", payload, offset)[0] != counts[page]:
            raise ValueError("group range page count differs")
        means[page - first] = np.frombuffer(
            payload, dtype="<f4", count=dimensions, offset=offset + 4
        )
    codes = np.frombuffer(payload, dtype=np.uint8, offset=header_bytes).reshape(rows, 48)
    if not np.isfinite(means).all():
        raise ValueError("group range means differ")
    return means, codes


def evaluate_group_query(
    *,
    query_ordinal: int,
    query: np.ndarray,
    retained_pages: Sequence[int],
    artifacts: GroupCodes,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
    truth_ids: Sequence[bytes],
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
    read_group_range: Callable[[int, int], bytes],
    prior_pq_hits_at_100: int,
    prior_residual_hits_at_100: int,
    owner_by_id: Mapping[bytes, int] | None = None,
) -> GroupSample:
    """Read real code groups; nominate pages with coded and exact row scores."""
    if (
        type(query_ordinal) is not int
        or query_ordinal < 0
        or type(artifacts) is not GroupCodes
        or type(vectors) is not np.ndarray
        or vectors.dtype != np.float32
        or vectors.shape != (len(stable_ids), query.size)
        or artifacts.codes.shape != (len(stable_ids), 48)
        or len(artifacts.source_ordinals) != len(stable_ids)
        or len(page_byte_sizes) != len(artifacts.page_row_counts)
        or len(truth_ids) != 100
        or len(set(truth_ids)) != 100
        or type(limits) is not EvaluationLimits
        or any(type(value) is not int or not 0 <= value <= 100 for value in (prior_pq_hits_at_100, prior_residual_hits_at_100))
    ):
        raise ValueError("group evaluation authority differs")
    plan = plan_groups(retained_pages, artifacts.group_ranges)
    before_gets = getattr(read_group_range, "gets", None)
    before_bytes = getattr(read_group_range, "bytes", None)
    if type(before_gets) is not int or type(before_bytes) is not int:
        raise ValueError("group read counters unavailable")
    offsets = np.concatenate(([0], np.cumsum(artifacts.page_row_counts, dtype=np.int64)))
    fetched_means = artifacts.page_means.copy()
    selected_codes: list[np.ndarray] = []
    source_ordinals: list[int] = []
    row_pages: list[int] = []
    for group in plan.ranges:
        first, end, offset, length, _ = group
        means, codes = _decode_group(
            read_group_range(offset, length),
            group,
            artifacts.page_row_counts,
            query.size,
        )
        if (
            not np.array_equal(means, artifacts.page_means[first:end])
            or not np.array_equal(codes, artifacts.codes[offsets[first] : offsets[end]])
        ):
            raise ValueError("group range identity differs")
        fetched_means[first:end] = means
        selected_codes.append(codes)
        for page in range(first, end):
            row_pages.extend([page] * artifacts.page_row_counts[page])
        source_ordinals.extend(artifacts.source_ordinals[offsets[first] : offsets[end]])
    code_gets = read_group_range.gets - before_gets
    code_bytes = read_group_range.bytes - before_bytes
    if code_gets != plan.gets or code_bytes != plan.bytes:
        raise ValueError("group actual code wave differs")
    coded = centered_scores(
        query,
        artifacts.books,
        fetched_means,
        np.concatenate(selected_codes),
        tuple(row_pages),
    )
    exact = _exact_scores(query, vectors[source_ordinals])
    coded_pages = nominate_pages(
        coded, source_ordinals, row_pages, page_byte_sizes, limits
    )
    exact_pages = nominate_pages(
        exact, source_ordinals, row_pages, page_byte_sizes, limits
    )
    if owner_by_id is None:
        owners = {
            stable_ids[artifacts.source_ordinals[position]]: page
            for page in range(len(page_byte_sizes))
            for position in range(int(offsets[page]), int(offsets[page + 1]))
        }
    else:
        owners = owner_by_id
    if any(stable_id not in owners for stable_id in truth_ids):
        raise ValueError("group truth authority differs")
    grouped_set = set(plan.pages)
    truth_counts = {page: 0 for page in grouped_set}
    for stable_id in truth_ids:
        if owners[stable_id] in truth_counts:
            truth_counts[owners[stable_id]] += 1
    oracle_pages = tuple(
        sorted(grouped_set, key=lambda page: (-truth_counts[page], page))[
            : limits.maximum_pages
        ]
    )
    if sum(page_byte_sizes[page] for page in oracle_pages) > limits.maximum_bytes:
        raise ValueError("group oracle byte cap needs exact optimization")
    oracle_set = set(oracle_pages)
    exact_set = set(exact_pages)
    coded_set = set(coded_pages)
    return GroupSample(
        query_ordinal=query_ordinal,
        retained_pages=tuple(retained_pages),
        group_ranges=plan.ranges,
        grouped_pages=plan.pages,
        code_gets=code_gets,
        code_bytes=code_bytes,
        grouped_hits_at_10=sum(owners[value] in grouped_set for value in truth_ids[:10]),
        grouped_hits_at_100=sum(owners[value] in grouped_set for value in truth_ids),
        oracle_pages=oracle_pages,
        oracle_hits_at_10=sum(owners[value] in oracle_set for value in truth_ids[:10]),
        oracle_hits_at_100=sum(owners[value] in oracle_set for value in truth_ids),
        exact_pages=exact_pages,
        exact_data_bytes=sum(page_byte_sizes[page] for page in exact_pages),
        exact_hits_at_10=sum(owners[value] in exact_set for value in truth_ids[:10]),
        exact_hits_at_100=sum(owners[value] in exact_set for value in truth_ids),
        coded_pages=coded_pages,
        coded_data_bytes=sum(page_byte_sizes[page] for page in coded_pages),
        coded_hits_at_10=sum(owners[value] in coded_set for value in truth_ids[:10]),
        coded_hits_at_100=sum(owners[value] in coded_set for value in truth_ids),
        prior_pq_hits_at_100=prior_pq_hits_at_100,
        prior_residual_hits_at_100=prior_residual_hits_at_100,
    )


def aggregate_group_samples(samples: Sequence[GroupSample]) -> dict[str, int | str]:
    """Reduce every paired query under the frozen quality and read gates."""
    if (
        not samples
        or any(type(sample) is not GroupSample or sample.query_ordinal != index for index, sample in enumerate(samples))
        or any(sample.code_gets > 32 or sample.code_bytes > 16_777_216 for sample in samples)
        or any(len(sample.exact_pages) > 32 or len(sample.coded_pages) > 32 for sample in samples)
        or any(sample.exact_data_bytes > 16_777_216 or sample.coded_data_bytes > 16_777_216 for sample in samples)
    ):
        raise ValueError("group aggregate authority differs")
    count = len(samples)

    def mean_ppm(values: Sequence[int], top: int) -> int:
        return sum(values) * 1_000_000 // (count * top)

    exact100 = [sample.exact_hits_at_100 for sample in samples]
    coded100 = [sample.coded_hits_at_100 for sample in samples]
    exact10 = [sample.exact_hits_at_10 for sample in samples]
    coded10 = [sample.coded_hits_at_10 for sample in samples]
    exact_mean = mean_ppm(exact100, 100)
    exact_p05 = sorted(exact100)[math.ceil(count * 0.05) - 1] * 10_000
    exact_r10 = mean_ppm(exact10, 10)
    coded_mean = mean_ppm(coded100, 100)
    coded_p05 = sorted(coded100)[math.ceil(count * 0.05) - 1] * 10_000
    coded_r10 = mean_ppm(coded10, 10)
    exact_pass = exact_mean >= 975_000 and exact_p05 >= 900_000 and exact_r10 >= 960_000
    coded_pass = coded_mean >= 975_000 and coded_p05 >= 900_000 and coded_r10 >= 960_000
    decision = (
        "grouping-killed"
        if not exact_pass
        else "representation-killed" if not coded_pass else "quality-advance-memory-pending"
    )
    return {
        "query_count": count,
        "prior_pq_mean_recall_at_100_ppm": mean_ppm([sample.prior_pq_hits_at_100 for sample in samples], 100),
        "prior_residual_mean_recall_at_100_ppm": mean_ppm([sample.prior_residual_hits_at_100 for sample in samples], 100),
        "grouped_mean_recall_at_100_ppm": mean_ppm([sample.grouped_hits_at_100 for sample in samples], 100),
        "oracle_mean_recall_at_100_ppm": mean_ppm([sample.oracle_hits_at_100 for sample in samples], 100),
        "exact_mean_recall_at_100_ppm": exact_mean,
        "exact_p05_recall_at_100_ppm": exact_p05,
        "exact_recall_at_10_ppm": exact_r10,
        "coded_mean_recall_at_100_ppm": coded_mean,
        "coded_p05_recall_at_100_ppm": coded_p05,
        "coded_recall_at_10_ppm": coded_r10,
        "coded_worst_recall_at_100_ppm": min(coded100) * 10_000,
        "max_code_gets": max(sample.code_gets for sample in samples),
        "max_code_bytes": max(sample.code_bytes for sample in samples),
        "max_data_pages": max(max(len(sample.exact_pages), len(sample.coded_pages)) for sample in samples),
        "max_data_bytes": max(max(sample.exact_data_bytes, sample.coded_data_bytes) for sample in samples),
        "decision": decision,
    }
