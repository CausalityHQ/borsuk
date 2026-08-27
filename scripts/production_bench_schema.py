"""Exact fail-closed schema contract for current production query samples."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

V10_PRODUCTION_BENCH_SCHEMA_VERSION = "borsuk-production-bench-v10"
PRODUCTION_BENCH_SCHEMA_VERSION = "borsuk-production-bench-v20"
V10_QUERY_TELEMETRY_FIELDS = (
    "global_leaf_directory_reads",
    "global_leaf_directory_bytes",
    "global_leaf_pages_read",
    "global_leaf_page_bytes",
    "global_leaf_waves",
    "global_leaf_continuations",
    "global_leaf_exact_scores",
    "backing_reads",
    "backing_bytes_read",
)
QUERY_STAGE_TIMING_FIELDS = (
    "global_base_approximate_us",
    "global_base_head_admission_us",
    "global_base_head_fetch_us",
    "global_base_head_read_attempts",
    "global_base_head_read_successes",
    "global_base_head_read_response_bytes",
    "global_base_head_read_us_max",
    "global_base_head_read_us_sum",
    "global_base_head_read_queue_us_max",
    "global_base_head_read_queue_us_sum",
    "global_base_head_reads_over_20ms",
    "global_base_head_reads_over_30ms",
    "global_base_head_reads_over_50ms",
    "global_base_head_reads_over_100ms",
    "global_base_head_decode_admission_us",
    "global_base_head_decode_us",
    "global_base_exact_admission_us",
    "global_base_exact_fetch_us",
    "global_base_exact_read_attempts",
    "global_base_exact_read_successes",
    "global_base_exact_read_response_bytes",
    "global_base_exact_read_queue_us_max",
    "global_base_exact_read_queue_us_sum",
    "global_base_exact_read_us_max",
    "global_base_exact_read_us_sum",
    "global_base_exact_reads_over_20ms",
    "global_base_exact_reads_over_30ms",
    "global_base_exact_reads_over_50ms",
    "global_base_exact_reads_over_100ms",
    "global_base_exact_cpu_us",
    "global_base_exact_rerank_us",
)
QUERY_STAGE_MAX_FIELDS = frozenset(
    {
        "global_base_head_read_us_max",
        "global_base_head_read_queue_us_max",
        "global_base_exact_read_queue_us_max",
        "global_base_exact_read_us_max",
    }
)
QUERY_STAGE_AGGREGATE_FIELD_BY_SAMPLE = {
    field: (
        f"{field}_across_queries"
        if field in QUERY_STAGE_MAX_FIELDS
        else f"{field}_total"
    )
    for field in QUERY_STAGE_TIMING_FIELDS
}
QUERY_STAGE_AGGREGATE_FIELDS = tuple(
    QUERY_STAGE_AGGREGATE_FIELD_BY_SAMPLE[field] for field in QUERY_STAGE_TIMING_FIELDS
)
PHYSICAL_EXACT_LAYOUT_FIELDS = (
    "global_leaf_code_requests",
    "global_leaf_exact_requests",
    "global_leaf_exact_cells",
    "global_leaf_exact_cards",
    "global_leaf_deepest_winning_card_rank",
    "global_leaf_exact_groups",
    "global_leaf_exact_selected_bytes",
    "global_leaf_exact_speculative_bytes",
)
CACHE_COHORT_FIELDS = (
    "cache_cohort_index",
    "cache_cohort_size",
    "cache_cohort_count",
)
CURRENT_QUERY_TELEMETRY_FIELDS = (
    *CACHE_COHORT_FIELDS,
    "global_leaf_directory_reads",
    "global_leaf_directory_bytes",
    "global_leaf_code_pages_read",
    "global_leaf_code_bytes",
    "global_leaf_pages_read",
    "global_leaf_page_bytes",
    *PHYSICAL_EXACT_LAYOUT_FIELDS,
    "global_leaf_waves",
    "global_leaf_continuations",
    "global_leaf_exact_scores",
    "decoded_cache_bytes_read",
    "backing_reads",
    "backing_bytes_read",
    *QUERY_STAGE_TIMING_FIELDS,
)


def validate_production_bench_schema_rows(
    rows: Sequence[Mapping[str, object]], path: str | Path
) -> None:
    """Reject every row outside the exact current production schema."""

    for line, row in enumerate(rows, start=2):
        version = row.get("schema_version")
        if version != PRODUCTION_BENCH_SCHEMA_VERSION:
            raise ValueError(
                f"{path}:{line} production benchmark schema {version!r}; "
                f"expected {PRODUCTION_BENCH_SCHEMA_VERSION!r}"
            )


def validate_v10_query_sample_rows(
    rows: Sequence[Mapping[str, object]], path: str | Path
) -> None:
    """Reject every query-sample row outside the exact current V10 contract."""

    for line, row in enumerate(rows, start=2):
        version = row.get("schema_version")
        if version != V10_PRODUCTION_BENCH_SCHEMA_VERSION:
            raise ValueError(
                f"{path}:{line} production benchmark schema {version!r}; "
                f"expected {V10_PRODUCTION_BENCH_SCHEMA_VERSION!r}"
            )
        missing = [
            field
            for field in V10_QUERY_TELEMETRY_FIELDS
            if field not in row or row[field] in (None, "")
        ]
        if missing:
            raise ValueError(
                f"{path}:{line} missing V10 telemetry: {', '.join(missing)}"
            )


def validate_current_query_sample_rows(
    rows: Sequence[Mapping[str, object]], path: str | Path
) -> None:
    """Reject every query-sample row outside the exact current contract."""

    validate_production_bench_schema_rows(rows, path)
    for line, row in enumerate(rows, start=2):
        missing = [
            field
            for field in CURRENT_QUERY_TELEMETRY_FIELDS
            if field not in row or row[field] in (None, "")
        ]
        if missing:
            raise ValueError(
                f"{path}:{line} missing current telemetry: {', '.join(missing)}"
            )
        validate_query_stage_timings(row, role=f"{path}:{line}")
        validate_query_planner_read_telemetry(row, role=f"{path}:{line}")


def validate_query_stage_timings(
    row: Mapping[str, object], *, role: str
) -> dict[str, int]:
    """Return one exact, internally consistent per-query timing sample."""
    parsed: dict[str, int] = {}
    for field in QUERY_STAGE_TIMING_FIELDS:
        value = row.get(field)
        if value is None or isinstance(value, bool):
            raise ValueError(f"{role} timing telemetry is missing")
        try:
            parsed[field] = int(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"{role} timing telemetry is invalid") from error
        if parsed[field] < 0:
            raise ValueError(f"{role} timing telemetry is invalid")
    if (
        parsed["global_base_exact_read_us_max"]
        > parsed["global_base_exact_read_us_sum"]
        or parsed["global_base_exact_read_us_max"]
        > parsed["global_base_exact_fetch_us"]
        or parsed["global_base_exact_fetch_us"] > parsed["global_base_exact_rerank_us"]
    ):
        raise ValueError(f"{role} timing telemetry is inconsistent")
    tails = [
        parsed["global_base_exact_reads_over_20ms"],
        parsed["global_base_exact_reads_over_30ms"],
        parsed["global_base_exact_reads_over_50ms"],
        parsed["global_base_exact_reads_over_100ms"],
    ]
    if tails != sorted(tails, reverse=True):
        raise ValueError(f"{role} timing telemetry is inconsistent")
    for stage in ("head", "exact"):
        prefix = f"global_base_{stage}_read"
        attempts = parsed[f"{prefix}_attempts"]
        successes = parsed[f"{prefix}_successes"]
        response_bytes = parsed[f"{prefix}_response_bytes"]
        service_max = parsed[f"{prefix}_us_max"]
        service_sum = parsed[f"{prefix}_us_sum"]
        queue_max = parsed[f"{prefix}_queue_us_max"]
        queue_sum = parsed[f"{prefix}_queue_us_sum"]
        stage_tails = [
            parsed[f"{prefix}s_over_20ms"],
            parsed[f"{prefix}s_over_30ms"],
            parsed[f"{prefix}s_over_50ms"],
            parsed[f"{prefix}s_over_100ms"],
        ]
        if (
            successes != attempts
            or service_max > service_sum
            or queue_max > queue_sum
            or stage_tails != sorted(stage_tails, reverse=True)
            or any(value > attempts for value in stage_tails)
            or (
                attempts == 0
                and any(
                    (
                        response_bytes,
                        service_max,
                        service_sum,
                        queue_max,
                        queue_sum,
                        *stage_tails,
                    )
                )
            )
            or (attempts > 0 and response_bytes == 0)
        ):
            raise ValueError(f"{role} timing telemetry is inconsistent")
    return parsed


def validate_query_planner_read_telemetry(
    row: Mapping[str, object], *, role: str
) -> None:
    """Bind one fresh-handle query's planner ranges to its physical reads."""
    if row.get("phase") != "uncached":
        return
    planner_fields = (
        "global_leaf_code_requests",
        "global_leaf_exact_requests",
        "global_leaf_code_bytes",
        "global_leaf_page_bytes",
        "backing_bytes_read",
    )
    planner: dict[str, int] = {}
    for field in planner_fields:
        value = row.get(field)
        if value is None or isinstance(value, bool):
            raise ValueError(f"{role} planner/read telemetry is missing")
        try:
            planner[field] = int(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"{role} planner/read telemetry is invalid") from error
        if planner[field] < 0:
            raise ValueError(f"{role} planner/read telemetry is invalid")
    if (
        int(row["global_base_head_read_attempts"])
        != planner["global_leaf_code_requests"]
        or int(row["global_base_exact_read_attempts"])
        != planner["global_leaf_exact_requests"]
        or int(row["global_base_head_read_response_bytes"])
        != planner["global_leaf_code_bytes"]
        or int(row["global_base_exact_read_response_bytes"])
        > planner["global_leaf_page_bytes"]
        or int(row["global_base_head_read_response_bytes"])
        + int(row["global_base_exact_read_response_bytes"])
        > planner["backing_bytes_read"]
    ):
        raise ValueError(f"{role} planner/read telemetry is inconsistent")
