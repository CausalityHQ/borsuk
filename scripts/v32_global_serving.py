"""Validate global serving receipts against preauthenticated physical authority.

This module performs no I/O. Callers authenticate the expected replay and page
registry before supplying them here. Raw receipt bytes retain their own digest;
valid floating-point lexemes are never reconstructed to establish byte identity.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass

_CONFIG = {
    "global_leaf_limit": 768,
    "scan_budget": 262144,
    "candidate_depth": 12288,
    "page_count": 16,
    "k": 10,
}
_TIMING = {
    "elapsed_ns",
    "process_cpu_ns",
    "peak_rss_bytes",
    "routing_elapsed_ns",
    "page_read_elapsed_ns",
    "exact_rerank_elapsed_ns",
    "routing_cpu_ns",
    "page_read_cpu_ns",
    "exact_rerank_cpu_ns",
}
_ROUTING = {
    "roots_scored",
    "leaves_eligible",
    "leaves_scanned",
    "query_table_pairs_built",
    "peak_query_table_pairs_live",
    "codes_scanned",
    "candidates_retained",
    "pages_considered",
    "selected_pages",
}
_PAGE = {"ordinal", "sha256", "encoded_bytes", "primary_rows", "replica_rows"}


@dataclass(frozen=True)
class GlobalPageIdentity:
    ordinal: int
    sha256: str
    encoded_bytes: int
    primary_rows: int
    replica_rows: int


@dataclass(frozen=True)
class GlobalQueryExpectation:
    query_ordinal: int
    candidate_replay_sha256: str
    page_ordinals: tuple[int, ...]


@dataclass(frozen=True)
class GlobalReplayRegistration:
    terminal_sha256: str
    terminal_bytes: int
    manifest_sha256: str
    manifest_bytes: int
    query_sha256: str
    truth_sha256: str
    truth_receipt_sha256: str
    source_rows: int
    query_start: int


@dataclass(frozen=True)
class GlobalReplayAuthority:
    expected: tuple[GlobalQueryExpectation, ...]
    pages: tuple[GlobalPageIdentity, ...]
    source_rows: int
    query_start: int


def load_global_replay_authority(
    terminal: bytes,
    manifest: bytes,
    page_locations: bytes,
    registration: GlobalReplayRegistration,
) -> GlobalReplayAuthority:
    """Project pinned historical control evidence, without I/O or page access.

    Registration is external authority, never inferred from these payloads.
    This authenticates the replay projection, not query/truth materialization.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    _require(type(registration) is GlobalReplayRegistration, "registration type")
    r = registration
    _require(
        all(
            _digest(v)
            for v in (
                r.terminal_sha256,
                r.manifest_sha256,
                r.query_sha256,
                r.truth_sha256,
                r.truth_receipt_sha256,
            )
        )
        and _integer(r.source_rows, 16, 10**9)
        and _integer(r.query_start, 0, 2**64 - 33),
        "registration values",
    )

    def authenticate(payload, digest, size):
        _require(
            type(payload) is bytes
            and _digest(digest)
            and _integer(size, 1, 8 * 1024**2)
            and len(payload) == size
            and hashlib.sha256(payload).hexdigest() == digest,
            "artifact bytes",
        )

    authenticate(terminal, r.terminal_sha256, r.terminal_bytes)
    authenticate(manifest, r.manifest_sha256, r.manifest_bytes)
    t, m = _parse(terminal), _parse(manifest)
    try:
        _require(
            type(m["schema_version"]) is int
            and m["schema_version"] == 3
            and m["page_key_suffix"] == ".arrow"
            and type(m["layout"]["source_rows"]) is int
            and m["layout"]["source_rows"] == r.source_rows
            and type(m["layout"]["page_rows"]) is int
            and m["layout"]["page_rows"] == 480,
            "manifest projection",
        )
        for key, value in (
            ("candidate_depth", 12288),
            ("page_count", 16),
            ("root_beam", 8),
        ):
            _require(
                type(m["routing"][key]) is int and m["routing"][key] == value,
                "manifest route",
            )
        location = m["serving"]["page_locations"]
        _require(
            _keys(location, {"role", "file", "sha256", "encoded_bytes"})
            and location["role"] == "v32-page-locations-parquet"
            and location["file"] == "page-locations.parquet",
            "page artifact identity",
        )
        authenticate(page_locations, location["sha256"], location["encoded_bytes"])
        for key, value in (
            ("schema_version", 7),
            ("global_leaf_limit", 768),
            ("root_beam", 8),
            ("leaf_beam", 256),
            ("source_rows", r.source_rows),
            ("query_start", r.query_start),
            ("query_count", 32),
        ):
            _require(type(t[key]) is int and t[key] == value, "terminal configuration")
        _require(
            t["claim_eligible"] is False
            and t["routing_scope"] == "global"
            and t["layout_algorithm"] == "v32-global-balanced-cosine-v1"
            and t["manifest_sha256"] == r.manifest_sha256
            and t["query_sha256"] == r.query_sha256
            and t["truth_sha256"] == r.truth_sha256
            and t["truth_receipt_sha256"] == r.truth_receipt_sha256,
            "terminal bindings",
        )
        schema = pa.schema(
            [
                pa.field("page_ordinal", pa.uint32(), nullable=False),
                pa.field("sha256", pa.binary(32), nullable=False),
                pa.field("encoded_bytes", pa.uint32(), nullable=False),
                pa.field("row_count", pa.uint16(), nullable=False),
            ]
        )
        parquet = pq.ParquetFile(pa.BufferReader(page_locations))
        _require(
            parquet.schema_arrow.equals(schema, check_metadata=False)
            and 16 <= parquet.metadata.num_rows <= r.source_rows,
            "page Parquet schema",
        )
        table = parquet.read()
        _require(all(column.null_count == 0 for column in table.columns), "page nulls")
        pages = []
        for ordinal, item in enumerate(table.to_pylist()):
            _require(item["page_ordinal"] == ordinal, "page ordinal")
            page = GlobalPageIdentity(
                ordinal,
                item["sha256"].hex(),
                item["encoded_bytes"],
                item["row_count"],
                0,
            )
            _page(asdict(page))
            pages.append(page)
        _require(sum(p.primary_rows for p in pages) == r.source_rows, "page coverage")
        control, replay = t["control"]["queries"], t["virtual_geometric"]["queries"]
        _require(
            type(control) is list
            and type(replay) is list
            and len(control) == len(replay) == 32,
            "query count",
        )
        expected = []
        for ordinal, (original, captured) in enumerate(
            zip(control, replay, strict=True), r.query_start
        ):
            _require(
                type(original["query_ordinal"]) is int
                and original["query_ordinal"] == ordinal
                and type(captured["query_ordinal"]) is int
                and captured["query_ordinal"] == ordinal
                and _digest(captured["candidate_replay_sha256"]),
                "query pairing",
            )
            selection = original["page_selections"]["first_distinct"]
            _require(
                type(selection["pages"]) is list and len(selection["pages"]) == 16,
                "control pages",
            )
            ordinals = []
            for item in selection["pages"]:
                _require(
                    _keys(item, {"ordinal", "sha256", "encoded_bytes"})
                    and _integer(item["ordinal"], 0, len(pages) - 1)
                    and _integer(item["encoded_bytes"], 1),
                    "control page schema",
                )
                page = pages[item["ordinal"]]
                _require(
                    item["sha256"] == page.sha256
                    and item["encoded_bytes"] == page.encoded_bytes,
                    "control page identity",
                )
                ordinals.append(page.ordinal)
            _require(
                len(set(ordinals)) == 16
                and type(selection["selected_page_bytes"]) is int
                and selection["selected_page_bytes"]
                == sum(pages[i].encoded_bytes for i in ordinals)
                <= 3145728,
                "control accounting",
            )
            expected.append(
                GlobalQueryExpectation(
                    ordinal, captured["candidate_replay_sha256"], tuple(ordinals)
                )
            )
        return GlobalReplayAuthority(
            tuple(expected), tuple(pages), r.source_rows, r.query_start
        )
    except (KeyError, TypeError, IndexError, pa.ArrowException) as error:
        raise ValueError("V32 global serving authority projection differs") from error


def _require(condition: bool, boundary: str) -> None:
    if not condition:
        raise ValueError(f"V32 global serving {boundary} differs")


def _integer(value: object, minimum: int = 0, maximum: int = 2**64 - 1) -> bool:
    return type(value) is int and minimum <= value <= maximum


def _digest(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and value != "0" * 64
        and all(c in "0123456789abcdef" for c in value)
    )


def _keys(value: object, keys: set[str]) -> bool:
    return type(value) is dict and set(value) == keys


def _parse(payload: bytes) -> dict:
    _require(
        type(payload) is bytes
        and 2 < len(payload) <= 8 * 1024**2
        and payload.startswith(b"{")
        and payload.endswith(b"}\n"),
        "byte framing",
    )
    quoted = escaped = False
    for byte in payload[:-1]:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        else:
            _require(byte not in (9, 10, 13, 32), "compact JSON")

    def object_pairs(pairs):
        keys = [key for key, _ in pairs]
        _require(keys == sorted(set(keys)), "object keys")
        return dict(pairs)

    def invalid_constant(_value):
        raise ValueError("V32 global serving nonfinite JSON")

    return json.loads(
        payload, object_pairs_hook=object_pairs, parse_constant=invalid_constant
    )


def _page(value: object) -> None:
    _require(_keys(value, _PAGE), "page schema")
    _require(
        _integer(value["ordinal"], 0, 2**32 - 1)
        and _digest(value["sha256"])
        and _integer(value["encoded_bytes"], 1, 3145728)
        and _integer(value["primary_rows"], 1, 480)
        and type(value["replica_rows"]) is int
        and value["replica_rows"] == 0,
        "page values",
    )


def _configuration(value: object) -> bool:
    return _keys(value, set(_CONFIG)) and all(
        type(value[k]) is int and value[k] == n for k, n in _CONFIG.items()
    )


def validate_global_serving_batch(
    payload: bytes,
    *,
    expected: tuple[GlobalQueryExpectation, ...],
    pages: tuple[GlobalPageIdentity, ...],
    source_rows: int,
) -> dict:
    """Return validated global rows; logical GETs are not transport retry counts."""
    _require(_integer(source_rows, 1, 10**9), "source rows")
    _require(
        type(expected) is tuple
        and len(expected) == 32
        and type(pages) is tuple
        and 16 <= len(pages) <= source_rows,
        "expected cardinality",
    )
    registry = {}
    for page in pages:
        _require(type(page) is GlobalPageIdentity, "expected page type")
        value = asdict(page)
        _page(value)
        _require(page.ordinal not in registry, "expected page uniqueness")
        registry[page.ordinal] = value
    start = None
    for query in expected:
        _require(
            type(query) is GlobalQueryExpectation
            and _integer(query.query_ordinal)
            and _digest(query.candidate_replay_sha256),
            "expected query",
        )
        start = query.query_ordinal if start is None else start
        _require(
            type(query.page_ordinals) is tuple
            and len(query.page_ordinals) == 16
            and all(_integer(p) and p in registry for p in query.page_ordinals)
            and len(set(query.page_ordinals)) == 16,
            "expected page sequence",
        )
    _require(
        [q.query_ordinal for q in expected] == list(range(start, start + 32)),
        "expected query sequence",
    )
    batch = _parse(payload)
    _require(
        _keys(
            batch,
            {
                "schema_version",
                "claim_eligible",
                "routing_scope",
                "configuration",
                "results",
            },
        )
        and type(batch["schema_version"]) is int
        and batch["schema_version"] == 3
        and batch["claim_eligible"] is False
        and batch["routing_scope"] == "global"
        and _configuration(batch["configuration"])
        and type(batch["results"]) is list
        and len(batch["results"]) == 32,
        "batch authority",
    )
    row_keys = {
        "schema_version",
        "claim_eligible",
        "routing_scope",
        "global_leaf_limit",
        "configuration",
        "candidate_replay_sha256",
        "requested_pages",
        "matches",
        "timing",
        "work",
    }
    for row, query in zip(batch["results"], expected, strict=True):
        _require(
            _keys(row, row_keys)
            and type(row["schema_version"]) is int
            and row["schema_version"] == 3
            and row["claim_eligible"] is False
            and row["routing_scope"] == "global"
            and type(row["global_leaf_limit"]) is int
            and row["global_leaf_limit"] == 768
            and _configuration(row["configuration"]),
            "row authority",
        )
        _require(
            row["candidate_replay_sha256"] == query.candidate_replay_sha256,
            "replay identity",
        )
        actual = row["requested_pages"]
        _require(type(actual) is list and len(actual) == 16, "page cardinality")
        for page, ordinal in zip(actual, query.page_ordinals, strict=True):
            _page(page)
            _require(page == registry[ordinal], "registered page identity")
        matches = row["matches"]
        _require(type(matches) is list and len(matches) == 10, "match cardinality")
        match_keys = []
        for match in matches:
            _require(
                _keys(match, {"source_ordinal", "squared_distance"}), "match schema"
            )
            distance = match["squared_distance"]
            _require(
                _integer(match["source_ordinal"], 0, source_rows - 1)
                and type(distance) in (int, float)
                and 0 <= distance <= 1.7976931348623157e308
                and math.isfinite(distance),
                "match value",
            )
            match_keys.append((distance, match["source_ordinal"]))
        _require(
            match_keys == sorted(match_keys) and len({s for _, s in match_keys}) == 10,
            "match order",
        )
        timing = row["timing"]
        _require(
            _keys(timing, _TIMING)
            and all(_integer(v) for v in timing.values())
            and timing["elapsed_ns"] > 0
            and timing["peak_rss_bytes"] > 0,
            "timing values",
        )
        for suffix, total in (
            ("cpu_ns", "process_cpu_ns"),
            ("elapsed_ns", "elapsed_ns"),
        ):
            _require(
                sum(
                    timing[f"{phase}_{suffix}"]
                    for phase in ("routing", "page_read", "exact_rerank")
                )
                <= timing[total],
                "phase sums",
            )
        work = row["work"]
        _require(
            _keys(
                work,
                {
                    "decoded_rows",
                    "unique_rows",
                    "encoded_bytes",
                    "get_count",
                    "routing",
                },
            ),
            "work schema",
        )
        routing = work["routing"]
        _require(
            _keys(routing, _ROUTING)
            and all(_integer(v) for v in routing.values())
            and all(
                _integer(work[k])
                for k in ("decoded_rows", "unique_rows", "encoded_bytes", "get_count")
            ),
            "work types",
        )
        _require(
            work["get_count"] == routing["selected_pages"] == 16
            and work["encoded_bytes"]
            == sum(p["encoded_bytes"] for p in actual)
            <= 3145728
            and work["decoded_rows"]
            == work["unique_rows"]
            == sum(p["primary_rows"] for p in actual),
            "physical accounting",
        )
        _require(
            routing["roots_scored"] >= 1
            and 1 <= routing["leaves_scanned"] <= min(768, routing["leaves_eligible"])
            and 1 <= routing["query_table_pairs_built"] <= routing["leaves_scanned"]
            and routing["peak_query_table_pairs_live"] == 1
            and 16
            <= routing["candidates_retained"]
            == min(12288, routing["codes_scanned"])
            and routing["codes_scanned"] <= min(262144, source_rows)
            and routing["pages_considered"] == 16,
            "routing bounds",
        )
    return batch


def summarize_global_serving_batch(
    payload: bytes,
    *,
    expected: tuple[GlobalQueryExpectation, ...],
    pages: tuple[GlobalPageIdentity, ...],
    source_rows: int,
    truth: tuple[tuple[int, ...], ...],
) -> dict:
    """Reduce authenticated query truth and serving rows, including quality failure.

    The caller must authenticate truth and identify the actual serving tier.
    Neither empirical quantiles nor logical reads claim stable tails or retries.
    """
    batch = validate_global_serving_batch(
        payload, expected=expected, pages=pages, source_rows=source_rows
    )
    _require(type(truth) is tuple and len(truth) == 32, "truth cardinality")
    samples = []
    for query, row, neighbors in zip(expected, batch["results"], truth, strict=True):
        _require(
            type(neighbors) is tuple
            and len(neighbors) == 10
            and all(_integer(n, 0, source_rows - 1) for n in neighbors)
            and len(set(neighbors)) == 10,
            "truth membership",
        )
        hits = len(set(neighbors) & {m["source_ordinal"] for m in row["matches"]})
        samples.append(
            {
                "query_ordinal": query.query_ordinal,
                "hits": hits,
                "recall_ppm": hits * 100000,
            }
        )

    def empirical(values):
        ranked = sorted(values)
        return {
            "sample_count": 32,
            "p50": ranked[15],
            "p95": ranked[30],
            "maximum": ranked[-1],
            "total": sum(ranked),
        }

    total_hits = sum(s["hits"] for s in samples)
    rows = batch["results"]
    return {
        "schema_version": 1,
        "status": "complete",
        "claim_eligible": False,
        "source_rows": source_rows,
        "query_start": expected[0].query_ordinal,
        "query_count": 32,
        "batch_sha256": hashlib.sha256(payload).hexdigest(),
        "batch_bytes": len(payload),
        "routing_parity_passed": True,
        "quality_target_ppm": 1000000,
        "quality_passed": total_hits == 320,
        "aggregate_recall_ppm": total_hits * 1000000 // 320,
        "minimum_recall_ppm": min(s["recall_ppm"] for s in samples),
        "perfect_queries": sum(s["hits"] == 10 for s in samples),
        "samples": samples,
        "timing": {
            name: empirical([row["timing"][name] for row in rows])
            for name in sorted(_TIMING - {"peak_rss_bytes"})
        },
        "quantile_method": "empirical-nearest-rank-not-stable-tail-estimate",
        "peak_reported_rss_bytes": max(row["timing"]["peak_rss_bytes"] for row in rows),
        "logical_page_reads": sum(row["work"]["get_count"] for row in rows),
        "encoded_bytes": sum(row["work"]["encoded_bytes"] for row in rows),
        "transport_attempts": None,
        "rows": rows,
    }
