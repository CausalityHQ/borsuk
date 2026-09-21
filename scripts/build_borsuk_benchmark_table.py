#!/usr/bin/env python3
"""Build the authenticated BORSUK next-result benchmark table."""

from __future__ import annotations

import argparse
import math
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = "borsuk-benchmark-table-v1"
STATUSES = {"measured", "estimated", "blocked"}
MISSING_FLOAT = -1.0
MISSING_INT = -1


@dataclass(frozen=True)
class BenchmarkRow:
    schema: str
    system: str
    corpus: str
    split: str
    n: int
    dimensions: int
    queries: int
    k: int
    mode: str
    source_commit: str
    seed: str
    raw_result_uri: str
    raw_samples_uri: str
    evidence_sha256: str
    hardware: str
    exact_command: str
    repetitions_ci: str
    latency_context: str
    build_time_s: float
    build_status: str
    index_bytes: int
    bytes_per_vector: float
    index_status: str
    peak_rss_bytes: int
    rss_status: str
    cold_latency_p50_ms: float
    cold_latency_p95_ms: float
    cold_latency_p99_ms: float
    cold_latency_status: str
    warm_latency_p50_ms: float
    warm_latency_p95_ms: float
    warm_latency_p99_ms: float
    warm_latency_status: str
    qps_c1: float
    qps_c1_status: str
    qps_batch: float
    batch_concurrency: int
    qps_batch_status: str
    get_count_mean: float
    get_count_p50: float
    get_count_status: str
    service_requests_per_query: float
    service_request_status: str
    bytes_query_mean: float
    bytes_query_p50: float
    bytes_query_status: str
    average_recall10_ppm: int
    average_recall100_ppm: int
    p05_recall100_ppm: int
    worst_recall100_ppm: int
    quality_status: str
    cost_usd: float
    cost_scope: str
    cost_status: str
    overall_status: str
    evidence_note: str
    scaling_from_n: int
    scale_n_ratio: float
    build_time_slope_exponent: float
    index_bytes_slope_exponent: float
    peak_rss_slope_exponent: float
    warm_p50_slope_exponent: float
    scaling_status: str
    scaling_note: str


def _slope(high: float, low: float) -> float:
    return math.log(high / low) / math.log(10.0)


def benchmark_rows() -> list[BenchmarkRow]:
    native_100k = BenchmarkRow(
        schema=SCHEMA,
        system="BORSUK",
        corpus="ReLAION",
        split="development-100k",
        n=100_000,
        dimensions=768,
        queries=1_000,
        k=100,
        mode="native-production-exact-page-score",
        source_commit="69be0e20f74b892dce20ecfa33a865c11b55a0a3",
        seed="deterministic-production-builder",
        raw_result_uri=(
            "s3://borsuk-bench-453182569524-euc1/research/native-ann-100k/"
            "69be0e20f74b892dce20ecfa33a865c11b55a0a3/runs/"
            "native-100k-dev1000-20260921T043822Z-69be0e20/a0001/evidence/result.json"
        ),
        raw_samples_uri=(
            "s3://borsuk-bench-453182569524-euc1/research/native-ann-100k/"
            "69be0e20f74b892dce20ecfa33a865c11b55a0a3/runs/"
            "native-100k-dev1000-20260921T043822Z-69be0e20/a0001/evidence/"
            "samples.parquet"
        ),
        evidence_sha256="d29690543c637d1d50f1cdc370e68d30bcad00b766ba32b5ef484c262d5426e6",
        hardware="c7i.8xlarge Spot client; local filesystem object store",
        exact_command=(
            "target/release/examples/native_ann_100k_qualify --source source.parquet "
            "s3://borsuk-bench-453182569524-euc1/research/v85-pq16-page-nomination/"
            "24383d853474a19702d18d2de700bee3618167f5/100k-a0023/attempt/inputs/"
            "source-100k.parquet a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d "
            "145121661 --queries queries.parquet s3://borsuk-bench-453182569524-euc1/research/"
            "v85-competitive-rescore/fb976932ecd4076e2f76a7cb7e7aa7efe01e9a2d/runs/"
            "v85-100k-dev1000-20260920T094401Z-fb976932/a0001/inputs/queries.parquet "
            "4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac "
            "1544342 --truth truth.parquet s3://borsuk-bench-453182569524-euc1/research/"
            "v85-competitive-rescore/fb976932ecd4076e2f76a7cb7e7aa7efe01e9a2d/runs/"
            "v85-100k-dev1000-20260920T094401Z-fb976932/a0001/inputs/truth-100k.parquet "
            "ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7 512093 "
            "--index-uri index --samples samples.parquet --result result.json --source-commit "
            "69be0e20f74b892dce20ecfa33a865c11b55a0a3 --execute-native-ann-100k"
        ),
        repetitions_ci="one immutable attempt; 1,000 per-query samples; no CI",
        latency_context="warm local object-store path; not S3 cold latency",
        build_time_s=18.588548841,
        build_status="measured",
        index_bytes=262_048_002,
        bytes_per_vector=2_620.48002,
        index_status="estimated",
        peak_rss_bytes=2_315_800_576,
        rss_status="measured",
        cold_latency_p50_ms=MISSING_FLOAT,
        cold_latency_p95_ms=MISSING_FLOAT,
        cold_latency_p99_ms=MISSING_FLOAT,
        cold_latency_status="blocked",
        warm_latency_p50_ms=19.984121,
        warm_latency_p95_ms=20.764932,
        warm_latency_p99_ms=21.931541,
        warm_latency_status="measured",
        qps_c1=49.89970538455465,
        qps_c1_status="measured",
        qps_batch=MISSING_FLOAT,
        batch_concurrency=MISSING_INT,
        qps_batch_status="blocked",
        get_count_mean=16.778,
        get_count_p50=17.0,
        get_count_status="measured",
        service_requests_per_query=MISSING_FLOAT,
        service_request_status="blocked",
        bytes_query_mean=16_347_716.032,
        bytes_query_p50=16_354_208.0,
        bytes_query_status="measured",
        average_recall10_ppm=517_200,
        average_recall100_ppm=339_530,
        p05_recall100_ppm=160_000,
        worst_recall100_ppm=80_000,
        quality_status="measured",
        cost_usd=MISSING_FLOAT,
        cost_scope="Spot price was not preserved in the terminal",
        cost_status="blocked",
        overall_status="measured-failed-quality",
        evidence_note=(
            "Exact scoring was applied only after routing; the router exposed 5.118% of rows "
            "on average and caused the quality failure. Index bytes are the reported segment "
            "plus vector bytes, not a filesystem high-water measurement."
        ),
        scaling_from_n=MISSING_INT,
        scale_n_ratio=MISSING_FLOAT,
        build_time_slope_exponent=MISSING_FLOAT,
        index_bytes_slope_exponent=MISSING_FLOAT,
        peak_rss_slope_exponent=MISSING_FLOAT,
        warm_p50_slope_exponent=MISSING_FLOAT,
        scaling_status="blocked",
        scaling_note="baseline row",
    )

    bounded_1m = BenchmarkRow(
        schema=SCHEMA,
        system="BORSUK",
        corpus="ReLAION",
        split="development-1m",
        n=1_000_000,
        dimensions=768,
        queries=1_000,
        k=100,
        mode="bounded-native-sq8",
        source_commit="26716f9eae90688ea0b047a83211d99855062858",
        seed="layout=frozen; pq=6801; row-router=7301",
        raw_result_uri=(
            "s3://borsuk-bench-453182569524-euc1/research/bounded-reader-next-result/"
            "26716f9eae90688ea0b047a83211d99855062858/runs/"
            "bounded-reader-20260921T074858Z-26716f9/a0001/result.json"
        ),
        raw_samples_uri=(
            "s3://borsuk-bench-453182569524-euc1/research/bounded-reader-next-result/"
            "26716f9eae90688ea0b047a83211d99855062858/runs/"
            "bounded-reader-20260921T074858Z-26716f9/a0001/samples.parquet"
        ),
        evidence_sha256="532080d202538dc252640257fb5f30eea0de3b4453b897f45eb000962989133d",
        hardware="c7i.12xlarge Spot client; S3 Standard eu-central-1",
        exact_command=(
            "env AWS_DEFAULT_REGION=eu-central-1 BORSUK_V71_REGION=eu-central-1 "
            "BORSUK_V71_QUERIES=1000 BORSUK_V71_REGIONS=256 BORSUK_V71_SHORTLIST=512 "
            "BORSUK_V71_GAP=2 BORSUK_V71_CONCURRENCY=128 BORSUK_V71_THROUGHPUT=1 "
            "BORSUK_V71_URI=s3://borsuk-bench-453182569524-euc1/research/"
            "v70-algorithm-first/single-stage-a4a695d66f508edf/index/sq8.bin "
            "BORSUK_V71_MANIFEST=/mnt/bounded-reader-1m/manifest.bin "
            "BORSUK_V71_OUTPUT=/mnt/bounded-reader-1m/result.json "
            "BORSUK_V71_SAMPLES=/mnt/bounded-reader-1m/samples.parquet "
            "BORSUK_SOURCE_COMMIT=26716f9eae90688ea0b047a83211d99855062858 "
            "BORSUK_SQ8_SHA256=2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b "
            "/mnt/bounded-reader-1m/repo/target/release/v71_native_reader"
        ),
        repetitions_ci="one immutable attempt; two fixed 1,000-query passes; no CI",
        latency_context=(
            "first connection and immediate connection reuse; neither label asserts service cache state"
        ),
        build_time_s=149.656,
        build_status="measured",
        index_bytes=872_669_264,
        bytes_per_vector=872.669264,
        index_status="estimated",
        peak_rss_bytes=11_606_437_888,
        rss_status="measured",
        cold_latency_p50_ms=41.457907,
        cold_latency_p95_ms=70.073566,
        cold_latency_p99_ms=161.861794,
        cold_latency_status="measured",
        warm_latency_p50_ms=41.765422,
        warm_latency_p95_ms=65.508648,
        warm_latency_p99_ms=89.746010,
        warm_latency_status="measured",
        qps_c1=21.199631216515264,
        qps_c1_status="measured",
        qps_batch=182.39226711391123,
        batch_concurrency=128,
        qps_batch_status="measured",
        get_count_mean=23.085,
        get_count_p50=20.0,
        get_count_status="measured",
        service_requests_per_query=MISSING_FLOAT,
        service_request_status="blocked",
        bytes_query_mean=11_989_336.0,
        bytes_query_p50=10_982_400.0,
        bytes_query_status="measured",
        average_recall10_ppm=992_800,
        average_recall100_ppm=990_260,
        p05_recall100_ppm=970_000,
        worst_recall100_ppm=820_000,
        quality_status="measured",
        cost_usd=0.0842,
        cost_scope="Spot compute only; excludes S3 request charges",
        cost_status="measured",
        overall_status="measured-next-result-baseline-not-rc",
        evidence_note=(
            "Build time is the measured build of the exact historical 780,000,000-byte SQ8 "
            "object and excludes global layout construction. Index bytes add that object to the "
            "format-derived 92,669,264-byte runtime manifest. Peak RSS includes the aggressive "
            "throughput ladder."
        ),
        scaling_from_n=100_000,
        scale_n_ratio=10.0,
        build_time_slope_exponent=_slope(149.656, native_100k.build_time_s),
        index_bytes_slope_exponent=_slope(872_669_264, native_100k.index_bytes),
        peak_rss_slope_exponent=_slope(11_606_437_888, native_100k.peak_rss_bytes),
        warm_p50_slope_exponent=_slope(41.765422, native_100k.warm_latency_p50_ms),
        scaling_status="estimated",
        scaling_note=(
            "cross-architecture descriptive slope only: 100k production exact-page routing and "
            "1M bounded SQ8 use different formats, storage paths, and routing; not a causal scale curve"
        ),
    )

    s3_vectors = BenchmarkRow(
        schema=SCHEMA,
        system="Amazon S3 Vectors",
        corpus="ReLAION",
        split="development-1m",
        n=1_000_000,
        dimensions=768,
        queries=1_000,
        k=100,
        mode="matched-managed-service",
        source_commit="db47333f3ace06d0e6cb0dbd6fcf41150b5e87c2",
        seed="query-order=20260921",
        raw_result_uri=(
            "s3://borsuk-bench-453182569524-euc1/research/matched-s3-vectors-next-result/"
            "db47333f3ace06d0e6cb0dbd6fcf41150b5e87c2/runs/"
            "matched-20260921T080153Z-db47333/a0001/evidence/result.json"
        ),
        raw_samples_uri=(
            "s3://borsuk-bench-453182569524-euc1/research/matched-s3-vectors-next-result/"
            "db47333f3ace06d0e6cb0dbd6fcf41150b5e87c2/runs/"
            "matched-20260921T080153Z-db47333/a0001/evidence/samples.parquet"
        ),
        evidence_sha256="b6d392170152a535596bd706349e44acd64b38ed1108197e71745c317b51d009",
        hardware="c7i.8xlarge Spot client; managed S3 Vectors server opaque",
        exact_command=(
            "python scripts/benchmark_s3_vectors_parquet.py --source source.parquet "
            "--source-uri s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/"
            "runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet "
            "--source-sha256 2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86 "
            "--source-bytes 1458450077 --queries queries.parquet --queries-uri s3://"
            "borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/"
            "v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-query.parquet "
            "--queries-sha256 310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54 "
            "--queries-bytes 1558506 --truth truth.parquet --truth-uri s3://"
            "borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/"
            "v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-gt100.parquet "
            "--truth-sha256 fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11 "
            "--truth-bytes 2046505 --output-dir result --vector-bucket "
            "borsuk-ma-ched-453182569524-20260921-080153 --source-commit "
            "db47333f3ace06d0e6cb0dbd6fcf41150b5e87c2 --settle-seconds 60"
        ),
        repetitions_ci="one immutable attempt; two fixed 1,000-query passes; no CI",
        latency_context=(
            "fresh index first pass and immediate repeated pass; vendor-managed cache state opaque; "
            "query order differs from BORSUK"
        ),
        build_time_s=848.089938547,
        build_status="measured",
        index_bytes=MISSING_INT,
        bytes_per_vector=MISSING_FLOAT,
        index_status="blocked",
        peak_rss_bytes=565_022_720,
        rss_status="measured",
        cold_latency_p50_ms=73.350227,
        cold_latency_p95_ms=239.556294,
        cold_latency_p99_ms=328.051430,
        cold_latency_status="measured",
        warm_latency_p50_ms=61.372256,
        warm_latency_p95_ms=93.515790,
        warm_latency_p99_ms=120.567274,
        warm_latency_status="measured",
        qps_c1=9.482279111807712,
        qps_c1_status="measured",
        qps_batch=MISSING_FLOAT,
        batch_concurrency=MISSING_INT,
        qps_batch_status="blocked",
        get_count_mean=MISSING_FLOAT,
        get_count_p50=MISSING_FLOAT,
        get_count_status="blocked",
        service_requests_per_query=1.0,
        service_request_status="measured",
        bytes_query_mean=5_044.491,
        bytes_query_p50=5_051.0,
        bytes_query_status="measured",
        average_recall10_ppm=976_800,
        average_recall100_ppm=909_380,
        p05_recall100_ppm=670_000,
        worst_recall100_ppm=390_000,
        quality_status="measured",
        cost_usd=0.212707,
        cost_scope="Spot client compute only; excludes S3 Vectors service charges",
        cost_status="measured",
        overall_status="measured-matched-workload",
        evidence_note=(
            "Service index bytes and physical object-store GETs are opaque. Response bytes are only "
            "the HTTP response body. Latency is matched-workload evidence, not paired query-order evidence."
        ),
        scaling_from_n=MISSING_INT,
        scale_n_ratio=MISSING_FLOAT,
        build_time_slope_exponent=MISSING_FLOAT,
        index_bytes_slope_exponent=MISSING_FLOAT,
        peak_rss_slope_exponent=MISSING_FLOAT,
        warm_p50_slope_exponent=MISSING_FLOAT,
        scaling_status="blocked",
        scaling_note="no authenticated matched 100k S3 Vectors cell",
    )
    rows = [native_100k, bounded_1m, s3_vectors]
    validate_rows(rows)
    return rows


def _require_value(value: float | int | None, status: str, name: str) -> None:
    if status not in STATUSES:
        raise ValueError(f"{name} status differs")
    missing = value is None or value < 0
    if status == "blocked" and not missing:
        raise ValueError(f"{name} blocked value differs")
    if status != "blocked" and missing:
        raise ValueError(f"{name} is absent for {status}")


def validate_rows(rows: list[BenchmarkRow]) -> None:
    if not rows or len({(row.system, row.n, row.mode) for row in rows}) != len(rows):
        raise ValueError("benchmark row identity differs")
    for row in rows:
        if (
            row.schema != SCHEMA
            or row.n <= 0
            or row.dimensions <= 0
            or row.queries != 1_000
            or row.k != 100
            or len(row.source_commit) != 40
            or len(row.evidence_sha256) != 64
            or not row.raw_result_uri.startswith("s3://")
            or not row.raw_samples_uri.startswith("s3://")
            or row.quality_status != "measured"
        ):
            raise ValueError("benchmark authority differs")
        for name, value, status in (
            ("build_time_s", row.build_time_s, row.build_status),
            ("index_bytes", row.index_bytes, row.index_status),
            ("peak_rss_bytes", row.peak_rss_bytes, row.rss_status),
            ("cold_latency", row.cold_latency_p50_ms, row.cold_latency_status),
            ("warm_latency", row.warm_latency_p50_ms, row.warm_latency_status),
            ("qps_c1", row.qps_c1, row.qps_c1_status),
            ("qps_batch", row.qps_batch, row.qps_batch_status),
            ("get_count", row.get_count_mean, row.get_count_status),
            ("service_requests", row.service_requests_per_query, row.service_request_status),
            ("bytes_query", row.bytes_query_mean, row.bytes_query_status),
            ("cost_usd", row.cost_usd, row.cost_status),
            ("scaling", row.scale_n_ratio, row.scaling_status),
        ):
            _require_value(value, status, name)
        if row.cold_latency_status == "blocked":
            if any(value >= 0 for value in (row.cold_latency_p95_ms, row.cold_latency_p99_ms)):
                raise ValueError("cold_latency blocked tail differs")
        elif min(row.cold_latency_p95_ms, row.cold_latency_p99_ms) < 0:
            raise ValueError("cold_latency tail is absent")
        if row.warm_latency_status == "blocked":
            if any(value >= 0 for value in (row.warm_latency_p95_ms, row.warm_latency_p99_ms)):
                raise ValueError("warm_latency blocked tail differs")
        elif min(row.warm_latency_p95_ms, row.warm_latency_p99_ms) < 0:
            raise ValueError("warm_latency tail is absent")


def _schema() -> pa.Schema:
    fields: list[pa.Field] = []
    sample = asdict(benchmark_rows()[0])
    integer_fields = {
        "n",
        "dimensions",
        "queries",
        "k",
        "index_bytes",
        "peak_rss_bytes",
        "batch_concurrency",
        "average_recall10_ppm",
        "average_recall100_ppm",
        "p05_recall100_ppm",
        "worst_recall100_ppm",
        "scaling_from_n",
    }
    float_fields = {
        name
        for name, value in sample.items()
        if isinstance(value, float)
    }
    for name in sample:
        if name in integer_fields:
            field_type = pa.int64()
        elif name in float_fields:
            field_type = pa.float64()
        else:
            field_type = pa.string()
        fields.append(pa.field(name, field_type, nullable=False))
    return pa.schema(fields, metadata={b"schema": SCHEMA.encode()})


def _percent(ppm: int) -> str:
    return f"{ppm / 10_000:.3f}%"


def _value(value: float, status: str, digits: int = 2) -> str:
    return "blocked" if status == "blocked" else f"{value:.{digits}f}"


def _corpus_label(row: BenchmarkRow) -> str:
    scale = f"{row.n // 1_000_000}M" if row.n % 1_000_000 == 0 else f"{row.n // 1_000}k"
    split = row.split.rsplit("-", 1)[0]
    return f"{row.corpus}-{scale} {split}"


def operator_markdown(rows: list[BenchmarkRow]) -> str:
    by_identity = {(row.system, row.n): row for row in rows}
    native = by_identity[("BORSUK", 100_000)]
    bounded = by_identity[("BORSUK", 1_000_000)]
    service = by_identity[("Amazon S3 Vectors", 1_000_000)]
    lines = [
        "# BORSUK measured benchmark checkpoint",
        "",
        "All rows use 1,000 queries, `k=100`, 768 dimensions, and exact GT100. ",
        "`blocked` means the authority did not measure the field; it is never treated as zero. ",
        "The 1M product comparison is matched-workload, not paired query order.",
        "",
        "| system / mode | corpus | R@10 | mean / p05 / worst R@100 | first p50/p95/p99 ms | reuse p50/p95/p99 ms | QPS c1 / batch | GETs mean / bytes mean | RSS | build | cost |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        first = "/".join(
            _value(value, row.cold_latency_status)
            for value in (
                row.cold_latency_p50_ms,
                row.cold_latency_p95_ms,
                row.cold_latency_p99_ms,
            )
        )
        warm = "/".join(
            _value(value, row.warm_latency_status)
            for value in (
                row.warm_latency_p50_ms,
                row.warm_latency_p95_ms,
                row.warm_latency_p99_ms,
            )
        )
        lines.append(
            f"| {row.system} / `{row.mode}` | {_corpus_label(row)} | "
            f"{_percent(row.average_recall10_ppm)} | {_percent(row.average_recall100_ppm)} / "
            f"{_percent(row.p05_recall100_ppm)} / {_percent(row.worst_recall100_ppm)} | "
            f"{first} | {warm} | {_value(row.qps_c1, row.qps_c1_status)} / "
            f"{_value(row.qps_batch, row.qps_batch_status)}@{row.batch_concurrency if row.batch_concurrency >= 0 else 'blocked'} | "
            f"{_value(row.get_count_mean, row.get_count_status, 3)} / "
            f"{_value(row.bytes_query_mean / (1024 * 1024), row.bytes_query_status, 3)} MiB | "
            f"{row.peak_rss_bytes / (1024**3):.2f} GiB | "
            f"{_value(row.build_time_s, row.build_status, 3)} s | "
            f"{_value(row.cost_usd, row.cost_status, 4)} USD |"
        )
    lines.extend(
        [
            "",
            "## Decision",
            "",
            f"- ReLAION-100k development production routing is killed: mean R@100 is {_percent(native.average_recall100_ppm)} despite exact candidate scoring.",
            f"- The unchanged 1M bounded reader is the quality/latency baseline: mean R@100 {_percent(bounded.average_recall100_ppm)}, reused p99 {bounded.warm_latency_p99_ms:.2f} ms, peak {bounded.qps_batch:.1f} QPS.",
            f"- Matched S3 Vectors reaches mean R@100 {_percent(service.average_recall100_ppm)}; its physical GETs and index bytes are service-opaque.",
            "- The 100k→1M slope fields are descriptive only because the two BORSUK rows use different formats, routing, and storage paths. No 10M/100M value is presented as measured.",
            "- An exact brute-force latency cell was not run: exact GT100 already fixes the quality control, and brute-force latency would not choose between the current product integration options.",
            "- Turbopuffer is blocked: no authenticated tenant/namespace credential was available, so no matched row is emitted.",
            "",
            "## Next production gate",
            "",
            "Bound concurrent ranged-response memory, then attach the measured bounded reader to authenticated native generations, delta merge, mutation visibility, and compaction. Qualify those semantics at 100k before any larger spend.",
            "",
        ]
    )
    return "\n".join(lines)


def write_artifacts(parquet_path: Path, markdown_path: Path) -> None:
    rows = benchmark_rows()
    table = pa.Table.from_pylist([asdict(row) for row in rows], schema=_schema())
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, parquet_path, compression="zstd", write_statistics=True)
    markdown_path.write_text(operator_markdown(rows))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--markdown", type=Path, required=True)
    args = parser.parse_args()
    write_artifacts(args.parquet, args.markdown)


if __name__ == "__main__":
    main()
