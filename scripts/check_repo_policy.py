#!/usr/bin/env python3
"""Repository hygiene checks that are cheap enough for CI and pre-commit."""

from __future__ import annotations

import csv
import io
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIN_HIGH_RECALL_TIE_AWARE_RECALL_AT_10 = 0.95


def git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def require(condition: bool, message: str) -> None:
    if not condition:
        print(message, file=sys.stderr)
        raise SystemExit(1)


def assert_tracked(path: str) -> None:
    result = git("ls-files", "--error-unmatch", path)
    require(
        result.returncode == 0,
        f"{path} must be tracked for reproducible CI and publish builds",
    )


def assert_not_ignored(path: str) -> None:
    result = git("check-ignore", "-q", path)
    require(result.returncode != 0, f"{path} must not be ignored")


def assert_ignored(path: str) -> None:
    result = git("check-ignore", "-q", path)
    require(result.returncode == 0, f"{path} must be ignored")


def assert_no_files_matching(root: str, patterns: list[str], reason: str) -> None:
    base = ROOT / root
    matches = sorted(
        str(path.relative_to(ROOT))
        for pattern in patterns
        for path in base.glob(pattern)
        if path.is_file()
    )
    require(
        not matches,
        f"{root} must not contain generated files for {reason}: {matches}",
    )


def assert_contains(path: str, needle: str, reason: str) -> None:
    text = (ROOT / path).read_text()
    require(
        needle in text,
        f"{path} must contain `{needle}` for {reason}",
    )


def assert_not_contains(path: str, needle: str, reason: str) -> None:
    text = (ROOT / path).read_text()
    require(
        needle not in text,
        f"{path} must not contain `{needle}` for {reason}",
    )


def assert_no_viewport_font_sizing(path: str) -> None:
    text = (ROOT / path).read_text()
    for match in re.finditer(r"font-size\s*:[^;]*(?:vw|vh|vmin|vmax)", text):
        declaration = match.group(0).strip()
        require(
            False,
            f"{path} must not use viewport units for font-size: `{declaration}`",
        )


def assert_benchmark_recall_rows(
    path: str,
    csv_text: str,
    required_rows: list[dict[str, str]],
    min_recall: float = MIN_HIGH_RECALL_TIE_AWARE_RECALL_AT_10,
) -> None:
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    for required in required_rows:
        matching = benchmark_row(path, rows, required)
        recall_text = matching.get("tie_aware_recall_at_10")
        require(
            recall_text is not None,
            f"{path} required benchmark row {required} is missing tie-aware recall",
        )
        try:
            recall = float(recall_text)
        except ValueError:
            require(
                False,
                f"{path} required benchmark row {required} has non-numeric tie-aware recall `{recall_text}`",
            )
        require(
            recall >= min_recall,
            f"{path} required benchmark row {required} tie-aware recall {recall:.6f} is below {min_recall:.2f}",
        )


def assert_benchmark_numeric_rows(
    path: str,
    csv_text: str,
    required_rows: list[dict[str, str]],
    field_minimums: dict[str, float],
) -> None:
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    for required in required_rows:
        matching = benchmark_row(path, rows, required)
        for field, minimum in field_minimums.items():
            value_text = matching.get(field)
            require(
                value_text is not None,
                f"{path} required benchmark row {required} is missing {field}",
            )
            try:
                value = float(value_text)
            except ValueError:
                require(
                    False,
                    f"{path} required benchmark row {required} has non-numeric {field} `{value_text}`",
                )
            require(
                value >= minimum,
                f"{path} required benchmark row {required} {field} {value:.6f} is below {minimum:.6f}",
            )


def benchmark_row(
    path: str, rows: list[dict[str, str]], required: dict[str, str]
) -> dict[str, str]:
    matching = next(
        (
            row
            for row in rows
            if all(row.get(column) == value for column, value in required.items())
        ),
        None,
    )
    require(
        matching is not None,
        f"{path} missing required benchmark row {required}",
    )
    return matching


def main() -> None:
    require((ROOT / "Cargo.lock").is_file(), "Cargo.lock must exist")
    require(not (ROOT / "design.md").exists(), "design.md was removed; use docs/ instead")
    require(not (ROOT / "multimode.md").exists(), "multimode.md was removed; use docs/ instead")
    assert_not_ignored("Cargo.lock")
    assert_tracked("Cargo.lock")
    assert_not_ignored("LICENSE")
    assert_not_ignored("python/LICENSE")
    assert_not_ignored("packages/borsuk/LICENSE")
    assert_tracked("crates/borsuk/examples/s3_index.rs")
    assert_tracked("docs/api.md")
    assert_tracked("docs/architecture.md")
    assert_tracked("docs/benchmarks.md")
    assert_tracked("docs/production-readiness.md")
    assert_tracked("docs/storage-format.md")
    assert_tracked("docs/web/assets/benchmarks/large-scale.csv")
    assert_tracked("crates/borsuk/tests/large_scale.rs")
    assert_tracked("python/README.md")
    assert_tracked("python/examples/local_index.py")
    assert_tracked("python/examples/s3_index.py")
    assert_tracked("python/src/borsuk/__init__.pyi")
    assert_tracked("python/src/borsuk/py.typed")
    assert_not_ignored("python/tests/typing_usage.py")
    assert_tracked("packages/borsuk/examples/local-index.ts")
    assert_tracked("packages/borsuk/examples/s3-index.ts")
    assert_tracked("examples/seaweedfs/run-smoke.sh")
    assert_tracked("python/tests/test_api.py")
    assert_tracked("packages/borsuk/test/api.test.ts")
    assert_tracked("scripts/test_check_repo_policy.py")
    assert_tracked("scripts/test_docs_web.mjs")
    assert_no_files_matching(
        "python/src/borsuk",
        ["_borsuk*.so", "_borsuk*.pyd", "_borsuk*.dll", "_borsuk*.dylib"],
        "reproducible Python 3.12+ imports; native extensions must come from built wheels",
    )
    assert_no_files_matching(
        "crates/borsuk-node",
        ["index.js", "index.d.ts", "index.*.node"],
        "reproducible TypeScript package builds; generated N-API outputs belong under packages/borsuk",
    )

    ignored_outputs = [
        "target/debug/example",
        "packages/borsuk/dist/src/index.js",
        "packages/borsuk/node_modules/example/package.json",
        "packages/borsuk/index.cjs",
        "packages/borsuk/native.d.ts",
        "packages/borsuk/index.darwin-arm64.node",
        "crates/borsuk-node/index.js",
        "crates/borsuk-node/index.d.ts",
        "crates/borsuk-node/index.darwin-arm64.node",
        "python/dist/borsuk-0.1.0.whl",
        "python/.venv/pyvenv.cfg",
        "python/src/borsuk/_borsuk.abi3.so",
    ]
    for path in ignored_outputs:
        assert_ignored(path)

    locked_cargo_commands = {
        ".github/workflows/ci.yml": [
            "GitHub Actions lint",
            "actions/setup-go@v6",
            "go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 -shellcheck= -pyflakes=",
            "python-package:",
            "TypeScript package (${{ matrix.os }}, node${{ matrix.node-version }})",
            "Python package (${{ matrix.os }}, py${{ matrix.python-version }})",
            "os: [ubuntu-latest, ubuntu-24.04-arm, macos-26, macos-15-intel, windows-latest]",
            'python-version: ["3.12", "3.13", "3.14"]',
            'node-version: ["22", "24", "26"]',
            "cargo clippy --locked --workspace --all-targets -- -D warnings",
            "cargo test --locked --workspace --all-targets",
            "python -m unittest scripts/test_check_repo_policy.py",
            "Run Rust local example",
            "cargo run --locked -p borsuk --example local_index",
            "cargo bench --locked --workspace --no-run",
            "Web docs smoke",
            "node scripts/test_docs_web.mjs",
            "python -m pip install --upgrade maturin pyright",
            "maturin build --locked --out dist",
            "pyright tests/typing_usage.py",
            "cargo test --locked -p borsuk --test s3_compatible -- --nocapture",
            "Run Rust S3-compatible example",
            "cargo run --locked -p borsuk --example s3_index",
            "Run Python S3-compatible API tests",
            "Run TypeScript S3-compatible API tests",
            "SeaweedFS S3-Compatible Smoke",
            "./examples/seaweedfs/run-smoke.sh",
        ],
        ".github/workflows/publish.yml": [
            "Repo policy",
            "python scripts/check_repo_policy.py",
            "os: [ubuntu-latest, ubuntu-24.04-arm, macos-26, macos-15-intel, windows-latest]",
            'python-version: ["3.12", "3.13", "3.14"]',
            "node-version: \"24\"",
            "npm test",
            "cargo package --locked -p borsuk",
            "cargo publish --locked -p borsuk --token",
        ],
        ".github/workflows/pages.yml": [
            "Repo policy",
            "python scripts/check_repo_policy.py",
            "needs: repo-policy",
            "path: docs/web",
            "continue-on-error: true",
            "id: deployment_retry",
            "sleep 30",
            "steps.deployment.outputs.page_url || steps.deployment_retry.outputs.page_url",
        ],
        "Cargo.toml": [
            'repository = "https://github.com/CausalityHQ/borsuk"',
            'homepage = "http://causality.pl/borsuk/"',
        ],
        "crates/borsuk/Cargo.toml": [
            'readme = "../../README.md"',
            'keywords = ["ann", "similarity-search", "vector-search", "s3", "parquet"]',
            'categories = ["algorithms", "database-implementations", "science"]',
        ],
        "LICENSE": [
            "Business Source License 1.1",
            "US $100,000",
            "Change Date: 2030-07-02",
            "Change License: MIT License",
        ],
        "python/LICENSE": [
            "Business Source License 1.1",
            "US $100,000",
            "Change Date: 2030-07-02",
            "Change License: MIT License",
        ],
        "packages/borsuk/LICENSE": [
            "Business Source License 1.1",
            "US $100,000",
            "Change Date: 2030-07-02",
            "Change License: MIT License",
        ],
        ".pre-commit-config.yaml": [
            "github-actions-lint",
            "actionlint -shellcheck= -pyflakes=",
            "go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 -shellcheck= -pyflakes=",
            "cargo clippy --locked --workspace --all-targets -- -D warnings",
            "cargo test --locked --workspace --all-targets",
            "cargo package --locked -p borsuk --allow-dirty",
            "uvx maturin build --locked --out dist",
            'uv run --with "./$wheel" python -m unittest discover tests',
            "uvx pyright tests/typing_usage.py",
            "npm ci && npm run build:native && npm test",
        ],
        "packages/borsuk/package.json": [
            '"license": "BUSL-1.1"',
            '"engines":',
            '"node": ">=22 <27"',
            '"example:local": "npm run build && node dist/examples/local-index.js"',
            '"example:s3": "npm run build && node dist/examples/s3-index.js"',
            '"types": "./dist/src/index.d.ts"',
            '"repository":',
            '"homepage": "http://causality.pl/borsuk/"',
            '"url": "git+https://github.com/CausalityHQ/borsuk.git"',
            '"url": "https://github.com/CausalityHQ/borsuk/issues"',
            '"bugs":',
            '"keywords":',
        ],
        "python/pyproject.toml": [
            'requires-python = ">=3.12"',
            '"Programming Language :: Python :: 3.12"',
            '"Programming Language :: Python :: 3.13"',
            '"Programming Language :: Python :: 3.14"',
            'readme = "README.md"',
            '{ path = "README.md", format = "wheel" }',
            '{ path = "src/borsuk/__init__.pyi", format = "wheel" }',
            '{ path = "src/borsuk/py.typed", format = "wheel" }',
            "[project.urls]",
            'Homepage = "http://causality.pl/borsuk/"',
            'Repository = "https://github.com/CausalityHQ/borsuk"',
            'Documentation = "http://causality.pl/borsuk/"',
            'Issues = "https://github.com/CausalityHQ/borsuk/issues"',
        ],
        "examples/seaweedfs/README.md": [
            "./examples/seaweedfs/run-smoke.sh",
            "cargo run --locked -p borsuk --example s3_index",
        ],
        "examples/seaweedfs/run-smoke.sh": [
            "cargo run --locked -p borsuk --example s3_index",
            "python -m unittest discover tests",
            "npm test",
        ],
        "crates/borsuk/src/format.rs": [
            "pub fn vector_records_to_parquet",
            "pub fn vector_records_from_parquet",
            "vector records must contain only finite f32 values",
            "pivot vectors must contain only finite f32 values",
            "routing centroids must contain only finite f32 values",
            "routing radii must contain only finite f32 values",
            "id_bloom must be {SEGMENT_ID_BLOOM_BYTES} bytes when present",
            "vector_signature_bloom must be {SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES} bytes when present",
            "segment record vectors must contain only finite f32 values",
            "segment centroids must contain only finite f32 values",
            "segment radii must contain only finite f32 values",
            "segment routing codes must contain only finite f32 values",
            "segment PQ codes must match vector dimensions",
            "segment graph distances must contain only finite f32 values",
            "record ids must not be empty",
            "duplicate record id",
            "manifest dimensions must be greater than zero",
            "manifest segment_max_vectors must be greater than zero",
            "manifest routing_page_fanout must be greater than one",
            "Field::new(\"routing_page_fanout\", DataType::UInt64, false)",
            "manifest_routing_page_fanout",
            "legacy_manifest_without_routing_page_fanout_uses_default",
            "{table} manifest_version {actual} does not match manifest version {expected}",
            "pivot ids must not be empty",
            "duplicate pivot id",
            "routing segment ids must not be empty",
            "duplicate routing segment id",
            "routing segment paths must not be empty",
            "duplicate routing segment path",
            "routing graph paths must not be empty",
            "duplicate routing graph path",
            "routing segment object_count must be greater than zero",
            "routing segment checksum",
            "must be {BLAKE3_HEX_CHECKSUM_LEN} lowercase hex characters",
            "routing segment size_bytes must be greater than zero",
            "routing graph checksum",
            "routing graph size_bytes must be greater than zero",
            "routing leaf_mode",
            "routing segment `{segment_id}` declares {actual} dimensions",
            "{field} `{id}` has {actual} dimensions, expected {expected}",
            "routing code count {routing_code_count} must match record count {record_count}",
            "pq code count {pq_code_count} must match record count {record_count}",
            "manifest_from_parquet_rejects_segment_dimension_mismatch",
            "manifest_from_parquet_rejects_routing_manifest_version_mismatch",
            "manifest_from_parquet_rejects_invalid_config_dimensions",
            "manifest_from_parquet_rejects_invalid_segment_max_vectors",
            "manifest_to_parquet_rejects_invalid_config_dimensions",
            "manifest_to_parquet_rejects_invalid_segment_max_vectors",
            "pivots_to_parquet_rejects_non_finite_vectors",
            "pivots_to_parquet_rejects_vectors_with_wrong_dimensions",
            "pivots_to_parquet_rejects_empty_pivot_ids",
            "pivots_to_parquet_rejects_duplicate_pivot_ids",
            "routing_to_parquet_rejects_non_finite_centroids",
            "routing_to_parquet_rejects_non_finite_radii",
            "routing_to_parquet_rejects_centroids_with_wrong_dimensions",
            "routing_to_parquet_rejects_segment_dimension_mismatch",
            "routing_to_parquet_rejects_malformed_id_bloom",
            "routing_to_parquet_rejects_malformed_vector_signature_bloom",
            "routing_to_parquet_round_trips_leaf_mode",
            "routing_to_parquet_round_trips_vector_signature_bloom",
            "routing_layer_page_to_parquet",
            "routing_layer_page_index_to_parquet",
            "routing_layer_page_index_from_parquet",
            "routing_layer_page_from_parquet",
            "routing_layer_page_schema",
            "routing_layer_page_index_schema",
            "validate_routing_layer_page_refs",
            "routing_page_ref_centroid",
            "routing_page_ref_radius",
            "routing_page_ref_id_bloom",
            "page_ordinal",
            "segment_ordinal",
            "segment_to_parquet_round_trips_pq_codes",
            "routing_to_parquet_rejects_empty_segment_ids",
            "routing_to_parquet_rejects_duplicate_segment_ids",
            "routing_to_parquet_rejects_empty_segment_paths",
            "routing_to_parquet_rejects_duplicate_segment_paths",
            "routing_to_parquet_rejects_empty_graph_paths",
            "routing_to_parquet_rejects_duplicate_graph_paths",
            "routing_to_parquet_rejects_malformed_segment_checksums",
            "routing_to_parquet_rejects_malformed_graph_checksums",
            "routing_to_parquet_rejects_empty_segment_summaries",
            "routing_to_parquet_rejects_zero_segment_sizes",
            "routing_to_parquet_rejects_zero_graph_sizes",
            "segment_to_parquet_rejects_non_finite_record_vectors",
            "segment_to_parquet_rejects_non_finite_centroids",
            "segment_to_parquet_rejects_non_finite_radii",
            "segment_to_parquet_rejects_non_finite_routing_codes",
            "segment_to_parquet_rejects_centroids_with_wrong_dimensions",
            "segment_to_parquet_rejects_record_vectors_with_wrong_dimensions",
            "segment_to_parquet_rejects_empty_record_ids",
            "segment_to_parquet_rejects_duplicate_record_ids",
            "segment_to_parquet_rejects_routing_code_count_mismatch",
            "graph_to_parquet_rejects_non_finite_edge_distances",
            "graph_to_parquet_writes_numeric_record_indices",
            "source_record_index",
            "neighbor_record_index",
            "legacy graph table",
            "pivots_from_parquet_rejects_non_finite_vectors",
            "pivots_from_parquet_rejects_empty_pivot_ids",
            "pivots_from_parquet_rejects_duplicate_pivot_ids",
            "routing_from_parquet_rejects_non_finite_centroids",
            "routing_from_parquet_rejects_non_finite_radii",
            "routing_from_parquet_rejects_centroids_with_wrong_dimensions",
            "routing_from_parquet_rejects_malformed_id_bloom",
            "routing_from_parquet_rejects_malformed_vector_signature_bloom",
            "routing_from_parquet_rejects_unknown_leaf_mode",
            "routing_from_parquet_rejects_empty_segment_ids",
            "routing_from_parquet_rejects_duplicate_segment_ids",
            "routing_from_parquet_rejects_empty_segment_paths",
            "routing_from_parquet_rejects_duplicate_segment_paths",
            "routing_from_parquet_rejects_empty_graph_paths",
            "routing_from_parquet_rejects_duplicate_graph_paths",
            "routing_from_parquet_rejects_malformed_segment_checksums",
            "routing_from_parquet_rejects_malformed_graph_checksums",
            "routing_from_parquet_rejects_empty_segment_summaries",
            "routing_from_parquet_rejects_zero_segment_sizes",
            "routing_from_parquet_rejects_zero_graph_sizes",
            "segment_from_parquet_rejects_non_finite_record_vectors",
            "segment_from_parquet_rejects_non_finite_centroids",
            "segment_from_parquet_rejects_non_finite_radii",
            "segment_from_parquet_rejects_non_finite_routing_codes",
            "segment_from_parquet_rejects_centroids_with_wrong_dimensions",
            "segment_from_parquet_rejects_record_vectors_with_wrong_dimensions",
            "segment_from_parquet_rejects_empty_record_ids",
            "segment_from_parquet_rejects_duplicate_record_ids",
            "segment_from_parquet_fills_legacy_missing_pq_codes",
            "graph_from_parquet_rejects_non_finite_edge_distances",
            "pub(crate) fn manifest_has_next_generated_id",
            "FixedSizeList",
            "BinaryArray",
            '"id_bloom"',
            '"leaf_mode"',
            '"vector_signature_bloom"',
            '"pq_code"',
            "SEGMENT_ID_BLOOM_BYTES",
            "SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES",
        ],
        "crates/borsuk/tests/format.rs": [
            "vector_records_to_parquet_rejects_non_finite_vectors",
            "vector_records_from_parquet_rejects_non_finite_vectors",
            "vector_records_to_parquet_rejects_empty_or_duplicate_ids",
            "vector_records_from_parquet_rejects_empty_or_duplicate_ids",
            "f32::INFINITY",
            "finite f32 values",
            "record ids must not be empty",
            "duplicate record id",
        ],
        "crates/borsuk/tests/package_metadata.rs": [
            "crate_metadata_declares_public_project_urls",
            "repository = \"https://github.com/CausalityHQ/borsuk\"",
            "homepage = \"http://causality.pl/borsuk/\"",
            "license-file = \"../../LICENSE\"",
        ],
        "crates/borsuk/src/lib.rs": [
            "pub use format::{vector_records_from_parquet, vector_records_to_parquet};",
        ],
        "crates/borsuk-cli/src/main.rs": [
            "CliInputFormat",
            "PqScan",
            "VamanaPq",
            "Hybrid",
            "input_format",
            "paged_routing",
            "resident_routing: !paged_routing",
            "vector_records_from_parquet",
            "eq_ignore_ascii_case(\"parquet\")",
            "index.try_stats()",
            "Commands::Rebuild",
            "delete_obsolete",
            "Commands::Gc",
            "GarbageCollectionOptions { dry_run: !delete }",
        ],
        "crates/borsuk-cli/tests/cli.rs": [
            "cli_add_accepts_parquet_vector_records",
            "cli_stats_can_use_paged_routing_without_resident_segment_summaries",
            "cli_search_accepts_pq_scan_leaf_mode",
            "cli_search_accepts_vamana_pq_leaf_mode",
            "cli_search_accepts_hybrid_leaf_mode",
            "cli_search_obeys_approx_byte_budget",
            'report["termination_reason"]',
            "cli_rebuild_compacts_and_deletes_obsolete_objects_when_requested",
            '"rebuild"',
            '"--delete-obsolete"',
            'report["compaction"]["segments_read"]',
            'report["garbage_collection"]["dry_run"]',
            'report["garbage_collection"]["objects_deleted"]',
            '"gc"',
            '"--delete"',
            "vector_records_to_parquet",
            "records.parquet",
        ],
        "crates/borsuk/examples/local_index.rs": [
            "LeafMode::Graph",
            "LeafMode::VamanaPq",
            "LeafMode::Hybrid",
            "LeafMode::PqScan",
            "LeafMode::SqScan",
            "SearchOptions::approx",
            "with_max_candidates_per_segment",
            "search_ids",
            "search_vectors",
            "get_vector",
            "resident_bytes_estimate",
            "recall_at_k",
            "tie_aware_recall_at_k",
        ],
        "crates/borsuk/src/record.rs": [
            "SqScan",
            "sq-scan",
            "PqScan",
            "pq-scan",
            "VamanaPq",
            "vamana-pq",
            "Hybrid",
            "hybrid",
            "pub fn approx",
            "pub fn with_eps",
            "pub fn with_max_segments",
            "pub fn with_max_bytes",
            "pub fn with_max_latency_ms",
            "pub fn with_routing_page_overfetch",
            "pub fn with_max_candidates_per_segment",
            "pub routing_max_level: u8",
            "pub routing_page_fanout: usize",
            "pub routing_leaf_pages: usize",
            "pub routing_pages: usize",
            "The default keeps compaction scoped to a bounded source-leaf batch",
        ],
        "crates/borsuk/tests/metrics.rs": [
            "vector_metrics_reject_non_finite_vector_coordinates",
            "minkowski_distance_rejects_non_finite_or_too_small_power",
            "f32::INFINITY",
            "minkowski:inf",
            "lp:NaN",
        ],
        "crates/borsuk/src/index.rs": [
            "pub fn add_vectors",
            "pub fn add_vectors_with_ids",
            "vector_locality_key",
            "fn kd_order_records",
            "fn sort_records_by_vector_locality",
            "finite f32 values",
            "k must be greater than zero",
            "eps must be finite and non-negative when set",
            "next_generated_id_after_explicit_records",
            "fn advance_generated_id",
            "might_contain_record_id",
            "validate_record_ids_against_existing_segments",
            "record ids must not be empty",
            "validate_object_size",
            "object size mismatch for",
            "validate_segment_object_count",
            "segment object_count mismatch for",
            "validate_segment_metadata",
            "segment metadata {field} mismatch for",
            "validate_graph_record_references",
            "graph edge references missing segment record",
            "validate_graph_edge_distance",
            "graph edge distance mismatch",
            "validate_graph_edge_not_self_referential",
            "graph edge self-reference",
            "validate_graph_edge_not_duplicate",
            "duplicate graph edge",
            "validate_graph_source_out_degree",
            "graph source out-degree exceeds local limit",
            "validate_graph_has_edges_for_multi_record_segment",
            "graph table must contain at least one edge",
            "routing_summaries_for_search",
            "routing_segments_total",
            "routing_layer_page_refs_for_search",
            "get_vector_from_routing_pages",
            "active_segment_object_paths",
            "active_segment_summaries",
            "try_stats",
            "routing_page_overfetch must be greater than zero when set",
            "fn routing_page_overfetch",
            "stats_totals",
            "routing_max_level: self.manifest.routing_max_level",
            "pub fn create_with_routing_page_fanout",
            "pub fn create_with_cache_and_routing_page_fanout",
            "routing_page_fanout: self.manifest.routing_page_fanout",
            "routing_leaf_pages: routing_leaf_page_count(",
            "routing_pages: routing_page_tree_content_page_count(",
            "add_records_to_top_routing_page_refs",
            "validate_record_ids_against_routing_pages",
            "routing_summaries_from_page_refs(&page_refs)",
            ".div_ceil(self.manifest.routing_page_fanout)",
            "SearchTerminationReason",
            "search_stop_reason_before_segment",
            "compact_overflow_does_not_read_unrelated_parent_routing_branches",
            "compact_max_segments_does_not_read_unneeded_source_parent_branches",
            "compact_stops_parent_branch_reads_once_source_batch_is_covered",
            "compact_updates_sparse_top_l0_page_refs_by_ordinal",
            "l0_page_routing_uses_leaf_segment_counts_for_sparse_pages",
            "l0_page_routing_overfetch_is_search_option",
            "selected_leaf_segments",
            "leaf_page_occupied_ranges_from_cached_tree",
            "upsert_leaf_page_ref_by_ordinal",
            "routing_top_page_refs_with_leaf_updates",
        ],
        "crates/borsuk/src/manifest.rs": [
            "next_generated_id",
            "SEGMENT_ID_BLOOM_BYTES",
            "SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES",
            "DEFAULT_ROUTING_PAGE_FANOUT",
            "routing_page_fanout",
            "new_with_routing_page_fanout",
            "routing_layer_page_file_name",
            "routing_layer_page_index_file_name",
            "routing_layer_page_content_file_name",
            "RoutingLayerPageRef",
            "centroid: Vec<f32>",
            "radius: f32",
            "id_bloom: Vec<u8>",
            "level_mask: u64",
            "page_records: usize",
            "page_segment_bytes: u64",
            "page_graph_bytes: u64",
            "impl RoutingLayerPageRef",
            "might_contain_level",
            "pub(crate) fn segment_id_bloom",
            "pub(crate) fn segment_vector_signature_bloom",
            "might_contain_vector_signature",
            "f32::NEG_INFINITY",
        ],
        "crates/borsuk/src/storage.rs": [
            "windows_drive_paths_are_local_paths_not_uri_schemes",
            "looks_like_windows_drive_path",
            "LocalFileSystem::new_with_prefix",
            "Url::from_directory_path",
            "read_current_metadata_table",
            "read_bytes_with_cache_status_and_checksum",
            "current_table_checksum(&read.bytes)",
            "blake3::hash(&read.bytes)",
            "self.delete_cache_file(relative)",
            "derive_legacy_next_generated_id_from_segments",
            "publish_manifest_reusing_routing_pages",
            "routing_layer_page_refs",
            "write_routing_layer_page_indexes",
            "parent_routing_layer_page_refs",
            ".chunks(manifest.routing_page_fanout)",
            "checked_mul(manifest.routing_page_fanout)",
            "checked_mul(previous.routing_page_fanout)",
            "write_parent_routing_layer_page",
            "routing_page_refs_centroid",
            "routing_page_refs_radius",
            "routing_layer_page_centroid",
            "routing_layer_page_radius",
            "routing_layer_page_id_bloom",
            "routing_layer_page_level_mask",
            "routing_layer_page_record_count",
            "routing_layer_page_segment_bytes",
            "routing_layer_page_graph_bytes",
            "publish_manifest_with_routing_page_refs",
            "write_routing_layer_page",
            "read_routing_layer_page_index",
            "routing_layer_page_unchanged",
            "routing_layer_page_index_to_parquet",
            "routing_layer_page_to_parquet",
        ],
        "crates/borsuk/tests/local_index.rs": [
            "generated_vector_add_does_not_scan_existing_segment_payloads",
            "legacy_manifest_without_generated_id_counter_skips_existing_numeric_ids",
            "rewrite_current_manifest_without_next_generated_id",
            "current_rejects_pivot_table_manifest_version_mismatch",
            "search_rejects_segment_object_size_mismatch",
            "search_rejects_segment_object_count_mismatch",
            "search_rejects_segment_metadata_id_mismatch",
            "graph_search_rejects_graph_object_size_mismatch",
            "graph_search_rejects_graph_edges_for_missing_segment_records",
            "graph_search_rejects_graph_edge_distance_mismatch",
            "graph_search_rejects_self_referential_graph_edges",
            "graph_search_rejects_duplicate_graph_edges",
            "graph_search_rejects_graph_source_out_degree_above_local_limit",
            "graph_search_rejects_empty_graph_for_multi_record_segment",
            "approximate_hybrid_leaf_mode_uses_stored_segment_leaf_mode",
            "approximate_hybrid_dispatches_mixed_l0_graph_and_l1_vamana_pq_leaves",
            "publish_writes_parent_routing_layer_indexes",
            "routing_page_fanout_is_configurable_and_persisted",
            "approximate_routing_prefers_segments_with_matching_vector_signatures",
            "compact_packs_vector_local_records_for_budgeted_high_recall_search",
            "compact_reads_only_selected_source_leaf_payloads",
            "compact_from_empty_routing_table_reads_only_selected_source_leaf_payloads",
            "compact_from_empty_routing_table_skips_unrelated_routing_pages",
            "compact_stops_leaf_page_reads_once_source_batch_is_covered",
            "compact_rejects_zero_target_segment_max_vectors_before_reading_routing_pages",
            "corrupt routing page index that validation must not read",
            "corrupt graph that non-resident compaction must not read",
            "corrupt sibling source-level routing leaf page",
            "compaction.graph_payloads_read",
            "approximate_search_reports_segments_skipped_by_routing_page_pruning",
            "stats_use_routing_page_index_when_full_routing_table_is_empty",
            "try_stats_rejects_corrupt_routing_page_index_when_full_routing_table_is_empty",
            "stats_expose_computed_routing_max_level",
            "open_can_use_paged_routing_without_resident_segment_summaries",
            "open_with_cache_refetches_current_metadata_when_cache_is_stale",
            "read_through_cache_refetches_corrupt_segment_and_graph_payloads",
            "add_after_empty_routing_table_preserves_existing_routing_pages",
            "generated_id_add_after_empty_routing_table_does_not_read_unrelated_parent_pages",
            "generated_id_add_after_empty_routing_table_reuses_rightmost_append_parent",
            "add_after_empty_routing_table_rejects_duplicate_ids_through_routing_pages",
            "compact_reuses_unaffected_routing_layer_page_objects",
            "approximate_search_reads_persisted_routing_layer_pages",
            "approximate_search_skips_unrelated_routing_leaf_pages",
            "approximate_search_opens_with_empty_full_routing_table_when_pages_exist",
            "get_vector_uses_routing_pages_when_full_routing_table_is_empty",
            "gc_preserves_active_objects_when_full_routing_table_is_empty",
            "rewrite_current_with_empty_routing_table",
            "routing layer page indexes must be persisted as parquet",
            "level_mask",
            "page_records",
            "page_segment_bytes",
            "page_graph_bytes",
            "corrupt unrelated routing leaf page",
            "routing layer pages must be persisted as parquet",
            "compaction must reuse the untouched routing page object",
            "routing_level",
            "page_ordinal",
            "segment_ordinal",
            "search_rejects_zero_k",
            "exact_search_with_inner_product_does_not_use_centroid_lower_bound",
            "get_vector_rejects_empty_record_ids",
            "get_vector_skips_segments_that_cannot_contain_the_id",
            "explicit_id_add_skips_segments_that_cannot_contain_the_ids",
            "compact_rewrites_l0_segments_into_l1_without_mutating_old_segments",
            "segment.leaf_mode == LeafMode::Graph",
            "segment.leaf_mode == LeafMode::VamanaPq",
            "corrupt segment that must not be read",
            "corrupt unrelated segment that must be skipped",
            "corrupt unrelated segment that duplicate validation must skip",
            "add_vectors",
            "local_index_rejects_non_finite_vectors_and_queries",
            "local_index_rejects_empty_record_ids",
            "f32::NEG_INFINITY",
            "f32::NAN",
            "record ids must not be empty",
            "k must be greater than zero",
            "eps must be finite and non-negative when set",
        ],
        "python/tests/test_api.py": [
            "BORSUK_S3_TEST_URI",
            "Path(path).as_uri()",
            "local_uri(tmp)",
            "test_create_rejects_conflicting_segment_size_aliases",
            "segment_size and segment_max_vectors disagree",
            "test_runtime_annotations_include_minkowski_metric",
            "get_type_hints(borsuk.create)",
            "test_runtime_config_type_aliases_are_exported",
            "self.assertIn(borsuk.VectorMetricName, get_args(borsuk.VectorMetric))",
            "test_vector_distance_runtime_annotations_accept_sequences",
            "Sequence[float]",
            "test_open_has_runtime_annotations",
            "test_open_can_use_paged_routing_without_resident_segment_summaries",
            "test_stats_propagates_corrupt_paged_routing_metadata",
            "resident_routing=False",
            "get_type_hints(borsuk.open)",
            "test_metric_helper_functions_have_runtime_annotations",
            "get_type_hints(borsuk.leaf_mode_names)",
            "get_type_hints(borsuk.recall_at_k)",
            "get_type_hints(borsuk.tie_aware_recall_at_k)",
            "test_result_classes_have_runtime_annotations",
            "get_type_hints(borsuk.SearchReport)",
            'self.assertEqual(report_hints["termination_reason"], borsuk.SearchTerminationReason)',
            'self.assertEqual(stats_hints["routing_max_level"], int)',
            'self.assertEqual(stats_hints["routing_page_fanout"], int)',
            'self.assertEqual(stats_hints["routing_leaf_pages"], int)',
            'self.assertEqual(stats_hints["routing_pages"], int)',
            'self.assertEqual(search_with_report_hints["routing_page_overfetch"], int | None)',
            "test_stats_expose_computed_routing_max_level",
            "test_create_supports_routing_page_fanout",
            'self.assertEqual(report.termination_reason, "max-bytes")',
            'self.assertEqual(hit_hints["id_bytes"], bytes)',
            "test_index_core_methods_have_runtime_annotations",
            "get_type_hints(borsuk.Index.add)",
            "test_index_batch_report_buffer_and_admin_methods_have_runtime_annotations",
            "get_type_hints(borsuk.Index.search_ids_batch)",
            "get_type_hints(borsuk.Index.search_with_report)",
            "get_type_hints(borsuk.Index.rebuild)",
            "get_type_hints(borsuk.Index.gc_obsolete_segments)",
            "test_add_accepts_vectors_with_optional_ids",
            "test_add_rejects_duplicate_ids_and_generated_ids_skip_collisions",
            "search_ids",
            "search_vectors",
            "search_ids_buffer",
            "search_vectors_buffer",
            "search_ids_batch",
            "search_vectors_batch",
            "search_ids_batch_buffer",
            "search_vectors_batch_buffer",
            "get_vector",
            "record ids must not be empty",
            "test_search_rejects_zero_k",
            "k must be greater than zero",
            "eps must be finite and non-negative when set",
            "routing_page_overfetch must be greater than zero when set",
            "test_local_package_search_reports_stay_subsecond",
            "test_rebuild_compacts_all_matching_segments_and_deletes_obsolete_objects",
            "delete_obsolete=True",
            "test_gc_obsolete_segments_dry_runs_and_deletes",
            "gc_obsolete_segments(dry_run=False)",
            "test_gc_obsolete_segments_removes_cached_inactive_objects",
        ],
        "python/tests/test_package.py": [
            "wheel must include native PyO3 extension",
            "Requires-Python: >=3.12",
            "Classifier: Programming Language :: Python :: 3.12",
            "Classifier: Programming Language :: Python :: 3.13",
            "Classifier: Programming Language :: Python :: 3.14",
            "test_wheel_installs_and_imports_from_clean_virtual_environment",
            "test_wheel_metadata_declares_public_project_urls",
        ],
        "python/tests/typing_usage.py": [
            "CanonicalLeafMode",
            "CanonicalVectorMetric",
            "VectorMetricName.COSINE",
            "SearchMode.APPROX",
            "LeafModeName.PQ_SCAN",
            "LeafModeName.VAMANA_PQ",
            "LeafModeName.HYBRID",
            "minkowski_metric(3)",
            "typed_index_methods",
            "int_ids: list[RecordId]",
            "index.get_vector(300)",
            "search_vectors",
            "search_ids_buffer",
            "search_vectors_batch_buffer",
            "routing_page_overfetch=2",
            "report_leaf_mode: CanonicalLeafMode",
            "stats_metric: CanonicalVectorMetric | MinkowskiMetric",
            "get_vector",
        ],
        "packages/borsuk/test/api.test.ts": [
            "BORSUK_S3_TEST_URI",
            "OpenOptions",
            "function localUri",
            "pathToFileURL(path).href",
            "readonlyVector",
            "readonlyIds",
            "residentRouting: false",
            "open can use paged routing without resident segment summaries",
            "stats propagates corrupt paged routing metadata",
            "index methods accept readonly vector and id inputs",
            "const vectors = [[0, 0], [1, 0], [0, 1]] as const",
            "const batch = [[0.9, 0], [0, 0.9]] as const",
            "create rejects conflicting segment size aliases",
            "segment_size and segment_max_vectors disagree",
            "add accepts vectors with optional ids",
            'index.add([[8, 0]], ["direct"])',
            'index.addBuffer(new Float32Array([7, 0]), ["buffer-direct"])',
            "add rejects duplicate ids and generated ids skip collisions",
            "binary ids can be added, searched, and loaded without UTF-8 decoding",
            "integer ids use compact binary encoding",
            "statsMetric: CanonicalVectorMetricName | MinkowskiMetricName",
            "searchIds",
            "searchVectors",
            "searchIdsBuffer",
            "searchVectorsBuffer",
            "searchIdsBatch",
            "searchVectorsBatch",
            "searchIdsBatchBuffer",
            "searchVectorsBatchBuffer",
            "getVector",
            "record ids must not be empty",
            "search rejects zero k",
            "k must be greater than zero",
            "eps must be finite and non-negative when set",
            "routingPageOverfetch",
            "routing_page_overfetch must be greater than zero when set",
            "local package search reports stay subsecond",
            "rebuild compacts all matching segments and deletes obsolete objects",
            "deleteObsolete: true",
            "gcObsoleteSegments dry-runs and deletes inactive segments",
            "gcObsoleteSegments({ dryRun: false })",
            "gcObsoleteSegments removes cached inactive objects",
            "stats expose computed routing max level",
            "routingMaxLevel",
        ],
        "packages/borsuk/test/package.test.ts": [
            "package must include at least one platform native addon",
            "paths.includes(\"index.cjs\")",
            "paths.includes(\"dist/src/index.d.ts\")",
            "published package excludes raw native bridge declarations",
            "!paths.includes(\"native.d.ts\")",
            "published declarations hide native bridge constructor details",
            "constructor\\(uri: string\\);",
            "constructor\\(uri: string, inner\\?: NativeIndex\\);",
            "published package declares supported Node runtime range",
            ">=22 <27",
            "packed package installs and imports from a clean project",
            "published package license contains BUSL revenue grant",
            "published package metadata declares public project urls",
            "packageJson.license, \"BUSL-1.1\"",
            "packageLock.packages?.[\"\"]?.license, \"BUSL-1.1\"",
        ],
        "packages/borsuk/tsconfig.json": [
            '"stripInternal": true',
        ],
        "python/examples/local_index.py": [
            "Path(root).as_uri()",
            "LeafModeName.GRAPH",
            "LeafModeName.VAMANA_PQ",
            "LeafModeName.HYBRID",
            "LeafModeName.PQ_SCAN",
            "LeafModeName.SQ_SCAN",
            "search_ids",
            "search_vectors",
            "search_ids_buffer",
            "search_ids_batch",
            "search_ids_batch_buffer",
            "get_vector",
            "tie_aware_recall_at_k",
        ],
        "packages/borsuk/examples/local-index.ts": [
            "pathToFileURL(root).href",
            "LeafModeName.Graph",
            "LeafModeName.VamanaPq",
            "LeafModeName.Hybrid",
            "LeafModeName.PqScan",
            "LeafModeName.SqScan",
            "searchIds",
            "searchVectors",
            "searchIdsBuffer",
            "searchIdsBatch",
            "searchIdsBatchBuffer",
            "getVector",
            "tieAwareRecallAtK",
        ],
        "crates/borsuk/tests/s3_compatible.rs": [
            "assert_s3_compatible_binary_layout",
            "CURRENT must be a fixed binary pointer",
            "segment-summary routing tables must be Parquet objects",
            "JSON or ad-hoc manifest files must not be durable S3-compatible storage",
        ],
        "packages/borsuk/src/index.ts": [
            "export enum VectorMetricName",
            "export enum LeafModeName",
            "SqScan",
            "PqScan",
            "VamanaPq",
            "Hybrid",
            "export enum SearchMode",
            "export type VectorMetric",
            "export type LeafMode",
            "export type SearchTerminationReason",
            "terminationReason: SearchTerminationReason",
            "metric: CanonicalVectorMetricName | MinkowskiMetricName",
            "routingMaxLevel: number",
            "routingPageFanout: number",
            "routingLeafPages: number",
            "routingPages: number",
            "routingPageFanout?: number",
            "export interface OpenOptions",
            "residentRouting?: boolean",
            "segmentSize?: number",
            "segmentMaxVectors?: number",
            "export interface RebuildOptions",
            "deleteObsolete?: boolean",
            "export interface GarbageCollectionOptions",
            "dryRun?: boolean",
            "rebuild(options: RebuildOptions = {})",
            "gcObsoleteSegments(",
            "export type VectorInput = readonly number[]",
            "export type VectorBatchInput = readonly VectorInput[]",
            "export type RecordId = string | Uint8Array | number | bigint",
            "idBytes: Uint8Array",
            "export type IdsInput = readonly RecordId[]",
            "export function tieAwareRecallAtK",
            "ids?: readonly TId[]",
            "add(vectors: VectorBatchInput, ids: readonly string[])",
            "add(vectors: VectorBatchInput, ids: readonly Uint8Array[])",
            "add(vectors: VectorBatchInput, ids: readonly number[])",
            "add(vectors: VectorBatchInput, ids: readonly bigint[])",
            "addBuffer(vectors: Float32Array, ids: readonly string[])",
            "addBuffer(vectors: Float32Array, ids: readonly Uint8Array[])",
            "addBuffer(vectors: Float32Array, ids: readonly number[])",
            "addBuffer(vectors: Float32Array, ids: readonly bigint[])",
            "/** @internal */",
            "constructor(uri: string, inner: NativeIndex)",
            "nativeVectors",
            "nativeStringIds",
            "nativeIdBytes",
            "integerIdBytes",
            "native search hit did not include idBytes",
            "function addIds",
            "addIdBytes",
            "searchIdBytes",
            "searchIds",
            "searchVectors",
            "searchIdsBuffer",
            "searchVectorsBuffer",
            "searchIdsBatch",
            "searchVectorsBatch",
            "searchIdsBatchBuffer",
            "searchVectorsBatchBuffer",
            "getVector",
            "getVectorById",
            "readonly string[]",
            "readonly Uint8Array[]",
            "readonly number[]",
            "readonly bigint[]",
            "export function minkowskiMetric",
        ],
        "crates/borsuk-node/src/lib.rs": [
            "fn resolve_segment_max_vectors",
            "segment_size and segment_max_vectors disagree",
            "resident_routing",
            ".try_stats()",
            "routing_page_fanout: Option<u32>",
            "routing_page_overfetch: Option<u32>",
            "DEFAULT_ROUTING_PAGE_FANOUT",
            "routing_max_level",
            "routing_page_fanout",
            "routing_leaf_pages",
            "routing_pages",
        ],
        "crates/borsuk-python/src/lib.rs": [
            "fn resolve_segment_max_vectors",
            "segment_size and segment_max_vectors disagree",
            "resident_routing",
            ".try_stats()",
            "routing_page_fanout: Option<usize>",
            "routing_page_overfetch: Option<usize>",
            "DEFAULT_ROUTING_PAGE_FANOUT",
            "routing_max_level",
            "routing_page_fanout",
            "routing_leaf_pages",
            "routing_pages",
        ],
        "python/src/borsuk/__init__.py": [
            "MinkowskiMetric = NewType",
            "Float32Buffer = Buffer",
            "CanonicalVectorMetric: TypeAlias = Literal",
            "VectorMetricAlias: TypeAlias = Literal",
            "VectorMetric: TypeAlias",
            "SearchModeName: TypeAlias = Literal",
            "LeafMode: TypeAlias",
            "Hit.__annotations__",
            '"id_bytes": bytes',
            "IndexStats.__annotations__",
            '"routing_max_level": int',
            '"routing_page_fanout": int',
            '"routing_leaf_pages": int',
            '"routing_pages": int',
            "SearchReport.__annotations__",
            "CompactionReport.__annotations__",
            "GarbageCollectionReport.__annotations__",
            "RecordId: TypeAlias = str | bytes | int",
            '"MinkowskiMetric"',
            "def minkowski_metric",
            "metric: VectorMetric",
            "routing_page_fanout: int | None = None",
            "routing_page_overfetch: int | None = None",
            "def open(",
            "resident_routing: bool = True",
            "def leaf_mode_names() -> list[CanonicalLeafMode]",
            "def recall_at_k(exact_ids: Sequence[str], actual_ids: Sequence[str], k: int) -> float",
            "def tie_aware_recall_at_k(",
            "exact_distances: Sequence[float]",
            "actual_distances: Sequence[float]",
            "left: Sequence[float]",
            "right: Sequence[float]",
            "def vector_metric_names() -> list[CanonicalVectorMetric]",
            "def _annotated_index_add",
            "def _annotated_index_add_buffer",
            "def _integer_id_bytes",
            "def _annotated_index_search_ids",
            "def _annotated_index_search_vectors",
            "def _annotated_index_search_ids_batch",
            "def _annotated_index_search_with_report",
            "def _annotated_index_compact",
            "Index.add = _annotated_index_add",
            "Index.add_buffer = _annotated_index_add_buffer",
            "segment_size: int | None = None",
        ],
        "python/src/borsuk/__init__.pyi": [
            "from collections.abc import Buffer",
            "MinkowskiMetric = NewType",
            "RecordId: TypeAlias = str | bytes | int",
            "def minkowski_metric",
            "segment_size: int | None = None",
            "routing_page_fanout: int | None = None",
            "routing_page_overfetch: int | None = None",
            "routing_max_level: int",
            "routing_page_fanout: int",
            "routing_leaf_pages: int",
            "routing_pages: int",
            "ids: Sequence[RecordId] | None = None",
            "def search_ids",
            "def search_vectors",
            "def search_ids_buffer",
            "def search_vectors_buffer",
            "def search_ids_batch",
            "def search_vectors_batch",
            "def search_ids_batch_buffer",
            "def search_vectors_batch_buffer",
            "def get_vector",
            "resident_routing: bool = True",
        ],
        "python/tests/test_examples.py": [
            "s3_index.py",
            "BORSUK_S3_TEST_URI",
        ],
        "packages/borsuk/test/examples.test.ts": [
            "s3-index.js",
            "BORSUK_S3_TEST_URI",
        ],
        "README.md": [
            "![CI]",
            "![Pages]",
            "![License]",
            "![Python]",
            "![Node]",
            "residentRouting: false",
            "## Why BORSUK Exists",
            "## ELI5 Intuition",
            "BORSUK is not promising magic perfect recall from a tiny budget",
            "reorganizing boxes after a delivery rush",
            "computed routing depth/page counts",
            "routing_page_fanout",
            "routing_page_overfetch",
            "routingPageOverfetch",
            "with_routing_page_overfetch",
            "## Architecture",
            "## Python Quick Start",
            "## TypeScript Quick Start",
            "## Full Documentation",
            "## Benchmarks And Performance Evidence",
            "interactive architecture and performance",
            "dataset-size scale",
            "10k/100k synthetic",
            "1,000,000 vectors",
            "1.000000 tie-aware recall@10",
            "tie_aware_recall_at_k",
            "stale or corrupt metadata cache entries",
            "corrupt local copies are",
            "docs/production-readiness.md",
            "```mermaid",
            "```math",
            "lb(q, s) = max",
            "max_candidates_per_segment",
            "without a second id lookup per",
            "crates/borsuk/examples/s3_index.rs",
            "python/examples/s3_index.py",
            "packages/borsuk/examples/s3-index.ts",
            "Python 3.12, 3.13, and 3.14",
            "Node 22, 24, and 26",
            "Linux x64, Linux arm64, Windows x64, macOS arm64, and macOS Intel",
            "add_vectors_with_ids",
            "SearchOptions::approx",
            "with_max_candidates_per_segment",
            "cargo package --locked -p borsuk --allow-dirty",
            "uvx maturin build --locked --out dist",
            'uv run --with "./$wheel" python -m unittest discover python/tests',
        ],
        "python/README.md": [
            "Supported Python versions are 3.12, 3.13, and 3.14",
            "Linux x64, Linux arm64,",
            "Windows x64, macOS arm64, and macOS Intel",
            "uvx maturin develop --locked",
            "stale or corrupt cached active metadata tables",
            "repaired from backing storage",
            "Record ids must be unique",
            "resident_routing=False",
            "pq-scan",
        ],
        "packages/borsuk/README.md": [
            "Supported Node versions are 22, 24, and 26",
            "Linux x64, Linux arm64, Windows",
            "x64, macOS arm64, and macOS Intel",
            "node >=22 <27",
            'index.add([[0, 0], [1, 0]], ["a", "b"])',
            'index.addBuffer(new Float32Array([2, 0, 3, 0]), ["c", "d"])',
            "stale or corrupt cached active",
            "repaired from backing storage",
            "Record ids must be unique",
            "residentRouting: false",
            "pq-scan",
            "tieAwareRecallAtK",
        ],
        "docs/api.md": [
            "SearchOptions::approx",
            "resident_routing",
            "residentRouting",
            "--paged-routing",
            "sq-scan",
            "pq-scan",
            "vamana-pq",
            "hybrid",
            "Production-scale indexes should use compact arbitrary binary ids",
            "vector-local leaves",
            "routing pages above bounded leaf blobs",
            "content-addressed routing",
            "writes only dirty",
            "source leaves from the active routing page Parquet metadata",
            "page-index `level_mask`",
            "routing page index aggregate columns",
            "IndexStats.routing_max_level",
            "create_with_routing_page_fanout",
            "`routing_page_fanout` controls the routing tree shape",
            "SearchOptions::with_routing_page_overfetch",
            "routingPageOverfetch",
            "Do not model production-scale search as one flat map",
            "routing_page_fanout",
            "routing_page_overfetch",
            "routing_leaf_pages",
            "routing_pages",
            "does not decode sibling L0 routing pages after the requested source batch is full",
            "target_segment_max_vectors",
            "preflight validation before routing pages",
            "allocates new L0 leaf ordinals after",
            "decode only the readable rightmost",
            "vector-bound lower bound with centroid/radius as the compatibility fallback",
            "`max_segments: None`",
            "explicit offline rebuild-style operation",
            "page-level id bloom",
            "full `routing/segments-*.parquet` summary table is empty",
            "stale or corrupt metadata cache files are refetched automatically",
            "Cached segment, graph, and routing page objects",
            "routing page Parquet metadata before deleting anything",
            "reads only the selected source leaf payloads",
            "with_max_candidates_per_segment",
            "tie_aware_recall_at_k",
            "tieAwareRecallAtK",
            "BorsukIndex::search_with_report",
            "BorsukIndex::search_ids_batch",
            "BorsukIndex::search_vectors_batch",
            "They do not perform a second",
            "Record ids must be unique",
            "BorsukIndex::add_vectors",
            "BorsukIndex::add_vectors_with_ids",
            "const explicitIds = await index.add(vectors, ids)",
            "const bufferIds = await index.addBuffer(new Float32Array(flatVectors), ids)",
            "records.parquet",
            "--input-format json",
        ],
        "docs/architecture.md": [
            "fixed-size id and vector-signature bloom filters",
            "vector_signature_bloom",
            "`leaf_mode` field",
            "get_vector(id)",
            "duplicate-id validation",
            "pq_code",
            "graph entry selection",
            "PQ-seeded graph expansion",
            "multi-level binary routing tree",
            "vector-local leaves",
            "dense internal numeric row ids",
            "assigns new L0 leaf ordinals after",
            "decode only the readable rightmost",
            "not the final billion-vector routing design",
            "Scoped compaction reads only selected source leaf payloads",
            "stale or corrupt metadata cache files are deleted and refetched",
            "corrupt cached immutable object is deleted and refetched",
            "```mermaid",
            "lb(q, s) = max",
            "L1+ segments declare `vamana-pq`",
        ],
        "docs/storage-format.md": [
            "id_bloom",
            "vector_signature_bloom",
            "leaf_mode",
            "pq_code",
            "arbitrary binary bytes",
            "source_record_index",
            "neighbor_record_index",
            "source_record_id",
            "neighbor_record_id",
            "routing/layers/<version>/L1",
            "routing/pages/L0",
            "content-addressed",
            "page centroid/radius",
            "full segment-summary routing",
            "The local read-through cache is not an authority for active metadata",
            "Segment payloads, graph payloads, and routing page payloads",
            "graph blocks as derived data",
            "negative filter for id lookups",
            "lower-bound-only approximate routing",
            "compacted L1+ segments",
            "declare `vamana-pq`",
            "write only the new append branch",
            "fill it before adding another parent branch",
            "explicit duplicate-id validation",
            "Older manifest tables without `next_generated_id`",
        ],
        "docs/web/index.html": [
            "Rust local example",
            "cargo run --locked -p borsuk --example local_index",
            "Rust S3 example",
            "Python native API",
            "TypeScript native API",
            "S3-compatible examples",
            "Business Source License 1.1",
            "US $100,000/year",
            "https://github.com/CausalityHQ/borsuk/blob/main/crates/borsuk/examples/local_index.rs",
            "https://github.com/CausalityHQ/borsuk/blob/main/crates/borsuk/examples/s3_index.rs",
            "https://github.com/CausalityHQ/borsuk/blob/main/python/examples/s3_index.py",
            "https://github.com/CausalityHQ/borsuk/blob/main/packages/borsuk/examples/s3-index.ts",
            'href="docs.html"',
            "pq-scan",
            "vamana-pq",
            "hybrid",
            "interactive mode comparison charts",
            "dataset-size scale charts",
            "parallel memory-pressure charts",
            "million-vector release gate charts",
        ],
        "docs/web/docs.html": [
            "Documentation",
            "ELI5 intuition",
            "BORSUK is not a magic always-perfect shortcut",
            "The report tells you when a query stopped because of a budget",
            "Decisions",
            "Architecture",
            "Functionality",
            "Testing and performance",
            "data-performance-root",
            "data-scale-root",
            "data-large-scale-root",
            "data-parallel-root",
            "data-stage",
            "data-code-tabs",
            "refetch stale active metadata cache files",
            "repair corrupt cached segment, graph, or routing page payloads",
            "cache hit/miss counters",
            "storage-map",
            "formula",
            "lb(q, s) = max(0, d(q, c_s) - r_s)",
            "Full markdown docs",
            "index.add_vectors",
            "index.add_vectors_with_ids",
            "search_ids",
            "search_vectors",
            "get_vector",
            "flat-scan",
            "sq-scan",
            "pq-scan",
            "graph",
            "vamana-pq",
            "hybrid",
            "Python 3.12",
            "Node 22, 24, and 26",
            "CURRENT is a fixed binary pointer",
            "Parquet-backed binary data",
            "docs/architecture.md",
            "docs/api.md",
            "docs/storage-format.md",
            "docs/benchmarks.md",
            "docs/production-readiness.md",
            "Business Source License 1.1",
            "US $100,000/year",
        ],
        "docs/production-readiness.md": [
            "not production-ready",
            "production-ready",
            "cargo clippy --locked --workspace --all-targets -- -D warnings",
            "cargo test --locked --workspace --all-targets",
            "cargo test --locked --release -p borsuk --test large_scale",
            "million_vector_local_search_scale_gate",
            "BORSUK_LARGE_SCALE_OUTPUT",
            "large-scale.csv",
            "python -m unittest discover python/tests",
            "npm test",
            "benchmark_report",
            "synthetic uniform, clustered, and adversarial",
            "rss_peak_delta",
            "cache hits/misses",
            "tie-aware recall",
            "id recall",
            "termination reasons",
            "resident_bytes_estimate",
            "ram_budget",
            "invalidates stale cached",
            "cached segment, graph, and routing page payloads",
            "max_candidates_per_segment",
            "vector-local compaction",
            "scoped compaction",
            "scoped compaction from routing page metadata",
            "page-level `level_mask` metadata",
            "routing page index aggregates",
            "append after non-resident open",
            "avoid unrelated parent routing pages",
            "reuse the rightmost append parent",
            "routing-page pruning",
            "reuses unchanged routing page objects",
            "persisted vector bounds with centroid/radius fallback",
            "page-level id blooms",
            "resident segment-summary vector empty",
            "GC protection of active segment/graph objects through",
            "computed multi-level routing pages",
            "top-down parent-to-leaf page-walk search",
            "compact arbitrary ids",
            "SeaweedFS",
            "## Evidence Map",
            "Candidate evidence",
            "Checked-in artifact",
            "Fresh command evidence",
            "Release decision",
        ],
        "docs/benchmarks.md": [
            "benchmark_report",
            "million_vector_local_search_scale_gate",
            "tie-aware recall@10",
            "strict id recall@10",
            "termination-reason counts",
            "object-cache hits/misses",
            "BORSUK_LARGE_SCALE_OUTPUT",
            "large-scale.csv",
            "vector-local L1 leaves",
            "ingested in 36.5s",
            "compacted in 63.4s",
            "ran the exact recall reference in 1.23s",
            "synthetic-uniform",
            "synthetic-clustered",
            "synthetic-adversarial",
            "sklearn-digits",
            "Parallel Graph Pressure",
            "scale.csv",
            "dataset-size scale sweeps",
        ],
        "crates/borsuk/examples/benchmark_report.rs": [
            "rss_peak_delta",
            "tie_aware_recall_at_k",
            "id_recall_at_10",
            "termination_reasons",
            "format_termination_reasons",
            "avg_cache_hits",
            "avg_cache_misses",
            "write_scale_csv",
            "scale_family_name",
            "compact_for_query_benchmark",
            "records,dimensions,segment_max_vectors",
            "parallelism",
            "SyntheticDataset::Adversarial",
            "LeafMode::VamanaPq",
            "LeafMode::Hybrid",
            "memory_stats",
        ],
        "crates/borsuk/tests/large_scale.rs": [
            "DEFAULT_RECORDS: usize = 1_000_000",
            "million_vector_local_search_scale_gate",
            "BORSUK_LARGE_SCALE_RECORDS",
            "BORSUK_LARGE_SCALE_OUTPUT",
            "large_scale_csv",
            "LargeScaleRunSummary",
            "(LeafMode::PqScan, false)",
            "(LeafMode::VamanaPq, true)",
            "(LeafMode::Hybrid, true)",
            "SearchOptions::approx(10, leaf_mode)",
            "termination_reason",
        ],
        "docs/web/app.js": [
            "assets/benchmarks/sequential.csv",
            "assets/benchmarks/parallel.csv",
            "assets/benchmarks/scale.csv",
            "assets/benchmarks/large-scale.csv",
            'loadCsv("assets/benchmarks/large-scale.csv")',
            "tie_aware_recall_at_10",
            "id_recall_at_10",
            "setupSequentialChart",
            "setupScaleChart",
            "setupLargeScaleChart",
            "termination_reasons",
            "termination_reason",
            "Termination",
            "avg_resident_bytes",
            "avg_routing_page_indexes_read",
            "avg_routing_pages_read",
            "Resident bytes",
            "avg_cache_hits",
            "avg_cache_misses",
            "Cache hits",
            "Cache misses",
            "LARGE_SCALE_METRICS",
            "SCALE_METRICS",
            "renderRecordScaleLine",
            "setupParallelChart",
            "initCodeTabs",
            "ARCH_STAGES",
        ],
        "scripts/test_docs_web.mjs": [
            "assertTableIncludes",
            "/Termination/",
            "/exact-pruned=10|max-segments=10/",
            "/max-segments/",
            "/Resident bytes/",
            "/resident metadata/",
            "/Routing indexes/",
            "/Routing pages/",
            "/Cache hits/",
            "/Cache misses/",
            "/cache misses\\/query/",
        ],
        "docs/web/assets/benchmarks/scale.csv": [
            "family,dataset,mode,records,dimensions,segment_max_vectors,max_segments,max_candidates_per_segment,queries,tie_aware_recall_at_10,id_recall_at_10,termination_reasons,p50_ms,p95_ms,avg_bytes_read,avg_graph_bytes_read,avg_routing_page_indexes_read,avg_routing_pages_read,avg_resident_bytes,avg_segments,avg_records_considered,avg_records_scored,avg_cache_hits,avg_cache_misses",
            "synthetic-uniform,synthetic-uniform-n10000,pq-scan,10000,64,256,8,64",
            "synthetic-uniform,synthetic-uniform-n100000,pq-scan,100000,64,256,8,64",
            "synthetic-clustered,synthetic-clustered-n100000,vamana-pq,100000,64,256,8,64",
            "synthetic-adversarial,synthetic-adversarial-n100000,hybrid,100000,64,256,8,64",
            "sklearn-digits,sklearn-digits,pq-scan,1797,64,256,8,64",
        ],
        "docs/web/assets/benchmarks/sequential.csv": [
            "dataset,mode,records,dimensions,segment_max_vectors,max_segments,max_candidates_per_segment,queries,tie_aware_recall_at_10,id_recall_at_10,termination_reasons,p50_ms,p95_ms,avg_bytes_read,avg_graph_bytes_read,avg_routing_page_indexes_read,avg_routing_pages_read,avg_resident_bytes,avg_segments,avg_records_considered,avg_records_scored,avg_cache_hits,avg_cache_misses",
            "synthetic-uniform-n10000,vamana-pq,10000,64,256,8,64",
            "sklearn-digits,pq-scan,1797,64,256,8,64",
        ],
        "docs/web/assets/benchmarks/parallel.csv": [
            "dataset,mode,records,dimensions,segment_max_vectors,max_segments,max_candidates_per_segment,parallelism,queries,tie_aware_recall_at_10,id_recall_at_10,termination_reasons,p50_ms,p95_ms,qps,avg_bytes_read,avg_graph_bytes_read,avg_routing_page_indexes_read,avg_routing_pages_read,avg_resident_bytes,avg_cache_hits,avg_cache_misses,rss_before,rss_peak,rss_after,rss_peak_delta",
            "synthetic-uniform-n10000,vamana-pq,10000,64,256,8,64,8",
            "sklearn-digits,graph,1797,64,256,8,64,8",
        ],
        "docs/web/assets/benchmarks/large-scale.csv": [
            "records,dimensions,segment_max_vectors,max_segments,max_candidates_per_segment,pre_segments,post_segments,ingest_ms,compaction_ms,exact_ms,compaction_bytes_read,compaction_bytes_written,mode,tie_aware_recall_at_10,termination_reason,query_ms,segments_searched,bytes_read,graph_bytes_read,routing_page_indexes_read,routing_pages_read,resident_bytes,records_considered,records_scored,graph_candidates_added",
            "1000000,16,128,512,128",
            ",pq-scan,",
            ",vamana-pq,",
            ",hybrid,",
        ],
    }
    for path, commands in locked_cargo_commands.items():
        for command in commands:
            assert_contains(path, command, "locked Cargo dependency resolution")

    scale_required_rows = [
        {"dataset": f"{family}-n{records}", "mode": mode, "records": str(records)}
        for family in [
            "synthetic-uniform",
            "synthetic-clustered",
            "synthetic-adversarial",
        ]
        for records in [10_000, 100_000]
        for mode in ["pq-scan", "vamana-pq", "hybrid"]
    ]
    scale_required_rows.extend(
        {"dataset": "sklearn-digits", "mode": mode, "records": "1797"}
        for mode in ["pq-scan", "vamana-pq", "hybrid"]
    )
    assert_benchmark_recall_rows(
        "docs/web/assets/benchmarks/scale.csv",
        (ROOT / "docs/web/assets/benchmarks/scale.csv").read_text(),
        scale_required_rows,
    )
    assert_benchmark_recall_rows(
        "docs/web/assets/benchmarks/large-scale.csv",
        (ROOT / "docs/web/assets/benchmarks/large-scale.csv").read_text(),
        [
            {"records": "1000000", "mode": "pq-scan"},
            {"records": "1000000", "mode": "vamana-pq"},
            {"records": "1000000", "mode": "hybrid"},
        ],
    )
    parallel_pressure_rows = [
        {
            "dataset": dataset,
            "mode": mode,
            "records": records,
            "parallelism": str(parallelism),
        }
        for dataset, records in [
            ("synthetic-uniform-n10000", "10000"),
            ("synthetic-clustered-n10000", "10000"),
            ("synthetic-adversarial-n10000", "10000"),
            ("sklearn-digits", "1797"),
        ]
        for mode in ["graph", "vamana-pq", "hybrid"]
        for parallelism in [1, 2, 4, 8]
    ]
    assert_benchmark_numeric_rows(
        "docs/web/assets/benchmarks/parallel.csv",
        (ROOT / "docs/web/assets/benchmarks/parallel.csv").read_text(),
        parallel_pressure_rows,
        {
            "avg_graph_bytes_read": 1.0,
            "avg_routing_page_indexes_read": 1.0,
            "avg_routing_pages_read": 1.0,
            "avg_resident_bytes": 1.0,
            "avg_cache_misses": 1.0,
            "p95_ms": 0.000001,
            "qps": 0.000001,
            "rss_peak_delta": 1.0,
        },
    )
    lifecycle_rows = [
        {"dataset": dataset, "records": "10000"}
        for dataset in [
            "synthetic-uniform-n10000",
            "synthetic-clustered-n10000",
            "synthetic-adversarial-n10000",
        ]
    ]
    assert_benchmark_numeric_rows(
        "docs/web/assets/benchmarks/lifecycle.csv",
        (ROOT / "docs/web/assets/benchmarks/lifecycle.csv").read_text(),
        lifecycle_rows,
        {
            "ingest_ms": 0.000001,
            "ingest_vectors_per_sec": 0.000001,
            "compaction_ms": 0.000001,
            "compaction_vectors_per_sec": 0.000001,
            "records_rewritten": 1.0,
            "routing_page_indexes_read": 1.0,
            "routing_pages_read": 1.0,
            "routing_page_indexes_written": 1.0,
            "routing_pages_written": 1.0,
            "compaction_bytes_read": 1.0,
            "compaction_bytes_written": 1.0,
        },
    )
    large_scale_rows = [
        {"records": "1000000", "mode": mode}
        for mode in ["pq-scan", "vamana-pq", "hybrid"]
    ]
    assert_benchmark_numeric_rows(
        "docs/web/assets/benchmarks/large-scale.csv",
        (ROOT / "docs/web/assets/benchmarks/large-scale.csv").read_text(),
        large_scale_rows,
        {
            "ingest_ms": 0.000001,
            "compaction_ms": 0.000001,
            "exact_ms": 0.000001,
            "compaction_bytes_read": 1.0,
            "compaction_bytes_written": 1.0,
            "query_ms": 0.000001,
            "segments_searched": 1.0,
            "bytes_read": 1.0,
            "routing_page_indexes_read": 1.0,
            "routing_pages_read": 1.0,
            "resident_bytes": 1.0,
            "records_considered": 1.0,
            "records_scored": 1.0,
        },
    )
    assert_benchmark_numeric_rows(
        "docs/web/assets/benchmarks/large-scale.csv",
        (ROOT / "docs/web/assets/benchmarks/large-scale.csv").read_text(),
        [
            {"records": "1000000", "mode": "vamana-pq"},
            {"records": "1000000", "mode": "hybrid"},
        ],
        {
            "graph_bytes_read": 1.0,
            "graph_candidates_added": 1.0,
        },
    )

    github_rich_markdown_paths = [
        "README.md",
        "docs/architecture.md",
    ]
    for path in github_rich_markdown_paths:
        assert_not_contains(
            path,
            r"\operatorname",
            "GitHub math renderer rejects the operatorname macro",
        )
        assert_not_contains(
            path,
            "  route --> graph[",
            "Mermaid treats graph as a reserved token; use a non-reserved node id",
        )
        assert_not_contains(
            path,
            "  graph -->",
            "Mermaid treats graph as a reserved token; use a non-reserved node id",
        )

    loose_python_buffer_stub_terms = [
        "vectors: Any",
        "query: Any",
        "queries: Any",
    ]
    for term in loose_python_buffer_stub_terms:
        assert_not_contains(
            "python/src/borsuk/__init__.pyi",
            term,
            "Python 3.12+ buffer APIs should be typed with collections.abc.Buffer, not Any",
        )

    deprecated_runtime_matrix_terms = [
        '"3.10"',
        '"3.11"',
        "node-version: \"20\"",
    ]
    for term in deprecated_runtime_matrix_terms:
        assert_not_contains(
            ".github/workflows/publish.yml",
            term,
            "published Python/Node package support matrix must start at Python 3.12 and maintained Node lines",
        )

    platform_matrix_requirements = {
        ".github/workflows/ci.yml": [
            "ubuntu-latest",
            "ubuntu-24.04-arm",
            "macos-26",
            "macos-15-intel",
            "windows-latest",
            'python-version: ["3.12", "3.13", "3.14"]',
            'node-version: ["22", "24", "26"]',
        ],
        ".github/workflows/publish.yml": [
            "ubuntu-latest",
            "ubuntu-24.04-arm",
            "macos-26",
            "macos-15-intel",
            "windows-latest",
            "borsuk-*manylinux*x86_64.whl",
            "borsuk-*manylinux*aarch64.whl",
            "index.linux-x64-gnu.node",
            "index.linux-arm64-gnu.node",
            "index.darwin-arm64.node",
            "index.darwin-x64.node",
            "index.win32-x64-msvc.node",
        ],
    }
    for path, requirements in platform_matrix_requirements.items():
        for requirement in requirements:
            assert_contains(
                path,
                requirement,
                "package CI/publish matrix must cover Linux x64+arm64, Windows x64, macOS arm64+Intel, Python 3.12+, and maintained Node lines",
            )

    assert_not_contains(
        "python/README.md",
        "maturin develop --manifest-path ../crates/borsuk-python/Cargo.toml",
        "Python development installs must use pyproject.toml so borsuk._borsuk is the native module",
    )

    assert_not_contains(
        "crates/borsuk/src/index.rs",
        "self.get_vector_by_id(hit.id.as_bytes())?",
        "search_vectors/search_vectors_batch must reuse vectors loaded during search instead of doing a second id lookup read",
    )

    removed_string_api_terms = [
        "stringDistance",
        "StringMetric",
        "StringMetricName",
        "string_metric_names",
        "string_distance",
        "stringMetricNames",
    ]
    removed_string_api_paths = [
        "README.md",
        "docs/api.md",
        "docs/web/index.html",
        "docs/web/docs.html",
        "python/README.md",
        "python/src/borsuk/__init__.py",
        "python/src/borsuk/__init__.pyi",
        "python/tests/test_api.py",
        "packages/borsuk/README.md",
        "packages/borsuk/src/index.ts",
        "packages/borsuk/test/api.test.ts",
    ]
    for path in removed_string_api_paths:
        for term in removed_string_api_terms:
            assert_not_contains(
                path,
                term,
                "vector-only public API surface after string API removal",
            )

    removed_generic_search_terms_by_path = {
        "README.md": [".search("],
        "docs/api.md": ["BorsukIndex::search(query", ".search("],
        "python/README.md": [".search("],
        "python/src/borsuk/__init__.pyi": ["def search("],
        "crates/borsuk-python/src/lib.rs": ["fn search("],
        "packages/borsuk/README.md": [".search("],
        "packages/borsuk/src/index.ts": ["async search(", "search(query: number[]"],
        "crates/borsuk-node/src/lib.rs": ["pub fn search("],
    }
    for path, terms in removed_generic_search_terms_by_path.items():
        for term in terms:
            assert_not_contains(
                path,
                term,
                "public API should expose id/vector searches plus get_vector, not a generic search method",
            )

    removed_hit_search_terms_by_path = {
        "docs/api.md": [
            "search_buffer(",
            "search_batch(",
            "search_batch_buffer(",
            "searchBuffer(",
            "searchBatch(",
            "searchBatchBuffer(",
            "BorsukIndex::search_batch(",
        ],
        "python/README.md": [
            "search_buffer(",
            "search_batch(",
            "search_batch_buffer(",
        ],
        "python/examples/local_index.py": [
            "search_buffer(",
            "search_batch(",
            "search_batch_buffer(",
        ],
        "python/src/borsuk/__init__.pyi": [
            "def search_buffer(",
            "def search_batch(",
            "def search_batch_buffer(",
        ],
        "crates/borsuk-python/src/lib.rs": [
            "fn search_buffer(",
            "fn search_batch(",
            "fn search_batch_buffer(",
        ],
        "packages/borsuk/README.md": [
            "searchBuffer(",
            "searchBatch(",
            "searchBatchBuffer(",
        ],
        "packages/borsuk/examples/local-index.ts": [
            "searchBuffer(",
            "searchBatch(",
            "searchBatchBuffer(",
        ],
        "packages/borsuk/src/index.ts": [
            "async searchBuffer(",
            "async searchBatch(",
            "async searchBatchBuffer(",
            "searchBuffer(query:",
            "searchBatch(queries:",
            "searchBatchBuffer(queries:",
        ],
        "crates/borsuk-node/src/lib.rs": [
            "pub fn search_buffer(",
            "pub fn search_batch(",
            "pub fn search_batch_buffer(",
        ],
    }
    for path, terms in removed_hit_search_terms_by_path.items():
        for term in terms:
            assert_not_contains(
                path,
                term,
                "normal search APIs should return ids or vectors; hit objects belong to report APIs",
            )

    public_payload_ref_terms = [
        "payload_refs",
        "payloadRefs",
        "payload_ref",
        "payloadRef",
        "with_payload_ref",
    ]
    public_payload_ref_paths = [
        "README.md",
        "docs/api.md",
        "docs/web/index.html",
        "docs/web/docs.html",
        "python/README.md",
        "python/src/borsuk/__init__.py",
        "python/src/borsuk/__init__.pyi",
        "python/tests/test_api.py",
        "python/tests/typing_usage.py",
        "python/examples/local_index.py",
        "python/examples/s3_index.py",
        "packages/borsuk/README.md",
        "packages/borsuk/src/index.ts",
        "packages/borsuk/test/api.test.ts",
        "packages/borsuk/examples/local-index.ts",
        "packages/borsuk/examples/s3-index.ts",
        "crates/borsuk/examples/local_index.rs",
        "crates/borsuk/examples/s3_index.rs",
        "crates/borsuk/tests/local_index.rs",
    ]
    for path in public_payload_ref_paths:
        for term in public_payload_ref_terms:
            assert_not_contains(
                path,
                term,
                "simple id/vector public API without external payload references",
            )

    python_result_type_requirements = {
        "python/src/borsuk/__init__.pyi": [
            "metric: CanonicalVectorMetric | MinkowskiMetric",
            "leaf_mode: CanonicalLeafMode",
        ],
    }
    for path, requirements in python_result_type_requirements.items():
        for requirement in requirements:
            assert_contains(path, requirement, "typed Python result metadata")

    forbidden_docs_tree_url = "https://github.com/riomus/borsuk/tree/main/" + "docs"
    assert_not_contains(
        "docs/web/index.html",
        forbidden_docs_tree_url,
        "public website documentation link should resolve inside the deployed Pages site",
    )
    assert_not_contains(
        "docs/web/docs.html",
        forbidden_docs_tree_url,
        "public website documentation link should resolve inside the deployed Pages site",
    )
    assert_not_contains(
        "docs/web/index.html",
        "https://github.com/riomus/borsuk",
        "public website GitHub links must target the actual public repository",
    )
    assert_contains(
        "docs/web/index.html",
        "https://github.com/CausalityHQ/borsuk",
        "public website GitHub links must target the actual public repository",
    )

    design_payload_ref_terms = [
        "payload references",
        "payload refs",
        "payload pointers",
        "ids/payload refs",
        "Object payload shards",
        "exact payloads",
        "Vector/payload columns",
    ]
    for path in ["docs/api.md", "docs/architecture.md", "docs/storage-format.md"]:
        for term in design_payload_ref_terms:
            assert_not_contains(
                path,
                term,
                "design documents must model records as ids plus vectors, not external payload references",
            )

    storage_payload_ref_terms = [
        "payload output",
        "payload shards",
        "payload rows",
        "payload/object rows",
        "vector/sketch/payload rows",
    ]
    for term in storage_payload_ref_terms:
        assert_not_contains(
            "docs/storage-format.md",
            term,
            "storage docs must describe ids plus vectors, not optional external payload storage",
        )

    no_custom_index_suffix_paths = [
        ".gitignore",
        "README.md",
        "docs/api.md",
        "docs/architecture.md",
        "docs/storage-format.md",
        "python/README.md",
        "packages/borsuk/README.md",
        "crates/borsuk/examples/local_index.rs",
        "crates/borsuk/src/storage.rs",
    ]
    for path in no_custom_index_suffix_paths:
        assert_not_contains(
            path,
            ".borsuk",
            "Parquet-only durable storage naming; index roots should not look like custom file formats",
        )

    unsafe_file_uri_terms = [
        'format!("file://{}", dir.path().display())',
        'format!("file://{}", dir.display())',
        'f"file://{tmp}"',
        'f"file://{root}"',
        "`file://${dir}`",
        "`file://${root}`",
    ]
    unsafe_file_uri_paths = [
        "crates/borsuk/examples/local_index.rs",
        "crates/borsuk/tests/local_index.rs",
        "crates/borsuk/tests/performance_smoke.rs",
        "crates/borsuk/benches/local_search.rs",
        "crates/borsuk-cli/tests/cli.rs",
        "python/examples/local_index.py",
        "python/tests/test_api.py",
        "packages/borsuk/examples/local-index.ts",
        "packages/borsuk/test/api.test.ts",
    ]
    for path in unsafe_file_uri_paths:
        for term in unsafe_file_uri_terms:
            assert_not_contains(
                path,
                term,
                "Windows-safe local path and file URL handling",
            )

    assert_no_viewport_font_sizing("docs/web/styles.css")

    benchmark_requirements = [
        "local_exact_search_10k_x_64",
        "local_approx_report_10k_x_64",
        "local_flat_scan_approx_report_10k_x_64",
        "local_sq_scan_approx_report_10k_x_64",
        "local_pq_scan_approx_report_10k_x_64",
        "local_vamana_pq_approx_report_10k_x_64",
        "local_hybrid_approx_report_10k_x_64",
        "local_warm_cache_approx_report_10k_x_64",
        "local_clustered_approx_report_10k_x_64",
        "local_adversarial_approx_report_10k_x_64",
    ]
    for requirement in benchmark_requirements:
        assert_contains(
            "crates/borsuk/benches/local_search.rs",
            requirement,
            "deterministic exact, approximate, and cache performance benchmarks",
        )
        assert_contains(
            "docs/benchmarks.md",
            requirement,
            "documented deterministic exact, approximate, and cache performance benchmarks",
        )
    for requirement in [
        "LeafMode::Graph",
        "LeafMode::VamanaPq",
        "LeafMode::Hybrid",
        "LeafMode::FlatScan",
        "LeafMode::SqScan",
        "LeafMode::PqScan",
    ]:
        assert_contains(
            "crates/borsuk/tests/performance_smoke.rs",
            requirement,
            "performance smoke coverage for every implemented leaf mode",
        )

    publish_workflow_requirements = [
        "pypi-build:",
        "needs: pypi-build",
        "PyO3/maturin-action@v1",
        "maturin-version: v1.11.5",
        'manylinux: "2_28"',
        "args: --locked --release --compatibility pypi --out dist",
        "python -m unittest discover tests",
        "Assert Python wheel coverage",
        "borsuk-*cp312-*.whl",
        "borsuk-*cp313-*.whl",
        "borsuk-*cp314-*.whl",
        "borsuk-*manylinux*x86_64.whl",
        "borsuk-*manylinux*aarch64.whl",
        "borsuk-*macosx*x86_64.whl",
        "borsuk-*macosx*arm64.whl",
        "borsuk-*win_amd64.whl",
        "npm-native:",
        "needs: npm-native",
        "node-native-${{ matrix.os }}",
        "native-artifacts",
        "actions/upload-artifact@v4",
        "actions/download-artifact@v4",
        "merge-multiple: true",
        "Assert native artifact coverage",
        "index.linux-x64-gnu.node",
        "index.linux-arm64-gnu.node",
        "index.darwin-arm64.node",
        "index.darwin-x64.node",
        "index.win32-x64-msvc.node",
        "test -f index.cjs",
        "test -f native.d.ts",
        "npm pack --dry-run --json",
    ]
    for requirement in publish_workflow_requirements:
        assert_contains(
            ".github/workflows/publish.yml",
            requirement,
            "multi-platform publish artifacts",
        )


if __name__ == "__main__":
    main()
