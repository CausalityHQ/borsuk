#!/usr/bin/env python3
"""Repository hygiene checks that are cheap enough for CI and pre-commit."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


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
            "os: [ubuntu-latest, macos-26, macos-15-intel, windows-latest]",
            'python-version: ["3.12", "3.13", "3.14"]',
            'node-version: ["22", "24", "26"]',
            "cargo clippy --locked --workspace --all-targets -- -D warnings",
            "cargo test --locked --workspace --all-targets",
            "Run Rust local example",
            "cargo run --locked -p borsuk --example local_index",
            "cargo bench --locked --workspace --no-run",
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
            "os: [ubuntu-latest, macos-26, macos-15-intel, windows-latest]",
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
            "routing_to_parquet_round_trips_leaf_mode",
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
            "pivots_from_parquet_rejects_non_finite_vectors",
            "pivots_from_parquet_rejects_empty_pivot_ids",
            "pivots_from_parquet_rejects_duplicate_pivot_ids",
            "routing_from_parquet_rejects_non_finite_centroids",
            "routing_from_parquet_rejects_non_finite_radii",
            "routing_from_parquet_rejects_centroids_with_wrong_dimensions",
            "routing_from_parquet_rejects_malformed_id_bloom",
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
            '"pq_code"',
            "SEGMENT_ID_BLOOM_BYTES",
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
            "vector_records_from_parquet",
            "eq_ignore_ascii_case(\"parquet\")",
        ],
        "crates/borsuk-cli/tests/cli.rs": [
            "cli_add_accepts_parquet_vector_records",
            "cli_search_accepts_pq_scan_leaf_mode",
            "cli_search_accepts_vamana_pq_leaf_mode",
            "cli_search_accepts_hybrid_leaf_mode",
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
            "pub fn with_max_candidates_per_segment",
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
        ],
        "crates/borsuk/src/manifest.rs": [
            "next_generated_id",
            "SEGMENT_ID_BLOOM_BYTES",
            "pub(crate) fn segment_id_bloom",
            "f32::NEG_INFINITY",
        ],
        "crates/borsuk/src/storage.rs": [
            "windows_drive_paths_are_local_paths_not_uri_schemes",
            "looks_like_windows_drive_path",
            "LocalFileSystem::new_with_prefix",
            "Url::from_directory_path",
            "derive_legacy_next_generated_id_from_segments",
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
            "get_type_hints(borsuk.open)",
            "test_metric_helper_functions_have_runtime_annotations",
            "get_type_hints(borsuk.leaf_mode_names)",
            "get_type_hints(borsuk.recall_at_k)",
            "test_result_classes_have_runtime_annotations",
            "get_type_hints(borsuk.SearchReport)",
            "test_index_core_methods_have_runtime_annotations",
            "get_type_hints(borsuk.Index.add)",
            "test_index_batch_report_buffer_and_admin_methods_have_runtime_annotations",
            "get_type_hints(borsuk.Index.search_ids_batch)",
            "get_type_hints(borsuk.Index.search_with_report)",
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
            "test_local_package_search_reports_stay_subsecond",
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
            "search_vectors",
            "search_ids_buffer",
            "search_vectors_batch_buffer",
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
            "index methods accept readonly vector and id inputs",
            "const vectors = [[0, 0], [1, 0], [0, 1]] as const",
            "const batch = [[0.9, 0], [0, 0.9]] as const",
            "create rejects conflicting segment size aliases",
            "segment_size and segment_max_vectors disagree",
            "add accepts vectors with optional ids",
            'index.add([[8, 0]], ["direct"])',
            'index.addBuffer(new Float32Array([7, 0]), ["buffer-direct"])',
            "add rejects duplicate ids and generated ids skip collisions",
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
            "local package search reports stay subsecond",
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
            "metric: CanonicalVectorMetricName | MinkowskiMetricName",
            "export interface OpenOptions",
            "segmentSize?: number",
            "segmentMaxVectors?: number",
            "export type VectorInput = readonly number[]",
            "export type VectorBatchInput = readonly VectorInput[]",
            "export type IdsInput = readonly string[]",
            "ids?: IdsInput",
            "add(vectors: VectorBatchInput, ids: IdsInput)",
            "addBuffer(vectors: Float32Array, ids: IdsInput)",
            "/** @internal */",
            "constructor(uri: string, inner: NativeIndex)",
            "nativeVectors",
            "nativeIds",
            "function addIds",
            "searchIds",
            "searchVectors",
            "searchIdsBuffer",
            "searchVectorsBuffer",
            "searchIdsBatch",
            "searchVectorsBatch",
            "searchIdsBatchBuffer",
            "searchVectorsBatchBuffer",
            "getVector",
            "readonly string[]",
            "readonly number[]",
            "export function minkowskiMetric",
        ],
        "crates/borsuk-node/src/lib.rs": [
            "fn resolve_segment_max_vectors",
            "segment_size and segment_max_vectors disagree",
        ],
        "crates/borsuk-python/src/lib.rs": [
            "fn resolve_segment_max_vectors",
            "segment_size and segment_max_vectors disagree",
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
            "IndexStats.__annotations__",
            "SearchReport.__annotations__",
            "CompactionReport.__annotations__",
            "GarbageCollectionReport.__annotations__",
            '"MinkowskiMetric"',
            "def minkowski_metric",
            "metric: VectorMetric",
            "def open(uri: str, cache_dir: str | None = None, ram_budget: str | None = None) -> Index",
            "def leaf_mode_names() -> list[CanonicalLeafMode]",
            "def recall_at_k(exact_ids: Sequence[str], actual_ids: Sequence[str], k: int) -> float",
            "left: Sequence[float]",
            "right: Sequence[float]",
            "def vector_metric_names() -> list[CanonicalVectorMetric]",
            "def _annotated_index_add",
            "def _annotated_index_add_buffer",
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
            "def minkowski_metric",
            "segment_size: int | None = None",
            "ids: Sequence[str] | None = None",
            "def search_ids",
            "def search_vectors",
            "def search_ids_buffer",
            "def search_vectors_buffer",
            "def search_ids_batch",
            "def search_vectors_batch",
            "def search_ids_batch_buffer",
            "def search_vectors_batch_buffer",
            "def get_vector",
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
            "## Why BORSUK Exists",
            "## Architecture",
            "## Python Quick Start",
            "## TypeScript Quick Start",
            "## Full Documentation",
            "interactive architecture and performance",
            "docs/production-readiness.md",
            "```mermaid",
            "```math",
            "lb(q, s) = max",
            "max_candidates_per_segment",
            "crates/borsuk/examples/s3_index.rs",
            "python/examples/s3_index.py",
            "packages/borsuk/examples/s3-index.ts",
            "Python 3.12, 3.13, and 3.14",
            "Node 22, 24, and 26",
            "macOS arm64, and macOS Intel",
            "add_vectors_with_ids",
            "SearchOptions::approx",
            "with_max_candidates_per_segment",
            "cargo package --locked -p borsuk --allow-dirty",
            "uvx maturin build --locked --out dist",
            'uv run --with "./$wheel" python -m unittest discover python/tests',
        ],
        "python/README.md": [
            "Supported Python versions are 3.12, 3.13, and 3.14",
            "Linux, Windows, macOS arm64, and macOS Intel",
            "uvx maturin develop --locked",
            "Record ids must be unique",
            "pq-scan",
        ],
        "packages/borsuk/README.md": [
            "Supported Node versions are 22, 24, and 26",
            "Linux, Windows, macOS arm64, and",
            "node >=22 <27",
            'index.add([[0, 0], [1, 0]], ["a", "b"])',
            'index.addBuffer(new Float32Array([2, 0, 3, 0]), ["c", "d"])',
            "Record ids must be unique",
            "pq-scan",
        ],
        "docs/api.md": [
            "SearchOptions::approx",
            "sq-scan",
            "pq-scan",
            "vamana-pq",
            "hybrid",
            "with_max_candidates_per_segment",
            "BorsukIndex::search_with_report",
            "BorsukIndex::search_ids_batch",
            "BorsukIndex::search_vectors_batch",
            "Record ids must be unique",
            "BorsukIndex::add_vectors",
            "BorsukIndex::add_vectors_with_ids",
            "const explicitIds = await index.add(vectors, ids)",
            "const bufferIds = await index.addBuffer(new Float32Array(flatVectors), ids)",
            "records.parquet",
            "--input-format json",
        ],
        "docs/architecture.md": [
            "fixed-size id bloom filter",
            "`leaf_mode` field",
            "get_vector(id)",
            "duplicate-id validation",
            "pq_code",
            "graph entry selection",
            "PQ-seeded graph expansion",
            "```mermaid",
            "lb(q, s) = max",
            "L1+ segments declare `vamana-pq`",
        ],
        "docs/storage-format.md": [
            "id_bloom",
            "leaf_mode",
            "pq_code",
            "negative filter for id lookups",
            "compacted L1+ segments",
            "declare `vamana-pq`",
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
            "parallel memory-pressure charts",
        ],
        "docs/web/docs.html": [
            "Documentation",
            "Decisions",
            "Architecture",
            "Functionality",
            "Testing and performance",
            "data-performance-root",
            "data-parallel-root",
            "data-stage",
            "data-code-tabs",
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
            "python -m unittest discover python/tests",
            "npm test",
            "benchmark_report",
            "synthetic uniform, clustered, and adversarial",
            "rss_peak_delta",
            "tie-aware recall",
            "id recall",
            "resident_bytes_estimate",
            "ram_budget",
            "max_candidates_per_segment",
            "SeaweedFS",
        ],
        "docs/benchmarks.md": [
            "benchmark_report",
            "million_vector_local_search_scale_gate",
            "tie-aware recall@10",
            "strict id recall@10",
            "synthetic-uniform",
            "synthetic-clustered",
            "synthetic-adversarial",
            "sklearn-digits",
            "Parallel Graph Pressure",
        ],
        "crates/borsuk/examples/benchmark_report.rs": [
            "rss_peak_delta",
            "tie_aware_recall_at_k",
            "id_recall_at_10",
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
            "SearchOptions::approx(10, LeafMode::PqScan)",
        ],
        "docs/web/app.js": [
            "assets/benchmarks/sequential.csv",
            "assets/benchmarks/parallel.csv",
            "tie_aware_recall_at_10",
            "id_recall_at_10",
            "setupSequentialChart",
            "setupParallelChart",
            "initCodeTabs",
            "ARCH_STAGES",
        ],
        "docs/web/assets/benchmarks/sequential.csv": [
            "dataset,mode,records,dimensions,segment_max_vectors,max_segments,max_candidates_per_segment,queries,tie_aware_recall_at_10,id_recall_at_10",
            "synthetic-uniform,vamana-pq,10000,64,256,8,64",
            "sklearn-digits,pq-scan,1797,64,256,8,64",
        ],
        "docs/web/assets/benchmarks/parallel.csv": [
            "dataset,mode,records,dimensions,segment_max_vectors,max_segments,max_candidates_per_segment,parallelism,queries,tie_aware_recall_at_10,id_recall_at_10",
            "synthetic-uniform,vamana-pq,10000,64,256,8,64,8",
            "sklearn-digits,graph,1797,64,256,8,64,8",
        ],
    }
    for path, commands in locked_cargo_commands.items():
        for command in commands:
            assert_contains(path, command, "locked Cargo dependency resolution")

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

    assert_not_contains(
        "python/README.md",
        "maturin develop --manifest-path ../crates/borsuk-python/Cargo.toml",
        "Python development installs must use pyproject.toml so borsuk._borsuk is the native module",
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
        "index.linux-*.node",
        "index.darwin-arm64.node",
        "index.darwin-x64.node",
        "index.win32-*.node",
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
