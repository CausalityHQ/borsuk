#!/usr/bin/env python3
"""Repository hygiene checks that are cheap enough for CI and pre-commit."""

from __future__ import annotations

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


def main() -> None:
    require((ROOT / "Cargo.lock").is_file(), "Cargo.lock must exist")
    assert_not_ignored("Cargo.lock")
    assert_tracked("Cargo.lock")
    assert_tracked("crates/borsuk/examples/s3_index.rs")
    assert_tracked("python/README.md")
    assert_tracked("python/examples/local_index.py")
    assert_tracked("python/examples/s3_index.py")
    assert_tracked("packages/borsuk/examples/local-index.ts")
    assert_tracked("packages/borsuk/examples/s3-index.ts")
    assert_tracked("examples/seaweedfs/run-smoke.sh")
    assert_tracked("python/tests/test_api.py")
    assert_tracked("packages/borsuk/test/api.test.ts")

    ignored_outputs = [
        "target/debug/example",
        "packages/borsuk/dist/src/index.js",
        "packages/borsuk/node_modules/example/package.json",
        "packages/borsuk/index.cjs",
        "packages/borsuk/native.d.ts",
        "packages/borsuk/index.darwin-arm64.node",
        "python/dist/borsuk-0.1.0.whl",
        "python/.venv/pyvenv.cfg",
        "python/src/borsuk/_borsuk.abi3.so",
    ]
    for path in ignored_outputs:
        assert_ignored(path)

    locked_cargo_commands = {
        ".github/workflows/ci.yml": [
            "cargo clippy --locked --workspace --all-targets -- -D warnings",
            "cargo test --locked --workspace --all-targets",
            "Run Rust local example",
            "cargo run --locked -p borsuk --example local_index",
            "cargo bench --locked --workspace --no-run",
            "maturin build --locked --out dist",
            "cargo test --locked -p borsuk --test s3_compatible -- --nocapture",
            "Run Rust S3-compatible example",
            "cargo run --locked -p borsuk --example s3_index",
            "Run Python S3-compatible API tests",
            "Run TypeScript S3-compatible API tests",
            "SeaweedFS S3-Compatible Smoke",
            "./examples/seaweedfs/run-smoke.sh",
        ],
        ".github/workflows/publish.yml": [
            "cargo package --locked -p borsuk",
            "cargo publish --locked -p borsuk --token",
        ],
        "crates/borsuk/Cargo.toml": [
            'readme = "../../README.md"',
            'keywords = ["ann", "similarity-search", "vector-search", "s3", "parquet"]',
            'categories = ["algorithms", "database-implementations", "science"]',
        ],
        ".pre-commit-config.yaml": [
            "cargo clippy --locked --workspace --all-targets -- -D warnings",
            "cargo test --locked --workspace --all-targets",
            "uvx maturin build --locked --out dist",
            'uv run --with "./$wheel" python -m unittest discover tests',
        ],
        "packages/borsuk/package.json": [
            '"example:local": "npm run build && node dist/examples/local-index.js"',
            '"example:s3": "npm run build && node dist/examples/s3-index.js"',
            '"repository":',
            '"homepage": "https://riomus.github.io/borsuk"',
            '"bugs":',
            '"keywords":',
        ],
        "python/pyproject.toml": [
            'readme = "README.md"',
            '{ path = "README.md", format = "wheel" }',
            "[project.urls]",
            'Homepage = "https://riomus.github.io/borsuk"',
            'Repository = "https://github.com/riomus/borsuk"',
            'Documentation = "https://riomus.github.io/borsuk"',
            'Issues = "https://github.com/riomus/borsuk/issues"',
        ],
        "examples/seaweedfs/README.md": [
            "./examples/seaweedfs/run-smoke.sh",
        ],
        "examples/seaweedfs/run-smoke.sh": [
            "cargo run --locked -p borsuk --example s3_index",
            "python -m unittest discover tests",
            "npm test",
        ],
        "python/tests/test_api.py": [
            "BORSUK_S3_TEST_URI",
        ],
        "packages/borsuk/test/api.test.ts": [
            "BORSUK_S3_TEST_URI",
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
            "crates/borsuk/examples/s3_index.rs",
            "python/examples/s3_index.py",
            "packages/borsuk/examples/s3-index.ts",
        ],
        "docs/web/index.html": [
            "Rust local example",
            "cargo run --locked -p borsuk --example local_index",
            "Rust S3 example",
            "Python native API",
            "TypeScript native API",
            "S3-compatible examples",
            "https://github.com/riomus/borsuk/blob/main/crates/borsuk/examples/local_index.rs",
            "https://github.com/riomus/borsuk/blob/main/crates/borsuk/examples/s3_index.rs",
            "https://github.com/riomus/borsuk/blob/main/python/examples/s3_index.py",
            "https://github.com/riomus/borsuk/blob/main/packages/borsuk/examples/s3-index.ts",
        ],
        "design.md": [
            "Blob-Oriented Retrieval with Segmental Unified KNN",
            "Rust/Python/TypeScript low-RAM similarity-search library",
        ],
    }
    for path, commands in locked_cargo_commands.items():
        for command in commands:
            assert_contains(path, command, "locked Cargo dependency resolution")

    conflicting_design_terms = [
        "K-nearest Retrieval on External Tiers",
        "Rust/Python low-RAM similarity-search library",
        "Rust/Python low-RAM similarity search library",
        "Rust/Python low-RAM external-tier design",
    ]
    for term in conflicting_design_terms:
        assert_not_contains(
            "design.md",
            term,
            "canonical BORSUK expansion and supported language surfaces",
        )

    benchmark_requirements = [
        "local_exact_search_10k_x_64",
        "local_approx_report_10k_x_64",
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

    publish_workflow_requirements = [
        "pypi-build:",
        "needs: pypi-build",
        "PyO3/maturin-action@v1",
        "maturin-version: v1.11.5",
        'manylinux: "2_28"',
        "args: --locked --release --compatibility pypi --out dist",
        "python -m unittest discover tests",
        "npm-native:",
        "needs: npm-native",
        "node-native-${{ matrix.os }}",
        "native-artifacts",
        "actions/upload-artifact@v4",
        "actions/download-artifact@v4",
        "merge-multiple: true",
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
