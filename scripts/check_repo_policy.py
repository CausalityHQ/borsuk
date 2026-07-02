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


def main() -> None:
    require((ROOT / "Cargo.lock").is_file(), "Cargo.lock must exist")
    assert_not_ignored("Cargo.lock")
    assert_tracked("Cargo.lock")
    assert_tracked("python/examples/local_index.py")
    assert_tracked("packages/borsuk/examples/local-index.ts")
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
            "cargo bench --locked --workspace --no-run",
            "maturin build --locked --out dist",
            "cargo test --locked -p borsuk --test s3_compatible -- --nocapture",
        ],
        ".github/workflows/publish.yml": [
            "cargo package --locked -p borsuk",
            "cargo publish --locked -p borsuk --token",
        ],
        ".pre-commit-config.yaml": [
            "cargo clippy --locked --workspace --all-targets -- -D warnings",
            "cargo test --locked --workspace --all-targets",
            "maturin build --locked --out dist",
        ],
        "packages/borsuk/package.json": [
            '"example:local": "npm run build && node dist/examples/local-index.js"',
        ],
        "examples/seaweedfs/README.md": [
            "./examples/seaweedfs/run-smoke.sh",
        ],
        "examples/seaweedfs/run-smoke.sh": [
            "python -m unittest discover tests",
            "npm test",
        ],
        "python/tests/test_api.py": [
            "BORSUK_S3_TEST_URI",
        ],
        "packages/borsuk/test/api.test.ts": [
            "BORSUK_S3_TEST_URI",
        ],
    }
    for path, commands in locked_cargo_commands.items():
        for command in commands:
            assert_contains(path, command, "locked Cargo dependency resolution")

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
