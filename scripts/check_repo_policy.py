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


def assert_contains(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    require(
        needle in text,
        f"{path} must contain `{needle}` for locked Cargo dependency resolution",
    )


def main() -> None:
    require((ROOT / "Cargo.lock").is_file(), "Cargo.lock must exist")
    assert_not_ignored("Cargo.lock")
    assert_tracked("Cargo.lock")

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
            "cargo test --locked -p borsuk --test s3_compatible -- --nocapture",
        ],
        ".github/workflows/publish.yml": [
            "cargo package --locked -p borsuk",
            "cargo publish --locked -p borsuk --token",
        ],
        ".pre-commit-config.yaml": [
            "cargo clippy --locked --workspace --all-targets -- -D warnings",
            "cargo test --locked --workspace --all-targets",
        ],
    }
    for path, commands in locked_cargo_commands.items():
        for command in commands:
            assert_contains(path, command)


if __name__ == "__main__":
    main()
