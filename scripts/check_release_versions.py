#!/usr/bin/env python3
"""Fail closed unless every publishable BORSUK package has one release version."""

from __future__ import annotations

import argparse
import json
import re
import tomllib
from pathlib import Path

RELEASE_VERSION = re.compile(
    r"^(?:v)?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$"
)
CARGO_PACKAGES = (
    ("crates/borsuk/Cargo.toml", "borsuk"),
    ("crates/borsuk-cli/Cargo.toml", "borsuk-cli"),
    ("crates/borsuk-node/Cargo.toml", "borsuk-node"),
    ("crates/borsuk-python/Cargo.toml", "borsuk-python"),
)


def _toml(path: Path) -> dict:
    with path.open("rb") as source:
        return tomllib.load(source)


def _package_version(path: Path, expected_name: str) -> str:
    package = _toml(path).get("package")
    if not isinstance(package, dict):
        raise ValueError(f"{path}: missing [package]")
    name = package.get("name")
    if name != expected_name:
        raise ValueError(
            f"{path}: package name is {name!r}, expected {expected_name!r}"
        )
    version = package.get("version")
    if not isinstance(version, str):
        raise ValueError(f"{path}: package version is missing")
    return version


def _assert_version(label: str, actual: object, expected: str) -> None:
    if actual != expected:
        raise ValueError(
            f"{label}: version {actual!r} does not match release {expected!r}"
        )


def validate_release_versions(root: Path, requested_version: str) -> str:
    """Validate package and lockfile versions, returning canonical `x.y.z`."""

    match = RELEASE_VERSION.fullmatch(requested_version)
    if match is None:
        raise ValueError(
            f"release version {requested_version!r} must be an exact stable semantic version"
        )
    version = ".".join(match.groups())

    for relative, package_name in CARGO_PACKAGES:
        _assert_version(
            relative,
            _package_version(root / relative, package_name),
            version,
        )

    python_manifest = _toml(root / "python/pyproject.toml")
    python_project = python_manifest.get("project")
    if not isinstance(python_project, dict):
        raise ValueError("python/pyproject.toml: missing [project]")
    _assert_version(
        "python/pyproject.toml",
        python_project.get("version"),
        version,
    )

    node_manifest_path = root / "packages/borsuk/package.json"
    node_manifest = json.loads(node_manifest_path.read_text(encoding="utf-8"))
    _assert_version(
        "packages/borsuk/package.json",
        node_manifest.get("version"),
        version,
    )

    node_lock_path = root / "packages/borsuk/package-lock.json"
    node_lock = json.loads(node_lock_path.read_text(encoding="utf-8"))
    _assert_version(
        "packages/borsuk/package-lock.json top level",
        node_lock.get("version"),
        version,
    )
    lock_root = node_lock.get("packages", {}).get("")
    if not isinstance(lock_root, dict):
        raise ValueError("packages/borsuk/package-lock.json: missing root package")
    _assert_version(
        "packages/borsuk/package-lock.json root package",
        lock_root.get("version"),
        version,
    )

    cargo_lock = _toml(root / "Cargo.lock")
    locked_by_name: dict[str, list[object]] = {}
    for package in cargo_lock.get("package", []):
        if isinstance(package, dict):
            locked_by_name.setdefault(str(package.get("name")), []).append(
                package.get("version")
            )
    for _, package_name in CARGO_PACKAGES:
        locked = locked_by_name.get(package_name, [])
        if locked != [version]:
            raise ValueError(
                f"Cargo.lock package {package_name}: versions {locked!r} "
                f"do not match release {version!r}"
            )

    return version


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("version", help="release version or v-prefixed release tag")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="repository root",
    )
    args = parser.parse_args()
    version = validate_release_versions(args.root.resolve(), args.version)
    print(f"release version {version} is consistent across all package manifests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
