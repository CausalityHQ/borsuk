import json
import tempfile
import unittest
from pathlib import Path

import check_release_versions


class CheckReleaseVersionsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self._write_release("0.1.0")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_release(self, version: str) -> None:
        cargo_packages = {
            "crates/borsuk/Cargo.toml": ("borsuk", version),
            "crates/borsuk-cli/Cargo.toml": ("borsuk-cli", version),
            "crates/borsuk-node/Cargo.toml": ("borsuk-node", version),
            "crates/borsuk-python/Cargo.toml": ("borsuk-python", version),
        }
        for relative, (name, package_version) in cargo_packages.items():
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                f'[package]\nname = "{name}"\nversion = "{package_version}"\n',
                encoding="utf-8",
            )

        python_manifest = self.root / "python/pyproject.toml"
        python_manifest.parent.mkdir(parents=True, exist_ok=True)
        python_manifest.write_text(
            f'[project]\nname = "borsuk"\nversion = "{version}"\n',
            encoding="utf-8",
        )

        node_manifest = self.root / "packages/borsuk/package.json"
        node_manifest.parent.mkdir(parents=True, exist_ok=True)
        node_manifest.write_text(
            json.dumps({"name": "borsuk", "version": version}),
            encoding="utf-8",
        )
        node_lock = self.root / "packages/borsuk/package-lock.json"
        node_lock.write_text(
            json.dumps(
                {
                    "name": "borsuk",
                    "version": version,
                    "packages": {"": {"name": "borsuk", "version": version}},
                }
            ),
            encoding="utf-8",
        )

        lock = self.root / "Cargo.lock"
        lock.write_text(
            "\n".join(
                f'[[package]]\nname = "{name}"\nversion = "{version}"\n'
                for name in ("borsuk", "borsuk-cli", "borsuk-node", "borsuk-python")
            ),
            encoding="utf-8",
        )

    def test_matching_release_is_accepted(self) -> None:
        self.assertEqual(
            check_release_versions.validate_release_versions(self.root, "v0.1.0"),
            "0.1.0",
        )

    def test_mismatched_package_is_rejected_with_its_path(self) -> None:
        manifest = self.root / "python/pyproject.toml"
        manifest.write_text(
            '[project]\nname = "borsuk"\nversion = "0.1.1"\n',
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "python/pyproject.toml.*0.1.1"):
            check_release_versions.validate_release_versions(self.root, "v0.1.0")

    def test_mismatched_lockfile_is_rejected(self) -> None:
        lock = self.root / "Cargo.lock"
        lock.write_text(
            lock.read_text(encoding="utf-8").replace(
                'name = "borsuk-node"\nversion = "0.1.0"',
                'name = "borsuk-node"\nversion = "0.0.9"',
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "Cargo.lock.*borsuk-node.*0.0.9"):
            check_release_versions.validate_release_versions(self.root, "0.1.0")

    def test_invalid_release_version_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "release version"):
            check_release_versions.validate_release_versions(self.root, "latest")


if __name__ == "__main__":
    unittest.main()
