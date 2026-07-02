import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path


class PythonPackageTests(unittest.TestCase):
    def wheel_path(self) -> str:
        wheel = os.environ.get("BORSUK_WHEEL_PATH")
        if not wheel:
            self.skipTest("BORSUK_WHEEL_PATH is not set")
        return str(Path(wheel).resolve())

    def wheel_names(self) -> list[str]:
        wheel = self.wheel_path()

        with zipfile.ZipFile(wheel) as archive:
            return archive.namelist()

    def wheel_text(self, suffix: str) -> str:
        wheel = self.wheel_path()

        with zipfile.ZipFile(wheel) as archive:
            matches = [name for name in archive.namelist() if name.endswith(suffix)]
            self.assertTrue(matches, f"wheel does not contain {suffix}: {archive.namelist()}")
            return archive.read(matches[0]).decode("utf-8")

    def wheel_metadata(self) -> str:
        return self.wheel_text(".dist-info/METADATA")

    def test_wheel_excludes_bytecode_cache(self) -> None:
        names = self.wheel_names()

        self.assertFalse(
            any("__pycache__" in name or name.endswith(".pyc") for name in names),
            f"wheel contains bytecode/cache files: {names}",
        )

    def test_wheel_includes_license_files(self) -> None:
        names = self.wheel_names()

        self.assertTrue(
            any(name.endswith("LICENSE") for name in names),
            f"wheel does not contain LICENSE: {names}",
        )

    def test_wheel_includes_native_extension(self) -> None:
        names = self.wheel_names()

        self.assertTrue(
            any(
                name.startswith("borsuk/_borsuk") and name.endswith((".so", ".pyd"))
                for name in names
            ),
            f"wheel must include native PyO3 extension: {names}",
        )

    def test_wheel_license_contains_busl_revenue_grant(self) -> None:
        license_text = self.wheel_text("LICENSE")

        self.assertIn("Business Source License 1.1", license_text)
        self.assertIn("US $100,000", license_text)
        self.assertIn("Change Date: 2030-07-02", license_text)
        self.assertIn("Change License: MIT License", license_text)

    def test_wheel_includes_package_readme(self) -> None:
        names = self.wheel_names()

        self.assertIn("README.md", names)

    def test_wheel_includes_typing_metadata(self) -> None:
        names = self.wheel_names()

        self.assertIn("borsuk/py.typed", names)
        self.assertIn("borsuk/__init__.pyi", names)

    def test_wheel_metadata_declares_python_312_plus(self) -> None:
        metadata = self.wheel_metadata()

        self.assertIn("Requires-Python: >=3.12", metadata)
        self.assertIn("Classifier: Programming Language :: Python :: 3.12", metadata)
        self.assertIn("Classifier: Programming Language :: Python :: 3.13", metadata)
        self.assertIn("Classifier: Programming Language :: Python :: 3.14", metadata)

    def test_wheel_metadata_declares_public_project_urls(self) -> None:
        metadata = self.wheel_metadata()

        self.assertIn("Project-URL: Homepage, http://causality.pl/borsuk/", metadata)
        self.assertIn("Project-URL: Documentation, http://causality.pl/borsuk/", metadata)
        self.assertIn("Project-URL: Repository, https://github.com/CausalityHQ/borsuk", metadata)
        self.assertIn("Project-URL: Issues, https://github.com/CausalityHQ/borsuk/issues", metadata)

    def test_wheel_installs_and_imports_from_clean_virtual_environment(self) -> None:
        wheel = self.wheel_path()
        with tempfile.TemporaryDirectory(prefix="borsuk-wheel-consumer-") as root:
            root_path = Path(root)
            venv_path = root_path / "venv"
            self.run_checked([sys.executable, "-m", "venv", str(venv_path)], cwd=root_path)
            python = venv_path / ("Scripts/python.exe" if os.name == "nt" else "bin/python")

            self.run_checked(
                [
                    str(python),
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    wheel,
                ],
                cwd=root_path,
            )
            smoke = root_path / "smoke.py"
            smoke.write_text(
                "\n".join(
                    [
                        "import borsuk",
                        "if 'cosine' not in borsuk.vector_metric_names():",
                        "    raise SystemExit('missing cosine metric in installed wheel')",
                        "distance = borsuk.vector_distance(",
                        "    borsuk.VectorMetricName.COSINE, [1.0, 0.0], [1.0, 0.0]",
                        ")",
                        "if distance != 0.0:",
                        "    raise SystemExit(f'wrong cosine distance: {distance}')",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            self.run_checked([str(python), str(smoke)], cwd=root_path)

    def run_checked(self, command: list[str], *, cwd: Path) -> None:
        result = subprocess.run(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(
            result.returncode,
            0,
            f"command failed: {command}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
