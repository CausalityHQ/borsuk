import os
import unittest
import zipfile


class PythonPackageTests(unittest.TestCase):
    def wheel_names(self) -> list[str]:
        wheel = os.environ.get("BORSUK_WHEEL_PATH")
        if not wheel:
            self.skipTest("BORSUK_WHEEL_PATH is not set")

        with zipfile.ZipFile(wheel) as archive:
            return archive.namelist()

    def test_wheel_excludes_bytecode_cache(self) -> None:
        names = self.wheel_names()

        self.assertFalse(
            any("__pycache__" in name or name.endswith(".pyc") for name in names),
            f"wheel contains bytecode/cache files: {names}",
        )

    def test_wheel_includes_license_files(self) -> None:
        names = self.wheel_names()

        self.assertTrue(
            any(name.endswith("LICENSE-MIT") for name in names),
            f"wheel does not contain LICENSE-MIT: {names}",
        )
        self.assertTrue(
            any(name.endswith("LICENSE-APACHE") for name in names),
            f"wheel does not contain LICENSE-APACHE: {names}",
        )

    def test_wheel_includes_package_readme(self) -> None:
        names = self.wheel_names()

        self.assertIn("README.md", names)

    def test_wheel_includes_typing_metadata(self) -> None:
        names = self.wheel_names()

        self.assertIn("borsuk/py.typed", names)
        self.assertIn("borsuk/__init__.pyi", names)


if __name__ == "__main__":
    unittest.main()
