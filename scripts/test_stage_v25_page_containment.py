import hashlib
import pathlib
import tempfile
import unittest

from scripts.stage_v25_page_containment import RegisteredInput, stage_exact_inputs


class _S3:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        pathlib.Path(filename).write_bytes(self.objects[(bucket, key)])


class V25ContainmentStagingTests(unittest.TestCase):
    def test_stage_authenticates_exact_bytes_and_never_uses_etag(self) -> None:
        payload = b"PAR1-v25-fixture"
        registered = RegisteredInput(
            role="construction-rows-parquet",
            uri="s3://borsuk-evidence/v25/construction.parquet",
            sha256=hashlib.sha256(payload).hexdigest(),
            encoded_bytes=len(payload),
            file_name="construction.parquet",
            generation="v25-open-262144",
        )
        with tempfile.TemporaryDirectory() as directory:
            staged = stage_exact_inputs(
                _S3({("borsuk-evidence", "v25/construction.parquet"): payload}),
                [registered],
                pathlib.Path(directory),
            )
            self.assertEqual(staged, [pathlib.Path(directory) / "construction.parquet"])
            changed = RegisteredInput(**{**registered.__dict__, "sha256": "0" * 64})
            with self.assertRaises(ValueError):
                stage_exact_inputs(
                    _S3({("borsuk-evidence", "v25/construction.parquet"): payload}),
                    [changed],
                    pathlib.Path(directory) / "changed",
                )


if __name__ == "__main__":
    unittest.main()
