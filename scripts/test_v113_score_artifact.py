"""Authenticated source-only artifact boundary for the V113 score screen."""

import tempfile
import unittest
import json
from pathlib import Path

import numpy as np

from scripts.v113_100k_score_screen import build_arrays
from scripts.v113_score_artifact import read_artifact, write_artifact


class ScoreArtifactTests(unittest.TestCase):
    def test_roundtrip_and_corruption_rejection(self) -> None:
        rng = np.random.default_rng(117)
        vectors = rng.normal(size=(256, 64)).astype(np.float32)
        built = build_arrays(
            vectors, np.arange(256, dtype=np.int64),
            pq_seed=7301, residual_seed=113031,
            sample_rows=256, iterations=1,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "sealed"
            write_artifact(
                root, built, source_sha256="a" * 64,
                pq_seed=7301, residual_seed=113031,
                sample_rows=256, iterations=1,
            )
            loaded = read_artifact(
                root, expected_source_sha256="a" * 64,
                expected_sample_rows=256, expected_iterations=1,
            )
            np.testing.assert_array_equal(loaded.pq_codes, built.pq_codes)
            np.testing.assert_array_equal(loaded.residual_codes, built.residual_codes)
            self.assertEqual(loaded.corrections.nu_scale, built.corrections.nu_scale)
            seal_path = root / "seal.json"
            seal_body = seal_path.read_bytes()
            seal = json.loads(seal_body)
            seal["pq_seed"] = 7302
            seal_path.write_text(json.dumps(seal, sort_keys=True, separators=(",", ":")) + "\n")
            with self.assertRaises(ValueError):
                read_artifact(
                    root, expected_source_sha256="a" * 64,
                    expected_sample_rows=256, expected_iterations=1,
                )
            seal_path.write_bytes(seal_body)
            code_path = root / "residual_codes.npy"
            body = bytearray(code_path.read_bytes())
            body[-1] ^= 1
            code_path.write_bytes(body)
            with self.assertRaises(ValueError):
                read_artifact(
                    root, expected_source_sha256="a" * 64,
                    expected_sample_rows=256, expected_iterations=1,
                )


if __name__ == "__main__":
    unittest.main()
