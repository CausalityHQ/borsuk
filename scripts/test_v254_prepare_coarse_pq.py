import unittest
import tempfile
import json
from pathlib import Path

import numpy as np

from scripts.v254_prepare_coarse_pq import digest, fit_and_assign, prepare


class CoarsePqTest(unittest.TestCase):
    def test_source_only_fit_is_deterministic_and_assigns_two_cells(self):
        data = np.asarray([[1, 0], [0.9, 0.1], [-1, 0], [-0.9, -0.1]], dtype=np.float32)
        first = fit_and_assign(data, 2)
        second = fit_and_assign(data, 2)
        np.testing.assert_array_equal(first[0], second[0])
        np.testing.assert_array_equal(first[1], second[1])
        self.assertTrue((first[1][:, 0] != first[1][:, 1]).all())

    def test_persisted_postings_have_two_distinct_owners_per_row(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, codes, prep = (root / name for name in ("vectors.raw", "codes.bin", "prep.json"))
            rows = 512
            vectors = np.tile(np.asarray([[1, 0], [-1, 0]], dtype="<f4"), (rows // 2, 1))
            vectors.tofile(source)
            codes.write_bytes(bytes(rows * 64))
            prep.write_text(json.dumps({"schema": "borsuk-v248-source-preparation-v1",
                "rows": rows, "dimensions": 2, "source_sha256": digest(source),
                "artifacts": {"codes.bin": {"sha256": digest(codes)}}}))
            output = root / "coarse"
            prepare(source, prep, codes, output)
            manifest = json.loads((output / "coarse.json").read_text())
            self.assertEqual(manifest["centroids"], 2)
            postings = np.fromfile(output / "postings.u32", dtype="<u4")
            self.assertTrue((np.bincount(postings, minlength=rows) == 2).all())


if __name__ == "__main__":
    unittest.main()
