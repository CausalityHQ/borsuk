import tempfile
import unittest
from pathlib import Path

import borsuk


class WarmTests(unittest.TestCase):
    def test_preload_option_and_warm_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = Path(tmp).as_uri()
            borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_max_vectors=1,
            )

            index = borsuk.open(uri, preload=True)
            index.add(
                [[0.0, 0.0], [1.0, 0.0], [2.0, 0.0]],
                ids=["a", "b", "c"],
            )
            self.assertGreaterEqual(index.stats().segments, 2)

            report = index.warm()

            self.assertGreaterEqual(report.segments_loaded, 1)
            self.assertGreater(report.bytes_resident, 0)


if __name__ == "__main__":
    unittest.main()
