"""Canonical, authenticated per-query row-score evidence."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.native_row_score_evaluation import RowScoreSample
from scripts.native_row_score_evidence import read_evidence, write_evidence


class RowScoreEvidenceTests(unittest.TestCase):
    @staticmethod
    def sample() -> RowScoreSample:
        return RowScoreSample(
            query_ordinal=0, retained_pages=(0,),
            code_blocks=((0, 1, 0, 48),), code_gets=1, code_bytes=48,
            retained_hits_at_10=10, retained_hits_at_100=100,
            restricted_oracle_pages=(0,), restricted_oracle_hits_at_10=10,
            restricted_oracle_hits_at_100=100,
            pq_pages=(0,), pq_data_bytes=100, pq_hits_at_10=10,
            pq_hits_at_100=100, exact_pages=(0,), exact_data_bytes=100,
            exact_hits_at_10=10, exact_hits_at_100=100,
        )

    def test_round_trip_and_byte_tamper(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.json"
            identity = write_evidence(path, (self.sample(),))
            samples, metrics = read_evidence(path, identity)
            self.assertEqual(samples, (self.sample(),))
            self.assertEqual(metrics["pq_mean_recall_at_100_ppm"], 1_000_000)
            path.write_bytes(path.read_bytes().replace(b'"pq_hits_at_100":100', b'"pq_hits_at_100":99'))
            with self.assertRaisesRegex(ValueError, "evidence identity"):
                read_evidence(path, identity)


if __name__ == "__main__":
    unittest.main()
