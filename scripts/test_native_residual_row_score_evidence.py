"""Canonical residual evidence must authenticate its per-query samples."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.native_residual_row_score_evaluation import ResidualSample
from scripts.native_residual_row_score_evidence import (
    read_residual_evidence,
    write_residual_evidence,
)
from scripts.native_row_score_evaluation import RowScoreSample


class ResidualEvidenceTests(unittest.TestCase):
    @staticmethod
    def sample() -> ResidualSample:
        baseline = RowScoreSample(
            query_ordinal=0, retained_pages=(0,),
            code_blocks=((0, 1, 0, 48),), code_gets=1, code_bytes=48,
            retained_hits_at_10=10, retained_hits_at_100=100,
            restricted_oracle_pages=(0,), restricted_oracle_hits_at_10=10,
            restricted_oracle_hits_at_100=100,
            pq_pages=(0,), pq_data_bytes=100,
            pq_hits_at_10=10, pq_hits_at_100=100,
            exact_pages=(0,), exact_data_bytes=100,
            exact_hits_at_10=10, exact_hits_at_100=100,
        )
        return ResidualSample(
            query_ordinal=0, baseline=baseline,
            code_blocks=((0, 1, 0, 72),), code_gets=1, code_bytes=72,
            residual_pages=(0,), residual_data_bytes=100,
            residual_hits_at_10=10, residual_hits_at_100=100,
        )

    def test_canonical_round_trip_and_modified_aggregate_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.json"
            identity = write_residual_evidence(path, (self.sample(),))
            samples, metrics = read_residual_evidence(path, identity)
            self.assertEqual(samples, (self.sample(),))
            self.assertEqual(metrics["decision"], "quality-advance-memory-pending")
            changed = json.loads(path.read_bytes())
            changed["metrics"]["residual_mean_recall_at_100_ppm"] = 0
            payload = (json.dumps(changed, sort_keys=True, separators=(",", ":")) + "\n").encode()
            path.write_bytes(payload)
            forged = dataclasses.replace(
                identity, sha256=hashlib.sha256(payload).hexdigest(), encoded_bytes=len(payload)
            )
            with self.assertRaisesRegex(ValueError, "aggregates"):
                read_residual_evidence(path, forged)


if __name__ == "__main__":
    unittest.main()
