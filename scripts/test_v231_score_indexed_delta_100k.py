import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import v231_score_indexed_delta_100k as scorer


class IndexedDeltaScoreTest(unittest.TestCase):
    def test_one_split_loss_rejects_index_even_when_latency_passes(self):
        ids = list(range(100))
        base = [{"ordinal": i, "arms": {"2048-2048": {"returned_ids": ids}}}
                for i in range(1000)]
        linear = [{"ordinal": i, "returned_ids": ids,
                   "delta_rows_scanned": 10_000, "vector_body_gets": 0}
                  for i in range(1000)]
        indexed = [{"ordinal": i, "returned_ids": ids,
                    "delta_rows_scanned": 256, "vector_body_gets": 0}
                   for i in range(1000)]
        indexed[0] = {**indexed[0], "returned_ids": ids[:-1] + [100]}
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            paths = {name: root / name for name in
                     ("linear_serving", "indexed_serving", "linear_raw", "indexed_raw",
                      "baseline", "truth", "output")}
            for name, mode in (("linear_serving", "linear-10k"),
                               ("indexed_serving", "indexed-10k")):
                paths[name].write_text(json.dumps({
                    "schema": "borsuk-v231-indexed-delta-100k-serving-v1",
                    "mode": mode, "raw_sha256": "identity", "upsert_rows": 10_000,
                    "upsert_stride": 10, "vector_body_gets": 0,
                    "index_candidates": 256 if mode == "indexed-10k" else 0,
                    "loaded": {"p95_ns": 10 if mode == "indexed-10k" else 20},
                    "index_build_ns": 1, "process_peak_rss_bytes": 1,
                    "delta_rows_scanned_total": 256_000,
                    "overlay_resident_bytes": 1,
                }))
            with (patch.object(scorer, "sha256", return_value="identity"),
                  patch.object(scorer, "BASE_SHA", "identity"),
                  patch.object(scorer, "BASE_SPLITS", (25_600, 74_400)),
                  patch.object(scorer, "jsonl", side_effect=[base, linear, indexed]),
                  patch.object(scorer, "load_truth", return_value=[ids] * 1000),
                  patch.object(scorer, "hits", side_effect=lambda values, gold:
                               len(set(values) & set(gold)))):
                scorer.run(argparse.Namespace(**paths))
            result = json.loads(paths["output"].read_text())
            self.assertTrue(result["performance_pass"])
            self.assertFalse(result["quality_pass"])
            self.assertFalse(result["pass"])


if __name__ == "__main__":
    unittest.main()
