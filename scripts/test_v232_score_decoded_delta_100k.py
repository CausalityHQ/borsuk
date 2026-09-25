import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import v232_score_decoded_delta_100k as scorer


class DecodedDeltaScoreTest(unittest.TestCase):
    def test_raw_loaded_percentiles_and_exact_ids_gate(self):
        ids = list(range(100))
        with tempfile.TemporaryDirectory() as folder:
            paths = {name: Path(folder) / name for name in
                     ("linear_serving", "decoded_serving", "linear_raw", "decoded_raw",
                      "linear_loaded_raw", "decoded_loaded_raw", "baseline", "truth", "output")}
            paths["truth"].write_bytes(b"fixture")
            paths["baseline"].write_text("".join(json.dumps({
                "ordinal": i, "arms": {"2048-2048": {"returned_ids": ids}}}) + "\n"
                for i in range(1000)))
            for mode, latency in (("linear", 20), ("decoded", 10)):
                paths[f"{mode}_raw"].write_text("".join(json.dumps({
                    "ordinal": i, "returned_ids": ids,
                    "delta_rows_scanned": 10_000, "vector_body_gets": 0}) + "\n"
                    for i in range(1000)))
                paths[f"{mode}_loaded_raw"].write_text("".join(json.dumps({
                    "ordinal": i, "whole_ns": latency}) + "\n" for i in range(1000)))
                paths[f"{mode}_serving"].write_text(json.dumps({
                    "schema": "borsuk-v232-decoded-delta-100k-serving-v1",
                    "mode": f"{mode}-10k", "upsert_rows": 10_000, "upsert_stride": 10,
                    "vector_body_gets": 0, "delta_rows_scanned_total": 10_000_000,
                    "raw_sha256": scorer.sha256(paths[f"{mode}_raw"]),
                    "loaded_raw_sha256": scorer.sha256(paths[f"{mode}_loaded_raw"]),
                    "loaded": {**{f"p{pct}_ns": latency for pct in (50, 90, 95, 99)},
                               "qps": 100 if mode == "linear" else 200},
                    "loaded_wall_ns": 10_000_000_000 if mode == "linear" else 5_000_000_000,
                    "decode_ns": 1, "process_peak_rss_bytes": 1,
                    "overlay_resident_bytes": 1,
                }))
            args = argparse.Namespace(**paths)
            with (patch.object(scorer, "BASE_SHA", scorer.sha256(paths["baseline"])),
                  patch.object(scorer, "BASE_SPLITS", (25_600, 74_400)),
                  patch.object(scorer, "load_truth", return_value=[ids] * 1000)):
                scorer.run(args)
                self.assertTrue(json.loads(paths["output"].read_text())["pass"])
                bad = [json.loads(line) for line in paths["decoded_raw"].read_text().splitlines()]
                bad[0]["returned_ids"][-1] = 100
                paths["decoded_raw"].write_text("".join(json.dumps(row) + "\n" for row in bad))
                serving = json.loads(paths["decoded_serving"].read_text())
                serving["raw_sha256"] = scorer.sha256(paths["decoded_raw"])
                paths["decoded_serving"].write_text(json.dumps(serving))
                scorer.run(args)
                result = json.loads(paths["output"].read_text())
                self.assertEqual(result["exact_linear_id_lists"], 999)
                self.assertFalse(result["pass"])


if __name__ == "__main__":
    unittest.main()
