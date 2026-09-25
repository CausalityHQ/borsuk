import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import v235_score_screened_delta_100k as scorer


class ScreenedDeltaScoreTest(unittest.TestCase):
    def test_exact_ids_rescore_count_and_loaded_gate(self):
        ids = list(range(100))
        with tempfile.TemporaryDirectory() as folder:
            paths = {name: Path(folder) / name for name in
                     ("control_serving", "candidate_serving", "control_raw", "candidate_raw",
                      "control_loaded_raw", "candidate_loaded_raw", "baseline", "truth", "output")}
            paths["truth"].write_bytes(b"fixture")
            paths["baseline"].write_text("".join(json.dumps({
                "ordinal": i, "arms": {"2048-2048": {"returned_ids": ids}}}) + "\n"
                for i in range(1000)))
            for mode, latency, scored in (("control", 20, 10_000), ("candidate", 10, 10)):
                paths[f"{mode}_raw"].write_text("".join(json.dumps({
                    "ordinal": i, "returned_ids": ids, "delta_rows_scanned": 10_000,
                    "delta_rows_scored": scored, "vector_body_gets": 0}) + "\n"
                    for i in range(1000)))
                paths[f"{mode}_loaded_raw"].write_text("".join(json.dumps({
                    "ordinal": i, "whole_ns": latency}) + "\n" for i in range(1000)))
                paths[f"{mode}_serving"].write_text(json.dumps({
                    "schema": ("borsuk-v232-decoded-delta-100k-serving-v1" if mode == "control"
                               else "borsuk-v235-screened-delta-100k-serving-v1"),
                    "mode": "decoded-10k" if mode == "control" else "screened-10k",
                    "upsert_rows": 10_000, "upsert_stride": 10,
                    "vector_body_gets": 0, "delta_rows_scanned_total": 10_000_000,
                    "delta_rows_scored_total": scored * 1000,
                    "raw_sha256": scorer.sha256(paths[f"{mode}_raw"]),
                    "loaded_raw_sha256": scorer.sha256(paths[f"{mode}_loaded_raw"]),
                    "loaded": {**{f"p{pct}_ns": latency for pct in (50, 90, 95, 99)},
                               "qps": 100 if mode == "control" else 200},
                    "loaded_wall_ns": 10_000_000_000 if mode == "control" else 5_000_000_000,
                    "decode_ns": 1, "process_peak_rss_bytes": 1,
                    "overlay_resident_bytes": 1,
                }))
            args = argparse.Namespace(**paths)
            with (patch.object(scorer, "BASE_SHA", scorer.sha256(paths["baseline"])),
                  patch.object(scorer, "BASE_SPLITS", (25_600, 74_400)),
                  patch.object(scorer, "load_truth", return_value=[ids] * 1000)):
                scorer.run(args)
                result = json.loads(paths["output"].read_text())
                self.assertTrue(result["pass"])
                self.assertEqual(result["exact_delta_rows_scored_total"], 10_000)
                bad = [json.loads(line) for line in paths["candidate_raw"].read_text().splitlines()]
                bad[0]["returned_ids"][-1] = 100
                paths["candidate_raw"].write_text("".join(json.dumps(row) + "\n" for row in bad))
                serving = json.loads(paths["candidate_serving"].read_text())
                serving["raw_sha256"] = scorer.sha256(paths["candidate_raw"])
                paths["candidate_serving"].write_text(json.dumps(serving))
                scorer.run(args)
                self.assertFalse(json.loads(paths["output"].read_text())["pass"])
