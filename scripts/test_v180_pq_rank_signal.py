"""Seal and source-truth boundaries for the V180 panel."""

import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import numpy as np

from scripts.v166_surrogate_ranking_run import canonical
from scripts.v155_relaion_returned_quality import sha256
from scripts.v180_pq_rank_signal import COUNT, FIRST, SCHEMA, _features, _truth


class PqRankSignalTests(unittest.TestCase):
    def test_truth_excludes_query_row(self):
        source = np.eye(101, dtype=np.float64)
        identifiers = np.arange(101, dtype=np.int64)
        with patch("scripts.v180_pq_rank_signal.ROWS", 101):
            truth = _truth(source, identifiers, 0)
        self.assertEqual(len(truth), 100)
        self.assertNotIn(0, truth)

    def test_prelabel_feature_seal_detects_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (root / "features.jsonl").open("x") as output:
                for ordinal in range(FIRST, FIRST + COUNT):
                    output.write(canonical({
                        "ordinal": ordinal, "source_id": ordinal,
                        "source_row": ordinal, "mandatory_units": [1],
                        "ranked_units": [1, 2],
                    }))
            (root / "prepare-seal.json").write_text(canonical({
                "schema": SCHEMA + "-prepare-seal", "source_truth_opened": False,
                "features_sha256": sha256(root / "features.jsonl"),
                "pseudo_ids": list(range(FIRST, FIRST + COUNT)),
            }))
            args = Namespace(output=root,
                             prepare_sha256=sha256(root / "prepare-seal.json"))
            _, rows = _features(args)
            self.assertEqual(len(rows), COUNT)
            with (root / "features.jsonl").open("a") as output:
                output.write(json.dumps({"tampered": True}) + "\n")
            with self.assertRaises(ValueError):
                _features(args)


if __name__ == "__main__":
    unittest.main()
