"""Focused deterministic OPQ8 routing-code and group-ranking checks."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import (
    artifact_names,
    build_plan,
    worker_script,
)
from scripts.native_hundred_thousand_opq8_router import (
    CENTROIDS,
    DIMENSIONS,
    SUBSPACES,
    Opq8Model,
    _assign,
    _farthest_first,
    _lloyd,
    encode_opq8,
    rank_opq8_groups,
    read_opq8_model,
    row_adc_scores,
    training_ordinals,
    write_opq8_model,
)


class Opq8RouterTests(unittest.TestCase):
    def test_spot_worker_preserves_query_truth_boundary(self) -> None:
        commit = "12" * 20
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", "34" * 32, 1234),
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-hundred-thousand-opq8-router/"
                + commit + "/runs/relaion-100k-dev1000-a0001"
            ),
            selector_kind="opq8",
        )
        script = worker_script(plan)
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertIn("borsuk-hundred-thousand-opq8-terminal-v1", script)
        self.assertEqual(len(artifact_names("opq8")), 11)
        self.assertLess(script.index("publish_artifact seal.json"), script.index("queries.parquet --only-show-errors"))
        self.assertLess(script.index("publish_artifact plans.json"), script.index("truth.parquet --only-show-errors"))
        self.assertIn("--body terminal.json --if-none-match '*'", script)
        self.assertEqual(script.count("timeout --foreground"), 4)
        self.assertIn("stop_active\n", script)

    def test_hash_selection_and_ties_are_stable(self) -> None:
        selected = training_ordinals(300, take=256)
        self.assertEqual(len(selected), 256)
        self.assertEqual(len(set(int(i) for i in selected)), 256)
        self.assertTrue(np.array_equal(selected, training_ordinals(300, take=256)))
        values = np.zeros((256, 2), dtype=np.float64)
        values[:, 0] = np.arange(256)
        centers = _farthest_first(values, np.arange(256, dtype=np.int64))
        self.assertEqual(centers.shape, (CENTROIDS, 2))
        self.assertEqual(len(set(float(i) for i in centers[:, 0])), 256)
        trained = _lloyd(values, centers, iterations=1)
        self.assertEqual(len(set(int(i) for i in _assign(values, trained))), 256)

    def test_model_serialization_adc_and_top_four_group_order(self) -> None:
        mean = np.zeros(DIMENSIONS, dtype="<f4")
        rotation = np.eye(DIMENSIONS, dtype="<f4")
        books = np.zeros((SUBSPACES, CENTROIDS, DIMENSIONS // SUBSPACES), dtype="<f4")
        books[:, 1, 0] = 2.0
        model = Opq8Model(mean, rotation, books)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "model.bin"
            write_opq8_model(path, model)
            loaded = read_opq8_model(path)
            self.assertTrue(np.array_equal(loaded.books, books))
            vectors = np.zeros((8, DIMENSIONS), dtype=np.float32)
            vectors[4:, 0] = 2.0
            order = np.arange(8, dtype=np.int64)
            codes = encode_opq8(vectors, order, loaded)
            self.assertEqual(codes.shape, (8, SUBSPACES))
            query = np.zeros(DIMENSIONS, dtype=np.float32)
            scores = row_adc_scores(query, loaded, codes)
            self.assertTrue(np.all(scores[:4] == 0))
            self.assertTrue(np.all(scores[4:] == 4))
            self.assertEqual(rank_opq8_groups(scores, (1,) * 8), (0, 1))
            with path.open("ab") as stream:
                stream.write(b"x")
            with self.assertRaises(ValueError):
                read_opq8_model(path)


if __name__ == "__main__":
    unittest.main()
