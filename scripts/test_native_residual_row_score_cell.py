"""Sealed source-only construction for the row-score cell."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    EvaluationLimits,
    LayoutMethod,
    MembershipRow,
)
from scripts.native_page_microcluster_cell import FROZEN_INPUTS
from scripts.native_residual_row_score_cell import (
    construct_cell,
    evaluate_routes,
    read_cell_seal,
    run_construct_phase,
    run_evaluate_phase,
    run_validate_phase,
)
from scripts.native_residual_row_score_codes import read_residual_codes


class ResidualRowScoreCellTests(unittest.TestCase):
    def test_cli_requires_output_directory_for_evaluation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = subprocess.run(
                [sys.executable, "-m", "scripts.native_residual_row_score_cell", "evaluate",
                 "--root", temporary, "--output-prefix", "s3://bucket/run"],
                capture_output=True, text=True, check=False,
            )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--out is required", result.stderr)

    def test_construct_phase_does_not_read_query_or_truth(self) -> None:
        ids = tuple(index.to_bytes(4, "little") for index in range(256))
        vectors = np.repeat(np.arange(256, dtype=np.float32)[:, None], 48, axis=1)
        source_sha = hashlib.sha256(b"source").digest()
        membership = tuple(
            MembershipRow(
                stable_id=ids[index], source_ordinal=index,
                page_ordinal=index // 128, in_page_ordinal=index % 128,
                page_rows=128, encoded_page_bytes=1000,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=source_sha, seed=7,
                construction_sha256=hashlib.sha256(b"layout").digest(),
            ) for index in range(256)
        )
        inputs = dataclasses.replace(
            FROZEN_INPUTS,
            layout=dataclasses.replace(
                FROZEN_INPUTS.layout, seed=7, dimensions=48,
                source=ArtifactIdentity("source", "file:///source", source_sha.hex(), 1),
            ),
            membership=ArtifactIdentity(
                "geometric-membership", "file:///membership",
                hashlib.sha256(b"membership").hexdigest(), 1,
            ),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("scripts.native_residual_row_score_cell._read_inputs", return_value=(ids, vectors, membership)):
                run_construct_phase(root, "s3://bucket/run", inputs)
            self.assertTrue((root / "sealed.json").exists())
            self.assertFalse((root / "queries.parquet").exists())
            plane = (root / "codes.bin").read_bytes()
            router = SimpleNamespace(
                pages=(SimpleNamespace(encoded_page_bytes=1000),) * 2
            )
            with (
                patch("scripts.native_residual_row_score_cell._read_inputs", return_value=(ids, vectors, membership)),
                patch("scripts.native_residual_row_score_cell._read_queries_truth", return_value=(
                    vectors[180:181], (tuple(ids[index] for index in range(128, 228)),)
                )),
                patch("scripts.native_residual_row_score_cell.read_geometric_router_parquet", return_value=router),
                patch("scripts.native_residual_row_score_cell.route_geometric_query", return_value=SimpleNamespace(
                    retained_leaf_pages=(0, 1)
                )),
            ):
                run_evaluate_phase(
                    root, root / "evaluation", "s3://bucket/run", inputs,
                    code_reader=lambda offset, length: plane[offset : offset + length],
                )
            result = json.loads((root / "evaluation" / "result.json").read_bytes())
            self.assertEqual(result["metrics"]["residual_mean_recall_at_100_ppm"], 1_000_000)
            with (
                patch("scripts.native_residual_row_score_cell._read_inputs", return_value=(ids, vectors, membership)),
                patch("scripts.native_residual_row_score_cell._read_queries_truth", return_value=(
                    vectors[180:181], (tuple(ids[index] for index in range(128, 228)),)
                )),
                patch("scripts.native_residual_row_score_cell.read_geometric_router_parquet", return_value=router),
                patch("scripts.native_residual_row_score_cell._independent_route", return_value=(
                    (0, 1), (0, 1), 2000, 0
                )),
            ):
                validation = run_validate_phase(
                    root, root / "evaluation", "s3://bucket/run", "ab" * 20, inputs
                )
            self.assertEqual(validation["decision"], "quality-advance-memory-pending")

    def test_construct_seals_codes_without_query_or_truth_inputs(self) -> None:
        ids = tuple(index.to_bytes(4, "little") for index in range(256))
        vectors = np.repeat(np.arange(256, dtype=np.float32)[:, None], 48, axis=1)
        source_sha = hashlib.sha256(b"source").digest()
        membership_sha = hashlib.sha256(b"membership").digest()
        membership = tuple(
            MembershipRow(
                stable_id=ids[index], source_ordinal=index,
                page_ordinal=index // 128, in_page_ordinal=index % 128,
                page_rows=128, encoded_page_bytes=1000,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=source_sha, seed=7,
                construction_sha256=hashlib.sha256(b"layout").digest(),
            ) for index in range(256)
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            construct_cell(
                root, "s3://bucket/run", ids, vectors, membership,
                source_sha, membership_sha, seed=7, iterations=1,
            )
            identities = read_cell_seal(
                root, "s3://bucket/run", source_sha, membership_sha
            )
            self.assertEqual(identities.codes.encoded_bytes, 256 * 72)
            self.assertEqual(
                identities.codes.uri, "s3://bucket/run/artifacts/codes.bin"
            )
            self.assertFalse((root / "queries.parquet").exists())
            self.assertFalse((root / "truth.parquet").exists())
            artifacts = read_residual_codes(
                root, identities, ids, membership, source_sha, membership_sha,
                dimensions=48, seed=7,
            )
            plane = (root / "codes.bin").read_bytes()
            samples, metrics = evaluate_routes(
                queries=vectors[180:181],
                truth=(tuple(ids[index] for index in range(128, 228)),),
                retained_by_query=((0, 1),),
                artifacts=artifacts,
                stable_ids=ids,
                vectors=vectors,
                page_byte_sizes=(1000, 1000),
                limits=EvaluationLimits(1, 1000),
                read_code_range=lambda offset, length: plane[offset : offset + length],
            )
            self.assertEqual(samples[0].residual_hits_at_100, 100)
            self.assertEqual(metrics["decision"], "quality-advance-memory-pending")


if __name__ == "__main__":
    unittest.main()
