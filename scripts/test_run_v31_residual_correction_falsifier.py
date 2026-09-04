import json
import subprocess
import sys
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.run_v30_variable_rate_reproduction import (
    ArtifactAuthority,
    Pq8Model,
    exact_truth,
)
from scripts.run_v31_residual_correction_falsifier import (
    ARM_NAMES,
    V31ResidualObservation,
    build_residual_correction_result,
    correct_residual_scores,
    encode_residual_correction_evidence,
    evaluate_residual_correction_arms,
    finalize_residual_correction_result,
    quantize_squared_error_u8,
    residual_projection_matrix,
    select_residual_pages,
)


class V31ResidualCorrectionTests(unittest.TestCase):
    def test_v31_residual_module_reaches_authority_parser(self) -> None:
        # Break caught: __main__ passes its own module name to the inherited
        # strict parser and fails at a spurious positional argument before authority.
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_v31_residual_correction_falsifier",
                "reproduce",
                "--execute",
            ],
            capture_output=True,
            check=False,
            text=True,
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("missing --pages-manifest-bytes", completed.stderr)
        self.assertNotIn("flag differs", completed.stderr)

    def test_v31_residual_quantizer_and_projection_are_query_independent(self) -> None:
        squared = np.array([0.0, 0.25, 1.0, 0.04, 0.16, 0.36], dtype=np.float32)
        leaves = np.array([0, 0, 0, 1, 1, 1], dtype=np.int32)
        codes, steps, decoded = quantize_squared_error_u8(squared, leaves)
        self.assertEqual(codes.dtype, np.uint8)
        self.assertEqual(steps.shape, (2,))
        self.assertEqual(decoded.shape, squared.shape)
        self.assertTrue(np.isfinite(decoded).all())
        self.assertLessEqual(float(np.max(np.abs(decoded - squared))), float(max(steps)))
        np.testing.assert_array_equal(
            residual_projection_matrix(96, 16, "1" * 64),
            residual_projection_matrix(96, 16, "1" * 64),
        )
        self.assertFalse(
            np.array_equal(
                residual_projection_matrix(96, 16, "1" * 64),
                residual_projection_matrix(96, 16, "2" * 64),
            )
        )
        with self.assertRaisesRegex(ValueError, "squared error"):
            quantize_squared_error_u8(np.array([np.nan], dtype=np.float32), np.array([0]))

    def test_v31_exact_cross_term_reproduces_true_distance(self) -> None:
        reconstruction = np.array(
            [[0.25, -0.5, 0.0, 0.75], [-0.1, 0.2, 0.3, -0.4]], dtype=np.float32
        )
        residual_error = np.array(
            [[0.1, -0.2, 0.05, 0.0], [0.03, 0.04, -0.02, 0.08]], dtype=np.float32
        )
        query_residual = np.array([0.2, -0.1, 0.4, 0.6], dtype=np.float32)
        delta = query_residual[None, :] - reconstruction
        adc = np.sum(delta * delta, axis=1, dtype=np.float32)
        expected = np.sum(
            (delta - residual_error) * (delta - residual_error), axis=1, dtype=np.float32
        )
        exact = correct_residual_scores(
            adc,
            residual_error,
            reconstruction,
            query_residual,
            mode="exact-cross-term",
        )
        np.testing.assert_allclose(exact, expected, rtol=0.0, atol=2e-7)
        scalar = correct_residual_scores(
            adc,
            residual_error,
            reconstruction,
            query_residual,
            mode="exact-error",
        )
        np.testing.assert_allclose(
            scalar,
            adc - np.sum(residual_error * residual_error, axis=1, dtype=np.float32),
            rtol=0.0,
            atol=2e-7,
        )
        projection = residual_projection_matrix(4, 16, "3" * 64)
        sketched = correct_residual_scores(
            adc,
            residual_error,
            reconstruction,
            query_residual,
            mode="sign16",
            projection=projection,
        )
        self.assertTrue(np.isfinite(sketched).all())

    def test_v31_corrected_order_recovers_page_without_expanding_page_count(self) -> None:
        scan = np.arange(11, dtype=np.float32)
        corrected = scan.copy()
        corrected[10] = -1.0
        pages = np.arange(11, dtype=np.int32)
        baseline = select_residual_pages(
            scan, scan, pages, candidate_depth=11, page_count=10
        )
        recovered = select_residual_pages(
            scan, corrected, pages, candidate_depth=11, page_count=10
        )
        self.assertEqual(baseline, tuple(range(10)))
        self.assertEqual(recovered, (10, 0, 1, 2, 3, 4, 5, 6, 7, 8))
        self.assertEqual(len(recovered), 10)
        self.assertEqual(len(set(recovered)), 10)

    def test_v31_result_freezes_ladder_and_selects_smallest_perfect_arm(self) -> None:
        self.assertEqual(
            ARM_NAMES,
            ("none", "u8-error", "sign8", "sign16", "exact-error", "exact-cross-term"),
        )
        observations = []
        for index, arm in enumerate(ARM_NAMES):
            hits = [10] * 32
            if index < 2:
                hits[-1] = 9
            observations.append(
                V31ResidualObservation(
                    arm=arm,
                    hits=tuple(hits),
                    selected_page_counts=(10,) * 32,
                    maximum_encoded_bytes=1_000_000,
                    maximum_scanned_codes=100_000,
                    maximum_candidates_retained=12_288,
                )
            )
        result = build_residual_correction_result(tuple(observations))
        self.assertTrue(result.endswith(b"\n"))
        self.assertEqual(result, json.dumps(json.loads(result), separators=(",", ":"), sort_keys=True).encode() + b"\n")
        value = json.loads(result)
        self.assertFalse(value["claim_eligible"])
        self.assertEqual(value["selected_arm"], "sign8")
        self.assertEqual(value["status"], "perfect-arm-found")
        changed = list(observations)
        changed[0] = V31ResidualObservation(
            arm="changed",
            hits=changed[0].hits,
            selected_page_counts=changed[0].selected_page_counts,
            maximum_encoded_bytes=changed[0].maximum_encoded_bytes,
            maximum_scanned_codes=changed[0].maximum_scanned_codes,
            maximum_candidates_retained=changed[0].maximum_candidates_retained,
        )
        with self.assertRaisesRegex(ValueError, "arm ordering"):
            build_residual_correction_result(tuple(changed))

    def test_v31_evaluator_runs_all_arms_over_one_fixed_layout(self) -> None:
        generator = np.random.Generator(np.random.PCG64(7))
        primary = generator.standard_normal((320, 96), dtype=np.float32)
        primary /= np.linalg.norm(primary, axis=1)[:, None]
        queries = primary[:32].copy()
        leaves = np.zeros(320, dtype=np.int32)
        leaf_centroids = np.zeros((1, 96), dtype=np.float32)
        centroids24 = np.zeros((24, 256, 4), dtype=np.float32)
        centroids48 = np.zeros((48, 256, 2), dtype=np.float32)
        base = Pq8Model(24, 4, centroids24)
        high = Pq8Model(48, 2, centroids48)
        truth = exact_truth(primary, queries)
        observations = evaluate_residual_correction_arms(
            primary,
            leaves,
            leaf_centroids,
            queries,
            truth,
            base,
            high,
            page_rows=10,
            leaf_beam=1,
            candidate_depth=320,
            page_encoded_bytes=(1_000,) * 32,
            projection_seed_sha256="4" * 64,
        )
        self.assertEqual(tuple(item.arm for item in observations), ARM_NAMES)
        self.assertTrue(all(item.selected_page_counts == (10,) * 32 for item in observations))
        self.assertEqual(observations[-1].hits, (10,) * 32)
        self.assertTrue(all(item.maximum_scanned_codes == 320 for item in observations))
        self.assertTrue(all(item.maximum_candidates_retained == 320 for item in observations))

    def test_v31_evidence_and_result_bind_exact_inputs(self) -> None:
        observations = tuple(
            V31ResidualObservation(
                arm=arm,
                hits=(10,) * 32,
                selected_page_counts=(10,) * 32,
                maximum_encoded_bytes=1_234,
                maximum_scanned_codes=5_678,
                maximum_candidates_retained=5_678,
            )
            for arm in ARM_NAMES
        )
        evidence = encode_residual_correction_evidence(observations)
        table = pq.read_table(pa.BufferReader(evidence))
        self.assertEqual(table.num_rows, 6 * 32)
        self.assertEqual(
            table.schema.names,
            ["arm", "query_ordinal", "hits", "selected_pages"],
        )
        artifacts = tuple(
            ArtifactAuthority(
                role=role,
                uri=f"s3://frozen/{role}",
                sha256=character * 64,
                encoded_bytes=index + 1,
            )
            for index, (role, character) in enumerate(
                (
                    ("pages-manifest", "1"),
                    ("leaf-postings", "2"),
                    ("leaf-centroids", "3"),
                    ("query-parquet", "4"),
                )
            )
        )
        result = finalize_residual_correction_result(
            observations,
            artifacts,
            construction_bytes_streamed=46_761_076,
            evidence_parquet=evidence,
            projection_seed_sha256="1" * 64,
        )
        value = json.loads(result)
        self.assertEqual(value["artifacts"][0]["sha256"], "1" * 64)
        self.assertEqual(value["projection_seed_sha256"], "1" * 64)
        self.assertEqual(value["construction_bytes_streamed"], 46_761_076)
        self.assertEqual(value["evidence_parquet_bytes"], len(evidence))
        self.assertEqual(len(value["evidence_parquet_sha256"]), 64)
        self.assertFalse(value["claim_eligible"])
        changed = bytearray(evidence)
        changed[-1] ^= 1
        with self.assertRaisesRegex(ValueError, "evidence"):
            finalize_residual_correction_result(
                observations,
                artifacts,
                construction_bytes_streamed=46_761_076,
                evidence_parquet=bytes(changed),
                projection_seed_sha256="1" * 64,
            )


if __name__ == "__main__":
    unittest.main()
