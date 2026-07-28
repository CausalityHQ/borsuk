import builtins
import importlib.util
import unittest
from pathlib import Path
from unittest import mock

from scripts.benchmark_s3_vectors import (
    normalize_metric,
    percentile,
    permuted_positions,
    sample_stddev,
)


class BenchmarkS3VectorsTests(unittest.TestCase):
    def test_module_import_does_not_require_the_optional_aws_sdk(self) -> None:
        module_path = Path("scripts/benchmark_s3_vectors.py")
        spec = importlib.util.spec_from_file_location(
            "benchmark_s3_vectors_without_aws",
            module_path,
        )
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        original_import = builtins.__import__

        def reject_aws_sdk(name, *args, **kwargs):
            if name == "boto3" or name.startswith("botocore"):
                raise ModuleNotFoundError(name)
            return original_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=reject_aws_sdk):
            spec.loader.exec_module(importlib.util.module_from_spec(spec))

    def test_angular_maps_to_the_service_cosine_metric(self) -> None:
        self.assertEqual(normalize_metric("angular"), "cosine")
        self.assertEqual(normalize_metric("cosine"), "cosine")
        self.assertEqual(normalize_metric("euclidean"), "euclidean")
        with self.assertRaisesRegex(ValueError, "unsupported"):
            normalize_metric("inner-product")

    def test_percentile_uses_nearest_rank(self) -> None:
        self.assertEqual(percentile([4.0, 1.0, 3.0, 2.0], 0.95), 4.0)
        self.assertAlmostEqual(sample_stddev([1.0, 2.0, 3.0, 4.0]), 1.2909944487)

    def test_query_permutation_is_seeded_and_membership_preserving(self) -> None:
        first = permuted_positions(20, 17)
        self.assertEqual(first, permuted_positions(20, 17))
        self.assertNotEqual(first, permuted_positions(20, 23))
        self.assertEqual(sorted(first), list(range(20)))
        self.assertEqual(permuted_positions(10, 17), [2, 6, 8, 9, 7, 1, 0, 5, 3, 4])

    def test_raw_sample_schema_is_declared(self) -> None:
        source = (
            __import__("pathlib")
            .Path("scripts/benchmark_s3_vectors.py")
            .read_text(encoding="utf-8")
        )
        self.assertIn("query_samples.csv", source)
        for field in (
            "repetition_id",
            "query_seed",
            "query_position",
            "query_source_index",
            "latency_ms",
            "recall_at_10",
        ):
            self.assertIn(field, source)


if __name__ == "__main__":
    unittest.main()
