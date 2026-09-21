"""Independent reducer contracts for the V103 metric-aware PQ48 spike."""

import dataclasses
import hashlib
import json
import unittest

import numpy as np

from scripts import test_v98_hierarchical_row_router as v98_tests
from scripts.test_v103_metric_aware_pq48 import (
    V103MetricAwarePq48Tests as _V103MetricAwarePq48Tests,
)
from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v103_metric_aware_pq48 import canonical_v103_result_bytes, evaluate_v103
from scripts.v103_metric_aware_pq48_rescore import (
    canonical_v103_rescore_bytes,
    rescore_v103_result,
)


class V103MetricAwarePq48RescoreTests(unittest.TestCase):
    @staticmethod
    def fixture():
        inputs = v98_tests.V98ProducerTests.inputs(
            dimensions=48,
            base_pages=128,
            delta_pages=9,
            query_count=2,
            truth_key=PageKey("base", 0),
            far_truth=False,
        )
        inputs = dataclasses.replace(
            inputs,
            vectors=np.ascontiguousarray(inputs.vectors + np.float32(0.000001)),
            queries=np.ascontiguousarray(inputs.queries + np.float32(0.000001)),
            training_rows=256,
            training_iterations=1,
            pages={
                key: RoutedPage(key, key.ordinal * 1_024, 1_024)
                for key in inputs.pages
            },
        )
        authority = v98_tests.V98ProducerTests.authority(inputs)
        config = _V103MetricAwarePq48Tests.config()
        body = canonical_v103_result_bytes(evaluate_v103(inputs, authority, config))
        return inputs, authority, config, body

    def test_recomputes_norm_io_sample_aggregate_interval_and_classification(self) -> None:
        inputs, authority, config, body = self.fixture()

        summary = rescore_v103_result(
            body,
            expected_inputs=inputs,
            expected_authority=authority,
            expected_config=config,
            expected_result_sha256=hashlib.sha256(body).hexdigest(),
        )

        self.assertEqual(summary.status, "verified")
        self.assertEqual(summary.query_count, 2)
        encoded = canonical_v103_rescore_bytes(summary)
        self.assertTrue(encoded.endswith(b"\n"))
        self.assertNotIn(b" ", encoded)

    def test_rejects_norm_sample_and_classification_drift(self) -> None:
        inputs, authority, config, body = self.fixture()
        value = json.loads(body)
        mutations = []
        norm = json.loads(json.dumps(value))
        norm["source_norms"]["maximum_squared_norm"] += 1.0
        mutations.append(("norm", norm))
        sample = json.loads(json.dumps(value))
        sample["metric_aware_samples"][0]["recall100_ppm"] ^= 1
        mutations.append(("sample", sample))
        classification = json.loads(json.dumps(value))
        classification["classification"] = "metric-aware-pq48-rejected"
        mutations.append(("classification", classification))

        for name, mutated in mutations:
            with self.subTest(name=name):
                mutated_body = (
                    json.dumps(
                        mutated,
                        allow_nan=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode()
                    + b"\n"
                )
                with self.assertRaises(ValueError):
                    rescore_v103_result(
                        mutated_body,
                        expected_inputs=inputs,
                        expected_authority=authority,
                        expected_config=config,
                        expected_result_sha256=hashlib.sha256(mutated_body).hexdigest(),
                    )


if __name__ == "__main__":
    unittest.main()
