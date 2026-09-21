"""Independent reducer contracts for the V106 anisotropic-PQ48 spike."""

import dataclasses
import hashlib
import json
import unittest

import numpy as np

from scripts import test_v98_hierarchical_row_router as v98_tests
from scripts.test_v106_anisotropic_pq48 import V106AnisotropicPq48Tests
from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v106_anisotropic_pq48 import canonical_v106_result_bytes, evaluate_v106
from scripts.v106_anisotropic_pq48_rescore import (
    canonical_v106_rescore_bytes,
    rescore_v106_result,
)


class V106AnisotropicPq48RescoreTests(unittest.TestCase):
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
            vectors=np.ascontiguousarray(inputs.vectors + np.float32(1.0)),
            training_rows=256,
            training_iterations=1,
            pages={
                key: RoutedPage(key, key.ordinal * 1_024, 1_024)
                for key in inputs.pages
            },
        )
        authority = v98_tests.V98ProducerTests.authority(inputs)
        config = V106AnisotropicPq48Tests.config()
        body = canonical_v106_result_bytes(evaluate_v106(inputs, authority, config))
        return inputs, authority, config, body

    def test_recomputes_io_samples_aggregates_intervals_and_classification(self) -> None:
        inputs, authority, config, body = self.fixture()

        receipt = rescore_v106_result(
            body,
            expected_inputs=inputs,
            expected_authority=authority,
            expected_config=config,
            expected_result_sha256=hashlib.sha256(body).hexdigest(),
        )

        self.assertEqual(receipt.status, "verified")
        self.assertEqual(receipt.query_count, 2)
        encoded = canonical_v106_rescore_bytes(receipt)
        self.assertTrue(encoded.endswith(b"\n"))
        self.assertNotIn(b" ", encoded)

    def test_rejects_sample_interval_identity_and_classification_drift(self) -> None:
        inputs, authority, config, body = self.fixture()
        value = json.loads(body)
        mutations = []
        for name, mutate in (
            ("sample", lambda item: item["challenger_samples"][0].__setitem__("recall100_ppm", 0)),
            ("paired", lambda item: item["paired"]["average_recall100_ppm"].__setitem__(0, -999_999)),
            ("identity", lambda item: item["challenger_codes_identity"].__setitem__("sha256", "0" * 64)),
            (
                "classification",
                lambda item: item.__setitem__(
                    "classification",
                    "anisotropic-pq48-rejected"
                    if item["classification"] == "anisotropic-pq48-qualified"
                    else "anisotropic-pq48-qualified",
                ),
            ),
        ):
            changed = json.loads(json.dumps(value))
            mutate(changed)
            mutations.append((name, changed))

        for name, changed in mutations:
            with self.subTest(name=name):
                changed_body = (
                    json.dumps(changed, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
                    + b"\n"
                )
                with self.assertRaises(ValueError):
                    rescore_v106_result(
                        changed_body,
                        expected_inputs=inputs,
                        expected_authority=authority,
                        expected_config=config,
                        expected_result_sha256=hashlib.sha256(changed_body).hexdigest(),
                    )


if __name__ == "__main__":
    unittest.main()
