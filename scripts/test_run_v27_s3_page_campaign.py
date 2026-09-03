import json
import unittest

from scripts.run_v27_s3_page_campaign import (
    S3LatencyProfile,
    V27QueryEvidence,
    preflight_v27_reduced_campaign,
    project_v27_query_latency,
)


class V27FastS3GateTests(unittest.TestCase):
    def evidence(self, **changes: object) -> V27QueryEvidence:
        values: dict[str, object] = {
            "cpu_p99_ms": 11.5,
            "get_count": 10,
            "encoded_bytes": 4_587_520,
            "recall_ppm": 1_000_000,
            "minimum_recall_ppm": 1_000_000,
        }
        values.update(changes)
        return V27QueryEvidence(**values)

    def profile(self, **changes: object) -> S3LatencyProfile:
        values: dict[str, object] = {
            "request_latency_ms": 40.0,
            "aggregate_bytes_per_second": 350 * 1024 * 1024,
        }
        values.update(changes)
        return S3LatencyProfile(**values)

    def test_v27_fast_gate_projects_one_concurrent_s3_wave_from_exact_work(self) -> None:
        # Break caught: ten parallel page GETs are modeled as ten serial RTTs, or transfer bytes
        # disappear from the fast projection before an expensive full-scale run.
        result = project_v27_query_latency(self.evidence(), self.profile())
        self.assertEqual(result.request_waves, 1)
        self.assertEqual(result.get_count, 10)
        self.assertEqual(result.encoded_bytes, 4_587_520)
        self.assertEqual(result.request_ms, 40.0)
        self.assertAlmostEqual(result.transfer_ms, 12.5)
        self.assertAlmostEqual(result.projected_p99_ms, 64.0)

    def test_v27_fast_gate_rejects_quality_work_and_latency_before_launch(self) -> None:
        # Break caught: an impossible arm reaches Spot or the 100M campaign instead of failing in
        # milliseconds from truthful work counters and injected S3 measurements.
        cases = {
            "requests": self.evidence(get_count=11),
            "bytes": self.evidence(encoded_bytes=4_587_521),
            "aggregate-recall": self.evidence(recall_ppm=999_999),
            "minimum-recall": self.evidence(minimum_recall_ppm=999_999),
            "cpu": self.evidence(cpu_p99_ms=15.001),
        }
        for label, evidence in cases.items():
            with self.subTest(label=label):
                launches: list[str] = []
                with self.assertRaisesRegex(ValueError, label):
                    preflight_v27_reduced_campaign(
                        evidence,
                        self.profile(),
                        launch=lambda launches=launches: launches.append("launched"),
                    )
                self.assertEqual(launches, [])

        launches = []
        with self.assertRaisesRegex(ValueError, "latency"):
            preflight_v27_reduced_campaign(
                self.evidence(),
                self.profile(request_latency_ms=140.0),
                launch=lambda: launches.append("launched"),
            )
        self.assertEqual(launches, [])

    def test_v27_fast_gate_emits_canonical_authority_before_one_launch(self) -> None:
        launches: list[str] = []
        receipt = preflight_v27_reduced_campaign(
            self.evidence(),
            self.profile(),
            launch=lambda: launches.append("launched"),
        )
        self.assertEqual(launches, ["launched"])
        self.assertTrue(receipt.endswith(b"\n"))
        value = json.loads(receipt)
        self.assertEqual(value["claim_eligible"], False)
        self.assertEqual(value["status"], "passed")
        self.assertEqual(value["request_waves"], 1)
        self.assertEqual(value["projected_p99_micros"], 64_000)
        self.assertEqual(
            receipt,
            json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
            + b"\n",
        )


if __name__ == "__main__":
    unittest.main()
