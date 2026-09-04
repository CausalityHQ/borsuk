import json
import unittest
from dataclasses import replace

from scripts.run_v32_s3_latency_preflight import (
    V32LatencyEvidence,
    V32S3LatencyProfile,
    preflight_v32_s3_latency,
    project_v32_s3_latency,
)


class V32S3LatencyPreflightTests(unittest.TestCase):
    def evidence(self, **changes: object) -> V32LatencyEvidence:
        values: dict[str, object] = {
            "terminal_sha256": "f7ca28d37e1fe1d2cc08790d7155980bdeede8b6ce8fd78faf8635373ca2641f",
            "terminal_bytes": 16_889,
            "routing_p99_ns": 8_185_812,
            "decode_rerank_p99_ns": 11_159_727,
            "get_count": 16,
            "encoded_bytes": 2_928_808,
            "recall_ppm": 1_000_000,
            "minimum_recall_ppm": 1_000_000,
            "perfect_queries": 32,
        }
        values.update(changes)
        return V32LatencyEvidence(**values)

    def profile(self, **changes: object) -> V32S3LatencyProfile:
        values: dict[str, object] = {
            "tier": "standard",
            "request_wave_p99_ns": 144_065_141,
            "parallel_gets": 16,
            "aggregate_bytes_per_second": 500_000_000,
        }
        values.update(changes)
        return V32S3LatencyProfile(**values)

    def test_v32_latency_projection_models_one_concurrent_wave_and_transfer(self) -> None:
        result = project_v32_s3_latency(self.evidence(), self.profile())
        self.assertEqual(result.request_waves, 1)
        self.assertEqual(result.get_count, 16)
        self.assertEqual(result.request_ns, 144_065_141)
        self.assertEqual(result.transfer_ns, 5_857_616)
        self.assertEqual(
            result.projected_p99_ns,
            8_185_812 + 144_065_141 + 5_857_616 + 11_159_727,
        )

    def test_v32_latency_preflight_rejects_authenticated_standard_144ms_result(self) -> None:
        with self.assertRaisesRegex(ValueError, "standard latency gate"):
            preflight_v32_s3_latency(
                self.evidence(), self.evidence(), self.profile()
            )

    def test_v32_latency_preflight_accepts_only_perfect_bounded_qualifying_profile(self) -> None:
        evidence = self.evidence(
            routing_p99_ns=2_000_000,
            decode_rerank_p99_ns=3_000_000,
        )
        profile = self.profile(
            tier="express",
            request_wave_p99_ns=1_000_000,
            aggregate_bytes_per_second=1_000_000_000,
        )
        receipt = preflight_v32_s3_latency(evidence, evidence, profile)
        value = json.loads(receipt)
        self.assertEqual(value["claim_eligible"], False)
        self.assertEqual(value["status"], "passed")
        self.assertEqual(value["tier"], "express")
        self.assertEqual(value["projected_p99_ns"], 8_928_808)
        self.assertEqual(
            receipt,
            json.dumps(
                value,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
            + b"\n",
        )

        bad = {
            "requests": self.evidence(get_count=15),
            "bytes": self.evidence(encoded_bytes=3_145_729),
            "aggregate recall": self.evidence(recall_ppm=999_999),
            "minimum recall": self.evidence(minimum_recall_ppm=999_999),
            "perfect queries": self.evidence(perfect_queries=31),
        }
        for label, changed in bad.items():
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, label):
                    preflight_v32_s3_latency(changed, changed, profile)

        registered = self.evidence()
        with self.assertRaisesRegex(ValueError, "terminal evidence"):
            preflight_v32_s3_latency(
                replace(registered, routing_p99_ns=1), registered, profile
            )

    def test_v32_latency_preflight_rejects_type_nonfinite_and_tier_drift(self) -> None:
        cases = (
            (self.evidence(routing_p99_ns=True), self.profile()),
            (self.evidence(), self.profile(request_wave_p99_ns=float("nan"))),
            (self.evidence(), self.profile(aggregate_bytes_per_second=0)),
            (self.evidence(), self.profile(parallel_gets=15)),
            (self.evidence(), self.profile(tier="fallback")),
        )
        for evidence, profile in cases:
            with self.subTest(evidence=evidence, profile=profile):
                with self.assertRaisesRegex(ValueError, "authority differs"):
                    project_v32_s3_latency(evidence, profile)


if __name__ == "__main__":
    unittest.main()
