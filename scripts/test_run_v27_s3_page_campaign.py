import base64
import json
import unittest

from scripts.run_v27_s3_page_campaign import (
    S3LatencyProfile,
    SpotTarget,
    V27QueryEvidence,
    V27ReducedSpotPlan,
    build_v27_spot_specs,
    preflight_v27_reduced_campaign,
    project_v27_query_latency,
    run_v27_reduced_spot,
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


class V27ReducedSpotTests(unittest.TestCase):
    def plan(self) -> V27ReducedSpotPlan:
        return V27ReducedSpotPlan(
            run_id="v27-reduced-20260903T120000Z",
            source_commit="a" * 40,
            source_archive_uri="s3://bucket/v27/source.tar.zst",
            source_archive_sha256="b" * 64,
            source_archive_bytes=1_000_000,
            train_uri="s3://bucket/deep/train-00000000.parquet",
            train_sha256="c" * 64,
            train_bytes=67_160_858,
            output_prefix="s3://bucket/v27/reduced/",
            row_limit=100_000,
            roots=64,
            leaves=4_096,
            iterations=4,
            workers=8,
            page_rows=512,
        )

    def test_v27_reduced_spot_specs_are_cross_zone_spot_and_subset_only(self) -> None:
        specs = build_v27_spot_specs(
            self.plan(),
            (
                SpotTarget("eu-central-1a", "subnet-a"),
                SpotTarget("eu-central-1b", "subnet-b"),
                SpotTarget("eu-central-1c", "subnet-c"),
            ),
        )
        self.assertEqual([spec["SubnetId"] for spec in specs], ["subnet-a", "subnet-b", "subnet-c"])
        for spec in specs:
            self.assertEqual(spec["MinCount"], 1)
            self.assertEqual(spec["MaxCount"], 1)
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
            self.assertEqual(
                spec["InstanceMarketOptions"]["SpotOptions"]["InstanceInterruptionBehavior"],
                "terminate",
            )
            script = base64.b64decode(spec["UserData"]).decode()
            self.assertIn("--row-limit 100000", script)
            self.assertIn("train-00000000.parquet", script)
            self.assertNotIn("train-00000001.parquet", script)
            self.assertNotIn("aws s3 sync", script)
            self.assertIn("BUILD_COMPLETE.json", script)
            self.assertIn("/root/.cargo/bin/cargo build", script)
            self.assertNotIn("source /root/.cargo/env", script)
            self.assertIn("dnf install -y gcc gcc-c++ python3 tar zstd", script)
            self.assertLess(
                script.index("python3 - \"$root/COMPLETE.json\""),
                script.rindex("put_once \"$root/worker.log\" worker.log"),
            )

    def test_v27_reduced_spot_falls_across_capacity_and_terminates_original(self) -> None:
        plan = self.plan()
        launches: list[str] = []
        terminated: list[str] = []
        sleeps: list[int] = []
        terminal = iter([None, None, b'{"status":"complete"}\n'])
        health = iter(
            [
                ("pending", "not-applicable", "not-applicable"),
                ("running", "ok", "ok"),
            ]
        )

        def launch(spec: dict[str, object]) -> str:
            launches.append(str(spec["SubnetId"]))
            if spec["SubnetId"] == "subnet-a":
                raise RuntimeError("InsufficientInstanceCapacity")
            return "i-0123456789abcdef0"

        result = run_v27_reduced_spot(
            plan,
            targets=(
                SpotTarget("eu-central-1a", "subnet-a"),
                SpotTarget("eu-central-1b", "subnet-b"),
            ),
            launch=launch,
            terminal=lambda: next(terminal),
            health=lambda _instance: next(health),
            sleep=lambda seconds: sleeps.append(seconds),
            terminate=lambda instance: terminated.append(instance),
        )
        self.assertEqual(result, b'{"status":"complete"}\n')
        self.assertEqual(launches, ["subnet-a", "subnet-b"])
        self.assertEqual(sleeps, [30, 30])
        self.assertEqual(terminated, ["i-0123456789abcdef0"])

    def test_v27_reduced_spot_failure_receipt_never_advances(self) -> None:
        terminated: list[str] = []
        with self.assertRaisesRegex(RuntimeError, "failed"):
            run_v27_reduced_spot(
                self.plan(),
                targets=(
                    SpotTarget("eu-central-1a", "subnet-a"),
                    SpotTarget("eu-central-1b", "subnet-b"),
                ),
                launch=lambda _spec: "i-0123456789abcdef0",
                terminal=lambda: b'{"status":"failed"}\n',
                health=lambda _instance: ("running", "ok", "ok"),
                sleep=lambda _seconds: None,
                terminate=lambda instance: terminated.append(instance),
            )
        self.assertEqual(terminated, ["i-0123456789abcdef0"])


if __name__ == "__main__":
    unittest.main()
