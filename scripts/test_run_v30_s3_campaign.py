import json
import unittest

from scripts.run_v30_s3_campaign import (
    SpotTarget,
    V30ConstructionPlan,
    V30EvaluationPlan,
    V30Observation,
    build_v30_construction_spot_specs,
    build_v30_corpus_manifest,
    build_v30_evaluation_spot_specs,
    monitor_v30_original_attempt,
)


class V30SpotCampaignTests(unittest.TestCase):
    def test_v30_corpus_manifest_selects_only_a_query_blind_training_prefix(self) -> None:
        source = {
            "dataset_id": "deep-image-96",
            "ordered_inputs": [
                {
                    "authority_kind": "dataset-meta",
                    "identity": {"role": "dataset-meta"},
                },
                {
                    "authority_kind": "training-shard",
                    "dimensions": 96,
                    "identity": {
                        "digest": "a" * 64,
                        "digest_algorithm": "sha256",
                        "encoded_bytes": 67_160_858,
                        "role": "training-shard-0000",
                        "uri": "s3://authority/train-00000000.parquet",
                    },
                    "ordinal_end": 174_762,
                    "ordinal_start": 0,
                    "physical_schema": "emb:fixed-size-list<element:f32;96>:non-null",
                    "rows": 174_762,
                },
                {
                    "authority_kind": "query",
                    "identity": {"role": "test"},
                },
            ],
        }
        payload = build_v30_corpus_manifest(
            (json.dumps(source, separators=(",", ":"), sort_keys=True) + "\n").encode(),
            expected_rows=100_000,
        )
        self.assertEqual(
            json.loads(payload),
            {
                "dataset_id": "deep-image-96",
                "schema_version": 1,
                "shards": [
                    {
                        "encoded_bytes": 67_160_858,
                        "physical_row_count": 174_762,
                        "row_count": 100_000,
                        "row_start": 0,
                        "sha256": "a" * 64,
                        "uri": "s3://authority/train-00000000.parquet",
                    }
                ],
                "source_rows": 100_000,
            },
        )
        self.assertEqual(payload[-1:], b"\n")
        self.assertNotIn(b"query", payload)
    def targets(self) -> tuple[SpotTarget, ...]:
        return (
            SpotTarget(
                "eu-central-1a", "subnet-a", "r7g.8xlarge", "ami-a", "sg-a", "profile-a"
            ),
            SpotTarget(
                "eu-central-1b", "subnet-b", "r7g.8xlarge", "ami-b", "sg-b", "profile-b"
            ),
        )

    def construction(self) -> V30ConstructionPlan:
        return V30ConstructionPlan(
            attempt_id="v30-deep-10m-build-a0001",
            source_commit="a" * 40,
            source_archive_uri="s3://authority/source.tar.zst",
            source_archive_sha256="b" * 64,
            source_archive_bytes=1_000_000,
            corpus_manifest_uri="s3://authority/deep-10m/corpus.json",
            corpus_manifest_sha256="c" * 64,
            corpus_manifest_bytes=4_000,
            output_prefix="s3://authority/v30/build-a0001/",
            expected_rows=9_990_000,
            roots=1_024,
            leaves=32_768,
            training_rows=262_144,
            page_rows=512,
        )

    def evaluation(self) -> V30EvaluationPlan:
        return V30EvaluationPlan(
            attempt_id="v30-deep-10m-eval-a0001",
            source_commit="a" * 40,
            source_archive_uri="s3://authority/source.tar.zst",
            source_archive_sha256="b" * 64,
            source_archive_bytes=1_000_000,
            construction_manifest_uri="s3://authority/v30/build-a0001/manifest.json",
            construction_manifest_sha256="d" * 64,
            construction_manifest_bytes=8_000,
            query_uri="s3://authority/deep-10m/test.parquet",
            query_sha256="e" * 64,
            query_bytes=1_500_000,
            truth_uri="s3://authority/deep-10m/neighbors.parquet",
            truth_sha256="f" * 64,
            truth_bytes=500_000,
            page_s3_prefix="s3://authority/v30/build-a0001/pages",
            output_prefix="s3://authority/v30/eval-a0001/",
            source_rows=9_990_000,
            query_start=64,
            query_count=32,
        )

    def reduced_construction(self) -> V30ConstructionPlan:
        return V30ConstructionPlan(
            attempt_id="v30-deep-100k-build-a0001",
            source_commit="a" * 40,
            source_archive_uri="s3://authority/source.tar.zst",
            source_archive_sha256="b" * 64,
            source_archive_bytes=1_000_000,
            corpus_manifest_uri="s3://authority/deep-100k/corpus.json",
            corpus_manifest_sha256="c" * 64,
            corpus_manifest_bytes=4_000,
            output_prefix="s3://authority/v30/build-100k-a0001/",
            expected_rows=100_000,
            roots=16,
            leaves=256,
            training_rows=8_192,
            page_rows=512,
        )

    def test_v30_campaign_separates_query_blind_construction_from_evaluation(self) -> None:
        reduced = build_v30_construction_spot_specs(
            self.reduced_construction(), self.targets()
        )
        self.assertIn("--expected-rows 100000", reduced[0]["UserData"])
        self.assertIn("--roots 16", reduced[0]["UserData"])
        specs = build_v30_construction_spot_specs(self.construction(), self.targets())
        self.assertEqual([spec["Placement"]["AvailabilityZone"] for spec in specs], ["eu-central-1a", "eu-central-1b"])
        for spec in specs:
            script = spec["UserData"]
            self.assertIn("v30_s3_build", script)
            self.assertIn("s3://authority/deep-10m/corpus.json", script)
            self.assertIn("--training-rows 262144", script)
            self.assertNotIn("test.parquet", script)
            self.assertNotIn("neighbors.parquet", script)
            self.assertNotIn("v30_s3_qualify", script)
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
            self.assertIn(
                "install -D -m 0555 target/release/examples/v30_s3_build /opt/borsuk/v30_s3_build",
                script,
            )

        evaluation = build_v30_evaluation_spot_specs(self.evaluation(), self.targets())
        for spec in evaluation:
            script = spec["UserData"]
            self.assertIn("v30_s3_qualify", script)
            self.assertIn("run_v30_untouched_quality.py", script)
            self.assertIn("test.parquet", script)
            self.assertIn("neighbors.parquet", script)
            self.assertIn("--s3-page-prefix s3://authority/v30/build-a0001/pages", script)
            self.assertIn("--query-start 64", script)
            self.assertIn("--query-count 32", script)
            self.assertNotIn("--construction-manifest-s3", script)
            self.assertNotIn("corpus.json", script)
            self.assertNotIn("v30_s3_build", script)
            self.assertIn(
                "install -D -m 0555 target/release/examples/v30_s3_qualify /opt/borsuk/v30_s3_qualify",
                script,
            )
            self.assertIn("uv venv --python 3.12 /opt/borsuk/venv", script)
            self.assertIn("/opt/borsuk/venv/bin/python", script)
            self.assertNotIn("python3 -m pip install", script)

    def test_v30_evaluation_page_namespace_is_derived_from_manifest(self) -> None:
        plan = self.evaluation()
        drifted = V30EvaluationPlan(
            **{
                **plan.__dict__,
                "page_s3_prefix": "s3://authority/v30/different-build/pages",
            }
        )
        with self.assertRaisesRegex(ValueError, "evaluation authority"):
            build_v30_evaluation_spot_specs(drifted, self.targets())

    def test_v30_campaign_preserves_one_original_terminal_and_always_terminates(self) -> None:
        launched: list[str] = []
        sleeps: list[int] = []
        terminated: list[str] = []
        observations = iter(
            [
                V30Observation("running", "ok", "ok", 1_000, 0.0, 0, 1, None),
                V30Observation("running", "ok", "ok", 2_000, 0.0, 0, 2, None),
                V30Observation(
                    "stopped",
                    "ok",
                    "ok",
                    2_000,
                    0.0,
                    0,
                    3,
                    b'{"claim_eligible":false,"source_rows":9990000,"status":"passed"}\n',
                ),
            ]
        )
        terminal = monitor_v30_original_attempt(
            launch=lambda spec: launched.append(spec["Placement"]["AvailabilityZone"])
            or "i-original",
            specs=build_v30_construction_spot_specs(self.construction(), self.targets()),
            observe=lambda _instance: next(observations),
            terminate=terminated.append,
            sleep=sleeps.append,
            wall_observations=10,
            rss_limit_bytes=192 * 1024**3,
        )
        self.assertEqual(launched, ["eu-central-1a"])
        self.assertEqual(sleeps, [30, 30])
        self.assertEqual(terminated, ["i-original"])
        self.assertEqual(
            terminal,
            b'{"claim_eligible":false,"source_rows":9990000,"status":"passed"}\n',
        )

    def test_v30_campaign_falls_across_capacity_zones_before_one_original(self) -> None:
        # Break caught: one unavailable AZ aborts the campaign even though an
        # independently registered causality Spot target has capacity.
        launched: list[str] = []
        terminated: list[str] = []

        def launch(spec: dict[str, object]) -> str:
            zone = spec["Placement"]["AvailabilityZone"]
            launched.append(zone)
            if zone == "eu-central-1a":
                raise RuntimeError("InsufficientInstanceCapacity")
            return "i-original"

        terminal = monitor_v30_original_attempt(
            launch=launch,
            specs=build_v30_construction_spot_specs(
                self.reduced_construction(), self.targets()
            ),
            observe=lambda _instance: V30Observation(
                "stopped",
                "ok",
                "ok",
                2_000,
                0.0,
                0,
                3,
                b'{"claim_eligible":false,"status":"passed"}\n',
            ),
            terminate=terminated.append,
            sleep=lambda _seconds: None,
            wall_observations=1,
            rss_limit_bytes=192 * 1024**3,
        )
        self.assertEqual(launched, ["eu-central-1a", "eu-central-1b"])
        self.assertEqual(terminated, ["i-original"])
        self.assertEqual(terminal, b'{"claim_eligible":false,"status":"passed"}\n')

    def test_v30_campaign_rejects_failed_health_and_terminal_without_retry(self) -> None:
        launched: list[str] = []
        terminated: list[str] = []
        with self.assertRaisesRegex(RuntimeError, "health"):
            monitor_v30_original_attempt(
                launch=lambda spec: launched.append(spec["Placement"]["AvailabilityZone"])
                or "i-original",
                specs=build_v30_construction_spot_specs(self.construction(), self.targets()),
                observe=lambda _instance: V30Observation(
                    "running", "impaired", "ok", 1_000, 0.0, 0, 1, None
                ),
                terminate=terminated.append,
                sleep=lambda _seconds: None,
                wall_observations=10,
                rss_limit_bytes=192 * 1024**3,
            )
        self.assertEqual(launched, ["eu-central-1a"])
        self.assertEqual(terminated, ["i-original"])

        with self.assertRaisesRegex(RuntimeError, "resource"):
            monitor_v30_original_attempt(
                launch=lambda _spec: "i-original",
                specs=build_v30_evaluation_spot_specs(self.evaluation(), self.targets()),
                observe=lambda _instance: V30Observation(
                    "running", "ok", "ok", 3 * 1024**3 + 1, 0.0, 0, 1, None
                ),
                terminate=lambda _instance: None,
                sleep=lambda _seconds: None,
                wall_observations=10,
                rss_limit_bytes=3 * 1024**3,
            )


if __name__ == "__main__":
    unittest.main()
