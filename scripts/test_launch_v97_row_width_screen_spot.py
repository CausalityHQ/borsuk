import unittest
from types import SimpleNamespace

from scripts.launch_v97_row_width_screen_spot import (
    SpotTarget,
    build_launch_specs,
    build_plan,
    claim_launched_attempt,
    ensure_unstarted,
    launch_one_spot,
    parse_args,
    worker_script,
)
from scripts.v97_row_width_screen import ObjectIdentity


class V97SpotLauncherTests(unittest.TestCase):
    def plan(self):
        roles = ("source", "queries", "truth", "generation", "base", "delta")
        return build_plan(
            profile="causality",
            source_commit="1" * 40,
            source_archive=ObjectIdentity(
                uri="s3://fixture/source.tar.gz", sha256="2" * 64, bytes=100
            ),
            inputs={
                role: ObjectIdentity(
                    uri=f"s3://fixture/{role}", sha256="3" * 64, bytes=index + 1
                )
                for index, role in enumerate(roles)
            },
            critique_result_sha256="4" * 64,
            output_prefix="s3://fixture/v97/attempt-0001",
            image_id="ami-fixture",
            security_group_id="sg-fixture",
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/fixture",
            targets=(
                SpotTarget("eu-central-1a", "subnet-a"),
                SpotTarget("eu-central-1b", "subnet-b"),
            ),
        )

    def test_plan_is_one_attempt_causality_spot_and_dev_only(self) -> None:
        # Break caught: the launcher silently retries science, tunes on a
        # protected split, or omits one frozen input identity.
        plan = self.plan()
        self.assertEqual(plan.profile, "causality")
        self.assertEqual(plan.attempt, 1)
        self.assertEqual(plan.query_count, 1_000)
        self.assertEqual(plan.bootstrap_resamples, 10_000)
        self.assertEqual(set(plan.inputs), {
            "source", "queries", "truth", "generation", "base", "delta"
        })
        self.assertNotIn("validation", " ".join(value.uri for value in plan.inputs.values()))
        self.assertNotIn("holdout", " ".join(value.uri for value in plan.inputs.values()))

    def test_cli_defaults_freeze_x86_image_for_c7i_worker(self) -> None:
        # Break caught: an ARM64 AMI is paired with the x86_64 c7i worker and
        # EC2 rejects the request before an immutable attempt can launch.
        plan = parse_args([
            "--source-commit", "1" * 40,
            "--source-archive-uri", "s3://fixture/source.tar.gz",
            "--source-archive-sha256", "2" * 64,
            "--source-archive-bytes", "100",
            "--output-prefix", "s3://fixture/v97/attempt-0001",
        ])
        self.assertEqual(plan.instance_type, "c7i.8xlarge")
        self.assertEqual(plan.image_id, "ami-06121aa3085b6f918")

    def test_worker_authenticates_runs_rescores_then_publishes_terminal(self) -> None:
        # Break caught: a terminal becomes visible before result/rescore, or an
        # interrupted cell is retried/reused instead of being discarded.
        script = worker_script(self.plan())
        self.assertIn("--query-count 1000", script)
        self.assertIn("--bootstrap-resamples 10000", script)
        self.assertIn("ulimit -v $((48 * 1024 * 1024))", script)
        self.assertIn("result.json", script)
        self.assertIn("rescore.json", script)
        self.assertIn("resources.json", script)
        self.assertIn("/proc/pressure/memory", script)
        self.assertIn("Maximum resident set size", script)
        self.assertIn("terminal.json", script)
        self.assertLess(script.index("rescore.json"), script.index("terminal.json"))
        self.assertLess(script.index("terminal.json"), script.index("shutdown -h now"))
        self.assertNotIn("\n+  --", script)
        self.assertNotIn("validation", script)
        self.assertNotIn("holdout", script)

    def test_launch_specs_are_one_time_spot_and_terminate_on_shutdown(self) -> None:
        # Break caught: an on-demand or persistent worker survives the cell.
        specs = build_launch_specs(self.plan())
        self.assertEqual(len(specs), 2)
        for spec in specs:
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(
                spec["InstanceMarketOptions"]["SpotOptions"]["SpotInstanceType"],
                "one-time",
            )
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
            self.assertEqual(spec["MinCount"], 1)
            self.assertEqual(spec["MaxCount"], 1)

    def test_capacity_fallback_launches_only_one_original_instance(self) -> None:
        # Break caught: multi-AZ fallback launches overlapping scientific
        # workers instead of trying capacity serially.
        class FakeEc2:
            def __init__(self) -> None:
                self.calls = 0

            def run_instances(self, **request):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("InsufficientInstanceCapacity")
                return {"Instances": [{"InstanceId": "i-v97"}]}

        client = FakeEc2()
        self.assertEqual(launch_one_spot(self.plan(), ec2_client=client), "i-v97")
        self.assertEqual(client.calls, 2)

    def test_existing_terminal_fences_duplicate_launch(self) -> None:
        # Break caught: rerunning the launcher overwrites an immutable attempt.
        class ExistingS3:
            def head_object(self, **request):
                return {"ContentLength": 10}

        with self.assertRaisesRegex(ValueError, "already exists"):
            ensure_unstarted(self.plan(), s3_client=ExistingS3())

    def test_launch_receipt_is_conditionally_persisted_before_monitoring(self) -> None:
        # Break caught: a pre-terminal Spot interruption leaves no durable
        # consumed-attempt marker and permits an accidental duplicate.
        class FakeEvents:
            def __init__(self) -> None:
                self.handler = None

            def register_first(self, name, handler, *, unique_id):
                self.handler = handler

            def unregister(self, name, unique_id):
                self.handler = None

        class FakeS3:
            def __init__(self) -> None:
                self.request = None
                self.signed_headers = None
                self.meta = SimpleNamespace(events=FakeEvents())

            def put_object(self, **request):
                signed = SimpleNamespace(headers={})
                self.meta.events.handler(signed)
                self.signed_headers = signed.headers
                self.request = request

        client = FakeS3()
        claim_launched_attempt(self.plan(), s3_client=client, instance_id="i-v97")
        self.assertNotIn("IfNoneMatch", client.request)
        self.assertEqual(client.signed_headers["If-None-Match"], "*")
        self.assertTrue(client.request["Key"].endswith("/launch.json"))
        self.assertIn(b'"instance_id":"i-v97"', client.request["Body"])


if __name__ == "__main__":
    unittest.main()
