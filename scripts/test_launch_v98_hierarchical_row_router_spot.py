import io
import json
import unittest
from types import SimpleNamespace

from scripts.launch_v98_hierarchical_row_router_spot import (
    CRITIQUE_RESULT_SHA256,
    SpotTarget,
    V98SpotPlan,
    build_launch_specs,
    build_plan,
    canonical_terminal_bytes,
    claim_launched_attempt,
    derive_attempt_prefix,
    launch_one_spot,
    monitor_and_terminate,
    parse_args,
    validate_terminal_bytes,
    worker_script,
)
from scripts.v97_row_width_screen import ObjectIdentity
from scripts.v98_hierarchical_row_router import HierarchyConfig


class V98SpotLauncherTests(unittest.TestCase):
    @staticmethod
    def plan() -> V98SpotPlan:
        roles = ("source", "queries", "truth", "generation", "base", "delta")
        return build_plan(
            profile="causality",
            source_commit="1" * 40,
            source_archive=ObjectIdentity(
                uri="s3://fixture/source.tar.gz", sha256="2" * 64, bytes=100
            ),
            inputs={
                role: ObjectIdentity(
                    uri=f"s3://fixture/development-{role}",
                    sha256=f"{index + 3:x}" * 64,
                    bytes=index + 1,
                )
                for index, role in enumerate(roles)
            },
            critique_result_sha256=CRITIQUE_RESULT_SHA256,
            output_prefix="s3://fixture/v98/a0001",
            image_id="ami-fixture-x86",
            image_architecture="x86_64",
            security_group_id="sg-fixture",
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/fixture",
            targets=(
                SpotTarget("eu-central-1a", "subnet-a"),
                SpotTarget("eu-central-1b", "subnet-b"),
            ),
            spot_price_usd_per_hour_micros=480_000,
        )

    def test_plan_freezes_one_development_attempt_and_every_cap(self) -> None:
        # Break caught: an attempt retunes on protected queries, omits an
        # immutable role, or silently relaxes the hierarchy/resource contract.
        plan = self.plan()
        self.assertEqual(plan.profile, "causality")
        self.assertEqual(plan.attempt, 1)
        self.assertEqual(plan.query_count, 1_000)
        self.assertEqual(plan.bootstrap_resamples, 10_000)
        self.assertEqual(plan.critique_result_sha256, CRITIQUE_RESULT_SHA256)
        self.assertEqual(
            set(plan.inputs),
            {"source", "queries", "truth", "generation", "base", "delta"},
        )
        self.assertEqual(
            plan.config,
            HierarchyConfig(
                pages_per_root=8,
                maximum_root_groups=65_536,
                maximum_exposed_pages=4_096,
                retained_pages=1_024,
                maximum_scanned_rows=262_144,
                shortlist_rows=2_048,
                maximum_gets=32,
                maximum_bytes=16 * 1024**2,
            ),
        )
        protected = " ".join(identity.uri for identity in plan.inputs.values())
        self.assertNotIn("validation", protected)
        self.assertNotIn("holdout", protected)

    def test_prefix_and_cli_freeze_campaign_source_time_and_x86_target(self) -> None:
        # Break caught: two attempts collide, or an Arm AMI is paired with the
        # x86_64 c7i scientific worker.
        self.assertEqual(
            derive_attempt_prefix("v98-g1", "1" * 40, "20260920T120000Z"),
            "v98-g1/1111111111111111111111111111111111111111/20260920T120000Z/a0001",
        )
        plan = parse_args(
            [
                "--source-commit",
                "1" * 40,
                "--source-archive-uri",
                "s3://fixture/source.tar.gz",
                "--source-archive-sha256",
                "2" * 64,
                "--source-archive-bytes",
                "100",
                "--output-prefix",
                "s3://fixture/v98/a0001",
            ]
        )
        self.assertEqual(plan.instance_type, "c7i.8xlarge")
        self.assertEqual(plan.image_architecture, "x86_64")

    def test_worker_authenticates_six_inputs_then_publishes_terminal_last(self) -> None:
        # Break caught: partial evidence becomes visible as terminal, or the
        # remote process runs an unregistered split/configuration.
        script = worker_script(self.plan())
        for role, identity in self.plan().inputs.items():
            self.assertIn(identity.uri, script, role)
            self.assertIn(identity.sha256, script, role)
            self.assertIn(str(identity.bytes), script, role)
        for literal in (
            "V98_QUERY_COUNT=1000",
            "V98_BOOTSTRAP_RESAMPLES=10000",
            "V98_MAXIMUM_EXPOSED_PAGES=4096",
            "V98_RETAINED_PAGES=1024",
            "V98_MAXIMUM_SCANNED_ROWS=262144",
            "V98_SHORTLIST_ROWS=2048",
            "V98_MAXIMUM_GETS=32",
            "V98_MAXIMUM_BYTES=16777216",
            CRITIQUE_RESULT_SHA256,
            "result.json",
            "rescore.json",
            "resources.json",
            "terminal.json",
            "/proc/pressure/memory",
        ):
            self.assertIn(literal, script)
        self.assertLess(script.index("rescore.json"), script.rindex("terminal.json"))
        self.assertLess(
            script.rindex("terminal.json"), script.rindex("shutdown -h now")
        )
        self.assertNotIn("validation-query", script)
        self.assertNotIn("holdout", script)
        self.assertNotIn("attempt=2", script)

    def test_launch_specs_are_one_time_spot_x86_and_terminate_on_shutdown(self) -> None:
        # Break caught: the experiment uses On-Demand, persists after terminal,
        # or launches overlapping workers in multiple zones.
        specs = build_launch_specs(self.plan())
        self.assertEqual(len(specs), 2)
        for spec in specs:
            self.assertEqual(spec["ImageId"], "ami-fixture-x86")
            self.assertEqual(spec["InstanceType"], "c7i.8xlarge")
            self.assertEqual(
                spec["InstanceMarketOptions"],
                {
                    "MarketType": "spot",
                    "SpotOptions": {
                        "InstanceInterruptionBehavior": "terminate",
                        "SpotInstanceType": "one-time",
                    },
                },
            )
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
            self.assertEqual((spec["MinCount"], spec["MaxCount"]), (1, 1))

    def test_capacity_fallback_launches_only_one_original(self) -> None:
        # Break caught: multi-AZ fallback overlaps duplicate scientific cells.
        class FakeEc2:
            def __init__(self) -> None:
                self.calls = 0

            def run_instances(self, **_request):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("InsufficientInstanceCapacity")
                return {"Instances": [{"InstanceId": "i-v98"}]}

        ec2 = FakeEc2()
        self.assertEqual(launch_one_spot(self.plan(), ec2_client=ec2), "i-v98")
        self.assertEqual(ec2.calls, 2)

    def test_launch_claim_is_atomic_and_binds_every_input(self) -> None:
        # Break caught: an interrupted prefix can be launched twice or its
        # launch receipt does not bind the immutable scientific inputs.
        class FakeEvents:
            def __init__(self) -> None:
                self.handler = None

            def register_first(self, _name, handler, *, unique_id):
                self.handler = handler

            def unregister(self, _name, _unique_id):
                self.handler = None

        class FakeS3:
            def __init__(self) -> None:
                self.meta = SimpleNamespace(events=FakeEvents())
                self.request = None
                self.headers = None

            def put_object(self, **request):
                signed = SimpleNamespace(headers={})
                self.meta.events.handler(signed)
                self.headers = signed.headers
                self.request = request

        s3 = FakeS3()
        claim_launched_attempt(self.plan(), s3_client=s3, instance_id="i-v98")
        receipt = json.loads(s3.request["Body"])
        self.assertEqual(s3.headers["If-None-Match"], "*")
        self.assertEqual(receipt["instance_id"], "i-v98")
        self.assertEqual(set(receipt["inputs"]), set(self.plan().inputs))
        self.assertEqual(receipt["critique_result_sha256"], CRITIQUE_RESULT_SHA256)

    def test_terminal_is_canonical_bound_and_only_complete_is_eligible(self) -> None:
        # Break caught: a failed/interrupted/mutated receipt is promoted or a
        # complete terminal does not bind result, rescore, resources and logs.
        complete = canonical_terminal_bytes(
            self.plan(),
            instance_id="i-v98",
            status="complete",
            exit_code=0,
            evidence={
                role: ObjectIdentity(
                    f"s3://fixture/v98/{role}", str(index + 1) * 64, index + 1
                )
                for index, role in enumerate(
                    ("result", "rescore", "resources", "worker_log")
                )
            },
        )
        terminal = validate_terminal_bytes(complete, self.plan(), "i-v98")
        self.assertTrue(terminal.claim_eligible)
        self.assertEqual(terminal.status, "complete")
        failed = canonical_terminal_bytes(
            self.plan(), instance_id="i-v98", status="failed", exit_code=98, evidence={}
        )
        self.assertFalse(
            validate_terminal_bytes(failed, self.plan(), "i-v98").claim_eligible
        )
        interrupted = canonical_terminal_bytes(
            self.plan(),
            instance_id="i-v98",
            status="interrupted",
            exit_code=143,
            evidence={},
        )
        self.assertFalse(
            validate_terminal_bytes(interrupted, self.plan(), "i-v98").claim_eligible
        )
        with self.assertRaises(ValueError):
            validate_terminal_bytes(
                complete.replace(b'"attempt":1', b'"attempt":2'), self.plan(), "i-v98"
            )

    def test_monitor_reads_only_terminal_and_always_terminates_instance(self) -> None:
        # Break caught: the launcher inspects incomplete result bytes or leaves
        # a stopped/failed Spot instance alive after terminal classification.
        body = canonical_terminal_bytes(
            self.plan(), instance_id="i-v98", status="failed", exit_code=98, evidence={}
        )

        class FakeS3:
            def __init__(self) -> None:
                self.get_keys = []

            def get_object(self, *, Bucket, Key):  # noqa: N803
                self.get_keys.append((Bucket, Key))
                return {"Body": io.BytesIO(body), "ContentLength": len(body)}

        class FakeEc2:
            def __init__(self) -> None:
                self.terminated = []

            def terminate_instances(self, *, InstanceIds):  # noqa: N803
                self.terminated.extend(InstanceIds)

        s3, ec2 = FakeS3(), FakeEc2()
        terminal = monitor_and_terminate(
            self.plan(), s3_client=s3, ec2_client=ec2, instance_id="i-v98"
        )
        self.assertEqual(terminal.status, "failed")
        self.assertEqual(s3.get_keys, [("fixture", "v98/a0001/terminal.json")])
        self.assertEqual(ec2.terminated, ["i-v98"])

        class MissingS3:
            def get_object(self, **_request):
                raise RuntimeError("instance terminated without terminal marker")

        ec2 = FakeEc2()
        with self.assertRaisesRegex(RuntimeError, "without terminal"):
            monitor_and_terminate(
                self.plan(), s3_client=MissingS3(), ec2_client=ec2, instance_id="i-v98"
            )
        self.assertEqual(ec2.terminated, ["i-v98"])


if __name__ == "__main__":
    unittest.main()
