import unittest

from scripts.launch_v25_qualification_spot import (
    build_v25_spot_plan,
    run_v25_spot_phase,
)


class _EC2:
    def __init__(self) -> None:
        self.terminated: list[str] = []

    def run_instances(self, **request):  # noqa: ANN003
        self.request = request
        return {"Instances": [{"InstanceId": "i-v25-fixture"}]}

    def terminate_instances(self, *, InstanceIds: list[str]):  # noqa: N803
        self.terminated.extend(InstanceIds)


class V25ContainmentSpotTests(unittest.TestCase):
    def test_plan_is_spot_only_ordered_multi_az_and_always_terminates(self) -> None:
        plan = build_v25_spot_plan(
            profile="causality",
            image_id="ami-v25",
            instance_type="c7i.4xlarge",
            subnet_ids=("subnet-a", "subnet-b"),
        )
        self.assertEqual(plan.profile, "causality")
        self.assertEqual(plan.subnet_ids, ("subnet-a", "subnet-b"))
        self.assertEqual(plan.market_options["MarketType"], "spot")
        self.assertEqual(
            plan.market_options["SpotOptions"]["InstanceInterruptionBehavior"],
            "terminate",
        )

        ec2 = _EC2()
        with self.assertRaisesRegex(RuntimeError, "terminal fixture"):
            run_v25_spot_phase(ec2, plan, lambda _instance_id: (_ for _ in ()).throw(RuntimeError("terminal fixture")))
        self.assertEqual(ec2.terminated, ["i-v25-fixture"])

    def test_malformed_launch_response_terminates_every_observed_instance(self) -> None:
        class MalformedEC2(_EC2):
            def run_instances(self, **request):  # noqa: ANN003
                self.request = request
                return {
                    "Instances": [
                        {"InstanceId": "i-v25-first"},
                        {"InstanceId": "i-v25-second"},
                    ]
                }

        plan = build_v25_spot_plan(
            profile="causality",
            image_id="ami-v25",
            instance_type="c7i.4xlarge",
            subnet_ids=("subnet-a",),
        )
        ec2 = MalformedEC2()
        with self.assertRaises(RuntimeError):
            run_v25_spot_phase(ec2, plan, lambda _instance_id: None)
        self.assertEqual(ec2.terminated, ["i-v25-first", "i-v25-second"])


if __name__ == "__main__":
    unittest.main()
