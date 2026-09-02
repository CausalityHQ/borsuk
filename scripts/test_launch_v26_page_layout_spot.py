import unittest

from scripts.launch_v26_page_layout_spot import (
    build_v26_spot_plan,
    run_v26_spot_phase,
)


class _EC2:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.requests: list[dict] = []
        self.terminated: list[str] = []

    def run_instances(self, **request):  # noqa: ANN003
        self.requests.append(request)
        if len(self.requests) <= self.failures:
            raise RuntimeError("capacity")
        return {"Instances": [{"InstanceId": "i-v26-fixture"}]}

    def terminate_instances(self, *, InstanceIds: list[str]):  # noqa: N803
        self.terminated.extend(InstanceIds)


class V26PageLayoutSpotTests(unittest.TestCase):
    def test_v26_plan_is_causality_spot_only_multi_az_and_always_terminates(self) -> None:
        plan = build_v26_spot_plan(
            profile="causality",
            image_id="ami-v26",
            instance_type="c7i.4xlarge",
            subnet_ids=("subnet-a", "subnet-b", "subnet-c"),
        )
        self.assertEqual(plan.profile, "causality")
        self.assertEqual(plan.subnet_ids, ("subnet-a", "subnet-b", "subnet-c"))
        self.assertEqual(plan.market_options["MarketType"], "spot")
        self.assertEqual(
            plan.market_options["SpotOptions"],
            {
                "InstanceInterruptionBehavior": "terminate",
                "SpotInstanceType": "one-time",
            },
        )

        ec2 = _EC2(failures=1)
        with self.assertRaisesRegex(RuntimeError, "terminal fixture"):
            run_v26_spot_phase(
                ec2,
                plan,
                lambda _instance_id: (_ for _ in ()).throw(
                    RuntimeError("terminal fixture")
                ),
            )
        self.assertEqual(
            [request["SubnetId"] for request in ec2.requests],
            ["subnet-a", "subnet-b"],
        )
        self.assertEqual(ec2.terminated, ["i-v26-fixture"])

    def test_v26_malformed_launch_never_leaves_an_instance_running(self) -> None:
        class MalformedEC2(_EC2):
            def run_instances(self, **request):  # noqa: ANN003
                self.requests.append(request)
                return {
                    "Instances": [
                        {"InstanceId": "i-v26-first"},
                        {"InstanceId": "i-v26-second"},
                    ]
                }

        plan = build_v26_spot_plan(
            profile="causality",
            image_id="ami-v26",
            instance_type="c7i.4xlarge",
            subnet_ids=("subnet-a",),
        )
        ec2 = MalformedEC2()
        with self.assertRaises(RuntimeError):
            run_v26_spot_phase(ec2, plan, lambda _instance_id: None)
        self.assertEqual(ec2.terminated, ["i-v26-first", "i-v26-second"])


if __name__ == "__main__":
    unittest.main()
