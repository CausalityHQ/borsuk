"""Regression for EC2's post-RunInstances describe consistency window."""

from unittest import TestCase
from unittest.mock import Mock

from botocore.exceptions import ClientError

from scripts.launch_v219_reachable_graph_1m_spot import describe_state


class DescribeStateTests(TestCase):
    def test_new_spot_instance_can_be_described_after_transient_not_found(self) -> None:
        missing = ClientError({"Error": {"Code": "InvalidInstanceID.NotFound",
                                         "Message": "not yet visible"}}, "DescribeInstances")
        ec2 = Mock()
        ec2.describe_instances.side_effect = [
            missing,
            {"Reservations": [{"Instances": [{"State": {"Name": "pending"}}]}]},
        ]
        self.assertEqual(describe_state(ec2, "i-123", retries=2, pause_seconds=0), "pending")
        self.assertEqual(ec2.describe_instances.call_count, 2)
