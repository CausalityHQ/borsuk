"""Focused 1M Spot launch contract tests; no AWS requests."""

import unittest
from types import SimpleNamespace

from scripts.launch_v114_1m_paired_spot import Plan, _reserve, launch_spec, user_data


class PairedOneMillionLaunchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = Plan(
            source_commit="a" * 40,
            archive_uri="s3://example/research/v114/" + "a" * 40 + "/source.tar.gz",
            archive_sha256="b" * 64,
            archive_bytes=1234,
            output_prefix="s3://example/research/v114/" + "a" * 40 + "/runs/a0001",
        )

    def test_spot_contract_and_source_only_order(self) -> None:
        spec = launch_spec(self.plan, "eu-central-1c", "subnet-test")
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertEqual(spec["InstanceType"], "c7i.12xlarge")
        self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
        self.assertGreaterEqual(spec["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"], 100)
        script = user_data(self.plan)
        self.assertIn("V114_SOURCE_URI", script)
        self.assertIn("V114_QUERIES_URI", script)
        self.assertIn("run_v114_1m_paired_remote.sh", script)
        self.assertIn("source.tar.gz", script)
        self.assertIn(self.plan.archive_sha256, script)
        self.assertIn("sha256sum -c -", script)
        self.assertIn("terminal.json", script)

    def test_immutable_reservation_uses_if_none_match(self) -> None:
        class Missing(Exception):
            response = {"Error": {"Code": "404"}}

        class Events:
            callback = None
            def register_first(self, _event, callback, *, unique_id):
                self.callback = callback
            def unregister(self, _event, *, unique_id):
                self.callback = None

        class S3:
            def __init__(self):
                self.meta = SimpleNamespace(events=Events())
                self.keys = set()
                self.headers = None
            def head_object(self, *, Bucket, Key):
                if Key not in self.keys:
                    raise Missing()
            def put_object(self, *, Bucket, Key, Body, ContentType):
                request = SimpleNamespace(headers={})
                self.meta.events.callback(request)
                self.headers = request.headers
                self.keys.add(Key)

        s3 = S3()
        _reserve(s3, self.plan, "example", "research/v114/a0001")
        self.assertEqual(s3.headers["If-None-Match"], "*")
        with self.assertRaisesRegex(ValueError, "already registered"):
            _reserve(s3, self.plan, "example", "research/v114/a0001")


if __name__ == "__main__":
    unittest.main()
