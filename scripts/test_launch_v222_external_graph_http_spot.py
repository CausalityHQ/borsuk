import hashlib
import io
import json
import unittest

from scripts.launch_v222_external_graph_http_spot import (
    ARTIFACTS, TERMINAL_SCHEMA, bootstrap, spot_request, terminal,
)


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, *, Key, **_):
        return {"Body": io.BytesIO(self.objects[Key])}


class V222Test(unittest.TestCase):
    def test_two_roles_have_plain_bootstraps_and_peer_port(self):
        for role in ("server", "client"):
            script = bootstrap(role, "0" * 40, "1" * 64, "source.tar.gz",
                               "research/v222/run", "10.0.0.1", "i-123")
            self.assertTrue(script.startswith("#!/bin/bash\n"))
            self.assertIn("v222-" + role + "-hard-stop", script)
            self.assertIn("BORSUK_V222_ARCHIVE_SHA='" + "1" * 64 + "'", script)
            spec = spot_request(role, "research/v222/run", script)
            self.assertEqual(spec["UserData"], script)
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(spec["NetworkInterfaces"][0]["Groups"],
                             ["sg-0b1fd3e4fbde4af0d"])

    def test_client_terminal_replays_all_artifacts_and_seals(self):
        prefix = "research/v222/run"
        objects = {f"{prefix}/client/artifacts/{name}": b"valid"
                   for name in ARTIFACTS["client"]}
        for label in ("first", "repeat"):
            objects[f"{prefix}/client/sealed/{label}.raw.jsonl"] = b"valid"
        artifacts = {name: {"bytes": 5,
                            "sha256": hashlib.sha256(b"valid").hexdigest()}
                     for name in ARTIFACTS["client"]}
        raw = json.dumps({"schema": TERMINAL_SCHEMA, "role": "client",
                          "status": "complete", "exit_code": 0,
                          "instance_id": "i-123", "source_commit": "0" * 40,
                          "source_archive_sha256": "1" * 64,
                          "artifacts": artifacts}).encode()
        result = terminal(FakeS3(objects), "client", prefix, raw,
                          "i-123", "0" * 40, "1" * 64)
        self.assertEqual(result["terminal_sha256"], hashlib.sha256(raw).hexdigest())
        objects[f"{prefix}/client/sealed/first.raw.jsonl"] = b"wrong"
        with self.assertRaisesRegex(ValueError, "sealed first raw differs"):
            terminal(FakeS3(objects), "client", prefix, raw,
                     "i-123", "0" * 40, "1" * 64)

    def test_failed_terminal_keeps_partial_evidence(self):
        prefix = "research/v222/run"
        name = "run-closed.log"
        body = b"request failed"
        raw = json.dumps({"schema": TERMINAL_SCHEMA, "role": "client",
                          "status": "failed", "exit_code": 94,
                          "instance_id": "i-123", "source_commit": "0" * 40,
                          "source_archive_sha256": "1" * 64,
                          "artifacts": {name: {"bytes": len(body),
                              "sha256": hashlib.sha256(body).hexdigest()}}}).encode()
        result = terminal(FakeS3({f"{prefix}/client/artifacts/{name}": body}),
                          "client", prefix, raw, "i-123", "0" * 40, "1" * 64)
        self.assertEqual(result["status"], "failed")


if __name__ == "__main__":
    unittest.main()
