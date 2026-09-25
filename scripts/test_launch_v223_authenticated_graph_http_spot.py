import hashlib
import io
import json
import unittest

from scripts.launch_v223_authenticated_graph_http_spot import (
    ARTIFACTS, ROOT_SHA, TERMINAL_SCHEMA, bootstrap, spot_request, summaries, terminal,
)


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, *, Key, **_):
        return {"Body": io.BytesIO(self.objects[Key])}


class V223Test(unittest.TestCase):
    def test_summary_readback_requires_terminal_digest(self):
        prefix = "research/v223/run"
        raw = json.dumps({"pass_label": "first_pass", "p50_ns": 1, "p90_ns": 2,
                          "p95_ns": 3, "p99_ns": 4, "qps": 5,
                          "raw_sha256": "0" * 64}).encode()
        objects = {f"{prefix}/client/artifacts/{label}.summary.json": raw
                   for label in ("first", "repeat")}
        identity = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        receipt = {"artifacts": {f"{label}.summary.json": identity
                                 for label in ("first", "repeat")}}
        self.assertEqual(len(summaries(FakeS3(objects), prefix, receipt)), 2)
        objects[f"{prefix}/client/artifacts/repeat.summary.json"] = b"changed"
        with self.assertRaisesRegex(ValueError, "repeat.summary.json differs"):
            summaries(FakeS3(objects), prefix, receipt)

    def test_two_roles_have_plain_bootstraps_and_peer_port(self):
        for role in ("server", "client"):
            script = bootstrap(role, "0" * 40, "1" * 64, "source.tar.gz",
                               "research/v223/run", "10.0.0.1", "i-123")
            self.assertTrue(script.startswith("#!/bin/bash\n"))
            self.assertIn("v223-" + role + "-hard-stop", script)
            self.assertIn("BORSUK_V223_ARCHIVE_SHA='" + "1" * 64 + "'", script)
            self.assertIn("BORSUK_V223_ROOT_SHA='" + ROOT_SHA + "'", script)
            spec = spot_request(role, "research/v223/run", script)
            self.assertEqual(spec["UserData"], script)
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(spec["NetworkInterfaces"][0]["Groups"],
                             ["sg-0b1fd3e4fbde4af0d"])

    def test_decoded_delta_bootstrap_is_explicit(self):
        script = bootstrap("server", "0" * 40, "1" * 64, "source.tar.gz",
                           "research/v233/run", mutation_stride=100,
                           delta_encoding="decoded")
        self.assertIn("BORSUK_V223_MUTATION_STRIDE='100'", script)
        self.assertIn("BORSUK_V223_DELTA_ENCODING='decoded'", script)

    def test_client_terminal_replays_all_artifacts_and_seals(self):
        prefix = "research/v223/run"
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
                          "generation_root_sha256": ROOT_SHA,
                          "artifacts": artifacts}).encode()
        result = terminal(FakeS3(objects), "client", prefix, raw,
                          "i-123", "0" * 40, "1" * 64)
        self.assertEqual(result["terminal_sha256"], hashlib.sha256(raw).hexdigest())
        objects[f"{prefix}/client/sealed/first.raw.jsonl"] = b"wrong"
        with self.assertRaisesRegex(ValueError, "sealed first raw differs"):
            terminal(FakeS3(objects), "client", prefix, raw,
                     "i-123", "0" * 40, "1" * 64)

    def test_failed_terminal_keeps_partial_evidence(self):
        prefix = "research/v223/run"
        name = "run-closed.log"
        body = b"request failed"
        raw = json.dumps({"schema": TERMINAL_SCHEMA, "role": "client",
                          "status": "failed", "exit_code": 94,
                          "instance_id": "i-123", "source_commit": "0" * 40,
                          "source_archive_sha256": "1" * 64,
                          "generation_root_sha256": ROOT_SHA,
                          "artifacts": {name: {"bytes": len(body),
                              "sha256": hashlib.sha256(body).hexdigest()}}}).encode()
        result = terminal(FakeS3({f"{prefix}/client/artifacts/{name}": body}),
                          "client", prefix, raw, "i-123", "0" * 40, "1" * 64)
        self.assertEqual(result["status"], "failed")


if __name__ == "__main__":
    unittest.main()
