import json
import os
import pathlib
import tempfile
import unittest

from scripts import run_v24_reduced_preflight as subject


class V24ReducedPreflightTests(unittest.TestCase):
    def test_two_process_worker_counts_emit_identical_authenticated_receipt(self) -> None:
        binary_value = os.environ.get("BORSUK_V24_BINARY")
        if binary_value is None:
            self.skipTest("BORSUK_V24_BINARY is required for direct-binary integration")
        binary = pathlib.Path(binary_value).resolve()
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            raw = subject.run_reduced_preflight(
                subject.ReducedPreflightRequest(
                    binary=binary,
                    binary_sha256=subject.sha256_file(binary),
                    binary_bytes=binary.stat().st_size,
                    root=root,
                    source_commit="1" * 40,
                    source_rows=257,
                    witness_count=32,
                    page_count=16,
                    worker_counts=(1, 4),
                )
            )
            value = json.loads(raw)
            self.assertEqual(raw, subject.canonical_json_bytes(value))
            self.assertEqual(value["schema"], "borsuk-v24-reduced-preflight-v1")
            self.assertFalse(value["claim_eligible"])
            self.assertEqual(value["worker_counts"], [1, 4])
            self.assertEqual(value["source_rows"], 257)
            self.assertEqual(value["witness_count"], 32)
            self.assertEqual(value["page_count"], 16)
            self.assertEqual(value["serving_bytes"], 1_644_167_168)
            self.assertEqual(value["runs"][0]["artifact_sha256"], value["runs"][1]["artifact_sha256"])
            self.assertEqual(
                value["runs"][0]["evaluation_evidence_sha256"],
                value["runs"][1]["evaluation_evidence_sha256"],
            )
            self.assertNotEqual(
                value["runs"][0]["development_result_sha256"], ""
            )
            for run in value["runs"]:
                cpu = run["cpu_preflight"]
                self.assertEqual(cpu["warmup_samples"], 1_024)
                self.assertEqual(cpu["timed_samples"], 10_000)
                self.assertEqual(len(cpu["selector_latency_ns"]), 10_000)
                self.assertEqual(
                    cpu["selector_p99_ns"],
                    sorted(cpu["selector_latency_ns"])[9_899],
                )
                self.assertTrue(cpu["scalar_simd_pages_equal"])
            self.assertEqual(
                {entry.name for entry in root.iterdir()},
                {"preflight-receipt.json", "worker-1", "worker-4"},
            )

    def test_cli_is_fixed_to_registered_reduced_shape(self) -> None:
        parser = subject.argument_parser()
        options = {action.dest for action in parser._actions}
        self.assertEqual(
            options,
            {
                "help",
                "binary",
                "binary_sha256",
                "binary_bytes",
                "root",
                "source_commit",
                "execute_reduced_preflight",
            },
        )
        request = subject.parse_args(
            [
                "--binary",
                "/opt/borsuk/v24_witness_page_router",
                "--binary-sha256",
                "2" * 64,
                "--binary-bytes",
                "10487832",
                "--root",
                "/opt/borsuk/preflight",
                "--source-commit",
                "1" * 40,
                "--execute-reduced-preflight",
            ]
        )
        self.assertEqual(request.source_rows, 65_536)
        self.assertEqual(request.witness_count, 4_096)
        self.assertEqual(request.page_count, 64)
        self.assertEqual(request.worker_counts, (1, 4))


if __name__ == "__main__":
    unittest.main()
