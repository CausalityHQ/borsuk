from __future__ import annotations

import dataclasses
import hashlib
import inspect
import json
import pathlib
import tempfile
import unittest

from scripts import run_v23_balanced_page_falsifier as subject


def _policy(root: pathlib.Path) -> subject.BalancedRunPolicy:
    executable = root / "v23-balanced-page-falsifier"
    executable.write_bytes(b"binary\n")
    manifest = root / "manifest.json"
    manifest.write_bytes(b"{}\n")
    inputs = root / "inputs"
    outputs = root / "outputs"
    inputs.mkdir()
    outputs.mkdir()
    return subject.BalancedRunPolicy(
        executable=executable,
        executable_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
        executable_bytes=executable.stat().st_size,
        manifest=manifest,
        manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
        manifest_bytes=manifest.stat().st_size,
        input_directory=inputs,
        output_directory=outputs,
        mode="execute",
        cleanup_paths=(manifest, executable),
    )


class BalancedOfflineWorkerTests(unittest.TestCase):
    def test_direct_command_has_no_loader_mount_network_or_aws_surface(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            policy = _policy(pathlib.Path(directory))
            self.assertEqual(
                subject.build_offline_command(policy),
                (
                    str(policy.executable),
                    "--manifest",
                    str(policy.manifest),
                    "--input-directory",
                    str(policy.input_directory),
                    "--output-directory",
                    str(policy.output_directory),
                    "--execute",
                ),
            )
            environment = subject.build_offline_environment()
            self.assertNotIn("AWS_ACCESS_KEY_ID", environment)
            self.assertNotIn("AWS_PROFILE", environment)
            source = inspect.getsource(subject)
            for forbidden in (
                "ldd",
                "ld-linux",
                "pivot_root",
                "unshare",
                "mount(",
                "boto3",
                "aws s3",
            ):
                self.assertNotIn(forbidden, source)

    def test_policy_authenticates_binary_manifest_and_empty_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            policy = _policy(pathlib.Path(directory))
            subject.validate_policy(policy)
            policy.output_directory.joinpath("stale").write_bytes(b"stale")
            with self.assertRaisesRegex(ValueError, "output directory"):
                subject.validate_policy(policy)
            policy.output_directory.joinpath("stale").unlink()
            with self.assertRaisesRegex(ValueError, "executable authority"):
                subject.validate_policy(
                    dataclasses.replace(policy, executable_sha256="00" * 32)
                )

    def test_terminal_is_exact_canonical_claim_ineligible_receipt(self) -> None:
        value = {
            "claim_eligible": False,
            "manifest_sha256": "11" * 32,
            "ordered_inputs": [{"role": "f16-control"}],
            "outputs": [],
            "pseudoquery_pairs": [],
            "schema": "borsuk-v23-balanced-page-receipt-v3",
            "selected_pair": None,
            "stop": None,
        }
        terminal = json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        self.assertEqual(subject.validate_terminal(terminal), value)
        with self.assertRaisesRegex(ValueError, "canonical"):
            subject.validate_terminal(json.dumps(value, indent=2).encode())
        changed = dict(value, claim_eligible=True)
        with self.assertRaisesRegex(ValueError, "authority"):
            subject.validate_terminal(
                json.dumps(changed, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )

    def test_cleanup_unlinks_only_explicit_regular_files_after_pid_clearance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            first = root / "first"
            second = root / "second"
            first.write_bytes(b"one")
            second.write_bytes(b"two")
            subject.cleanup_explicit_files((first, second), process_group_alive=False)
            self.assertFalse(first.exists())
            self.assertFalse(second.exists())
            nested = root / "nested"
            nested.mkdir()
            with self.assertRaisesRegex(ValueError, "regular file"):
                subject.cleanup_explicit_files((nested,), process_group_alive=False)
            with self.assertRaisesRegex(ValueError, "process group"):
                subject.cleanup_explicit_files((), process_group_alive=True)


if __name__ == "__main__":
    unittest.main()
