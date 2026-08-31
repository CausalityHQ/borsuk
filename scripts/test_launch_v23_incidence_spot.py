from __future__ import annotations

import hashlib
import inspect
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.launch_v23_incidence_spot import (
    EXPECTED_AWS_ACCOUNT,
    _build_source_archive,
    _maximum_compute_cost,
    _phase_policy,
    _rewrite_tree_receipt_uri,
    _spot_price,
    _validate_terminal_bytes,
    _write_bulk_manifest,
    build_launch_plan,
    build_launch_spec,
    build_worker_script,
    worker_tree,
)
from scripts.run_v23_leaf_page_incidence_falsifier import validate_phase_inputs

ROOT = Path(__file__).resolve().parent.parent
LAUNCHER = ROOT / "scripts/launch_v23_incidence_spot.py"
SOURCE_SHA = "4dfe1c0ddfff86a2c346405e3df2336b22a00920"


class V23IncidenceSpotLauncherTests(unittest.TestCase):
    def test_tree_plan_is_one_ephemeral_spot_worker_with_registered_stops(self) -> None:
        plan = build_launch_plan(
            phase="tree-training",
            run_id="fixture-run",
            source_commit=SOURCE_SHA,
        )

        self.assertEqual(plan["aws_profile"], "causality")
        self.assertEqual(plan["aws_account_id"], EXPECTED_AWS_ACCOUNT)
        self.assertEqual(plan["region"], "eu-central-1")
        self.assertEqual(plan["phase"], "tree-training")
        self.assertEqual(plan["instance_type"], "c7g.8xlarge")
        self.assertEqual(plan["purchase_option"], "spot")
        self.assertEqual(plan["instance_count"], 1)
        self.assertEqual(plan["root_volume_gib"], 200)
        self.assertTrue(plan["root_volume_delete_on_termination"])
        self.assertEqual(plan["rss_stop_bytes"], 2 << 30)
        self.assertEqual(plan["swap_delta_stop_bytes"], 256 << 20)
        self.assertEqual(plan["psi_full_immediate"], 0.79)
        self.assertEqual(plan["psi_full_sustained"], 0.50)
        self.assertEqual(plan["progress_stop_seconds"], 300)
        self.assertEqual(plan["wall_stop_seconds"], 7200)
        self.assertEqual(plan["outer_wall_stop_seconds"], 16_200)
        self.assertEqual(plan["billable_wall_stop_seconds"], 21_600)
        self.assertLessEqual(plan["maximum_compute_cost_usd"], 5.0)
        self.assertEqual(plan["source_commit"], SOURCE_SHA)
        self.assertEqual(
            plan["construction_manifest"],
            "scripts/fixtures/v23_incidence_training_manifest.json",
        )
        self.assertEqual(plan["preflight_input_count"], 1)
        self.assertEqual(plan["execute_input_count"], 59)
        self.assertFalse(plan["d3_allowed"])
        self.assertEqual(plan["supported_phases"], ["tree-training"])
        self.assertIn("posting-construction", plan["blocked_phases"])

    def test_non_tree_phases_refuse_without_committed_immutable_manifests(self) -> None:
        for phase in (
            "posting-construction",
            "development-evaluation",
            "holdout-binding",
            "holdout-evaluation",
        ):
            with self.subTest(phase=phase), self.assertRaisesRegex(
                ValueError, "immutable phase manifest"
            ):
                build_launch_plan(
                    phase=phase,
                    run_id="fixture-run",
                    source_commit=SOURCE_SHA,
                )

    def test_launch_spec_is_one_time_spot_and_self_terminating(self) -> None:
        user_data = "#!/bin/bash\nshutdown -h now\n"
        spec = build_launch_spec(
            run_id="fixture-run",
            source_commit=SOURCE_SHA,
            user_data=user_data,
        )

        self.assertEqual(spec["MinCount"], 1)
        self.assertEqual(spec["MaxCount"], 1)
        self.assertEqual(spec["UserData"], user_data)
        self.assertEqual(
            spec["ClientToken"],
            "borsuk-v23-"
            + hashlib.sha256(f"{SOURCE_SHA}:fixture-run".encode()).hexdigest()[:48],
        )
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertEqual(
            spec["InstanceMarketOptions"]["SpotOptions"],
            {
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate",
            },
        )
        self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
        self.assertEqual(spec["MetadataOptions"]["HttpTokens"], "required")
        self.assertTrue(
            spec["BlockDeviceMappings"][0]["Ebs"]["DeleteOnTermination"]
        )
        tags = {
            item["Key"]: item["Value"]
            for item in spec["TagSpecifications"][0]["Tags"]
        }
        self.assertEqual(tags["Project"], "BorsukBenchmark")
        self.assertEqual(tags["Phase"], "tree-training")
        self.assertEqual(tags["AutoTerminate"], "true")

        with self.assertRaisesRegex(ValueError, "user data length"):
            build_launch_spec(
                run_id="fixture-run",
                source_commit=SOURCE_SHA,
                user_data="#!/bin/bash\nshutdown -h now\n" + "x" * 16_384,
            )

    def test_worker_runs_preflight_then_execute_and_publishes_only_terminal_evidence(
        self,
    ) -> None:
        worker = build_worker_script(
            run_id="fixture-run",
            source_commit=SOURCE_SHA,
            source_uri="s3://borsuk-evidence/source.tar",
            source_sha256="11" * 32,
            result_uri="s3://borsuk-evidence/incidence/fixture-run",
            spot_price_usd_per_hour="0.321",
        )

        self.assertIn("set -euo pipefail", worker)
        self.assertIn("trap finish EXIT", worker)
        self.assertIn("shutdown -h now", worker)
        self.assertIn("shutdown --poweroff +360", worker)
        self.assertLess(worker.index("shutdown --poweroff +360"), worker.index("aws s3 cp"))
        self.assertIn("cargo build --locked --release", worker)
        self.assertIn("v23_leaf_page_incidence_falsifier", worker)
        self.assertIn("--worker-tree", worker)
        worker_source = inspect.getsource(worker_tree)
        self.assertLess(
            worker_source.index("preflight_policy"),
            worker_source.index("execute_policy"),
        )
        self.assertIn("MANIFEST_RELATIVE", worker_source)
        self.assertIn("run_phase(preflight_policy", worker_source)
        self.assertIn("run_phase(execute_policy", worker_source)
        self.assertIn("_stage(preflight_manifest", worker_source)
        self.assertIn("_stage(execute_manifest", worker_source)
        self.assertIn("MemoryMax=3G", worker)
        self.assertIn("MemorySwapMax=0", worker)
        self.assertIn("RuntimeMaxSec=16200", worker)
        self.assertIn(
            "scratch_root=/var/lib/borsuk-v23-incidence/scratch", worker
        )
        self.assertIn('mkdir -p "$evidence" "$scratch_root"', worker)
        self.assertIn('--setenv=TMPDIR="$scratch_root"', worker)
        self.assertIn("systemd-run --wait --collect", worker)
        self.assertIn('--working-directory="$workspace"', worker)
        self.assertIn('--setenv=PYTHONPATH="$workspace"', worker)
        self.assertIn(
            'find target -type f -path \'*/release/examples/'
            'v23_leaf_page_incidence_falsifier\' -perm -0100 -print -quit '
            '2>/dev/null || true',
            worker,
        )
        self.assertNotIn("find /data/target target", worker)
        self.assertIn("--namespace-probe", worker)
        self.assertLess(worker.index("--namespace-probe"), worker.index("--worker-tree"))
        self.assertIn("spot/instance-action", worker)
        self.assertIn("systemctl stop", worker)
        self.assertIn("ATTEMPT_COMPLETE.json", worker)
        self.assertIn("ATTEMPT_FAILED.json", worker)
        self.assertIn("publish_status=0", worker)
        self.assertIn("if [[ \"$publish_status\" -eq 0 ]]", worker)
        self.assertNotIn("phase-resource.json", worker)
        self.assertNotIn("MemoryPeak", worker)
        self.assertIn("ATTEMPT_INTERRUPTED.json", worker)
        self.assertIn("interruption-monitor-failed.json", worker)
        self.assertIn("incidence-executable", worker)
        self.assertIn("--output-uri-prefix \"$result_uri\"", worker)
        self.assertIn("--if-none-match '*'", worker)
        self.assertIn("--generate-cli-skeleton input", worker)
        self.assertLess(
            worker.index("--generate-cli-skeleton input"),
            worker.index('aws s3 cp "$source_uri"'),
        )
        self.assertNotIn("D3", worker)
        self.assertNotIn("production_bench", worker)
        self.assertNotIn("holdout-evaluation", worker)

        heredocs = []
        lines = worker.splitlines()
        for index, line in enumerate(lines):
            if line.endswith("<<'PY'"):
                end = lines.index("PY", index + 1)
                heredocs.append("\n".join(lines[index + 1 : end]) + "\n")
        self.assertGreaterEqual(len(heredocs), 3)
        for ordinal, program in enumerate(heredocs):
            with self.subTest(heredoc=ordinal):
                compile(program, f"worker-heredoc-{ordinal}", "exec")

    def test_bulk_manifest_generator_is_canonical_and_mode_exact(self) -> None:
        source = ROOT / "scripts/fixtures/v23_incidence_training_manifest.json"
        source_value = json.loads(source.read_bytes())
        with tempfile.TemporaryDirectory() as directory:
            preflight = Path(directory) / "preflight.json"
            execute = Path(directory) / "execute.json"
            _write_bulk_manifest(source, preflight, False)
            _write_bulk_manifest(source, execute, True)

            preflight_value = json.loads(preflight.read_bytes())
            execute_value = json.loads(execute.read_bytes())
            self.assertEqual(
                preflight_value["ordered_inputs"],
                [source_value["ordered_inputs"][1]],
            )
            self.assertEqual(execute_value, source_value)
            self.assertEqual(
                preflight.read_bytes(),
                json.dumps(
                    preflight_value, sort_keys=True, separators=(",", ":")
                ).encode()
                + b"\n",
            )
            self.assertNotEqual(
                hashlib.sha256(preflight.read_bytes()).hexdigest(),
                hashlib.sha256(execute.read_bytes()).hexdigest(),
            )

    def test_policy_builder_registers_distinct_manifest_roles_and_runtime_closure(
        self,
    ) -> None:
        source = (ROOT / "scripts/fixtures/v23_incidence_training_manifest.json").resolve()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bulk = root / "execute-manifest.json"
            receipt = root / "staging-receipt.json"
            staging = root / "staging"
            scratch = root / "scratch"
            output = root / "output"
            _write_bulk_manifest(source, bulk, True)
            receipt.write_text("{}\n", encoding="utf-8")
            staging.mkdir()
            scratch.mkdir()
            output.mkdir()
            binary = Path("/bin/true").resolve()
            binary_sha = hashlib.sha256(binary.read_bytes()).hexdigest()

            policy = _phase_policy(
                binary=binary,
                binary_sha256=binary_sha,
                manifest=source,
                bulk_manifest=bulk,
                staging=staging,
                staging_receipt=receipt,
                scratch=scratch,
                output=output,
                preflight_receipt=None,
            )
            validate_phase_inputs(policy)
            manifests = policy.inputs[:2]
            self.assertEqual(
                [mount.role for mount in manifests],
                ["construction-manifest", "bulk-manifest"],
            )
            self.assertNotEqual(manifests[0].source, manifests[1].source)
            self.assertNotEqual(manifests[0].uri, manifests[1].uri)

            preflight_receipt = root / "preflight-receipt.json"
            preflight_receipt.write_text("{}\n", encoding="utf-8")
            execute_policy = _phase_policy(
                binary=binary,
                binary_sha256=binary_sha,
                manifest=source,
                bulk_manifest=bulk,
                staging=staging,
                staging_receipt=receipt,
                scratch=scratch,
                output=output,
                preflight_receipt=preflight_receipt,
            )
            validate_phase_inputs(execute_policy)
            self.assertIsNone(execute_policy.parent_receipt_sha256)
            self.assertEqual(
                [mount.role for mount in execute_policy.inputs][-1],
                "preflight-receipt",
            )

    def test_source_archive_contains_exact_commit_marker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "source.tar"
            digest = _build_source_archive(SOURCE_SHA, archive)
            self.assertRegex(digest, r"^[0-9a-f]{64}$")
            marker = subprocess.run(
                ["tar", "-xOf", str(archive), ".borsuk-source-commit"],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(marker.stdout, SOURCE_SHA)

    def test_terminal_marker_is_canonical_and_bound_to_attempt(self) -> None:
        value = {
            "claim_eligible": False,
            "phase": "tree-training",
            "run_id": "fixture-run",
            "schema": "borsuk-v23-incidence-attempt-failed-v1",
            "source_archive_sha256": "11" * 32,
            "source_commit": SOURCE_SHA,
            "status": "failed",
            "worker_exit": 1,
        }
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        self.assertEqual(
            _validate_terminal_bytes(raw, "fixture-run", SOURCE_SHA), "failed"
        )
        for mutation in (
            raw[:-1],
            raw.replace(b"fixture-run", b"other-run"),
            raw.replace(SOURCE_SHA.encode(), ("f" * 40).encode()),
            raw.replace(b'"claim_eligible":false', b'"claim_eligible":true'),
        ):
            with self.subTest(mutation=mutation[:100]), self.assertRaises(ValueError):
                _validate_terminal_bytes(mutation, "fixture-run", SOURCE_SHA)

        interrupted = {
            "claim_eligible": False,
            "phase": "tree-training",
            "run_id": "fixture-run",
            "schema": "borsuk-v23-incidence-attempt-interrupted-v1",
            "source_archive_sha256": "11" * 32,
            "source_commit": SOURCE_SHA,
            "status": "interrupted",
        }
        interrupted_raw = (
            json.dumps(interrupted, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        self.assertEqual(
            _validate_terminal_bytes(interrupted_raw, "fixture-run", SOURCE_SHA),
            "interrupted",
        )

        complete = {
            "binary": {"encoded_bytes": 1, "sha256": "22" * 32},
            "claim_eligible": False,
            "incidence_tree": {"encoded_bytes": 1, "sha256": "33" * 32},
            "phase": "tree-training",
            "purchase_option": "spot",
            "receipt": {"encoded_bytes": 1, "sha256": "44" * 32},
            "run_id": "fixture-run",
            "schema": "borsuk-v23-incidence-attempt-complete-v1",
            "source_archive_sha256": "11" * 32,
            "source_commit": SOURCE_SHA,
            "spot_price_usd_per_hour": "nan",
            "status": "complete",
        }
        complete_raw = (
            json.dumps(complete, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        with self.assertRaisesRegex(ValueError, "Spot price"):
            _validate_terminal_bytes(complete_raw, "fixture-run", SOURCE_SHA)

    def test_spot_price_is_scoped_to_the_registered_subnet_zone(self) -> None:
        with patch(
            "scripts.launch_v23_incidence_spot._aws",
            side_effect=["eu-central-1a", "0.321"],
        ) as aws:
            self.assertEqual(_spot_price(), "0.321")
        self.assertIn("describe-subnets", aws.call_args_list[0].args[0])
        price_arguments = aws.call_args_list[1].args[0]
        self.assertIn("describe-spot-price-history", price_arguments)
        self.assertEqual(
            price_arguments[price_arguments.index("--availability-zone") + 1],
            "eu-central-1a",
        )
        self.assertEqual(str(_maximum_compute_cost("0.321")), "1.926")
        with self.assertRaisesRegex(ValueError, "Spot price"):
            _maximum_compute_cost("not-a-price")

    def test_tree_receipt_is_rewritten_to_the_immutable_handoff_uri(self) -> None:
        import blake3

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tree = root / "incidence-tree-a.bin"
            receipt = root / "receipt.json"
            tree.write_bytes(b"tree")
            digest = blake3.blake3(b"tree").hexdigest()
            value = {
                "outputs": [
                    {
                        "digest": digest,
                        "digest_algorithm": "blake3",
                        "encoded_bytes": 4,
                        "generation": "content-" + digest,
                        "role": "incidence-tree",
                        "uri": f"file://{tree}",
                    }
                ]
            }
            receipt.write_bytes(
                json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            )
            _rewrite_tree_receipt_uri(
                receipt,
                tree,
                "s3://borsuk-evidence/incidence/source/run/incidence-tree.bin",
            )
            rewritten = json.loads(receipt.read_bytes())
            self.assertEqual(
                rewritten["outputs"][0]["uri"],
                "s3://borsuk-evidence/incidence/source/run/incidence-tree.bin",
            )
            self.assertEqual(rewritten["outputs"][0]["digest"], digest)
            with self.assertRaisesRegex(ValueError, "output URI"):
                _rewrite_tree_receipt_uri(
                    receipt,
                    tree,
                    "s3://borsuk-evidence/incidence/source/run/other.bin",
                )

    def test_dry_run_has_no_aws_side_effect(self) -> None:
        result = subprocess.run(
            [
                "python3",
                str(LAUNCHER),
                "--phase",
                "tree-training",
                "--run-id",
                "fixture-run",
                "--dry-run",
            ],
            cwd=ROOT,
            env={**os.environ, "BORSUK_SOURCE_COMMIT": SOURCE_SHA},
            check=True,
            capture_output=True,
            text=True,
        )
        plan = json.loads(result.stdout)
        self.assertEqual(plan["run_id"], "fixture-run")
        self.assertEqual(plan["source_commit"], SOURCE_SHA)
        self.assertEqual(plan["phase"], "tree-training")


if __name__ == "__main__":
    unittest.main()
