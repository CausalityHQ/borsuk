from __future__ import annotations

import dataclasses
import errno
import hashlib
import inspect
import json
import os
import pathlib
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch

from scripts import run_v23_leaf_page_incidence_falsifier as subject


def _mount(role: str, name: str) -> subject.AuthenticatedInput:
    algorithm = (
        "blake3"
        if role
        in {"incidence-tree", "incidence-postings-one", "incidence-postings-two"}
        or role.startswith("page-body-")
        else "sha256"
    )
    return subject.AuthenticatedInput(
        role=role,
        source=pathlib.Path("/authority") / name,
        uri=f"s3://borsuk-evidence/{role}",
        digest_algorithm=algorithm,
        digest="11" * 32,
        encoded_bytes=17,
        generation="generation-0001",
    )


def _policy(
    phase: str = "tree-training", parent_digest: str | None = None
) -> subject.OfflinePhasePolicy:
    manifest_role = (
        "construction-manifest" if phase == "tree-training" else "phase-manifest"
    )
    inputs = (
        _mount(manifest_role, f"{manifest_role}.json"),
        _mount("bulk-manifest", "bulk-manifest.json"),
        _mount("staging-receipt", "staging-receipt.json"),
        _mount("preflight-receipt", "preflight-receipt.json"),
    )
    directory_capabilities = (
        subject.AuthenticatedDirectory(
            role="bulk-inputs",
            source=pathlib.Path("/authority/bulk-inputs"),
            manifest_role="bulk-manifest",
            staging_receipt_role="staging-receipt",
        ),
    )
    policy = subject.OfflinePhasePolicy(
        phase=phase,
        executable=pathlib.Path("/opt/borsuk/v23-incidence"),
        executable_sha256="aa" * 32,
        executable_bytes=19,
        inputs=inputs,
        scratch=pathlib.Path("/scratch/v23-incidence"),
        output=pathlib.Path("/output/v23-incidence"),
        parent_receipt_sha256=parent_digest,
        directory_capabilities=directory_capabilities,
        phase_argv=(),
    )
    return dataclasses.replace(policy, phase_argv=subject.build_phase_argv(policy))


def _progress_bytes(
    *,
    phase: str = "tree-training",
    sequence: int = 0,
    completed_units: int = 0,
    total_units: int = 128,
    last_object_digest: str = "66" * 32,
    previous_progress_sha256: str | None = None,
) -> bytes:
    value = {
        "completed_units": completed_units,
        "last_object_digest": last_object_digest,
        "phase": phase,
        "previous_progress_sha256": previous_progress_sha256,
        "sequence": sequence,
        "total_units": total_units,
    }
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _write_staged_inventory(
    bulk_manifest: pathlib.Path,
    staging_receipt: pathlib.Path,
    staging: pathlib.Path,
    role: str = "training-shard-0000",
    payload: bytes = b"shard\n",
) -> None:
    staging.mkdir()
    staged = staging / role
    staged.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    identity = {
        "digest": digest,
        "digest_algorithm": "sha256",
        "encoded_bytes": len(payload),
        "generation": f"unversioned-sha256:{digest}",
        "role": role,
        "uri": f"s3://borsuk-evidence/{role}",
    }
    bulk_manifest.write_bytes(
        json.dumps(
            {"ordered_inputs": [{"identity": identity}]},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )
    staging_receipt.write_bytes(
        json.dumps(
            {
                "claim_eligible": False,
                "manifest_sha256": hashlib.sha256(
                    bulk_manifest.read_bytes()
                ).hexdigest(),
                "ordered_objects": [{**identity, "relative_path": role}],
                "schema": "borsuk-v23-incidence-staging-receipt-v1",
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


class OfflinePhasePolicyTests(unittest.TestCase):
    def test_offline_phase_contract_has_no_private_root_or_runtime_mounts(self) -> None:
        self.assertTrue(hasattr(subject, "OfflinePhasePolicy"))
        self.assertTrue(hasattr(subject, "AuthenticatedInput"))
        self.assertTrue(hasattr(subject, "AuthenticatedDirectory"))
        self.assertTrue(hasattr(subject, "build_offline_command"))
        source = inspect.getsource(subject)
        self.assertNotIn("pivot_root", source)
        self.assertNotIn("runtime_mounts", source)
        self.assertNotIn("runtime-loader", source)
        self.assertNotIn("_bind_mount", source)

    def test_offline_phase_reauthenticates_complete_staged_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            staging = root / "staging"
            staging.mkdir()
            shard = staging / "training-shard-0000"
            shard.write_bytes(b"shard\n")
            identity = {
                "digest": hashlib.sha256(shard.read_bytes()).hexdigest(),
                "digest_algorithm": "sha256",
                "encoded_bytes": shard.stat().st_size,
                "generation": "unversioned-sha256:" + hashlib.sha256(
                    shard.read_bytes()
                ).hexdigest(),
                "role": shard.name,
                "uri": "s3://borsuk-evidence/training-shard-0000",
            }
            bulk_manifest = root / "bulk-manifest.json"
            bulk_manifest.write_bytes(
                json.dumps(
                    {"ordered_inputs": [{"identity": identity}]},
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                + b"\n"
            )
            staging_receipt = root / "staging-receipt.json"
            staging_receipt.write_bytes(
                json.dumps(
                    {
                        "claim_eligible": False,
                        "manifest_sha256": hashlib.sha256(
                            bulk_manifest.read_bytes()
                        ).hexdigest(),
                        "ordered_objects": [
                            {**identity, "relative_path": shard.name}
                        ],
                        "schema": "borsuk-v23-incidence-staging-receipt-v1",
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                + b"\n"
            )
            singleton_payloads = {
                "construction-manifest": b"manifest\n",
                "bulk-manifest": bulk_manifest.read_bytes(),
                "staging-receipt": staging_receipt.read_bytes(),
                "preflight-receipt": b"preflight\n",
            }
            inputs = []
            for role, payload in singleton_payloads.items():
                path = root / role
                path.write_bytes(payload)
                inputs.append(
                    subject.AuthenticatedInput(
                        role=role,
                        source=path,
                        uri=f"file://{path}",
                        digest_algorithm="sha256",
                        digest=hashlib.sha256(payload).hexdigest(),
                        encoded_bytes=len(payload),
                        generation="fixture-v1",
                    )
                )
            executable = pathlib.Path("/bin/true").resolve()
            policy = subject.OfflinePhasePolicy(
                phase="tree-training",
                executable=executable,
                executable_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
                executable_bytes=executable.stat().st_size,
                inputs=tuple(inputs),
                scratch=root / "scratch",
                output=root / "output",
                parent_receipt_sha256=None,
                directory_capabilities=(
                    subject.AuthenticatedDirectory(
                        role="bulk-inputs",
                        source=staging,
                        manifest_role="bulk-manifest",
                        staging_receipt_role="staging-receipt",
                    ),
                ),
                phase_argv=(),
            )
            policy = dataclasses.replace(
                policy, phase_argv=subject.build_phase_argv(policy)
            )
            subject.authenticate_policy_files(policy)
            (staging / "unexpected").write_bytes(b"leak\n")
            with self.assertRaisesRegex(ValueError, "inventory"):
                subject.authenticate_policy_files(policy)

    def test_staged_inventory_rejects_coherent_concrete_type_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            bulk_manifest = root / "bulk-manifest.json"
            staging_receipt = root / "staging-receipt.json"
            staging = root / "staging"
            _write_staged_inventory(bulk_manifest, staging_receipt, staging)

            manifest = json.loads(bulk_manifest.read_bytes())
            manifest["ordered_inputs"][0]["identity"]["encoded_bytes"] = "6"
            bulk_manifest.write_bytes(
                json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            receipt = json.loads(staging_receipt.read_bytes())
            receipt["manifest_sha256"] = hashlib.sha256(
                bulk_manifest.read_bytes()
            ).hexdigest()
            receipt["ordered_objects"][0]["encoded_bytes"] = "6"
            staging_receipt.write_bytes(
                json.dumps(receipt, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )

            def authority(role: str, path: pathlib.Path) -> subject.AuthenticatedInput:
                payload = path.read_bytes()
                return subject.AuthenticatedInput(
                    role=role,
                    source=path,
                    uri=path.as_uri(),
                    digest_algorithm="sha256",
                    digest=hashlib.sha256(payload).hexdigest(),
                    encoded_bytes=len(payload),
                    generation="fixture-v1",
                )

            policy = types.SimpleNamespace(
                inputs=(
                    authority("bulk-manifest", bulk_manifest),
                    authority("staging-receipt", staging_receipt),
                )
            )
            capability = subject.AuthenticatedDirectory(
                role="bulk-inputs",
                source=staging,
                manifest_role="bulk-manifest",
                staging_receipt_role="staging-receipt",
            )
            with self.assertRaisesRegex(ValueError, "manifest"):
                subject._authenticate_staged_inventory(policy, capability)

    def test_run_phase_strips_credentials_from_offline_child_environment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            scratch = root / "scratch"
            output = root / "output"
            scratch.mkdir()
            output.mkdir()
            policy = dataclasses.replace(
                _policy(), scratch=scratch, output=output, phase_argv=()
            )
            policy = dataclasses.replace(
                policy, phase_argv=subject.build_phase_argv(policy)
            )
            process = types.SimpleNamespace(pid=12345, returncode=None)
            with (
                patch.object(subject, "_network_namespace_inode", return_value=91),
                patch.object(subject, "authenticate_policy_files"),
                patch.object(subject.subprocess, "Popen", return_value=process) as popen,
                patch.object(
                    subject, "monitor_process_group", return_value=(0, None)
                ),
            ):
                self.assertEqual(subject.run_phase(policy), 0)
            environment = popen.call_args.kwargs["env"]
            self.assertEqual(environment["HOME"], "/nonexistent")
            self.assertEqual(environment["TMPDIR"], str(scratch))
            self.assertFalse(
                any(key.startswith(("AWS_", "BOTO_")) for key in environment)
            )

    def test_run_phase_authenticates_inputs_before_and_after_science(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            scratch = root / "scratch"
            output = root / "output"
            scratch.mkdir()
            output.mkdir()
            policy = dataclasses.replace(
                _policy(), scratch=scratch, output=output, phase_argv=()
            )
            policy = dataclasses.replace(
                policy, phase_argv=subject.build_phase_argv(policy)
            )
            process = types.SimpleNamespace(pid=12345, returncode=None)
            with (
                patch.object(subject, "_network_namespace_inode", return_value=91),
                patch.object(subject, "authenticate_policy_files") as authenticate,
                patch.object(subject.subprocess, "Popen", return_value=process),
                patch.object(
                    subject, "monitor_process_group", return_value=(0, None)
                ),
            ):
                self.assertEqual(subject.run_phase(policy), 0)
            self.assertEqual(authenticate.call_count, 2)
            for call in authenticate.call_args_list:
                (bound_policy,) = call.args
                self.assertEqual(bound_policy.host_network_namespace_inode, 91)

    def test_run_phase_cleans_policy_when_input_authentication_fails(self) -> None:
        for stage in ("before", "after"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                scratch = root / "scratch"
                output = root / "output"
                scratch.mkdir()
                output.mkdir()
                policy = dataclasses.replace(
                    _policy(), scratch=scratch, output=output, phase_argv=()
                )
                policy = dataclasses.replace(
                    policy, phase_argv=subject.build_phase_argv(policy)
                )
                process = types.SimpleNamespace(pid=12345, returncode=None)
                side_effect = (
                    [ValueError("pre-authentication failed")]
                    if stage == "before"
                    else [None, ValueError("post-authentication failed")]
                )
                with (
                    patch.object(subject, "_network_namespace_inode", return_value=91),
                    patch.object(
                        subject,
                        "authenticate_policy_files",
                        side_effect=side_effect,
                    ),
                    patch.object(subject.subprocess, "Popen", return_value=process),
                    patch.object(
                        subject, "monitor_process_group", return_value=(0, None)
                    ),
                    self.assertRaisesRegex(ValueError, "authentication failed"),
                ):
                    subject.run_phase(policy)
                self.assertFalse((root / ".scratch.policy.json").exists())

    def test_scientific_entrypoint_preserves_full_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            policy = pathlib.Path(directory) / "invalid-policy.json"
            policy.write_bytes(b"{}\n")
            completed = subprocess.run(
                [
                    sys.executable,
                    str(pathlib.Path(subject.__file__).resolve()),
                    "--enter-offline-policy",
                    str(policy),
                    "--policy-sha256",
                    hashlib.sha256(policy.read_bytes()).hexdigest(),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("Traceback (most recent call last):", completed.stderr)
        self.assertIn("decode_policy_bytes", completed.stderr)

    def test_phase_argv_uses_three_authorities_and_one_bulk_directory(self) -> None:
        base = _policy()
        policy = dataclasses.replace(
            base,
            inputs=(
                _mount("construction-manifest", "construction-manifest.json"),
                _mount("bulk-manifest", "bulk-manifest.json"),
                _mount("staging-receipt", "staging-receipt.json"),
                _mount("preflight-receipt", "preflight-receipt.json"),
            ),
            directory_capabilities=(
                subject.AuthenticatedDirectory(
                    role="bulk-inputs",
                    source=pathlib.Path("/authority/training-shards"),
                    manifest_role="bulk-manifest",
                    staging_receipt_role="staging-receipt",
                ),
            ),
            phase_argv=(),
        )
        argv = subject.build_phase_argv(policy)
        self.assertLess(sum(len(argument) + 1 for argument in argv), 16_384)
        self.assertEqual(argv[0], "--execute-tree-training")
        self.assertEqual(argv.count("--manifest"), 1)
        self.assertEqual(argv.count("--bulk-manifest"), 1)
        self.assertEqual(argv.count("--staging-directory"), 1)
        self.assertEqual(argv.count("--staging-receipt"), 1)
        self.assertEqual(argv.count("--preflight-receipt"), 1)
        self.assertFalse(
            any(
                argument.startswith(("training-shard-", "page-body-"))
                for argument in argv
            )
        )
        subject.validate_phase_inputs(dataclasses.replace(policy, phase_argv=argv))

    def test_progress_requires_canonical_hash_chained_completed_work(self) -> None:
        monitor = subject.AuthenticatedProgressMonitor("tree-training")
        initial = _progress_bytes()
        initial_digest = hashlib.sha256(initial).hexdigest()
        self.assertEqual(monitor.observe(initial), (0, 0, initial_digest))

        advanced = _progress_bytes(
            sequence=1,
            completed_units=64,
            previous_progress_sha256=initial_digest,
        )
        advanced_digest = hashlib.sha256(advanced).hexdigest()
        self.assertEqual(
            monitor.observe(initial + advanced), (1, 64, advanced_digest)
        )

        second = _progress_bytes(
            sequence=2,
            completed_units=96,
            previous_progress_sha256=advanced_digest,
        )
        second_digest = hashlib.sha256(second).hexdigest()
        third = _progress_bytes(
            sequence=3,
            completed_units=128,
            previous_progress_sha256=second_digest,
        )
        third_digest = hashlib.sha256(third).hexdigest()
        catchup_monitor = subject.AuthenticatedProgressMonitor("tree-training")
        self.assertEqual(catchup_monitor.observe(initial), (0, 0, initial_digest))
        self.assertEqual(
            catchup_monitor.observe(initial + advanced + second + third),
            (3, 128, third_digest),
        )

        mutations = (
            _progress_bytes(
                sequence=2,
                completed_units=64,
                previous_progress_sha256=advanced_digest,
            ),
            _progress_bytes(
                sequence=2,
                completed_units=65,
                previous_progress_sha256="77" * 32,
            ),
            _progress_bytes(
                phase="posting-construction",
                sequence=2,
                completed_units=65,
                previous_progress_sha256=advanced_digest,
            ),
            _progress_bytes(
                sequence=2,
                completed_units=65,
                total_units=129,
                previous_progress_sha256=advanced_digest,
            ),
            advanced[:-1] + b" \n",
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation[:80]):
                with self.assertRaises(ValueError):
                    monitor.observe(initial + advanced + mutation)

        with self.assertRaises(ValueError):
            catchup_monitor.observe(initial + second + third)

    def test_bulk_directory_capabilities_replace_per_object_phase_mounts(self) -> None:
        for phase in ("tree-training", "posting-construction"):
            with self.subTest(phase=phase):
                parent = None if phase == "tree-training" else "ab" * 32
                policy = _policy(phase, parent)
                subject.validate_phase_inputs(policy)
                self.assertEqual(
                    tuple(item.role for item in policy.directory_capabilities),
                    ("bulk-inputs",),
                )
                self.assertFalse(
                    any(
                        mount.role.startswith(("training-shard-", "page-body-"))
                        for mount in policy.inputs
                    )
                )
                self.assertLess(len(subject.canonical_policy_bytes(policy)), 16_384)

                with self.assertRaisesRegex(ValueError, "directory"):
                    subject.validate_phase_inputs(
                        dataclasses.replace(policy, directory_capabilities=())
                    )
                leaked = policy.inputs + (_mount("page-body-0000", "page.bin"),)
                with self.assertRaisesRegex(ValueError, "phase input"):
                    subject.validate_phase_inputs(
                        dataclasses.replace(policy, inputs=leaked)
                    )

    def test_directory_capability_binds_manifest_receipt_and_canonical_policy(
        self,
    ) -> None:
        policy = _policy()
        capability = policy.directory_capabilities[0]
        subject.validate_phase_inputs(policy)
        raw = subject.canonical_policy_bytes(policy)
        self.assertEqual(subject.decode_policy_bytes(raw), policy)

        for changed in (
            dataclasses.replace(capability, source=pathlib.Path("relative")),
            dataclasses.replace(capability, source=policy.inputs[0].source),
            dataclasses.replace(capability, manifest_role="missing-manifest"),
            dataclasses.replace(capability, staging_receipt_role="missing-receipt"),
        ):
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    subject.validate_phase_inputs(
                        dataclasses.replace(policy, directory_capabilities=(changed,))
                    )

        with self.assertRaisesRegex(ValueError, "duplicate"):
            subject.validate_phase_inputs(
                dataclasses.replace(
                    policy, directory_capabilities=(capability, capability)
                )
            )

    def test_holdout_preflight_roles_exclude_sealed_evaluation_inputs(self) -> None:
        binding_preflight, binding_prefixes = subject._phase_roles(
            "holdout-binding", preflight=True
        )
        binding_execute, _ = subject._phase_roles("holdout-binding", preflight=False)
        self.assertEqual(
            binding_preflight,
            {"phase-manifest", "bulk-manifest", "staging-receipt"},
        )
        self.assertEqual(binding_prefixes, ("bulk-inputs",))
        self.assertEqual(
            binding_execute,
            binding_preflight | {"preflight-receipt"},
        )

        evaluation_preflight, evaluation_prefixes = subject._phase_roles(
            "holdout-evaluation", preflight=True
        )
        evaluation_execute, _ = subject._phase_roles(
            "holdout-evaluation", preflight=False
        )
        self.assertEqual(
            evaluation_preflight,
            {"phase-manifest", "bulk-manifest", "staging-receipt"},
        )
        self.assertEqual(evaluation_prefixes, ("bulk-inputs",))
        self.assertEqual(
            evaluation_execute,
            evaluation_preflight | {"preflight-receipt"},
        )

    def test_execute_roles_require_preflight_receipt_in_every_phase(self) -> None:
        for phase in (
            "tree-training",
            "posting-construction",
            "development-evaluation",
            "holdout-binding",
            "holdout-evaluation",
        ):
            with self.subTest(phase=phase):
                preflight_roles, _ = subject._phase_roles(phase, preflight=True)
                execute_roles, _ = subject._phase_roles(phase, preflight=False)
                self.assertNotIn("preflight-receipt", preflight_roles)
                self.assertIn("preflight-receipt", execute_roles)

    def test_run_phase_proves_os_namespace_capability_separation(self) -> None:
        if os.geteuid() != 0:
            if (
                shutil.which("sudo") is None
                or subprocess.run(
                    ["sudo", "-n", "true"],
                    check=False,
                    capture_output=True,
                ).returncode
            ):
                self.skipTest(
                    "OS namespace integration requires root or passwordless sudo"
                )
            repository = pathlib.Path(__file__).resolve().parents[1]
            completed = subprocess.run(
                [
                    "sudo",
                    "-n",
                    sys.executable,
                    "-m",
                    "unittest",
                    "scripts.test_run_v23_leaf_page_incidence_falsifier."
                    "OfflinePhasePolicyTests."
                    "test_run_phase_proves_os_namespace_capability_separation",
                ],
                cwd=repository,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                completed.returncode,
                0,
                f"stdout={completed.stdout}\nstderr={completed.stderr}",
            )
            return

        root = pathlib.Path(tempfile.mkdtemp(prefix="v23-incidence-namespace-"))
        manifest = root / "construction-manifest.json"
        bulk_manifest = root / "bulk-manifest.json"
        staging_receipt = root / "staging-receipt.json"
        bulk_inputs = root / "bulk-inputs"
        shard = bulk_inputs / "training-shard-0000"
        launcher = root / "run_v23_leaf_page_incidence_falsifier.py"
        scratch = root / "scratch"
        output = root / "output"
        manifest.write_bytes(b"manifest\n")
        _write_staged_inventory(
            bulk_manifest,
            staging_receipt,
            bulk_inputs,
            "training-shard-0000",
        )
        launcher.write_bytes(pathlib.Path(subject.__file__).read_bytes())
        scratch.mkdir()
        output.mkdir()

        def identity(
            role: str,
            source: pathlib.Path,
        ) -> subject.AuthenticatedInput:
            payload = source.read_bytes()
            return subject.AuthenticatedInput(
                role=role,
                source=source.resolve(),
                uri=source.as_uri(),
                digest_algorithm="sha256",
                digest=hashlib.sha256(payload).hexdigest(),
                encoded_bytes=len(payload),
                generation="namespace-integration-v1",
            )

        executable = pathlib.Path("/usr/bin/echo").resolve()
        executable_payload = executable.read_bytes()
        policy = subject.OfflinePhasePolicy(
            phase="tree-training",
            executable=executable,
            executable_sha256=hashlib.sha256(executable_payload).hexdigest(),
            executable_bytes=len(executable_payload),
            inputs=(
                identity(
                    "construction-manifest",
                    manifest,
                ),
                identity(
                    "bulk-manifest",
                    bulk_manifest,
                ),
                identity(
                    "staging-receipt",
                    staging_receipt,
                ),
            ),
            scratch=scratch,
            output=output,
            parent_receipt_sha256=None,
            directory_capabilities=(
                subject.AuthenticatedDirectory(
                    role="bulk-inputs",
                    source=bulk_inputs,
                    manifest_role="bulk-manifest",
                    staging_receipt_role="staging-receipt",
                ),
            ),
            phase_argv=(),
        )
        policy = dataclasses.replace(
            policy, phase_argv=subject.build_phase_argv(policy)
        )
        original_subject_file = subject.__file__
        subject.__file__ = str(launcher)
        try:
            self.assertEqual(
                subject.run_phase(
                    policy,
                    dataclasses.replace(
                        subject.MonitorLimits(),
                        psi_immediate=float("inf"),
                        psi_sustained=float("inf"),
                        wall_seconds=30,
                    ),
                ),
                0,
            )
            self.assertEqual(tuple(output.iterdir()), ())
            self.assertEqual(tuple(scratch.iterdir()), ())
        finally:
            subject.__file__ = original_subject_file
            for path in (manifest, bulk_manifest, staging_receipt, shard, launcher):
                if path.exists():
                    path.unlink()
            if bulk_inputs.exists():
                bulk_inputs.rmdir()
            for path in (scratch, output):
                if path.exists():
                    path.rmdir()
            if root.exists():
                root.rmdir()

    def test_preflight_mounts_only_fixed_subset_before_remaining_inputs(self) -> None:
        execute = _policy("development-evaluation", "ab" * 32)
        subject.validate_phase_inputs(execute)

        preflight = dataclasses.replace(
            execute,
            inputs=tuple(
                mount for mount in execute.inputs if mount.role != "preflight-receipt"
            ),
            phase_argv=(),
        )
        preflight = dataclasses.replace(
            preflight, phase_argv=subject.build_phase_argv(preflight)
        )
        subject.validate_phase_inputs(preflight)
        self.assertNotIn("query-parquet", {mount.role for mount in preflight.inputs})
        self.assertNotIn("d2-report", {mount.role for mount in preflight.inputs})

        leaked = dataclasses.replace(
            preflight,
            inputs=preflight.inputs + (_mount("query-parquet", "query.parquet"),),
        )
        with self.assertRaisesRegex(ValueError, "preflight input"):
            subject.validate_phase_inputs(leaked)

    def test_training_offline_command_has_no_filesystem_namespace_or_runtime_mounts(
        self,
    ) -> None:
        policy = _policy()
        subject.validate_phase_inputs(policy)
        command = subject.build_offline_command(
            pathlib.Path("/authority/policy.json"), "44" * 32
        )

        self.assertEqual(policy.phase, "tree-training")
        rendered = " ".join(command)
        self.assertNotIn("query.parquet", rendered)
        self.assertNotIn("neighbors.parquet", rendered)
        self.assertNotIn("page-roster", rendered)
        self.assertNotIn("--user", command)
        self.assertNotIn("--map-root-user", command)
        self.assertFalse(any(argument.startswith("--mount") for argument in command))
        self.assertIn("--net", command)
        self.assertIn("--pid", command)
        self.assertIn("--fork", command)
        self.assertIn("--kill-child=SIGKILL", command)
        self.assertIn("--enter-offline-policy", command)
        self.assertIn("--policy-sha256", command)
        self.assertLess(sum(len(argument) + 1 for argument in command), 16_384)
        self.assertNotIn(policy.executable_sha256, rendered)

        preflight = dataclasses.replace(
            policy,
            inputs=tuple(
                mount for mount in policy.inputs if mount.role != "preflight-receipt"
            ),
            phase_argv=(),
        )
        preflight = dataclasses.replace(
            preflight, phase_argv=subject.build_phase_argv(preflight)
        )
        subject.validate_phase_inputs(preflight)
        self.assertNotIn("dataset-meta", {mount.role for mount in preflight.inputs})

    def test_receipt_chain_prevents_later_capability_before_parent_digest(self) -> None:
        with self.assertRaisesRegex(ValueError, "parent receipt"):
            subject.validate_phase_inputs(_policy("posting-construction"))

        policy = _policy("posting-construction", "ab" * 32)
        subject.validate_phase_inputs(policy)

        forbidden = list(policy.inputs)
        forbidden.append(_mount("query-parquet", "query.parquet"))
        with self.assertRaisesRegex(ValueError, "phase input"):
            subject.validate_phase_inputs(
                subject.OfflinePhasePolicy(
                    phase=policy.phase,
                    executable=policy.executable,
                    executable_sha256=policy.executable_sha256,
                    executable_bytes=policy.executable_bytes,
                    inputs=tuple(forbidden),
                    scratch=policy.scratch,
                    output=policy.output,
                    parent_receipt_sha256=policy.parent_receipt_sha256,
                    directory_capabilities=policy.directory_capabilities,
                    phase_argv=policy.phase_argv,
                )
            )

    def test_forbidden_role_probe_is_derived_from_exact_phase_inventory(self) -> None:
        policy = _policy()
        self.assertTrue(
            subject._forbidden_roles_absent(
                policy, frozenset({"dataset-meta", "training-shard-0000"})
            )
        )
        self.assertFalse(
            subject._forbidden_roles_absent(policy, frozenset({"query-parquet"}))
        )
        leaked = dataclasses.replace(
            policy,
            inputs=(*policy.inputs, _mount("query-parquet", "query.parquet")),
        )
        self.assertFalse(subject._forbidden_roles_absent(leaked, frozenset()))

    def test_network_canary_requires_explicit_kernel_denial(self) -> None:
        class SocketStub:
            def __init__(self, failure: BaseException | None) -> None:
                self.failure = failure

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def settimeout(self, _seconds: float) -> None:
                pass

            def connect(self, _address: tuple[str, int]) -> None:
                if self.failure is not None:
                    raise self.failure

        for failure, expected in (
            (None, False),
            (TimeoutError(), False),
            (OSError(errno.ENETUNREACH, "network unreachable"), True),
        ):
            with self.subTest(failure=failure), patch.object(
                subject.socket, "socket", return_value=SocketStub(failure)
            ):
                self.assertEqual(subject._network_canary_denied(), expected)

    def test_offline_probe_failure_names_exact_failed_capability(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            policy = types.SimpleNamespace(
                host_network_namespace_inode=91,
                inputs=(),
                directory_capabilities=(),
                output=pathlib.Path(directory),
            )
            with (
                patch.object(subject, "_network_namespace_inode", return_value=92),
                patch.object(subject, "_network_canary_denied", return_value=False),
                patch.object(subject, "_forbidden_roles_absent", return_value=True),
                self.assertRaisesRegex(RuntimeError, "network_canary_denied"),
            ):
                subject._offline_startup_probes(policy, frozenset())

    def test_pressure_equality_stops_and_cleanup_names_are_explicit(self) -> None:
        limits = subject.MonitorLimits()
        self.assertEqual(
            subject.classify_sample(
                limits=limits,
                rss_bytes=2 << 30,
                psi_full_avg10=0.0,
                consecutive_psi_samples=0,
                swap_delta_bytes=0,
                progress_age_seconds=0,
                wall_seconds=0,
            ),
            "rss-cap",
        )
        self.assertEqual(
            subject.classify_sample(
                limits=limits,
                rss_bytes=0,
                psi_full_avg10=0.79,
                consecutive_psi_samples=0,
                swap_delta_bytes=0,
                progress_age_seconds=0,
                wall_seconds=0,
            ),
            "psi-immediate",
        )
        self.assertEqual(
            subject.classify_sample(
                limits=limits,
                rss_bytes=0,
                psi_full_avg10=0.50,
                consecutive_psi_samples=3,
                swap_delta_bytes=0,
                progress_age_seconds=0,
                wall_seconds=0,
            ),
            "psi-sustained",
        )
        self.assertEqual(
            subject.classify_sample(
                limits=limits,
                rss_bytes=0,
                psi_full_avg10=0.0,
                consecutive_psi_samples=0,
                swap_delta_bytes=256 * 1024 * 1024 + 1,
                progress_age_seconds=0,
                wall_seconds=0,
            ),
            "swap-delta",
        )
        self.assertIsNone(
            subject.classify_sample(
                limits=limits,
                rss_bytes=(2 << 30) - 1,
                psi_full_avg10=0.50,
                consecutive_psi_samples=2,
                swap_delta_bytes=256 * 1024 * 1024,
                progress_age_seconds=299,
                wall_seconds=7199,
            )
        )
        self.assertEqual(
            subject.classify_sample(
                limits=limits,
                rss_bytes=0,
                psi_full_avg10=0.0,
                consecutive_psi_samples=0,
                swap_delta_bytes=0,
                progress_age_seconds=300,
                wall_seconds=0,
            ),
            "progress-gap",
        )
        self.assertEqual(
            subject.classify_sample(
                limits=limits,
                rss_bytes=0,
                psi_full_avg10=0.0,
                consecutive_psi_samples=0,
                swap_delta_bytes=0,
                progress_age_seconds=0,
                wall_seconds=7200,
            ),
            "wall-cap",
        )

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            (root / "known.json").write_text("{}\n", encoding="utf-8")
            (root / "unexpected.bin").write_bytes(b"unexpected")
            with self.assertRaisesRegex(ValueError, "unexpected"):
                subject.cleanup_known_files(root, ("known.json",))
            self.assertTrue((root / "known.json").exists())
            self.assertTrue((root / "unexpected.bin").exists())

        source = inspect.getsource(subject)
        self.assertNotIn("rm -rf", source)
        self.assertNotIn("shutil.rmtree", source)

    def test_policy_rejects_mutable_duplicate_and_noncanonical_authority(self) -> None:
        policy = _policy()

        mutable = list(policy.inputs)
        mutable[0] = dataclasses.replace(
            mutable[0], source=pathlib.Path("relative")
        )
        with self.assertRaisesRegex(ValueError, "absolute"):
            subject.validate_phase_inputs(
                dataclasses.replace(policy, inputs=tuple(mutable))
            )

        duplicate = policy.inputs + (policy.inputs[0],)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            subject.validate_phase_inputs(dataclasses.replace(policy, inputs=duplicate))

        with self.assertRaisesRegex(ValueError, "absolute"):
            subject.validate_phase_inputs(
                dataclasses.replace(policy, executable=pathlib.Path("relative"))
            )

        with self.assertRaisesRegex(ValueError, "disjoint"):
            subject.validate_phase_inputs(
                dataclasses.replace(policy, output=policy.scratch)
            )

    def test_phase_input_identity_rejects_non_string_uri_and_generation(self) -> None:
        policy = _policy()
        for field in ("uri", "generation"):
            changed = dataclasses.replace(policy.inputs[0], **{field: 7})
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError, "phase input"
            ):
                subject.validate_phase_inputs(
                    dataclasses.replace(
                        policy,
                        inputs=(changed, *policy.inputs[1:]),
                    )
                )

    def test_cleanup_removes_only_registered_names_and_empty_root(self) -> None:
        root = pathlib.Path(tempfile.mkdtemp())
        (root / "known.json").write_text("{}\n", encoding="utf-8")
        (root / "known.bin").write_bytes(b"known")
        subject.cleanup_known_files(root, ("known.json", "known.bin"))
        self.assertFalse(root.exists())

    def test_cli_requires_one_explicit_phase_and_refuses_network_surfaces(self) -> None:
        with self.assertRaises(SystemExit):
            subject.parse_args([])
        with self.assertRaises(SystemExit):
            subject.parse_args(
                ["--execute-tree-training", "--aws-profile", "causality"]
            )
        with self.assertRaises(SystemExit):
            subject.parse_args(
                ["--execute-tree-training", "--execute-posting-construction"]
            )

    def test_cli_gate_must_match_preflight_or_execute_policy_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            execute_path = root / "execute-policy.json"
            execute = _policy()
            execute_path.write_bytes(subject.canonical_policy_bytes(execute))
            with self.assertRaisesRegex(ValueError, "execution gate"):
                subject.main(
                    [
                        "--preflight-tree-training",
                        "--policy",
                        str(execute_path),
                    ]
                )

            preflight = dataclasses.replace(
                execute,
                inputs=tuple(
                    item
                    for item in execute.inputs
                    if item.role != "preflight-receipt"
                ),
                phase_argv=(),
            )
            preflight = dataclasses.replace(
                preflight, phase_argv=subject.build_phase_argv(preflight)
            )
            preflight_path = root / "preflight-policy.json"
            preflight_path.write_bytes(subject.canonical_policy_bytes(preflight))
            with patch.object(subject, "run_phase", return_value=0) as run:
                self.assertEqual(
                    subject.main(
                        [
                            "--preflight-tree-training",
                            "--policy",
                            str(preflight_path),
                        ]
                    ),
                    0,
                )
            run.assert_called_once_with(preflight)

    def test_canonical_policy_bytes_round_trip_without_ambient_fields(self) -> None:
        policy = _policy()
        raw = subject.canonical_policy_bytes(policy)
        self.assertEqual(subject.decode_policy_bytes(raw), policy)
        self.assertTrue(raw.endswith(b"\n"))
        self.assertEqual(raw.count(b"\n"), 1)

        value = json.loads(raw)
        value["aws_profile"] = "causality"
        changed = (
            json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        )
        with self.assertRaisesRegex(ValueError, "policy schema"):
            subject.decode_policy_bytes(changed)

    def test_policy_file_is_exclusive_mode_0600_and_digest_bound(self) -> None:
        policy = _policy()
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "policy.json"
            digest = subject.write_canonical_policy_file(policy, path)
            self.assertEqual(digest, hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(subject.read_canonical_policy_file(path, digest), policy)
            with self.assertRaisesRegex(FileExistsError, "policy.json"):
                subject.write_canonical_policy_file(policy, path)
            with self.assertRaisesRegex(ValueError, "digest"):
                subject.read_canonical_policy_file(path, "55" * 32)

    def test_monitor_reaps_original_and_stops_original_group_once(self) -> None:
        pid = os.posix_spawn(
            sys.executable,
            [sys.executable, "-c", "pass"],
            os.environ.copy(),
            setsid=True,
        )
        status, stop = subject.monitor_process_group(
            pid,
            dataclasses.replace(
                subject.MonitorLimits(),
                psi_immediate=float("inf"),
                psi_sustained=float("inf"),
            ),
            sample_interval_seconds=0.001,
        )
        self.assertEqual((status, stop), (0, None))

        pid = os.posix_spawn(
            sys.executable,
            [sys.executable, "-c", "import time; time.sleep(60)"],
            os.environ.copy(),
            setsid=True,
        )
        time.sleep(0.05)
        status, stop = subject.monitor_process_group(
            pid,
            dataclasses.replace(subject.MonitorLimits(), wall_seconds=0),
            sample_interval_seconds=0.001,
        )
        self.assertEqual(stop, "wall-cap")
        self.assertLess(status, 0)

        pid = os.posix_spawn(
            sys.executable,
            [
                sys.executable,
                "-c",
                "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)",
            ],
            os.environ.copy(),
            setsid=True,
        )
        time.sleep(0.05)
        status, stop = subject.monitor_process_group(
            pid,
            dataclasses.replace(subject.MonitorLimits(), wall_seconds=0),
            sample_interval_seconds=0.001,
            term_grace_seconds=0.01,
        )
        self.assertEqual((status, stop), (-signal.SIGKILL, "wall-cap"))

    def test_offline_namespace_stop_reaps_scientific_child(self) -> None:
        node = (
            "scripts.test_run_v23_leaf_page_incidence_falsifier."
            "OfflinePhasePolicyTests."
            "test_offline_namespace_stop_reaps_scientific_child"
        )
        if os.geteuid() != 0:
            if shutil.which("sudo") is None:
                self.skipTest("offline namespace stop integration requires root")
            completed = subprocess.run(
                ["sudo", "-n", sys.executable, "-m", "unittest", node],
                cwd=pathlib.Path(__file__).resolve().parents[1],
                check=False,
                capture_output=True,
                text=True,
            )
            if completed.returncode == 1 and "password" in completed.stderr.lower():
                self.skipTest("passwordless sudo is unavailable")
            self.assertEqual(
                completed.returncode,
                0,
                f"stdout={completed.stdout}\nstderr={completed.stderr}",
            )
            return

        process = subprocess.Popen(  # noqa: S603
            [
                "unshare",
                "--net",
                "--pid",
                "--fork",
                "--kill-child=SIGKILL",
                sys.executable,
                "-c",
                "import time; time.sleep(60)",
            ],
            start_new_session=True,
        )

        def group_members() -> list[int]:
            members = []
            for entry in pathlib.Path("/proc").iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    candidate = int(entry.name)
                    if os.getpgid(candidate) == process.pid:
                        members.append(candidate)
                except (ProcessLookupError, PermissionError):
                    continue
            return members

        try:
            time.sleep(0.05)
            _, stop = subject.monitor_process_group(
                process.pid,
                dataclasses.replace(subject.MonitorLimits(), wall_seconds=0),
                sample_interval_seconds=0.001,
                term_grace_seconds=0.05,
            )
            self.assertEqual(stop, "wall-cap")
            self.assertEqual(group_members(), [])
        finally:
            for member in group_members():
                os.kill(member, signal.SIGKILL)

    def test_termination_waits_for_group_after_leader_exit(self) -> None:
        arguments = [
            sys.executable,
            "-c",
            "import os,signal,time; child=os.fork(); "
            "(signal.signal(signal.SIGTERM, signal.SIG_IGN), time.sleep(60)) "
            "if child == 0 else time.sleep(0.05)",
        ]
        pid = os.posix_spawn(
            sys.executable,
            arguments,
            os.environ.copy(),
            setsid=True,
        )

        def group_members() -> list[int]:
            members = []
            for entry in pathlib.Path("/proc").iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    candidate = int(entry.name)
                    if os.getpgid(candidate) == pid:
                        members.append(candidate)
                except (ProcessLookupError, PermissionError):
                    continue
            return members

        try:
            time.sleep(0.1)
            subject._terminate_process_group(pid, 0.01)
            self.assertEqual(group_members(), [])
        finally:
            for member in group_members():
                os.kill(member, signal.SIGKILL)

    def test_monitor_exception_terminates_and_reaps_original_group(self) -> None:
        pid = os.posix_spawn(
            sys.executable,
            [sys.executable, "-c", "import time; time.sleep(60)"],
            os.environ.copy(),
            setsid=True,
        )
        leaked = True
        try:
            with (
                patch.object(
                    subject,
                    "_memory_psi_full_avg10",
                    side_effect=RuntimeError("PSI unavailable"),
                ),
                self.assertRaisesRegex(RuntimeError, "PSI unavailable"),
            ):
                subject.monitor_process_group(
                    pid,
                    subject.MonitorLimits(),
                    sample_interval_seconds=0.001,
                    term_grace_seconds=0.01,
                )
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                leaked = False
        finally:
            if leaked:
                try:
                    os.killpg(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    os.waitpid(pid, 0)
                except ChildProcessError:
                    pass
        self.assertFalse(leaked, "monitor exception leaked its process group")

    def test_monitor_stops_immediately_on_invalid_authenticated_progress(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            progress = pathlib.Path(temporary) / "progress.json"
            progress.write_bytes(_progress_bytes(phase="posting-construction"))
            pid = os.posix_spawn(
                sys.executable,
                [sys.executable, "-c", "import time; time.sleep(60)"],
                os.environ.copy(),
                setsid=True,
            )
            status, stop = subject.monitor_process_group(
                pid,
                subject.MonitorLimits(),
                sample_interval_seconds=0.001,
                progress_path=progress,
                progress_phase="tree-training",
                term_grace_seconds=0.01,
            )
            self.assertLess(status, 0)
            self.assertEqual(stop, "progress-authority")

    def test_phase_input_bytes_are_authenticated_before_offline_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            executable = root / "phase"
            manifest = root / "manifest"
            bulk_manifest = root / "bulk-manifest"
            staging = root / "staging"
            bulk_inputs = root / "bulk-inputs"
            for path, payload in (
                (executable, b"executable"),
                (manifest, b"manifest"),
            ):
                path.write_bytes(payload)
            _write_staged_inventory(bulk_manifest, staging, bulk_inputs, "shard", b"shard")

            def authenticated(mount: subject.AuthenticatedInput, path: pathlib.Path):
                payload = path.read_bytes()
                return dataclasses.replace(
                    mount,
                    source=path,
                    digest=hashlib.sha256(payload).hexdigest(),
                    encoded_bytes=len(payload),
                )

            policy = _policy()
            policy = dataclasses.replace(
                policy,
                executable=executable,
                executable_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
                executable_bytes=executable.stat().st_size,
                inputs=(
                    authenticated(policy.inputs[0], manifest),
                    authenticated(policy.inputs[1], bulk_manifest),
                    authenticated(policy.inputs[2], staging),
                ),
                directory_capabilities=(
                    dataclasses.replace(
                        policy.directory_capabilities[0], source=bulk_inputs
                    ),
                ),
                phase_argv=(),
            )
            policy = dataclasses.replace(
                policy, phase_argv=subject.build_phase_argv(policy)
            )
            subject.authenticate_policy_files(policy)

            manifest.write_bytes(b"swapped")
            with self.assertRaisesRegex(ValueError, "digest|length"):
                subject.authenticate_policy_files(policy)

if __name__ == "__main__":
    unittest.main()
