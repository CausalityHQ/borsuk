from __future__ import annotations

import base64
import dataclasses
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
import unittest

from scripts import run_v23_leaf_page_incidence_falsifier as subject


def _mount(role: str, name: str) -> subject.SandboxMount:
    algorithm = (
        "blake3"
        if role
        in {"incidence-tree", "incidence-postings-one", "incidence-postings-two"}
        or role.startswith("page-body-")
        else "sha256"
    )
    return subject.SandboxMount(
        role=role,
        source=pathlib.Path("/authority") / name,
        target=pathlib.PurePosixPath("/inputs") / name,
        read_only=True,
        digest_algorithm=algorithm,
        digest="11" * 32,
        encoded_bytes=17,
        generation="generation-0001",
    )


def _policy(
    phase: str = "tree-training", parent_digest: str | None = None
) -> subject.SandboxPolicy:
    inputs = (
        _mount("construction-manifest", "construction-manifest.json"),
        _mount("dataset-meta", "meta.json"),
        _mount("training-shard-0000", "train-0000.f32"),
    )
    if phase == "posting-construction":
        inputs = (
            _mount("phase-manifest", "phase-manifest.json"),
            _mount("parent-receipt", "tree-receipt.json"),
            _mount("incidence-tree", "incidence-tree.bin"),
            _mount("page-roster", "page-roster.json"),
            _mount("page-body-0000", "page-0000.bin"),
        )
    if phase == "development-evaluation":
        inputs = (
            _mount("phase-manifest", "phase-manifest.json"),
            _mount("parent-receipt", "posting-receipt.json"),
            _mount("incidence-tree", "incidence-tree.bin"),
            _mount("incidence-postings-one", "postings-one.bin"),
            _mount("incidence-postings-two", "postings-two.bin"),
            _mount("d2-report", "d2-report.json"),
            _mount("query-parquet", "query.parquet"),
        )
    inputs += (_mount("preflight-receipt", "preflight-receipt.json"),)
    return subject.SandboxPolicy(
        phase=phase,
        executable=pathlib.Path("/opt/borsuk/v23-incidence"),
        executable_sha256="aa" * 32,
        executable_bytes=19,
        runtime_mounts=(
            subject.SandboxMount(
                role="runtime-loader",
                source=pathlib.Path("/lib/ld-linux-aarch64.so.1"),
                target=pathlib.PurePosixPath("/lib/ld-linux-aarch64.so.1"),
                read_only=True,
                digest_algorithm="sha256",
                digest="22" * 32,
                encoded_bytes=23,
                generation="runtime-0001",
            ),
        ),
        inputs=inputs,
        scratch=pathlib.Path("/scratch/v23-incidence"),
        output=pathlib.Path("/output/v23-incidence"),
        parent_receipt_sha256=parent_digest,
        phase_argv=(f"--execute-{phase}",),
    )


class SandboxPolicyTests(unittest.TestCase):
    def test_holdout_preflight_roles_exclude_sealed_evaluation_inputs(self) -> None:
        binding_preflight, binding_prefixes = subject._phase_roles(
            "holdout-binding", preflight=True
        )
        binding_execute, _ = subject._phase_roles(
            "holdout-binding", preflight=False
        )
        self.assertEqual(
            binding_preflight,
            {"phase-manifest", "parent-receipt", "page-roster"},
        )
        self.assertEqual(binding_prefixes, ("page-body-",))
        self.assertEqual(
            binding_execute,
            binding_preflight
            | {"development-result", "neighbors-parquet", "preflight-receipt"},
        )

        evaluation_preflight, evaluation_prefixes = subject._phase_roles(
            "holdout-evaluation", preflight=True
        )
        evaluation_execute, _ = subject._phase_roles(
            "holdout-evaluation", preflight=False
        )
        self.assertEqual(
            evaluation_preflight,
            {
                "phase-manifest",
                "parent-receipt",
                "incidence-tree",
                "incidence-postings-one",
                "incidence-postings-two",
            },
        )
        self.assertEqual(evaluation_prefixes, ())
        self.assertEqual(
            evaluation_execute,
            evaluation_preflight
            | {
                "development-result",
                "development-latency",
                "preflight-receipt",
                "query-parquet",
                "holdout-truth",
            },
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
            if shutil.which("sudo") is None or subprocess.run(
                ["sudo", "-n", "true"],
                check=False,
                capture_output=True,
            ).returncode:
                self.skipTest("OS namespace integration requires root or passwordless sudo")
            repository = pathlib.Path(__file__).resolve().parents[1]
            completed = subprocess.run(
                [
                    "sudo",
                    "-n",
                    sys.executable,
                    "-m",
                    "unittest",
                    "scripts.test_run_v23_leaf_page_incidence_falsifier."
                    "SandboxPolicyTests."
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

        runtime_paths = {
            "aarch64": (
                "/lib/ld-linux-aarch64.so.1",
                "/lib/aarch64-linux-gnu/libc.so.6",
            ),
            "x86_64": (
                "/lib64/ld-linux-x86-64.so.2",
                "/lib/x86_64-linux-gnu/libc.so.6",
            ),
        }
        if os.uname().machine not in runtime_paths:
            self.skipTest("namespace integration runtime paths are unregistered")
        root = pathlib.Path(tempfile.mkdtemp(prefix="v23-incidence-namespace-"))
        manifest = root / "construction-manifest.json"
        shard = root / "training-shard-0000.bin"
        launcher = root / "run_v23_leaf_page_incidence_falsifier.py"
        scratch = root / "scratch"
        output = root / "output"
        manifest.write_bytes(b"manifest\n")
        shard.write_bytes(b"shard\n")
        launcher.write_bytes(pathlib.Path(subject.__file__).read_bytes())
        scratch.mkdir()
        output.mkdir()

        def identity(
            role: str,
            source: pathlib.Path,
            target: str,
        ) -> subject.SandboxMount:
            payload = source.read_bytes()
            return subject.SandboxMount(
                role=role,
                source=source.resolve(),
                target=pathlib.PurePosixPath(target),
                read_only=True,
                digest_algorithm="sha256",
                digest=hashlib.sha256(payload).hexdigest(),
                encoded_bytes=len(payload),
                generation="namespace-integration-v1",
            )

        executable = pathlib.Path("/usr/bin/echo").resolve()
        loader_target, libc_target = runtime_paths[os.uname().machine]
        loader = pathlib.Path(loader_target).resolve()
        libc = pathlib.Path(libc_target).resolve()
        executable_payload = executable.read_bytes()
        policy = subject.SandboxPolicy(
            phase="tree-training",
            executable=executable,
            executable_sha256=hashlib.sha256(executable_payload).hexdigest(),
            executable_bytes=len(executable_payload),
            runtime_mounts=(
                identity("runtime-loader", loader, loader_target),
                identity(
                    "runtime-library-libc",
                    libc,
                    libc_target,
                ),
            ),
            inputs=(
                identity(
                    "construction-manifest",
                    manifest,
                    "/inputs/construction-manifest.json",
                ),
                identity(
                    "training-shard-0000",
                    shard,
                    "/inputs/training-shard-0000.bin",
                ),
            ),
            scratch=scratch,
            output=output,
            parent_receipt_sha256=None,
            phase_argv=("--preflight-tree-training",),
        )
        original_subject_file = subject.__file__
        subject.__file__ = str(launcher)
        try:
            self.assertEqual(
                subject.run_phase(
                    policy,
                    dataclasses.replace(subject.MonitorLimits(), wall_seconds=30),
                ),
                0,
            )
            self.assertEqual(tuple(output.iterdir()), ())
            self.assertEqual(tuple(scratch.iterdir()), ())
        finally:
            subject.__file__ = original_subject_file
            for path in (manifest, shard, launcher):
                if path.exists():
                    path.unlink()
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
            inputs=execute.inputs[:5],
            phase_argv=("--preflight-development-evaluation",),
        )
        subject.validate_phase_inputs(preflight)
        self.assertNotIn("query-parquet", {mount.role for mount in preflight.inputs})
        self.assertNotIn("d2-report", {mount.role for mount in preflight.inputs})

        leaked = dataclasses.replace(preflight, inputs=preflight.inputs + (execute.inputs[-1],))
        with self.assertRaisesRegex(ValueError, "preflight input"):
            subject.validate_phase_inputs(leaked)

    def test_training_mounts_only_manifest_shards_binary_runtime_and_output(self) -> None:
        policy = _policy()
        subject.validate_phase_inputs(policy)
        command = subject.build_unshare_command(policy)

        self.assertEqual(policy.phase, "tree-training")
        rendered = " ".join(command)
        self.assertNotIn("query.parquet", rendered)
        self.assertNotIn("neighbors.parquet", rendered)
        self.assertNotIn("page-roster", rendered)
        self.assertIn("--user", command)
        self.assertIn("--map-root-user", command)
        self.assertIn("--mount", command)
        self.assertIn("--net", command)
        self.assertIn("--pid", command)
        self.assertIn("--fork", command)

        preflight = dataclasses.replace(
            policy,
            inputs=(policy.inputs[0], policy.inputs[2]),
            phase_argv=("--preflight-tree-training",),
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
                subject.SandboxPolicy(
                    phase=policy.phase,
                    executable=policy.executable,
                    executable_sha256=policy.executable_sha256,
                    executable_bytes=policy.executable_bytes,
                    runtime_mounts=policy.runtime_mounts,
                    inputs=tuple(forbidden),
                    scratch=policy.scratch,
                    output=policy.output,
                    parent_receipt_sha256=policy.parent_receipt_sha256,
                    phase_argv=policy.phase_argv,
                )
            )

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
        mutable[0] = subject.SandboxMount(
            role=mutable[0].role,
            source=mutable[0].source,
            target=mutable[0].target,
            read_only=False,
            digest_algorithm=mutable[0].digest_algorithm,
            digest=mutable[0].digest,
            encoded_bytes=mutable[0].encoded_bytes,
            generation=mutable[0].generation,
        )
        with self.assertRaisesRegex(ValueError, "read-only"):
            subject.validate_phase_inputs(
                dataclasses.replace(policy, inputs=tuple(mutable))
            )

        duplicate = policy.inputs + (policy.inputs[0],)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            subject.validate_phase_inputs(
                dataclasses.replace(policy, inputs=duplicate)
            )

        with self.assertRaisesRegex(ValueError, "absolute"):
            subject.validate_phase_inputs(
                dataclasses.replace(policy, executable=pathlib.Path("relative"))
            )

        with self.assertRaisesRegex(ValueError, "disjoint"):
            subject.validate_phase_inputs(
                dataclasses.replace(policy, output=policy.scratch)
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
            subject.parse_args(["--execute-tree-training", "--aws-profile", "causality"])
        with self.assertRaises(SystemExit):
            subject.parse_args(
                ["--execute-tree-training", "--execute-posting-construction"]
            )

    def test_canonical_policy_round_trips_without_ambient_fields(self) -> None:
        policy = _policy()
        encoded = subject.canonical_policy_argument(policy)
        self.assertEqual(subject.decode_policy_argument(encoded), policy)

        raw = base64.urlsafe_b64decode(encoded.encode("ascii"))
        value = json.loads(raw)
        value["aws_profile"] = "causality"
        changed = base64.urlsafe_b64encode(
            json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
        ).decode("ascii")
        with self.assertRaisesRegex(ValueError, "policy schema"):
            subject.decode_policy_argument(changed)

    def test_monitor_reaps_original_and_stops_original_group_once(self) -> None:
        pid = os.posix_spawn(
            sys.executable,
            [sys.executable, "-c", "pass"],
            os.environ.copy(),
            setsid=True,
        )
        status, stop = subject.monitor_process_group(
            pid,
            subject.MonitorLimits(),
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

    def test_mount_bytes_are_authenticated_before_namespace_creation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            executable = root / "phase"
            runtime = root / "loader"
            manifest = root / "manifest"
            shard = root / "shard"
            for path, payload in (
                (executable, b"executable"),
                (runtime, b"runtime"),
                (manifest, b"manifest"),
                (shard, b"shard"),
            ):
                path.write_bytes(payload)

            def authenticated(mount: subject.SandboxMount, path: pathlib.Path):
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
                runtime_mounts=(authenticated(policy.runtime_mounts[0], runtime),),
                inputs=(
                    authenticated(policy.inputs[0], manifest),
                    authenticated(policy.inputs[2], shard),
                ),
                phase_argv=("--preflight-tree-training",),
            )
            subject.authenticate_policy_files(policy)

            changed = dataclasses.replace(
                policy,
                inputs=(dataclasses.replace(policy.inputs[0], digest="33" * 32),)
                + policy.inputs[1:],
            )
            with self.assertRaisesRegex(ValueError, "digest"):
                subject.authenticate_policy_files(changed)

    def test_bound_mount_bytes_are_reauthenticated_before_pivot_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            policy = _policy()
            targets = {
                root / "phase/v23-incidence": b"executable",
                root / policy.runtime_mounts[0].target.as_posix().lstrip("/"): b"runtime",
                root / policy.inputs[0].target.as_posix().lstrip("/"): b"manifest",
                root / policy.inputs[1].target.as_posix().lstrip("/"): b"shard",
            }
            for path, payload in targets.items():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(payload)

            def identity(mount: subject.SandboxMount, payload: bytes):
                return dataclasses.replace(
                    mount,
                    digest=hashlib.sha256(payload).hexdigest(),
                    encoded_bytes=len(payload),
                )

            policy = dataclasses.replace(
                policy,
                executable_sha256=hashlib.sha256(b"executable").hexdigest(),
                executable_bytes=len(b"executable"),
                runtime_mounts=(identity(policy.runtime_mounts[0], b"runtime"),),
                inputs=(
                    identity(policy.inputs[0], b"manifest"),
                    identity(policy.inputs[1], b"shard"),
                ),
            )
            subject.authenticate_mounted_policy_files(root, policy)

            (root / policy.inputs[1].target.as_posix().lstrip("/")).write_bytes(
                b"swapped"
            )
            with self.assertRaisesRegex(ValueError, "digest|length"):
                subject.authenticate_mounted_policy_files(root, policy)


if __name__ == "__main__":
    unittest.main()
