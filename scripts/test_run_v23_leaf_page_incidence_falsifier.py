from __future__ import annotations

import base64
import dataclasses
import hashlib
import inspect
import json
import os
import pathlib
import signal
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
        _mount("training-shard-0000", "train-0000.f32"),
    )
    if phase == "posting-construction":
        inputs = (
            _mount("parent-receipt", "tree-receipt.json"),
            _mount("incidence-tree", "incidence-tree.bin"),
            _mount("page-roster", "page-roster.json"),
            _mount("page-body-0000", "page-0000.bin"),
        )
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
    )


class SandboxPolicyTests(unittest.TestCase):
    def test_training_mounts_only_manifest_shards_binary_runtime_and_output(self) -> None:
        policy = _policy()
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
                    authenticated(policy.inputs[1], shard),
                ),
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
