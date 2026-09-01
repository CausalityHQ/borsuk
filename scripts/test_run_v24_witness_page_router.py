from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import inspect
import io
import json
import os
import pathlib
import signal
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch

from scripts import run_v24_witness_page_router as subject


def _request(root: pathlib.Path) -> subject.PhaseRequest:
    executable = root / "v24-witness-page-router"
    executable.write_bytes(b"static-binary")
    manifest = root / "manifest.json"
    manifest.write_bytes(
        json.dumps(
            {
                "claim_eligible": False,
                "generation": "generation-v24-fixture",
                "inputs": [
                    {
                        "digest": "11" * 32,
                        "digest_algorithm": "sha256",
                        "encoded_bytes": 7,
                        "generation": "s3-version:input-version",
                        "role": "construction-rows-parquet",
                        "uri": "s3://registered/v24/construction-rows.parquet",
                    }
                ],
                "output_uris": {
                    "witness-graph": "s3://registered/v24/witness-graph.arrow",
                    "witnesses-arrow": "s3://registered/v24/witnesses.arrow",
                },
                "phase": "witness-training",
                "schema": "borsuk-v24-local-manifest-v1",
                "seed": 1,
                "source_row_count": 2,
                "witness_count": 2,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )
    input_dir = root / "inputs"
    output_dir = root / "output"
    scratch = root / "scratch"
    input_dir.mkdir()
    output_dir.mkdir()
    scratch.mkdir()
    staging_receipt = root / "staging-receipt.json"
    staging_receipt.write_bytes(b"{}\n")
    return subject.PhaseRequest(
        phase="train-witnesses",
        executable=executable,
        executable_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
        executable_bytes=executable.stat().st_size,
        manifest=manifest,
        manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
        staging_receipt=staging_receipt,
        input_dir=input_dir,
        output_dir=output_dir,
        scratch=scratch,
        scratch_names=("phase.tmp",),
    )


def _with_manifest_phase(
    request: subject.PhaseRequest,
    phase: str,
    roles: tuple[str, ...],
) -> subject.PhaseRequest:
    value = json.loads(request.manifest.read_bytes())
    value["phase"] = phase
    value["inputs"] = [
        {
            "digest": f"{index + 1:02x}" * 32,
            "digest_algorithm": "sha256",
            "encoded_bytes": 7,
            "generation": f"s3-version:version-{index}",
            "role": role,
            "uri": f"s3://registered/v24/{role}",
        }
        for index, role in enumerate(roles)
    ]
    request.manifest.write_bytes(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )
    return dataclasses.replace(
        request,
        manifest_sha256=hashlib.sha256(request.manifest.read_bytes()).hexdigest(),
    )


class V24RunnerTests(unittest.TestCase):
    def test_runner_cli_requires_every_authority_and_rejects_network_flags(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            request = _request(pathlib.Path(temporary))
            arguments = [
                "--phase",
                request.phase,
                "--executable",
                str(request.executable),
                "--executable-sha256",
                request.executable_sha256,
                "--executable-bytes",
                str(request.executable_bytes),
                "--manifest",
                str(request.manifest),
                "--manifest-sha256",
                request.manifest_sha256,
                "--staging-receipt",
                str(request.staging_receipt),
                "--input-dir",
                str(request.input_dir),
                "--output-dir",
                str(request.output_dir),
                "--scratch",
                str(request.scratch),
                "--scratch-name",
                "phase.tmp",
            ]
            self.assertEqual(subject.parse_args(arguments), request)
            for suffix in (
                ["--bucket", "forbidden"],
                ["--aws-profile", "causality"],
                ["--manifest", str(request.manifest)],
            ):
                with (
                    self.subTest(suffix=suffix),
                    contextlib.redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit),
                ):
                    subject.parse_args(arguments + suffix)

    def test_progress_file_requires_canonical_monotonic_completed_work(self) -> None:
        monitor = subject.AuthenticatedProgressMonitor("witness-training")
        first = {
            "completed_units": 0,
            "phase": "witness-training",
            "sequence": 0,
            "total_units": 100,
        }
        second = {
            "completed_units": 17,
            "phase": "witness-training",
            "sequence": 1,
            "total_units": 100,
        }
        encode = lambda value: (  # noqa: E731
            json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        )
        self.assertEqual(monitor.observe(encode(first)), (0, 0, 100))
        self.assertEqual(monitor.observe(encode(second)), (1, 17, 100))
        skipped = second | {"completed_units": 71, "sequence": 3}
        self.assertEqual(monitor.observe(encode(skipped)), (3, 71, 100))
        late_monitor = subject.AuthenticatedProgressMonitor("witness-training")
        self.assertEqual(late_monitor.observe(encode(skipped)), (3, 71, 100))
        for mutation in (
            skipped | {"sequence": 1, "completed_units": 72},
            skipped | {"sequence": 4, "completed_units": 70},
            skipped | {"phase": "holdout-evaluation", "sequence": 4},
            skipped | {"total_units": 101, "sequence": 4},
        ):
            with self.subTest(mutation=mutation), self.assertRaisesRegex(
                ValueError, "progress"
            ):
                changed = subject.AuthenticatedProgressMonitor("witness-training")
                changed.observe(encode(first))
                changed.observe(encode(second))
                changed.observe(encode(skipped))
                changed.observe(encode(mutation))

    def test_phase_command_is_direct_static_binary_with_one_explicit_phase(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            request = _request(pathlib.Path(temporary))
            self.assertEqual(
                subject.build_phase_command(request),
                [
                    str(request.executable),
                    "--manifest",
                    str(request.manifest),
                    "--input-dir",
                    str(request.input_dir),
                    "--output-dir",
                    str(request.output_dir),
                    "--train-witnesses",
                    "--execute",
                ],
            )
            for phase, manifest_phase, roles, flag in (
                (
                    "build-postings",
                    "posting-construction",
                    (
                        "training-result",
                        "witness-graph",
                        "witnesses-arrow",
                        "page-rows-parquet",
                    ),
                    "--build-postings",
                ),
                (
                    "evaluate-development",
                    "development-evaluation",
                    (
                        "witness-graph",
                        "witness-postings",
                        "query-parquet",
                        "neighbors-parquet",
                    ),
                    "--evaluate-development",
                ),
                (
                    "bind-holdout",
                    "holdout-binding",
                    (
                        "development-result",
                        "query-parquet",
                        "neighbors-parquet",
                    ),
                    "--bind-holdout",
                ),
                (
                    "evaluate-holdout",
                    "holdout-evaluation",
                    (
                        "holdout-truth",
                        "witness-graph",
                        "witness-postings",
                        "query-parquet",
                        "neighbors-parquet",
                    ),
                    "--evaluate-holdout",
                ),
            ):
                changed = dataclasses.replace(
                    _with_manifest_phase(request, manifest_phase, roles),
                    phase=phase,
                )
                self.assertIn(flag, subject.build_phase_command(changed))
            request = _with_manifest_phase(
                request, "witness-training", ("construction-rows-parquet",)
            )
            with self.assertRaisesRegex(ValueError, "manifest"):
                subject.build_phase_command(
                    dataclasses.replace(request, manifest_sha256="00" * 32)
                )
            with self.assertRaisesRegex(ValueError, "phase"):
                subject.build_phase_command(
                    dataclasses.replace(request, phase="evaluate-development")
                )

        source = inspect.getsource(subject).lower()
        for forbidden in (
            "ldd",
            "loader search",
            "pivot_root",
            "chroot",
            "mount(",
            "rmtree",
            "boto3",
            "s3client",
        ):
            self.assertNotIn(forbidden, source)

    def test_child_environment_is_minimal_and_strips_aws_boto_and_proxy_state(self) -> None:
        ambient = {
            "AWS_PROFILE": "causality",
            "AWS_SESSION_TOKEN": "secret",
            "BOTO_CONFIG": "/secret",
            "HTTPS_PROXY": "http://proxy",
            "PATH": "/ambient/bin",
            "RAYON_NUM_THREADS": "64",
        }
        environment = subject.offline_environment(pathlib.Path("/scratch"), ambient)
        self.assertEqual(
            environment,
            {
                "HOME": "/nonexistent",
                "LANG": "C",
                "LC_ALL": "C",
                "RAYON_NUM_THREADS": "64",
                "TMPDIR": "/scratch",
            },
        )

    def test_monitor_classifies_exact_resource_progress_and_wall_boundaries(self) -> None:
        limits = subject.MonitorLimits(
            rss_bytes=32 << 30,
            psi_full_avg10=0.50,
            swap_delta_bytes=1,
            progress_seconds=1200,
            wall_seconds=7200,
        )
        cases = (
            ({"rss_bytes": 32 << 30}, "rss-cap"),
            ({"psi_full_avg10": 0.50}, "psi-cap"),
            ({"swap_delta_bytes": 2}, "swap-growth"),
            ({"progress_age_seconds": 1200}, "progress-gap"),
            ({"wall_seconds": 7200}, "wall-cap"),
        )
        baseline = {
            "rss_bytes": 0,
            "psi_full_avg10": 0.0,
            "swap_delta_bytes": 0,
            "progress_age_seconds": 0.0,
            "wall_seconds": 0.0,
        }
        for mutation, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(
                    subject.classify_sample(limits=limits, **(baseline | mutation)),
                    expected,
                )
        self.assertIsNone(
            subject.classify_sample(
                limits=limits,
                rss_bytes=(32 << 30) - 1,
                psi_full_avg10=0.49,
                swap_delta_bytes=1,
                progress_age_seconds=1199,
                wall_seconds=7199,
            )
        )

    def test_monitor_limits_bind_construction_and_serving_phase_rss(self) -> None:
        for phase in ("train-witnesses", "build-postings"):
            self.assertEqual(subject.MonitorLimits.for_phase(phase).rss_bytes, 32 << 30)
        for phase in (
            "evaluate-development",
            "bind-holdout",
            "evaluate-holdout",
        ):
            self.assertEqual(subject.MonitorLimits.for_phase(phase).rss_bytes, 3 << 30)

    def test_monitor_preserves_original_exit_and_terms_then_kills_one_group(self) -> None:
        pid = os.posix_spawn(
            sys.executable,
            [sys.executable, "-c", "raise SystemExit(7)"],
            os.environ.copy(),
            setsid=True,
        )
        status, stop = subject.monitor_process_group(
            pid,
            dataclasses.replace(subject.MonitorLimits(), psi_full_avg10=float("inf")),
            sample_interval_seconds=0.001,
        )
        self.assertEqual((status, stop), (7, None))

        with tempfile.TemporaryDirectory() as temporary:
            ready = pathlib.Path(temporary) / "ready"
            pid = os.posix_spawn(
                sys.executable,
                [
                    sys.executable,
                    "-c",
                    (
                        "import pathlib,signal,time; "
                        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                        f"pathlib.Path({str(ready)!r}).write_bytes(b'ready'); "
                        "time.sleep(60)"
                    ),
                ],
                os.environ.copy(),
                setsid=True,
            )
            deadline = time.monotonic() + 2.0
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertTrue(ready.exists())
            status, stop = subject.monitor_process_group(
                pid,
                dataclasses.replace(
                    subject.MonitorLimits(),
                    psi_full_avg10=float("inf"),
                    wall_seconds=0,
                ),
                sample_interval_seconds=0.001,
                term_grace_seconds=0.01,
            )
            self.assertEqual((status, stop), (-signal.SIGKILL, "wall-cap"))
            with self.assertRaises(ProcessLookupError):
                os.killpg(pid, 0)

    def test_monitor_requires_authenticated_progress_at_process_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            progress = pathlib.Path(temporary) / "progress.json"
            payload = json.dumps(
                {
                    "completed_units": 17,
                    "phase": "witness-training",
                    "sequence": 2,
                    "total_units": 17,
                },
                separators=(",", ":"),
                sort_keys=True,
            ) + "\n"
            pid = os.posix_spawn(
                sys.executable,
                [
                    sys.executable,
                    "-c",
                    f"import pathlib; pathlib.Path({str(progress)!r}).write_text({payload!r})",
                ],
                os.environ.copy(),
                setsid=True,
            )
            status, stop = subject.monitor_process_group(
                pid,
                dataclasses.replace(
                    subject.MonitorLimits(), psi_full_avg10=float("inf")
                ),
                sample_interval_seconds=0.001,
                progress_path=progress,
                progress_phase="witness-training",
            )
            self.assertEqual((status, stop), (0, None))

            progress.unlink()
            pid = os.posix_spawn(
                sys.executable,
                [sys.executable, "-c", "pass"],
                os.environ.copy(),
                setsid=True,
            )
            status, stop = subject.monitor_process_group(
                pid,
                dataclasses.replace(
                    subject.MonitorLimits(), psi_full_avg10=float("inf")
                ),
                sample_interval_seconds=0.001,
                progress_path=progress,
                progress_phase="witness-training",
            )
            self.assertEqual((status, stop), (0, "progress-authority"))

    def test_cleanup_unlinks_only_explicit_regular_files_and_rejects_surprises(self) -> None:
        root = pathlib.Path(tempfile.mkdtemp())
        (root / "manifest.json").write_bytes(b"{}\n")
        (root / "rows.parquet").write_bytes(b"PAR1")
        subject.cleanup_known_files(root, ("manifest.json", "rows.parquet"))
        self.assertFalse(root.exists())

        root = pathlib.Path(tempfile.mkdtemp())
        (root / "known.arrow").write_bytes(b"ARROW1")
        (root / "unexpected").write_bytes(b"owned-by-someone-else")
        with self.assertRaisesRegex(ValueError, "unexpected"):
            subject.cleanup_known_files(root, ("known.arrow",))
        self.assertTrue((root / "known.arrow").exists())
        self.assertTrue((root / "unexpected").exists())
        (root / "known.arrow").unlink()
        (root / "unexpected").unlink()
        root.rmdir()

    def test_run_phase_reauthenticates_cleans_scratch_and_fails_nonzero_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            request = _request(pathlib.Path(temporary))
            process = types.SimpleNamespace(pid=12345, returncode=None)

            def completed(*_args: object, **_kwargs: object) -> tuple[int, None]:
                (request.scratch / "phase.tmp").write_bytes(b"owned")
                return 0, None

            with (
                patch.object(subject, "validate_inventory") as authenticate,
                patch.object(subject.subprocess, "Popen", return_value=process) as popen,
                patch.object(subject, "monitor_process_group", side_effect=completed),
            ):
                self.assertEqual(subject.run_phase(request), 0)
            self.assertEqual(authenticate.call_count, 2)
            self.assertFalse(request.scratch.exists())
            self.assertEqual(popen.call_args.args[0][0], str(request.executable))
            self.assertFalse(
                any(
                    name.startswith(("AWS_", "BOTO_"))
                    for name in popen.call_args.kwargs["env"]
                )
            )

        with tempfile.TemporaryDirectory() as temporary:
            request = _request(pathlib.Path(temporary))
            process = types.SimpleNamespace(pid=12345, returncode=None)
            with (
                patch.object(subject, "validate_inventory"),
                patch.object(subject.subprocess, "Popen", return_value=process),
                patch.object(subject, "monitor_process_group", return_value=(7, None)),
                self.assertRaisesRegex(RuntimeError, "exited 7"),
            ):
                subject.run_phase(request)
            self.assertFalse(request.scratch.exists())


if __name__ == "__main__":
    unittest.main()
