from __future__ import annotations

import dataclasses
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.run_v37_relation_router_spot import (
    AMI_ID,
    INSTANCE_PROFILE,
    INSTANCE_TYPE,
    PREFLIGHT_COORDINATE_SCORES,
    PREFLIGHT_COORDINATE_SHA256,
    PROFILE,
    REGION,
    SPOT_TARGETS,
    V37MonitorSample,
    V37StagedArtifact,
    V37StagedOutput,
    V37StagedPhase,
    build_v37_binary_command,
    build_v37_launch_specs,
    build_v37_science_service_command,
    build_v37_spot_plan,
    build_v37_worker_script,
    canonical_v37_phase_manifest_bytes,
    canonical_v37_terminal_bytes,
    classify_v37_monitor_sample,
    execute_v37_native_science,
    execute_v37_worker,
    parse_v37_worker_args,
    phase_input_roles,
    publish_v37_worker_failure,
    publish_v37_worker_success,
    run_v37_native_process,
    run_v37_spot_phase,
    stage_v37_phase_inputs,
    validate_v37_local_result_bytes,
    validate_v37_preflight_admission,
    validate_v37_progress_bytes,
    validate_v37_relation_predecessors,
    validate_v37_terminal_bytes,
)

SOURCE_COMMIT = "12" * 20


def _plan(phase: str = "build-ownership"):
    return build_v37_spot_plan(
        phase=phase,
        run_id=f"v37-fixture-{phase}",
        source_commit=SOURCE_COMMIT,
        source_archive_uri="s3://fixture/source.tar.zst",
        source_archive_sha256="21" * 32,
        source_archive_bytes=1024,
        binary_uri="s3://fixture/v37-relation-router",
        binary_sha256="22" * 32,
        binary_bytes=2048,
        manifest_uri=f"s3://fixture/manifest-{phase}.json",
        manifest_sha256="23" * 32,
        manifest_bytes=4096,
        output_prefix=f"s3://fixture/results/{phase}/",
    )


def _phase_manifest_fixture(plan):  # noqa: ANN001
    import blake3

    payloads = {
        role: f"{plan.phase}-{role}\n".encode()
        for role in phase_input_roles(plan.phase)
    }
    manifest = {
        "claim_eligible": False,
        "inputs": [
            {
                "blake3": blake3.blake3(payloads[role]).hexdigest(),
                "encoded_bytes": len(payloads[role]),
                "role": role,
                "sha256": hashlib.sha256(payloads[role]).hexdigest(),
                "uri": f"s3://fixture/inputs/{plan.phase}/{role}",
            }
            for role in phase_input_roles(plan.phase)
        ],
        "outputs": [
            {"role": role, "uri": f"{plan.output_prefix}{role}"}
            for role in {
                "preflight-training": (),
                "build-ownership": ("ownership-tree", "ownership"),
                "evaluate-ceiling": ("ceiling",),
            }[plan.phase]
        ],
        "phase": plan.phase,
        "run_id": plan.run_id,
        "schema": "borsuk-v37-spot-phase-manifest-v1",
        "source_commit": plan.source_commit,
        "workers": 32,
    }
    raw = canonical_v37_phase_manifest_bytes(manifest)
    return raw, payloads


def _controller_plan_fixture(phase: str):
    plan = _plan(phase)
    manifest_raw, _payloads = _phase_manifest_fixture(plan)
    plan = dataclasses.replace(
        plan,
        manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
        manifest_bytes=len(manifest_raw),
    )
    return plan, {_s3_object_key(plan.manifest_uri): manifest_raw}


def _preflight_admission_fixture():
    import blake3

    authority_raw = b"v37-authority-fixture\n"
    authority = {
        "blake3": blake3.blake3(authority_raw).hexdigest(),
        "encoded_bytes": len(authority_raw),
        "role": "v37-authority",
        "sha256": hashlib.sha256(authority_raw).hexdigest(),
        "uri": "s3://fixture/inputs/v37-authority",
    }
    preflight_plan = _plan("preflight-training")
    preflight_manifest = {
        "claim_eligible": False,
        "inputs": [authority],
        "outputs": [],
        "phase": preflight_plan.phase,
        "run_id": preflight_plan.run_id,
        "schema": "borsuk-v37-spot-phase-manifest-v1",
        "source_commit": preflight_plan.source_commit,
        "workers": 32,
    }
    preflight_manifest_raw = canonical_v37_phase_manifest_bytes(preflight_manifest)
    preflight_plan = dataclasses.replace(
        preflight_plan,
        manifest_sha256=hashlib.sha256(preflight_manifest_raw).hexdigest(),
        manifest_bytes=len(preflight_manifest_raw),
    )
    inputs = []
    for role in phase_input_roles("build-ownership"):
        if role == "v37-authority":
            inputs.append(authority)
            continue
        raw = f"{role}-fixture\n".encode()
        inputs.append(
            {
                "blake3": blake3.blake3(raw).hexdigest(),
                "encoded_bytes": len(raw),
                "role": role,
                "sha256": hashlib.sha256(raw).hexdigest(),
                "uri": f"s3://fixture/inputs/{role}",
            }
        )
    build_plan = _plan("build-ownership")
    build_manifest = {
        "claim_eligible": False,
        "inputs": inputs,
        "outputs": [
            {"role": role, "uri": f"{build_plan.output_prefix}{role}"}
            for role in ("ownership-tree", "ownership")
        ],
        "phase": build_plan.phase,
        "run_id": build_plan.run_id,
        "schema": "borsuk-v37-spot-phase-manifest-v1",
        "source_commit": build_plan.source_commit,
        "workers": 32,
    }
    build_manifest_raw = canonical_v37_phase_manifest_bytes(build_manifest)
    build_plan = dataclasses.replace(
        build_plan,
        manifest_sha256=hashlib.sha256(build_manifest_raw).hexdigest(),
        manifest_bytes=len(build_manifest_raw),
    )
    staged = V37StagedPhase(
        phase=preflight_plan.phase,
        run_id=preflight_plan.run_id,
        source_commit=preflight_plan.source_commit,
        workers=32,
        inputs=(
            V37StagedArtifact(
                role=authority["role"],
                path=Path("/authenticated/v37-authority"),
                uri=authority["uri"],
                sha256=authority["sha256"],
                blake3=authority["blake3"],
                encoded_bytes=authority["encoded_bytes"],
            ),
        ),
        outputs=(),
    )
    result = {
        "artifacts": [],
        "claim_eligible": False,
        "inputs": [authority],
        "mode": "preflight-training",
        "preflight_evidence": {
            "coordinate_generator": "splitmix23-f32-v1",
            "coordinate_sha256": PREFLIGHT_COORDINATE_SHA256,
            "projected_construction_bytes": 2_137_615_120,
            "scalar_fused_comparisons": 15_360,
            "scalar_fused_max_ulp_delta": 0,
        },
        "schema": "borsuk-v37-local-result-v3",
        "training_evidence": {
            "dimensions": 192,
            "fma_backend": "aarch64-neon-fma",
            "leaf_count": 16,
            "partition_coordinate_scores": PREFLIGHT_COORDINATE_SCORES,
            "partition_coordinate_scores_per_second": 25_165_824,
            "partition_scoring_elapsed_ns": 2_000_000_000,
            "rows": 65_536,
            "training_elapsed_ns": 2_100_000_000,
        },
    }
    result_raw = json.dumps(result, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    progress_raw = (
        b'{"completed_internal_nodes":15,"partition_coordinate_scores":50331648,'
        b'"schema":"borsuk-v37-training-progress-v1","sequence":15,'
        b'"total_internal_nodes":15}\n'
    )
    uploads = []
    terminal_raw = publish_v37_worker_success(
        preflight_plan,
        staged,
        instance_id="i-v37-fixture",
        result_raw=result_raw,
        final_progress_raw=progress_raw,
        monitor={
            "elapsed_milliseconds": 2_100,
            "peak_psi_full_ppm": 0,
            "peak_rss_bytes": 100_000_000,
            "swap_end_bytes": 0,
            "swap_start_bytes": 0,
        },
        upload_once=lambda bucket, key, body: uploads.append((bucket, key, body)),
    )
    return {
        "build_plan": build_plan,
        "build_manifest_raw": build_manifest_raw,
        "preflight_manifest_raw": preflight_manifest_raw,
        "preflight_result_raw": result_raw,
        "preflight_terminal_raw": terminal_raw,
    }


def _s3_object_key(uri: str) -> tuple[str, str]:
    bucket, key = uri.removeprefix("s3://").split("/", 1)
    return bucket, key


def _admitted_controller_fixture():
    admission = _preflight_admission_fixture()
    terminal = json.loads(admission["preflight_terminal_raw"])
    terminal_uri = terminal["result"]["uri"].removesuffix(
        "local-result.json"
    ) + "ATTEMPT_TERMINAL.json"
    objects = {
        _s3_object_key(terminal_uri): admission["preflight_terminal_raw"],
        _s3_object_key(terminal["manifest_uri"]): admission["preflight_manifest_raw"],
        _s3_object_key(terminal["result"]["uri"]): admission["preflight_result_raw"],
        _s3_object_key(admission["build_plan"].manifest_uri): admission[
            "build_manifest_raw"
        ],
    }
    return admission["build_plan"], terminal_uri, objects


class _EC2:
    def __init__(self) -> None:
        self.launches: list[dict[str, object]] = []
        self.terminated: list[str] = []

    def run_instances(self, **request):  # noqa: ANN003
        self.launches.append(request)
        return {"Instances": [{"InstanceId": "i-v37-fixture"}]}

    def terminate_instances(self, *, InstanceIds):  # noqa: ANN001, N803
        self.terminated.extend(InstanceIds)

    def describe_instances(self, *, InstanceIds):  # noqa: ANN001, N803
        return {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": InstanceIds[0],
                            "State": {"Name": "running"},
                        }
                    ]
                }
            ]
        }


class _AwsError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _DeniedEC2(_EC2):
    def run_instances(self, **request):  # noqa: ANN003
        self.launches.append(request)
        raise _AwsError("UnauthorizedOperation")


class _StoppedEC2(_EC2):
    def describe_instances(self, *, InstanceIds):  # noqa: ANN001, N803
        return {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": InstanceIds[0],
                            "State": {"Name": "terminated"},
                        }
                    ]
                }
            ]
        }


class _Body:
    def __init__(self, raw: bytes) -> None:
        self.raw = raw
        self.reads: list[int | None] = []

    def read(self, amount: int | None = None) -> bytes:
        self.reads.append(amount)
        return self.raw if amount is None else self.raw[:amount]


class _S3:
    def __init__(
        self,
        objects: dict[tuple[str, str], bytes],
        *,
        unavailable_reads: int = 0,
        unavailable_error_code: str | None = None,
    ) -> None:
        self.objects = objects
        self.unavailable_reads = unavailable_reads
        self.unavailable_error_code = unavailable_error_code
        self.reads: list[tuple[str, str]] = []
        self.writes: list[tuple[str, str, bytes]] = []

    def get_object(self, *, Bucket, Key, **_request):  # noqa: ANN001, N803
        self.reads.append((Bucket, Key))
        if len(self.reads) <= self.unavailable_reads:
            if self.unavailable_error_code is not None:
                raise _AwsError(self.unavailable_error_code)
            raise KeyError(Key)
        raw = self.objects[(Bucket, Key)]
        return {"Body": _Body(raw), "ContentLength": len(raw)}

    def put_object(self, *, Bucket, Key, Body, IfNoneMatch, **_request):  # noqa: ANN001, N803
        if IfNoneMatch != "*" or (Bucket, Key) in self.objects:
            raise _AwsError("PreconditionFailed")
        raw = bytes(Body)
        self.objects[(Bucket, Key)] = raw
        self.writes.append((Bucket, Key, raw))
        return {}


class V37SpotAuthorityTests(unittest.TestCase):
    def test_v37_controller_rejects_oversized_terminal_before_body_read(self) -> None:
        body = _Body(b"not-read")

        class OversizedTerminalS3:
            def get_object(self, **_request):  # noqa: ANN003
                return {"Body": body, "ContentLength": 1_048_577}

        ec2 = _EC2()
        with self.assertRaisesRegex(ValueError, "terminal length"):
            run_v37_spot_phase(
                _plan("preflight-training"),
                ec2_client=ec2,
                s3_client=OversizedTerminalS3(),
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(body.reads, [])
        self.assertEqual(ec2.launches, [])

    def test_v37_plan_manifest_phase_mismatch_stops_before_download_or_launch(self) -> None:
        preflight_plan = _plan("preflight-training")
        build_raw, build_payloads = _phase_manifest_fixture(_plan("build-ownership"))
        mismatched_plan = dataclasses.replace(
            preflight_plan,
            manifest_sha256=hashlib.sha256(build_raw).hexdigest(),
            manifest_bytes=len(build_raw),
        )
        downloads = []

        def download(bucket: str, key: str, path: Path) -> None:
            downloads.append((bucket, key, path))
            path.write_bytes(build_payloads[path.name])

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "plan manifest"):
                execute_v37_worker(
                    mismatched_plan,
                    root=Path(directory) / "phase",
                    manifest_raw=build_raw,
                    binary=Path("/opt/v37"),
                    instance_id="i-v37-fixture",
                    download=download,
                    execute=lambda *_arguments: self.fail("science executed"),
                    upload_once=lambda *_arguments: self.fail("result uploaded"),
                )
        self.assertEqual(downloads, [])

        ec2 = _DeniedEC2()
        manifest_key = _s3_object_key(mismatched_plan.manifest_uri)
        with self.assertRaisesRegex(ValueError, "plan manifest"):
            run_v37_spot_phase(
                mismatched_plan,
                ec2_client=ec2,
                s3_client=_S3({manifest_key: build_raw}),
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(ec2.launches, [])

    def test_v37_build_controller_rejects_missing_preflight_before_launch(self) -> None:
        plan, objects = _controller_plan_fixture("build-ownership")
        ec2 = _DeniedEC2()
        with self.assertRaisesRegex(ValueError, "preflight terminal is required"):
            run_v37_spot_phase(
                plan,
                ec2_client=ec2,
                s3_client=_S3(objects),
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(ec2.launches, [])

    def test_v37_build_controller_authenticates_preflight_before_launch(self) -> None:
        plan, terminal_uri, objects = _admitted_controller_fixture()
        ec2 = _DeniedEC2()
        with self.assertRaisesRegex(_AwsError, "UnauthorizedOperation"):
            run_v37_spot_phase(
                plan,
                preflight_terminal_uri=terminal_uri,
                ec2_client=ec2,
                s3_client=_S3(objects),
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(len(ec2.launches), 1)

    def test_v37_build_admission_requires_bound_successful_preflight(self) -> None:
        fixture = _preflight_admission_fixture()
        result = validate_v37_preflight_admission(**fixture)
        self.assertEqual(result["preflight_evidence"]["scalar_fused_max_ulp_delta"], 0)
        self.assertGreaterEqual(
            result["training_evidence"]["partition_coordinate_scores_per_second"],
            20_000_000,
        )
        for field, mutation in (
            (
                "preflight_result_raw",
                fixture["preflight_result_raw"].replace(
                    PREFLIGHT_COORDINATE_SHA256.encode(), b"0" * 64
                ),
            ),
            (
                "preflight_terminal_raw",
                fixture["preflight_terminal_raw"].replace(
                    b'"status":"complete"', b'"status":"failed"'
                ),
            ),
            (
                "build_manifest_raw",
                fixture["build_manifest_raw"].replace(
                    b'"role":"v37-authority"', b'"role":"v37-authoritx"'
                ),
            ),
        ):
            changed = {**fixture, field: mutation}
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    validate_v37_preflight_admission(**changed)

        preflight_manifest_raw = fixture["preflight_manifest_raw"]
        coherently_rehashed_wrong_phase = {
            **fixture,
            "build_plan": dataclasses.replace(
                fixture["build_plan"],
                manifest_sha256=hashlib.sha256(preflight_manifest_raw).hexdigest(),
                manifest_bytes=len(preflight_manifest_raw),
            ),
            "build_manifest_raw": preflight_manifest_raw,
        }
        with self.assertRaisesRegex(ValueError, "successor manifest"):
            validate_v37_preflight_admission(**coherently_rehashed_wrong_phase)

    def test_v37_native_process_monitor_preserves_progress_and_resource_evidence(self) -> None:
        progress_line = (
            '{"completed_internal_nodes":1,"partition_coordinate_scores":20000000,'
            '"schema":"borsuk-v37-training-progress-v1","sequence":1,'
            '"total_internal_nodes":1}\\n'
        )
        program = (
            "import sys;"
            f"sys.stderr.write({progress_line!r});sys.stderr.flush();"
            "sys.stdout.write('result\\n');sys.stdout.flush()"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stdout_path = root / "stdout"
            progress_path = root / "progress"
            memory_cgroup = root / "science-cgroup"
            memory_cgroup.mkdir()
            (memory_cgroup / "memory.current").write_text("123456\n")
            (memory_cgroup / "memory.swap.current").write_text("0\n")
            with mock.patch(
                "scripts.run_v37_relation_router_spot._v37_psi_full_avg10",
                return_value=0.0,
            ):
                returncode, sample = run_v37_native_process(
                    [sys.executable, "-c", program],
                    stdout_path,
                    progress_path,
                    poll_seconds=0.01,
                    memory_cgroup=memory_cgroup,
                )
            self.assertEqual(returncode, 0)
            self.assertEqual(stdout_path.read_bytes(), b"result\n")
            self.assertEqual(progress_path.read_text(), progress_line)
            self.assertGreater(sample.elapsed_seconds, 0)
            self.assertEqual(sample.rss_bytes, 123_456)
            self.assertIsNone(sample.coordinate_scores_per_second)

    def test_v37_native_monitor_preserves_success_after_transient_cgroup_disappears(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cgroup = root / "science-cgroup"
            cgroup.mkdir()
            current = cgroup / "memory.current"
            swap = cgroup / "memory.swap.current"
            current.write_text("123456\n")
            swap.write_text("0\n")
            program = (
                "import pathlib,time;"
                "time.sleep(.03);"
                f"pathlib.Path({str(current)!r}).unlink();"
                f"pathlib.Path({str(swap)!r}).unlink();"
                "print('result')"
            )
            with mock.patch(
                "scripts.run_v37_relation_router_spot._v37_psi_full_avg10",
                return_value=0.0,
            ):
                returncode, sample = run_v37_native_process(
                    [sys.executable, "-c", program],
                    root / "stdout",
                    root / "progress",
                    poll_seconds=0.01,
                    memory_cgroup=cgroup,
                )
            self.assertEqual(returncode, 0)
            self.assertEqual(sample.rss_bytes, 123_456)
            self.assertEqual(sample.swap_bytes, 0)

    def test_v37_worker_cli_requires_only_explicit_local_boot_capabilities(self) -> None:
        arguments = [
            "run_v37_relation_router_spot.py",
            "--execute-v37-worker",
            "--root",
            "/var/lib/borsuk-v37-relation/phase",
            "--plan",
            "/var/lib/borsuk-v37-relation/scratch/plan.json",
            "--manifest",
            "/var/lib/borsuk-v37-relation/scratch/manifest.json",
            "--binary",
            "/var/lib/borsuk-v37-relation/scratch/v37-relation-router",
            "--instance-id",
            "i-0123456789abcdef0",
        ]
        invocation = parse_v37_worker_args(arguments)
        self.assertEqual(invocation.root, Path(arguments[3]))
        self.assertEqual(invocation.instance_id, "i-0123456789abcdef0")
        for mutation in (
            arguments[:-2],
            arguments + ["--bucket", "forbidden"],
            arguments + ["--root", "/duplicate"],
        ):
            with self.assertRaises(ValueError):
                parse_v37_worker_args(mutation)

    def test_v37_native_monitor_kills_the_science_unit_on_pressure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cgroup = root / "science-cgroup"
            cgroup.mkdir()
            (cgroup / "memory.current").write_text("3221225473\n")
            (cgroup / "memory.swap.current").write_text("0\n")
            marker = root / "terminated"
            returncode, sample = run_v37_native_process(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                root / "stdout",
                root / "progress",
                poll_seconds=0.01,
                memory_cgroup=cgroup,
                terminate_command=[
                    sys.executable,
                    "-c",
                    f"from pathlib import Path; Path({str(marker)!r}).write_text('yes')",
                ],
            )
            self.assertNotEqual(returncode, 0)
            self.assertEqual(sample.rss_bytes, 3_221_225_473)
            self.assertEqual(marker.read_text(), "yes")

    def test_v37_worker_executes_one_native_process_and_cleans_named_phase_files(self) -> None:
        import blake3

        plan = _plan()
        payloads = {
            role: f"{role}-fixture\n".encode()
            for role in phase_input_roles("build-ownership")
        }
        manifest = {
            "claim_eligible": False,
            "inputs": [
                {
                    "blake3": blake3.blake3(payloads[role]).hexdigest(),
                    "encoded_bytes": len(payloads[role]),
                    "role": role,
                    "sha256": hashlib.sha256(payloads[role]).hexdigest(),
                    "uri": f"s3://fixture/inputs/{role}",
                }
                for role in phase_input_roles("build-ownership")
            ],
            "outputs": [
                {"role": role, "uri": f"{plan.output_prefix}{role}"}
                for role in ("ownership-tree", "ownership")
            ],
            "phase": plan.phase,
            "run_id": plan.run_id,
            "schema": "borsuk-v37-spot-phase-manifest-v1",
            "source_commit": plan.source_commit,
            "workers": 32,
        }
        manifest_raw = canonical_v37_phase_manifest_bytes(manifest)
        plan = dataclasses.replace(
            plan,
            manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
            manifest_bytes=len(manifest_raw),
        )
        executions = []

        def download(_bucket: str, _key: str, path: Path) -> None:
            path.write_bytes(payloads[path.name])

        def execute(command, staged, stdout_path, progress_path):  # noqa: ANN001
            executions.append(command)
            artifacts = []
            for output in staged.outputs:
                body = f"{output.role}-body\n".encode()
                output.path.write_bytes(body)
                artifacts.append(
                    {
                        "blake3": blake3.blake3(body).hexdigest(),
                        "encoded_bytes": len(body),
                        "role": output.role,
                        "sha256": hashlib.sha256(body).hexdigest(),
                        "uri": f"file://{output.path}",
                    }
                )
            evidence = {
                "dimensions": 192,
                "fma_backend": "aarch64-neon-fma",
                "leaf_count": 3,
                "partition_coordinate_scores": 40_000_000,
                "partition_coordinate_scores_per_second": 20_000_000,
                "partition_scoring_elapsed_ns": 2_000_000_000,
                "rows": 1_000_000,
                "training_elapsed_ns": 2_100_000_000,
            }
            result = {
                "artifacts": artifacts,
                "claim_eligible": False,
                "inputs": [
                    {
                        "blake3": item.blake3,
                        "encoded_bytes": item.encoded_bytes,
                        "role": item.role,
                        "sha256": item.sha256,
                        "uri": item.uri,
                    }
                    for item in staged.inputs
                ],
                "mode": staged.phase,
                "schema": "borsuk-v37-local-result-v3",
                "training_evidence": evidence,
            }
            stdout_path.write_bytes(
                json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            )
            progress_path.write_bytes(
                b'{"completed_internal_nodes":1,"partition_coordinate_scores":20000000,"schema":"borsuk-v37-training-progress-v1","sequence":1,"total_internal_nodes":2}\n'
                b'{"completed_internal_nodes":2,"partition_coordinate_scores":40000000,"schema":"borsuk-v37-training-progress-v1","sequence":2,"total_internal_nodes":2}\n'
            )
            return 0, V37MonitorSample(12, 0, 20_000_000, 2_000_000_000, 0, 0)

        uploads = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "phase"
            terminal_raw = execute_v37_worker(
                plan,
                root=root,
                manifest_raw=manifest_raw,
                binary=Path("/opt/v37"),
                instance_id="i-v37-fixture",
                download=download,
                execute=execute,
                upload_once=lambda bucket, key, body: uploads.append(
                    (bucket, key, body)
                ),
            )
            self.assertFalse(root.exists())
        self.assertEqual(len(executions), 1)
        self.assertTrue(executions[0][0].endswith("/opt/v37"))
        self.assertTrue(uploads[-1][1].endswith("ATTEMPT_TERMINAL.json"))
        validate_v37_terminal_bytes(terminal_raw, plan, "complete")

    def test_v37_worker_preserves_bounded_native_failure_before_cleanup(self) -> None:
        plan = _plan("preflight-training")
        manifest_raw, payloads = _phase_manifest_fixture(plan)
        plan = dataclasses.replace(
            plan,
            manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
            manifest_bytes=len(manifest_raw),
        )
        native_stderr = b"first-line\n" + b"x" * 70_000 + b"\nlast-line\n"

        def download(_bucket: str, _key: str, path: Path) -> None:
            path.write_bytes(payloads[path.name])

        def execute(_command, _staged, _stdout_path, progress_path):  # noqa: ANN001
            progress_path.write_bytes(native_stderr)
            return 23, V37MonitorSample(1, 0, None, 123_456, 0, 0)

        uploads = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "phase"
            with self.assertRaisesRegex(RuntimeError, "native worker exited 23"):
                execute_v37_worker(
                    plan,
                    root=root,
                    manifest_raw=manifest_raw,
                    binary=Path("/opt/v37"),
                    instance_id="i-v37-fixture",
                    download=download,
                    execute=execute,
                    upload_once=lambda bucket, key, body: uploads.append(
                        (bucket, key, body)
                    ),
                )
            self.assertFalse(root.exists())

        self.assertEqual(len(uploads), 1)
        bucket, key, diagnostic = uploads[0]
        self.assertEqual(bucket, "fixture")
        self.assertEqual(
            key,
            "results/preflight-training/WORKER_FAILURE.log",
        )
        self.assertLessEqual(len(diagnostic), 65_536)
        self.assertTrue(diagnostic.startswith(b"returncode=23\n"))
        self.assertTrue(diagnostic.endswith(b"\nlast-line\n"))
        self.assertNotIn(b"first-line", diagnostic)

    def test_v37_controller_never_masks_hard_launch_failures_as_spot_capacity(self) -> None:
        plan, objects = _controller_plan_fixture("preflight-training")
        ec2 = _DeniedEC2()
        with self.assertRaisesRegex(_AwsError, "UnauthorizedOperation"):
            run_v37_spot_phase(
                plan,
                ec2_client=ec2,
                s3_client=_S3(objects),
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(len(ec2.launches), 1)

    def test_v37_native_result_and_progress_are_recomputed_before_publication(self) -> None:
        import blake3

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = _plan()
            inputs = []
            for role in phase_input_roles("build-ownership"):
                raw = f"{role}-fixture\n".encode()
                path = root / role
                path.write_bytes(raw)
                inputs.append(
                    V37StagedArtifact(
                        role=role,
                        path=path,
                        uri=f"s3://fixture/inputs/{role}",
                        sha256=hashlib.sha256(raw).hexdigest(),
                        blake3=blake3.blake3(raw).hexdigest(),
                        encoded_bytes=len(raw),
                    )
                )
            outputs = []
            artifact_values = []
            for role in ("ownership-tree", "ownership"):
                raw = f"{role}-output\n".encode()
                path = root / role
                path.write_bytes(raw)
                outputs.append(
                    V37StagedOutput(
                        role=role,
                        path=path,
                        uri=f"{plan.output_prefix}{role}",
                    )
                )
                artifact_values.append(
                    {
                        "blake3": blake3.blake3(raw).hexdigest(),
                        "encoded_bytes": len(raw),
                        "role": role,
                        "sha256": hashlib.sha256(raw).hexdigest(),
                        "uri": f"file://{path}",
                    }
                )
            staged = V37StagedPhase(
                phase="build-ownership",
                run_id="v37-fixture-build-ownership",
                source_commit=SOURCE_COMMIT,
                workers=32,
                inputs=tuple(inputs),
                outputs=tuple(outputs),
            )
            result = {
                "artifacts": artifact_values,
                "claim_eligible": False,
                "inputs": [
                    {
                        "blake3": item.blake3,
                        "encoded_bytes": item.encoded_bytes,
                        "role": item.role,
                        "sha256": item.sha256,
                        "uri": item.uri,
                    }
                    for item in staged.inputs
                ],
                "mode": staged.phase,
                "schema": "borsuk-v37-local-result-v3",
                "training_evidence": {
                    "dimensions": 192,
                    "fma_backend": "aarch64-neon-fma",
                    "leaf_count": 123,
                    "partition_coordinate_scores": 40_000_000,
                    "partition_coordinate_scores_per_second": 20_000_000,
                    "partition_scoring_elapsed_ns": 2_000_000_000,
                    "rows": 1_000_000,
                    "training_elapsed_ns": 2_100_000_000,
                },
            }
            raw = json.dumps(result, sort_keys=True, separators=(",", ":")).encode() + b"\n"
            validated = validate_v37_local_result_bytes(raw, staged)
            self.assertEqual(validated["training_evidence"]["rows"], 1_000_000)
            for changed in (
                {**result, "claim_eligible": True},
                {
                    **result,
                    "training_evidence": {
                        **result["training_evidence"],
                        "partition_coordinate_scores_per_second": 19_999_999,
                    },
                },
            ):
                mutated = json.dumps(changed, sort_keys=True, separators=(",", ":")).encode() + b"\n"
                with self.assertRaises(ValueError):
                    validate_v37_local_result_bytes(mutated, staged)

            previous = None
            for sequence, scores in ((1, 20_000_000), (2, 40_000_000)):
                progress = {
                    "completed_internal_nodes": sequence,
                    "partition_coordinate_scores": scores,
                    "schema": "borsuk-v37-training-progress-v1",
                    "sequence": sequence,
                    "total_internal_nodes": 122,
                }
                progress_raw = json.dumps(progress, sort_keys=True, separators=(",", ":")).encode() + b"\n"
                previous = validate_v37_progress_bytes(progress_raw, previous)
            with self.assertRaises(ValueError):
                validate_v37_progress_bytes(progress_raw, previous)

            uploads: list[tuple[str, str, bytes]] = []
            terminal_raw = publish_v37_worker_success(
                plan,
                staged,
                instance_id="i-v37-fixture",
                result_raw=raw,
                final_progress_raw=json.dumps(
                    {
                        "completed_internal_nodes": 122,
                        "partition_coordinate_scores": 40_000_000,
                        "schema": "borsuk-v37-training-progress-v1",
                        "sequence": 122,
                        "total_internal_nodes": 122,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
                + b"\n",
                monitor={
                    "elapsed_milliseconds": 12_000,
                    "peak_psi_full_ppm": 0,
                    "peak_rss_bytes": 2_000_000_000,
                    "swap_end_bytes": 0,
                    "swap_start_bytes": 0,
                },
                upload_once=lambda bucket, key, body: uploads.append(
                    (bucket, key, body)
                ),
            )
            terminal = validate_v37_terminal_bytes(terminal_raw, plan, "complete")
            self.assertEqual(
                [key for _, key, _ in uploads],
                [
                    "results/build-ownership/ownership-tree",
                    "results/build-ownership/ownership",
                    "results/build-ownership/local-result.json",
                    "results/build-ownership/progress.json",
                    "results/build-ownership/ATTEMPT_TERMINAL.json",
                ],
            )
            self.assertEqual(terminal["result"]["sha256"], hashlib.sha256(raw).hexdigest())

    def test_v37_ceiling_result_is_validated_and_published_without_training_progress(self) -> None:
        import blake3

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = _plan("evaluate-ceiling")
            payloads = {
                role: f"{role}-fixture\n".encode()
                for role in phase_input_roles(plan.phase)
            }
            inputs = tuple(
                V37StagedArtifact(
                    role=role,
                    path=root / role,
                    uri=f"s3://fixture/inputs/{role}",
                    sha256=hashlib.sha256(payloads[role]).hexdigest(),
                    blake3=blake3.blake3(payloads[role]).hexdigest(),
                    encoded_bytes=len(payloads[role]),
                )
                for role in phase_input_roles(plan.phase)
            )
            for item in inputs:
                item.path.write_bytes(payloads[item.role])
            output_path = root / "ceiling"
            staged = V37StagedPhase(
                phase=plan.phase,
                run_id=plan.run_id,
                source_commit=plan.source_commit,
                workers=32,
                inputs=inputs,
                outputs=(
                    V37StagedOutput(
                        role="ceiling",
                        path=output_path,
                        uri=f"{plan.output_prefix}ceiling",
                    ),
                ),
            )
            identity = {
                item.role: {
                    "blake3": item.blake3,
                    "encoded_bytes": item.encoded_bytes,
                    "role": item.role,
                    "sha256": item.sha256,
                    "uri": item.uri,
                }
                for item in inputs
            }
            authority = {
                "construction_authority": {
                    "blake3": "41" * 32,
                    "encoded_bytes": 512,
                    "role": "v37-authority",
                    "sha256": "42" * 32,
                    "uri": "s3://fixture/inputs/v37-authority",
                },
                "development_ground_truth": identity["development-ground-truth"],
                "gt_neighbors": 100,
                "ownership": identity["ownership"],
                "ownership_tree": identity["ownership-tree"],
                "query_count": 2,
                "schema": "borsuk-v37-ceiling-authority-v1",
                "selected_postings": 14,
            }
            ceiling = {
                "aggregate_gate_ppm": 998_000,
                "aggregate_recall_ppm": 900_000,
                "claim_eligible": False,
                "disposition": "layout-rejected",
                "gt_neighbors": 100,
                "minimum_gate_ppm": 800_000,
                "minimum_recall_ppm": 800_000,
                "passed": False,
                "samples": [
                    {
                        "hits": 100,
                        "query_ordinal": 0,
                        "recall_ppm": 1_000_000,
                        "selected_postings": [0],
                    },
                    {
                        "hits": 80,
                        "query_ordinal": 1,
                        "recall_ppm": 800_000,
                        "selected_postings": [1],
                    },
                ],
                "schema": "borsuk-v37-layout-ceiling-v1",
                "selected_postings_limit": 14,
            }
            result = {
                "ceiling": ceiling,
                "ceiling_authority": identity["ceiling-authority"],
                "claim_eligible": False,
                "inputs": authority,
                "schema": "borsuk-v37-bound-ceiling-v1",
            }
            result_raw = json.dumps(
                result, sort_keys=True, separators=(",", ":")
            ).encode() + b"\n"
            output_path.write_bytes(result_raw)
            validated = validate_v37_local_result_bytes(result_raw, staged)
            self.assertEqual(validated["ceiling"]["disposition"], "layout-rejected")

            uploads: list[tuple[str, str, bytes]] = []
            terminal_raw = publish_v37_worker_success(
                plan,
                staged,
                instance_id="i-v37-fixture",
                result_raw=result_raw,
                final_progress_raw=None,
                monitor={
                    "elapsed_milliseconds": 1_000,
                    "peak_psi_full_ppm": 0,
                    "peak_rss_bytes": 1_000_000,
                    "swap_end_bytes": 0,
                    "swap_start_bytes": 0,
                },
                upload_once=lambda bucket, key, body: uploads.append(
                    (bucket, key, body)
                ),
            )
            terminal = validate_v37_terminal_bytes(terminal_raw, plan, "complete")
            self.assertIsNone(terminal["progress"])
            self.assertEqual(
                [key for _, key, _ in uploads],
                [
                    "results/evaluate-ceiling/ceiling",
                    "results/evaluate-ceiling/local-result.json",
                    "results/evaluate-ceiling/ATTEMPT_TERMINAL.json",
                ],
            )

    def test_v37_manifest_stages_only_exact_objects_and_builds_local_only_command(self) -> None:
        import blake3

        roles = phase_input_roles("evaluate-ceiling")
        payloads = {role: f"{role}-fixture\n".encode() for role in roles}
        inputs = []
        for role in roles:
            raw = payloads[role]
            inputs.append(
                {
                    "blake3": blake3.blake3(raw).hexdigest(),
                    "encoded_bytes": len(raw),
                    "role": role,
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "uri": f"s3://fixture/inputs/{role}",
                }
            )
        manifest = {
            "claim_eligible": False,
            "inputs": inputs,
            "outputs": [{"role": "ceiling", "uri": "s3://fixture/results/ceiling.json"}],
            "phase": "evaluate-ceiling",
            "run_id": "v37-fixture-evaluate-ceiling",
            "schema": "borsuk-v37-spot-phase-manifest-v1",
            "source_commit": SOURCE_COMMIT,
            "workers": 32,
        }
        raw = canonical_v37_phase_manifest_bytes(manifest)
        self.assertEqual(
            raw,
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode() + b"\n",
        )

        reads: list[tuple[str, str]] = []

        def download(bucket: str, key: str, path: Path) -> None:
            reads.append((bucket, key))
            path.write_bytes(payloads[path.name])

        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(
                Path,
                "read_bytes",
                side_effect=AssertionError("staged inputs must be streamed"),
            ):
                staged = stage_v37_phase_inputs(raw, Path(directory), download)
            command = build_v37_binary_command(Path("/opt/v37"), staged)
            self.assertEqual(reads, [("fixture", f"inputs/{role}") for role in roles])
            self.assertEqual(command[:6], ["/opt/v37", "--execute-v37-local", "--mode", "evaluate-ceiling", "--workers", "32"])
            self.assertEqual(command.count("--ceiling-authority"), 1)
            self.assertIn("--ceiling-output", command)
            flags = {value for value in command if value.startswith("--")}
            self.assertTrue(flags.isdisjoint({"--bucket", "--endpoint", "--execute-d3"}))

        changed = json.loads(raw)
        changed["inputs"][0]["role"] = "source"
        with self.assertRaises(ValueError):
            canonical_v37_phase_manifest_bytes(changed)

    def test_v37_spot_plan_is_exact_causality_multi_az_and_spot_only(self) -> None:
        plan = _plan()
        self.assertEqual(PROFILE, "causality")
        self.assertEqual(REGION, "eu-central-1")
        self.assertEqual(AMI_ID, "ami-07bcecd13a160173f")
        self.assertEqual(INSTANCE_TYPE, "c7g.8xlarge")
        self.assertEqual(INSTANCE_PROFILE, "borsuk-bench-profile")
        self.assertEqual(
            [(target.availability_zone, target.subnet_id) for target in SPOT_TARGETS],
            [
                ("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
                ("eu-central-1b", "subnet-00243d923761c047c"),
                ("eu-central-1a", "subnet-034528fbd6977848f"),
            ],
        )
        user_data = "#!/bin/bash\nshutdown -h now\n"
        specs = build_v37_launch_specs(plan, user_data=user_data)
        repeated_specs = build_v37_launch_specs(plan, user_data=user_data)
        self.assertEqual(len(specs), 3)
        self.assertEqual(len({spec["ClientToken"] for spec in specs}), 3)
        self.assertEqual(
            [spec["ClientToken"] for spec in specs],
            [spec["ClientToken"] for spec in repeated_specs],
        )
        for spec, target in zip(specs, SPOT_TARGETS, strict=True):
            self.assertEqual(spec["UserData"], "#!/bin/bash\nshutdown -h now\n")
            self.assertEqual(spec["SubnetId"], target.subnet_id)
            self.assertEqual(spec["Placement"]["AvailabilityZone"], target.availability_zone)
            self.assertEqual(spec["IamInstanceProfile"], {"Name": INSTANCE_PROFILE})
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(
                spec["InstanceMarketOptions"]["SpotOptions"],
                {
                    "InstanceInterruptionBehavior": "terminate",
                    "SpotInstanceType": "one-time",
                },
            )
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
            self.assertEqual(spec["MetadataOptions"]["HttpTokens"], "required")
            self.assertEqual(
                spec["BlockDeviceMappings"][0]["Ebs"],
                {
                    "DeleteOnTermination": True,
                    "Encrypted": True,
                    "Iops": 3000,
                    "Throughput": 250,
                    "VolumeSize": 200,
                    "VolumeType": "gp3",
                },
            )

    def test_v37_phases_are_capability_separated_and_relation_admission_is_bound(self) -> None:
        self.assertEqual(phase_input_roles("preflight-training"), ("v37-authority",))
        self.assertEqual(
            phase_input_roles("build-ownership"),
            (
                "v36-authority",
                "v36-execution-authority",
                "v36-receipt",
                "v36-source-registry",
                "v37-authority",
                "source",
            ),
        )
        self.assertEqual(
            phase_input_roles("evaluate-ceiling"),
            (
                "ceiling-authority",
                "development-ground-truth",
                "ownership-tree",
                "ownership",
            ),
        )
        self.assertNotIn("development-ground-truth", phase_input_roles("build-ownership"))
        self.assertNotIn("source", phase_input_roles("evaluate-ceiling"))
        common = {
            "source_sha256": "31" * 32,
            "projection_sha256": "32" * 32,
            "ownership_tree_sha256": "33" * 32,
            "ownership_sha256": "34" * 32,
        }
        ceiling = {**common, "schema": "borsuk-v37-ceiling-passed-v1", "passed": True}
        direct = {**common, "schema": "borsuk-v37-direct-failed-v1", "passed": False}
        validate_v37_relation_predecessors(ceiling, direct)
        for changed in (
            {**ceiling, "passed": False},
            {**direct, "ownership_sha256": "35" * 32},
        ):
            with self.assertRaises(ValueError):
                validate_v37_relation_predecessors(changed, direct)

    def test_v37_preflight_manifest_and_result_are_exactly_bounded(self) -> None:
        import blake3

        plan = _plan("preflight-training")
        authority_raw = b"v37-authority-fixture\n"
        identity = {
            "blake3": blake3.blake3(authority_raw).hexdigest(),
            "encoded_bytes": len(authority_raw),
            "role": "v37-authority",
            "sha256": hashlib.sha256(authority_raw).hexdigest(),
            "uri": "s3://fixture/inputs/v37-authority",
        }
        manifest = {
            "claim_eligible": False,
            "inputs": [identity],
            "outputs": [],
            "phase": plan.phase,
            "run_id": plan.run_id,
            "schema": "borsuk-v37-spot-phase-manifest-v1",
            "source_commit": plan.source_commit,
            "workers": 32,
        }
        manifest_raw = canonical_v37_phase_manifest_bytes(manifest)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def download(_bucket: str, _key: str, path: Path) -> None:
                path.write_bytes(authority_raw)

            staged = stage_v37_phase_inputs(manifest_raw, root, download)
            self.assertEqual(staged.phase, "preflight-training")
            self.assertEqual([item.role for item in staged.inputs], ["v37-authority"])
            self.assertEqual(staged.outputs, ())
            result = {
                "artifacts": [],
                "claim_eligible": False,
                "inputs": [identity],
                "mode": "preflight-training",
                "preflight_evidence": {
                    "coordinate_generator": "splitmix23-f32-v1",
                    "coordinate_sha256": PREFLIGHT_COORDINATE_SHA256,
                    "projected_construction_bytes": 2_137_615_120,
                    "scalar_fused_comparisons": 15_360,
                    "scalar_fused_max_ulp_delta": 0,
                },
                "schema": "borsuk-v37-local-result-v3",
                "training_evidence": {
                    "dimensions": 192,
                    "fma_backend": "aarch64-neon-fma",
                    "leaf_count": 16,
                    "partition_coordinate_scores": PREFLIGHT_COORDINATE_SCORES,
                    "partition_coordinate_scores_per_second": 25_165_824,
                    "partition_scoring_elapsed_ns": 2_000_000_000,
                    "rows": 65_536,
                    "training_elapsed_ns": 2_100_000_000,
                },
            }
            raw = json.dumps(result, sort_keys=True, separators=(",", ":")).encode() + b"\n"
            self.assertEqual(validate_v37_local_result_bytes(raw, staged), result)
            mutations = (
                ("training_evidence", "rows", 65_535),
                ("preflight_evidence", "coordinate_generator", "other"),
                ("preflight_evidence", "coordinate_sha256", "36" * 32),
                ("preflight_evidence", "projected_construction_bytes", 1),
                ("preflight_evidence", "scalar_fused_comparisons", 15_359),
                ("preflight_evidence", "scalar_fused_max_ulp_delta", 1),
            )
            for section, field, replacement in mutations:
                mutated = json.loads(raw)
                mutated[section][field] = replacement
                mutated_raw = (
                    json.dumps(mutated, sort_keys=True, separators=(",", ":")).encode()
                    + b"\n"
                )
                with self.subTest(section=section, field=field):
                    with self.assertRaisesRegex(ValueError, "preflight"):
                        validate_v37_local_result_bytes(mutated_raw, staged)
            mutated = json.loads(raw)
            mutated["training_evidence"]["partition_coordinate_scores"] = (
                PREFLIGHT_COORDINATE_SCORES - 1
            )
            mutated["training_evidence"][
                "partition_coordinate_scores_per_second"
            ] = 25_165_823
            mutated_raw = (
                json.dumps(mutated, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "preflight"):
                validate_v37_local_result_bytes(mutated_raw, staged)

    def test_v37_preflight_science_sandbox_has_no_output_capability(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = root / "inputs"
            outputs = root / "outputs"
            inputs.mkdir()
            outputs.mkdir()
            authority = inputs / "v37-authority"
            authority.write_bytes(b"authority\n")
            staged = V37StagedPhase(
                phase="preflight-training",
                run_id="v37-fixture-preflight-training",
                source_commit=SOURCE_COMMIT,
                workers=32,
                inputs=(
                    V37StagedArtifact(
                        role="v37-authority",
                        path=authority,
                        uri="s3://fixture/v37-authority",
                        sha256="31" * 32,
                        blake3="32" * 32,
                        encoded_bytes=10,
                    ),
                ),
                outputs=(),
            )
            captured = []

            def runner(command, stdout, progress):  # noqa: ANN001
                captured.append((command, stdout, progress))
                return 0, V37MonitorSample(1, 0, 21_000_000, 1, 0, 0)

            execute_v37_native_science(
                ["/opt/v37-relation-router", "--execute-v37-local"],
                staged,
                root / "stdout",
                root / "progress",
                runner=runner,
            )
            self.assertEqual(len(captured), 1)
            properties = [part for part in captured[0][0] if part.startswith("--property=")]
            self.assertNotIn(f"--property=ReadWritePaths={outputs}", properties)
            self.assertEqual(outputs.stat().st_mode & 0o777, 0o555)

    def test_v37_preflight_progress_is_terminally_bound_without_artifacts(self) -> None:
        import blake3

        plan = _plan("preflight-training")
        with tempfile.TemporaryDirectory() as directory:
            authority_path = Path(directory) / "v37-authority"
            authority_raw = b"authority\n"
            authority_path.write_bytes(authority_raw)
            staged = V37StagedPhase(
                phase=plan.phase,
                run_id=plan.run_id,
                source_commit=plan.source_commit,
                workers=32,
                inputs=(
                    V37StagedArtifact(
                        role="v37-authority",
                        path=authority_path,
                        uri="s3://fixture/inputs/v37-authority",
                        sha256=hashlib.sha256(authority_raw).hexdigest(),
                        blake3=blake3.blake3(authority_raw).hexdigest(),
                        encoded_bytes=len(authority_raw),
                    ),
                ),
                outputs=(),
            )
            result = {
                "artifacts": [],
                "claim_eligible": False,
                "inputs": [
                    {
                        "blake3": staged.inputs[0].blake3,
                        "encoded_bytes": staged.inputs[0].encoded_bytes,
                        "role": staged.inputs[0].role,
                        "sha256": staged.inputs[0].sha256,
                        "uri": staged.inputs[0].uri,
                    }
                ],
                "mode": plan.phase,
                "preflight_evidence": {
                    "coordinate_generator": "splitmix23-f32-v1",
                    "coordinate_sha256": PREFLIGHT_COORDINATE_SHA256,
                    "projected_construction_bytes": 2_137_615_120,
                    "scalar_fused_comparisons": 15_360,
                    "scalar_fused_max_ulp_delta": 0,
                },
                "schema": "borsuk-v37-local-result-v3",
                "training_evidence": {
                    "dimensions": 192,
                    "fma_backend": "aarch64-neon-fma",
                    "leaf_count": 16,
                    "partition_coordinate_scores": PREFLIGHT_COORDINATE_SCORES,
                    "partition_coordinate_scores_per_second": 25_165_824,
                    "partition_scoring_elapsed_ns": 2_000_000_000,
                    "rows": 65_536,
                    "training_elapsed_ns": 2_100_000_000,
                },
            }
            result_raw = (
                json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            )
            progress_raw = (
                b'{"completed_internal_nodes":15,"partition_coordinate_scores":50331648,'
                b'"schema":"borsuk-v37-training-progress-v1","sequence":15,'
                b'"total_internal_nodes":15}\n'
            )
            uploads = []
            terminal_raw = publish_v37_worker_success(
                plan,
                staged,
                instance_id="i-v37-fixture",
                result_raw=result_raw,
                final_progress_raw=progress_raw,
                monitor={
                    "elapsed_milliseconds": 2_100,
                    "peak_psi_full_ppm": 0,
                    "peak_rss_bytes": 100_000_000,
                    "swap_end_bytes": 0,
                    "swap_start_bytes": 0,
                },
                upload_once=lambda bucket, key, body: uploads.append(
                    (bucket, key, body)
                ),
            )
            terminal = validate_v37_terminal_bytes(terminal_raw, plan, "complete")
            self.assertEqual(terminal["artifacts"], [])
            self.assertEqual(terminal["progress"]["sha256"], hashlib.sha256(progress_raw).hexdigest())
            self.assertEqual(
                [key for _, key, _ in uploads],
                [
                    "results/preflight-training/local-result.json",
                    "results/preflight-training/progress.json",
                    "results/preflight-training/ATTEMPT_TERMINAL.json",
                ],
            )

    def test_v37_worker_has_one_process_group_exact_stops_and_no_prefix_listing(self) -> None:
        worker = build_v37_worker_script(_plan())
        self.assertNotIn("setsid env", worker)
        self.assertIn("memory.current", worker)
        self.assertIn("memory.swap.current", worker)
        self.assertIn("/proc/pressure/memory", worker)
        self.assertIn("3221225472", worker)
        self.assertIn("0.75", worker)
        self.assertIn("600", worker)
        self.assertIn("720", worker)
        self.assertIn("120", worker)
        self.assertIn("20000000", worker)
        self.assertIn("ATTEMPT_TERMINAL.json", worker)
        self.assertNotIn("ATTEMPT_COMPLETE.json", worker)
        self.assertNotIn("ATTEMPT_FAILED.json", worker)
        self.assertIn("sha256sum", worker)
        self.assertIn('chmod 0555 "$binary"', worker)
        self.assertIn('chmod 0711 "$root" "$scratch"', worker)
        self.assertNotIn('chmod 0500 "$binary"', worker)
        self.assertIn("tar --zstd -xf", worker)
        self.assertIn(
            'env PYTHONPATH="$source/.v37-python" python3 -c',
            worker,
        )
        self.assertIn('boto3.__version__ == "1.42.97"', worker)
        self.assertIn('blake3.__version__ == "1.0.8"', worker)
        self.assertIn(
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
            worker,
        )
        self.assertIn('env PYTHONPATH="$source/.v37-python" python3', worker)
        self.assertNotIn("uv run", worker)
        self.assertNotIn("dnf install", worker)
        self.assertNotIn("pip install", worker)
        self.assertNotIn("boto3==1.34.46", worker)
        requirements = (
            Path(__file__).with_name("requirements-v37-relation-router.txt").read_text()
        )
        self.assertEqual(requirements, "boto3==1.42.97\nblake3==1.0.8\n")
        self.assertIn("latest/api/token", worker)
        self.assertIn("--instance-id", worker)
        self.assertIn("MemoryMax=3G", worker)
        self.assertIn("MemorySwapMax=0", worker)
        self.assertIn("shutdown -h now", worker)
        self.assertIn("BOOT_FAILURE.log", worker)
        self.assertIn('aws s3 cp "$boot_log"', worker)
        self.assertIn('if test -s "$boot_log"; then', worker)
        self.assertNotIn('test "$status" -ne 0 && test -s "$boot_log"', worker)
        self.assertNotIn('sync -f "$boot_log"', worker)
        self.assertIn('systemctl start "$slice"', worker)
        self.assertLess(
            worker.index('exec >"$boot_log" 2>&1'),
            worker.index('trap cleanup EXIT INT TERM'),
        )
        self.assertLess(
            worker.index('trap cleanup EXIT INT TERM'),
            worker.index('systemctl start "$slice"'),
        )
        self.assertLess(
            worker.index('systemctl start "$slice"'),
            worker.index('systemctl set-property --runtime "$slice"'),
        )
        self.assertNotIn("list-objects", worker)
        self.assertNotIn("s3 ls", worker)
        self.assertNotIn('"$binary" --bucket', worker)
        self.assertNotIn("--endpoint", worker)
        self.assertNotIn("--execute-d3", worker)
        self.assertNotIn("find ", worker)
        self.assertNotIn("--worker", worker)

    def test_v37_native_science_service_has_no_network_or_storage_capability(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = root / "inputs"
            outputs = root / "outputs"
            inputs.mkdir()
            outputs.mkdir()
            staged = V37StagedPhase(
                phase="build-ownership",
                run_id="v37-fixture-build-ownership",
                source_commit=SOURCE_COMMIT,
                workers=32,
                inputs=(),
                outputs=(
                    V37StagedOutput(
                        role="ownership",
                        path=outputs / "ownership",
                        uri="s3://fixture/results/ownership",
                    ),
                ),
            )
            native = ["/opt/v37-relation-router", "--execute-v37-local"]
            command = build_v37_science_service_command(
                native,
                staged,
                binary=Path(native[0]),
                inputs=inputs,
                outputs=outputs,
            )
        joined = " ".join(command)
        self.assertEqual(command[:3], ["systemd-run", "--wait", "--pipe"])
        self.assertNotIn("--collect", command)
        for boundary in (
            "User=65534",
            "Group=65534",
            "PrivateNetwork=yes",
            "ProtectSystem=strict",
            "ProtectHome=yes",
            "NoNewPrivileges=yes",
            "PrivateDevices=yes",
            "CapabilityBoundingSet=",
            "RestrictAddressFamilies=AF_UNIX",
            "MemoryMax=3221225472",
            "MemorySwapMax=0",
            "RuntimeMaxSec=600",
            "MemoryAccounting=yes",
            f"ReadOnlyPaths={inputs}",
            f"ReadOnlyPaths={native[0]}",
            f"ReadWritePaths={outputs}",
        ):
            self.assertIn(boundary, joined)
        self.assertEqual(command[-2:], native)
        self.assertNotIn("AWS_", joined)
        self.assertNotIn("s3://", joined)
        self.assertNotIn("User=nobody", joined)
        self.assertNotIn("Group=nogroup", joined)

    def test_v37_worker_and_science_share_one_aggregate_memory_slice(self) -> None:
        plan = _plan("build-ownership")
        expected_slice = "borsuk-v37-5a2a113184f7b4de6bfcd4cf.slice"
        worker = build_v37_worker_script(plan)
        self.assertIn(f"slice={expected_slice}\n", worker)
        self.assertIn(
            'systemctl set-property --runtime "$slice" '
            "MemoryMax=3221225472 MemorySwapMax=0 MemoryAccounting=yes",
            worker,
        )
        self.assertIn('--slice="$slice"', worker)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = root / "inputs"
            outputs = root / "outputs"
            inputs.mkdir()
            outputs.mkdir()
            staged = V37StagedPhase(
                phase=plan.phase,
                run_id=plan.run_id,
                source_commit=plan.source_commit,
                workers=32,
                inputs=(),
                outputs=(),
            )
            command = build_v37_science_service_command(
                ["/opt/v37-relation-router", "--execute-v37-local"],
                staged,
                binary=Path("/opt/v37-relation-router"),
                inputs=inputs,
                outputs=outputs,
            )
        self.assertIn(f"--slice={expected_slice}", command)

    def test_v37_worker_executes_native_command_only_through_science_service(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = root / "inputs"
            outputs = root / "outputs"
            inputs.mkdir()
            outputs.mkdir()
            input_path = inputs / "source"
            input_path.write_bytes(b"source")
            output_path = outputs / "ownership"
            staged = V37StagedPhase(
                phase="build-ownership",
                run_id="v37-fixture-build-ownership",
                source_commit=SOURCE_COMMIT,
                workers=32,
                inputs=(
                    V37StagedArtifact(
                        role="source",
                        path=input_path,
                        uri="s3://fixture/source",
                        sha256="31" * 32,
                        blake3="32" * 32,
                        encoded_bytes=6,
                    ),
                ),
                outputs=(
                    V37StagedOutput(
                        role="ownership",
                        path=output_path,
                        uri="s3://fixture/ownership",
                    ),
                ),
            )
            captured = []

            def runner(command, stdout, progress):  # noqa: ANN001
                captured.append((command, stdout, progress))
                return 0, V37MonitorSample(1, 0, 21_000_000, 1, 0, 0)

            native = ["/opt/v37-relation-router", "--execute-v37-local"]
            execute_v37_native_science(
                native,
                staged,
                root / "stdout",
                root / "progress",
                runner=runner,
            )
            self.assertEqual(captured[0][0][-2:], native)
            self.assertEqual(input_path.stat().st_mode & 0o777, 0o444)
            self.assertEqual(root.stat().st_mode & 0o777, 0o711)
            self.assertEqual(inputs.stat().st_mode & 0o777, 0o555)
            self.assertEqual(outputs.stat().st_mode & 0o777, 0o733)

    def test_v37_monitor_stops_on_each_registered_pressure_or_wedge_boundary(self) -> None:
        healthy = V37MonitorSample(
            elapsed_seconds=300,
            last_progress_seconds=20,
            coordinate_scores_per_second=21_000_000,
            rss_bytes=3_000_000_000,
            psi_full_avg10=0.0,
            swap_bytes=0,
        )
        self.assertIsNone(classify_v37_monitor_sample(healthy))
        mutations = {
            "science-timeout": {"elapsed_seconds": 601},
            "progress-timeout": {"last_progress_seconds": 121},
            "throughput-stop": {"coordinate_scores_per_second": 19_999_999},
            "rss-stop": {"rss_bytes": 3_221_225_473},
            "psi-stop": {"psi_full_avg10": 0.76},
            "swap-stop": {"swap_bytes": 1},
        }
        for reason, changed in mutations.items():
            sample = V37MonitorSample(**{**healthy.__dict__, **changed})
            self.assertEqual(classify_v37_monitor_sample(sample), reason)

    def test_v37_terminal_is_canonical_bound_and_controller_always_terminates(self) -> None:
        plan, preflight_terminal_uri, admission_objects = _admitted_controller_fixture()
        terminal = {
            "artifacts": [
                {
                    "blake3": "31" * 32,
                    "encoded_bytes": 8192,
                    "role": "ownership-tree",
                    "sha256": "32" * 32,
                    "uri": f"{plan.output_prefix}ownership-tree",
                },
                {
                    "blake3": "33" * 32,
                    "encoded_bytes": 16384,
                    "role": "ownership",
                    "sha256": "34" * 32,
                    "uri": f"{plan.output_prefix}ownership",
                },
            ],
            "binary_bytes": plan.binary_bytes,
            "binary_sha256": plan.binary_sha256,
            "binary_uri": plan.binary_uri,
            "claim_eligible": False,
            "instance_id": "i-v37-fixture",
            "manifest_bytes": plan.manifest_bytes,
            "manifest_sha256": plan.manifest_sha256,
            "manifest_uri": plan.manifest_uri,
            "monitor": {
                "elapsed_milliseconds": 12_000,
                "peak_psi_full_ppm": 0,
                "peak_rss_bytes": 2_000_000_000,
                "swap_end_bytes": 0,
                "swap_start_bytes": 0,
            },
            "phase": plan.phase,
            "progress": {
                "encoded_bytes": 256,
                "role": "progress",
                "sha256": "35" * 32,
                "uri": f"{plan.output_prefix}progress.json",
            },
            "result": {
                "encoded_bytes": 4096,
                "role": "local-result",
                "sha256": "36" * 32,
                "uri": f"{plan.output_prefix}local-result.json",
            },
            "run_id": plan.run_id,
            "schema": "borsuk-v37-spot-terminal-v2",
            "source_archive_bytes": plan.source_archive_bytes,
            "source_archive_sha256": plan.source_archive_sha256,
            "source_archive_uri": plan.source_archive_uri,
            "source_commit": plan.source_commit,
            "status": "complete",
        }
        raw = canonical_v37_terminal_bytes(terminal, plan)
        self.assertEqual(raw, json.dumps(terminal, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        validate_v37_terminal_bytes(raw, plan, "complete")
        changed = raw.replace(b'"status":"complete"', b'"status":"failed"')
        with self.assertRaises(ValueError):
            validate_v37_terminal_bytes(changed, plan, "complete")

        failed_uploads = []
        failed_raw = publish_v37_worker_failure(
            plan,
            instance_id="i-v37-fixture",
            reason="progress-timeout",
            upload_once=lambda upload_bucket, upload_key, body: failed_uploads.append(
                (upload_bucket, upload_key, body)
            ),
        )
        failed = validate_v37_terminal_bytes(failed_raw, plan, "failed")
        self.assertEqual(failed["reason"], "progress-timeout")
        self.assertTrue(failed_uploads[-1][1].endswith("ATTEMPT_TERMINAL.json"))

        bucket = "fixture"
        key = "results/build-ownership/ATTEMPT_TERMINAL.json"
        ec2 = _EC2()
        s3 = _S3({**admission_objects, (bucket, key): raw}, unavailable_reads=1)
        uri = run_v37_spot_phase(
            plan,
            preflight_terminal_uri=preflight_terminal_uri,
            ec2_client=ec2,
            s3_client=s3,
            sleep=lambda _seconds: None,
            monotonic=iter([0.0, 1.0]).__next__,
        )
        self.assertEqual(uri, f"s3://{bucket}/{key}")
        self.assertEqual(ec2.terminated, ["i-v37-fixture"])
        self.assertEqual(s3.reads[0], (bucket, key))
        self.assertEqual(s3.reads[-1], (bucket, key))
        self.assertEqual(len(s3.reads), 7)

        duplicate_ec2 = _EC2()
        with self.assertRaisesRegex(ValueError, "terminal already exists"):
            run_v37_spot_phase(
                plan,
                preflight_terminal_uri=preflight_terminal_uri,
                ec2_client=duplicate_ec2,
                s3_client=_S3({(bucket, key): raw}),
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(duplicate_ec2.launches, [])

    def test_v37_controller_treats_only_aws_missing_terminal_as_absent(self) -> None:
        plan, objects = _controller_plan_fixture("preflight-training")
        ec2 = _DeniedEC2()
        s3 = _S3(objects, unavailable_reads=1, unavailable_error_code="NoSuchKey")
        with self.assertRaisesRegex(_AwsError, "UnauthorizedOperation"):
            run_v37_spot_phase(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(len(s3.reads), 2)
        self.assertEqual(len(ec2.launches), 1)

        denied_s3 = _S3({}, unavailable_reads=1, unavailable_error_code="AccessDenied")
        with self.assertRaisesRegex(_AwsError, "AccessDenied"):
            run_v37_spot_phase(
                plan,
                ec2_client=_EC2(),
                s3_client=denied_s3,
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )

    def test_v37_controller_seals_boot_failure_after_instance_terminal(self) -> None:
        plan, objects = _controller_plan_fixture("preflight-training")
        ec2 = _StoppedEC2()
        s3 = _S3(objects)
        with self.assertRaisesRegex(RuntimeError, "boot-failure"):
            run_v37_spot_phase(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                sleep=lambda _seconds: None,
                monotonic=iter([0.0, 1.0]).__next__,
            )
        self.assertEqual(ec2.terminated, ["i-v37-fixture"])
        self.assertEqual(len(s3.writes), 1)
        bucket, key, raw = s3.writes[0]
        self.assertEqual(bucket, "fixture")
        self.assertEqual(key, "results/preflight-training/ATTEMPT_TERMINAL.json")
        terminal = validate_v37_terminal_bytes(raw, plan, "failed")
        self.assertEqual(terminal["reason"], "boot-failure")

    def test_v37_controller_terminates_then_seals_wrapper_timeout(self) -> None:
        plan, objects = _controller_plan_fixture("preflight-training")
        ec2 = _EC2()
        s3 = _S3(objects)
        with self.assertRaisesRegex(TimeoutError, "wrapper timed out"):
            run_v37_spot_phase(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                sleep=lambda _seconds: None,
                monotonic=iter([0.0, 721.0]).__next__,
            )
        self.assertEqual(ec2.terminated, ["i-v37-fixture"])
        self.assertEqual(len(s3.writes), 1)
        terminal = validate_v37_terminal_bytes(s3.writes[0][2], plan, "failed")
        self.assertEqual(terminal["reason"], "wrapper-timeout")


if __name__ == "__main__":
    unittest.main()
