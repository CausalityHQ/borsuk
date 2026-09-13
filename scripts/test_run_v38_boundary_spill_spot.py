from __future__ import annotations

import dataclasses
import hashlib
import json
import pathlib
import sys
import tempfile
import time
import unittest
from unittest import mock

from scripts.run_v38_boundary_spill_spot import (
    AMI_ID,
    EXPECTED_AWS_ACCOUNT,
    INSTANCE_TYPE,
    PROFILE,
    REGION,
    SPOT_TARGETS,
    V38MonitorSample,
    V38StagedArtifact,
    V38StagedOutput,
    V38StagedPhase,
    V38WorkerInvocation,
    V38WrapperMonitor,
    build_v38_binary_command,
    build_v38_launch_specs,
    build_v38_science_service_command,
    build_v38_spot_plan,
    build_v38_worker_script,
    canonical_v38_phase_manifest_bytes,
    canonical_v38_terminal_bytes,
    classify_v38_monitor_sample,
    execute_v38_worker,
    main,
    parse_v38_worker_args,
    phase_input_roles,
    phase_output_roles,
    run_v38_causality_phase,
    run_v38_native_process,
    run_v38_spot_phase,
    run_v38_worker_invocation,
    stage_v38_phase_inputs,
    validate_v38_ceiling_predecessor,
    validate_v38_local_result_bytes,
    validate_v38_preflight_admission,
    validate_v38_terminal_bytes,
)

SOURCE_COMMIT = "12" * 20


def _plan(phase: str = "build-spill"):
    return build_v38_spot_plan(
        phase=phase,
        run_id=f"v38-fixture-{phase}",
        source_commit=SOURCE_COMMIT,
        source_archive_uri="s3://fixture/source.tar.zst",
        source_archive_sha256="21" * 32,
        source_archive_bytes=1_024,
        binary_uri="s3://fixture/v38-boundary-spill",
        binary_sha256="22" * 32,
        binary_bytes=2_048,
        manifest_uri=f"s3://fixture/manifest-{phase}.json",
        manifest_sha256="23" * 32,
        manifest_bytes=4_096,
        output_prefix=f"s3://fixture/results/{phase}/",
    )


def _identity(role: str, marker: str) -> dict[str, object]:
    return {
        "blake3": marker * 64,
        "encoded_bytes": 4_096,
        "role": role,
        "sha256": marker * 64,
        "uri": f"s3://fixture/inputs/{role}",
    }


def _preserve_monitor(sample):
    return sample


def _manifest(plan, inputs, outputs, workers: int = 32) -> bytes:
    value = {
        "claim_eligible": False,
        "inputs": inputs,
        "outputs": outputs,
        "phase": plan.phase,
        "run_id": plan.run_id,
        "schema": "borsuk-v38-spot-phase-manifest-v1",
        "source_commit": plan.source_commit,
        "workers": workers,
    }
    return canonical_v38_phase_manifest_bytes(value)


def _terminal(plan, *, artifacts, result_raw: bytes, status: str = "complete") -> bytes:
    result = {
        "encoded_bytes": len(result_raw),
        "role": "local-result",
        "sha256": hashlib.sha256(result_raw).hexdigest(),
        "uri": f"{plan.output_prefix}local-result.json",
    }
    value = {
        "artifacts": artifacts,
        "binary_bytes": plan.binary_bytes,
        "binary_sha256": plan.binary_sha256,
        "binary_uri": plan.binary_uri,
        "claim_eligible": False,
        "instance_id": "i-0123456789abcdef0",
        "manifest_bytes": plan.manifest_bytes,
        "manifest_sha256": plan.manifest_sha256,
        "manifest_uri": plan.manifest_uri,
        "monitor": dataclasses.asdict(V38SpotMonitorTests()._sample(plan.phase)),
        "phase": plan.phase,
        "progress": {
            "encoded_bytes": 100,
            "role": "progress",
            "sha256": "6" * 64,
            "uri": f"{plan.output_prefix}progress.json",
        },
        "result": result,
        "run_id": plan.run_id,
        "schema": "borsuk-v38-spot-terminal-v1",
        "source_archive_bytes": plan.source_archive_bytes,
        "source_archive_sha256": plan.source_archive_sha256,
        "source_archive_uri": plan.source_archive_uri,
        "source_commit": plan.source_commit,
        "status": status,
    }
    return canonical_v38_terminal_bytes(value, plan, status)


def _preflight_chain():
    authority = _identity("v38-authority", "1")
    preflight_plan = _plan("preflight-spill")
    preflight_manifest_raw = _manifest(preflight_plan, [authority], [])
    preflight_plan = dataclasses.replace(
        preflight_plan,
        manifest_sha256=hashlib.sha256(preflight_manifest_raw).hexdigest(),
        manifest_bytes=len(preflight_manifest_raw),
    )
    evidence = {
        "coordinate_generator": "splitmix23-f32-v1",
        "dimensions": 192,
        "leaf_count": 16,
        "predecessor_replay_allowance_ns": 120_000_000_000,
        "projected_construction_elapsed_ns": 449_000_000_000,
        "projected_peak_bytes": 2_783_657_984,
        "projected_scoring_elapsed_ns": 299_000_000_000,
        "projected_spill_post_scoring_elapsed_ns": 30_000_000_000,
        "proposal_coordinate_scores": 188_743_680,
        "proposal_coordinates_per_second": 78_080_000,
        "relation_rows": 70_000,
        "rows": 65_536,
        "scalar_fused_comparisons": 15_360,
        "scalar_fused_max_ulp_delta": 0,
    }
    result = {
        "claim_eligible": False,
        "inputs": [authority],
        "mode": "preflight-spill",
        "preflight_evidence": evidence,
        "schema": "borsuk-v38-local-result-v1",
    }
    result_raw = json.dumps(result, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    terminal_raw = _terminal(preflight_plan, artifacts=[], result_raw=result_raw)

    build_plan = _plan("build-spill")
    build_inputs = [authority]
    for index, role in enumerate(phase_input_roles("build-spill")[1:], start=2):
        build_inputs.append(_identity(role, format(index, "x")))
    build_outputs = [
        {"role": role, "uri": f"{build_plan.output_prefix}{role}"}
        for role in phase_output_roles("build-spill")
    ]
    build_manifest_raw = _manifest(build_plan, build_inputs, build_outputs)
    build_plan = dataclasses.replace(
        build_plan,
        manifest_sha256=hashlib.sha256(build_manifest_raw).hexdigest(),
        manifest_bytes=len(build_manifest_raw),
    )
    return {
        "preflight_plan": preflight_plan,
        "build_plan": build_plan,
        "build_manifest_raw": build_manifest_raw,
        "preflight_manifest_raw": preflight_manifest_raw,
        "preflight_result_raw": result_raw,
        "preflight_terminal_raw": terminal_raw,
    }


class _Body:
    def __init__(self, raw: bytes) -> None:
        self.raw = raw
        self.offset = 0

    def read(self, amount: int | None = None) -> bytes:
        end = len(self.raw) if amount is None else min(len(self.raw), self.offset + amount)
        chunk = self.raw[self.offset : end]
        self.offset = end
        return chunk


class _S3:
    def __init__(self, objects, *, unavailable_reads: int = 0, fail_write_suffix=None):
        self.objects = objects
        self.unavailable_reads = unavailable_reads
        self.fail_write_suffix = fail_write_suffix
        self.reads = []
        self.writes = []

    def get_object(self, *, Bucket, Key, **_request):
        self.reads.append((Bucket, Key))
        if len(self.reads) <= self.unavailable_reads:
            raise KeyError(Key)
        raw = self.objects[(Bucket, Key)]
        return {"Body": _Body(raw), "ContentLength": len(raw)}

    def put_object(self, *, Bucket, Key, Body, IfNoneMatch, **_request):
        if self.fail_write_suffix is not None and Key.endswith(self.fail_write_suffix):
            raise RuntimeError("injected write failure")
        if IfNoneMatch != "*" or (Bucket, Key) in self.objects:
            raise RuntimeError("object already exists")
        raw = bytes(Body)
        self.objects[(Bucket, Key)] = raw
        self.writes.append((Bucket, Key, raw))
        return {}


class _EC2:
    def __init__(self) -> None:
        self.launches = []
        self.terminated = []

    def run_instances(self, **request):
        self.launches.append(request)
        return {"Instances": [{"InstanceId": "i-0123456789abcdef0"}]}

    def terminate_instances(self, *, InstanceIds):
        self.terminated.extend(InstanceIds)

    def describe_instances(self, *, InstanceIds):
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


class _DeniedEC2(_EC2):
    def run_instances(self, **request):
        self.launches.append(request)
        raise RuntimeError("launch-probe")


class _StoppedEC2(_EC2):
    def describe_instances(self, *, InstanceIds):
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


class V38SpotAuthorityTests(unittest.TestCase):
    def test_v38_preflight_local_result_enforces_complete_projection_evidence(self):
        chain = _preflight_chain()
        result = json.loads(chain["preflight_result_raw"])
        identity = result["inputs"][0]
        staged = V38StagedPhase(
            phase="preflight-spill",
            run_id=chain["preflight_plan"].run_id,
            source_commit=chain["preflight_plan"].source_commit,
            workers=32,
            inputs=(
                V38StagedArtifact(
                    role=identity["role"],
                    path=pathlib.Path("/input/v38-authority"),
                    uri=identity["uri"],
                    sha256=identity["sha256"],
                    blake3=identity["blake3"],
                    encoded_bytes=identity["encoded_bytes"],
                ),
            ),
            outputs=(),
        )

        def encode(value):
            return json.dumps(
                value, sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode() + b"\n"

        self.assertEqual(validate_v38_local_result_bytes(encode(result), staged), result)
        mutations = [
            lambda value: value["preflight_evidence"].update(extra=0),
            lambda value: value["preflight_evidence"].update(
                projected_construction_elapsed_ns=448_000_000_000
            ),
            lambda value: value["preflight_evidence"].update(relation_rows=65_535),
            lambda value: value["preflight_evidence"].update(
                predecessor_replay_allowance_ns=119_999_999_999
            ),
        ]
        for ordinal, mutation in enumerate(mutations):
            changed = json.loads(encode(result))
            mutation(changed)
            with self.subTest(ordinal=ordinal):
                with self.assertRaises(ValueError):
                    validate_v38_local_result_bytes(encode(changed), staged)

    def test_v38_causality_entrypoint_enforces_profile_region_and_account(self):
        calls = []

        class STS:
            account = EXPECTED_AWS_ACCOUNT

            def get_caller_identity(self):
                calls.append("identity")
                return {"Account": self.account}

        class Session:
            region_name = REGION
            sts = STS()

            def client(self, service, *, region_name):
                calls.append((service, region_name))
                if service == "sts":
                    return self.sts
                return object()

        def factory(*, profile_name, region_name):
            calls.append((profile_name, region_name))
            return Session()

        with mock.patch(
            "scripts.run_v38_boundary_spill_spot.run_v38_spot_phase",
            return_value="s3://fixture/terminal",
        ) as run:
            self.assertEqual(
                run_v38_causality_phase(
                    _plan("preflight-spill"), session_factory=factory
                ),
                "s3://fixture/terminal",
            )
        self.assertEqual(calls[0], ("causality", "eu-central-1"))
        self.assertIn(("sts", "eu-central-1"), calls)
        self.assertIn(("ec2", "eu-central-1"), calls)
        self.assertIn(("s3", "eu-central-1"), calls)
        run.assert_called_once()

        Session.sts.account = "000000000000"
        with mock.patch(
            "scripts.run_v38_boundary_spill_spot.run_v38_spot_phase"
        ) as forbidden:
            with self.assertRaisesRegex(ValueError, "AWS account"):
                run_v38_causality_phase(
                    _plan("preflight-spill"), session_factory=factory
                )
        forbidden.assert_not_called()
    def test_v38_construction_result_recomputes_all_evidence_formulas(self):
        plan = _plan("build-spill")
        inputs = [
            V38StagedArtifact(
                role=role,
                path=pathlib.Path(f"/input/{role}"),
                uri=identity["uri"],
                sha256=identity["sha256"],
                blake3=identity["blake3"],
                encoded_bytes=identity["encoded_bytes"],
            )
            for role, identity in (
                (role, _identity(role, format(index + 1, "x")))
                for index, role in enumerate(phase_input_roles("build-spill"))
            )
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            outputs = tuple(
                V38StagedOutput(
                    role=role,
                    path=root / role,
                    uri=f"{plan.output_prefix}{role}",
                )
                for role in phase_output_roles("build-spill")
            )
            for output in outputs:
                output.path.write_bytes(f"{output.role}\n".encode())
            staged = V38StagedPhase(
                phase="build-spill",
                run_id=plan.run_id,
                source_commit=plan.source_commit,
                workers=32,
                inputs=tuple(inputs),
                outputs=outputs,
            )
            import blake3

            artifacts = []
            for output in outputs:
                body = output.path.read_bytes()
                artifacts.append(
                    {
                        "blake3": blake3.blake3(body).hexdigest(),
                        "encoded_bytes": len(body),
                        "role": output.role,
                        "sha256": hashlib.sha256(body).hexdigest(),
                        "uri": output.uri,
                    }
                )
            evidence = {
                "accepted_alternates": 200_000,
                "capacity_rejected": 700_000,
                "exhausted": True,
                "maximum_posting_population": 10_000,
                "owners_one": 800_000,
                "owners_two": 200_000,
                "proposed_alternates": 1_000_000,
                "total_assignments": 1_200_000,
            }
            result = {
                "artifacts": artifacts,
                "claim_eligible": False,
                "construction_evidence": evidence,
                "inputs": [
                    {
                        "blake3": item.blake3,
                        "encoded_bytes": item.encoded_bytes,
                        "role": item.role,
                        "sha256": item.sha256,
                        "uri": item.uri,
                    }
                    for item in inputs
                ],
                "mode": "build-spill",
                "schema": "borsuk-v38-local-result-v1",
            }

            def encode(value):
                return json.dumps(
                    value, sort_keys=True, separators=(",", ":"), allow_nan=False
                ).encode() + b"\n"

            self.assertEqual(
                validate_v38_local_result_bytes(encode(result), staged), result
            )
            mutations = [
                lambda value: value["construction_evidence"].pop("owners_one"),
                lambda value: value["construction_evidence"].update(extra=0),
                lambda value: value["construction_evidence"].update(owners_one=True),
                lambda value: value["construction_evidence"].update(proposed_alternates=999_999),
                lambda value: value["construction_evidence"].update(accepted_alternates=250_001),
                lambda value: value["construction_evidence"].update(capacity_rejected=1_000_001),
                lambda value: value["construction_evidence"].update(capacity_rejected=900_001),
                lambda value: value["construction_evidence"].update(owners_one=799_999),
                lambda value: value["construction_evidence"].update(owners_two=199_999),
                lambda value: value["construction_evidence"].update(total_assignments=1_199_999),
                lambda value: value["construction_evidence"].update(maximum_posting_population=0),
                lambda value: value["construction_evidence"].update(maximum_posting_population=10_241),
                lambda value: value["construction_evidence"].update(exhausted=False),
            ]
            for ordinal, mutation in enumerate(mutations):
                changed = json.loads(encode(result))
                mutation(changed)
                with self.subTest(ordinal=ordinal):
                    with self.assertRaises(ValueError):
                        validate_v38_local_result_bytes(encode(changed), staged)

    def test_v38_ceiling_result_recomputes_certificates_aggregates_and_disposition(self):
        import blake3

        plan = _plan("evaluate-ceiling")
        subordinate = [
            _identity("v38-construction-result", "2"),
            _identity("spill-relation", "3"),
            _identity("spill-postings", "4"),
            _identity("gt100", "5"),
        ]
        authority = {
            "aggregate_gate_ppm": 998_000,
            "construction_result": subordinate[0],
            "development_ground_truth": subordinate[3],
            "gt_neighbors": 100,
            "maximum_certificate_bytes": 256,
            "maximum_query_solver_nodes": 250_000,
            "maximum_solver_nodes": 25_000_000,
            "minimum_gate_ppm": 800_000,
            "posting_summary": subordinate[2],
            "query_count": 1_000,
            "relation": subordinate[1],
            "schema": "borsuk-v38-boundary-spill-ceiling-authority-v1",
            "selected_postings": 14,
        }
        authority_raw = json.dumps(
            authority, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode() + b"\n"
        authority_identity = {
            "blake3": blake3.blake3(authority_raw).hexdigest(),
            "encoded_bytes": len(authority_raw),
            "role": "v38-ceiling-authority",
            "sha256": hashlib.sha256(authority_raw).hexdigest(),
            "uri": "s3://fixture/inputs/v38-ceiling-authority",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            authority_path = root / "v38-ceiling-authority"
            authority_path.write_bytes(authority_raw)
            artifacts = [authority_identity, *subordinate]
            staged_inputs = tuple(
                V38StagedArtifact(
                    role=item["role"],
                    path=authority_path if index == 0 else root / item["role"],
                    uri=item["uri"],
                    sha256=item["sha256"],
                    blake3=item["blake3"],
                    encoded_bytes=item["encoded_bytes"],
                )
                for index, item in enumerate(artifacts)
            )
            output = V38StagedOutput(
                role="ceiling",
                path=root / "ceiling",
                uri=f"{plan.output_prefix}ceiling",
            )
            staged = V38StagedPhase(
                phase="evaluate-ceiling",
                run_id=plan.run_id,
                source_commit=plan.source_commit,
                workers=32,
                inputs=staged_inputs,
                outputs=(output,),
            )
            certificates = [
                {
                    "certified_upper_hits": 100,
                    "exact": True,
                    "feasible_hits": 100,
                    "query_ordinal": query,
                    "selected_postings": list(range(14)),
                    "solver_visits": 0,
                }
                for query in range(1_000)
            ]
            result = {
                "aggregate_certified_upper_ppm": 1_000_000,
                "aggregate_feasible_ppm": 1_000_000,
                "aggregate_gate_ppm": 998_000,
                "authority": authority,
                "certificates": certificates,
                "claim_eligible": False,
                "disposition": "layout-feasible",
                "exact_query_count": 1_000,
                "gt_neighbors": 100,
                "minimum_certified_upper_ppm": 1_000_000,
                "minimum_feasible_ppm": 1_000_000,
                "minimum_gate_ppm": 800_000,
                "passed": True,
                "schema": "borsuk-v38-boundary-spill-ceiling-v1",
                "selected_postings": 14,
                "total_solver_visits": 0,
            }

            def validate(value):
                raw = json.dumps(
                    value, sort_keys=True, separators=(",", ":"), allow_nan=False
                ).encode() + b"\n"
                output.path.write_bytes(raw)
                return validate_v38_local_result_bytes(raw, staged)

            self.assertEqual(validate(result), result)
            mutations = [
                lambda value: value.pop("passed"),
                lambda value: value.update(extra=0),
                lambda value: value.update(gt_neighbors=99),
                lambda value: value["authority"].update(query_count=999),
                lambda value: value["authority"]["relation"].update(sha256="f" * 64),
                lambda value: value["certificates"][0].update(query_ordinal=1),
                lambda value: value["certificates"][0].update(selected_postings=list(range(13))),
                lambda value: value["certificates"][0].update(feasible_hits=99),
                lambda value: value["certificates"][0].update(certified_upper_hits=99),
                lambda value: value["certificates"][0].update(exact=False),
                lambda value: value["certificates"][0].update(solver_visits=250_001),
                lambda value: value.update(aggregate_feasible_ppm=999_999),
                lambda value: value.update(aggregate_certified_upper_ppm=999_999),
                lambda value: value.update(minimum_feasible_ppm=999_999),
                lambda value: value.update(minimum_certified_upper_ppm=999_999),
                lambda value: value.update(exact_query_count=999),
                lambda value: value.update(total_solver_visits=1),
                lambda value: value.update(passed=False),
                lambda value: value.update(disposition="indeterminate"),
            ]
            for ordinal, mutation in enumerate(mutations):
                changed = json.loads(json.dumps(result))
                mutation(changed)
                with self.subTest(ordinal=ordinal):
                    with self.assertRaises(ValueError):
                        validate(changed)

    def test_v38_controller_seals_boot_failure_before_release(self):
        chain = _preflight_chain()
        plan = chain["preflight_plan"]
        s3 = _S3(
            {("fixture", "manifest-preflight-spill.json"): chain["preflight_manifest_raw"]},
            unavailable_reads=1,
        )
        ec2 = _StoppedEC2()
        with self.assertRaisesRegex(RuntimeError, "boot-failure"):
            run_v38_spot_phase(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                sleep=lambda _seconds: None,
                monotonic=iter([0.0, 1.0]).__next__,
            )
        self.assertEqual(ec2.terminated, ["i-0123456789abcdef0"])
        self.assertEqual(s3.writes[-1][1], "results/preflight-spill/ATTEMPT_TERMINAL.json")
        terminal = validate_v38_terminal_bytes(s3.writes[-1][2], plan, "failed")
        self.assertEqual(terminal["reason"], "boot-failure")

    def test_v38_controller_terminates_once_then_seals_wrapper_timeout(self):
        chain = _preflight_chain()
        plan = chain["preflight_plan"]
        s3 = _S3(
            {("fixture", "manifest-preflight-spill.json"): chain["preflight_manifest_raw"]},
            unavailable_reads=1,
        )
        ec2 = _EC2()
        with self.assertRaisesRegex(TimeoutError, "wrapper timed out"):
            run_v38_spot_phase(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                sleep=lambda _seconds: None,
                monotonic=iter([0.0, 181.0]).__next__,
            )
        self.assertEqual(ec2.terminated, ["i-0123456789abcdef0"])
        terminal = validate_v38_terminal_bytes(s3.writes[-1][2], plan, "failed")
        self.assertEqual(terminal["reason"], "wrapper-timeout")

    def test_v38_controller_seals_poll_failure_and_rereads_after_timeout(self):
        chain = _preflight_chain()
        plan = chain["preflight_plan"]

        class BrokenPoll(_EC2):
            def describe_instances(self, *, InstanceIds):
                raise RuntimeError(f"poll failed for {InstanceIds[0]}")

        failed_s3 = _S3(
            {("fixture", "manifest-preflight-spill.json"): chain["preflight_manifest_raw"]},
            unavailable_reads=1,
        )
        failed_ec2 = BrokenPoll()
        with self.assertRaisesRegex(RuntimeError, "poll failed"):
            run_v38_spot_phase(
                plan,
                ec2_client=failed_ec2,
                s3_client=failed_s3,
                sleep=lambda _seconds: None,
                monotonic=iter([0.0, 1.0]).__next__,
            )
        self.assertEqual(failed_ec2.terminated, ["i-0123456789abcdef0"])
        failure = validate_v38_terminal_bytes(failed_s3.writes[-1][2], plan, "failed")
        self.assertEqual(failure["reason"], "controller-failure")

        completed = chain["preflight_terminal_raw"]
        terminal_key = "results/preflight-spill/ATTEMPT_TERMINAL.json"
        raced_s3 = _S3(
            {("fixture", "manifest-preflight-spill.json"): chain["preflight_manifest_raw"]},
            unavailable_reads=1,
        )

        class TerminalOnTerminate(_EC2):
            def terminate_instances(self, *, InstanceIds):
                super().terminate_instances(InstanceIds=InstanceIds)
                raced_s3.objects[("fixture", terminal_key)] = completed

        raced_ec2 = TerminalOnTerminate()
        self.assertEqual(
            run_v38_spot_phase(
                plan,
                ec2_client=raced_ec2,
                s3_client=raced_s3,
                sleep=lambda _seconds: None,
                monotonic=iter([0.0, 181.0]).__next__,
            ),
            f"s3://fixture/{terminal_key}",
        )
        self.assertEqual(raced_ec2.terminated, ["i-0123456789abcdef0"])
        self.assertEqual(raced_s3.writes, [])

    def test_v38_worker_seals_pre_native_failure_and_cleans_phase(self):
        chain = _preflight_chain()
        plan = chain["preflight_plan"]
        binary_raw = b"v38-binary-fixture\n"
        plan = dataclasses.replace(
            plan,
            binary_bytes=len(binary_raw),
            binary_sha256=hashlib.sha256(binary_raw).hexdigest(),
        )
        plan_raw = json.dumps(
            dataclasses.asdict(plan), sort_keys=True, separators=(",", ":")
        ).encode()
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            plan_path = root / "plan.json"
            manifest_path = root / "manifest.json"
            binary_path = root / "v38"
            plan_path.write_bytes(plan_raw)
            manifest_path.write_bytes(chain["preflight_manifest_raw"])
            binary_path.write_bytes(binary_raw)
            s3 = _S3({})
            with self.assertRaises(KeyError):
                run_v38_worker_invocation(
                    V38WorkerInvocation(
                        root=root / "phase",
                        plan=plan_path,
                        manifest=manifest_path,
                        binary=binary_path,
                        instance_id="i-0123456789abcdef0",
                    ),
                    s3,
                    finalize_monitor=_preserve_monitor,
                )
            self.assertFalse((root / "phase").exists())
        self.assertEqual(
            [key.rsplit("/", 1)[-1] for _, key, _ in s3.writes],
            ["WORKER_FAILURE.log", "ATTEMPT_TERMINAL.json"],
        )
        self.assertLessEqual(len(s3.writes[0][2]), 65_536)
        terminal = validate_v38_terminal_bytes(s3.writes[1][2], plan, "failed")
        self.assertEqual(terminal["reason"], "authority-failure")

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            plan_path = root / "plan.json"
            manifest_path = root / "manifest.json"
            binary_path = root / "v38"
            plan_path.write_bytes(plan_raw)
            manifest_path.write_bytes(chain["preflight_manifest_raw"])
            binary_path.write_bytes(binary_raw)
            diagnostic_failure = _S3({}, fail_write_suffix="WORKER_FAILURE.log")
            with self.assertRaises(KeyError):
                run_v38_worker_invocation(
                    V38WorkerInvocation(
                        root=root / "phase",
                        plan=plan_path,
                        manifest=manifest_path,
                        binary=binary_path,
                        instance_id="i-0123456789abcdef0",
                    ),
                    diagnostic_failure,
                    finalize_monitor=_preserve_monitor,
                )
        self.assertEqual(
            [key.rsplit("/", 1)[-1] for _, key, _ in diagnostic_failure.writes],
            ["ATTEMPT_TERMINAL.json"],
        )

    def test_v38_worker_seals_binary_authentication_failure(self):
        chain = _preflight_chain()
        plan = chain["preflight_plan"]
        expected_binary = b"v38-binary-fixture\n"
        plan = dataclasses.replace(
            plan,
            binary_bytes=len(expected_binary),
            binary_sha256=hashlib.sha256(expected_binary).hexdigest(),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            plan_path = root / "plan.json"
            manifest_path = root / "manifest.json"
            binary_path = root / "v38"
            plan_path.write_text(
                json.dumps(
                    dataclasses.asdict(plan), sort_keys=True, separators=(",", ":")
                )
            )
            manifest_path.write_bytes(chain["preflight_manifest_raw"])
            binary_path.write_bytes(expected_binary + b"tampered")
            s3 = _S3({})
            with self.assertRaisesRegex(ValueError, "binary authority"):
                run_v38_worker_invocation(
                    V38WorkerInvocation(
                        root=root / "phase",
                        plan=plan_path,
                        manifest=manifest_path,
                        binary=binary_path,
                        instance_id="i-0123456789abcdef0",
                    ),
                    s3,
                    finalize_monitor=_preserve_monitor,
                )
        self.assertEqual(
            [key.rsplit("/", 1)[-1] for _, key, _ in s3.writes],
            ["WORKER_FAILURE.log", "ATTEMPT_TERMINAL.json"],
        )
        terminal = validate_v38_terminal_bytes(s3.writes[-1][2], plan, "failed")
        self.assertEqual(terminal["reason"], "authority-failure")
        self.assertIsNone(terminal["monitor"])

    def test_v38_worker_entrypoint_authenticates_boot_files_and_runs_once(self):
        import blake3

        chain = _preflight_chain()
        plan = chain["preflight_plan"]
        authority_raw = b"v38-authority-fixture\n"
        binary_raw = b"v38-binary-fixture\n"
        manifest = json.loads(chain["preflight_manifest_raw"])
        manifest["inputs"][0].update(
            {
                "blake3": blake3.blake3(authority_raw).hexdigest(),
                "encoded_bytes": len(authority_raw),
                "sha256": hashlib.sha256(authority_raw).hexdigest(),
            }
        )
        manifest_raw = canonical_v38_phase_manifest_bytes(manifest)
        plan = dataclasses.replace(
            plan,
            binary_bytes=len(binary_raw),
            binary_sha256=hashlib.sha256(binary_raw).hexdigest(),
            manifest_bytes=len(manifest_raw),
            manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
        )
        result = json.loads(chain["preflight_result_raw"])
        result["inputs"] = manifest["inputs"]
        result_raw = (
            json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        progress_raw = b'{"completed_rows":65536,"schema":"borsuk-v38-preflight-progress-v1","total_rows":65536}\n'
        plan_raw = json.dumps(
            dataclasses.asdict(plan), sort_keys=True, separators=(",", ":")
        ).encode()
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            plan_path = root / "plan.json"
            manifest_path = root / "manifest.json"
            binary_path = root / "v38"
            plan_path.write_bytes(plan_raw)
            manifest_path.write_bytes(manifest_raw)
            binary_path.write_bytes(binary_raw)
            s3 = _S3({("fixture", "inputs/v38-authority"): authority_raw})
            calls = []

            def native(command, staged, stdout_path, progress_path):
                calls.append((command, staged.phase))
                stdout_path.write_bytes(result_raw)
                progress_path.write_bytes(progress_raw)
                return 0, V38SpotMonitorTests()._sample("preflight-spill")

            with mock.patch(
                "scripts.run_v38_boundary_spill_spot.execute_v38_native_science",
                side_effect=native,
            ):
                terminal_raw = run_v38_worker_invocation(
                    V38WorkerInvocation(
                        root=root / "phase",
                        plan=plan_path,
                        manifest=manifest_path,
                        binary=binary_path,
                        instance_id="i-0123456789abcdef0",
                    ),
                    s3,
                    finalize_monitor=_preserve_monitor,
                )
        self.assertEqual(len(calls), 1)
        self.assertEqual(json.loads(terminal_raw)["status"], "complete")
        self.assertEqual(
            s3.writes[-1][1],
            "results/preflight-spill/ATTEMPT_TERMINAL.json",
        )

    def test_v38_worker_failure_preserves_resource_stop_and_monitor(self):
        import blake3

        chain = _preflight_chain()
        plan = chain["preflight_plan"]
        authority_raw = b"v38-authority-fixture\n"
        binary_raw = b"v38-binary-fixture\n"
        manifest = json.loads(chain["preflight_manifest_raw"])
        manifest["inputs"][0].update(
            {
                "blake3": blake3.blake3(authority_raw).hexdigest(),
                "encoded_bytes": len(authority_raw),
                "sha256": hashlib.sha256(authority_raw).hexdigest(),
            }
        )
        manifest_raw = canonical_v38_phase_manifest_bytes(manifest)
        plan = dataclasses.replace(
            plan,
            binary_bytes=len(binary_raw),
            binary_sha256=hashlib.sha256(binary_raw).hexdigest(),
            manifest_bytes=len(manifest_raw),
            manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
        )
        plan_raw = json.dumps(
            dataclasses.asdict(plan), sort_keys=True, separators=(",", ":")
        ).encode()
        breached = dataclasses.replace(
            V38SpotMonitorTests()._sample("preflight-spill"),
            memory_peak_bytes=256 * 1_048_576 + 1,
        )
        def terminal_wrapper_timeout(_sample):
            return dataclasses.replace(breached, wrapper_elapsed_seconds=181.0)

        def terminal_cgroup_missing(_sample):
            raise FileNotFoundError("aggregate cgroup disappeared")

        for label, finalize in (
            ("later-wrapper-timeout", terminal_wrapper_timeout),
            ("terminal-cgroup-missing", terminal_cgroup_missing),
        ):
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                plan_path = root / "plan.json"
                manifest_path = root / "manifest.json"
                binary_path = root / "v38"
                plan_path.write_bytes(plan_raw)
                manifest_path.write_bytes(manifest_raw)
                binary_path.write_bytes(binary_raw)
                s3 = _S3({("fixture", "inputs/v38-authority"): authority_raw})

                def breached_native(_command, _staged, _stdout_path, progress_path):
                    progress_path.write_bytes(b"resource monitor stopped the worker\n")
                    return 1, breached

                with mock.patch(
                    "scripts.run_v38_boundary_spill_spot.execute_v38_native_science",
                    side_effect=breached_native,
                ):
                    with self.assertRaisesRegex(RuntimeError, "memory-stop"):
                        run_v38_worker_invocation(
                            V38WorkerInvocation(
                                root=root / "phase",
                                plan=plan_path,
                                manifest=manifest_path,
                                binary=binary_path,
                                instance_id="i-0123456789abcdef0",
                            ),
                            s3,
                            finalize_monitor=finalize,
                        )
            terminal = validate_v38_terminal_bytes(s3.writes[-1][2], plan, "failed")
            self.assertEqual(terminal["reason"], "memory-stop")
            self.assertEqual(terminal["monitor"], dataclasses.asdict(breached))

    def test_v38_controller_fences_successors_before_spot_launch(self):
        chain = _preflight_chain()
        build_plan = chain["build_plan"]
        denied = _DeniedEC2()
        with self.assertRaisesRegex(ValueError, "preflight terminal is required"):
            run_v38_spot_phase(
                build_plan,
                ec2_client=denied,
                s3_client=_S3({}),
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(denied.launches, [])

        preflight_terminal = json.loads(chain["preflight_terminal_raw"])
        preflight_terminal_uri = (
            preflight_terminal["result"]["uri"].removesuffix("local-result.json")
            + "ATTEMPT_TERMINAL.json"
        )
        objects = {
            ("fixture", "manifest-build-spill.json"): chain["build_manifest_raw"],
            ("fixture", "results/preflight-spill/ATTEMPT_TERMINAL.json"): chain[
                "preflight_terminal_raw"
            ],
            ("fixture", "manifest-preflight-spill.json"): chain[
                "preflight_manifest_raw"
            ],
            ("fixture", "results/preflight-spill/local-result.json"): chain[
                "preflight_result_raw"
            ],
        }
        admitted = _DeniedEC2()
        with self.assertRaisesRegex(RuntimeError, "launch-probe"):
            run_v38_spot_phase(
                build_plan,
                preflight_terminal_uri=preflight_terminal_uri,
                ec2_client=admitted,
                s3_client=_S3(objects, unavailable_reads=1),
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(len(admitted.launches), 1)

        ceiling_plan = _plan("evaluate-ceiling")
        denied = _DeniedEC2()
        with self.assertRaisesRegex(ValueError, "build terminal is required"):
            run_v38_spot_phase(
                ceiling_plan,
                ec2_client=denied,
                s3_client=_S3({}),
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(denied.launches, [])
        with self.assertRaisesRegex(ValueError, "build terminal URI differs"):
            run_v38_spot_phase(
                ceiling_plan,
                build_terminal_uri="s3://fixture/results/build/not-terminal.json",
                ec2_client=denied,
                s3_client=_S3({}),
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(denied.launches, [])

    def test_v38_controller_launches_one_spot_and_always_terminates(self):
        fixture = _preflight_chain()
        plan = fixture["preflight_plan"]
        terminal_raw = fixture["preflight_terminal_raw"]
        bucket = "fixture"
        terminal_key = "results/preflight-spill/ATTEMPT_TERMINAL.json"
        manifest_key = "manifest-preflight-spill.json"
        s3 = _S3(
            {
                (bucket, terminal_key): terminal_raw,
                (bucket, manifest_key): fixture["preflight_manifest_raw"],
            },
            unavailable_reads=1,
        )
        ec2 = _EC2()
        uri = run_v38_spot_phase(
            plan,
            ec2_client=ec2,
            s3_client=s3,
            sleep=lambda _seconds: None,
            monotonic=iter([0.0, 1.0]).__next__,
        )
        self.assertEqual(uri, f"s3://{bucket}/{terminal_key}")
        self.assertEqual(len(ec2.launches), 1)
        self.assertEqual(ec2.terminated, ["i-0123456789abcdef0"])

        duplicate_ec2 = _EC2()
        with self.assertRaisesRegex(ValueError, "terminal already exists"):
            run_v38_spot_phase(
                plan,
                ec2_client=duplicate_ec2,
                s3_client=_S3({(bucket, terminal_key): terminal_raw}),
                sleep=lambda _seconds: None,
                monotonic=iter([0.0]).__next__,
            )
        self.assertEqual(duplicate_ec2.launches, [])

    def test_v38_worker_publishes_terminal_last_and_cleans_named_files(self):
        fixture = _preflight_chain()
        plan = fixture["preflight_plan"]
        manifest = json.loads(fixture["preflight_manifest_raw"])
        authority_raw = b"v38-authority-fixture\n"
        import blake3

        manifest["inputs"][0].update(
            {
                "blake3": blake3.blake3(authority_raw).hexdigest(),
                "encoded_bytes": len(authority_raw),
                "sha256": hashlib.sha256(authority_raw).hexdigest(),
            }
        )
        manifest_raw = canonical_v38_phase_manifest_bytes(manifest)
        plan = dataclasses.replace(
            plan,
            manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
            manifest_bytes=len(manifest_raw),
        )
        result = json.loads(fixture["preflight_result_raw"])
        result["inputs"] = manifest["inputs"]
        result_raw = json.dumps(result, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        progress_raw = (
            b'{"completed_rows":1,"schema":"borsuk-v38-preflight-progress-v1","total_rows":65536}\n'
            b'{"completed_rows":65536,"schema":"borsuk-v38-preflight-progress-v1","total_rows":65536}\n'
        )
        uploads = []
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory) / "phase"

            def download(bucket: str, key: str, path: pathlib.Path) -> None:
                self.assertEqual((bucket, key), ("fixture", "inputs/v38-authority"))
                path.write_bytes(authority_raw)

            def execute(command, staged, stdout_path, progress_path):
                self.assertEqual(command[0], "/opt/v38")
                self.assertEqual(staged.phase, "preflight-spill")
                stdout_path.write_bytes(result_raw)
                progress_path.write_bytes(progress_raw)
                return 0, V38SpotMonitorTests()._sample("preflight-spill")

            def upload_once(bucket, key, body):
                if key.endswith("ATTEMPT_TERMINAL.json"):
                    self.assertFalse(root.exists())
                uploads.append((bucket, key, body))

            def finalize_monitor(sample):
                self.assertEqual(
                    [key.rsplit("/", 1)[-1] for _, key, _ in uploads],
                    ["local-result.json", "progress.json"],
                )
                return dataclasses.replace(
                    sample,
                    wrapper_elapsed_seconds=3.0,
                    memory_peak_bytes=300_000,
                )

            terminal_raw = execute_v38_worker(
                plan,
                root=root,
                manifest_raw=manifest_raw,
                binary=pathlib.Path("/opt/v38"),
                instance_id="i-0123456789abcdef0",
                download=download,
                execute=execute,
                upload_once=upload_once,
                finalize_monitor=finalize_monitor,
            )
            self.assertFalse(root.exists())
        self.assertEqual(
            [key.rsplit("/", 1)[-1] for _, key, _ in uploads],
            ["local-result.json", "progress.json", "ATTEMPT_TERMINAL.json"],
        )
        terminal = json.loads(terminal_raw)
        self.assertEqual(terminal["status"], "complete")
        self.assertEqual(terminal["monitor"]["wrapper_elapsed_seconds"], 3.0)
        self.assertEqual(terminal["monitor"]["memory_peak_bytes"], 300_000)
        self.assertEqual(terminal["result"]["sha256"], hashlib.sha256(result_raw).hexdigest())

        with tempfile.TemporaryDirectory() as directory:
            failed_root = pathlib.Path(directory) / "phase"

            def failed_download(_bucket, _key, path):
                path.write_bytes(authority_raw)

            def failed_execute(_command, _staged, _stdout_path, progress_path):
                progress_path.write_bytes(b"discarded-prefix\n" * 8_192 + b"native-tail\n")
                return 42, V38SpotMonitorTests()._sample("preflight-spill")

            with self.assertRaisesRegex(
                RuntimeError, "V38 native worker exited 42.*native-tail"
            ):
                execute_v38_worker(
                    plan,
                    root=failed_root,
                    manifest_raw=manifest_raw,
                    binary=pathlib.Path("/opt/v38"),
                    instance_id="i-0123456789abcdef0",
                    download=failed_download,
                    execute=failed_execute,
                    upload_once=lambda *_arguments: self.fail("must not publish"),
                )
            self.assertFalse(failed_root.exists())

    def test_v38_worker_cleans_partial_staging_before_failure_receipt(self):
        import blake3

        plan = _plan("build-spill")
        bodies = {
            role: f"{role}-fixture\n".encode()
            for role in phase_input_roles("build-spill")
        }
        identities = [
            {
                "blake3": blake3.blake3(bodies[role]).hexdigest(),
                "encoded_bytes": len(bodies[role]),
                "role": role,
                "sha256": hashlib.sha256(bodies[role]).hexdigest(),
                "uri": f"s3://fixture/inputs/{role}",
            }
            for role in phase_input_roles("build-spill")
        ]
        outputs = [
            {"role": role, "uri": f"{plan.output_prefix}{role}"}
            for role in phase_output_roles("build-spill")
        ]
        manifest_raw = _manifest(plan, identities, outputs)
        plan = dataclasses.replace(
            plan,
            manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
            manifest_bytes=len(manifest_raw),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory) / "phase"
            calls = 0

            def download(_bucket, _key, path):
                nonlocal calls
                role = identities[calls]["role"]
                body = bodies[role] + (b"corrupt" if calls == 1 else b"")
                calls += 1
                path.write_bytes(body)

            with self.assertRaisesRegex(ValueError, "staged bytes"):
                execute_v38_worker(
                    plan,
                    root=root,
                    manifest_raw=manifest_raw,
                    binary=pathlib.Path("/opt/v38"),
                    instance_id="i-0123456789abcdef0",
                    download=download,
                    execute=lambda *_arguments: self.fail("science must not execute"),
                    upload_once=lambda *_arguments: self.fail("worker must not publish"),
                )
            self.assertFalse(root.exists())

    def test_v38_staging_authenticates_exact_objects_before_local_command(self):
        import blake3

        plan = _plan("preflight-spill")
        body = b"v38-authority-fixture\n"
        identity = {
            "blake3": blake3.blake3(body).hexdigest(),
            "encoded_bytes": len(body),
            "role": "v38-authority",
            "sha256": hashlib.sha256(body).hexdigest(),
            "uri": "s3://fixture/inputs/v38-authority",
        }
        raw = _manifest(plan, [identity], [])
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory) / "phase"

            def download(bucket: str, key: str, path: pathlib.Path) -> None:
                self.assertEqual((bucket, key), ("fixture", "inputs/v38-authority"))
                path.write_bytes(body)

            staged = stage_v38_phase_inputs(raw, root, download)
            self.assertEqual(
                staged,
                V38StagedPhase(
                    phase="preflight-spill",
                    run_id=plan.run_id,
                    source_commit=plan.source_commit,
                    workers=32,
                    inputs=(
                        V38StagedArtifact(
                            role="v38-authority",
                            path=root / "inputs" / "v38-authority",
                            uri=identity["uri"],
                            sha256=identity["sha256"],
                            blake3=identity["blake3"],
                            encoded_bytes=len(body),
                        ),
                    ),
                    outputs=(),
                ),
            )
            command = build_v38_binary_command(pathlib.Path("/bin/v38"), staged)
            self.assertEqual(
                command,
                [
                    "/bin/v38",
                    "--execute-v38-local",
                    "--mode",
                    "preflight-spill",
                    "--workers",
                    "32",
                    "--v38-authority",
                    str(root / "inputs" / "v38-authority"),
                    "--v38-authority-uri",
                    "s3://fixture/inputs/v38-authority",
                    "--v38-authority-sha256",
                    identity["sha256"],
                    "--v38-authority-blake3",
                    identity["blake3"],
                    "--v38-authority-bytes",
                    str(len(body)),
                ],
            )
            self.assertFalse(any("bucket" in argument for argument in command))
            self.assertFalse(any("endpoint" in argument for argument in command))

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory) / "phase"

            def corrupt(_bucket: str, _key: str, path: pathlib.Path) -> None:
                path.write_bytes(body + b"changed")

            with self.assertRaisesRegex(ValueError, "staged bytes"):
                stage_v38_phase_inputs(raw, root, corrupt)

    def test_v38_build_admission_requires_one_bound_successful_preflight(self):
        fixture = _preflight_chain()
        admission = {key: value for key, value in fixture.items() if key != "preflight_plan"}
        admitted = validate_v38_preflight_admission(**admission)
        self.assertEqual(
            admitted["preflight_evidence"]["projected_peak_bytes"], 2_783_657_984
        )
        self.assertEqual(
            admitted["preflight_evidence"]["proposal_coordinates_per_second"],
            78_080_000,
        )
        for field, replacement in (
            ("preflight_result_raw", fixture["preflight_result_raw"].replace(b"449000000000", b"449000000001")),
            ("preflight_terminal_raw", fixture["preflight_terminal_raw"].replace(b'"status":"complete"', b'"status":"failed"')),
            ("build_manifest_raw", fixture["build_manifest_raw"].replace(b'"workers":32', b'"workers":16')),
        ):
            with self.subTest(field=field):
                changed = dict(admission)
                changed[field] = replacement
                with self.assertRaises(ValueError):
                    validate_v38_preflight_admission(**changed)
        wrong_manifest = json.loads(admission["build_manifest_raw"])
        wrong_manifest["outputs"][0]["uri"] = "s3://fixture/wrong/spill-relation"
        wrong_raw = canonical_v38_phase_manifest_bytes(wrong_manifest)
        changed = {
            **admission,
            "build_plan": dataclasses.replace(
                admission["build_plan"],
                manifest_sha256=hashlib.sha256(wrong_raw).hexdigest(),
                manifest_bytes=len(wrong_raw),
            ),
            "build_manifest_raw": wrong_raw,
        }
        with self.assertRaisesRegex(ValueError, "manifest binding"):
            validate_v38_preflight_admission(**changed)

    def test_v38_ceiling_admission_requires_sealed_build_outputs(self):
        import blake3

        preflight = _preflight_chain()
        build_plan = preflight["build_plan"]
        build_manifest = json.loads(preflight["build_manifest_raw"])
        relation_raw = b"relation-parquet-fixture"
        postings_raw = b"posting-parquet-fixture"
        artifacts = []
        for role, raw in (
            ("spill-relation", relation_raw),
            ("spill-postings", postings_raw),
        ):
            artifacts.append(
                {
                    "blake3": blake3.blake3(raw).hexdigest(),
                    "encoded_bytes": len(raw),
                    "role": role,
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "uri": f"{build_plan.output_prefix}{role}",
                }
            )
        construction = {
            "artifacts": artifacts,
            "claim_eligible": False,
            "construction_evidence": {
                "accepted_alternates": 200_000,
                "capacity_rejected": 800_000,
                "exhausted": True,
                "maximum_posting_population": 10_000,
                "owners_one": 800_000,
                "owners_two": 200_000,
                "proposed_alternates": 1_000_000,
                "total_assignments": 1_200_000,
            },
            "inputs": build_manifest["inputs"],
            "mode": "build-spill",
            "schema": "borsuk-v38-local-result-v1",
        }
        build_result_raw = (
            json.dumps(construction, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        build_terminal_raw = _terminal(
            build_plan, artifacts=artifacts, result_raw=build_result_raw
        )

        ceiling_plan = _plan("evaluate-ceiling")
        result_identity = {
            "blake3": blake3.blake3(build_result_raw).hexdigest(),
            "encoded_bytes": len(build_result_raw),
            "role": "v38-construction-result",
            "sha256": hashlib.sha256(build_result_raw).hexdigest(),
            "uri": f"{build_plan.output_prefix}local-result.json",
        }
        gt = _identity("gt100", "a")
        ceiling_inputs = [
            _identity("v38-ceiling-authority", "9"),
            result_identity,
            artifacts[0],
            artifacts[1],
            gt,
        ]
        ceiling_manifest_raw = _manifest(ceiling_plan, ceiling_inputs, [
            {"role": "ceiling", "uri": f"{ceiling_plan.output_prefix}ceiling"}
        ])
        ceiling_plan = dataclasses.replace(
            ceiling_plan,
            manifest_sha256=hashlib.sha256(ceiling_manifest_raw).hexdigest(),
            manifest_bytes=len(ceiling_manifest_raw),
        )
        fixture = {
            "ceiling_plan": ceiling_plan,
            "ceiling_manifest_raw": ceiling_manifest_raw,
            "build_plan": build_plan,
            "build_manifest_raw": preflight["build_manifest_raw"],
            "build_result_raw": build_result_raw,
            "build_terminal_raw": build_terminal_raw,
        }
        validated = validate_v38_ceiling_predecessor(**fixture)
        self.assertEqual(validated["artifacts"], artifacts)
        for field, replacement in (
            ("build_result_raw", build_result_raw.replace(b'"accepted_alternates":200000', b'"accepted_alternates":200001')),
            ("build_terminal_raw", build_terminal_raw.replace(b'"status":"complete"', b'"status":"stopped"')),
            ("ceiling_manifest_raw", ceiling_manifest_raw.replace(artifacts[0]["sha256"].encode(), ("f" * 64).encode())),
        ):
            with self.subTest(field=field):
                changed = dict(fixture)
                changed[field] = replacement
                with self.assertRaises(ValueError):
                    validate_v38_ceiling_predecessor(**changed)
        changed = {
            **fixture,
            "ceiling_plan": dataclasses.replace(
                ceiling_plan,
                binary_uri="s3://fixture/other/v38-boundary-spill",
            ),
        }
        with self.assertRaisesRegex(ValueError, "executable"):
            validate_v38_ceiling_predecessor(**changed)

    def test_v38_spot_plan_is_spot_only_and_uses_ordered_targets(self):
        self.assertEqual(PROFILE, "causality")
        self.assertEqual(REGION, "eu-central-1")
        self.assertEqual(AMI_ID, "ami-07bcecd13a160173f")
        self.assertEqual(INSTANCE_TYPE, "c7g.8xlarge")
        self.assertEqual(
            [(target.availability_zone, target.subnet_id) for target in SPOT_TARGETS],
            [
                ("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
                ("eu-central-1b", "subnet-00243d923761c047c"),
                ("eu-central-1a", "subnet-034528fbd6977848f"),
            ],
        )
        specs = build_v38_launch_specs(_plan(), user_data="#!/bin/bash\nshutdown -h now\n")
        self.assertEqual(len(specs), 3)
        for spec, target in zip(specs, SPOT_TARGETS, strict=True):
            self.assertEqual(spec["SubnetId"], target.subnet_id)
            self.assertEqual(spec["Placement"]["AvailabilityZone"], target.availability_zone)
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(
                spec["InstanceMarketOptions"]["SpotOptions"][
                    "InstanceInterruptionBehavior"
                ],
                "terminate",
            )
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")

    def test_v38_phase_manifest_has_exact_capability_roles(self):
        roles = {
            "preflight-spill": ("v38-authority",),
            "build-spill": (
                "v38-authority",
                "v37-authority",
                "v37-build-manifest",
                "v37-local-result",
                "v37-terminal",
                "v37-tree",
                "v37-ownership",
                "source",
            ),
            "evaluate-ceiling": (
                "v38-ceiling-authority",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
                "gt100",
            ),
        }
        outputs = {
            "preflight-spill": (),
            "build-spill": ("spill-relation", "spill-postings"),
            "evaluate-ceiling": ("ceiling",),
        }
        for phase in roles:
            self.assertEqual(phase_input_roles(phase), roles[phase])
            self.assertEqual(phase_output_roles(phase), outputs[phase])
            manifest = {
                "claim_eligible": False,
                "inputs": [
                    _identity(role, format(index + 1, "x"))
                    for index, role in enumerate(roles[phase])
                ],
                "outputs": [
                    {"role": role, "uri": f"s3://fixture/output/{role}"}
                    for role in outputs[phase]
                ],
                "phase": phase,
                "run_id": f"v38-fixture-{phase}",
                "schema": "borsuk-v38-spot-phase-manifest-v1",
                "source_commit": SOURCE_COMMIT,
                "workers": 32,
            }
            raw = canonical_v38_phase_manifest_bytes(manifest)
            self.assertEqual(raw[-1:], b"\n")
            self.assertEqual(json.loads(raw), manifest)
            changed = json.loads(raw)
            changed["inputs"][0]["uri"] = changed["outputs"][0]["uri"] if changed["outputs"] else changed["inputs"][0]["uri"] + "/"
            with self.assertRaises(ValueError):
                canonical_v38_phase_manifest_bytes(changed)

    def test_v38_plan_rejects_mutable_or_malformed_authority(self):
        baseline = _plan()
        for field, value in [
            ("source_commit", "abbreviated"),
            ("binary_sha256", "A" * 64),
            ("manifest_bytes", 0),
            ("output_prefix", "s3://fixture/results/not-a-prefix"),
            ("phase", "combined"),
        ]:
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    build_v38_spot_plan(**dataclasses.asdict(dataclasses.replace(baseline, **{field: value})))
        with self.assertRaisesRegex(ValueError, "overlap"):
            build_v38_spot_plan(
                **dataclasses.asdict(
                    dataclasses.replace(
                        baseline,
                        binary_uri=baseline.source_archive_uri,
                    )
                )
            )


class V38SpotMonitorTests(unittest.TestCase):
    def test_v38_wrapper_monitor_covers_staging_and_publication_intervals(self):
        with tempfile.TemporaryDirectory() as directory:
            cgroup = pathlib.Path(directory)
            (cgroup / "memory.current").write_text("123456\n")
            (cgroup / "memory.peak").write_text("234567\n")
            (cgroup / "memory.swap.current").write_text("0\n")
            (cgroup / "memory.stat").write_text(
                "anon 80000\nfile 30000\nkernel 10000\n"
            )
            psi = [0.0]
            with mock.patch(
                "scripts.run_v38_boundary_spill_spot._v38_psi_full_avg10",
                side_effect=lambda: psi[0],
            ):
                monitor = V38WrapperMonitor(
                    "preflight-spill", cgroup, poll_seconds=0.005
                )
                psi[0] = 0.5
                (cgroup / "memory.peak").write_text("250000\n")
                time.sleep(0.03)
                psi[0] = 0.0
                sample = monitor.finish(self._sample("preflight-spill"))
        self.assertGreater(sample.wrapper_elapsed_seconds, 0.02)
        self.assertEqual(sample.memory_peak_bytes, 250_000)
        self.assertEqual(sample.psi_full_avg10, 0.5)

    def test_v38_native_science_service_has_no_network_or_unregistered_paths(self):
        staged = V38StagedPhase(
            phase="build-spill",
            run_id="v38-fixture-build",
            source_commit=SOURCE_COMMIT,
            workers=32,
            inputs=(
                V38StagedArtifact(
                    role="v38-authority",
                    path=pathlib.Path("/work/inputs/v38-authority"),
                    uri="s3://fixture/inputs/v38-authority",
                    sha256="1" * 64,
                    blake3="2" * 64,
                    encoded_bytes=100,
                ),
            ),
            outputs=(
                V38StagedOutput(
                    role="spill-relation",
                    path=pathlib.Path("/work/outputs/spill-relation"),
                    uri="s3://fixture/results/spill-relation",
                ),
            ),
        )
        native = ["/work/v38", "--execute-v38-local"]
        command = build_v38_science_service_command(native, staged)
        self.assertEqual(command.count("systemd-run"), 1)
        self.assertIn("--pipe", command)
        self.assertIn("--property=PrivateNetwork=true", command)
        self.assertIn("--property=IPAddressDeny=any", command)
        self.assertIn("--property=RestrictAddressFamilies=AF_UNIX", command)
        self.assertIn("--property=ProtectSystem=strict", command)
        self.assertIn("--property=ProtectHome=true", command)
        self.assertIn("--property=ProtectControlGroups=true", command)
        self.assertIn("--property=CapabilityBoundingSet=", command)
        self.assertIn("--property=InaccessiblePaths=/run", command)
        self.assertIn("--property=TemporaryFileSystem=/work:ro", command)
        self.assertIn("--property=MemorySwapMax=0", command)
        self.assertIn("--property=BindReadOnlyPaths=/work/inputs /work/v38", command)
        self.assertIn("--property=BindPaths=/work/outputs", command)
        self.assertNotIn("ReadWritePaths", " ".join(command))
        self.assertEqual(command[-2:], native)

    def test_v38_native_monitor_preserves_progress_and_cgroup_components(self):
        progress_line = (
            '{"completed_rows":65536,"schema":"borsuk-v38-preflight-progress-v1",'
            '"total_rows":65536}\n'
        )
        program = (
            "import sys;"
            f"sys.stderr.write({progress_line!r});sys.stderr.flush();"
            "sys.stdout.write('result\\n');sys.stdout.flush()"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            cgroup = root / "science-cgroup"
            cgroup.mkdir()
            (cgroup / "memory.current").write_text("123456\n")
            (cgroup / "memory.peak").write_text("234567\n")
            (cgroup / "memory.swap.current").write_text("0\n")
            (cgroup / "memory.stat").write_text(
                "anon 80000\nfile 30000\nkernel 10000\n"
            )
            with mock.patch(
                "scripts.run_v38_boundary_spill_spot._v38_psi_full_avg10",
                return_value=0.0,
            ):
                returncode, sample = run_v38_native_process(
                    [sys.executable, "-c", program],
                    root / "stdout",
                    root / "progress",
                    phase="preflight-spill",
                    poll_seconds=0.01,
                    memory_cgroup=cgroup,
                )
            self.assertEqual(returncode, 0)
            self.assertEqual((root / "stdout").read_bytes(), b"result\n")
            self.assertEqual((root / "progress").read_text(), progress_line)
            self.assertGreater(sample.science_elapsed_seconds, 0)
            self.assertGreaterEqual(
                sample.wrapper_elapsed_seconds, sample.science_elapsed_seconds
            )
            self.assertEqual(sample.memory_current_bytes, 123_456)
            self.assertEqual(sample.memory_peak_bytes, 234_567)
            self.assertEqual(sample.memory_anon_bytes, 80_000)
            self.assertEqual(sample.memory_file_bytes, 30_000)
            self.assertEqual(sample.memory_kernel_bytes, 10_000)
            self.assertEqual(sample.swap_start_bytes, 0)
            self.assertEqual(sample.swap_end_bytes, 0)

    def test_v38_native_monitor_samples_terminal_slice_peak_after_child_exit(self):
        progress_line = (
            '{"completed_rows":65536,"schema":"borsuk-v38-preflight-progress-v1",'
            '"total_rows":65536}\n'
        )
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            cgroup = root / "science-cgroup"
            cgroup.mkdir()
            (cgroup / "memory.current").write_text("123456\n")
            (cgroup / "memory.peak").write_text("234567\n")
            (cgroup / "memory.swap.current").write_text("0\n")
            (cgroup / "memory.stat").write_text(
                "anon 80000\nfile 30000\nkernel 10000\n"
            )
            terminal_peak = 256 * 1_048_576 + 1
            program = (
                "import pathlib,sys;"
                f"root=pathlib.Path({str(cgroup)!r});"
                f"sys.stderr.write({progress_line!r});sys.stderr.flush();"
                "root.joinpath('memory.current').write_text('0\\n');"
                f"root.joinpath('memory.peak').write_text('{terminal_peak}\\n')"
            )
            with mock.patch(
                "scripts.run_v38_boundary_spill_spot._v38_psi_full_avg10",
                return_value=0.0,
            ):
                returncode, sample = run_v38_native_process(
                    [sys.executable, "-c", program],
                    root / "stdout",
                    root / "progress",
                    phase="preflight-spill",
                    poll_seconds=1.0,
                    memory_cgroup=cgroup,
                )
        self.assertEqual(returncode, 1)
        self.assertEqual(sample.memory_current_bytes, 0)
        self.assertEqual(sample.memory_peak_bytes, terminal_peak)
        self.assertEqual(classify_v38_monitor_sample(sample), "memory-stop")

    def test_v38_native_monitor_preserves_trigger_when_terminal_cgroup_is_gone(self):
        progress_line = (
            '{"completed_rows":65536,"schema":"borsuk-v38-preflight-progress-v1",'
            '"total_rows":65536}\n'
        )
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            cgroup = root / "science-cgroup"
            cgroup.mkdir()
            triggered = 256 * 1_048_576 + 1
            (cgroup / "memory.current").write_text(f"{triggered}\n")
            (cgroup / "memory.peak").write_text(f"{triggered}\n")
            (cgroup / "memory.swap.current").write_text("0\n")
            (cgroup / "memory.stat").write_text(
                "anon 80000\nfile 30000\nkernel 10000\n"
            )
            program = (
                "import sys,time;"
                f"sys.stderr.write({progress_line!r});sys.stderr.flush();"
                "time.sleep(30)"
            )
            remove_cgroup = (
                "import pathlib;"
                f"root=pathlib.Path({str(cgroup)!r});"
                "[path.unlink() for path in root.iterdir()]"
            )
            with mock.patch(
                "scripts.run_v38_boundary_spill_spot._v38_psi_full_avg10",
                return_value=0.0,
            ):
                returncode, sample = run_v38_native_process(
                    [sys.executable, "-c", program],
                    root / "stdout",
                    root / "progress",
                    phase="preflight-spill",
                    poll_seconds=0.01,
                    memory_cgroup=cgroup,
                    terminate_command=[sys.executable, "-c", remove_cgroup],
                )
        self.assertEqual(returncode, 1)
        self.assertEqual(sample.memory_current_bytes, triggered)
        self.assertEqual(classify_v38_monitor_sample(sample), "memory-stop")

    def _sample(self, phase: str) -> V38MonitorSample:
        return V38MonitorSample(
            phase=phase,
            science_elapsed_seconds=1.0,
            wrapper_elapsed_seconds=2.0,
            last_progress_seconds=1.0,
            memory_current_bytes=100,
            memory_peak_bytes=200,
            memory_anon_bytes=50,
            memory_file_bytes=25,
            memory_kernel_bytes=25,
            psi_full_avg10=0.0,
            swap_start_bytes=0,
            swap_end_bytes=0,
        )

    def test_v38_monitor_enforces_phase_specific_stops(self):
        for phase, changes, expected in [
            ("build-spill", {"memory_current_bytes": 3 * 1_073_741_824 + 1}, "memory-stop"),
            ("build-spill", {"memory_peak_bytes": 3 * 1_073_741_824 + 1}, "memory-stop"),
            ("build-spill", {"science_elapsed_seconds": 600.001}, "science-timeout"),
            ("build-spill", {"wrapper_elapsed_seconds": 720.001}, "wrapper-timeout"),
            ("build-spill", {"last_progress_seconds": 120.001}, "progress-timeout"),
            ("evaluate-ceiling", {"memory_current_bytes": 256 * 1_048_576 + 1}, "memory-stop"),
            ("evaluate-ceiling", {"science_elapsed_seconds": 120.001}, "science-timeout"),
            ("evaluate-ceiling", {"wrapper_elapsed_seconds": 180.001}, "wrapper-timeout"),
            ("evaluate-ceiling", {"last_progress_seconds": 30.001}, "progress-timeout"),
            ("evaluate-ceiling", {"psi_full_avg10": 0.751}, "psi-stop"),
            ("evaluate-ceiling", {"swap_end_bytes": 1}, "swap-stop"),
        ]:
            with self.subTest(phase=phase, expected=expected):
                sample = dataclasses.replace(self._sample(phase), **changes)
                self.assertEqual(classify_v38_monitor_sample(sample), expected)
        self.assertIsNone(classify_v38_monitor_sample(self._sample("build-spill")))
        self.assertIsNone(classify_v38_monitor_sample(self._sample("evaluate-ceiling")))

    def test_v38_native_monitor_fails_closed_on_missing_or_invalid_telemetry(self):
        progress_line = (
            '{"completed_rows":65536,"schema":"borsuk-v38-preflight-progress-v1",'
            '"total_rows":65536}\n'
        )
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            cgroup = root / "missing-cgroup"
            cgroup.mkdir()
            marker = root / "launched"
            program = f"import pathlib;pathlib.Path({str(marker)!r}).write_text('bad')"
            with mock.patch(
                "scripts.run_v38_boundary_spill_spot._v38_psi_full_avg10",
                return_value=0.0,
            ):
                with self.assertRaises(FileNotFoundError):
                    run_v38_native_process(
                        [sys.executable, "-c", program],
                        root / "missing-stdout",
                        root / "missing-progress",
                        phase="preflight-spill",
                        poll_seconds=0.01,
                        memory_cgroup=cgroup,
                    )
            self.assertFalse(marker.exists())

            cgroup = root / "valid-cgroup"
            cgroup.mkdir()
            (cgroup / "memory.current").write_text("123456\n")
            (cgroup / "memory.peak").write_text("234567\n")
            (cgroup / "memory.swap.current").write_text("0\n")
            (cgroup / "memory.stat").write_text(
                "anon 80000\nfile 30000\nkernel 10000\n"
            )
            marker = root / "garbage-finished"
            program = (
                "import pathlib,sys,time;"
                "sys.stderr.write('garbage\\n');sys.stderr.flush();"
                "time.sleep(0.5);"
                f"pathlib.Path({str(marker)!r}).write_text('bad');"
                f"sys.stderr.write({progress_line!r});sys.stderr.flush()"
            )
            with mock.patch(
                "scripts.run_v38_boundary_spill_spot._v38_psi_full_avg10",
                return_value=0.0,
            ):
                with self.assertRaisesRegex(ValueError, "progress"):
                    run_v38_native_process(
                        [sys.executable, "-c", program],
                        root / "garbage-stdout",
                        root / "garbage-progress",
                        phase="preflight-spill",
                        poll_seconds=0.01,
                        memory_cgroup=cgroup,
                    )
            self.assertFalse(marker.exists())

    def test_v38_worker_parser_is_local_only_and_exact(self):
        arguments = [
            "run-v38",
            "--execute-v38-worker",
            "--root",
            "/work/v38",
            "--plan",
            "/work/plan.json",
            "--manifest",
            "/work/manifest.json",
            "--binary",
            "/work/v38-boundary-spill",
            "--instance-id",
            "i-0123456789abcdef0",
        ]
        self.assertEqual(
            parse_v38_worker_args(arguments),
            V38WorkerInvocation(
                root=pathlib.Path("/work/v38"),
                plan=pathlib.Path("/work/plan.json"),
                manifest=pathlib.Path("/work/manifest.json"),
                binary=pathlib.Path("/work/v38-boundary-spill"),
                instance_id="i-0123456789abcdef0",
            ),
        )
        for extra in (
            ["--bucket", "forbidden"],
            ["--page-prefix", "forbidden"],
            ["--endpoint", "forbidden"],
            ["--execute-d3", "true"],
        ):
            with self.subTest(flag=extra[0]):
                with self.assertRaises(ValueError):
                    parse_v38_worker_args(arguments + extra)
        with mock.patch("sys.stderr"):
            self.assertEqual(main(["run-v38", "--unknown"]), 1)

    def test_v38_terminal_binds_plan_result_monitor_and_exact_status(self):
        plan = _plan("build-spill")
        artifacts = [_identity("spill-relation", "4"), _identity("spill-postings", "5")]
        for artifact in artifacts:
            artifact["uri"] = f"{plan.output_prefix}{artifact['role']}"
        terminal = {
            "artifacts": artifacts,
            "binary_bytes": plan.binary_bytes,
            "binary_sha256": plan.binary_sha256,
            "binary_uri": plan.binary_uri,
            "claim_eligible": False,
            "instance_id": "i-0123456789abcdef0",
            "manifest_bytes": plan.manifest_bytes,
            "manifest_sha256": plan.manifest_sha256,
            "manifest_uri": plan.manifest_uri,
            "monitor": dataclasses.asdict(self._sample("build-spill")),
            "phase": plan.phase,
            "progress": {
                "encoded_bytes": 100,
                "role": "progress",
                "sha256": "6" * 64,
                "uri": f"{plan.output_prefix}progress.json",
            },
            "result": {
                "encoded_bytes": 200,
                "role": "local-result",
                "sha256": "7" * 64,
                "uri": f"{plan.output_prefix}local-result.json",
            },
            "run_id": plan.run_id,
            "schema": "borsuk-v38-spot-terminal-v1",
            "source_archive_bytes": plan.source_archive_bytes,
            "source_archive_sha256": plan.source_archive_sha256,
            "source_archive_uri": plan.source_archive_uri,
            "source_commit": plan.source_commit,
            "status": "complete",
        }
        raw = canonical_v38_terminal_bytes(terminal, plan, "complete")
        self.assertEqual(raw[-1:], b"\n")
        independently_sampled = json.loads(raw)
        independently_sampled["monitor"]["memory_current_bytes"] = 50
        self.assertEqual(
            json.loads(canonical_v38_terminal_bytes(independently_sampled, plan, "complete")),
            independently_sampled,
        )
        for path, value in [
            (("status",), "failed"),
            (("manifest_sha256",), "8" * 64),
            (("monitor", "memory_current_bytes"), 3 * 1_073_741_824 + 1),
            (("monitor", "memory_peak_bytes"), 3 * 1_073_741_824 + 1),
            (("artifacts",), terminal["artifacts"][:1]),
        ]:
            changed = json.loads(raw)
            target = changed
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    canonical_v38_terminal_bytes(changed, plan, "complete")

    def test_v38_worker_script_has_one_sandbox_and_explicit_cleanup(self):
        plan = _plan()
        script = build_v38_worker_script(plan)
        self.assertEqual(script.count("systemd-run"), 1)
        self.assertIn("MemoryMax=", script)
        self.assertIn("MemorySwapMax=0", script)
        self.assertIn("ProtectSystem=strict", script)
        self.assertIn("shutdown -h now", script)
        for uri in (plan.source_archive_uri, plan.binary_uri, plan.manifest_uri):
            self.assertIn(f"aws s3 cp {uri}", script)
        for digest in (
            plan.source_archive_sha256,
            plan.binary_sha256,
            plan.manifest_sha256,
        ):
            self.assertIn(digest, script)
        self.assertIn("--execute-v38-worker", script)
        self.assertIn('--root "$phase_root"', script)
        self.assertIn('--plan "$plan_path"', script)
        self.assertIn('--manifest "$manifest_path"', script)
        self.assertIn('--binary "$binary_path"', script)
        self.assertIn('--instance-id "$instance_id"', script)
        self.assertIn('systemctl start "$slice_unit"', script)
        self.assertIn("MemoryAccounting=yes", script)
        self.assertIn("dnf install -y python3.12", script)
        self.assertIn('env PYTHONPATH="$source_root/.v38-python" python3.12', script)
        self.assertIn('if test "${V38_IN_SLICE:-0}" != 1; then', script)
        self.assertIn("V38_IN_SLICE=1", script)
        self.assertLess(
            script.index("dnf install -y python3.12"),
            script.index('if test "${V38_IN_SLICE:-0}" != 1; then'),
        )
        self.assertLess(script.index("systemd-run"), script.index("aws s3 cp"))
        self.assertIn('--slice="$slice_unit"', script)
        self.assertIn("--property=RuntimeMaxSec=720", script)
        self.assertIn('if test "$status" -ne 0', script)
        self.assertIn('tail -c 65536 "$boot_log" >"$boot_failure_log"', script)
        self.assertIn("BOOT_FAILURE.log", script)
        self.assertIn("aws s3api put-object", script)
        self.assertIn("--if-none-match '*'", script)
        self.assertNotIn("list-objects", script)
        self.assertNotIn("rm -rf", script)
        self.assertNotIn("--recursive", script)
        self.assertNotIn("find ", script)
        self.assertIn("unlink", script)


if __name__ == "__main__":
    unittest.main()
