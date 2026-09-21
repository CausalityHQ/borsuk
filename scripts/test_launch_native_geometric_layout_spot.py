from __future__ import annotations

import dataclasses
import json
import shlex
import subprocess
import unittest
from types import SimpleNamespace

from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS,
    FROZEN_SOURCE,
    FROZEN_TRUTH,
    SourceArchiveIdentity,
    SpotLayoutPlan,
    _atomic_put,
    _terminate_and_wait,
    _validate_terminal_bytes,
    build_launch_specs,
    build_plan,
    worker_script,
)


class NativeGeometricLayoutSpotTests(unittest.TestCase):
    def valid_plan(self) -> SpotLayoutPlan:
        return build_plan(
            profile="causality",
            source_commit="12" * 20,
            source_archive=SourceArchiveIdentity(
                uri="s3://borsuk-bench-453182569524-euc1/research/native-geometric-layout/source.tar.gz",
                sha256="34" * 32,
                encoded_bytes=123_456,
            ),
            source=FROZEN_SOURCE,
            truth=FROZEN_TRUTH,
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/"
                "native-geometric-layout/1212121212121212121212121212121212121212/"
                "runs/relaion-100k-dev1000-a0001"
            ),
            image_id="ami-06121aa3085b6f918",
            security_group_id="sg-0b1fd3e4fbde4af0d",
            instance_profile_arn=(
                "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
            ),
            targets=DEFAULT_TARGETS,
        )

    def test_plan_is_one_spot_attempt_over_only_frozen_100k_inputs(self) -> None:
        plan = self.valid_plan()
        self.assertEqual(plan.attempt, 1)
        self.assertEqual(plan.market, "spot")
        self.assertEqual(plan.instance_type, "c7i.8xlarge")
        self.assertEqual(plan.wall_seconds, 7200)
        self.assertEqual(plan.maximum_rss_bytes, 16 * 1024**3)
        self.assertEqual(plan.source, FROZEN_SOURCE)
        self.assertEqual(plan.truth, FROZEN_TRUTH)

        invalid = (
            dataclasses.replace(plan, profile="default"),
            dataclasses.replace(plan, market="on-demand"),
            dataclasses.replace(plan, attempt=2),
            dataclasses.replace(plan, source_commit="main"),
            dataclasses.replace(plan, wall_seconds=0),
            dataclasses.replace(plan, maximum_rss_bytes=0),
            dataclasses.replace(
                plan,
                source=dataclasses.replace(
                    plan.source,
                    uri=plan.source.uri.replace("100k", "1m"),
                ),
            ),
        )
        for candidate in invalid:
            with self.subTest(candidate=candidate), self.assertRaises(ValueError):
                build_plan(**dataclasses.asdict(candidate))

    def test_worker_constructs_before_truth_exists_and_scrubs_constructor_env(self) -> None:
        script = worker_script(self.valid_plan())
        syntax = subprocess.run(
            ["bash", "-n"], input=script, text=True, capture_output=True, check=False
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        construct_start = script.index("phase=construct")
        seal_start = script.index("phase=seal")
        evaluate_start = script.index("phase=evaluate")
        construct = script[construct_start:evaluate_start]
        self.assertNotIn(FROZEN_TRUTH.uri, construct)
        self.assertNotIn("truth.parquet", construct)
        self.assertIn("env -i", construct)
        self.assertIn("MAXIMUM_RSS_BYTES=17179869184", script)
        self.assertIn("run_capped()", script)
        self.assertIn("kill -TERM -- \"-$pid\"", script)
        self.assertIn("OPENBLAS_NUM_THREADS=32", construct)
        self.assertIn("OMP_NUM_THREADS=32", construct)
        self.assertIn("unshare --net --fork", construct)
        self.assertIn("sealed-memberships.json", script[seal_start:evaluate_start])
        self.assertIn("chmod 0444 membership-*.parquet", script[seal_start:evaluate_start])
        self.assertLess(construct_start, seal_start)
        self.assertLess(seal_start, evaluate_start)
        self.assertIn("export OPENBLAS_NUM_THREADS=32", script[evaluate_start:])
        self.assertIn("export OMP_NUM_THREADS=32", script[evaluate_start:])
        command_lines = [
            line
            for line in construct.splitlines()
            if "native_geometric_layout_screen.py" in line and " construct " in line
        ]
        self.assertEqual(len(command_lines), 4)
        for line in command_lines:
            tokens = shlex.split(line)
            self.assertEqual(
                tokens[:7],
                ["run_capped", "timeout", "7200", "unshare", "--net", "--fork", "env"],
            )
            command = tokens.index("construct")
            self.assertEqual(tokens[command + 1], "--authority")
            self.assertEqual(tokens[command + 3], "--source")
            self.assertEqual(tokens[command + 5], "--output")
        self.assertIn(FROZEN_TRUTH.uri, script[evaluate_start:])
        evaluate = script[evaluate_start:script.index("phase=assemble")]
        self.assertIn("setpriv --reuid=nobody --regid=nobody --clear-groups", evaluate)
        self.assertIn("mkdir evaluation", evaluate)
        self.assertIn("chown nobody:nobody evaluation", evaluate)
        self.assertIn("trap terminal EXIT", script)
        self.assertIn("shutdown -h now", script)

    def test_launch_specs_are_serial_one_time_spot_and_delete_storage(self) -> None:
        plan = self.valid_plan()
        specs = build_launch_specs(plan)
        self.assertEqual(len(specs), len(DEFAULT_TARGETS))
        for spec, target in zip(specs, DEFAULT_TARGETS, strict=True):
            self.assertLessEqual(len(spec["ClientToken"]), 64)
            self.assertEqual(spec["MaxCount"], 1)
            self.assertEqual(spec["MinCount"], 1)
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(
                spec["InstanceMarketOptions"]["SpotOptions"],
                {
                    "InstanceInterruptionBehavior": "terminate",
                    "SpotInstanceType": "one-time",
                },
            )
            self.assertEqual(
                spec["NetworkInterfaces"][0]["SubnetId"], target.subnet_id
            )
            self.assertTrue(
                spec["BlockDeviceMappings"][0]["Ebs"]["DeleteOnTermination"]
            )
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")

    def test_worker_independently_validates_before_complete(self) -> None:
        script = worker_script(self.valid_plan())
        validate_start = script.index("phase=validate")
        complete_start = script.index("status=complete", validate_start)
        validation = script[validate_start:complete_start]
        self.assertIn("run_capped timeout 7200 env", validation)
        self.assertIn("validate_native_geometric_layout_result", validation)
        self.assertIn("validate_result(paths, authority)", validation)
        self.assertIn("validation.json", validation)
        self.assertIn('json.loads(pathlib.Path("sealed-memberships.json")', validation)
        self.assertIn('$output/artifacts/validation.json', script)
        self.assertLess(validate_start, complete_start)

    def test_terminal_receipt_is_complete_and_bound_to_source(self) -> None:
        commit = "12" * 20
        output = "s3://bucket/frozen"
        artifacts = {
            role: {
                "encoded_bytes": 123 + ordinal,
                "role": role,
                "sha256": str(ordinal + 1) * 64,
                "uri": f"{output}/artifacts/{name}",
            }
            for ordinal, (role, name) in enumerate(
                (
                    ("membership-seal", "sealed-memberships.json"),
                    ("result", "result.json"),
                    ("validation", "validation.json"),
                )
            )
        }
        terminal = {
            "artifacts": artifacts,
            "claim_eligible": True,
            "elapsed_seconds": 123,
            "exit_code": 0,
            "instance_id": "i-0123456789abcdef0",
            "phase": "complete",
            "schema": "borsuk-native-geometric-layout-terminal-v1",
            "source_commit": commit,
            "status": "complete",
        }
        body = json.dumps(terminal, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        self.assertEqual(
            _validate_terminal_bytes(body, commit, terminal["instance_id"], output),
            terminal,
        )
        for key, value in (
            ("claim_eligible", False),
            ("exit_code", 1),
            ("instance_id", ""),
            ("phase", "validate"),
            ("source_commit", "34" * 20),
            ("status", "failed"),
        ):
            mutated = dict(terminal)
            mutated[key] = value
            payload = (
                json.dumps(mutated, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            )
            with self.subTest(key=key), self.assertRaises(ValueError):
                _validate_terminal_bytes(
                    payload, commit, terminal["instance_id"], output
                )
        mutated = json.loads(body)
        mutated["artifacts"]["result"]["sha256"] = "a" * 63
        with self.assertRaises(ValueError):
            _validate_terminal_bytes(
                json.dumps(mutated, sort_keys=True, separators=(",", ":")).encode()
                + b"\n",
                commit,
                terminal["instance_id"],
                output,
            )
        self.assertIn("latest/api/token", worker_script(self.valid_plan()))

    def test_worker_authenticates_requirements_before_installing(self) -> None:
        script = worker_script(self.valid_plan())
        self.assertIn(
            "dnf install -y -q python3.12 python3.12-pip tar gzip time util-linux",
            script,
        )
        self.assertIn("python3.12 -m venv .venv", script)
        authenticate = script.index(
            "printf '%s  repo/scripts/requirements-format-bench.txt"
        )
        install = script.index("pip install")
        self.assertLess(authenticate, install)
        self.assertIn(
            '[ "$(cat repo/.borsuk-source-commit)" = '
            "1212121212121212121212121212121212121212 ]",
            script,
        )

    def test_reservation_uses_create_only_header_with_pinned_boto3(self) -> None:
        class Events:
            def __init__(self) -> None:
                self.header = None
                self.unregistered = False

            def register_first(self, name, callback, *, unique_id):
                request = SimpleNamespace(headers={})
                callback(request)
                self.header = request.headers.get("If-None-Match")

            def unregister(self, name, *, unique_id):
                self.unregistered = True

        class S3:
            def __init__(self) -> None:
                self.events = Events()
                self.meta = SimpleNamespace(events=self.events)
                self.arguments = None

            def put_object(self, **arguments):
                self.arguments = arguments

        s3 = S3()
        _atomic_put(s3, bucket="bucket", key="prefix/reservation.json", body=b"{}\n")
        self.assertEqual(s3.events.header, "*")
        self.assertTrue(s3.events.unregistered)
        self.assertNotIn("IfNoneMatch", s3.arguments)

    def test_termination_waits_for_instance_clearance(self) -> None:
        class Waiter:
            def __init__(self) -> None:
                self.arguments = None

            def wait(self, **arguments):
                self.arguments = arguments

        class Ec2:
            def __init__(self) -> None:
                self.terminated = None
                self.waiter = Waiter()

            def terminate_instances(self, **arguments):
                self.terminated = arguments

            def get_waiter(self, name):
                self.waiter_name = name
                return self.waiter

        ec2 = Ec2()
        _terminate_and_wait(ec2, "i-0123456789abcdef0")
        self.assertEqual(ec2.terminated, {"InstanceIds": ["i-0123456789abcdef0"]})
        self.assertEqual(ec2.waiter_name, "instance_terminated")
        self.assertEqual(
            ec2.waiter.arguments,
            {
                "InstanceIds": ["i-0123456789abcdef0"],
                "WaiterConfig": {"Delay": 5, "MaxAttempts": 60},
            },
        )


if __name__ == "__main__":
    unittest.main()
