import base64
import unittest
from unittest import mock

from scripts import launch_v23_rabitq_spot as launcher


class V23RaBitQSpotLauncherTests(unittest.TestCase):
    def test_cli_dispatches_exact_construction_plan(self) -> None:
        arguments = [
            "--run-id", "v23-rabitq-construction-fixture",
            "--source-commit", "1" * 40,
            "--source-archive-uri", "s3://fixture/source.tar.zst",
            "--source-archive-sha256", "2" * 64,
            "--source-archive-bytes", "8192",
            "--binary-uri", "s3://fixture/constructor",
            "--binary-sha256", "3" * 64,
            "--binary-bytes", "4096",
            "--manifest-uri", "s3://fixture/construction-manifest",
            "--manifest-sha256", "4" * 64,
            "--manifest-bytes", "2048",
            "--d2-report-uri", "s3://fixture/d2-report",
            "--d2-report-sha256", "5" * 64,
            "--d2-report-bytes", "1024",
            "--query-parquet-uri", "s3://fixture/query-parquet",
            "--query-parquet-sha256", "6" * 64,
            "--query-parquet-bytes", "8192",
            "--development-output-prefix", "s3://fixture/development/",
            "--output-prefix", "s3://fixture/terminal/construction/",
            "--execute-construction",
        ]
        sts = mock.Mock()
        sts.get_caller_identity.return_value = {"Account": launcher.EXPECTED_AWS_ACCOUNT}
        with (
            mock.patch.object(launcher, "_clients", return_value=(sts, mock.Mock(), mock.Mock())),
            mock.patch.object(launcher, "run_spot", return_value="s3://fixture/terminal") as run,
            mock.patch("builtins.print"),
        ):
            self.assertEqual(launcher.main(arguments), 0)
        self.assertIsInstance(run.call_args.args[0], launcher.ConstructionLaunchPlan)

    def test_construction_launch_is_phase_separated_and_opens_no_query_object(self) -> None:
        plan = launcher.build_construction_launch_plan(
            run_id="v23-rabitq-construction-fixture",
            source_commit="1" * 40,
            source_archive_sha256="2" * 64,
            source_archive_uri="s3://fixture/source.tar.zst",
            source_archive_bytes=8192,
            binary_uri="s3://fixture/constructor",
            binary_sha256="3" * 64,
            binary_bytes=4096,
            manifest_uri="s3://fixture/construction-manifest",
            manifest_sha256="4" * 64,
            manifest_bytes=2048,
            d2_report_uri="s3://fixture/d2-report",
            d2_report_sha256="5" * 64,
            d2_report_bytes=1024,
            query_parquet_uri="s3://fixture/query-parquet",
            query_parquet_sha256="6" * 64,
            query_parquet_bytes=8192,
            development_output_prefix="s3://fixture/development/",
            output_prefix="s3://fixture/terminal/v23-rabitq-construction-fixture/",
        )
        spec = launcher.build_launch_spec(plan)
        user_data = base64.b64decode(spec["UserData"]).decode()

        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertIn("scripts.run_v23_rabitq_construction", user_data)
        self.assertIn(
            "/opt/borsuk-rabitq-venv/bin/python -m scripts.run_v23_rabitq_construction",
            user_data,
        )
        self.assertIn("--execute-construction", user_data)
        self.assertIn("--d2-report-sha256 " + "5" * 64, user_data)
        self.assertIn("--query-parquet-sha256 " + "6" * 64, user_data)
        self.assertNotIn("aws s3 cp 's3://fixture/d2-report'", user_data)
        self.assertNotIn("aws s3 cp 's3://fixture/query-parquet'", user_data)
        self.assertNotIn("--execute-development", user_data)
        self.assertNotIn("holdout", user_data)
        self.assertNotIn("d3", user_data.lower())
        self.assertEqual(
            spec["TagSpecifications"][0]["Tags"][1]["Value"],
            "v23-rabitq-construction",
        )

    def test_launch_spec_is_spot_only_and_binds_immutable_execution(self) -> None:
        plan = launcher.build_launch_plan(
            run_id="v23-rabitq-fixture",
            source_commit="1" * 40,
            source_archive_sha256="2" * 64,
            source_archive_uri="s3://fixture/source.tar.zst",
            source_archive_bytes=8192,
            binary_uri="s3://fixture/binary",
            binary_sha256="3" * 64,
            binary_bytes=4096,
            manifest_uri="s3://fixture/manifest",
            manifest_sha256="4" * 64,
            manifest_bytes=2048,
            output_prefix="s3://fixture/terminal/v23-rabitq-fixture/",
        )
        spec = launcher.build_launch_spec(plan)
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertEqual(
            spec["InstanceMarketOptions"]["SpotOptions"]["InstanceInterruptionBehavior"],
            "terminate",
        )
        user_data = base64.b64decode(spec["UserData"]).decode()
        self.assertIn("--execute-development", user_data)
        self.assertIn(
            "/opt/borsuk-rabitq-venv/bin/python -m scripts.run_v23_rabitq_falsifier",
            user_data,
        )
        self.assertIn(plan.manifest_sha256, user_data)
        self.assertIn(plan.binary_sha256, user_data)
        self.assertIn("/proc/pressure/memory", user_data)
        self.assertIn("/proc/meminfo", user_data)
        self.assertIn("rss-limit", user_data)
        self.assertIn("psi-limit", user_data)
        self.assertIn("swap-growth-limit", user_data)
        self.assertIn("progress-limit", user_data)
        self.assertIn("ps -eo pgid=,cputimes=", user_data)
        self.assertNotIn('"/proc/$pid/stat"', user_data)
        self.assertIn("wall-limit", user_data)
        self.assertIn("kill -TERM -- -$pgid", user_data)
        self.assertNotIn("holdout", user_data)
        self.assertNotIn("d3", user_data.lower())

    def test_idempotent_token_and_terminal_prefix_bind_every_immutable_input(self) -> None:
        values = dict(
            run_id="v23-rabitq-fixture",
            source_commit="1" * 40,
            source_archive_sha256="2" * 64,
            source_archive_uri="s3://fixture/source.tar.zst",
            source_archive_bytes=8192,
            binary_uri="s3://fixture/binary",
            binary_sha256="3" * 64,
            binary_bytes=4096,
            manifest_uri="s3://fixture/manifest",
            manifest_sha256="4" * 64,
            manifest_bytes=2048,
            output_prefix="s3://fixture/terminal/v23-rabitq-fixture/",
        )
        left = launcher.build_launch_plan(**values)
        right = launcher.build_launch_plan(**values)
        self.assertEqual(left.client_token, right.client_token)
        self.assertEqual(left, right)
        changed = launcher.build_launch_plan(**(values | {"manifest_sha256": "5" * 64}))
        self.assertNotEqual(left.client_token, changed.client_token)

    def test_instance_is_terminated_immediately_after_terminal_receipt(self) -> None:
        plan = launcher.build_launch_plan(
            run_id="v23-rabitq-fixture",
            source_commit="1" * 40,
            source_archive_sha256="2" * 64,
            source_archive_uri="s3://fixture/source.tar.zst",
            source_archive_bytes=8192,
            binary_uri="s3://fixture/binary",
            binary_sha256="3" * 64,
            binary_bytes=4096,
            manifest_uri="s3://fixture/manifest",
            manifest_sha256="4" * 64,
            manifest_bytes=2048,
            output_prefix="s3://fixture/terminal/v23-rabitq-fixture/",
        )
        ec2 = mock.Mock()
        ec2.run_instances.return_value = {"Instances": [{"InstanceId": "i-fixture"}]}
        s3 = mock.Mock()
        s3.head_object.side_effect = [Exception("not ready"), {"ContentLength": 64}]
        with mock.patch.object(launcher.time, "sleep"):
            terminal = launcher.run_spot(plan, ec2_client=ec2, s3_client=s3)
        self.assertEqual(terminal, "s3://fixture/terminal/v23-rabitq-fixture/COMPLETE.json")
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-fixture"])

    def test_worker_publishes_authenticated_log_before_terminal_marker(self) -> None:
        plan = launcher.build_launch_plan(
            run_id="v23-rabitq-fixture",
            source_commit="1" * 40,
            source_archive_sha256="2" * 64,
            source_archive_uri="s3://fixture/source.tar.zst",
            source_archive_bytes=8192,
            binary_uri="s3://fixture/binary",
            binary_sha256="3" * 64,
            binary_bytes=4096,
            manifest_uri="s3://fixture/manifest",
            manifest_sha256="4" * 64,
            manifest_bytes=2048,
            output_prefix="s3://fixture/terminal/v23-rabitq-fixture/",
        )
        user_data = base64.b64decode(launcher.build_launch_spec(plan)["UserData"]).decode()

        self.assertIn('exec >> "$root/worker.log" 2>&1', user_data)
        self.assertIn('worker_log_sha256=$(sha256sum "$root/worker.log"', user_data)
        self.assertIn('worker_log_bytes=$(stat -c %s "$root/worker.log")', user_data)
        self.assertIn('"worker_log_sha256"', user_data)
        self.assertIn('"worker_log_bytes"', user_data)
        self.assertIn('"worker_log_uri"', user_data)
        log_upload = user_data.index('worker.log" \'s3://fixture/terminal/v23-rabitq-fixture/worker.log\'')
        failed_upload = user_data.index("s3://fixture/terminal/v23-rabitq-fixture/FAILED.json")
        complete_upload = user_data.index("s3://fixture/terminal/v23-rabitq-fixture/COMPLETE.json")
        self.assertLess(log_upload, failed_upload)
        self.assertLess(log_upload, complete_upload)

    def test_worker_provisions_pinned_python_before_direct_phase_execution(self) -> None:
        plan = launcher.build_launch_plan(
            run_id="v23-rabitq-fixture",
            source_commit="1" * 40,
            source_archive_sha256="2" * 64,
            source_archive_uri="s3://fixture/source.tar.zst",
            source_archive_bytes=8192,
            binary_uri="s3://fixture/binary",
            binary_sha256="3" * 64,
            binary_bytes=4096,
            manifest_uri="s3://fixture/manifest",
            manifest_sha256="4" * 64,
            manifest_bytes=2048,
            output_prefix="s3://fixture/terminal/v23-rabitq-fixture/",
        )
        user_data = base64.b64decode(launcher.build_launch_spec(plan)["UserData"]).decode()

        install = user_data.index("python3 -m pip install uv==0.8.17")
        create = user_data.index("uv venv --python 3.12 /opt/borsuk-rabitq-venv")
        dependencies = user_data.index(
            "uv pip install --python /opt/borsuk-rabitq-venv/bin/python "
            "--requirement scripts/requirements-format-bench.txt"
        )
        execute = user_data.index(
            "/opt/borsuk-rabitq-venv/bin/python -m scripts.run_v23_rabitq_falsifier"
        )
        self.assertLess(install, create)
        self.assertLess(create, dependencies)
        self.assertLess(dependencies, execute)
        self.assertNotIn("uv run --offline", user_data)


if __name__ == "__main__":
    unittest.main()
