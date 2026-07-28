#!/usr/bin/env python3
"""Static safety checks for the single detached AWS launcher."""

from __future__ import annotations

import unittest
from pathlib import Path


class AwsFormatLauncherTest(unittest.TestCase):
    def test_launcher_packages_exact_source_and_uses_remote_tmux(self) -> None:
        source = (
            Path(__file__).resolve().parent / "launch_aws_format_qualification.sh"
        ).read_text()
        self.assertIn("git ls-files", source)
        self.assertIn("shasum -a 256", source)
        self.assertIn("ec2 wait instance-status-ok", source)
        self.assertIn("ssm wait command-executed", source)
        self.assertIn("tmux new-session -d", source)
        self.assertIn("bench_format_qualification_aws.sh", source)
        self.assertIn("bench_format_tuning_aws.sh", source)
        self.assertIn("BORSUK_FORMAT_CAMPAIGN", source)
        self.assertIn("range-cap", source)
        self.assertIn("wal-layout", source)
        self.assertIn("bench_wal_layout_qualification_aws.sh", source)
        self.assertIn("BORSUK_RUN_WAL_LAYOUT_QUALIFICATION", source)
        self.assertIn("BORSUK_LOCAL_DISK_CLASS", source)
        self.assertIn("BORSUK_INSTANCE_ID", source)
        self.assertIn("BORSUK_AMI_ID", source)
        self.assertIn("BORSUK_FORMAT_SOURCE_ARCHIVE", source)
        self.assertIn("describe-volumes", source)
        self.assertIn("COPYFILE_DISABLE=1 tar", source)
        self.assertIn("tar_xattr_args", source)
        self.assertIn("--no-xattrs", source)
        self.assertIn(
            "docs/research/storage-layout-qualification-protocol.json", source
        )
        self.assertIn("docs/research/wal-layout-qualification-protocol.json", source)
        self.assertIn('remote_sha256="$(sha256sum "$remote_archive"', source)
        self.assertIn('[[ "$remote_sha256" == "$expected_sha256" ]]', source)
        self.assertIn(
            'printf "%s\\n" "$expected_sha256" > "$workspace/source.ready"', source
        )
        self.assertIn(
            '[[ "$(cat "$workspace/source.ready")" == "$expected_sha256" ]]', source
        )
        self.assertIn("FORMAT_DECISION_REQUIRED", source)
        self.assertNotIn("bench_s3_full.sh", source)


if __name__ == "__main__":
    unittest.main()
