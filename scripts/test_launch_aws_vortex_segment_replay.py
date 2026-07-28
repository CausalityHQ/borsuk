#!/usr/bin/env python3
"""Static safety checks for the detached real-artifact replay launcher."""

from __future__ import annotations

import unittest
from pathlib import Path


class AwsVortexSegmentReplayLauncherTest(unittest.TestCase):
    def test_launcher_pins_source_identity_and_detaches_without_launching_tests(
        self,
    ) -> None:
        source = (
            Path(__file__).resolve().parent / "launch_aws_vortex_segment_replay.sh"
        ).read_text()

        self.assertIn("git ls-files", source)
        self.assertIn("shasum -a 256", source)
        self.assertIn("sts get-caller-identity", source)
        self.assertIn("AWS account mismatch", source)
        self.assertIn("BORSUK_VORTEX_SOURCE_URI", source)
        self.assertIn(
            "publication/indexes/20260724T092000Z/full-s3/"
            "20260724T092000Z/fashion-mnist-784/srht-pq-scan",
            source,
        )
        self.assertIn("s3api list-objects-v2", source)
        self.assertIn("Contents[?ends_with(Key, `.parquet`)]", source)
        self.assertIn("tmux new-session -d", source)
        self.assertIn("sudo shutdown -c", source)
        self.assertIn("loginctl enable-linger ec2-user", source)
        self.assertIn("loginctl show-user ec2-user", source)
        self.assertIn("sudo chown -R ec2-user:ec2-user", source)
        self.assertIn(
            "sudo install -d -o ec2-user -g ec2-user "
            "/home/ec2-user/borsuk-vortex-segment-replay",
            source,
        )
        self.assertIn(
            'sudo -iu ec2-user test -w "$campaign_root"',
            source,
        )
        self.assertIn(
            'stat -c "%U:%G" "$campaign_root"',
            source,
        )
        self.assertIn("sudo -iu ec2-user env PATH=", source)
        self.assertIn("remain-on-exit on", source)
        self.assertIn("tmux pipe-pane", source)
        self.assertIn("tmux send-keys", source)
        self.assertIn('-l -- "$campaign_command"', source)
        self.assertIn('send-keys -t "$session" Enter', source)
        self.assertIn("printf -v campaign_command", source)
        self.assertIn("#{pane_dead}", source)
        self.assertIn("capture-pane", source)
        self.assertIn("bootstrap.log", source)
        self.assertIn("campaign.log", source)
        self.assertIn("for _ in $(seq 1 20)", source)
        self.assertIn("campaign pane exited during startup", source)
        self.assertIn(
            'sudo -iu ec2-user env PATH="$PATH" tmux list-sessions',
            source,
        )
        self.assertIn(
            'sudo -iu ec2-user env PATH="$PATH" tmux has-session',
            source,
        )
        self.assertNotIn(
            '\n  "tmux new-session -d',
            source,
        )
        self.assertLess(
            source.index("sudo shutdown -c"),
            source.index("tmux new-session -d"),
        )
        self.assertLess(
            source.index("loginctl enable-linger ec2-user"),
            source.index("tmux new-session -d"),
        )
        self.assertLess(
            source.index("sudo chown -R ec2-user:ec2-user"),
            source.index("tmux new-session -d"),
        )
        self.assertLess(
            source.index(
                "sudo install -d -o ec2-user -g ec2-user "
                "/home/ec2-user/borsuk-vortex-segment-replay"
            ),
            source.index("tmux new-session -d"),
        )
        self.assertLess(
            source.index('sudo -iu ec2-user test -w "$campaign_root"'),
            source.index("tmux new-session -d"),
        )
        self.assertLess(
            source.index("tmux new-session -d"),
            source.index("remain-on-exit on"),
        )
        self.assertLess(
            source.index("remain-on-exit on"),
            source.index("tmux pipe-pane"),
        )
        self.assertLess(
            source.index("tmux pipe-pane"),
            source.index("tmux send-keys"),
        )
        session_start = next(
            line for line in source.splitlines() if "tmux new-session -d" in line
        )
        self.assertNotIn("bench_vortex_segment_replay_aws.sh", session_start)
        self.assertIn("command -v tmux", source)
        self.assertIn("sudo dnf install -y tmux", source)
        self.assertIn("sudo yum install -y tmux", source)
        self.assertIn("tmux -V", source)
        self.assertIn("BORSUK_TMUX_VERSION", source)
        self.assertIn("BORSUK_TMUX_PROVISIONING", source)
        self.assertIn("tmux is unavailable and neither dnf nor yum", source)
        self.assertIn("python3 -m pip --version", source)
        self.assertIn("sudo dnf install -y python3-pip", source)
        self.assertIn("sudo yum install -y python3-pip", source)
        self.assertIn(
            "sudo python3 -m pip install --no-cache-dir uv==0.11.28",
            source,
        )
        self.assertIn('export PATH="/usr/local/bin:$PATH"', source)
        self.assertIn("command -v uv", source)
        self.assertIn("uv --version", source)
        self.assertIn("BORSUK_UV_VERSION", source)
        self.assertIn("BORSUK_UV_PROVISIONING", source)
        self.assertLess(
            source.index("python3 -m pip --version"),
            source.index("tmux new-session -d"),
        )
        self.assertLess(
            source.index("command -v tmux"),
            source.index("tmux list-sessions"),
        )
        self.assertIn("bench_vortex_segment_replay_aws.sh", source)
        self.assertIn("ec2 wait instance-status-ok", source)
        self.assertIn("ssm wait command-executed", source)
        self.assertIn("BORSUK_VORTEX_LAUNCHED_INSTANCE", source)
        self.assertIn("BORSUK_VORTEX_SHUTDOWN", source)
        campaign = (
            Path(__file__).resolve().parent / "bench_vortex_segment_replay_aws.sh"
        ).read_text()
        self.assertIn('if [[ "$LAUNCHED_INSTANCE" == "1"', campaign)
        self.assertIn("sha256sum /tmp/borsuk-vortex-replay-source.tar.gz", source)
        self.assertIn('if [[ "$invocation_status" != "Success"', source)
        self.assertIn('if [[ "$LAUNCHED_INSTANCE" == "1"', source)
        self.assertIn('if [[ "$source_bucket" != "$BUCKET"', source)
        self.assertIn("COPYFILE_DISABLE=1 tar --no-xattrs", source)
        self.assertIn("tmux_version=${BORSUK_TMUX_VERSION:-unknown}", campaign)
        self.assertIn(
            "tmux_provisioning=${BORSUK_TMUX_PROVISIONING:-unknown}",
            campaign,
        )
        self.assertIn("uv_version=${BORSUK_UV_VERSION:-unknown}", campaign)
        self.assertIn(
            "uv_provisioning=${BORSUK_UV_PROVISIONING:-unknown}",
            campaign,
        )
        self.assertIn("sudo shutdown -h now", campaign)
        self.assertNotIn("shutdown -h +1", campaign)
        self.assertNotIn("bench_publication_aws.sh", source)


if __name__ == "__main__":
    unittest.main()
