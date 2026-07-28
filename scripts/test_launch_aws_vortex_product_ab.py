#!/usr/bin/env python3
"""Static safety checks for the detached product A/B launcher."""

from __future__ import annotations

import unittest
from pathlib import Path


class AwsVortexProductAbLauncherTest(unittest.TestCase):
    def test_launcher_is_content_addressed_fresh_detached_and_observable(self) -> None:
        source = (
            Path(__file__).resolve().parent / "launch_aws_vortex_product_ab.sh"
        ).read_text()

        self.assertIn("i-0e73bacb470807838", source)
        self.assertIn("causality", source)
        self.assertIn("eu-central-1", source)
        self.assertIn("453182569524", source)
        self.assertIn("git ls-files", source)
        self.assertIn("shasum -a 256", source)
        self.assertIn("sha256sum", source)
        self.assertIn('actual="${actual%% *}"', source)
        self.assertNotIn('awk "{print \\\\$1}"', source)
        self.assertIn("sts get-caller-identity", source)
        self.assertIn("AWS account mismatch", source)
        self.assertIn("s3api list-objects-v2", source)
        self.assertIn("refusing to overwrite", source)
        self.assertIn("loginctl enable-linger ec2-user", source)
        self.assertIn("tmux new-session -d", source)
        self.assertIn("remain-on-exit on", source)
        self.assertIn("tmux pipe-pane", source)
        self.assertIn("tmux send-keys", source)
        self.assertIn("capture-pane", source)
        self.assertIn("bench_vortex_product_ab_aws.sh", source)
        self.assertIn("BORSUK_VORTEX_PRODUCT_LAUNCHED_INSTANCE", source)
        self.assertIn("BORSUK_VORTEX_PRODUCT_SHUTDOWN", source)
        self.assertIn("ec2 wait instance-status-ok", source)
        self.assertIn("ssm wait command-executed", source)
        self.assertIn("VORTEX_PRODUCT_AB_COMPLETE", source)
        self.assertNotIn("bench_publication_aws.sh", source)


if __name__ == "__main__":
    unittest.main()
