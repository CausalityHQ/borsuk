import json
import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SMOKE = ROOT / "examples" / "seaweedfs" / "run-smoke.sh"


class SeaweedFsSmokeTests(unittest.TestCase):
    def test_compose_publishes_only_the_s3_endpoint(self) -> None:
        if shutil.which("docker") is None:
            self.skipTest("Docker Compose is required to inspect the rendered service")
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(ROOT / "examples/seaweedfs/compose.yaml"),
                "config",
                "--format",
                "json",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        rendered = json.loads(result.stdout)
        ports = rendered["services"]["seaweedfs"]["ports"]
        self.assertEqual([port["published"] for port in ports], ["8333"])

    def run_smoke_with_fake_commands(
        self, aws_ready: bool
    ) -> tuple[subprocess.CompletedProcess[str], list[str]]:
        with tempfile.TemporaryDirectory() as temporary:
            temporary_path = Path(temporary)
            command_log = temporary_path / "commands.log"
            fake = temporary_path / "fake-command"
            fake.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    name="$(basename "$0")"
                    printf '%s %s\n' "$name" "$*" >>"$BORSUK_SMOKE_COMMAND_LOG"
                    if [[ "$name" == "aws" && "${BORSUK_SMOKE_AWS_READY:-0}" != "1" ]]; then
                      exit 1
                    fi
                    exit 0
                    """
                ),
                encoding="utf-8",
            )
            fake.chmod(0o755)
            for name in ("aws", "cargo", "docker", "npm", "sleep", "uv"):
                (temporary_path / name).symlink_to(fake)

            result = subprocess.run(
                ["bash", str(SMOKE)],
                cwd=ROOT,
                env={
                    **os.environ,
                    "BORSUK_SMOKE_COMMAND_LOG": str(command_log),
                    "BORSUK_SMOKE_AWS_READY": "1" if aws_ready else "0",
                    "PATH": f"{temporary_path}:{os.environ['PATH']}",
                },
                capture_output=True,
                text=True,
                check=False,
            )

            return result, command_log.read_text(encoding="utf-8").splitlines()

    def test_unready_endpoint_stops_before_language_gates(self) -> None:
        result, commands = self.run_smoke_with_fake_commands(aws_ready=False)
        joined = "\n".join(commands)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("SeaweedFS did not become ready", result.stderr)
        self.assertNotIn(
            "cargo test --locked -p borsuk --test s3_compatible -- --nocapture", joined
        )
        self.assertNotIn(
            "cargo test --locked -p borsuk --test s3_soak -- --nocapture", joined
        )
        self.assertNotIn("cargo run --locked -p borsuk --example s3_index", joined)
        self.assertNotIn("npm ", joined)
        self.assertNotIn("uv ", joined)

    def test_rust_targets_build_before_starting_seaweedfs(self) -> None:
        _, commands = self.run_smoke_with_fake_commands(aws_ready=True)
        compose_up = commands.index(
            "docker compose -p borsuk-seaweedfs -f "
            + str(ROOT / "examples/seaweedfs/compose.yaml")
            + " up -d"
        )
        expected_builds = (
            "cargo test --locked -p borsuk --test s3_compatible --no-run",
            "cargo test --locked -p borsuk --test s3_soak --no-run",
            "cargo build --locked -p borsuk --example s3_index",
        )
        for command in expected_builds:
            self.assertIn(command, commands)
            self.assertLess(commands.index(command), compose_up)


if __name__ == "__main__":
    unittest.main()
