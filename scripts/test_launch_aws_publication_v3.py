import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.publication_v3_protocol import canonical_json_bytes, validate_manifest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "launch_aws_publication_v3.sh"


def run(command: list[str], cwd: Path) -> None:
    completed = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    if completed.returncode != 0:
        raise AssertionError(completed.stderr or completed.stdout)


def make_clean_repository(root: Path) -> None:
    copied = (
        "Cargo.toml",
        "Cargo.lock",
        "crates/borsuk/Cargo.toml",
        "python/uv.lock",
        "packages/borsuk/package-lock.json",
        "scripts/publication_v3_protocol.py",
        "scripts/publication_v3_aws.py",
        "scripts/validate_publication_v3_results.py",
        "scripts/launch_aws_publication_v3.sh",
        "docs/research/publication-v3-manifest.json",
    )
    for relative in copied:
        source = ROOT / relative
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    run(["git", "init", "-b", "main"], root)
    run(["git", "config", "user.email", "publication-v3@example.invalid"], root)
    run(["git", "config", "user.name", "Publication V3 Test"], root)
    run(["git", "add", "."], root)
    run(["git", "commit", "-m", "fixture"], root)
    bare = root.parent / "origin.git"
    run(["git", "init", "--bare", str(bare)], root.parent)
    run(["git", "remote", "add", "origin", str(bare)], root)
    run(["git", "push", "-u", "origin", "main"], root)


class LaunchAwsPublicationV3Tests(unittest.TestCase):
    def test_dry_run_is_deterministic_and_never_calls_aws(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            fake_bin = temp / "bin"
            fake_bin.mkdir()
            fake_aws = fake_bin / "aws"
            fake_aws.write_text("#!/bin/sh\necho AWS_WAS_CALLED >&2\nexit 97\n")
            fake_aws.chmod(0o755)
            environment = {**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"}
            reports = []
            for _ in range(2):
                completed = subprocess.run(
                    ["bash", "scripts/launch_aws_publication_v3.sh", "--dry-run"],
                    cwd=repository,
                    env=environment,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertNotIn("AWS_WAS_CALLED", completed.stderr)
                reports.append(json.loads(completed.stdout))
            self.assertEqual(reports[0], reports[1])
            self.assertFalse(reports[0]["paid_ready"])
            self.assertEqual(reports[0]["unstaged_datasets"], 11)
            self.assertEqual(reports[0]["staging_jobs"], 11)
            self.assertRegex(reports[0]["staging_plan_sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(reports[0]["staging_plan"]["job_count"], 11)
            self.assertNotIn(
                "sift-128",
                {job["dataset_id"] for job in reports[0]["staging_plan"]["jobs"]},
            )
            self.assertEqual(
                reports[0]["staging_plan_sha256"],
                hashlib.sha256(
                    canonical_json_bytes(reports[0]["staging_plan"])
                ).hexdigest(),
            )
            self.assertEqual(
                reports[0]["staging_plan"]["manifest_sha256"],
                reports[0]["manifest_sha256"],
            )
            self.assertEqual(
                reports[0]["structural_replay"], "blocked-until-paid-ready"
            )
            materialized = json.loads(
                (repository / "docs/research/publication-v3-manifest.json").read_text()
            )
            materialized["source"] = {
                "state": "frozen",
                "git_commit": reports[0]["source_commit"],
                "archive_sha256": reports[0]["source_archive_sha256"],
                "cargo_lock_sha256": reports[0]["cargo_lock_sha256"],
                "python_lock_sha256": reports[0]["python_lock_sha256"],
                "node_lock_sha256": reports[0]["node_lock_sha256"],
            }
            expected_manifest_sha256 = hashlib.sha256(
                canonical_json_bytes(validate_manifest(materialized))
            ).hexdigest()
            self.assertEqual(reports[0]["manifest_sha256"], expected_manifest_sha256)

    def test_dry_run_rejects_any_untracked_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory) / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            (repository / "untracked.txt").write_text("not frozen\n")
            completed = subprocess.run(
                ["bash", "scripts/launch_aws_publication_v3.sh", "--dry-run"],
                cwd=repository,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("clean worktree", completed.stderr)

    def test_paid_preflight_rejects_unpushed_source_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory) / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            (repository / "Cargo.toml").write_text("# local-only commit\n")
            run(["git", "add", "Cargo.toml"], repository)
            run(["git", "commit", "-m", "local only"], repository)
            completed = subprocess.run(
                ["bash", "scripts/launch_aws_publication_v3.sh", "--dry-run"],
                cwd=repository,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("delivered to origin/main", completed.stderr)

    def test_stage_sift_passes_frozen_archive_and_manifest_to_controller(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            fake = temp / "controller.py"
            fake.write_text(
                """#!/usr/bin/env python3
import hashlib, json, pathlib, sys
assert sys.argv[1:3] == ['stage', '--manifest']
manifest_path = pathlib.Path(sys.argv[3])
archive_path = pathlib.Path(sys.argv[sys.argv.index('--source-archive') + 1])
manifest = json.loads(manifest_path.read_text())
assert manifest['source']['state'] == 'frozen'
assert manifest['source']['archive_sha256'] == hashlib.sha256(archive_path.read_bytes()).hexdigest()
assert sys.argv[sys.argv.index('--dataset') + 1] == 'sift-128'
print(json.dumps({'dataset_id':'sift-128','attempt':4}, sort_keys=True))
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                ["bash", "scripts/launch_aws_publication_v3.sh", "--stage-sift"],
                cwd=repository,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(json.loads(completed.stdout)["dataset_id"], "sift-128")

    def test_build_sift_passes_frozen_archive_and_manifest_to_controller(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            fake = temp / "controller.py"
            fake.write_text(
                """#!/usr/bin/env python3
import hashlib, json, pathlib, sys
assert sys.argv[1:3] == ['build-sift', '--manifest']
manifest_path = pathlib.Path(sys.argv[3])
archive_path = pathlib.Path(sys.argv[sys.argv.index('--source-archive') + 1])
manifest = json.loads(manifest_path.read_text())
assert manifest['source']['state'] == 'frozen'
assert manifest['source']['archive_sha256'] == hashlib.sha256(archive_path.read_bytes()).hexdigest()
assert sys.argv[sys.argv.index('--attempt') + 1] == '2'
print(json.dumps({'role':'build','attempt':1}, sort_keys=True))
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                ["bash", "scripts/launch_aws_publication_v3.sh", "--build-sift"],
                cwd=repository,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                    "BORSUK_PUBLICATION_V3_BUILD_ATTEMPT": "2",
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(json.loads(completed.stdout)["role"], "build")

    def test_read_recall_sift_passes_frozen_archive_and_manifest_to_controller(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            fake = temp / "controller.py"
            fake.write_text(
                """#!/usr/bin/env python3
import hashlib, json, pathlib, sys
assert sys.argv[1:3] == ['read-recall-sift', '--manifest']
manifest_path = pathlib.Path(sys.argv[3])
archive_path = pathlib.Path(sys.argv[sys.argv.index('--source-archive') + 1])
manifest = json.loads(manifest_path.read_text())
assert manifest['source']['state'] == 'frozen'
assert manifest['source']['archive_sha256'] == hashlib.sha256(archive_path.read_bytes()).hexdigest()
for name in ('--image-id', '--subnet-id', '--security-group-id', '--instance-profile-arn', '--attempt', '--build-attempt'):
    assert name in sys.argv
assert sys.argv[sys.argv.index('--purchase-option') + 1] == 'on-demand'
print(json.dumps({'role':'runtime','attempt':1}, sort_keys=True))
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                ["bash", "scripts/launch_aws_publication_v3.sh", "--read-recall-sift"],
                cwd=repository,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                    "BORSUK_PUBLICATION_V3_PURCHASE_OPTION": "on-demand",
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(json.loads(completed.stdout)["role"], "runtime")

    def test_concurrency_sift_uses_its_own_attempt_authority(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            fake = temp / "controller.py"
            fake.write_text(
                """#!/usr/bin/env python3
import json, sys
assert sys.argv[1] == 'read-concurrency-sift'
assert sys.argv[sys.argv.index('--attempt') + 1] == '17'
print(json.dumps({'role':'runtime','attempt':17}, sort_keys=True))
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                ["bash", "scripts/launch_aws_publication_v3.sh", "--read-concurrency-sift"],
                cwd=repository,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                    "BORSUK_PUBLICATION_V3_CONCURRENCY_ATTEMPT": "17",
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(json.loads(completed.stdout)["attempt"], 17)

    def test_build_and_read_can_replay_frozen_ancestor_but_not_unpushed_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            frozen = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=repository, text=True
            ).strip()
            (repository / "Cargo.toml").write_text("# later main\n")
            run(["git", "add", "Cargo.toml"], repository)
            run(["git", "commit", "-m", "later main"], repository)
            run(["git", "push", "origin", "main"], repository)
            run(["git", "checkout", "--detach", frozen], repository)
            fake = temp / "controller.py"
            fake.write_text("#!/usr/bin/env python3\nprint('{}')\n", encoding="utf-8")
            fake.chmod(0o755)
            environment = {
                **os.environ,
                "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
            }
            for mode in ("--build-sift", "--read-recall-sift", "--read-concurrency-sift"):
                replay = subprocess.run(
                    ["bash", "scripts/launch_aws_publication_v3.sh", mode],
                    cwd=repository,
                    env=environment,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(replay.returncode, 0, replay.stderr)

            (repository / "Cargo.toml").write_text("# unpushed\n")
            run(["git", "add", "Cargo.toml"], repository)
            run(["git", "commit", "-m", "unpushed"], repository)
            rejected = subprocess.run(
                ["bash", "scripts/launch_aws_publication_v3.sh", "--read-recall-sift"],
                cwd=repository,
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("contained in origin/main", rejected.stderr)


if __name__ == "__main__":
    unittest.main()
