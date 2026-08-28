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


def make_clean_repository(
    root: Path, *, unstaged_dataset_id: str | None = None
) -> None:
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
    if unstaged_dataset_id is not None:
        manifest_path = root / "docs/research/publication-v3-manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        dataset = next(
            item for item in manifest["datasets"] if item["id"] == unstaged_dataset_id
        )
        dataset["source"] = {
            "state": "unstaged",
            "expected_source": "s3://assets.zilliz.com/benchmark/cohere_large_10m",
            "license": dataset["source"]["license"],
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
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
    def test_v23_launcher_forwards_stage_and_historical_build_authority(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            fake = temp / "controller.py"
            fake.write_text(
                """#!/usr/bin/env python3
import json, sys
assert sys.argv[1] == 'diagnose-v23'
assert sys.argv[sys.argv.index('--stage') + 1] == 'd2'
assert sys.argv[sys.argv.index('--base-build-terminal-uri') + 1] == 's3://bucket/results/base/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json'
assert sys.argv[sys.argv.index('--base-build-terminal-sha256') + 1] == 'c' * 64
assert sys.argv[sys.argv.index('--attempt') + 1] == '0'
assert sys.argv[sys.argv.index('--purchase-option') + 1] == 'spot'
print(json.dumps({'operation': sys.argv[1], 'stage': 'd2'}, sort_keys=True))
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--diagnose-v23",
                    "d2",
                    "s3://bucket/results/base/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json",
                    "c" * 64,
                ],
                cwd=repository,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(json.loads(completed.stdout)["stage"], "d2")

    def test_v22_launcher_forwards_explicit_historical_terminal_authority(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            fake = temp / "controller.py"
            fake.write_text(
                """#!/usr/bin/env python3
import json, sys
assert sys.argv[1] == 'diagnose-v22-stage-l'
assert sys.argv[sys.argv.index('--base-build-terminal-uri') + 1] == 's3://bucket/results/base/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json'
assert sys.argv[sys.argv.index('--base-build-terminal-sha256') + 1] == 'b' * 64
assert sys.argv[sys.argv.index('--attempt') + 1] == '0'
assert sys.argv[sys.argv.index('--arm-index') + 1] == '0'
print(json.dumps({'operation': sys.argv[1]}, sort_keys=True))
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--diagnose-v22-stage-l",
                    "s3://bucket/results/base/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json",
                    "b" * 64,
                ],
                cwd=repository,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(
                json.loads(completed.stdout)["operation"], "diagnose-v22-stage-l"
            )

    def test_v21_launcher_forwards_explicit_historical_terminal_authority(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            fake = temp / "controller.py"
            fake.write_text(
                """#!/usr/bin/env python3
import json, sys
assert sys.argv[1] == 'diagnose-v21-selector'
assert sys.argv[sys.argv.index('--base-build-terminal-uri') + 1] == 's3://bucket/results/base/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json'
assert sys.argv[sys.argv.index('--base-build-terminal-sha256') + 1] == 'a' * 64
assert sys.argv[sys.argv.index('--attempt') + 1] == '0'
assert sys.argv[sys.argv.index('--max-attempts') + 1] == '6'
assert sys.argv[sys.argv.index('--arm-index') + 1] == '0'
assert '--build-attempt' not in sys.argv
print(json.dumps({'operation': sys.argv[1]}, sort_keys=True))
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--diagnose-v21-selector",
                    "s3://bucket/results/base/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json",
                    "a" * 64,
                ],
                cwd=repository,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                    "BORSUK_PUBLICATION_V3_RUNTIME_ATTEMPT": "9",
                    "BORSUK_PUBLICATION_V3_MAX_ATTEMPTS": "9",
                    "BORSUK_PUBLICATION_V3_ARM_INDEX": "9",
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(
                json.loads(completed.stdout)["operation"],
                "diagnose-v21-selector",
            )

    def test_generic_read_commands_forward_exact_frozen_cell_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            fake = temp / "controller.py"
            fake.write_text(
                """#!/usr/bin/env python3
import json, sys
operation = sys.argv[1]
assert sys.argv[sys.argv.index('--workload') + 1] == 'realistic-dense-read'
assert sys.argv[sys.argv.index('--dataset') + 1] == 'laion-100m-768'
if operation == 'run-read':
    assert sys.argv[sys.argv.index('--repetition') + 1] == 'r05'
    assert sys.argv[sys.argv.index('--arm-index') + 1] == '2'
    assert sys.argv[sys.argv.index('--attempt') + 1] == '7'
    assert sys.argv[sys.argv.index('--build-attempt') + 1] == '3'
if operation == 'diagnose-read':
    assert sys.argv[sys.argv.index('--repetition') + 1] == 'r01'
    assert sys.argv[sys.argv.index('--nprobes') + 1] == '32,64'
    assert sys.argv[sys.argv.index('--candidates') + 1] == '512,1024,2048,4096'
    assert sys.argv[sys.argv.index('--attempt') + 1] == '7'
    assert sys.argv[sys.argv.index('--build-attempt') + 1] == '3'
print(json.dumps({'operation': operation}, sort_keys=True))
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            environment = {
                **os.environ,
                "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                "BORSUK_PUBLICATION_V3_BUILD_ATTEMPT": "3",
                "BORSUK_PUBLICATION_V3_RUNTIME_ATTEMPT": "7",
            }
            build = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--build-read",
                    "realistic-dense-read",
                    "laion-100m-768",
                ],
                cwd=repository,
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertEqual(build.returncode, 0, build.stderr)
            self.assertEqual(json.loads(build.stdout)["operation"], "build-read")
            runtime = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--run-read",
                    "realistic-dense-read",
                    "laion-100m-768",
                    "r05",
                    "2",
                ],
                cwd=repository,
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertEqual(runtime.returncode, 0, runtime.stderr)
            self.assertEqual(json.loads(runtime.stdout)["operation"], "run-read")
            diagnostic = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--diagnose-read",
                    "realistic-dense-read",
                    "laion-100m-768",
                ],
                cwd=repository,
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertEqual(diagnostic.returncode, 0, diagnostic.stderr)
            self.assertEqual(
                json.loads(diagnostic.stdout)["operation"], "diagnose-read"
            )

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
            self.assertTrue(reports[0]["paid_ready"])
            self.assertEqual(reports[0]["unstaged_datasets"], 0)
            self.assertEqual(reports[0]["staging_jobs"], 0)
            self.assertRegex(reports[0]["staging_plan_sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(reports[0]["staging_plan"]["job_count"], 0)
            self.assertEqual(len(reports[0]["staging_plan"]["jobs"]), 0)
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
            self.assertEqual(reports[0]["structural_replay"], "structurally-valid")
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

    def test_stage_dataset_passes_requested_id_and_frozen_inputs_to_controller(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(
                repository, unstaged_dataset_id="cohere-large-10m-768"
            )
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
assert sys.argv[sys.argv.index('--dataset') + 1] == 'cohere-large-10m-768'
assert sys.argv[sys.argv.index('--start-attempt') + 1] == '5'
print(json.dumps({'dataset_id':'cohere-large-10m-768','attempt':1}, sort_keys=True))
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--stage-dataset",
                    "cohere-large-10m-768",
                ],
                cwd=repository,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                    "BORSUK_PUBLICATION_V3_START_ATTEMPT": "5",
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(
                json.loads(completed.stdout)["dataset_id"], "cohere-large-10m-768"
            )

    def test_stage_dataset_rejects_unknown_id_before_controller(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            fake = temp / "controller.py"
            fake.write_text(
                "#!/usr/bin/env python3\nimport sys\n"
                "print('CONTROLLER_WAS_CALLED', file=sys.stderr)\n",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--stage-dataset",
                    "cohere-large-10m-786",
                ],
                cwd=repository,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 2)
            self.assertIn("not an unstaged manifest dataset", completed.stderr)
            self.assertNotIn("CONTROLLER_WAS_CALLED", completed.stderr)

    def test_lifecycle_build_and_runtime_forward_dataset_arm_and_frozen_inputs(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            updater = temp / "updater"
            run(
                [
                    "git",
                    "clone",
                    "--branch",
                    "main",
                    str(temp / "origin.git"),
                    str(updater),
                ],
                temp,
            )
            run(
                ["git", "config", "user.email", "publication-v3@example.invalid"],
                updater,
            )
            run(["git", "config", "user.name", "Publication V3 Test"], updater)
            (updater / "later-main-change").write_text("later\n", encoding="utf-8")
            run(["git", "add", "later-main-change"], updater)
            run(["git", "commit", "-m", "advance origin"], updater)
            run(["git", "push", "origin", "main"], updater)
            fake = temp / "controller.py"
            fake.write_text(
                """#!/usr/bin/env python3
import hashlib, json, pathlib, sys
operation = sys.argv[1]
manifest_path = pathlib.Path(sys.argv[sys.argv.index('--manifest') + 1])
archive_path = pathlib.Path(sys.argv[sys.argv.index('--source-archive') + 1])
manifest = json.loads(manifest_path.read_text())
assert manifest['source']['state'] == 'frozen'
assert manifest['source']['archive_sha256'] == hashlib.sha256(archive_path.read_bytes()).hexdigest()
assert sys.argv[sys.argv.index('--dataset') + 1] == 'sift-128'
if operation == 'run-lifecycle':
    assert sys.argv[sys.argv.index('--arm-index') + 1] == '4'
    assert sys.argv[sys.argv.index('--build-attempt') + 1] == '2'
if operation == 'diagnose-lifecycle':
    assert '--arm-index' not in sys.argv
    assert sys.argv[sys.argv.index('--write-ops') + 1] == '2560'
    assert sys.argv[sys.argv.index('--timeout-seconds') + 1] == '1200'
    assert sys.argv[sys.argv.index('--build-attempt') + 1] == '2'
print(json.dumps({'operation': operation}, sort_keys=True))
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            environment = {
                **os.environ,
                "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                "BORSUK_PUBLICATION_V3_BUILD_ATTEMPT": "2",
                "BORSUK_PUBLICATION_V3_RUNTIME_ATTEMPT": "3",
            }
            build = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--build-lifecycle",
                    "sift-128",
                ],
                cwd=repository,
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertEqual(build.returncode, 0, build.stderr)
            self.assertEqual(json.loads(build.stdout)["operation"], "build-lifecycle")
            runtime = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--run-lifecycle",
                    "sift-128",
                    "4",
                ],
                cwd=repository,
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertEqual(runtime.returncode, 0, runtime.stderr)
            self.assertEqual(json.loads(runtime.stdout)["operation"], "run-lifecycle")
            diagnostic = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--diagnose-lifecycle",
                    "sift-128",
                ],
                cwd=repository,
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertEqual(diagnostic.returncode, 0, diagnostic.stderr)
            self.assertEqual(
                json.loads(diagnostic.stdout)["operation"], "diagnose-lifecycle"
            )

    def test_stage_dataset_requires_one_nonempty_id(self) -> None:
        for arguments in (["--stage-dataset"], ["--stage-dataset", ""]):
            with self.subTest(arguments=arguments):
                completed = subprocess.run(
                    ["bash", str(SCRIPT), *arguments],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(completed.returncode, 2)
                self.assertIn("usage:", completed.stderr)

    def test_stage_dataset_rejects_undelivered_commit_before_controller(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repository = temp / "repository"
            repository.mkdir()
            make_clean_repository(repository)
            (repository / "Cargo.toml").write_text("# local-only commit\n")
            run(["git", "add", "Cargo.toml"], repository)
            run(["git", "commit", "-m", "local only"], repository)
            fake = temp / "controller.py"
            fake.write_text(
                "#!/usr/bin/env python3\nimport sys\n"
                "print('CONTROLLER_WAS_CALLED', file=sys.stderr)\n",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--stage-dataset",
                    "cohere-large-10m-768",
                ],
                cwd=repository,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_CONTROLLER": str(fake),
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 2)
            self.assertIn("delivered to origin/main", completed.stderr)
            self.assertNotIn("CONTROLLER_WAS_CALLED", completed.stderr)

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

    def test_read_recall_sift_passes_frozen_archive_and_manifest_to_controller(
        self,
    ) -> None:
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
                [
                    "bash",
                    "scripts/launch_aws_publication_v3.sh",
                    "--read-concurrency-sift",
                ],
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

    def test_build_and_read_can_replay_frozen_ancestor_but_not_unpushed_commit(
        self,
    ) -> None:
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
            for mode in (
                "--build-sift",
                "--read-recall-sift",
                "--read-concurrency-sift",
            ):
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
