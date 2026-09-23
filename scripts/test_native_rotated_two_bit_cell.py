"""Two-bit source boundary, replay, immutable Spot attempt and worker checks."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import scripts.test_native_rotated_two_bit_evaluation as evaluation_tests
from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS,
    FROZEN_QUERIES,
    FROZEN_SOURCE,
    FROZEN_TRUTH,
    SourceArchiveIdentity,
)
from scripts.launch_native_rotated_two_bit_spot import (
    PRIOR_EVIDENCE,
    PRIOR_TREE,
    _validate_terminal_bytes,
    build_launch_specs,
    build_plan,
    launch_and_monitor,
    worker_script,
)
from scripts.native_geometric_layout_screen import ArtifactIdentity, EvaluationLimits
from scripts.native_page_microcluster_cell import FROZEN_INPUTS
from scripts.native_rotated_two_bit_cell import (
    PRIOR_EVIDENCE as CELL_EVIDENCE,
)
from scripts.native_rotated_two_bit_cell import (
    construct_cell,
    run_construct_phase,
    run_evaluate_phase,
    run_validate_phase,
)
from scripts.native_rotated_two_bit_evaluation import exact_scores
from scripts.native_row_score_nomination import nominate_pages
from scripts.test_native_page_centered_group_evaluation import CountingReader


class TwoBitCellTests(unittest.TestCase):
    @staticmethod
    def fixture():
        ids, vectors, membership, _ = evaluation_tests.TwoBitEvaluationTests.fixture()
        source_sha = hashlib.sha256(b"source").digest()
        inputs = dataclasses.replace(
            FROZEN_INPUTS,
            layout=dataclasses.replace(
                FROZEN_INPUTS.layout,
                source=ArtifactIdentity("source", "file:///source", source_sha.hex(), 1),
            ),
            membership=ArtifactIdentity(
                "geometric-membership", "file:///membership",
                hashlib.sha256(b"membership").hexdigest(), 1,
            ),
            limits=EvaluationLimits(5, 5000),
        )
        sources = tuple(range(96, 120)) + tuple(range(96))
        pages = (4,) * 24 + tuple(index // 24 for index in range(96))
        exact_pages = nominate_pages(
            exact_scores(vectors[0], vectors, sources), sources, pages,
            (1000,) * 5, EvaluationLimits(5, 5000),
        )
        exact_set = set(exact_pages)
        exact10 = sum(index // 24 in exact_set for index in range(10))
        exact100 = sum(index // 24 in exact_set for index in range(100))
        prior = SimpleNamespace(
            query_ordinal=0,
            retained_pages=(4, 0),
            grouped_pages=(4, 0, 1, 2, 3),
            exact_pages=exact_pages,
            exact_hits_at_10=exact10,
            exact_hits_at_100=exact100,
            prior_pq_hits_at_100=80,
            prior_residual_hits_at_100=81,
        )
        return ids, vectors, membership, inputs, prior

    def test_construct_is_source_only_and_sealed(self) -> None:
        ids, vectors, membership, inputs, _ = self.fixture()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("scripts.native_rotated_two_bit_cell._read_inputs", return_value=(ids, vectors, membership)):
                run_construct_phase(root, "s3://bucket/run", inputs)
            self.assertTrue((root / "sealed.json").exists())
            self.assertTrue((root / "mean.bin").exists())
            self.assertTrue((root / "groups.bin").exists())
            self.assertFalse((root / "queries.parquet").exists())
            self.assertFalse((root / "truth.parquet").exists())
            with self.assertRaisesRegex(ValueError, "source"):
                construct_cell(
                    root, "s3://bucket/run", ids, vectors, membership,
                    hashlib.sha256(b"other").digest(),
                    bytes.fromhex(inputs.membership.sha256),
                    bytes.fromhex(PRIOR_TREE.sha256),
                    layout_seed=20260921,
                )

    def test_evaluate_and_validate_replay_closed_exact_arm(self) -> None:
        ids, vectors, membership, inputs, prior = self.fixture()
        self.assertEqual(PRIOR_EVIDENCE.sha256, CELL_EVIDENCE.sha256)
        archive = SourceArchiveIdentity("s3://bucket/source.tar.gz", "ab" * 32, 1234)
        router = SimpleNamespace(
            pages=tuple(SimpleNamespace(encoded_page_bytes=1000) for _ in range(5))
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("scripts.native_rotated_two_bit_cell._read_inputs", return_value=(ids, vectors, membership)):
                run_construct_phase(root, "s3://bucket/run", inputs)
            with (
                patch("scripts.native_rotated_two_bit_cell._read_inputs", return_value=(ids, vectors, membership)),
                patch("scripts.native_rotated_two_bit_cell._read_queries_truth", return_value=(vectors[:1], (ids[:100],))),
                patch("scripts.native_rotated_two_bit_cell.read_geometric_router_parquet", return_value=router),
                patch("scripts.native_rotated_two_bit_cell.route_geometric_query", return_value=SimpleNamespace(retained_leaf_pages=(4, 0))),
                patch("scripts.native_rotated_two_bit_cell._load_prior_samples", return_value=(prior,)),
            ):
                result = run_evaluate_phase(
                    root, root / "evaluation", "s3://bucket/run", inputs,
                    code_reader=CountingReader((root / "groups.bin").read_bytes()),
                    source_archive=archive, requirements_sha256="cd" * 32,
                )
            self.assertEqual(result["metrics"]["query_count"], 1)
            self.assertEqual(result["prior_evidence"]["sha256"], PRIOR_EVIDENCE.sha256)
            with (
                patch("scripts.native_rotated_two_bit_cell._read_inputs", return_value=(ids, vectors, membership)),
                patch("scripts.native_rotated_two_bit_cell._read_queries_truth", return_value=(vectors[:1], (ids[:100],))),
                patch("scripts.native_rotated_two_bit_cell.read_geometric_router_parquet", return_value=router),
                patch("scripts.native_rotated_two_bit_cell._independent_route", return_value=((), (4, 0), 0, 0)),
                patch("scripts.native_rotated_two_bit_cell._load_prior_samples", return_value=(prior,)),
            ):
                validation = run_validate_phase(
                    root, root / "evaluation", "s3://bucket/run", "ab" * 20,
                    inputs, source_archive=archive, requirements_sha256="cd" * 32,
                )
                with self.assertRaisesRegex(ValueError, "result authority"):
                    run_validate_phase(
                        root, root / "evaluation", "s3://bucket/run", "ab" * 20,
                        inputs,
                        source_archive=dataclasses.replace(archive, sha256="ef" * 32),
                        requirements_sha256="cd" * 32,
                    )
            self.assertEqual(validation["metrics"], result["metrics"])


class TwoBitSpotTests(unittest.TestCase):
    @staticmethod
    def plan():
        return build_plan(
            profile="causality", source_commit="12" * 20,
            source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", "34" * 32, 1234),
            source=FROZEN_SOURCE, queries=FROZEN_QUERIES, truth=FROZEN_TRUTH,
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-rotated-two-bit/"
                + "12" * 20 + "/runs/relaion-100k-dev1000-a0001"
            ),
            attempt=1, image_id="ami-06121aa3085b6f918",
            security_group_id="sg-0b1fd3e4fbde4af0d",
            instance_profile_arn="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
            targets=DEFAULT_TARGETS,
        )

    def test_worker_source_boundary_and_terminal_roster(self) -> None:
        script = worker_script(self.plan())
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertNotRegex(script, r"@[A-Z_]+@")
        before_evaluation = script[: script.index("phase=evaluate")]
        self.assertIn("unshare --net", before_evaluation)
        self.assertNotIn(FROZEN_QUERIES.uri, before_evaluation)
        self.assertNotIn(FROZEN_TRUTH.uri, before_evaluation)
        self.assertIn("mean.bin", before_evaluation)
        self.assertIn(PRIOR_EVIDENCE.uri, script[script.index("phase=evaluate") :])
        self.assertIn('env PYTHONPATH="$root/repo"', script[script.index("phase=validate") :])
        terminal_body = script[script.index("terminal() {") : script.index("trap terminal EXIT")]
        terminal_python = terminal_body.split("python3 - <<'PY'\n", 1)[1].split("\nPY", 1)[0]
        compile(terminal_python, "<two-bit terminal>", "exec")
        specs = build_launch_specs(self.plan())
        self.assertEqual(len(specs), len(DEFAULT_TARGETS))
        self.assertTrue(all(spec["InstanceMarketOptions"]["MarketType"] == "spot" for spec in specs))
        plan = self.plan()
        terminal = {
            "schema": "borsuk-rotated-two-bit-terminal-v1", "artifacts": {},
            "attempt": 1, "claim_eligible": False, "elapsed_seconds": 12,
            "exit_code": 0, "instance_id": "i-0123456789abcdef0", "phase": "complete",
            "source_commit": plan.source_commit, "source_archive": dataclasses.asdict(plan.source_archive),
            "requirements_sha256": plan.requirements_sha256, "status": "complete",
        }
        body = (json.dumps(terminal, sort_keys=True, separators=(",", ":")) + "\n").encode()
        with self.assertRaisesRegex(ValueError, "artifact roster"):
            _validate_terminal_bytes(body, plan, "i-0123456789abcdef0")
        failed = dict(terminal, status="failed", phase="evaluate", exit_code=143)
        failed_body = (json.dumps(failed, sort_keys=True, separators=(",", ":")) + "\n").encode()
        self.assertEqual(
            _validate_terminal_bytes(failed_body, plan, "i-0123456789abcdef0"),
            failed,
        )

    def test_artifacts_only_prefix_collision(self) -> None:
        class S3:
            def list_objects_v2(self, **kwargs):
                return {"KeyCount": 1, "Contents": [{"Key": kwargs["Prefix"] + "artifacts/mean.bin"}]}

            def put_object(self, **kwargs):
                raise AssertionError("existing attempt must not be reserved")

        session = SimpleNamespace(client=lambda name: S3() if name == "s3" else object())
        with patch.dict(sys.modules, {"boto3": SimpleNamespace(Session=lambda **kwargs: session)}):
            with self.assertRaisesRegex(ValueError, "immutable attempt already exists"):
                launch_and_monitor(self.plan())


if __name__ == "__main__":
    unittest.main()
