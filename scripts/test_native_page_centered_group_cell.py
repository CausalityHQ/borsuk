"""Phase boundary and immutable attempt checks for grouped code cell."""

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

import numpy as np

from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS,
    FROZEN_QUERIES,
    FROZEN_SOURCE,
    FROZEN_TRUTH,
    SourceArchiveIdentity,
)
from scripts.launch_native_page_centered_group_spot import (
    PRIOR_PAGES,
    PRIOR_TREE,
    _validate_terminal_bytes,
    build_launch_specs,
    build_plan,
    worker_script,
)
from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    LayoutMethod,
    MembershipRow,
)
from scripts.native_page_centered_group_cell import (
    PRIOR_PAGES as CELL_PAGES,
)
from scripts.native_page_centered_group_cell import (
    PRIOR_TREE as CELL_TREE,
)
from scripts.native_page_centered_group_cell import (
    construct_cell,
    read_cell_seal,
    run_construct_phase,
    run_evaluate_phase,
    run_validate_phase,
)
from scripts.native_page_centered_group_codes import read_group_codes
from scripts.native_page_microcluster_cell import FROZEN_INPUTS
from scripts.test_native_page_centered_group_evaluation import CountingReader


class GroupCellTests(unittest.TestCase):
    @staticmethod
    def fixture():
        ids = tuple(index.to_bytes(4, "little") for index in range(320))
        vectors = np.random.default_rng(14).normal(size=(320, 48)).astype(np.float32)
        source_sha = hashlib.sha256(b"source").digest()
        membership = tuple(
            MembershipRow(
                stable_id=ids[index],
                source_ordinal=index,
                page_ordinal=index // 64,
                in_page_ordinal=index % 64,
                page_rows=64,
                encoded_page_bytes=1000,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=source_sha,
                seed=7,
                construction_sha256=hashlib.sha256(b"layout").digest(),
            )
            for index in range(320)
        )
        inputs = dataclasses.replace(
            FROZEN_INPUTS,
            layout=dataclasses.replace(
                FROZEN_INPUTS.layout,
                seed=7,
                dimensions=48,
                source=ArtifactIdentity("source", "file:///source", source_sha.hex(), 1),
            ),
            membership=ArtifactIdentity(
                "geometric-membership",
                "file:///membership",
                hashlib.sha256(b"membership").hexdigest(),
                1,
            ),
        )
        return ids, vectors, membership, source_sha, inputs

    def test_construct_seals_before_query_or_truth_access(self) -> None:
        ids, vectors, membership, source_sha, inputs = self.fixture()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch(
                "scripts.native_page_centered_group_cell._read_inputs",
                return_value=(ids, vectors, membership),
            ):
                run_construct_phase(root, "s3://bucket/run", inputs, iterations=1)
            self.assertTrue((root / "sealed.json").exists())
            self.assertTrue((root / "groups.bin").exists())
            self.assertFalse((root / "queries.parquet").exists())
            self.assertFalse((root / "truth.parquet").exists())
            value = json.loads((root / "sealed.json").read_bytes())
            self.assertEqual(value["source_sha256"], source_sha.hex())
            with self.assertRaisesRegex(ValueError, "source"):
                construct_cell(
                    root,
                    "s3://bucket/run",
                    ids,
                    vectors,
                    membership,
                    hashlib.sha256(b"other").digest(),
                    bytes.fromhex(inputs.membership.sha256),
                    bytes.fromhex(CELL_TREE.sha256),
                    seed=7,
                    iterations=1,
                )

    def test_cli_rejects_missing_evaluation_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "scripts.native_page_centered_group_cell",
                    "evaluate",
                    "--root",
                    temporary,
                    "--output-prefix",
                    "s3://bucket/run",
                    "--source-archive-uri",
                    "s3://bucket/source.tar.gz",
                    "--source-archive-sha256",
                    "ab" * 32,
                    "--source-archive-bytes",
                    "1234",
                    "--requirements-sha256",
                    "cd" * 32,
                ],
                capture_output=True,
                text=True,
                check=False,
            )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--out is required", result.stderr)

    def test_evaluation_and_validation_bind_the_same_grouped_rows(self) -> None:
        ids, vectors, membership, _, inputs = self.fixture()
        queries = vectors[:1]
        truth = (tuple(ids[index] for index in range(192, 292)),)
        archive = SourceArchiveIdentity("s3://bucket/source.tar.gz", "ab" * 32, 1234)
        router = SimpleNamespace(
            pages=tuple(SimpleNamespace(encoded_page_bytes=1000) for _ in range(5))
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch(
                "scripts.native_page_centered_group_cell._read_inputs",
                return_value=(ids, vectors, membership),
            ):
                run_construct_phase(root, "s3://bucket/run", inputs, iterations=1)
            with (
                patch(
                    "scripts.native_page_centered_group_cell._read_inputs",
                    return_value=(ids, vectors, membership),
                ),
                patch(
                    "scripts.native_page_centered_group_cell._read_queries_truth",
                    return_value=(queries, truth),
                ),
                patch(
                    "scripts.native_page_centered_group_cell.read_geometric_router_parquet",
                    return_value=router,
                ),
                patch(
                    "scripts.native_page_centered_group_cell.route_geometric_query",
                    return_value=SimpleNamespace(retained_leaf_pages=(4, 0)),
                ),
                patch(
                    "scripts.native_page_centered_group_cell._load_prior_hits",
                    return_value=((42, 43),),
                ),
            ):
                result = run_evaluate_phase(
                    root,
                    root / "evaluation",
                    "s3://bucket/run",
                    inputs,
                    code_reader=CountingReader((root / "groups.bin").read_bytes()),
                    source_archive=archive,
                    requirements_sha256="cd" * 32,
                )
            self.assertEqual(result["prior_evidence"]["sha256"], (
                "d91c726afb584826c24a3fd1c0d804cd8d71d95c4967cf16f3cdceb924c53f1f"
            ))
            self.assertEqual(result["metrics"]["query_count"], 1)
            with (
                patch(
                    "scripts.native_page_centered_group_cell._read_inputs",
                    return_value=(ids, vectors, membership),
                ),
                patch(
                    "scripts.native_page_centered_group_cell._read_queries_truth",
                    return_value=(queries, truth),
                ),
                patch(
                    "scripts.native_page_centered_group_cell.read_geometric_router_parquet",
                    return_value=router,
                ),
                patch(
                    "scripts.native_page_centered_group_cell._independent_route",
                    return_value=((), (4, 0), 0, 0),
                ),
                patch(
                    "scripts.native_page_centered_group_cell._load_prior_hits",
                    return_value=((42, 43),),
                ),
                patch(
                    "scripts.native_page_centered_group_cell.construct_group_codes"
                ) as rebuild,
            ):
                source_sha = bytes.fromhex(inputs.layout.source.sha256)
                member_sha = bytes.fromhex(inputs.membership.sha256)
                tree_sha = bytes.fromhex(CELL_TREE.sha256)
                identities = read_cell_seal(
                    root, "s3://bucket/run", source_sha, member_sha, tree_sha
                )
                rebuild.return_value = read_group_codes(
                    root,
                    identities,
                    ids,
                    membership,
                    source_sha,
                    member_sha,
                    tree_sha,
                    dimensions=48,
                    seed=7,
                )
                validation = run_validate_phase(
                    root,
                    root / "evaluation",
                    "s3://bucket/run",
                    "ab" * 20,
                    inputs,
                    source_archive=archive,
                    requirements_sha256="cd" * 32,
                )
                with self.assertRaisesRegex(ValueError, "result authority"):
                    run_validate_phase(
                        root,
                        root / "evaluation",
                        "s3://bucket/run",
                        "ab" * 20,
                        inputs,
                        source_archive=dataclasses.replace(archive, sha256="ef" * 32),
                        requirements_sha256="cd" * 32,
                    )
            self.assertEqual(validation["metrics"], result["metrics"])


class GroupSpotTests(unittest.TestCase):
    @staticmethod
    def plan():
        return build_plan(
            profile="causality",
            source_commit="12" * 20,
            source_archive=SourceArchiveIdentity(
                uri="s3://frozen/source.tar.gz", sha256="34" * 32, encoded_bytes=1234
            ),
            source=FROZEN_SOURCE,
            queries=FROZEN_QUERIES,
            truth=FROZEN_TRUTH,
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-page-centered-groups/"
                + "12" * 20
                + "/runs/relaion-100k-dev1000-a0001"
            ),
            image_id="ami-06121aa3085b6f918",
            security_group_id="sg-0b1fd3e4fbde4af0d",
            instance_profile_arn=(
                "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
            ),
            targets=DEFAULT_TARGETS,
        )

    def test_controller_import_needs_no_science_dependencies(self) -> None:
        result = subprocess.run(
            ["/usr/bin/python3", "-c", "import scripts.launch_native_page_centered_group_spot"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_worker_is_phase_separated_and_source_bound(self) -> None:
        for worker, cell in ((PRIOR_TREE, CELL_TREE), (PRIOR_PAGES, CELL_PAGES)):
            self.assertEqual(worker.sha256, cell.sha256)
        script = worker_script(self.plan())
        syntax = subprocess.run(
            ["bash", "-n"], input=script, text=True, capture_output=True, check=False
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        before_queries = script[: script.index("phase=evaluate")]
        self.assertIn("unshare --net", before_queries)
        self.assertIn("artifacts/groups.bin", before_queries)
        self.assertNotIn(FROZEN_QUERIES.uri, before_queries)
        self.assertNotIn(FROZEN_TRUTH.uri, before_queries)
        self.assertIn("prior-evidence.json", script[script.index("phase=evaluate") :])
        self.assertIn("scripts.native_page_centered_group_cell validate", script)
        self.assertIn('env PYTHONPATH="$root/repo"', script[script.index("phase=validate") :])
        terminal_body = script[script.index("terminal() {") : script.index("trap terminal EXIT")]
        terminal_python = terminal_body.split("python3 - <<'PY'\n", 1)[1].split("\nPY", 1)[0]
        compile(terminal_python, "<group Spot terminal>", "exec")
        self.assertIn("set +e", terminal_body)
        self.assertLess(len(script.encode()), 16_384)
        self.assertEqual(len(build_launch_specs(self.plan())), len(DEFAULT_TARGETS))

    def test_complete_terminal_requires_exact_roster(self) -> None:
        plan = self.plan()
        value = {
            "schema": "borsuk-page-centered-group-terminal-v1",
            "artifacts": {},
            "attempt": 1,
            "claim_eligible": False,
            "elapsed_seconds": 12,
            "exit_code": 0,
            "instance_id": "i-0123456789abcdef0",
            "phase": "complete",
            "source_commit": plan.source_commit,
            "source_archive": dataclasses.asdict(plan.source_archive),
            "requirements_sha256": plan.requirements_sha256,
            "status": "complete",
        }
        body = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        with self.assertRaisesRegex(ValueError, "artifact roster"):
            _validate_terminal_bytes(body, plan, "i-0123456789abcdef0")


if __name__ == "__main__":
    unittest.main()
