from __future__ import annotations

import hashlib
import inspect
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.launch_v23_incidence_spot import (
    EXPECTED_AWS_ACCOUNT,
    FROZEN_PAGE_ROSTER_URI,
    _build_source_archive,
    _maximum_compute_cost,
    _phase_policy,
    _rewrite_posting_receipt_uris,
    _rewrite_tree_receipt_uri,
    _spot_price,
    _validate_terminal_bytes,
    _validate_tree_progress_binding,
    _write_bulk_manifest,
    build_development_manifest,
    build_launch_plan,
    build_launch_spec,
    build_posting_manifest,
    build_worker_script,
    main,
    offline_probe,
    worker_development,
    worker_posting,
    worker_tree,
)
from scripts.run_v23_leaf_page_incidence_falsifier import validate_phase_inputs

ROOT = Path(__file__).resolve().parent.parent
LAUNCHER = ROOT / "scripts/launch_v23_incidence_spot.py"
SOURCE_SHA = "4dfe1c0ddfff86a2c346405e3df2336b22a00920"


def _canonical_progress_bytes(
    *,
    completed_units: int,
    previous_progress_sha256: str | None,
    sequence: int,
    total_units: int,
    phase: str = "tree-training",
) -> bytes:
    return (
        json.dumps(
            {
                "completed_units": completed_units,
                "last_object_digest": "11" * 32,
                "phase": phase,
                "previous_progress_sha256": previous_progress_sha256,
                "sequence": sequence,
                "total_units": total_units,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


class V23IncidenceSpotLauncherTests(unittest.TestCase):
    def test_development_manifest_binds_sealed_postings_and_burned_queries(
        self,
    ) -> None:
        tree = {
            "digest": "21" * 32,
            "digest_algorithm": "blake3",
            "encoded_bytes": 40_369_836,
            "generation": f"content-{'21' * 32}",
            "role": "incidence-tree",
            "uri": "s3://fixture/tree.bin",
        }
        one = {
            "digest": "22" * 32,
            "digest_algorithm": "blake3",
            "encoded_bytes": 51_502_404,
            "generation": f"content-{'22' * 32}",
            "role": "incidence-postings-one",
            "uri": "s3://fixture/one.bin",
        }
        two = {
            "digest": "23" * 32,
            "digest_algorithm": "blake3",
            "encoded_bytes": 59_186_088,
            "generation": f"content-{'23' * 32}",
            "role": "incidence-postings-two",
            "uri": "s3://fixture/two.bin",
        }
        posting_receipt = {
            "claim_eligible": False,
            "executable_sha256": "24" * 32,
            "final_progress_sha256": "25" * 32,
            "fma_backend": "aarch64-neon-fma",
            "network_namespace_inode": 42,
            "ordered_inputs": [
                {
                    "digest": "26" * 32,
                    "digest_algorithm": "sha256",
                    "encoded_bytes": 123,
                    "generation": "fixture-parent",
                    "role": "parent-receipt",
                    "uri": "s3://fixture/tree-receipt.json",
                },
                tree,
            ],
            "outputs": [one, two],
            "parent_receipt_sha256": "27" * 32,
            "phase": "posting-construction",
            "preflight_evidence": None,
            "probes": {
                "allowlisted_inputs_opened": True,
                "forbidden_roles_absent": True,
                "network_canary_denied": True,
                "network_namespace_changed": True,
                "output_writable": True,
            },
            "run_mode": "execute",
            "schema": "borsuk-v23-incidence-receipt-v3",
            "stop": None,
        }
        posting_bytes = (
            json.dumps(posting_receipt, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        d2_bytes = b'{"fixture":"d2"}\n'
        query_bytes = b"PAR1fixture"

        def identity(role: str, uri: str, raw: bytes) -> dict[str, object]:
            digest = hashlib.sha256(raw).hexdigest()
            return {
                "digest": digest,
                "digest_algorithm": "sha256",
                "encoded_bytes": len(raw),
                "generation": f"unversioned-sha256:{digest}",
                "role": role,
                "uri": uri,
            }

        posting_identity = identity(
            "parent-receipt", "s3://fixture/posting-receipt.json", posting_bytes
        )
        d2_identity = identity("d2-report", "s3://fixture/d2.json", d2_bytes)
        query_identity = identity(
            "query-parquet", "s3://fixture/query.parquet", query_bytes
        )
        with (
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_POSTING_RECEIPT_URI",
                posting_identity["uri"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_POSTING_RECEIPT_SHA256",
                posting_identity["digest"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_POSTING_RECEIPT_BYTES",
                posting_identity["encoded_bytes"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_D2_REPORT_URI",
                d2_identity["uri"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_D2_REPORT_SHA256",
                d2_identity["digest"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_D2_REPORT_BYTES",
                d2_identity["encoded_bytes"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_QUERY_URI",
                query_identity["uri"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_QUERY_SHA256",
                query_identity["digest"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_QUERY_BYTES",
                query_identity["encoded_bytes"],
            ),
        ):
            raw = build_development_manifest(
                posting_receipt_bytes=posting_bytes,
                posting_receipt_identity=posting_identity,
                d2_report_bytes=d2_bytes,
                d2_report_identity=d2_identity,
                query_bytes=query_bytes,
                query_identity=query_identity,
            )
        manifest = json.loads(raw)
        self.assertEqual(raw, json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        self.assertEqual(manifest["phase"], "development-evaluation")
        self.assertEqual(manifest["parent_receipt_sha256"], posting_identity["digest"])
        self.assertEqual(
            [item["identity"]["role"] for item in manifest["ordered_inputs"]],
            [
                "parent-receipt",
                "incidence-tree",
                "incidence-postings-one",
                "incidence-postings-two",
                "d2-report",
                "query-parquet",
            ],
        )

    def test_development_plan_binds_sealed_inputs_and_keeps_holdout_fenced(self) -> None:
        plan = build_launch_plan(
            phase="development-evaluation",
            run_id="fixture-development-run",
            source_commit=SOURCE_SHA,
        )
        self.assertEqual(plan["preflight_input_count"], 4)
        self.assertEqual(plan["execute_input_count"], 6)
        self.assertIn("development-evaluation", plan["supported_phases"])
        self.assertNotIn("development-evaluation", plan["blocked_phases"])
        self.assertIn("holdout-binding", plan["blocked_phases"])
        self.assertFalse(plan["d3_allowed"])

    def test_development_worker_has_no_page_body_or_holdout_surface(self) -> None:
        worker = build_worker_script(
            phase="development-evaluation",
            run_id="fixture-development-run",
            source_commit=SOURCE_SHA,
            source_uri="s3://borsuk-evidence/source.tar",
            source_sha256="11" * 32,
            result_uri="s3://borsuk-evidence/incidence/fixture-development-run",
            spot_price_usd_per_hour="0.321",
        )
        self.assertIn("--build-development-manifest", worker)
        self.assertIn("--worker-development", worker)
        self.assertIn("development-result.json", worker)
        self.assertIn("development-latency.bin", worker)
        self.assertIn("development-receipt.json", worker)
        self.assertNotIn("page-body-", worker)
        self.assertNotIn("neighbors-parquet", worker)
        self.assertNotIn("holdout-evaluation", worker)
        self.assertLessEqual(len(worker.encode()), 16_384)

    def test_development_bulk_manifest_preflight_excludes_burned_queries(self) -> None:
        construction = json.loads(
            (ROOT / "scripts/fixtures/v23_incidence_training_manifest.json").read_bytes()
        )
        identity = construction["ordered_inputs"][0]["identity"]

        def phase_object(role: str) -> dict[str, object]:
            changed = dict(identity)
            changed["role"] = role
            changed["uri"] = f"s3://fixture/{role}"
            return {"authority_kind": "phase-object", "identity": changed}

        fixed = [
            phase_object("parent-receipt"),
            phase_object("incidence-tree"),
            phase_object("incidence-postings-one"),
            phase_object("incidence-postings-two"),
        ]
        burned = [phase_object("d2-report"), phase_object("query-parquet")]
        manifest = dict(construction)
        manifest["phase"] = "development-evaluation"
        manifest["ordered_inputs"] = fixed + burned
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "development.json"
            preflight = root / "preflight.json"
            execute = root / "execute.json"
            source.write_bytes(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            )
            _write_bulk_manifest(source, preflight, False)
            _write_bulk_manifest(source, execute, True)
            self.assertEqual(json.loads(preflight.read_bytes())["ordered_inputs"], fixed)
            self.assertEqual(json.loads(execute.read_bytes())["ordered_inputs"], fixed + burned)

    def test_development_cli_builds_manifest_and_dispatches_worker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            posting = root / "posting-receipt.json"
            d2 = root / "d2.json"
            query = root / "query.parquet"
            manifest = root / "development-manifest.json"
            evidence = root / "evidence"
            posting.write_bytes(b"posting\n")
            d2.write_bytes(b"d2\n")
            query.write_bytes(b"query\n")
            expected = b'{"phase":"development-evaluation"}\n'
            with patch(
                "scripts.launch_v23_incidence_spot.build_development_manifest",
                return_value=expected,
            ) as build:
                self.assertEqual(
                    main(
                        [
                            "--build-development-manifest",
                            "--posting-receipt",
                            str(posting),
                            "--d2-report",
                            str(d2),
                            "--query-parquet",
                            str(query),
                            "--development-manifest-output",
                            str(manifest),
                        ]
                    ),
                    0,
                )
            self.assertEqual(manifest.read_bytes(), expected)
            self.assertEqual(build.call_args.kwargs["posting_receipt_bytes"], posting.read_bytes())
            self.assertEqual(build.call_args.kwargs["d2_report_bytes"], d2.read_bytes())
            self.assertEqual(build.call_args.kwargs["query_bytes"], query.read_bytes())

            binary = Path("/bin/true").resolve()
            binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
            with patch(
                "scripts.launch_v23_incidence_spot.worker_development",
                return_value=0,
            ) as worker:
                self.assertEqual(
                    main(
                        [
                            "--worker-development",
                            "--binary",
                            str(binary),
                            "--binary-sha256",
                            binary_sha256,
                            "--evidence-directory",
                            str(evidence),
                            "--output-uri-prefix",
                            "s3://fixture/development",
                            "--development-manifest",
                            str(manifest),
                        ]
                    ),
                    0,
                )
            self.assertEqual(worker.call_args.args[4], expected)

    def test_worker_development_runs_two_modes_and_publishes_canonical_outputs(
        self,
    ) -> None:
        import blake3

        manifest = {
            "algorithm": "fixture",
            "claim_eligible": False,
            "dataset_id": "deep-image-96",
            "index_id": "fixture-index",
            "ordered_inputs": [],
            "parent_receipt_sha256": "ab" * 32,
            "phase": "development-evaluation",
            "schema": "borsuk-v23-incidence-manifest-v1",
            "source_archive_sha256": "cd" * 32,
            "source_commit": SOURCE_SHA,
        }
        manifest_bytes = (
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )

        def stage_stub(_manifest: Path, directory: Path, receipt: Path) -> None:
            directory.mkdir()
            receipt.write_text('{"staged":true}\n', encoding="utf-8")

        calls = 0
        progress_bytes = b""

        def run_stub(policy: Any, _limits: object) -> int:
            nonlocal calls, progress_bytes
            calls += 1
            policy.output.mkdir(exist_ok=True)
            if calls == 1:
                policy.output.joinpath("receipt.json").write_text(
                    '{"preflight":true}\n', encoding="utf-8"
                )
                return 0
            result = b'{"claim_eligible":false}\n'
            latency = b"latency-bundle"
            result_digest = hashlib.sha256(result).hexdigest()
            latency_digest = blake3.blake3(latency).hexdigest()
            result_path = policy.output / f"development-result-{result_digest}.bin"
            latency_path = policy.output / f"development-latency-{latency_digest}.bin"
            result_path.write_bytes(result)
            latency_path.write_bytes(latency)
            progress_start = _canonical_progress_bytes(
                completed_units=0,
                previous_progress_sha256=None,
                sequence=0,
                total_units=18,
                phase="development-evaluation",
            )
            progress_final = _canonical_progress_bytes(
                completed_units=18,
                previous_progress_sha256=hashlib.sha256(progress_start).hexdigest(),
                sequence=1,
                total_units=18,
                phase="development-evaluation",
            )
            progress = progress_start + progress_final
            progress_bytes = progress
            policy.output.joinpath("progress.json").write_bytes(progress)
            identities = [
                {
                    "digest": result_digest,
                    "digest_algorithm": "sha256",
                    "encoded_bytes": len(result),
                    "generation": f"content-{result_digest}",
                    "role": "development-result",
                    "uri": f"file://{result_path}",
                },
                {
                    "digest": latency_digest,
                    "digest_algorithm": "blake3",
                    "encoded_bytes": len(latency),
                    "generation": f"content-{latency_digest}",
                    "role": "development-latency",
                    "uri": f"file://{latency_path}",
                },
            ]
            policy.output.joinpath("receipt.json").write_bytes(
                json.dumps(
                    {
                        "final_progress_sha256": hashlib.sha256(progress).hexdigest(),
                        "outputs": identities,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
                + b"\n"
            )
            return 0

        with (
            tempfile.TemporaryDirectory() as directory,
            patch("scripts.launch_v23_incidence_spot._stage", side_effect=stage_stub),
            patch(
                "scripts.run_v23_leaf_page_incidence_falsifier.run_phase",
                side_effect=run_stub,
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_POSTING_RECEIPT_SHA256",
                "ab" * 32,
            ),
        ):
            root = Path(directory)
            binary = Path("/bin/true").resolve()
            self.assertEqual(
                worker_development(
                    binary=binary,
                    binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                    evidence=root / "evidence",
                    output_uri_prefix="s3://fixture/development-run",
                    manifest_bytes=manifest_bytes,
                ),
                0,
            )
            evidence = root / "evidence"
            self.assertEqual(calls, 2)
            self.assertTrue(evidence.joinpath("preflight-receipt.json").is_file())
            self.assertEqual(
                evidence.joinpath("progress.json").read_bytes(), progress_bytes
            )
            self.assertEqual(
                evidence.joinpath("development-result.json").read_bytes(),
                b'{"claim_eligible":false}\n',
            )
            self.assertEqual(
                evidence.joinpath("development-latency.bin").read_bytes(),
                b"latency-bundle",
            )
            receipt = json.loads(evidence.joinpath("development-receipt.json").read_bytes())
            self.assertEqual(
                [item["uri"] for item in receipt["outputs"]],
                [
                    "s3://fixture/development-run/development-result.json",
                    "s3://fixture/development-run/development-latency.bin",
                ],
            )

    def test_posting_manifest_binds_tree_roster_and_every_page_without_reads(
        self,
    ) -> None:
        construction_raw = (
            ROOT / "scripts/fixtures/v23_incidence_training_manifest.json"
        ).read_bytes()
        construction_digest = hashlib.sha256(construction_raw).hexdigest()
        tree_digest = "ab" * 32
        tree_receipt = {
            "claim_eligible": False,
            "executable_sha256": "12" * 32,
            "final_progress_sha256": "13" * 32,
            "fma_backend": "aarch64-neon-fma",
            "network_namespace_inode": 42,
            "ordered_inputs": [
                {
                    "digest": construction_digest,
                    "digest_algorithm": "sha256",
                    "encoded_bytes": len(construction_raw),
                    "generation": f"unversioned-sha256:{construction_digest}",
                    "role": "construction-manifest",
                    "uri": (
                        "git://borsuk/scripts/fixtures/"
                        "v23_incidence_training_manifest.json"
                    ),
                }
            ],
            "outputs": [
                {
                    "digest": tree_digest,
                    "digest_algorithm": "blake3",
                    "encoded_bytes": 40_369_836,
                    "generation": f"content-{tree_digest}",
                    "role": "incidence-tree",
                    "uri": "s3://borsuk-evidence/tree/incidence-tree.bin",
                }
            ],
            "parent_receipt_sha256": "16" * 32,
            "phase": "tree-training",
            "preflight_evidence": None,
            "probes": {
                "allowlisted_inputs_opened": True,
                "forbidden_roles_absent": True,
                "network_canary_denied": True,
                "network_namespace_changed": True,
                "output_writable": True,
            },
            "run_mode": "execute",
            "schema": "borsuk-v23-incidence-receipt-v3",
            "stop": None,
        }
        tree_receipt_bytes = (
            json.dumps(tree_receipt, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        generation = list(
            bytes.fromhex(
                "b20f22206edd140fdd5474a3786f3f1a"
                "6ff51fa5f9d5f1be9363092156cb74ec"
            )
        )
        primary_base, primary_extra = divmod(9_990_000, 28_282)
        replica_base, replica_extra = divmod(8_630_111, 28_282)
        encoded_base, encoded_extra = divmod(3_780_639_674, 28_282)
        pages = [
            {
                "checksum": f"{ordinal + 1:064x}",
                "code_width": 192,
                "dimensions": 96,
                "encoded_bytes": encoded_base + int(ordinal < encoded_extra),
                "family": "f16-flat",
                "generation_checksum": generation,
                "metric": "cosine",
                "page_ordinal": ordinal,
                "path": f"pages/{ordinal + 1:064x}",
                "primary_rows": primary_base + int(ordinal < primary_extra),
                "replicated_rows": replica_base + int(ordinal < replica_extra),
            }
            for ordinal in range(28_282)
        ]
        roster = {
            "claim_eligible": False,
            "d1_report_sha256": (
                "91717a4077c8a7d6b909f1f8d14f59d6"
                "a6d422a29e06b3d665a02c29743cbc39"
            ),
            "dataset_id": "deep-image-96",
            "document_kind": "publication-v3-v23-page-roster",
            "index_id": "index-bcda7bb66812e162d45077e6",
            "page_uri": (
                "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/"
                "results/r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/"
                "0000/attempts/0001/pages"
            ),
            "pages": pages,
            "schema": "borsuk-v23-pages-v1",
            "source_archive_sha256": (
                "77917b0f5621d2580fef444ee362669a"
                "39d01c8453bee1c10ca1823631117f6d"
            ),
            "stage": "d2",
        }
        roster_bytes = (
            json.dumps(roster, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        )
        tree_receipt_identity = {
            "digest": hashlib.sha256(tree_receipt_bytes).hexdigest(),
            "digest_algorithm": "sha256",
            "encoded_bytes": len(tree_receipt_bytes),
            "generation": "generation-tree-receipt",
            "role": "parent-receipt",
            "uri": "s3://borsuk-evidence/tree/tree-receipt.json",
        }
        roster_identity = {
            "digest": hashlib.sha256(roster_bytes).hexdigest(),
            "digest_algorithm": "sha256",
            "encoded_bytes": len(roster_bytes),
            "generation": "generation-page-roster",
            "role": "page-roster",
            "uri": (
                "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/"
                "results/r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/"
                "0000/attempts/0001/bench_v23_pages.json"
            ),
        }
        frozen_roster = (
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_PAGE_ROSTER_SHA256",
                roster_identity["digest"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_PAGE_ROSTER_BYTES",
                roster_identity["encoded_bytes"],
            ),
        )
        frozen_tree = (
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_TREE_RECEIPT_URI",
                tree_receipt_identity["uri"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_TREE_RECEIPT_SHA256",
                tree_receipt_identity["digest"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_TREE_RECEIPT_BYTES",
                tree_receipt_identity["encoded_bytes"],
            ),
        )
        with (
            frozen_roster[0],
            frozen_roster[1],
            frozen_tree[0],
            frozen_tree[1],
            frozen_tree[2],
        ):
            manifest_bytes = build_posting_manifest(
                tree_receipt_bytes=tree_receipt_bytes,
                tree_receipt_identity=tree_receipt_identity,
                roster_bytes=roster_bytes,
                roster_identity=roster_identity,
            )
        manifest = json.loads(manifest_bytes)

        self.assertEqual(
            manifest_bytes,
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
            + b"\n",
        )
        self.assertEqual(manifest["phase"], "posting-construction")
        self.assertEqual(
            manifest["parent_receipt_sha256"],
            hashlib.sha256(tree_receipt_bytes).hexdigest(),
        )
        self.assertEqual(len(manifest["ordered_inputs"]), 28_285)
        identities = [item["identity"] for item in manifest["ordered_inputs"]]
        self.assertEqual(
            [item["role"] for item in identities[:3]],
            ["parent-receipt", "incidence-tree", "page-roster"],
        )
        self.assertEqual(identities[1], tree_receipt["outputs"][0])
        self.assertEqual(identities[3]["role"], "page-body-00000")
        self.assertEqual(
            identities[3]["uri"],
            roster["page_uri"] + "/pages/"
            + pages[0]["checksum"],
        )
        self.assertEqual(identities[3]["digest_algorithm"], "blake3")
        self.assertEqual(
            identities[3]["generation"],
            f"unversioned-blake3:{pages[0]['checksum']}",
        )
        self.assertEqual(identities[-1]["role"], "page-body-28281")
        self.assertEqual(identities[-1]["encoded_bytes"], pages[-1]["encoded_bytes"])

        wrong_tree_identity = dict(tree_receipt_identity)
        wrong_tree_identity["uri"] = "s3://borsuk-evidence/tree/wrong-receipt.json"
        with (
            frozen_roster[0],
            frozen_roster[1],
            frozen_tree[0],
            frozen_tree[1],
            frozen_tree[2],
            self.assertRaisesRegex(ValueError, "parent receipt authority"),
        ):
            build_posting_manifest(
                tree_receipt_bytes=tree_receipt_bytes,
                tree_receipt_identity=wrong_tree_identity,
                roster_bytes=roster_bytes,
                roster_identity=roster_identity,
            )

        changed = json.loads(roster_bytes)
        changed["pages"][7]["page_ordinal"] = 8
        changed_bytes = (
            json.dumps(changed, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        )
        changed_identity = dict(roster_identity)
        changed_identity["digest"] = hashlib.sha256(changed_bytes).hexdigest()
        changed_identity["encoded_bytes"] = len(changed_bytes)
        with (
            frozen_roster[0],
            frozen_roster[1],
            frozen_tree[0],
            frozen_tree[1],
            frozen_tree[2],
            self.assertRaisesRegex(ValueError, "page roster authority"),
        ):
            build_posting_manifest(
                tree_receipt_bytes=tree_receipt_bytes,
                tree_receipt_identity=tree_receipt_identity,
                roster_bytes=changed_bytes,
                roster_identity=changed_identity,
            )

        duplicate = json.loads(roster_bytes)
        duplicate["pages"][1]["checksum"] = duplicate["pages"][0]["checksum"]
        duplicate["pages"][1]["path"] = duplicate["pages"][0]["path"]
        duplicate_bytes = (
            json.dumps(duplicate, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        duplicate_identity = dict(roster_identity)
        duplicate_identity["digest"] = hashlib.sha256(duplicate_bytes).hexdigest()
        duplicate_identity["encoded_bytes"] = len(duplicate_bytes)
        with patch(
            "scripts.launch_v23_incidence_spot.FROZEN_PAGE_ROSTER_SHA256",
            duplicate_identity["digest"],
        ), patch(
            "scripts.launch_v23_incidence_spot.FROZEN_PAGE_ROSTER_BYTES",
            duplicate_identity["encoded_bytes"],
        ), frozen_tree[0], frozen_tree[1], frozen_tree[2], self.assertRaisesRegex(
            ValueError, "page roster authority"
        ):
            build_posting_manifest(
                tree_receipt_bytes=tree_receipt_bytes,
                tree_receipt_identity=tree_receipt_identity,
                roster_bytes=duplicate_bytes,
                roster_identity=duplicate_identity,
            )

        encoded_drift = json.loads(roster_bytes)
        encoded_drift["pages"][0]["encoded_bytes"] += 1
        encoded_drift_bytes = (
            json.dumps(
                encoded_drift, sort_keys=True, separators=(",", ":")
            ).encode()
            + b"\n"
        )
        encoded_drift_identity = dict(roster_identity)
        encoded_drift_identity["digest"] = hashlib.sha256(
            encoded_drift_bytes
        ).hexdigest()
        encoded_drift_identity["encoded_bytes"] = len(encoded_drift_bytes)
        with patch(
            "scripts.launch_v23_incidence_spot.FROZEN_PAGE_ROSTER_SHA256",
            encoded_drift_identity["digest"],
        ), patch(
            "scripts.launch_v23_incidence_spot.FROZEN_PAGE_ROSTER_BYTES",
            encoded_drift_identity["encoded_bytes"],
        ), frozen_tree[0], frozen_tree[1], frozen_tree[2], self.assertRaisesRegex(
            ValueError, "page roster authority"
        ):
            build_posting_manifest(
                tree_receipt_bytes=tree_receipt_bytes,
                tree_receipt_identity=tree_receipt_identity,
                roster_bytes=encoded_drift_bytes,
                roster_identity=encoded_drift_identity,
            )

        changed_receipt = json.loads(tree_receipt_bytes)
        changed_receipt["ordered_inputs"][0]["digest"] = "17" * 32
        changed_receipt_bytes = (
            json.dumps(changed_receipt, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        changed_receipt_identity = dict(tree_receipt_identity)
        changed_receipt_identity["digest"] = hashlib.sha256(
            changed_receipt_bytes
        ).hexdigest()
        changed_receipt_identity["encoded_bytes"] = len(changed_receipt_bytes)
        with (
            frozen_roster[0],
            frozen_roster[1],
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_TREE_RECEIPT_URI",
                changed_receipt_identity["uri"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_TREE_RECEIPT_SHA256",
                changed_receipt_identity["digest"],
            ),
            patch(
                "scripts.launch_v23_incidence_spot.FROZEN_TREE_RECEIPT_BYTES",
                changed_receipt_identity["encoded_bytes"],
            ),
            self.assertRaisesRegex(ValueError, "tree receipt authority"),
        ):
            build_posting_manifest(
                tree_receipt_bytes=changed_receipt_bytes,
                tree_receipt_identity=changed_receipt_identity,
                roster_bytes=roster_bytes,
                roster_identity=roster_identity,
            )

    def test_worker_uses_disposable_offline_phase_without_runtime_discovery(
        self,
    ) -> None:
        worker = build_worker_script(
            phase="tree-training",
            run_id="fixture-run",
            source_commit=SOURCE_SHA,
            source_uri="s3://borsuk-evidence/source.tar",
            source_sha256="11" * 32,
            result_uri="s3://borsuk-evidence/incidence/fixture-run",
            spot_price_usd_per_hour="0.321",
        )
        launcher_source = inspect.getsource(
            sys.modules["scripts.launch_v23_incidence_spot"]
        )
        self.assertNotIn("ldd", worker)
        self.assertNotIn("_runtime_mounts", launcher_source)
        self.assertNotIn("runtime-loader", launcher_source)
        self.assertNotIn('"--mount-proc"', launcher_source)
        self.assertNotIn("--namespace-probe", worker)
        self.assertIn("--offline-probe", worker)

    def test_launcher_direct_script_offline_probe_resolves_sibling_module(
        self,
    ) -> None:
        program = r"""
import importlib.util
import pathlib
import sys
import tempfile
import types

root = pathlib.Path(sys.argv[1])
scripts = root / "scripts"
sys.path.insert(0, str(scripts))
spec = importlib.util.spec_from_file_location(
    "launch_v23_incidence_spot_direct",
    scripts / "launch_v23_incidence_spot.py",
)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
with tempfile.TemporaryDirectory() as directory:
    psi = pathlib.Path(directory) / "memory.pressure"
    psi.write_text("full avg10=0.00 total=0\n", encoding="ascii")
    module.MEMORY_PSI_PATH = psi
    module.subprocess.run = lambda *args, **kwargs: types.SimpleNamespace(returncode=0)
    assert module.offline_probe() == 0
"""
        completed = subprocess.run(
            [sys.executable, "-I", "-c", program, str(ROOT)],
            cwd="/",
            env={"PATH": os.environ["PATH"]},
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_launcher_entrypoint_preserves_full_traceback(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(LAUNCHER),
                "--phase",
                "tree-training",
                "--run-id",
                "/",
                "--dry-run",
            ],
            cwd=ROOT,
            env={**os.environ, "BORSUK_SOURCE_COMMIT": SOURCE_SHA},
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("Traceback (most recent call last):", completed.stderr)
        self.assertIn("_require_token", completed.stderr)

    def test_tree_receipt_binds_the_final_canonical_progress_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            progress = Path(directory) / "progress.json"
            root = _canonical_progress_bytes(
                completed_units=0,
                previous_progress_sha256=None,
                sequence=0,
                total_units=18,
            )
            final = _canonical_progress_bytes(
                completed_units=18,
                previous_progress_sha256=hashlib.sha256(root).hexdigest(),
                sequence=1,
                total_units=18,
            )
            progress.write_bytes(root + final)
            digest = hashlib.sha256(progress.read_bytes()).hexdigest()
            receipt = {"final_progress_sha256": digest}

            _validate_tree_progress_binding(receipt, progress)
            receipt["final_progress_sha256"] = "00" * 32
            with self.assertRaisesRegex(ValueError, "progress binding"):
                _validate_tree_progress_binding(receipt, progress)

    def test_worker_captures_and_publishes_failure_evidence(self) -> None:
        worker = build_worker_script(
            phase="tree-training",
            run_id="fixture-run",
            source_commit=SOURCE_SHA,
            source_uri="s3://borsuk-evidence/source.tar",
            source_sha256="11" * 32,
            result_uri="s3://borsuk-evidence/incidence/fixture-run",
            spot_price_usd_per_hour="0.321",
        )

        self.assertIn('phase_log="$evidence/phase.log"', worker)
        self.assertIn('phase_journal="$evidence/phase-journal.txt"', worker)
        self.assertIn('exec >>"$worker_log" 2>&1', worker)
        self.assertNotIn("tee -a", worker)
        self.assertIn("--property=StandardOutput=append:$phase_log", worker)
        self.assertIn("--property=StandardError=append:$phase_log", worker)
        self.assertIn(
            'journalctl --no-pager -o short-iso -u "$unit" >"$phase_journal"',
            worker,
        )
        for evidence_name in (
            "binary.json",
            "preflight-receipt.json",
            "preflight-staging-receipt.json",
            "execute-staging-receipt.json",
            "phase.log",
            "phase-journal.txt",
            "phase-traceback.txt",
            "phase-failure.json",
            "progress.json",
        ):
            self.assertIn(evidence_name, worker)
        evidence_upload = worker.index("phase-failure.json")
        self.assertLess(evidence_upload, worker.index("ATTEMPT_FAILED.json"))
        self.assertIn(
            'put_once "$evidence/progress.json" progress.json || publish_status=86',
            worker,
        )
        self.assertEqual(
            worker.count(
                "cargo build --locked --release -p borsuk --example "
                "v23_leaf_page_incidence_falsifier"
            ),
            1,
        )

    def test_posting_worker_builds_manifest_and_publishes_both_artifacts(
        self,
    ) -> None:
        worker = build_worker_script(
            phase="posting-construction",
            run_id="fixture-posting-run",
            source_commit=SOURCE_SHA,
            source_uri="s3://borsuk-evidence/source.tar",
            source_sha256="11" * 32,
            result_uri="s3://borsuk-evidence/incidence/fixture-posting-run",
            spot_price_usd_per_hour="0.321",
        )

        self.assertIn(
            "s3://borsuk-bench-453182569524-euc1/research/"
            "v23-leaf-page-incidence/"
            "a321c473cb38a3b38c4757a50acf14e144b0441b0ca4bbbe7a8c7f3baaef78cc/"
            "v23-incidence-tree-20260831T120514Z/tree-receipt.json",
            worker,
        )
        self.assertIn(FROZEN_PAGE_ROSTER_URI, worker)
        self.assertIn("--build-posting-manifest", worker)
        self.assertIn("--worker-posting", worker)
        self.assertIn('phase=posting-construction', worker)
        self.assertIn(
            'if [[ "$phase" == "posting-construction" ]]; then', worker
        )
        self.assertIn(
            'worker_mode=(--worker-posting --posting-manifest "$posting_manifest")',
            worker,
        )
        self.assertIn("posting-receipt.json", worker)
        self.assertIn("incidence-postings-one.bin", worker)
        self.assertIn("incidence-postings-two.bin", worker)
        self.assertIn('"phase":phase', worker)
        self.assertLessEqual(len(worker.encode()), 16_384)
        self.assertLess(
            worker.index(
                "posting_bootstrap=/var/lib/borsuk-v23-incidence/posting-bootstrap"
            ),
            worker.index("trap finish EXIT"),
        )

    def test_worker_tree_preserves_traceback_and_partial_receipts(self) -> None:
        def stage_stub(
            _manifest: Path, directory: Path, receipt: Path
        ) -> None:
            directory.mkdir()
            receipt.write_text('{"staged":true}\n', encoding="utf-8")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "evidence"
            binary = Path("/bin/true").resolve()
            binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
            with (
                patch(
                    "scripts.launch_v23_incidence_spot._stage",
                    side_effect=stage_stub,
                ),
                patch(
                    "scripts.launch_v23_incidence_spot._phase_policy",
                    return_value=object(),
                ),
                patch(
                    "scripts.run_v23_leaf_page_incidence_falsifier.run_phase",
                    side_effect=RuntimeError("phase boom"),
                ),
                self.assertRaisesRegex(RuntimeError, "phase boom"),
            ):
                worker_tree(
                    binary=binary,
                    binary_sha256=binary_sha256,
                    evidence=evidence,
                    output_uri_prefix="s3://borsuk-evidence/incidence/fixture-run",
                )

            traceback_bytes = (evidence / "phase-traceback.txt").read_bytes()
            self.assertIn(b"RuntimeError: phase boom", traceback_bytes)
            failure_bytes = (evidence / "phase-failure.json").read_bytes()
            failure = json.loads(failure_bytes)
            self.assertEqual(
                failure_bytes,
                json.dumps(failure, sort_keys=True, separators=(",", ":")).encode()
                + b"\n",
            )
            self.assertEqual(failure["phase"], "tree-training")
            self.assertEqual(failure["stage"], "preflight-run")
            self.assertEqual(failure["exception_type"], "RuntimeError")
            self.assertEqual(failure["message"], "phase boom")
            self.assertFalse(failure["claim_eligible"])
            self.assertEqual(
                (evidence / "preflight-staging-receipt.json").read_text(
                    encoding="utf-8"
                ),
                '{"staged":true}\n',
            )

    def test_worker_tree_cleans_private_root_when_initialization_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            private_root = parent / "private-root"
            private_root.mkdir()
            evidence = parent / "evidence"
            binary = Path("/bin/true").resolve()
            binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
            original_mkdir = Path.mkdir

            def mkdir(path: Path, *args: Any, **kwargs: Any) -> None:
                if path.name == "preflight-scratch":
                    raise OSError("scratch mkdir boom")
                original_mkdir(path, *args, **kwargs)

            with (
                patch(
                    "scripts.launch_v23_incidence_spot.tempfile.mkdtemp",
                    return_value=str(private_root),
                ),
                patch("pathlib.Path.mkdir", new=mkdir),
                self.assertRaisesRegex(OSError, "scratch mkdir boom"),
            ):
                worker_tree(
                    binary=binary,
                    binary_sha256=binary_sha256,
                    evidence=evidence,
                    output_uri_prefix="s3://borsuk-evidence/incidence/fixture-run",
                )

            self.assertFalse(private_root.exists())
            self.assertEqual(
                json.loads((evidence / "phase-failure.json").read_bytes())["stage"],
                "initialization",
            )

    def test_worker_tree_preserves_existing_preflight_receipt_on_execute_failure(
        self,
    ) -> None:
        def stage_stub(
            _manifest: Path, directory: Path, receipt: Path
        ) -> None:
            directory.mkdir()
            receipt.write_text('{"staged":true}\n', encoding="utf-8")

        calls = 0

        def run_stub(policy: Any, _limits: object) -> int:
            nonlocal calls
            calls += 1
            if calls == 1:
                output = policy.output
                output.joinpath("receipt.json").write_text(
                    '{"preflight":true}\n', encoding="utf-8"
                )
                return 0
            policy.output.joinpath("progress.json").write_text(
                '{"completed_units":2}\n', encoding="utf-8"
            )
            raise RuntimeError("execute boom")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "evidence"
            evidence.mkdir()
            evidence.joinpath("preflight-staging-receipt.json").write_text(
                '{"stale":true}\n', encoding="utf-8"
            )
            binary = Path("/bin/true").resolve()
            binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
            with (
                patch(
                    "scripts.launch_v23_incidence_spot._stage",
                    side_effect=stage_stub,
                ),
                patch(
                    "scripts.run_v23_leaf_page_incidence_falsifier.run_phase",
                    side_effect=run_stub,
                ),
                self.assertRaisesRegex(RuntimeError, "execute boom"),
            ):
                worker_tree(
                    binary=binary,
                    binary_sha256=binary_sha256,
                    evidence=evidence,
                    output_uri_prefix="s3://borsuk-evidence/incidence/fixture-run",
                )

            self.assertEqual(
                (evidence / "preflight-receipt.json").read_text(encoding="utf-8"),
                '{"preflight":true}\n',
            )
            self.assertIn(
                "RuntimeError: execute boom",
                (evidence / "phase-traceback.txt").read_text(encoding="utf-8"),
            )
            self.assertEqual(
                json.loads((evidence / "phase-failure.json").read_bytes())["stage"],
                "execute-run",
            )
            self.assertEqual(
                (evidence / "progress.json").read_text(encoding="utf-8"),
                '{"completed_units":2}\n',
            )
            self.assertEqual(
                (evidence / "preflight-staging-receipt.json").read_text(
                    encoding="utf-8"
                ),
                '{"stale":true}\n',
            )

    def test_worker_posting_runs_two_staged_modes_and_publishes_two_artifacts(
        self,
    ) -> None:
        import blake3

        construction = json.loads(
            (ROOT / "scripts/fixtures/v23_incidence_training_manifest.json").read_bytes()
        )
        template = construction["ordered_inputs"][0]["identity"]

        def phase_object(role: str, ordinal: int) -> dict[str, object]:
            identity = dict(template)
            identity["role"] = role
            identity["uri"] = f"s3://fixture/{role}-{ordinal}"
            return {"authority_kind": "phase-object", "identity": identity}

        construction["phase"] = "posting-construction"
        construction["parent_receipt_sha256"] = (
            "c1af5ab84ef20797ffe52fa0a93872008"
            "df817c142957f009895c8b7fc853a99"
        )
        construction["ordered_inputs"] = [
            phase_object("parent-receipt", 0),
            phase_object("incidence-tree", 0),
            phase_object("page-roster", 0),
            *[
                phase_object(f"page-body-{ordinal:05}", ordinal)
                for ordinal in range(300)
            ],
        ]
        manifest_bytes = (
            json.dumps(construction, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )

        def stage_stub(manifest: Path, directory: Path, receipt: Path) -> None:
            staged_counts.append(len(json.loads(manifest.read_bytes())["ordered_inputs"]))
            directory.mkdir()
            receipt.write_text('{"staged":true}\n', encoding="utf-8")

        calls = 0

        def run_stub(policy: Any, _limits: object) -> int:
            nonlocal calls
            calls += 1
            if calls == 1:
                policy.output.joinpath("receipt.json").write_text(
                    '{"preflight":true}\n', encoding="utf-8"
                )
                return 0
            outputs = []
            for role, payload in {
                "incidence-postings-one": b"posting-one",
                "incidence-postings-two": b"posting-two",
            }.items():
                path = policy.output / f"{role}-content.bin"
                path.write_bytes(payload)
                digest = blake3.blake3(payload).hexdigest()
                outputs.append(
                    {
                        "digest": digest,
                        "digest_algorithm": "blake3",
                        "encoded_bytes": len(payload),
                        "generation": f"content-{digest}",
                        "role": role,
                        "uri": f"file://{path}",
                    }
                )
            root_progress = _canonical_progress_bytes(
                completed_units=0,
                previous_progress_sha256=None,
                sequence=0,
                total_units=1,
                phase="posting-construction",
            )
            final_progress = _canonical_progress_bytes(
                completed_units=1,
                previous_progress_sha256=hashlib.sha256(root_progress).hexdigest(),
                sequence=1,
                total_units=1,
                phase="posting-construction",
            )
            progress = root_progress + final_progress
            policy.output.joinpath("progress.json").write_bytes(progress)
            policy.output.joinpath("receipt.json").write_bytes(
                json.dumps(
                    {
                        "final_progress_sha256": hashlib.sha256(progress).hexdigest(),
                        "outputs": outputs,
                        "schema": "fixture-receipt",
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
                + b"\n"
            )
            return 0

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private_root = root / "posting-private"
            private_root.mkdir()
            evidence = root / "evidence"
            binary = Path("/bin/true").resolve()
            binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
            staged_counts: list[int] = []
            with (
                patch(
                    "scripts.launch_v23_incidence_spot.tempfile.mkdtemp",
                    return_value=str(private_root),
                ),
                patch(
                    "scripts.launch_v23_incidence_spot._stage",
                    side_effect=stage_stub,
                ),
                patch(
                    "scripts.run_v23_leaf_page_incidence_falsifier.run_phase",
                    side_effect=run_stub,
                ),
            ):
                status = worker_posting(
                    binary=binary,
                    binary_sha256=binary_sha256,
                    evidence=evidence,
                    output_uri_prefix="s3://borsuk-evidence/posting/fixture-run",
                    manifest_bytes=manifest_bytes,
                )

            self.assertEqual(status, 0)
            self.assertEqual(staged_counts, [259, 303])
            self.assertEqual(calls, 2)
            self.assertFalse(private_root.exists())
            self.assertEqual(
                evidence.joinpath("incidence-postings-one.bin").read_bytes(),
                b"posting-one",
            )
            self.assertEqual(
                evidence.joinpath("incidence-postings-two.bin").read_bytes(),
                b"posting-two",
            )
            receipt = json.loads(evidence.joinpath("posting-receipt.json").read_bytes())
            self.assertEqual(
                [output["uri"] for output in receipt["outputs"]],
                [
                    "s3://borsuk-evidence/posting/fixture-run/incidence-postings-one.bin",
                    "s3://borsuk-evidence/posting/fixture-run/incidence-postings-two.bin",
                ],
            )
            self.assertTrue(evidence.joinpath("progress.json").is_file())

    def test_worker_posting_rejects_unregistered_parent_before_staging(self) -> None:
        construction = json.loads(
            (ROOT / "scripts/fixtures/v23_incidence_training_manifest.json").read_bytes()
        )
        construction["phase"] = "posting-construction"
        construction["parent_receipt_sha256"] = "ab" * 32
        manifest_bytes = (
            json.dumps(construction, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )

        with (
            tempfile.TemporaryDirectory() as directory,
            patch(
                "scripts.launch_v23_incidence_spot._stage",
                side_effect=AssertionError("staging must not start"),
            ),
            self.assertRaisesRegex(ValueError, "parent receipt authority"),
        ):
            root = Path(directory)
            binary = Path("/bin/true").resolve()
            worker_posting(
                binary=binary,
                binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                evidence=root / "evidence",
                output_uri_prefix="s3://borsuk-evidence/posting/fixture-run",
                manifest_bytes=manifest_bytes,
            )

    def test_posting_cli_builds_manifest_and_dispatches_worker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tree = root / "tree-receipt.json"
            roster = root / "page-roster.json"
            manifest = root / "posting-manifest.json"
            evidence = root / "evidence"
            tree.write_bytes(b"tree-receipt\n")
            roster.write_bytes(b"page-roster\n")
            expected_manifest = b'{"phase":"posting-construction"}\n'
            with patch(
                "scripts.launch_v23_incidence_spot.build_posting_manifest",
                return_value=expected_manifest,
            ) as build:
                self.assertEqual(
                    main(
                        [
                            "--build-posting-manifest",
                            "--tree-receipt",
                            str(tree),
                            "--page-roster",
                            str(roster),
                            "--posting-manifest-output",
                            str(manifest),
                        ]
                    ),
                    0,
                )
            self.assertEqual(manifest.read_bytes(), expected_manifest)
            request = build.call_args.kwargs
            self.assertEqual(request["tree_receipt_bytes"], tree.read_bytes())
            self.assertEqual(request["roster_bytes"], roster.read_bytes())
            self.assertEqual(
                request["tree_receipt_identity"]["digest"],
                hashlib.sha256(tree.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                request["roster_identity"]["digest"],
                hashlib.sha256(roster.read_bytes()).hexdigest(),
            )

            binary = Path("/bin/true").resolve()
            binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
            with patch(
                "scripts.launch_v23_incidence_spot.worker_posting", return_value=0
            ) as worker:
                self.assertEqual(
                    main(
                        [
                            "--worker-posting",
                            "--binary",
                            str(binary),
                            "--binary-sha256",
                            binary_sha256,
                            "--evidence-directory",
                            str(evidence),
                            "--output-uri-prefix",
                            "s3://borsuk-evidence/posting/fixture-run",
                            "--posting-manifest",
                            str(manifest),
                        ]
                    ),
                    0,
                )
            self.assertEqual(worker.call_args.args[4], expected_manifest)

    def test_offline_probe_requires_memory_psi_full_avg10(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            absent = Path(directory) / "missing-memory-psi"
            with (
                patch(
                    "scripts.launch_v23_incidence_spot.MEMORY_PSI_PATH",
                    absent,
                ),
                patch("scripts.launch_v23_incidence_spot.subprocess.run") as run,
                self.assertRaises(FileNotFoundError),
            ):
                offline_probe()
            run.assert_not_called()

            malformed = Path(directory) / "malformed-memory-psi"
            for content in (
                "some avg10=0.00 total=0\n",
                "full total=0\n",
                "full avg10=not-a-number total=0\n",
                "full avg10=nan total=0\n",
                "full avg10=-0.01 total=0\n",
            ):
                with self.subTest(content=content):
                    malformed.write_text(content, encoding="ascii")
                    with (
                        patch(
                            "scripts.launch_v23_incidence_spot.MEMORY_PSI_PATH",
                            malformed,
                        ),
                        patch(
                            "scripts.launch_v23_incidence_spot.subprocess.run"
                        ) as run,
                        self.assertRaisesRegex(RuntimeError, "memory PSI full avg10"),
                    ):
                        offline_probe()
                    run.assert_not_called()

    def test_tree_plan_is_one_ephemeral_spot_worker_with_registered_stops(self) -> None:
        plan = build_launch_plan(
            phase="tree-training",
            run_id="fixture-run",
            source_commit=SOURCE_SHA,
        )

        self.assertEqual(plan["aws_profile"], "causality")
        self.assertEqual(plan["aws_account_id"], EXPECTED_AWS_ACCOUNT)
        self.assertEqual(plan["region"], "eu-central-1")
        self.assertEqual(plan["phase"], "tree-training")
        self.assertEqual(plan["instance_type"], "c7g.8xlarge")
        self.assertEqual(plan["purchase_option"], "spot")
        self.assertEqual(plan["instance_count"], 1)
        self.assertEqual(plan["root_volume_gib"], 200)
        self.assertTrue(plan["root_volume_delete_on_termination"])
        self.assertEqual(plan["rss_stop_bytes"], 2 << 30)
        self.assertEqual(plan["swap_delta_stop_bytes"], 256 << 20)
        self.assertEqual(plan["psi_full_immediate"], 0.79)
        self.assertEqual(plan["psi_full_sustained"], 0.50)
        self.assertEqual(plan["progress_stop_seconds"], 300)
        self.assertEqual(plan["wall_stop_seconds"], 7200)
        self.assertEqual(plan["outer_wall_stop_seconds"], 16_200)
        self.assertEqual(plan["billable_wall_stop_seconds"], 21_600)
        self.assertLessEqual(plan["maximum_compute_cost_usd"], 5.0)
        self.assertEqual(plan["source_commit"], SOURCE_SHA)
        self.assertEqual(
            plan["construction_manifest"],
            "scripts/fixtures/v23_incidence_training_manifest.json",
        )
        self.assertEqual(plan["preflight_input_count"], 1)
        self.assertEqual(plan["execute_input_count"], 59)
        self.assertFalse(plan["d3_allowed"])
        self.assertEqual(
            plan["supported_phases"],
            ["tree-training", "posting-construction", "development-evaluation"],
        )
        self.assertNotIn("posting-construction", plan["blocked_phases"])

    def test_posting_plan_is_one_ephemeral_spot_worker_with_immutable_handoff(
        self,
    ) -> None:
        plan = build_launch_plan(
            phase="posting-construction",
            run_id="fixture-posting-run",
            source_commit=SOURCE_SHA,
        )

        self.assertEqual(plan["phase"], "posting-construction")
        self.assertEqual(plan["instance_count"], 1)
        self.assertEqual(plan["purchase_option"], "spot")
        self.assertEqual(plan["preflight_input_count"], 259)
        self.assertEqual(plan["execute_input_count"], 28_285)
        self.assertEqual(
            plan["parent_receipt_uri"],
            "s3://borsuk-bench-453182569524-euc1/research/"
            "v23-leaf-page-incidence/"
            "a321c473cb38a3b38c4757a50acf14e144b0441b0ca4bbbe7a8c7f3baaef78cc/"
            "v23-incidence-tree-20260831T120514Z/tree-receipt.json",
        )
        self.assertEqual(plan["page_roster_uri"], FROZEN_PAGE_ROSTER_URI)
        self.assertEqual(
            plan["supported_phases"],
            ["tree-training", "posting-construction", "development-evaluation"],
        )
        self.assertNotIn("posting-construction", plan["blocked_phases"])
        self.assertNotIn("development-evaluation", plan["blocked_phases"])
        self.assertFalse(plan["d3_allowed"])

    def test_later_phases_refuse_without_committed_immutable_manifests(self) -> None:
        for phase in ("holdout-binding", "holdout-evaluation"):
            with self.subTest(phase=phase), self.assertRaisesRegex(
                ValueError, "immutable phase manifest"
            ):
                build_launch_plan(
                    phase=phase,
                    run_id="fixture-run",
                    source_commit=SOURCE_SHA,
                )

    def test_launch_spec_is_one_time_spot_and_self_terminating(self) -> None:
        user_data = "#!/bin/bash\nshutdown -h now\n"
        spec = build_launch_spec(
            phase="tree-training",
            run_id="fixture-run",
            source_commit=SOURCE_SHA,
            user_data=user_data,
        )

        self.assertEqual(spec["MinCount"], 1)
        self.assertEqual(spec["MaxCount"], 1)
        self.assertEqual(spec["UserData"], user_data)
        self.assertEqual(
            spec["ClientToken"],
            "borsuk-v23-"
            + hashlib.sha256(f"{SOURCE_SHA}:fixture-run".encode()).hexdigest()[:48],
        )
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertEqual(
            spec["InstanceMarketOptions"]["SpotOptions"],
            {
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate",
            },
        )
        self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
        self.assertEqual(spec["MetadataOptions"]["HttpTokens"], "required")
        self.assertTrue(
            spec["BlockDeviceMappings"][0]["Ebs"]["DeleteOnTermination"]
        )
        tags = {
            item["Key"]: item["Value"]
            for item in spec["TagSpecifications"][0]["Tags"]
        }
        self.assertEqual(tags["Project"], "BorsukBenchmark")
        self.assertEqual(tags["Phase"], "tree-training")
        self.assertEqual(tags["AutoTerminate"], "true")

        with self.assertRaisesRegex(ValueError, "user data length"):
            build_launch_spec(
                phase="tree-training",
                run_id="fixture-run",
                source_commit=SOURCE_SHA,
                user_data="#!/bin/bash\nshutdown -h now\n" + "x" * 16_384,
            )

    def test_posting_launch_spec_tags_the_actual_phase(self) -> None:
        spec = build_launch_spec(
            phase="posting-construction",
            run_id="fixture-posting-run",
            source_commit=SOURCE_SHA,
            user_data="#!/bin/bash\nshutdown -h now\n",
        )
        tags = {
            item["Key"]: item["Value"]
            for item in spec["TagSpecifications"][0]["Tags"]
        }
        self.assertEqual(tags["Phase"], "posting-construction")
        self.assertEqual(tags["RunId"], "fixture-posting-run")

    def test_worker_runs_preflight_then_execute_and_publishes_only_terminal_evidence(
        self,
    ) -> None:
        worker = build_worker_script(
            phase="tree-training",
            run_id="fixture-run",
            source_commit=SOURCE_SHA,
            source_uri="s3://borsuk-evidence/source.tar",
            source_sha256="11" * 32,
            result_uri="s3://borsuk-evidence/incidence/fixture-run",
            spot_price_usd_per_hour="0.321",
        )

        self.assertIn("set -euo pipefail", worker)
        self.assertIn("trap finish EXIT", worker)
        self.assertIn("shutdown -h now", worker)
        self.assertIn("shutdown --poweroff +360", worker)
        self.assertLess(worker.index("shutdown --poweroff +360"), worker.index("aws s3 cp"))
        self.assertIn("cargo build --locked --release", worker)
        install_line = next(
            line for line in worker.splitlines() if line.startswith("dnf install -y ")
        )
        self.assertNotIn("curl", install_line.split())
        self.assertIn("curl --proto '=https'", worker)
        self.assertIn("export HOME=/root PATH=/root/.cargo/bin:$PATH", worker)
        self.assertNotIn("source /root/.cargo/env", worker)
        self.assertIn("v23_leaf_page_incidence_falsifier", worker)
        self.assertIn("--worker-tree", worker)
        self.assertIn("primary_evidence_attempted=0", worker)
        self.assertIn("primary_evidence_attempted=1", worker)
        self.assertIn(
            "binary.json|preflight-receipt.json|progress.json) continue",
            worker,
        )
        worker_source = inspect.getsource(worker_tree)
        self.assertLess(
            worker_source.index("preflight_policy"),
            worker_source.index("execute_policy"),
        )
        self.assertIn("MANIFEST_RELATIVE", worker_source)
        self.assertIn("run_phase(preflight_policy", worker_source)
        self.assertIn("run_phase(execute_policy", worker_source)
        self.assertIn("_stage(preflight_manifest", worker_source)
        self.assertIn("_stage(execute_manifest", worker_source)
        self.assertIn("MemoryMax=3G", worker)
        self.assertIn("MemorySwapMax=0", worker)
        self.assertIn("RuntimeMaxSec=16200", worker)
        self.assertIn(
            "scratch_root=/var/lib/borsuk-v23-incidence/scratch", worker
        )
        self.assertIn('mkdir -p "$evidence" "$scratch_root"', worker)
        self.assertIn('--setenv=TMPDIR="$scratch_root"', worker)
        self.assertIn("systemd-run --wait --collect", worker)
        self.assertIn('--working-directory="$workspace"', worker)
        self.assertIn('--setenv=PYTHONPATH="$workspace"', worker)
        self.assertIn(
            'find target -type f -path \'*/release/examples/'
            'v23_leaf_page_incidence_falsifier\' -perm -0100 -print -quit '
            '2>/dev/null || true',
            worker,
        )
        self.assertNotIn("find /data/target target", worker)
        self.assertIn("--offline-probe", worker)
        self.assertLess(worker.index("--offline-probe"), worker.index("--worker-tree"))
        self.assertIn("spot/instance-action", worker)
        self.assertIn("systemctl stop", worker)
        self.assertIn("ATTEMPT_COMPLETE.json", worker)
        self.assertIn("ATTEMPT_FAILED.json", worker)
        self.assertIn("publish_status=0", worker)
        self.assertIn("if [[ \"$publish_status\" -eq 0 ]]", worker)
        self.assertNotIn("phase-resource.json", worker)
        self.assertNotIn("MemoryPeak", worker)
        self.assertIn("ATTEMPT_INTERRUPTED.json", worker)
        self.assertIn("interruption-monitor-failed.json", worker)
        self.assertIn("incidence-executable", worker)
        self.assertIn("--output-uri-prefix \"$result_uri\"", worker)
        self.assertIn("--if-none-match '*'", worker)
        self.assertIn("--generate-cli-skeleton input", worker)
        self.assertLess(
            worker.index("--generate-cli-skeleton input"),
            worker.index('aws s3 cp "$source_uri"'),
        )
        self.assertNotIn("D3", worker)
        self.assertNotIn("production_bench", worker)
        self.assertNotIn("holdout-evaluation", worker)

        heredocs = []
        lines = worker.splitlines()
        for index, line in enumerate(lines):
            if line.endswith("<<'PY'"):
                end = lines.index("PY", index + 1)
                heredocs.append("\n".join(lines[index + 1 : end]) + "\n")
        self.assertGreaterEqual(len(heredocs), 3)
        for ordinal, program in enumerate(heredocs):
            with self.subTest(heredoc=ordinal):
                compile(program, f"worker-heredoc-{ordinal}", "exec")

    def test_bulk_manifest_generator_is_canonical_and_mode_exact(self) -> None:
        source = ROOT / "scripts/fixtures/v23_incidence_training_manifest.json"
        source_value = json.loads(source.read_bytes())
        with tempfile.TemporaryDirectory() as directory:
            preflight = Path(directory) / "preflight.json"
            execute = Path(directory) / "execute.json"
            _write_bulk_manifest(source, preflight, False)
            _write_bulk_manifest(source, execute, True)

            preflight_value = json.loads(preflight.read_bytes())
            execute_value = json.loads(execute.read_bytes())
            self.assertEqual(
                preflight_value["ordered_inputs"],
                [source_value["ordered_inputs"][1]],
            )
            self.assertEqual(execute_value, source_value)
            self.assertEqual(
                preflight.read_bytes(),
                json.dumps(
                    preflight_value, sort_keys=True, separators=(",", ":")
                ).encode()
                + b"\n",
            )
            self.assertNotEqual(
                hashlib.sha256(preflight.read_bytes()).hexdigest(),
                hashlib.sha256(execute.read_bytes()).hexdigest(),
            )

    def test_posting_bulk_manifest_preflight_keeps_authority_and_256_pages(
        self,
    ) -> None:
        construction = json.loads(
            (ROOT / "scripts/fixtures/v23_incidence_training_manifest.json").read_bytes()
        )
        identity = construction["ordered_inputs"][0]["identity"]

        def phase_object(role: str, ordinal: int) -> dict[str, object]:
            changed = dict(identity)
            changed["role"] = role
            changed["uri"] = f"s3://fixture/{role}-{ordinal}"
            return {"authority_kind": "phase-object", "identity": changed}

        fixed = [
            phase_object("parent-receipt", 0),
            phase_object("incidence-tree", 0),
            phase_object("page-roster", 0),
        ]
        pages = [
            phase_object(f"page-body-{ordinal:05}", ordinal)
            for ordinal in range(300)
        ]
        posting = dict(construction)
        posting["phase"] = "posting-construction"
        posting["ordered_inputs"] = fixed + pages
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "posting.json"
            preflight = root / "preflight.json"
            execute = root / "execute.json"
            source.write_bytes(
                json.dumps(posting, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            )

            _write_bulk_manifest(source, preflight, False)
            _write_bulk_manifest(source, execute, True)

            preflight_value = json.loads(preflight.read_bytes())
            execute_value = json.loads(execute.read_bytes())
            self.assertEqual(preflight_value["ordered_inputs"], fixed + pages[:256])
            self.assertEqual(execute_value, posting)
            self.assertEqual(
                preflight.read_bytes(),
                json.dumps(
                    preflight_value, sort_keys=True, separators=(",", ":")
                ).encode()
                + b"\n",
            )

    def test_policy_builder_registers_distinct_manifest_roles_and_runtime_closure(
        self,
    ) -> None:
        source = (ROOT / "scripts/fixtures/v23_incidence_training_manifest.json").resolve()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bulk = root / "execute-manifest.json"
            receipt = root / "staging-receipt.json"
            staging = root / "staging"
            scratch = root / "scratch"
            output = root / "output"
            _write_bulk_manifest(source, bulk, True)
            receipt.write_text("{}\n", encoding="utf-8")
            staging.mkdir()
            scratch.mkdir()
            output.mkdir()
            binary = Path("/bin/true").resolve()
            binary_sha = hashlib.sha256(binary.read_bytes()).hexdigest()

            policy = _phase_policy(
                phase="tree-training",
                binary=binary,
                binary_sha256=binary_sha,
                manifest=source,
                bulk_manifest=bulk,
                staging=staging,
                staging_receipt=receipt,
                scratch=scratch,
                output=output,
                preflight_receipt=None,
            )
            validate_phase_inputs(policy)
            manifests = policy.inputs[:2]
            self.assertEqual(
                [mount.role for mount in manifests],
                ["construction-manifest", "bulk-manifest"],
            )
            self.assertNotEqual(manifests[0].source, manifests[1].source)
            self.assertNotEqual(manifests[0].uri, manifests[1].uri)

            preflight_receipt = root / "preflight-receipt.json"
            preflight_receipt.write_text("{}\n", encoding="utf-8")
            execute_policy = _phase_policy(
                phase="tree-training",
                binary=binary,
                binary_sha256=binary_sha,
                manifest=source,
                bulk_manifest=bulk,
                staging=staging,
                staging_receipt=receipt,
                scratch=scratch,
                output=output,
                preflight_receipt=preflight_receipt,
            )
            validate_phase_inputs(execute_policy)
            self.assertIsNone(execute_policy.parent_receipt_sha256)
            self.assertEqual(
                [mount.role for mount in execute_policy.inputs][-1],
                "preflight-receipt",
            )

    def test_posting_policy_binds_phase_manifest_and_parent_tree_receipt(self) -> None:
        construction = json.loads(
            (ROOT / "scripts/fixtures/v23_incidence_training_manifest.json").read_bytes()
        )
        construction["phase"] = "posting-construction"
        construction["parent_receipt_sha256"] = "ab" * 32
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "posting-manifest.json"
            bulk = root / "posting-bulk.json"
            receipt = root / "staging-receipt.json"
            preflight_receipt = root / "preflight-receipt.json"
            staging = root / "staging"
            scratch = root / "scratch"
            output = root / "output"
            manifest.write_bytes(
                json.dumps(
                    construction, sort_keys=True, separators=(",", ":")
                ).encode()
                + b"\n"
            )
            bulk.write_bytes(manifest.read_bytes())
            receipt.write_text("{}\n", encoding="utf-8")
            preflight_receipt.write_text("{}\n", encoding="utf-8")
            for path in (staging, scratch, output):
                path.mkdir()
            binary = Path("/bin/true").resolve()
            binary_sha = hashlib.sha256(binary.read_bytes()).hexdigest()

            preflight = _phase_policy(
                phase="posting-construction",
                binary=binary,
                binary_sha256=binary_sha,
                manifest=manifest,
                bulk_manifest=bulk,
                staging=staging,
                staging_receipt=receipt,
                scratch=scratch,
                output=output,
                preflight_receipt=None,
            )
            execute = _phase_policy(
                phase="posting-construction",
                binary=binary,
                binary_sha256=binary_sha,
                manifest=manifest,
                bulk_manifest=bulk,
                staging=staging,
                staging_receipt=receipt,
                scratch=scratch,
                output=output,
                preflight_receipt=preflight_receipt,
            )

            validate_phase_inputs(preflight)
            validate_phase_inputs(execute)
            self.assertEqual(preflight.phase, "posting-construction")
            self.assertEqual(preflight.parent_receipt_sha256, "ab" * 32)
            self.assertEqual(
                [item.role for item in preflight.inputs],
                ["phase-manifest", "bulk-manifest", "staging-receipt"],
            )
            self.assertEqual(
                [item.role for item in execute.inputs],
                [
                    "phase-manifest",
                    "bulk-manifest",
                    "staging-receipt",
                    "preflight-receipt",
                ],
            )
            self.assertEqual(
                preflight.phase_argv[0], "--preflight-posting-construction"
            )
            self.assertEqual(execute.phase_argv[0], "--execute-posting-construction")

    def test_source_archive_contains_exact_commit_marker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "source.tar"
            digest = _build_source_archive(SOURCE_SHA, archive)
            self.assertRegex(digest, r"^[0-9a-f]{64}$")
            marker = subprocess.run(
                ["tar", "-xOf", str(archive), ".borsuk-source-commit"],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(marker.stdout, SOURCE_SHA)

    def test_terminal_marker_is_canonical_and_bound_to_attempt(self) -> None:
        value = {
            "claim_eligible": False,
            "phase": "tree-training",
            "run_id": "fixture-run",
            "schema": "borsuk-v23-incidence-attempt-failed-v1",
            "source_archive_sha256": "11" * 32,
            "source_commit": SOURCE_SHA,
            "status": "failed",
            "worker_exit": 1,
        }
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        self.assertEqual(
            _validate_terminal_bytes(raw, "fixture-run", SOURCE_SHA, "tree-training"),
            "failed",
        )
        for mutation in (
            raw[:-1],
            raw.replace(b"fixture-run", b"other-run"),
            raw.replace(SOURCE_SHA.encode(), ("f" * 40).encode()),
            raw.replace(b'"claim_eligible":false', b'"claim_eligible":true'),
        ):
            with self.subTest(mutation=mutation[:100]), self.assertRaises(ValueError):
                _validate_terminal_bytes(
                    mutation, "fixture-run", SOURCE_SHA, "tree-training"
                )

        interrupted = {
            "claim_eligible": False,
            "phase": "tree-training",
            "run_id": "fixture-run",
            "schema": "borsuk-v23-incidence-attempt-interrupted-v1",
            "source_archive_sha256": "11" * 32,
            "source_commit": SOURCE_SHA,
            "status": "interrupted",
        }
        interrupted_raw = (
            json.dumps(interrupted, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        self.assertEqual(
            _validate_terminal_bytes(
                interrupted_raw, "fixture-run", SOURCE_SHA, "tree-training"
            ),
            "interrupted",
        )

        complete = {
            "binary": {"encoded_bytes": 1, "sha256": "22" * 32},
            "claim_eligible": False,
            "incidence_tree": {"encoded_bytes": 1, "sha256": "33" * 32},
            "phase": "tree-training",
            "purchase_option": "spot",
            "receipt": {"encoded_bytes": 1, "sha256": "44" * 32},
            "run_id": "fixture-run",
            "schema": "borsuk-v23-incidence-attempt-complete-v1",
            "source_archive_sha256": "11" * 32,
            "source_commit": SOURCE_SHA,
            "spot_price_usd_per_hour": "nan",
            "status": "complete",
        }
        complete_raw = (
            json.dumps(complete, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        with self.assertRaisesRegex(ValueError, "Spot price"):
            _validate_terminal_bytes(
                complete_raw, "fixture-run", SOURCE_SHA, "tree-training"
            )

    def test_posting_terminal_binds_both_posting_artifacts(self) -> None:
        value = {
            "binary": {"encoded_bytes": 1, "sha256": "22" * 32},
            "claim_eligible": False,
            "incidence_postings_one": {
                "encoded_bytes": 2,
                "sha256": "33" * 32,
            },
            "incidence_postings_two": {
                "encoded_bytes": 3,
                "sha256": "44" * 32,
            },
            "phase": "posting-construction",
            "purchase_option": "spot",
            "receipt": {"encoded_bytes": 4, "sha256": "55" * 32},
            "run_id": "fixture-posting-run",
            "schema": "borsuk-v23-incidence-attempt-complete-v1",
            "source_archive_sha256": "11" * 32,
            "source_commit": SOURCE_SHA,
            "spot_price_usd_per_hour": "0.321",
            "status": "complete",
        }
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"

        self.assertEqual(
            _validate_terminal_bytes(
                raw,
                "fixture-posting-run",
                SOURCE_SHA,
                "posting-construction",
            ),
            "complete",
        )
        del value["incidence_postings_two"]
        mutation = (
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        )
        with self.assertRaisesRegex(ValueError, "complete terminal authority"):
            _validate_terminal_bytes(
                mutation,
                "fixture-posting-run",
                SOURCE_SHA,
                "posting-construction",
            )

    def test_development_terminal_binds_result_latency_and_receipt(self) -> None:
        value = {
            "binary": {"encoded_bytes": 1, "sha256": "22" * 32},
            "claim_eligible": False,
            "development_latency": {"encoded_bytes": 2, "sha256": "33" * 32},
            "development_result": {"encoded_bytes": 3, "sha256": "44" * 32},
            "phase": "development-evaluation",
            "purchase_option": "spot",
            "receipt": {"encoded_bytes": 4, "sha256": "55" * 32},
            "run_id": "fixture-development-run",
            "schema": "borsuk-v23-incidence-attempt-complete-v1",
            "source_archive_sha256": "11" * 32,
            "source_commit": SOURCE_SHA,
            "spot_price_usd_per_hour": "0.321",
            "status": "complete",
        }
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        self.assertEqual(
            _validate_terminal_bytes(
                raw,
                "fixture-development-run",
                SOURCE_SHA,
                "development-evaluation",
            ),
            "complete",
        )

    def test_spot_price_is_scoped_to_the_registered_subnet_zone(self) -> None:
        with patch(
            "scripts.launch_v23_incidence_spot._aws",
            side_effect=["eu-central-1a", "0.321"],
        ) as aws:
            self.assertEqual(_spot_price(), "0.321")
        self.assertIn("describe-subnets", aws.call_args_list[0].args[0])
        price_arguments = aws.call_args_list[1].args[0]
        self.assertIn("describe-spot-price-history", price_arguments)
        self.assertEqual(
            price_arguments[price_arguments.index("--availability-zone") + 1],
            "eu-central-1a",
        )
        self.assertEqual(str(_maximum_compute_cost("0.321")), "1.926")
        with self.assertRaisesRegex(ValueError, "Spot price"):
            _maximum_compute_cost("not-a-price")

    def test_tree_receipt_is_rewritten_to_the_immutable_handoff_uri(self) -> None:
        import blake3

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tree = root / "incidence-tree-a.bin"
            receipt = root / "receipt.json"
            tree.write_bytes(b"tree")
            digest = blake3.blake3(b"tree").hexdigest()
            value = {
                "outputs": [
                    {
                        "digest": digest,
                        "digest_algorithm": "blake3",
                        "encoded_bytes": 4,
                        "generation": "content-" + digest,
                        "role": "incidence-tree",
                        "uri": f"file://{tree}",
                    }
                ]
            }
            receipt.write_bytes(
                json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            )
            _rewrite_tree_receipt_uri(
                receipt,
                tree,
                "s3://borsuk-evidence/incidence/source/run/incidence-tree.bin",
            )
            rewritten = json.loads(receipt.read_bytes())
            self.assertEqual(
                rewritten["outputs"][0]["uri"],
                "s3://borsuk-evidence/incidence/source/run/incidence-tree.bin",
            )
            self.assertEqual(rewritten["outputs"][0]["digest"], digest)
            with self.assertRaisesRegex(ValueError, "output URI"):
                _rewrite_tree_receipt_uri(
                    receipt,
                    tree,
                    "s3://borsuk-evidence/incidence/source/run/other.bin",
                )

    def test_posting_receipt_rewrite_authenticates_both_artifacts(self) -> None:
        import blake3

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            receipt_path = root / "receipt.json"
            artifacts = {
                "incidence-postings-one": root / "one-content.bin",
                "incidence-postings-two": root / "two-content.bin",
            }
            outputs = []
            for role, path in artifacts.items():
                payload = f"{role}-payload".encode()
                path.write_bytes(payload)
                digest = blake3.blake3(payload).hexdigest()
                outputs.append(
                    {
                        "digest": digest,
                        "digest_algorithm": "blake3",
                        "encoded_bytes": len(payload),
                        "generation": f"content-{digest}",
                        "role": role,
                        "uri": f"file://{path}",
                    }
                )
            receipt = {"outputs": outputs, "schema": "fixture-receipt"}
            receipt_path.write_bytes(
                json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            )

            rewritten = _rewrite_posting_receipt_uris(
                receipt_path,
                root,
                "s3://borsuk-evidence/run",
            )

            value = json.loads(receipt_path.read_bytes())
            self.assertEqual(rewritten, artifacts)
            self.assertEqual(
                [item["uri"] for item in value["outputs"]],
                [
                    "s3://borsuk-evidence/run/incidence-postings-one.bin",
                    "s3://borsuk-evidence/run/incidence-postings-two.bin",
                ],
            )
            original_receipt_bytes = (
                json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            )
            artifacts["incidence-postings-two"].write_bytes(b"changed")
            receipt_path.write_bytes(original_receipt_bytes)
            with self.assertRaisesRegex(ValueError, "posting output URI or bytes"):
                _rewrite_posting_receipt_uris(
                    receipt_path,
                    root,
                    "s3://borsuk-evidence/run",
                )

    def test_dry_run_has_no_aws_side_effect(self) -> None:
        result = subprocess.run(
            [
                "python3",
                str(LAUNCHER),
                "--phase",
                "tree-training",
                "--run-id",
                "fixture-run",
                "--dry-run",
            ],
            cwd=ROOT,
            env={**os.environ, "BORSUK_SOURCE_COMMIT": SOURCE_SHA},
            check=True,
            capture_output=True,
            text=True,
        )
        plan = json.loads(result.stdout)
        self.assertEqual(plan["run_id"], "fixture-run")
        self.assertEqual(plan["source_commit"], SOURCE_SHA)
        self.assertEqual(plan["phase"], "tree-training")


if __name__ == "__main__":
    unittest.main()
