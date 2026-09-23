"""Reconstruct exactly the old authenticated code-wave candidate rows."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from scripts import native_two_bit_returned_replay as replay
from scripts.native_rotated_two_bit_codes import _fit_records


def fixture():
    first = (0, 1, 0, 408, "a" * 64)
    second = (1, 2, 408, 408, "b" * 64)
    codes = SimpleNamespace(page_row_counts=(2, 2), group_ranges=(first, second))
    sample = SimpleNamespace(group_ranges=(second, first), code_gets=2, code_bytes=816)
    return sample, codes


class SelectedPositionsTests(unittest.TestCase):
    def test_returns_physical_rows_in_selected_group_order(self) -> None:
        sample, codes = fixture()
        self.assertEqual(replay.selected_positions(sample, codes), (2, 3, 0, 1))

    def test_rejects_repeated_or_corrupt_group(self) -> None:
        sample, codes = fixture()
        sample.group_ranges = (codes.group_ranges[0], codes.group_ranges[0])
        with self.assertRaisesRegex(ValueError, "group"):
            replay.selected_positions(sample, codes)
        sample.group_ranges = ((0, 1, 0, 408, "0" * 64),)
        sample.code_gets = 1
        sample.code_bytes = 408
        with self.assertRaisesRegex(ValueError, "group"):
            replay.selected_positions(sample, codes)

    def test_rejects_budget_or_accounting_drift(self) -> None:
        sample, codes = fixture()
        sample.code_bytes = 817
        with self.assertRaisesRegex(ValueError, "budget"):
            replay.selected_positions(sample, codes)

    def test_scores_and_returns_from_same_fetched_rows(self) -> None:
        vectors = np.zeros((3, 768), dtype=np.float32)
        vectors[1, 0] = 1.0
        vectors[2, 0] = 10.0
        codes = SimpleNamespace(
            page_row_counts=(3,),
            group_ranges=((0, 1, 0, 608, "a" * 64),),
            records=_fit_records(vectors.astype(np.float64), rotation_seed=20260923),
            mean=np.zeros(768, dtype=np.float32),
            rotation_seed=20260923,
            source_ordinals=(0, 1, 2),
        )
        sample = SimpleNamespace(group_ranges=codes.group_ranges, code_gets=1,
                                 code_bytes=608, query_ordinal=0,
                                 grouped_hits_at_100=2)
        result = replay.replay_query(
            sample, codes, vectors[0], vectors,
            (b"a", b"b", b"c"), (b"a", b"b"), top_k=2,
        )
        self.assertEqual(result["candidate_rows"], 3)
        self.assertEqual(result["grouped_hits"], 2)
        self.assertEqual(result["exact_hits"], 2)
        self.assertEqual(result["two_bit_hits"], 2)
        self.assertEqual(result["stored_norm_hits"], 2)
        self.assertEqual(result["stored_norm_paired_loss"], 0)

    def test_stored_norm_must_equal_source_centered_norm(self) -> None:
        vectors = np.zeros((2, 768), dtype=np.float32)
        vectors[1, 0] = 1.0
        codes = SimpleNamespace(
            records=_fit_records(vectors.astype(np.float64), rotation_seed=20260923),
            mean=np.zeros(768, dtype=np.float32), source_ordinals=(0, 1),
        )
        replay.verify_stored_norms(codes, vectors)
        codes.records[1, 196:200] = np.frombuffer(
            np.asarray([2.0], dtype="<f4").tobytes(), dtype=np.uint8,
        )
        with self.assertRaisesRegex(ValueError, "stored norm"):
            replay.verify_stored_norms(codes, vectors)

    def test_summary_keeps_paired_loss_separate_from_marginal_tail(self) -> None:
        cases = [
            {"query_ordinal": 0, "candidate_rows": 3, "code_gets": 1,
             "code_bytes": 608, "grouped_hits": 2,
             "exact_hits": 2, "two_bit_hits": 1,
             "stored_norm_hits": 2, "stored_norm_paired_loss": 0,
             "paired_loss": 1},
            {"query_ordinal": 1, "candidate_rows": 3, "code_gets": 1,
             "code_bytes": 608, "grouped_hits": 1,
             "exact_hits": 1, "two_bit_hits": 1,
             "stored_norm_hits": 1, "stored_norm_paired_loss": 0,
             "paired_loss": 0},
        ]
        summary = replay.summarize_results(cases, expected_queries=2, top_k=2)
        self.assertEqual(summary["two_bit_recall_at_k_ppm"], 500_000)
        self.assertEqual(summary["exact_recall_at_k_ppm"], 750_000)
        self.assertEqual(summary["stored_norm_recall_at_k_ppm"], 750_000)
        self.assertEqual(summary["two_bit_p05_hits"], 1)
        self.assertEqual(summary["p95_paired_loss_hits"], 1)
        cases[0]["exact_hits"] = 0
        cases[0]["paired_loss"] = -1
        cases[0]["stored_norm_paired_loss"] = -2
        with self.assertRaisesRegex(ValueError, "exact control"):
            replay.summarize_results(cases, expected_queries=2, top_k=2)

    def test_terminal_receipt_binds_closed_artifact_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifacts = {}
            for role, filename in (("groups", "groups.bin"), ("mean", "mean.bin"),
                                   ("code-seal", "seal.json"),
                                   ("evidence", "evidence.json")):
                body = filename.encode()
                (root / filename).write_bytes(body)
                artifacts[role] = {"role": role, "uri": "s3://closed/" + filename,
                                   "sha256": hashlib.sha256(body).hexdigest(),
                                   "encoded_bytes": len(body)}
            terminal = {"status": "complete", "exit_code": 0,
                        "artifacts": artifacts}
            body = (json.dumps(terminal, sort_keys=True, separators=(",", ":"))
                    + "\n").encode()
            (root / "terminal.json").write_bytes(body)
            digest = hashlib.sha256(body).hexdigest()
            self.assertEqual(set(replay.authenticate_legacy_artifacts(root, digest)),
                             set(artifacts))
            (root / "groups.bin").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "artifact"):
                replay.authenticate_legacy_artifacts(root, digest)

    def test_loaded_replay_writes_canonical_returned_evidence(self) -> None:
        vectors = np.zeros((3, 768), dtype=np.float32)
        vectors[1, 0] = 1.0
        vectors[2, 0] = 10.0
        codes = SimpleNamespace(
            page_row_counts=(3,),
            group_ranges=((0, 1, 0, 608, "a" * 64),),
            records=_fit_records(vectors.astype(np.float64), rotation_seed=20260923),
            mean=np.zeros(768, dtype=np.float32), rotation_seed=20260923,
            source_ordinals=(0, 1, 2),
        )
        sample = SimpleNamespace(group_ranges=codes.group_ranges, code_gets=1,
                                 code_bytes=608, query_ordinal=0,
                                 grouped_hits_at_100=2)
        with tempfile.TemporaryDirectory() as temporary:
            out = Path(temporary)
            summary = replay.replay_loaded(
                vectors[:1], vectors, (b"a", b"b", b"c"),
                ((b"a", b"b"),), codes, (sample,), out, top_k=2,
            )
            self.assertEqual(summary["two_bit_recall_at_k_ppm"], 1_000_000)
            evidence = json.loads((out / "returned-evidence.json").read_bytes())
            self.assertEqual(evidence["samples"][0]["two_bit_hits"], 2)
            self.assertEqual(evidence["samples"][0]["stored_norm_hits"], 2)
            self.assertEqual(
                (out / "returned-result.json").read_bytes(),
                (json.dumps(json.loads((out / "returned-result.json").read_bytes()),
                            sort_keys=True, separators=(",", ":")) + "\n").encode(),
            )

    def test_closed_replay_checks_terminal_before_source_load(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "terminal.json").write_bytes(b"{}\n")
            with self.assertRaisesRegex(ValueError, "terminal identity"):
                replay.run_closed_replay(root, root)
