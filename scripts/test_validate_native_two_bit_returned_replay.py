"""Independent returned-rank replay on the fixed fetched-row roster."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from scripts.native_rotated_two_bit_codes import _fit_records
from scripts.native_two_bit_returned_replay import replay_loaded
from scripts.validate_native_two_bit_returned_replay import (
    run_closed_validation,
    validate_loaded,
)


class ReturnedValidationTests(unittest.TestCase):
    def test_closed_validation_rejects_wrong_terminal_before_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "terminal.json").write_bytes(b"{}\n")
            with self.assertRaisesRegex(ValueError, "terminal identity"):
                run_closed_validation(root, root)

    def test_replays_ranking_and_rejects_coordinated_hit_change(self) -> None:
        vectors = np.zeros((3, 768), dtype=np.float32)
        vectors[1, 0] = 1.0
        vectors[2, 0] = 10.0
        codes = SimpleNamespace(
            page_row_counts=(3,), group_ranges=((0, 1, 0, 608, "a" * 64),),
            records=_fit_records(vectors.astype(np.float64), rotation_seed=20260923),
            mean=np.zeros(768, dtype=np.float32), rotation_seed=20260923,
            source_ordinals=(0, 1, 2),
        )
        sample = SimpleNamespace(group_ranges=codes.group_ranges, code_gets=1,
                                 code_bytes=608, query_ordinal=0,
                                 grouped_hits_at_100=2)
        ids = (b"a", b"b", b"c")
        truth = ((b"a", b"b"),)
        with tempfile.TemporaryDirectory() as temporary:
            out = Path(temporary)
            replay_loaded(vectors[:1], vectors, ids, truth, codes,
                          (sample,), out, top_k=2)
            with self.assertRaisesRegex(ValueError, "authority"):
                validate_loaded(vectors[:1], vectors, ids, truth + truth,
                                codes, (sample,), out, top_k=2)
            self.assertTrue(validate_loaded(vectors[:1], vectors, ids, truth,
                                            codes, (sample,), out, top_k=2)["valid"])
            original_result = (out / "returned-result.json").read_bytes()
            wrong_decision = json.loads(original_result)
            wrong_decision["decision"] = "advance-fidelity-only"
            (out / "returned-result.json").write_bytes(
                (json.dumps(wrong_decision, sort_keys=True,
                            separators=(",", ":")) + "\n").encode()
            )
            with self.assertRaisesRegex(ValueError, "decision"):
                validate_loaded(vectors[:1], vectors, ids, truth,
                                codes, (sample,), out, top_k=2)
            (out / "returned-result.json").write_bytes(original_result)
            evidence = json.loads((out / "returned-evidence.json").read_bytes())
            evidence["samples"][0]["stored_norm_hits"] = 1
            evidence["samples"][0]["stored_norm_paired_loss"] = 1
            evidence_body = (json.dumps(evidence, sort_keys=True,
                                        separators=(",", ":")) + "\n").encode()
            (out / "returned-evidence.json").write_bytes(evidence_body)
            result = json.loads((out / "returned-result.json").read_bytes())
            result["evidence_sha256"] = hashlib.sha256(evidence_body).hexdigest()
            (out / "returned-result.json").write_bytes(
                (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
            )
            with self.assertRaisesRegex(ValueError, "returned replay"):
                validate_loaded(vectors[:1], vectors, ids, truth, codes,
                                (sample,), out, top_k=2)
