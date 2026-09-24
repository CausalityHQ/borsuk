"""V183 sealed plan boundary and lower-bound score accounting."""

import hashlib
import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from scripts.v183_source_physical_admission import (
    COUNT, FIRST, SCHEMA, V182_COMMIT, canonical, evaluate, plan, sha256,
)


class SourcePhysicalAdmissionTests(unittest.TestCase):
    def test_plan_seals_before_labels_and_evaluation_checks_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "v182"
            output = root / "v183"
            source.mkdir()
            with (source / "features.jsonl").open("x") as stream:
                for ordinal in range(FIRST, FIRST + COUNT):
                    stream.write(canonical({
                        "ordinal": ordinal, "source_id": ordinal,
                        "mandatory_units": [0], "ranked_units": [0, 1, 2],
                    }))
            (source / "fit-seal.json").write_text("{}\n")
            labels = "".join(canonical({
                "ordinal": ordinal, "source_id": ordinal,
                "candidate_hits": 1,
                "candidate_truth_by_unit": [[0, 1]],
            }) for ordinal in range(FIRST, FIRST + COUNT))
            lines = labels.splitlines(keepends=True)
            fit_labels = "".join(lines[:COUNT // 2]).encode()
            holdout_labels = "".join(lines[COUNT // 2:]).encode()
            artifacts = {
                "out/features.jsonl": sha256(source / "features.jsonl"),
                "out/fit-seal.json": sha256(source / "fit-seal.json"),
                "out/fit-labels.jsonl": hashlib.sha256(fit_labels).hexdigest(),
                "out/holdout-labels.jsonl": hashlib.sha256(holdout_labels).hexdigest(),
            }
            (source / "terminal.json").write_text(canonical({
                "status": "complete", "exit_code": 0,
                "source_commit": V182_COMMIT,
                "artifacts": {name: {"sha256": digest}
                              for name, digest in artifacts.items()},
            }))
            terminal_sha = sha256(source / "terminal.json")
            args = Namespace(v182=source, output=output, plan_sha256="")
            with patch("scripts.v183_source_physical_admission.V182_TERMINAL_SHA",
                       terminal_sha), patch(
                    "scripts.v183_source_physical_admission.ROWS", 1024):
                plan(args)
                seal = json.loads((output / "plan-seal.json").read_text())
                self.assertIs(seal["truth_opened"], False)
                self.assertFalse((source / "fit-labels.jsonl").exists())
                (source / "fit-labels.jsonl").write_bytes(fit_labels)
                (source / "holdout-labels.jsonl").write_bytes(holdout_labels)
                args.plan_sha256 = sha256(output / "plan-seal.json")
                evaluate(args)
            summary = json.loads((output / "summary.json").read_text())
            self.assertEqual(summary["schema"], SCHEMA + "-summary")
            self.assertEqual(summary["results"]["holdout"]["v155_mean"]["lower_hits"],
                             COUNT // 2)
            self.assertEqual(summary["decision"],
                             "inconclusive-outside-candidate-truth")
            with (output / "plans.jsonl").open("a") as stream:
                stream.write("{}\n")
            with patch("scripts.v183_source_physical_admission.V182_TERMINAL_SHA",
                       terminal_sha), patch(
                    "scripts.v183_source_physical_admission.ROWS", 1024):
                with self.assertRaises(ValueError):
                    evaluate(args)


if __name__ == "__main__":
    unittest.main()
