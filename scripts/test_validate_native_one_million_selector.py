"""Independent source reconstruction and evidence tamper checks."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.native_one_million_group_selector import build_selector
from scripts.native_one_million_selector_evaluation import evaluate_selector
from scripts.test_native_one_million_group_selector import _fixture as source_fixture
from scripts.test_native_one_million_selector_evaluation import (
    _fixture as evaluation_fixture,
)
from scripts.validate_native_one_million_selector import (
    rebuild_source_selector,
    replay_selector_evidence,
)


class IndependentSelectorTests(unittest.TestCase):
    def test_rebuilds_source_centroid_bytes_and_membership(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = source_fixture(root)
            build_selector(root, root / "selector", identities, batch_rows=2)
            rebuilt = rebuild_source_selector(root, identities, batch_rows=3)
            self.assertEqual(rebuilt[0], (root / "selector/centroids.bin").read_bytes())
            self.assertEqual(rebuilt[1], (root / "selector/membership.bin").read_bytes())
            self.assertEqual(rebuilt[2], json.loads((root / "selector/seal.json").read_bytes())["page_order_sha256"])
            self.assertEqual(rebuilt[3], json.loads((root / "selector/seal.json").read_bytes())["groups"])

    def test_replays_query_plans_and_rejects_altered_hits(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifact, identities = evaluation_fixture(root)
            evaluate_selector(artifact, root / "queries.parquet", root / "truth.parquet", root / "out", identities, query_count=1)
            replay_selector_evidence(artifact, root / "queries.parquet", root / "truth.parquet", root / "out", identities, query_count=1)
            evidence_path = root / "out/evidence.json"
            evidence = json.loads(evidence_path.read_bytes())
            evidence["samples"][0]["hits_at_100"] = 97
            evidence_path.write_text(json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n")
            with self.assertRaisesRegex(ValueError, "evidence|result"):
                replay_selector_evidence(artifact, root / "queries.parquet", root / "truth.parquet", root / "out", identities, query_count=1)


if __name__ == "__main__":
    unittest.main()
