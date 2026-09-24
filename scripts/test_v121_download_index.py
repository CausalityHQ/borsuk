"""V120 index receipt checks for the later untouched quality consumer."""

import json
import tempfile
import unittest
from pathlib import Path

from scripts.v121_download_index import (
    REQUIRED, V119_SOURCE_SHA256, validate_terminal, verify_bindings,
)


class IndexConsumerTests(unittest.TestCase):
    def test_rejects_an_incomplete_or_missing_source_artifact(self) -> None:
        artifacts = {name: {"bytes": 1, "sha256": "a" * 64} for name in REQUIRED}
        terminal = {"schema": "borsuk-v120-source-index-spot-v1",
                    "source_commit": "b919685cf1db6c14912c2118d5226c0fb426226b",
                    "status": "complete", "phase": "complete", "exit_code": 0,
                    "artifacts": artifacts}
        self.assertEqual(set(validate_terminal(terminal)), set(REQUIRED))
        terminal["status"] = "failed"
        with self.assertRaisesRegex(ValueError, "terminal"):
            validate_terminal(terminal)
        terminal["status"] = "complete"
        del artifacts["built/sq8.bin"]
        with self.assertRaisesRegex(ValueError, "artifact"):
            validate_terminal(terminal)

    def test_rejects_a_router_bound_to_another_sq8_body(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("built", "router", "mirror", "authority"):
                (root / name).mkdir()
            (root / "built/manifest.json").write_text(json.dumps({
                "source_sha256": V119_SOURCE_SHA256, "layout_sha256": "b" * 64,
                "sq8_sha256": "c" * 64, "rows": 9_990_000, "dimensions": 96,
                "query_or_truth_used": False,
            }))
            (root / "router/manifest.json").write_text(json.dumps({
                "schema": "borsuk-source-router-v2",
                "source_sha256": V119_SOURCE_SHA256,
                "layout_sha256": "b" * 64, "sq8_sha256": "d" * 64,
                "geometry": {"rows": 9_990_000, "dimensions": 96,
                             "page_rows": 256, "blocks_per_page": 2,
                             "subspaces": 64, "pq_width": 2,
                             "pq_partition": "balanced_floor_v1"},
            }))
            with self.assertRaisesRegex(ValueError, "router"):
                verify_bindings(root)


if __name__ == "__main__":
    unittest.main()
