import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "docs/research/storage-object-roles.csv"
SCRIPT = ROOT / "scripts/storage_layout_inventory.py"


class StorageLayoutInventoryTests(unittest.TestCase):
    def test_inventory_declares_every_production_object_role(self):
        with INVENTORY.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        required = {
            "catalog",
            "wal_run",
            "lane_head",
            "commit_marker",
            "routing_page",
            "graph_index",
            "normal_segment",
            "product_codes",
            "exact_vectors",
            "filter_index",
            "lexical_block",
            "late_interaction",
            "tombstone",
            "id_directory",
        }
        self.assertEqual({row["object_role"] for row in rows}, required)
        self.assertEqual(len(rows), len(required))
        self.assertTrue(all(row["current_format"] for row in rows))
        self.assertTrue(all(row["access_patterns"] for row in rows))

    def test_cli_validates_and_emits_stable_json(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "inventory.json"
            completed = subprocess.run(
                [
                    "python3",
                    str(SCRIPT),
                    "validate",
                    str(INVENTORY),
                    "--output",
                    str(output),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            payload = json.loads(output.read_text())
            self.assertEqual(
                [row["object_role"] for row in payload],
                sorted(row["object_role"] for row in payload),
            )
            self.assertEqual(len(payload), 14)


if __name__ == "__main__":
    unittest.main()
