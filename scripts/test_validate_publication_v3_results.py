import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.publication_v3_protocol import (
    build_schedule_document,
    canonical_json_bytes,
    validate_manifest,
)
from scripts.test_publication_v3_protocol import paid_ready_v3_manifest
from scripts.validate_publication_v3_results import validate_structural_tree

ROOT = Path(__file__).resolve().parents[1]
PROTOCOL = ROOT / "scripts" / "publication_v3_protocol.py"


def canonical_write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")


def structural_manifest(source_archive: bytes) -> dict[str, object]:
    manifest = paid_ready_v3_manifest()
    manifest["source"]["archive_sha256"] = hashlib.sha256(source_archive).hexdigest()
    return validate_manifest(manifest)


def write_structural_fixture(root: Path) -> dict[str, object]:
    archive = b"publication-v3-structural-source\n"
    manifest = structural_manifest(archive)
    schedule = build_schedule_document(manifest)
    canonical_write(root / "manifest.json", manifest)
    canonical_write(root / "schedule.json", schedule)
    canonical_write(root / "environment.json", manifest["environment_contract"])
    (root / "source-archive.tar.gz").write_bytes(archive)
    for cell in schedule["cells"]:
        cell_root = root / "cells" / cell["cell_id"]
        protocol_bytes = canonical_json_bytes(cell) + b"\n"
        cell_root.mkdir(parents=True)
        (cell_root / "protocol.json").write_bytes(protocol_bytes)
        canonical_write(
            cell_root / "CELL_COMPLETE",
            {
                "schema_version": 1,
                "status": "complete",
                "cell_id": cell["cell_id"],
                "manifest_sha256": schedule["manifest_sha256"],
                "protocol_sha256": hashlib.sha256(protocol_bytes).hexdigest(),
            },
        )
    schedule_bytes = canonical_json_bytes(schedule) + b"\n"
    canonical_write(
        root / "PUBLICATION_V3_COMPLETE",
        {
            "schema_version": 1,
            "status": "complete",
            "manifest_sha256": schedule["manifest_sha256"],
            "schedule_sha256": hashlib.sha256(schedule_bytes).hexdigest(),
            "complete_cells": len(schedule["cells"]),
        },
    )
    return schedule


class ValidatePublicationV3ResultsTests(unittest.TestCase):
    def test_structural_replay_accepts_exact_complete_tree(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule = write_structural_fixture(root)
            summary = validate_structural_tree(root)
            self.assertEqual(
                summary,
                {
                    "status": "structurally-valid",
                    "scheduled_cells": len(schedule["cells"]),
                    "complete_cells": len(schedule["cells"]),
                    "manifest_sha256": schedule["manifest_sha256"],
                },
            )

    def test_structural_validator_rejects_drift_missing_extra_and_measurements(
        self,
    ) -> None:
        mutations = ("protocol", "missing", "extra", "measurement", "source")
        for mutation in mutations:
            with (
                self.subTest(mutation=mutation),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                schedule = write_structural_fixture(root)
                first = root / "cells" / schedule["cells"][0]["cell_id"]
                if mutation == "protocol":
                    value = json.loads((first / "protocol.json").read_text())
                    value["result_key"] = value.pop("result_prefix")
                    canonical_write(first / "protocol.json", value)
                elif mutation == "missing":
                    (first / "CELL_COMPLETE").unlink()
                elif mutation == "extra":
                    (root / "cells" / "unscheduled-cell").mkdir()
                elif mutation == "measurement":
                    (first / "measurements.json").write_text("{}\n")
                else:
                    (root / "source-archive.tar.gz").write_bytes(b"foreign")
                with self.assertRaises(ValueError):
                    validate_structural_tree(root)

    def test_replay_cli_writes_the_same_tree_admitted_by_validator(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            archive = temp / "source.tar.gz"
            archive.write_bytes(b"publication-v3-replay-source\n")
            manifest = temp / "manifest.json"
            canonical_write(manifest, structural_manifest(archive.read_bytes()))
            output = temp / "output"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(PROTOCOL),
                    "replay",
                    str(manifest),
                    "--source-archive",
                    str(archive),
                    "--output",
                    str(output),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(
                validate_structural_tree(output)["status"], "structurally-valid"
            )


if __name__ == "__main__":
    unittest.main()
