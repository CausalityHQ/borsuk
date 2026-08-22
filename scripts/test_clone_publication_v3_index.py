import json
import tempfile
import unittest
from pathlib import Path

from scripts.clone_publication_v3_index import (
    clone_index_from_files,
    clone_index_objects,
)
from scripts.publication_v3_clones import require_verified_clone_inventory
from scripts.publication_v3_protocol import canonical_json_bytes
from scripts.test_publication_v3_receipts import data_roster
from scripts.test_publication_v3_results import receipt_for
from scripts.test_run_publication_v3_cell import plan_arms, scheduled_cell


class PublicationV3CloneExecutorTests(unittest.TestCase):
    def test_file_entrypoint_emits_canonical_clone_authority(self) -> None:
        cell = scheduled_cell(kind="write-update-delete-compact")
        arm = next(arm for arm in plan_arms(cell) if arm["writers"] == 4)
        base = receipt_for(cell)
        roster = data_roster(cell)
        source_etags = {item["path"]: item["etag"] for item in roster}
        destination_etags = {
            item["path"]: f'"{index + 20:032x}"'
            for index, item in enumerate(roster)
        }
        base_key_prefix = base["index_uri"][5:].split("/", 1)[1]

        def aws_json(arguments: list[str]) -> dict[str, object]:
            operation = arguments[1]
            if operation == "list-objects-v2":
                return {"KeyCount": 0}
            key = arguments[arguments.index("--key") + 1]
            path = next(path for path in source_etags if key.endswith("/" + path))
            item = next(item for item in roster if item["path"] == path)
            if operation == "head-object":
                return {
                    "ContentLength": item["bytes"],
                    "ETag": (
                        source_etags[path]
                        if key.startswith(base_key_prefix + "/")
                        else destination_etags[path]
                    ),
                }
            if operation == "copy-object":
                return {"CopyObjectResult": {"ETag": destination_etags[path]}}
            raise AssertionError(arguments)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {
                "cell_path": root / "cell.json",
                "arm_path": root / "arm.json",
                "base_receipt_path": root / "base.json",
                "base_roster_path": root / "roster.json",
                "receipt_output": root / "CLONE_COMPLETE.json",
                "inventory_output": root / "CLONE_OBJECTS.json",
            }
            for key, value in (
                ("cell_path", cell),
                ("arm_path", arm),
                ("base_receipt_path", base),
                ("base_roster_path", roster),
            ):
                paths[key].write_bytes(canonical_json_bytes(value) + b"\n")
            receipt = clone_index_from_files(
                **paths,
                attempt_id="runtime-attempt-01",
                aws_json=aws_json,
                workers=4,
            )
            self.assertEqual(
                paths["receipt_output"].read_bytes(),
                canonical_json_bytes(receipt) + b"\n",
            )
            payload = paths["inventory_output"].read_bytes()
            inventory = json.loads(payload)
            self.assertEqual(payload, canonical_json_bytes(inventory) + b"\n")
            self.assertEqual(
                require_verified_clone_inventory(receipt, payload, base_roster=roster),
                inventory,
            )

    def test_clone_uses_atomic_server_side_copies_and_refuses_partial_target(self) -> None:
        cell = scheduled_cell(kind="write-update-delete-compact")
        arm = next(arm for arm in plan_arms(cell) if arm["writers"] == 1)
        base = receipt_for(cell)
        roster = data_roster(cell)
        calls: list[list[str]] = []
        source_etags = {item["path"]: item["etag"] for item in roster}
        destination_etags = {
            item["path"]: f'"{index + 10:032x}"' for index, item in enumerate(roster)
        }
        base_key_prefix = base["index_uri"][5:].split("/", 1)[1]

        def aws_json(arguments: list[str]) -> dict[str, object]:
            calls.append(arguments)
            self.assertEqual(
                arguments[arguments.index("--expected-bucket-owner") + 1],
                "453182569524",
            )
            operation = arguments[1]
            if operation == "list-objects-v2":
                return {"KeyCount": 0}
            key = arguments[arguments.index("--key") + 1]
            path = next(path for path in source_etags if key.endswith("/" + path))
            item = next(item for item in roster if item["path"] == path)
            if operation == "head-object":
                etag = (
                    source_etags[path]
                    if key.startswith(base_key_prefix + "/")
                    else destination_etags[path]
                )
                return {"ContentLength": item["bytes"], "ETag": etag}
            if operation == "copy-object":
                self.assertEqual(
                    arguments[
                        arguments.index("--expected-source-bucket-owner") + 1
                    ],
                    "453182569524",
                )
                self.assertEqual(
                    arguments[arguments.index("--copy-source-if-match") + 1],
                    source_etags[path],
                )
                return {"CopyObjectResult": {"ETag": destination_etags[path]}}
            raise AssertionError(arguments)

        receipt, payload = clone_index_objects(
            cell=cell,
            arm=arm,
            attempt_id="attempt-01",
            base_receipt=base,
            base_roster=roster,
            aws_json=aws_json,
        )
        self.assertEqual(
            require_verified_clone_inventory(receipt, payload, base_roster=roster),
            [
                {
                    "path": item["path"],
                    "bytes": item["bytes"],
                    "source_etag": source_etags[item["path"]],
                    "destination_etag": destination_etags[item["path"]],
                }
                for item in roster
            ],
        )
        operations = [call[1] for call in calls]
        self.assertEqual(operations.count("copy-object"), len(roster))
        self.assertEqual(operations.count("head-object"), 2 * len(roster))
        self.assertNotIn("get-object", operations)

        def occupied(arguments: list[str]) -> dict[str, object]:
            if arguments[1] == "list-objects-v2":
                return {"KeyCount": 1}
            raise AssertionError(arguments)

        with self.assertRaisesRegex(ValueError, "fresh clone prefix"):
            clone_index_objects(
                cell=cell,
                arm=arm,
                attempt_id="attempt-02",
                base_receipt=base,
                base_roster=roster,
                aws_json=occupied,
            )

    def test_clone_rejects_objects_above_atomic_copy_limit_before_aws(self) -> None:
        cell = scheduled_cell(kind="write-update-delete-compact")
        arm = next(arm for arm in plan_arms(cell) if arm["writers"] == 1)
        roster = data_roster(cell)
        roster[0]["bytes"] = 5 * 1024**3 + 1
        calls: list[list[str]] = []

        def aws_json(arguments: list[str]) -> dict[str, object]:
            calls.append(arguments)
            return {"KeyCount": 0}

        with self.assertRaisesRegex(ValueError, "atomic copy"):
            clone_index_objects(
                cell=cell,
                arm=arm,
                attempt_id="attempt-03",
                base_receipt=receipt_for(cell),
                base_roster=roster,
                aws_json=aws_json,
            )
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
