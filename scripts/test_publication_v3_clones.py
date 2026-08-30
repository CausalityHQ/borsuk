import json
import unittest

from scripts.publication_v3_clones import (
    build_clone_receipt,
    clone_index_uri,
    require_verified_clone_inventory,
    validate_clone_receipt,
)
from scripts.publication_v3_protocol import canonical_json_bytes
from scripts.test_publication_v3_receipts import data_roster
from scripts.test_publication_v3_results import receipt_for
from scripts.test_run_publication_v3_cell import plan_arms, scheduled_cell


class PublicationV3CloneTests(unittest.TestCase):
    def test_clone_receipt_binds_base_arm_attempt_and_exact_copy_inventory(
        self,
    ) -> None:
        cell = scheduled_cell(kind="write-update-delete-compact")
        arm = next(arm for arm in plan_arms(cell) if arm["writers"] == 1)
        base = receipt_for(cell)
        roster = data_roster(cell)
        inventory = [
            {
                "path": item["path"],
                "bytes": item["bytes"],
                "source_etag": item["etag"],
                "destination_etag": f'"{index:032x}"',
            }
            for index, item in enumerate(roster)
        ]
        receipt = build_clone_receipt(
            cell=cell,
            arm=arm,
            attempt_id="attempt-01",
            base_receipt=base,
            base_roster=roster,
            copy_inventory=inventory,
        )
        self.assertEqual(
            receipt["clone_index_uri"],
            clone_index_uri(cell, arm=arm, attempt_id="attempt-01"),
        )
        self.assertNotEqual(receipt["clone_index_uri"], base["index_uri"])
        self.assertEqual(
            validate_clone_receipt(
                receipt,
                cell=cell,
                arm=arm,
                attempt_id="attempt-01",
                base_receipt=base,
            ),
            receipt,
        )
        payload = canonical_json_bytes(inventory) + b"\n"
        self.assertEqual(
            require_verified_clone_inventory(receipt, payload, base_roster=roster),
            inventory,
        )

        for mutation in (
            {**receipt, "base_receipt_sha256": "0" * 64},
            {**receipt, "clone_index_uri": base["index_uri"]},
            {**receipt, "attempt_id": "replayed-attempt"},
        ):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_clone_receipt(
                    mutation,
                    cell=cell,
                    arm=arm,
                    attempt_id="attempt-01",
                    base_receipt=base,
                )

        damaged = json.loads(json.dumps(inventory))
        damaged[0]["destination_etag"] = '"different"'
        with self.assertRaisesRegex(ValueError, "copy evidence"):
            require_verified_clone_inventory(
                receipt,
                canonical_json_bytes(damaged) + b"\n",
                base_roster=roster,
            )


if __name__ == "__main__":
    unittest.main()
