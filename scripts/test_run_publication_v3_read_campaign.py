from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

from scripts.run_publication_v3_read_campaign import campaign_commands, main

ROOT = Path(__file__).resolve().parents[1]


class RunPublicationV3ReadCampaignTests(unittest.TestCase):
    def test_driver_forces_controller_reconciliation_over_ambient_attempts(
        self,
    ) -> None:
        with (
            mock.patch.object(
                sys,
                "argv",
                ["run_publication_v3_read_campaign.py", "realistic-dense-read"],
            ),
            mock.patch.dict(
                os.environ,
                {
                    "BORSUK_PUBLICATION_V3_BUILD_ATTEMPT": "91",
                    "BORSUK_PUBLICATION_V3_RUNTIME_ATTEMPT": "92",
                },
            ),
            mock.patch(
                "scripts.run_publication_v3_read_campaign.subprocess.run"
            ) as run,
        ):
            self.assertEqual(main(), 0)

        self.assertEqual(run.call_count, 63)
        for call in run.call_args_list:
            self.assertEqual(call.kwargs["env"]["AWS_PROFILE"], "causality")
            self.assertEqual(
                call.kwargs["env"]["BORSUK_PUBLICATION_V3_BUILD_ATTEMPT"], "0"
            )
            self.assertEqual(
                call.kwargs["env"]["BORSUK_PUBLICATION_V3_RUNTIME_ATTEMPT"], "0"
            )

    def test_realistic_campaign_is_sequential_resumable_and_includes_100m(self) -> None:
        manifest = json.loads(
            (ROOT / "docs/research/publication-v3-manifest.json").read_text()
        )

        commands = campaign_commands(manifest, "realistic-dense-read")

        datasets = [
            "cohere-medium-1m-768",
            "cohere-large-10m-768",
            "laion-100m-768",
        ]
        self.assertEqual(len(commands), len(datasets) * (1 + 5 * 4))
        for dataset_index, dataset_id in enumerate(datasets):
            offset = dataset_index * 21
            self.assertEqual(
                commands[offset],
                ("--build-read", "realistic-dense-read", dataset_id),
            )
            self.assertEqual(
                commands[offset + 1],
                ("--run-read", "realistic-dense-read", dataset_id, "r01", "0"),
            )
            self.assertEqual(
                commands[offset + 20],
                ("--run-read", "realistic-dense-read", dataset_id, "r05", "3"),
            )

    def test_campaign_rejects_non_read_unknown_or_generated_workloads(self) -> None:
        manifest = json.loads(
            (ROOT / "docs/research/publication-v3-manifest.json").read_text()
        )
        raw_generated = json.loads(json.dumps(manifest))
        for dataset in raw_generated["datasets"]:
            source = dataset["source"]
            if source["state"] != "staged-generated":
                continue
            dataset["source"] = {
                "state": "generated",
                "generator": source["generator"],
                "seed": source["seed"],
            }
        for workload_id, error in (
            ("durable-lifecycle", "read-recall workload"),
            ("missing", "read-recall workload"),
            ("synthetic-dense-read", "generated dataset handoff"),
        ):
            with self.subTest(workload_id=workload_id):
                with self.assertRaisesRegex(ValueError, error):
                    campaign_commands(
                        raw_generated
                        if workload_id == "synthetic-dense-read"
                        else manifest,
                        workload_id,
                    )

    def test_promoted_synthetic_campaign_includes_both_100m_datasets(self) -> None:
        manifest = json.loads(
            (ROOT / "docs/research/publication-v3-manifest.json").read_text()
        )
        for dataset in manifest["datasets"]:
            source = dataset["source"]
            if source["state"] != "generated":
                continue
            attempt_root = (
                f"{manifest['prefixes']['dataset']}/{dataset['id']}/attempts/0001"
            )
            dataset["source"] = {
                "state": "staged-generated",
                "generator": source["generator"],
                "seed": source["seed"],
                "generator_source_archive_sha256": "a" * 64,
                "url": f"{attempt_root}/materialized",
                "sha256": "b" * 64,
                "receipt_uri": f"{attempt_root}/STAGING_COMPLETE.json",
                "receipt_sha256": "c" * 64,
            }
        commands = campaign_commands(manifest, "synthetic-dense-read")
        self.assertEqual(len(commands), 10 * 21)
        builds = [command for command in commands if command[0] == "--build-read"]
        self.assertIn(
            ("--build-read", "synthetic-dense-read", "synthetic-clustered-100m-768"),
            builds,
        )
        self.assertIn(
            ("--build-read", "synthetic-dense-read", "synthetic-uniform-100m-768"),
            builds,
        )


if __name__ == "__main__":
    unittest.main()
