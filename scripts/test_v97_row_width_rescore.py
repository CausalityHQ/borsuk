import copy
import dataclasses
import hashlib
import json
import pathlib
import tempfile
import unittest

from scripts.v97_row_width_rescore import (
    ArmEvidenceSummary,
    choose_screen_winner,
    paired_bootstrap_interval,
    parse_rescore_args,
    rescore_screen_result,
    validate_arm_evidence,
    validate_screen_result,
)
from scripts.v97_row_width_screen import (
    PQ16X8,
    PQ24X8,
    PQ32X4,
    PQ32X8,
    SUMMARY_ONLY_PQ16X8,
    project_resident_bytes_100m,
)


class RowWidthRescoreTests(unittest.TestCase):
    def test_rescore_cli_requires_result_identity_and_registered_inputs(self) -> None:
        # Break caught: the independent step validates an ambient result or
        # omits the exact producer digest/10k-resample contract.
        arguments = []
        for role in ("source", "queries", "truth", "generation", "base", "delta"):
            arguments.extend(
                [
                    f"--{role}",
                    f"{role}.bin",
                    f"--{role}-uri",
                    f"s3://fixture/{role}",
                    f"--{role}-sha256",
                    "1" * 64,
                    f"--{role}-bytes",
                    "1",
                ]
            )
        arguments.extend(
            [
                "--source-commit",
                "2" * 40,
                "--critique-result-sha256",
                "3" * 64,
                "--bootstrap-resamples",
                "10000",
                "--result",
                "result.json",
                "--result-sha256",
                "4" * 64,
                "--output",
                "rescore.json",
            ]
        )
        parsed = parse_rescore_args(arguments)
        self.assertEqual(parsed.bootstrap_resamples, 10_000)
        self.assertEqual(parsed.query_count, 1_000)

    def setUp(self) -> None:
        self.page_by_id = {
            1: ("base", 0),
            2: ("delta", 0),
            3: ("base", 1),
            4: ("delta", 1),
        }
        self.page_bytes = {
            ("base", 0): 4,
            ("base", 1): 6,
            ("delta", 0): 5,
            ("delta", 1): 7,
        }
        self.page_ranges = {
            page: (ordinal * 10, encoded_bytes)
            for ordinal, (page, encoded_bytes) in enumerate(self.page_bytes.items())
        }
        self.samples = [
            {
                "bytes": 4,
                "gets": 1,
                "hit10_ids": [1],
                "hit_ids": [1],
                "hits10": 1,
                "hits": 1,
                "query": 0,
                "ranges": [
                    {"bytes": 4, "object_role": "base", "offset": 0, "ordinal": 0}
                ],
                "recall10_ppm": 500_000,
                "recall100_ppm": 500_000,
                "selected_pages": [{"object_role": "base", "ordinal": 0}],
                "truth_ids": [1, 2],
            },
            {
                "bytes": 13,
                "gets": 2,
                "hit10_ids": [3, 4],
                "hit_ids": [3, 4],
                "hits10": 2,
                "hits": 2,
                "query": 1,
                "ranges": [
                    {"bytes": 6, "object_role": "base", "offset": 10, "ordinal": 1},
                    {"bytes": 7, "object_role": "delta", "offset": 30, "ordinal": 1},
                ],
                "recall10_ppm": 1_000_000,
                "recall100_ppm": 1_000_000,
                "selected_pages": [
                    {"object_role": "base", "ordinal": 1},
                    {"object_role": "delta", "ordinal": 1},
                ],
                "truth_ids": [3, 4],
            },
        ]

    def test_validator_reconstructs_hits_aggregates_and_resource_gates(self) -> None:
        # Break caught: the rescorer accepts producer aggregates or gives an
        # unfetched delta truth row credit without reconstructing membership.
        summary = validate_arm_evidence(
            self.samples,
            page_by_id=self.page_by_id,
            page_bytes=self.page_bytes,
            page_ranges=self.page_ranges,
            expected_queries=2,
            neighbors=2,
            max_gets=32,
            max_bytes=16 * 1024 * 1024,
        )
        self.assertEqual(summary.average_recall10_ppm, 750_000)
        self.assertEqual(summary.average_recall100_ppm, 750_000)
        self.assertEqual(summary.p05_recall100_ppm, 500_000)
        self.assertEqual(summary.worst_recall100_ppm, 500_000)
        self.assertEqual(summary.max_gets_per_query, 2)
        self.assertEqual(summary.max_bytes_per_query, 13)

    def test_validator_rejects_each_producer_claim_drift(self) -> None:
        # Break caught: canonical-looking but false hit, byte, query-order, or
        # aggregate evidence reaches the G1 decision.
        mutations = []
        changed = copy.deepcopy(self.samples)
        changed[0]["hit_ids"] = [1, 2]
        mutations.append((changed, "sample hit evidence differs"))
        changed = copy.deepcopy(self.samples)
        changed[1]["bytes"] = 10
        mutations.append((changed, "sample resource evidence differs"))
        changed = copy.deepcopy(self.samples)
        changed[1]["query"] = 0
        mutations.append((changed, "query ordinals differ"))
        changed = copy.deepcopy(self.samples)
        changed[0]["selected_pages"] = [
            {"object_role": "unknown", "ordinal": 0}
        ]
        mutations.append((changed, "selected page differs"))
        changed = copy.deepcopy(self.samples)
        changed[0]["ranges"][0]["offset"] = 1
        mutations.append((changed, "sample range evidence differs"))
        for samples, message in mutations:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    validate_arm_evidence(
                        samples,
                        page_by_id=self.page_by_id,
                        page_bytes=self.page_bytes,
                        page_ranges=self.page_ranges,
                        expected_queries=2,
                        neighbors=2,
                        max_gets=32,
                        max_bytes=16 * 1024 * 1024,
                    )

    def test_paired_bootstrap_uses_one_seeded_query_index_matrix(self) -> None:
        # Break caught: arms are resampled independently or p05 is not
        # recomputed within each resample.
        control = [0, 250_000, 500_000, 750_000, 1_000_000]
        challenger = [value + 100_000 for value in control]
        first = paired_bootstrap_interval(
            challenger, control, seed=7216, resamples=10_000, statistic="mean"
        )
        second = paired_bootstrap_interval(
            challenger, control, seed=7216, resamples=10_000, statistic="mean"
        )
        self.assertEqual(first, (100_000, 100_000))
        self.assertEqual(second, first)
        self.assertEqual(
            paired_bootstrap_interval(
                challenger,
                control,
                seed=7216,
                resamples=10_000,
                statistic="p05",
            ),
            (100_000, 100_000),
        )

    def test_whole_result_recomputes_aggregates_ci_memory_and_winner(self) -> None:
        # Break caught: a canonical producer document can nominate an arm by
        # forging its aggregate, paired CI, memory worksheet, or winner.
        perfect = copy.deepcopy(self.samples)
        perfect[0].update(
            {
                "bytes": 9,
                "gets": 2,
                "hit10_ids": [1, 2],
                "hit_ids": [1, 2],
                "hits10": 2,
                "hits": 2,
                "recall10_ppm": 1_000_000,
                "recall100_ppm": 1_000_000,
                "ranges": [
                    {"bytes": 4, "object_role": "base", "offset": 0, "ordinal": 0},
                    {"bytes": 5, "object_role": "delta", "offset": 20, "ordinal": 0},
                ],
                "selected_pages": [
                    {"object_role": "base", "ordinal": 0},
                    {"object_role": "delta", "ordinal": 0},
                ],
            }
        )
        aggregate = {
            "average_recall10_ppm": 1_000_000,
            "average_recall100_ppm": 1_000_000,
            "p05_recall100_ppm": 1_000_000,
            "worst_recall100_ppm": 1_000_000,
            "max_gets_per_query": 2,
            "max_bytes_per_query": 13,
            "quality_gate_passed": True,
            "resource_gate_passed": True,
        }
        arms = []
        for spec in (
            PQ16X8,
            PQ24X8,
            PQ32X8,
            PQ32X4,
            SUMMARY_ONLY_PQ16X8,
        ):
            arms.append(
                {
                    "aggregate": aggregate.copy(),
                    "name": spec.name,
                    "paired_vs_pq16": {
                        "average_recall10_ppm": [0, 0],
                        "average_recall100_ppm": [0, 0],
                        "p05_recall100_ppm": [0, 0],
                    },
                    "projection": dataclasses.asdict(
                        project_resident_bytes_100m(spec)
                    ),
                    "row_bytes": spec.row_bytes,
                    "samples": copy.deepcopy(perfect),
                }
            )
        result = {
            "arms": arms,
            "artifacts": {
                name: {
                    role: {
                        "bytes": 1 if role == "codes" else 4,
                        "dtype": "uint8" if role == "codes" else "float32",
                        "sha256": "6" * 64,
                        "shape": [1],
                    }
                    for role in ("codebook", "codes")
                }
                for name in (
                    "summary-router",
                    "pq16x8",
                    "pq24x8",
                    "pq32x8",
                    "pq32x4",
                )
            },
            "authority": {
                "critique_result_sha256": "2" * 64,
                "dimensions": 768,
                "identities": {
                    role: {
                        "bytes": 1,
                        "sha256": "4" * 64,
                        "uri": f"s3://fixture/{role}",
                    }
                    for role in (
                        "source",
                        "queries",
                        "truth",
                        "generation",
                        "base",
                        "delta",
                    )
                },
                "page_map_sha256": "3" * 64,
                "seed": 7216,
                "source_commit": "1" * 40,
                "training_declaration": "source-only-base-tier",
            },
            "bootstrap_resamples": 10_000,
            "bootstrap_seed": 7216,
            "exact_f32": {
                "aggregate": aggregate.copy(),
                "samples": copy.deepcopy(perfect),
            },
            "max_bytes": 16 * 1024 * 1024,
            "max_gets": 32,
            "neighbors": 2,
            "queries": 2,
            "schema": "borsuk-v97-row-width-screen-v2",
            "shortlist_rows": 512,
            "summary_page_limit": 128,
            "winner": "summary-only-pq16x8",
        }

        summary = validate_screen_result(
            result,
            page_by_id=self.page_by_id,
            page_bytes=self.page_bytes,
            page_ranges=self.page_ranges,
            expected_authority=result["authority"],
            expected_truth_ids=[[1, 2], [3, 4]],
        )
        self.assertEqual(summary["winner"], "summary-only-pq16x8")
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            result_path = root / "result.json"
            output_path = root / "rescore.json"
            body = (
                json.dumps(result, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            result_path.write_bytes(body)
            receipt = rescore_screen_result(
                result_path,
                expected_sha256=hashlib.sha256(body).hexdigest(),
                page_by_id=self.page_by_id,
                page_bytes=self.page_bytes,
                page_ranges=self.page_ranges,
                expected_authority=result["authority"],
                expected_truth_ids=[[1, 2], [3, 4]],
                output=output_path,
            )
            self.assertEqual(receipt["winner"], "summary-only-pq16x8")
            self.assertEqual(json.loads(output_path.read_bytes()), receipt)

        mutations = []
        changed = copy.deepcopy(result)
        changed["arms"][0]["aggregate"]["average_recall100_ppm"] -= 1
        mutations.append((changed, "arm aggregate differs"))
        changed = copy.deepcopy(result)
        changed["arms"][0]["projection"]["total_bytes"] -= 1
        mutations.append((changed, "resident projection differs"))
        changed = copy.deepcopy(result)
        changed["arms"][3]["paired_vs_pq16"]["average_recall100_ppm"] = [-1, 0]
        mutations.append((changed, "paired confidence interval differs"))
        changed = copy.deepcopy(result)
        changed["winner"] = "pq16x8"
        mutations.append((changed, "screen winner differs"))
        changed = copy.deepcopy(result)
        changed["authority"]["identities"]["delta"]["sha256"] = "5" * 64
        mutations.append((changed, "screen authority differs"))
        changed = copy.deepcopy(result)
        changed["artifacts"]["pq32x4"]["codes"]["shape"] = []
        mutations.append((changed, "screen artifact identity differs"))
        changed = copy.deepcopy(result)
        changed["summary_page_limit"] = 127
        mutations.append((changed, "screen routing limits differ"))
        changed = copy.deepcopy(result)
        changed["shortlist_rows"] = 511
        mutations.append((changed, "screen routing limits differ"))
        changed = copy.deepcopy(result)
        changed["arms"][3]["samples"][0]["truth_ids"] = [2, 1]
        changed["arms"][3]["samples"][0]["hit_ids"] = [2, 1]
        changed["arms"][3]["samples"][0]["hit10_ids"] = [2, 1]
        mutations.append((changed, "arm truth differs"))
        changed = copy.deepcopy(result)
        for evidence in [changed["exact_f32"], *changed["arms"]]:
            evidence["samples"][0]["truth_ids"] = [2, 1]
            evidence["samples"][0]["hit_ids"] = [2, 1]
            evidence["samples"][0]["hit10_ids"] = [2, 1]
        mutations.append((changed, "registered truth differs"))
        for changed, message in mutations:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    validate_screen_result(
                        changed,
                        page_by_id=self.page_by_id,
                        page_bytes=self.page_bytes,
                        page_ranges=self.page_ranges,
                        expected_authority=result["authority"],
                        expected_truth_ids=[[1, 2], [3, 4]],
                    )

    def test_equal_width_winner_applies_paired_ci_kill_before_secondary_metrics(self) -> None:
        # Break caught: PQ32x4 wins on a secondary p05 despite a strictly
        # negative paired average-R@100 interval against PQ16.
        strong_p05 = ArmEvidenceSummary(
            average_recall10_ppm=990_000,
            average_recall100_ppm=976_000,
            p05_recall100_ppm=930_000,
            worst_recall100_ppm=900_000,
            max_gets_per_query=32,
            max_bytes_per_query=16 * 1024 * 1024,
            quality_gate_passed=True,
            resource_gate_passed=True,
        )
        control = dataclasses.replace(
            strong_p05,
            average_recall10_ppm=980_000,
            average_recall100_ppm=980_000,
            p05_recall100_ppm=910_000,
        )
        winner = choose_screen_winner(
            {"pq16x8": control, "pq32x4": strong_p05},
            projections={"pq16x8": True, "pq32x4": True},
            paired_r100_ci={"pq16x8": (0, 0), "pq32x4": (-8_000, -1_000)},
            exact_gate_passed=True,
        )
        self.assertEqual(winner, "pq16x8")


if __name__ == "__main__":
    unittest.main()
