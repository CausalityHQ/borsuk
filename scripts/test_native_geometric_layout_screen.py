from __future__ import annotations

import dataclasses
import itertools
import random
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    EvaluationLimits,
    LayoutAuthority,
    LayoutEvaluation,
    LayoutMethod,
    MembershipRow,
    construct_layout,
    encoded_sq8_page_bytes,
    evaluate_layout,
    exact_page_coverage,
    finalize_layout_screen,
    layout_authority_from_dict,
    membership_schema,
    read_membership_parquet,
    write_membership_parquet,
)


class AuthorityAndMembershipTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_ids = tuple(
            value.to_bytes(16, "big")
            for value in (91, 3, 77, 14, 62, 8, 55, 21, 48, 34, 41, 27)
        )
        self.source = ArtifactIdentity(
            role="source",
            uri="s3://frozen/source.parquet",
            sha256="11" * 32,
            encoded_bytes=12_345,
        )
        self.authority = LayoutAuthority(
            schema="borsuk-native-geometric-layout-authority-v1",
            source=self.source,
            rows=12,
            dimensions=3,
            metric="l2",
            seed=20260921,
            method=LayoutMethod.ID_ORDER_256,
            maximum_page_rows=4,
            maximum_page_bytes=4096,
        )
        construction = bytes.fromhex("33" * 32)
        self.rows = tuple(
            MembershipRow(
                stable_id=stable_id,
                source_ordinal=source_ordinal,
                page_ordinal=source_ordinal // 4,
                in_page_ordinal=source_ordinal % 4,
                page_rows=4,
                encoded_page_bytes=1024,
                method=self.authority.method,
                source_sha256=bytes.fromhex(self.source.sha256),
                seed=self.authority.seed,
                construction_sha256=construction,
            )
            for source_ordinal, stable_id in enumerate(self.source_ids)
        )

    def authority_payload(self) -> dict[str, object]:
        return {
            "schema": self.authority.schema,
            "source": {
                "role": self.source.role,
                "uri": self.source.uri,
                "sha256": self.source.sha256,
                "encoded_bytes": self.source.encoded_bytes,
            },
            "rows": self.authority.rows,
            "dimensions": self.authority.dimensions,
            "metric": self.authority.metric,
            "seed": self.authority.seed,
            "method": self.authority.method.value,
            "maximum_page_rows": self.authority.maximum_page_rows,
            "maximum_page_bytes": self.authority.maximum_page_bytes,
        }

    def test_layout_authority_rejects_schema_type_identity_and_limit_drift(self) -> None:
        self.assertEqual(layout_authority_from_dict(self.authority_payload()), self.authority)

        mutations = []
        missing = self.authority_payload()
        del missing["seed"]
        mutations.append(missing)
        extra = self.authority_payload()
        extra["unknown"] = 1
        mutations.append(extra)
        bool_rows = self.authority_payload()
        bool_rows["rows"] = True
        mutations.append(bool_rows)
        zero_dimensions = self.authority_payload()
        zero_dimensions["dimensions"] = 0
        mutations.append(zero_dimensions)
        bad_method = self.authority_payload()
        bad_method["method"] = "id-order"
        mutations.append(bad_method)
        bad_source = self.authority_payload()
        bad_source["source"] = {**bad_source["source"], "sha256": "11" * 31}
        mutations.append(bad_source)
        aliasing_source = self.authority_payload()
        aliasing_source["source"] = {
            **aliasing_source["source"],
            "role": "membership",
        }
        mutations.append(aliasing_source)
        zero_page_bytes = self.authority_payload()
        zero_page_bytes["maximum_page_bytes"] = 0
        mutations.append(zero_page_bytes)

        for mutation in mutations:
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                layout_authority_from_dict(mutation)

    def test_membership_requires_exact_single_owner_source_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "membership.parquet"
            write_membership_parquet(path, self.authority, self.rows)
            self.assertEqual(
                read_membership_parquet(path, self.authority, self.source_ids),
                list(self.rows),
            )

            mutations = [
                self.rows[:-1],
                self.rows[:-1] + (dataclasses.replace(self.rows[-1], stable_id=self.rows[0].stable_id),),
                self.rows[:5]
                + (dataclasses.replace(self.rows[5], source_ordinal=4),)
                + self.rows[6:],
                self.rows[:5]
                + (dataclasses.replace(self.rows[5], in_page_ordinal=3),)
                + self.rows[6:],
                self.rows[:5]
                + (dataclasses.replace(self.rows[5], page_rows=3),)
                + self.rows[6:],
                self.rows[:5]
                + (dataclasses.replace(self.rows[5], encoded_page_bytes=2048),)
                + self.rows[6:],
            ]
            for ordinal, mutation in enumerate(mutations):
                with self.subTest(ordinal=ordinal), self.assertRaises(ValueError):
                    write_membership_parquet(path, self.authority, mutation)

            wrong_ids = self.source_ids[:-1] + ((999).to_bytes(16, "big"),)
            with self.assertRaises(ValueError):
                read_membership_parquet(path, self.authority, wrong_ids)

    def test_membership_parquet_has_one_strict_physical_schema(self) -> None:
        expected = pa.schema(
            [
                pa.field("stable_id", pa.binary(), nullable=False),
                pa.field("source_ordinal", pa.uint32(), nullable=False),
                pa.field("page_ordinal", pa.uint32(), nullable=False),
                pa.field("in_page_ordinal", pa.uint16(), nullable=False),
                pa.field("page_rows", pa.uint16(), nullable=False),
                pa.field("encoded_page_bytes", pa.uint32(), nullable=False),
                pa.field("method", pa.string(), nullable=False),
                pa.field("source_sha256", pa.binary(32), nullable=False),
                pa.field("seed", pa.uint64(), nullable=False),
                pa.field("construction_sha256", pa.binary(32), nullable=False),
            ]
        )
        self.assertEqual(membership_schema(), expected)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "membership.parquet"
            write_membership_parquet(path, self.authority, self.rows)
            self.assertEqual(pq.read_schema(path), expected)

            nullable = expected.set(0, pa.field("stable_id", pa.binary(), nullable=True))
            valid_table = pq.read_table(path)
            pq.write_table(
                pa.Table.from_arrays(valid_table.columns, schema=nullable),
                path,
            )
            with self.assertRaises(ValueError):
                read_membership_parquet(path, self.authority, self.source_ids)

    def test_membership_bytes_are_deterministic_for_the_same_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.parquet"
            second = Path(directory) / "second.parquet"
            first_identity = write_membership_parquet(first, self.authority, self.rows)
            second_identity = write_membership_parquet(second, self.authority, self.rows)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(first_identity.sha256, second_identity.sha256)
            self.assertEqual(first_identity.encoded_bytes, second_identity.encoded_bytes)


class QueryBlindConstructorTests(unittest.TestCase):
    def test_page_byte_cap_models_sq8_not_float32_payload(self) -> None:
        generator = np.random.default_rng(20260921)
        vectors = generator.standard_normal((700, 768), dtype=np.float32)
        stable_ids = tuple(value.to_bytes(16, "big") for value in range(700))
        within = encoded_sq8_page_bytes(
            np.arange(599, dtype=np.int64), stable_ids, vectors
        )
        over = encoded_sq8_page_bytes(
            np.arange(600, dtype=np.int64), stable_ids, vectors
        )
        self.assertEqual(within, 491_002)
        self.assertEqual(over, 491_802)
        self.assertLessEqual(within, 491_520)
        self.assertGreater(over, 491_520)

    def authority(
        self,
        method: LayoutMethod,
        *,
        rows: int = 8,
        dimensions: int = 2,
        maximum_page_rows: int = 2,
        maximum_page_bytes: int = 4096,
    ) -> LayoutAuthority:
        return LayoutAuthority(
            schema="borsuk-native-geometric-layout-authority-v1",
            source=ArtifactIdentity(
                role="source",
                uri="s3://frozen/source.parquet",
                sha256="11" * 32,
                encoded_bytes=12_345,
            ),
            rows=rows,
            dimensions=dimensions,
            metric="l2",
            seed=20260921,
            method=method,
            maximum_page_rows=maximum_page_rows,
            maximum_page_bytes=maximum_page_bytes,
        )

    @staticmethod
    def pages(rows: list[MembershipRow]) -> list[list[int]]:
        pages: dict[int, list[int]] = {}
        for row in rows:
            pages.setdefault(row.page_ordinal, []).append(row.source_ordinal)
        return list(pages.values())

    def test_id_and_random_projection_controls_are_literal_and_deterministic(self) -> None:
        stable_ids = tuple(
            value.to_bytes(16, "big")
            for value in (80, 10, 70, 20, 60, 30, 50, 40)
        )
        vectors = np.asarray(
            ((0, 0), (1, 0), (0, 2), (3, 0), (0, 4), (5, 0), (0, 6), (7, 0)),
            dtype=np.float32,
        )

        id_rows = construct_layout(
            self.authority(LayoutMethod.ID_ORDER_256), stable_ids, vectors
        )
        self.assertEqual(self.pages(id_rows), [[1, 3], [5, 7], [6, 4], [2, 0]])

        projection_authority = self.authority(LayoutMethod.RANDOM_PROJECTION_256)
        first = construct_layout(projection_authority, stable_ids, vectors)
        second = construct_layout(projection_authority, stable_ids, vectors.copy())
        self.assertEqual(self.pages(first), [[6, 4], [2, 0], [1, 3], [5, 7]])
        self.assertEqual(first, second)

    def test_balanced_two_means_obeys_capacity_and_stable_ties(self) -> None:
        stable_ids = tuple(value.to_bytes(16, "big") for value in range(8))
        vectors = np.asarray(
            ((-9, 0), (-8, 0), (-7, 0), (-6, 0), (6, 0), (7, 0), (8, 0), (9, 0)),
            dtype=np.float32,
        )
        authority = self.authority(LayoutMethod.TWO_MEANS_256)
        rows = construct_layout(authority, stable_ids, vectors)
        self.assertEqual(self.pages(rows), [[6, 7], [4, 5], [2, 3], [0, 1]])
        self.assertTrue(all(row.page_rows == 2 for row in rows))
        self.assertTrue(
            all(row.encoded_page_bytes <= authority.maximum_page_bytes for row in rows)
        )
        self.assertEqual(len({row.construction_sha256 for row in rows}), 1)

    def test_two_means_uses_minimum_capacity_aware_leaf_count(self) -> None:
        stable_ids = tuple(value.to_bytes(16, "big") for value in range(70))
        vectors = np.asarray(
            [(float(value), float((value * value) % 17)) for value in range(70)],
            dtype=np.float32,
        )
        authority = self.authority(
            LayoutMethod.TWO_MEANS_256,
            rows=70,
            dimensions=2,
            maximum_page_rows=30,
            maximum_page_bytes=491_520,
        )
        pages = self.pages(construct_layout(authority, stable_ids, vectors))
        self.assertEqual(len(pages), 3)
        self.assertEqual(sorted(map(len, pages)), [23, 23, 24])

    def test_two_means_rejects_nonfinite_empty_and_unsplittable_geometry(self) -> None:
        authority = self.authority(LayoutMethod.TWO_MEANS_256, rows=4)
        stable_ids = tuple(value.to_bytes(16, "big") for value in range(4))
        fixtures = (
            np.asarray(((0, 0), (1, 0), (2, 0), (np.nan, 0)), dtype=np.float32),
            np.zeros((4, 2), dtype=np.float32),
        )
        for vectors in fixtures:
            with self.subTest(vectors=vectors), self.assertRaises(ValueError):
                construct_layout(authority, stable_ids, vectors)

    def test_constructor_cli_has_no_query_truth_or_evaluation_surface(self) -> None:
        script = Path(__file__).with_name("native_geometric_layout_screen.py")
        help_result = subprocess.run(
            [sys.executable, str(script), "construct", "--help"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(help_result.returncode, 0, help_result.stderr)
        help_text = help_result.stdout.lower()
        for forbidden in ("query", "truth", "ground-truth", "gt-path"):
            self.assertNotIn(forbidden, help_text)

        rejected = subprocess.run(
            [
                sys.executable,
                str(script),
                "construct",
                "--authority",
                "authority.json",
                "--source",
                "source.parquet",
                "--output",
                "membership.parquet",
                "--query-path",
                "queries.parquet",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("unrecognized arguments: --query-path", rejected.stderr)


class ExactCoverageTests(unittest.TestCase):
    def test_control_drift_invalidates_every_layout_decision(self) -> None:
        id_control = LayoutEvaluation(
            method=LayoutMethod.ID_ORDER_256,
            samples=(),
            recall_at_10_ppm=0,
            mean_recall_at_100_ppm=613_770,
            p05_recall_at_100_ppm=470_000,
            worst_recall_at_100_ppm=410_000,
            decision="control",
        )
        arms = (
            id_control,
            dataclasses.replace(
                id_control,
                method=LayoutMethod.RANDOM_PROJECTION_256,
                mean_recall_at_100_ppm=960_000,
                p05_recall_at_100_ppm=900_000,
                decision="killed",
            ),
            dataclasses.replace(
                id_control,
                method=LayoutMethod.TWO_MEANS_256,
                mean_recall_at_100_ppm=980_000,
                p05_recall_at_100_ppm=920_000,
                decision="insufficient",
            ),
            dataclasses.replace(
                id_control,
                method=LayoutMethod.TWO_MEANS_480K,
                mean_recall_at_100_ppm=995_000,
                p05_recall_at_100_ppm=960_000,
                decision="advance",
            ),
        )
        self.assertEqual(finalize_layout_screen(arms), arms)
        drifted = (dataclasses.replace(id_control, mean_recall_at_100_ppm=623_771),) + arms[1:]
        with self.assertRaises(ValueError):
            finalize_layout_screen(drifted)

    def test_evaluator_cli_is_separate_from_constructor_capability(self) -> None:
        script = Path(__file__).with_name("native_geometric_layout_screen.py")
        result = subprocess.run(
            [sys.executable, str(script), "evaluate", "--help"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--truth", result.stdout)
        self.assertIn("--membership", result.stdout)
        self.assertIn("--evidence", result.stdout)
        self.assertIn("--result", result.stdout)

    def test_equal_page_oracle_matches_literal_top_hit_counts(self) -> None:
        sample = exact_page_coverage(
            {0: 4, 1: 3, 2: 2, 3: 1},
            {0: 100, 1: 100, 2: 100, 3: 100},
            EvaluationLimits(maximum_pages=2, maximum_bytes=250),
        )
        self.assertEqual(sample.selected_page_ordinals, (0, 1))
        self.assertEqual(sample.hits_at_100, 7)
        self.assertEqual(sample.encoded_bytes, 200)

    def test_variable_page_oracle_matches_bruteforce_not_greedy_ratio(self) -> None:
        hits = {0: 9, 1: 6, 2: 5}
        sizes = {0: 6, 1: 4, 2: 3}
        limits = EvaluationLimits(maximum_pages=2, maximum_bytes=10)
        sample = exact_page_coverage(hits, sizes, limits)
        self.assertEqual(sample.selected_page_ordinals, (0, 1))
        self.assertEqual(sample.hits_at_100, 15)
        self.assertEqual(sample.encoded_bytes, 10)

        generator = random.Random(20260921)
        for case in range(30):
            pages = range(6)
            random_hits = {page: generator.randrange(0, 8) for page in pages}
            random_sizes = {page: generator.randrange(1, 8) for page in pages}
            random_limits = EvaluationLimits(maximum_pages=3, maximum_bytes=12)
            candidates = []
            for count in range(random_limits.maximum_pages + 1):
                for selected in itertools.combinations(pages, count):
                    encoded = sum(random_sizes[page] for page in selected)
                    if encoded <= random_limits.maximum_bytes:
                        candidates.append(
                            (
                                sum(random_hits[page] for page in selected),
                                -encoded,
                                tuple(-page for page in selected),
                                selected,
                            )
                        )
            expected = max(candidates)[3]
            actual = exact_page_coverage(random_hits, random_sizes, random_limits)
            with self.subTest(case=case):
                self.assertEqual(actual.selected_page_ordinals, expected)

    def test_oracle_ties_choose_lexicographically_smallest_page_tuple(self) -> None:
        sample = exact_page_coverage(
            {0: 4, 1: 4, 2: 4},
            {0: 100, 1: 100, 2: 100},
            EvaluationLimits(maximum_pages=2, maximum_bytes=200),
        )
        self.assertEqual(sample.selected_page_ordinals, (0, 1))
        self.assertEqual(sample.hits_at_100, 8)

    def test_evaluation_recomputes_r10_mean_p05_worst_and_decision(self) -> None:
        owner_by_id: dict[bytes, int] = {}
        queries: list[tuple[bytes, ...]] = []
        for query_ordinal, main_hits in enumerate((100, 98, 90)):
            neighbors = tuple(
                f"q{query_ordinal:02d}-n{rank:03d}".encode() for rank in range(100)
            )
            queries.append(neighbors)
            for rank, stable_id in enumerate(neighbors):
                owner_by_id[stable_id] = query_ordinal * 2 + (rank >= main_hits)
        page_bytes = {page: 100 for page in range(6)}
        evaluation = evaluate_layout(
            LayoutMethod.RANDOM_PROJECTION_256,
            owner_by_id,
            page_bytes,
            tuple(queries),
            EvaluationLimits(maximum_pages=1, maximum_bytes=100),
        )
        self.assertEqual(evaluation.recall_at_10_ppm, 1_000_000)
        self.assertEqual(evaluation.mean_recall_at_100_ppm, 960_000)
        self.assertEqual(evaluation.p05_recall_at_100_ppm, 900_000)
        self.assertEqual(evaluation.worst_recall_at_100_ppm, 900_000)
        self.assertEqual(evaluation.decision, "killed")
        self.assertEqual(
            [sample.hits_at_100 for sample in evaluation.samples], [100, 98, 90]
        )

        advancing_owner = dict(owner_by_id)
        for query_ordinal, neighbors in enumerate(queries):
            for rank, stable_id in enumerate(neighbors):
                advancing_owner[stable_id] = query_ordinal * 2 + (rank >= 99)
        advancing = evaluate_layout(
            LayoutMethod.TWO_MEANS_256,
            advancing_owner,
            page_bytes,
            tuple(queries),
            EvaluationLimits(maximum_pages=1, maximum_bytes=100),
        )
        self.assertEqual(advancing.mean_recall_at_100_ppm, 990_000)
        self.assertEqual(advancing.p05_recall_at_100_ppm, 990_000)
        self.assertEqual(advancing.decision, "advance")


if __name__ == "__main__":
    unittest.main()
