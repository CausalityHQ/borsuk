from __future__ import annotations

import dataclasses
import hashlib
import json
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
    LayoutMethod,
    MembershipRow,
    evaluate_layout,
    write_coverage_parquet,
    write_membership_parquet,
)
from scripts.validate_native_geometric_layout_result import (
    LayoutScreenAuthority,
    ValidatedLayoutDecision,
    ValidationPaths,
    validate_result,
)


def artifact(path: Path, role: str) -> ArtifactIdentity:
    payload = path.read_bytes()
    return ArtifactIdentity(
        role=role,
        uri=f"s3://frozen/{path.name}",
        sha256=hashlib.sha256(payload).hexdigest(),
        encoded_bytes=len(payload),
    )


class ResultFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.seed = 20260921
        self.ids = tuple(value.to_bytes(16, "big") for value in range(100))
        self.source_path = root / "source.parquet"
        flat = pa.array(np.arange(200, dtype=np.float32), type=pa.float32())
        vectors = pa.FixedSizeListArray.from_arrays(flat, 2)
        pq.write_table(
            pa.Table.from_arrays(
                [pa.array(self.ids, type=pa.binary()), vectors],
                names=["feature_row_id", "embedding"],
            ),
            self.source_path,
        )
        self.source = artifact(self.source_path, "source")

        self.truth_path = root / "truth.parquet"
        truth_schema = pa.schema(
            [
                pa.field("query", pa.uint32(), nullable=False),
                pa.field(
                    "neighbors",
                    pa.list_(pa.field("element", pa.int64(), nullable=False), 100),
                    nullable=False,
                ),
            ]
        )
        pq.write_table(
            pa.Table.from_arrays(
                [
                    pa.array([0], type=pa.uint32()),
                    pa.array(
                        [list(range(100))],
                        type=truth_schema.field("neighbors").type,
                    ),
                ],
                schema=truth_schema,
            ),
            self.truth_path,
        )
        self.truth = artifact(self.truth_path, "truth")
        self.membership_paths: list[tuple[LayoutMethod, Path]] = []
        self.evidence_paths: list[tuple[LayoutMethod, Path]] = []
        membership_identities = []
        evidence_identities = []
        arms = []
        for method in LayoutMethod:
            authority = LayoutAuthority(
                schema="borsuk-native-geometric-layout-authority-v1",
                source=self.source,
                rows=100,
                dimensions=2,
                metric="l2",
                seed=self.seed,
                method=method,
                maximum_page_rows=25,
                maximum_page_bytes=4096,
            )
            membership_path = root / f"membership-{method.value}.parquet"
            construction = hashlib.sha256(method.value.encode()).digest()
            rows = tuple(
                MembershipRow(
                    stable_id=stable_id,
                    source_ordinal=ordinal,
                    page_ordinal=ordinal // 25,
                    in_page_ordinal=ordinal % 25,
                    page_rows=25,
                    encoded_page_bytes=1000,
                    method=method,
                    source_sha256=bytes.fromhex(self.source.sha256),
                    seed=self.seed,
                    construction_sha256=construction,
                )
                for ordinal, stable_id in enumerate(self.ids)
            )
            written_membership = write_membership_parquet(
                membership_path, authority, rows
            )
            membership_identity = dataclasses.replace(
                written_membership,
                role=f"membership:{method.value}",
                uri=f"s3://frozen/{membership_path.name}",
            )
            membership_identities.append(membership_identity)
            self.membership_paths.append((method, membership_path))

            owner_by_id = {row.stable_id: row.page_ordinal for row in rows}
            evaluation = evaluate_layout(
                method,
                owner_by_id,
                {page: 1000 for page in range(4)},
                (self.ids,),
                EvaluationLimits(maximum_pages=4, maximum_bytes=4000),
            )
            evidence_path = root / f"evidence-{method.value}.parquet"
            written_evidence = write_coverage_parquet(evidence_path, evaluation)
            evidence_identity = dataclasses.replace(
                written_evidence,
                role=f"evidence:{method.value}",
                uri=f"s3://frozen/{evidence_path.name}",
            )
            evidence_identities.append(evidence_identity)
            self.evidence_paths.append((method, evidence_path))
            arms.append(
                {
                    "decision": evaluation.decision,
                    "evidence": dataclasses.asdict(evidence_identity),
                    "mean_recall_at_100_ppm": 1_000_000,
                    "membership": dataclasses.asdict(membership_identity),
                    "method": method.value,
                    "p05_recall_at_100_ppm": 1_000_000,
                    "recall_at_10_ppm": 1_000_000,
                    "worst_recall_at_100_ppm": 1_000_000,
                }
            )

        self.result_path = root / "result.json"
        self.result = {
            "arms": arms,
            "claim_eligible": False,
            "dimensions": 2,
            "limits": {"maximum_bytes": 4000, "maximum_pages": 4},
            "metric": "l2",
            "rows": 100,
            "schema": "borsuk-native-geometric-layout-screen-result-v1",
            "seed": self.seed,
            "source": dataclasses.asdict(self.source),
            "truth": dataclasses.asdict(self.truth),
        }
        self.write_result()
        self.expected = LayoutScreenAuthority(
            schema="borsuk-native-geometric-layout-screen-authority-v1",
            source=self.source,
            truth=self.truth,
            memberships=tuple(membership_identities),
            evidence=tuple(evidence_identities),
            result=artifact(self.result_path, "result"),
            rows=100,
            dimensions=2,
            metric="l2",
            seed=self.seed,
            limits=EvaluationLimits(maximum_pages=4, maximum_bytes=4000),
            expected_control_mean_ppm=1_000_000,
            expected_control_p05_ppm=1_000_000,
            expected_control_worst_ppm=1_000_000,
            control_tolerance_ppm=0,
        )
        self.paths = ValidationPaths(
            source=self.source_path,
            truth=self.truth_path,
            memberships=tuple(self.membership_paths),
            evidence=tuple(self.evidence_paths),
            result=self.result_path,
        )

    def write_result(self, *, canonical: bool = True) -> None:
        if canonical:
            payload = json.dumps(self.result, sort_keys=True, separators=(",", ":")) + "\n"
        else:
            payload = json.dumps(self.result, indent=2) + "\n"
        self.result_path.write_text(payload)

    def refresh_result_identity(self) -> None:
        self.expected = dataclasses.replace(
            self.expected, result=artifact(self.result_path, "result")
        )


class NativeGeometricResultValidationTests(unittest.TestCase):
    def test_validates_all_four_arms_and_returns_only_the_recomputed_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = ResultFixture(Path(directory))
            self.assertEqual(
                validate_result(fixture.paths, fixture.expected),
                ValidatedLayoutDecision(
                    decisions=(
                        (LayoutMethod.ID_ORDER_256, "control"),
                        (LayoutMethod.RANDOM_PROJECTION_256, "advance"),
                        (LayoutMethod.TWO_MEANS_256, "advance"),
                        (LayoutMethod.TWO_MEANS_480K, "advance"),
                    )
                ),
            )

    def test_rejects_registered_identity_and_canonical_byte_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = ResultFixture(Path(directory))
            wrong_source = dataclasses.replace(
                fixture.expected.source, sha256="ff" * 32
            )
            with self.assertRaises(ValueError):
                validate_result(
                    fixture.paths,
                    dataclasses.replace(fixture.expected, source=wrong_source),
                )
            fixture.write_result(canonical=False)
            fixture.refresh_result_identity()
            with self.assertRaises(ValueError):
                validate_result(fixture.paths, fixture.expected)

    def test_rejects_result_samples_aggregates_decisions_and_claim_drift(self) -> None:
        mutations = (
            ("claim", lambda result: result.__setitem__("claim_eligible", True)),
            (
                "aggregate",
                lambda result: result["arms"][1].__setitem__(
                    "mean_recall_at_100_ppm", 999_999
                ),
            ),
            (
                "decision",
                lambda result: result["arms"][1].__setitem__("decision", "killed"),
            ),
            ("seed", lambda result: result.__setitem__("seed", 7)),
            (
                "limit",
                lambda result: result["limits"].__setitem__("maximum_pages", 3),
            ),
        )
        for label, mutate in mutations:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                fixture = ResultFixture(Path(directory))
                mutate(fixture.result)
                fixture.write_result()
                fixture.refresh_result_identity()
                with self.assertRaises(ValueError):
                    validate_result(fixture.paths, fixture.expected)

    def test_rejects_membership_and_evidence_drift_after_identity_rebinding(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = ResultFixture(Path(directory))
            method, evidence_path = fixture.evidence_paths[1]
            table = pq.read_table(evidence_path)
            columns = list(table.columns)
            columns[3] = pa.chunked_array([pa.array([99], type=pa.uint8())])
            pq.write_table(pa.Table.from_arrays(columns, schema=table.schema), evidence_path)
            rebound_evidence = artifact(evidence_path, f"evidence:{method.value}")
            evidence = list(fixture.expected.evidence)
            evidence[1] = rebound_evidence
            fixture.result["arms"][1]["evidence"] = dataclasses.asdict(rebound_evidence)
            fixture.write_result()
            fixture.expected = dataclasses.replace(
                fixture.expected,
                evidence=tuple(evidence),
                result=artifact(fixture.result_path, "result"),
            )
            with self.assertRaises(ValueError):
                validate_result(fixture.paths, fixture.expected)

        with tempfile.TemporaryDirectory() as directory:
            fixture = ResultFixture(Path(directory))
            method, membership_path = fixture.membership_paths[1]
            table = pq.read_table(membership_path)
            columns = list(table.columns)
            page_ordinals = columns[2].combine_chunks().to_pylist()
            page_ordinals[0] = 1
            columns[2] = pa.chunked_array([pa.array(page_ordinals, type=pa.uint32())])
            pq.write_table(
                pa.Table.from_arrays(columns, schema=table.schema), membership_path
            )
            rebound_membership = artifact(
                membership_path, f"membership:{method.value}"
            )
            memberships = list(fixture.expected.memberships)
            memberships[1] = rebound_membership
            fixture.result["arms"][1]["membership"] = dataclasses.asdict(
                rebound_membership
            )
            fixture.write_result()
            fixture.expected = dataclasses.replace(
                fixture.expected,
                memberships=tuple(memberships),
                result=artifact(fixture.result_path, "result"),
            )
            with self.assertRaises(ValueError):
                validate_result(fixture.paths, fixture.expected)


if __name__ == "__main__":
    unittest.main()
