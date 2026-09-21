from __future__ import annotations

import dataclasses
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    LayoutAuthority,
    LayoutMethod,
    MembershipRow,
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


if __name__ == "__main__":
    unittest.main()
