"""Small 768D fixtures for source-only one-million group selection."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from scripts.native_one_million_group_selector import (
    _source_schema,
    build_selector,
    read_selector,
)
from scripts.v97_row_width_screen import ObjectIdentity

DIMENSIONS = 768


def _identity(path: Path) -> ObjectIdentity:
    body = path.read_bytes()
    return ObjectIdentity("s3://frozen/" + path.name, hashlib.sha256(body).hexdigest(), len(body))


def _page_body(row_id: int) -> bytes:
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("sequence", pa.uint64(), nullable=False),
        pa.field("state", pa.uint8(), nullable=False),
        pa.field("code", pa.list_(pa.field("element", pa.uint8(), nullable=False), DIMENSIONS), nullable=False),
    ])
    table = pa.Table.from_arrays([
        pa.array([row_id], type=pa.int64()),
        pa.array([0], type=pa.uint64()),
        pa.array([0], type=pa.uint8()),
        pa.FixedSizeListArray.from_arrays(pa.array(np.zeros(DIMENSIONS, dtype=np.uint8)), DIMENSIONS),
    ], schema=schema)
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _fixture(root: Path, *, duplicate_delta: bool = False, omit_source: bool = False):
    generation = {"schema": "fixture", "dimensions": DIMENSIONS, "runs": []}
    for role, ids in (("base", tuple(range(9))), ("delta", (8 if duplicate_delta else 9,))):
        body = bytearray()
        pages = []
        for page, row_id in enumerate(ids):
            chunk = _page_body(row_id)
            pages.append({"page": page, "offset": len(body), "bytes": len(chunk), "rows": 1})
            body.extend(chunk)
        path = root / (role + ".arrow")
        path.write_bytes(body)
        generation["runs"].append({"kind": role, "object": dataclasses.asdict(_identity(path)), "pages": pages})
    (root / "generation.json").write_text(json.dumps(generation, sort_keys=True, separators=(",", ":")) + "\n")
    (root / "router.arrow").write_bytes(b"source-only-router-fixture")
    ids = list(range(10 if not omit_source else 9))[::-1]
    vectors = np.zeros((len(ids), DIMENSIONS), dtype=np.float32)
    for index, row_id in enumerate(ids):
        vectors[index, 0] = row_id
    table = pa.Table.from_arrays([
        pa.array(ids, type=pa.uint64()),
        pa.FixedSizeListArray.from_arrays(
            pa.array(vectors.reshape(-1), type=pa.float32()),
            type=pa.list_(pa.field("item", pa.float32(), nullable=False), DIMENSIONS),
        ),
    ], schema=_source_schema())
    pq.write_table(table, root / "source.parquet")
    return {role: _identity(root / name) for role, name in {
        "source": "source.parquet", "generation": "generation.json", "base": "base.arrow",
        "delta": "delta.arrow", "router": "router.arrow",
    }.items()}


class OneMillionGroupSelectorTests(unittest.TestCase):
    def test_source_only_centroids_and_membership_are_batch_stable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = _fixture(root)
            first = build_selector(root, root / "first", identities, batch_rows=2)
            build_selector(root, root / "second", identities, batch_rows=3)
            self.assertEqual((root / "first/centroids.bin").read_bytes(), (root / "second/centroids.bin").read_bytes())
            self.assertEqual((root / "first/membership.bin").read_bytes(), (root / "second/membership.bin").read_bytes())
            self.assertEqual(len(first.groups), 3)
            self.assertEqual([group.row_count for group in first.groups], [8, 1, 1])
            self.assertEqual([group.code_bytes for group in first.groups], [1636, 208, 208])
            self.assertEqual(first.centroids[:, 0].tolist(), [3.5, 8.0, 9.0])
            self.assertEqual(first.membership_groups.tolist(), [0] * 8 + [1, 2])
            self.assertEqual(read_selector(root / "first", identities).seal, first.seal)
            self.assertEqual(first.seal["source_identities"]["router"]["sha256"], identities["router"].sha256)
            (root / "first/centroids.bin").write_bytes(b"x" + (root / "first/centroids.bin").read_bytes()[1:])
            with self.assertRaisesRegex(ValueError, "centroids identity"):
                read_selector(root / "first", identities)
            membership = root / "second/membership.bin"
            membership.write_bytes(b"x" + membership.read_bytes()[1:])
            with self.assertRaisesRegex(ValueError, "membership identity"):
                read_selector(root / "second", identities)

    def test_duplicate_or_missing_id_and_corrupt_page_fail_before_seal(self) -> None:
        for duplicate, missing in ((True, False), (False, True)):
            with self.subTest(duplicate=duplicate, missing=missing), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                identities = _fixture(root, duplicate_delta=duplicate, omit_source=missing)
                with self.assertRaisesRegex(ValueError, "rows|source"):
                    build_selector(root, root / "out", identities, batch_rows=2)
                self.assertFalse((root / "out/seal.json").exists())
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = _fixture(root)
            with (root / "base.arrow").open("ab") as handle:
                handle.write(b"altered")
            with self.assertRaisesRegex(ValueError, "base identity"):
                build_selector(root, root / "out", identities, batch_rows=2)


if __name__ == "__main__":
    unittest.main()
