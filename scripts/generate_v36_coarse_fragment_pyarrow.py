#!/usr/bin/env python3
"""Generate the independent PyArrow V36 coarse-fragment IPC fixture."""

from __future__ import annotations

import argparse
import base64
import json
import pathlib

import pyarrow as pa
import pyarrow.ipc as ipc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=pathlib.Path)
    arguments = parser.parse_args()
    manifest = {
        "arm": "sign24",
        "codebook_sha256": None,
        "first_dense_ordinal": 7,
        "format": "borsuk-v36-coarse-fragment-v1",
        "fragment_ordinal": 0,
        "generation_manifest_sha256": "11" * 32,
        "last_dense_ordinal": 11,
        "owner_centroids_sha256": "22" * 32,
        "posting_ordinal": 4,
        "projection_sha256": "33" * 32,
        "row_count": 2,
    }
    schema = pa.schema(
        [
            pa.field("dense_ordinal", pa.uint64(), nullable=False),
            pa.field("source_feature_id", pa.uint64(), nullable=False),
            pa.field("code", pa.binary(24), nullable=False),
            pa.field("residual_norm", pa.float32(), nullable=False),
        ],
        metadata={
            b"borsuk.v36.coarse_fragment_manifest": json.dumps(
                manifest, separators=(",", ":"), sort_keys=True
            ).encode("ascii")
        },
    )
    batch = pa.record_batch(
        [
            pa.array([7, 11], type=pa.uint64()),
            pa.array([2**63 + 5, 2**64 - 3], type=pa.uint64()),
            pa.array([bytes(24), bytes([0x80]) + bytes(23)], type=pa.binary(24)),
            pa.array([0.0, 1.25], type=pa.float32()),
        ],
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    options = ipc.IpcWriteOptions(
        metadata_version=ipc.MetadataVersion.V5,
        use_legacy_format=False,
        compression=None,
    )
    with ipc.new_file(sink, schema, options=options) as writer:
        writer.write_batch(batch)
    encoded = base64.b64encode(sink.getvalue().to_pybytes()).decode("ascii")
    lines = [encoded[offset : offset + 76] for offset in range(0, len(encoded), 76)]
    arguments.output.write_text("\n".join(lines) + "\n", encoding="ascii")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
