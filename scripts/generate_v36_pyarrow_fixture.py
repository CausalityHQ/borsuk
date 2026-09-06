#!/usr/bin/env python3
"""Generate the independent PyArrow V36 registered-input contract fixture."""

from __future__ import annotations

import argparse
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=pathlib.Path)
    arguments = parser.parse_args()
    values = pa.array([1.0, *([0.0] * 767)], type=pa.float32())
    embeddings = pa.FixedSizeListArray.from_arrays(values, 768)
    table = pa.table(
        {
            "url": pa.array([None], type=pa.string()),
            "natural_score": pa.array([None], type=pa.float32()),
            "feature_row_id": pa.array([7], type=pa.int64()),
            "embedding": embeddings,
        }
    )
    pq.write_table(
        table,
        arguments.output,
        compression="NONE",
        row_group_size=1,
        use_compliant_nested_type=False,
        use_dictionary=False,
        version="2.6",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
