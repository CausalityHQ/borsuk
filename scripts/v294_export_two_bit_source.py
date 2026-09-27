"""Query-blind historical two-bit source receipt for Rust parity."""

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.native_rotated_two_bit_codes import BATCH_ROWS, _fit_records
from scripts.v284_page_primary_dev64 import sha
from scripts.v285_exact_page_rank_bound import LAYOUT_SHA, RAW_SHA


def export(raw, layout, output):
    assert sha(raw.read_bytes()) == RAW_SHA and sha(layout.read_bytes()) == LAYOUT_SHA
    assert not output.exists()
    order = np.load(layout, allow_pickle=False)
    assert order.shape == (100_000,) and np.array_equal(np.sort(order), np.arange(100_000))
    vectors = np.memmap(raw, dtype="<f4", mode="r", shape=(100_000, 768))
    mean = vectors.mean(axis=0, dtype=np.float64).astype("<f4")
    assert np.isfinite(mean).all()
    records = np.empty((100_000, 200), dtype=np.uint8)
    for first in range(0, 100_000, BATCH_ROWS):
        last = min(first+BATCH_ROWS, 100_000)
        rows = np.asarray(vectors[order[first:last]], dtype=np.float64)
        assert np.isfinite(rows).all()
        records[first:last] = _fit_records(rows-mean.astype(np.float64), rotation_seed=20260923)
    output.mkdir()
    mean.tofile(output/"mean.bin")
    records.tofile(output/"reference.bin")
    receipt = {"schema":"borsuk-v294-source-reference-v1", "rows":100_000,
               "dimensions":768, "seed":20260923, "record_bytes":200,
               "source_sha256":RAW_SHA, "layout_sha256":LAYOUT_SHA,
               "mean_sha256":sha(mean.tobytes()), "reference_sha256":sha(records.tobytes()),
               "prefix_plane_sha256":sha(records[:,:196].tobytes()), "query_or_truth_used":False}
    (output/"receipt.json").write_text(json.dumps(receipt, sort_keys=True, separators=(",", ":"))+"\n")
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("raw", "layout", "output"):
        parser.add_argument("--"+name, type=Path, required=True)
    args = parser.parse_args()
    export(args.raw, args.layout, args.output)
