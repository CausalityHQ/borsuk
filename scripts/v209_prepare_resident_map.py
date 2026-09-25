#!/usr/bin/env python3
"""Convert authenticated V115/V164 research artifacts to the V209 generation."""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np

V1_SHA = "d558a77443d6a1a50b9b3d01e821f134b1cc0992aa8bcb7ef3dc9ed2941221fe"
V2_SHA = "c188766121a6e77f48cdc705bf579431192f93548d2e322bba0f429506adb36e"
OLD_LAYOUT_SHA = "32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b"
NEW_ORDER_SHA = "5b5ef48d86570e5ca68fdaaac9aef231ec7368dd526baef00474cd0a2f59a06f"
ROWS = 1_000_000


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run(args: argparse.Namespace) -> None:
    if (sha(args.router / "manifest-v1.json") != V1_SHA
            or sha(args.old_layout) != OLD_LAYOUT_SHA
            or sha(args.order) != NEW_ORDER_SHA):
        raise ValueError("frozen V115/V164 identity differs")
    manifest = json.loads((args.router / "manifest-v1.json").read_bytes())
    if (manifest["schema"] != "borsuk-v115-source-router-v1"
            or manifest["generation"] != 1
            or manifest["geometry"]["rows"] != ROWS
            or manifest["geometry"]["dimensions"] != 768
            or manifest["geometry"]["subspaces"] != 64):
        raise ValueError("frozen router geometry differs")
    manifest["schema"] = "borsuk-source-router-v2"
    manifest["generation"] = 196
    manifest["geometry"]["pq_partition"] = "balanced_floor_v1"
    manifest["source_manifest_sha256"] = V1_SHA
    (args.router / "manifest.json").write_bytes(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode())
    if sha(args.router / "manifest.json") != V2_SHA:
        raise ValueError("converted router manifest differs")
    old = np.load(args.old_layout, allow_pickle=False)
    new_order = np.load(args.order, allow_pickle=False)
    if (old.shape != (ROWS,) or new_order.shape != (ROWS,)
            or old.dtype not in (np.int32, np.int64) or new_order.dtype != np.int64
            or not np.array_equal(np.sort(old), np.arange(ROWS))
            or not np.array_equal(np.sort(new_order), np.arange(ROWS))):
        raise ValueError("old/new physical orders are not bijections")
    inverse_old = np.empty(ROWS, dtype=np.int64)
    inverse_old[old] = np.arange(ROWS)
    new_to_old = inverse_old[new_order]
    if (new_to_old.min() != 0 or new_to_old.max() != ROWS - 1
            or np.unique(new_to_old).size != ROWS):
        raise ValueError("physical row map is not bijective")
    args.map.write_bytes(new_to_old.astype("<u4").tobytes())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--router", type=Path, required=True)
    parser.add_argument("--old-layout", type=Path, required=True)
    parser.add_argument("--order", type=Path, required=True)
    parser.add_argument("--map", type=Path, required=True)
    run(parser.parse_args())
