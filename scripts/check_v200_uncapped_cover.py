#!/usr/bin/env python3
"""Independent closed V200 identity, weight and exact-witness replay."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

from scripts.unconstrained_priced_interval import unconstrained_priced_cover
from scripts.v200_uncapped_weights import prepare

TERMINAL_SHA = "1520355b548e1f1d9322a34a19b8cc44ee70efb54f3964c1f8dd2a1e42cd2429"
HASHES = {
    "bench-resources.txt": "889137567d6b4b62f6cab5fb86921a5a11fe2e67a96397bb025413008227438a",
    "bench.json": "482a8a526f049c16e838240ab6c6a7dbda0709038b390399fd527878f14acf93",
    "build.log": "d76084c5cbb7fd97408d1dc8238e0d9f63c3b0490a1ae14f3d926e631b0149a4",
    "run-closed.log": "c4d4ec13b3f88cbab31ae2f9a80cb9618dc290ecc46524ff9ecfd2d8c967a802",
    "weight-seal.json": "375db5cf3ba9662eb12f5de56e206fd7b4b9156011a2c44ead36e262bc44439c",
    "weights-resources.txt": "6aa22609b84c07eefc9ae10d503934f4158124a9a1c36c414dd587b34731992d",
    "weights.jsonl": "797a83a7d830afd5cec491e70de3f49023a52c2f972af2045704b68df6f76313",
}


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def check(root: Path, features_path: Path, plans_path: Path,
          fit_path: Path) -> dict:
    if digest(root / "terminal.json") != TERMINAL_SHA:
        raise ValueError("V200 terminal SHA-256 differs")
    terminal = json.loads((root / "terminal.json").read_text())
    if (terminal.get("schema") != "borsuk-v200-uncapped-cover-spot-v1"
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0
            or terminal.get("instance_id") != "i-00aedf7afc2fc51c0"
            or terminal.get("source_commit") != "b573a2fcf6300d2d37acd400521d9a1a504f8b61"
            or terminal.get("source_archive_sha256") !=
                "71ca1aca41584b76acde0a240361866cbaf5f5e48c9b6c7a766ae92a74b88693"
            or set(terminal.get("artifacts", {})) != set(HASHES)):
        raise ValueError("V200 terminal identity differs")
    for name, expected in HASHES.items():
        path = root / "artifacts" / name
        if (digest(path) != expected or terminal["artifacts"][name] !=
                {"bytes": path.stat().st_size, "sha256": expected}):
            raise ValueError(f"V200 artifact differs: {name}")
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "weights.jsonl"
        independent_seal = prepare(features_path, plans_path, fit_path, path)
        if digest(path) != HASHES["weights.jsonl"]:
            raise ValueError("V200 independent model weights differ")
    seal = json.loads((root / "artifacts/weight-seal.json").read_text())
    if seal != independent_seal or seal.get("source_truth_opened") is not False:
        raise ValueError("V200 GT-blind weight seal differs")
    weights = records(root / "artifacts/weights.jsonl")
    plans = records(plans_path)
    if len(weights) != 1000 or len(plans) != 1000:
        raise ValueError("V200 paired cohort differs")
    admitted = over_units = over_gets = 0
    for index, (row, plan) in enumerate(zip(weights, plans, strict=True)):
        if row["ordinal"] != index or plan["ordinal"] != index:
            raise ValueError(f"V200 ordinal differs at {index}")
        pairs = row["weights"]
        if pairs != sorted(pairs) or len({unit for unit, _ in pairs}) != len(pairs):
            raise ValueError(f"V200 weight geometry differs at {index}")
        cover = unconstrained_priced_cover(
            dict(pairs), row["mandatory"], page_count=31_250,
            unit_price=1000, get_price=50_000)
        if cover.units <= plan["unit_cap"] and cover.gets <= 32:
            admitted += 1
            prior = plan["optional_risk"]
            if (list(map(list, cover.intervals)) != prior["intervals"]
                    or cover.mass != prior["predicted_mass"]
                    or cover.units != prior["units"]
                    or cover.gets != prior["gets"]):
                raise ValueError(f"V200 exact interval witness differs at {index}")
        else:
            over_units += cover.units > plan["unit_cap"]
            over_gets += cover.gets > 32
    bench = json.loads((root / "artifacts/bench.json").read_text())
    if (bench.get("schema") != "borsuk-v200-uncapped-cover-preflight-v1"
            or bench.get("queries") != 1000
            or bench.get("fast_path_admitted") != admitted
            or bench.get("exact_interval_witnesses") != admitted
            or bench.get("fallback_required") != 1000 - admitted
            or bench.get("over_unit_cap") != over_units
            or bench.get("over_get_cap") != over_gets
            or not (0 < bench["admitted_ns"]["p50"] <=
                    bench["admitted_ns"]["p95"] <=
                    bench["admitted_ns"]["p99"])
            or bench["admitted_ns"]["p95"] > 5_000_000):
        raise ValueError("V200 Rust parity or latency guard differs")
    return {"status":"pass", "decision":"advance-capped-fallback",
            "admitted":admitted,"fallback_required":1000-admitted,
            "over_unit_cap":over_units,"over_get_cap":over_gets,
            "admitted_p95_ns":bench["admitted_ns"]["p95"],
            "all_uncapped_p95_ns":bench["all_uncapped_ns"]["p95"],
            "peak_process_rss_bytes":bench["peak_process_rss_bytes"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root",type=Path)
    parser.add_argument("features",type=Path)
    parser.add_argument("plans",type=Path)
    parser.add_argument("fit",type=Path)
    args = parser.parse_args()
    print(json.dumps(check(args.root,args.features,args.plans,args.fit),sort_keys=True))


if __name__ == "__main__":
    main()
