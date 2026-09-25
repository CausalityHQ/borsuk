#!/usr/bin/env python3
"""Replay the closed V199 live Rust transport and paired returned quality."""

from __future__ import annotations

import argparse
import hashlib
import json
from math import ceil
from pathlib import Path

TERMINAL_SHA = "c072215e0c4fd604d10012b219079f758f7e8efa6392ddaf5fd530150d258097"
ARTIFACT_HASHES = {
    "build.log": "7611e0f5c8e847cbdff2a6755b25be61d013d17c876394385711fcf9f97d5c9f",
    "live-mismatch.json": "b3caf14730ad268213182cf65f985913726b9767ffa2c14120a0360c5600868d",
    "live-raw.jsonl": "aa7da295092ba7eb679e28dbc01e6ba0be4c625133ae9b5ca84bab5f6223eced",
    "live-resources.txt": "b6faf93f414fd464aa4e1f3ebea71dd131d3e87ad500d2138c79ddfbaf69ec55",
    "live-summary.json": "9d6dd30fb6b76c575989bd9a2858a6d6cb9ae603227da5de8646c6f73f8bda40",
    "page-build.json": "abb94c449e1af93c2810790583561eccfb9729e51d81f9baab173560a22d9102",
    "page-digests.bin": "0557740acdf82da45b18500f1b0338e5c6a2729067abb5cfbef336801359f332",
    "page-manifest.json": "4012a47955d139011ffae82c82d5552a8a8b7f9226c07d8af62f84c2a39c6caa",
    "page-resources.txt": "453b9cf86d07eba2244d87f6ce1d9b956a16341b5ff0f2cd22983c6c85606174",
    "run-closed.log": "394ee50e78765d737d60d5b6db81b768adcff69bd36c0c703f8430cb3dbab073",
}
V198_HASHES = {
    "raw.jsonl": "2b18321435642de3fad4df02b84abcc046fb6c9b17808e72d6a50eea54a9ec98",
    "plans.jsonl": "0a61974457030d2e2ce828e7bbbbaf3d8acd9849e70a7aaafd0cb44b0c5dda00",
    "cases.jsonl": "e1ba95fbd0cb58e28f1601740edf8c8775230e2ab5c8954e7f11c522dd94cd39",
    "summary.json": "69e30c056e744663e3d79d641d3e5412e9db69280092cd7c96f528e1dccb60c4",
}
OBJECT_SHA = "aecf0f2704f44906f411a74ab81b36e5e05f81bab35f4c70558e88acbc4d05c9"
ETAG = '"fbdab9e60d3b28a988d5109d4febb4fb-93"'


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def percentile(values: list[int], p: int) -> int:
    ordered = sorted(values)
    return ordered[ceil(len(ordered) * p / 100) - 1]


def check(live_root: Path, v198_root: Path) -> dict:
    if digest(live_root / "terminal.json") != TERMINAL_SHA:
        raise ValueError("V199 terminal SHA-256 differs")
    terminal = json.loads((live_root / "terminal.json").read_text())
    if (terminal.get("schema") != "borsuk-v199-live-s3-resident-fp16-spot-v1"
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0
            or terminal.get("instance_id") != "i-045b9ab718def47a0"
            or terminal.get("source_commit") != "5221e5c2075e59f8d068a182a406dbc33d095d89"
            or terminal.get("source_archive_sha256") !=
                "38f3c0a77f797a5a70745e36654a76dbefe0fdc7c5a1d7ca21cb6d0bf4347b05"
            or set(terminal.get("artifacts", {})) != set(ARTIFACT_HASHES)):
        raise ValueError("V199 complete terminal identity differs")
    for name, expected in ARTIFACT_HASHES.items():
        path = live_root / "artifacts" / name
        if (digest(path) != expected
                or terminal["artifacts"][name] !=
                    {"bytes": path.stat().st_size, "sha256": expected}):
            raise ValueError(f"V199 artifact identity differs: {name}")
    for name, expected in V198_HASHES.items():
        if digest(v198_root / name) != expected:
            raise ValueError(f"V198 paired authority differs: {name}")
    summary = json.loads((live_root / "artifacts/live-summary.json").read_text())
    page = json.loads((live_root / "artifacts/page-manifest.json").read_text())
    build = json.loads((live_root / "artifacts/page-build.json").read_text())
    mismatch = json.loads((live_root / "artifacts/live-mismatch.json").read_text())
    if (page != {"schema":"borsuk-v115-sq8-page-authority-v2",
                 "generation":196,"rows":1_000_000,"dimensions":768,
                 "page_rows":32,"object_sha256":OBJECT_SHA,
                 "page_digest_sha256":ARTIFACT_HASHES["page-digests.bin"]}
            or build.get("object_sha256") != OBJECT_SHA
            or build.get("manifest_sha256") != ARTIFACT_HASHES["page-manifest.json"]
            or build.get("page_digest_sha256") != ARTIFACT_HASHES["page-digests.bin"]
            or build.get("page_count") != 31_250
            or mismatch.get("ordinal") != 617
            or mismatch.get("sets_equal") is not False
            or mismatch.get("intersection") != 127):
        raise ValueError("V199 object/page or numeric discrepancy differs")
    live = records(live_root / "artifacts/live-raw.jsonl")
    prior = records(v198_root / "raw.jsonl")
    plans = records(v198_root / "plans.jsonl")
    cases = records(v198_root / "cases.jsonl")
    if any(len(rows) != 1000 for rows in (live, prior, plans, cases)):
        raise ValueError("V199 paired cohort differs")
    hits = []
    set_mismatches = ordered_mismatches = returned_mismatches = 0
    gets = payload_bytes = 0
    for index, (row, prior_row, plan, case) in enumerate(
            zip(live, prior, plans, cases, strict=True)):
        arm = plan["optional_risk"]
        returned = row["returned_ids"]
        if (row["ordinal"] != index or prior_row["ordinal"] != index
                or plan["ordinal"] != index or case["ordinal"] != index
                or len(returned) != 100 or len(set(returned)) != 100
                or row["gets"] != arm["gets"]
                or row["bytes"] != arm["bytes"]
                or not all(isinstance(row[key], int) and row[key] > 0
                           for key in ("s3_ns", "sq8_ns", "fp16_ns", "total_ns"))
                or row["total_ns"] < max(row["s3_ns"],row["sq8_ns"],row["fp16_ns"])):
            raise ValueError(f"V199 live record differs at {index}")
        actual = len(set(returned) & set(prior_row["gold_ids"]))
        hits.append(actual)
        gets += row["gets"]
        payload_bytes += row["bytes"]
        set_mismatches += not row["sq8_set_parity"]
        ordered_mismatches += not row["sq8_ordered_parity"]
        returned_mismatches += not row["fp16_ordered_parity"]
        if row["fp16_ordered_parity"] != (
                returned == prior_row["optional_risk"]["fp16_returned_ids"]
                == case["expected"]):
            raise ValueError(f"V199 returned parity indicator differs at {index}")
    if (summary.get("schema") != "borsuk-v199-live-s3-resident-fp16-v1"
            or summary.get("queries") != 1000
            or summary.get("etag") != ETAG
            or summary.get("page_manifest_sha256") != ARTIFACT_HASHES["page-manifest.json"]
            or summary.get("submitted_gets") != gets
            or summary.get("response_bytes") != payload_bytes
            or summary.get("sq8_shortlist_set_mismatch_queries") != set_mismatches
            or summary.get("sq8_ordered_mismatch_queries") != ordered_mismatches
            or summary.get("fp16_returned_mismatch_queries") != returned_mismatches):
        raise ValueError("V199 summary charge/parity differs")
    for phase in ("s3_ns", "sq8_ns", "fp16_ns", "total_ns"):
        values = [row[phase] for row in live]
        if summary.get(phase) != {f"p{p}": percentile(values, p) for p in (50,95,99)}:
            raise ValueError(f"V199 {phase} percentile differs")
    ordered = sorted(hits)
    qualifies = (sum(hits) >= 99_567 and ordered[49] >= 98
                 and gets == 10_047 and payload_bytes == 7_388_559_360
                 and gets <= 22_126 and payload_bytes <= 11_134_007_040)
    return {"status":"pass", "decision":"advance-production-planner" if qualifies else
            "revise-scoring-or-precision", "rust_returned_hits":sum(hits),
            "p05_hits":ordered[49], "min_hits":ordered[0],
            "below_98":sum(hit < 98 for hit in hits),
            "sq8_set_mismatches":set_mismatches,
            "sq8_ordered_mismatches":ordered_mismatches,
            "fp16_returned_mismatches":returned_mismatches,
            "observed_gets":gets,"observed_payload_bytes":payload_bytes,
            "latency_ns":summary["total_ns"],
            "peak_process_rss_bytes":summary["peak_process_rss_bytes"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("live_root", type=Path)
    parser.add_argument("v198_root", type=Path)
    args = parser.parse_args()
    print(json.dumps(check(args.live_root,args.v198_root),sort_keys=True))


if __name__ == "__main__":
    main()
