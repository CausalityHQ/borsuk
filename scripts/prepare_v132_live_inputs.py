#!/usr/bin/env python3
"""Stage and authenticate the sealed V132 live-serving worker inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

BUCKET = "borsuk-bench-453182569524-euc1"
PREFIX = (
    "research/v132-live-s3/01c4590b80b540bfb5e4f01909362d1c3ab487f9/"
    "generation-131-a0001"
)
MANIFEST_SHA = "968ef7d795b53e5869400998ca19c6ab20d9b39830295fac3081c419f63f0f20"
TERMINAL_SHA = "0ec4b70965d11c83eec1c5f0ee70c2f181fb2954df1429c8f02f57837842f4a6"
V131_REPLAY = (
    "s3://borsuk-bench-453182569524-euc1/"
    "research/v131-source-replay/56f7071716681fde524432d29a436015584df773/"
    "runs/v131-20260924T052935Z/a0001/artifacts/built/replay.jsonl"
)
V131_REPLAY_SHA = "9c687edc8ea66a5ef41b1e83d1d51788022b58cf526f2e9dcdb21a38bb5dd7d9"
V131_REPLAY_BYTES = 2_956_987

LOCAL = {
    "router_manifest": ("derived", "router_manifest", "router/manifest.json"),
    "router_summaries": ("inputs", "router_summaries", "router/summaries.bin"),
    "router_books": ("inputs", "router_books", "router/books.bin"),
    "router_codes": ("inputs", "router_codes", "router/codes.bin"),
    "router_low": ("inputs", "router_low", "router/low.bin"),
    "router_step": ("inputs", "router_step", "router/step.bin"),
    "sq8": ("inputs", "sq8", "sq8.bin"),
    "source_tier": ("inputs", "source_tier", "source-tier.bin"),
    "source_id_map": ("inputs", "source_id_map", "source-id-map.bin"),
    "page_manifest": ("derived", "page_manifest", "page-manifest.json"),
    "page_digests": ("derived", "page_digests", "page-digests.bin"),
    "mirror_digests": ("derived", "mirror_digests", "mirror-digests.bin"),
    "queries": ("inputs", "queries.jsonl", "queries.jsonl"),
    "evidence": ("inputs", "evidence.jsonl", "evidence.jsonl"),
}


def fetch(
    uri: str,
    expected_sha: str,
    expected_bytes: int,
    target: Path,
    expected_etag: str | None = None,
) -> dict[str, Any]:
    bucket, slash, key = uri.removeprefix("s3://").partition("/")
    if not slash or bucket != BUCKET or not key or target.exists():
        raise ValueError("input URI, bucket or local target differs")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".partial")
    command = [
        "aws",
        "s3api",
        "get-object",
        "--region",
        "eu-central-1",
        "--bucket",
        bucket,
        "--key",
        key,
    ]
    if expected_etag is not None:
        command.extend(("--if-match", expected_etag))
    command.extend(("--output", "json", str(temporary)))
    response = json.loads(subprocess.check_output(command, text=True))
    if expected_etag is not None and response["ETag"] != expected_etag:
        raise ValueError("input ETag differs")
    digest = hashlib.sha256()
    count = 0
    with temporary.open("rb") as downloaded:
        for block in iter(lambda: downloaded.read(4 * 1024 * 1024), b""):
            count += len(block)
            if count > expected_bytes:
                raise ValueError("input exceeds pinned length")
            digest.update(block)
    if count != expected_bytes or digest.hexdigest() != expected_sha:
        raise ValueError("input length or SHA-256 differs")
    os.replace(temporary, target)
    return {
        "uri": uri,
        "bytes": count,
        "sha256": digest.hexdigest(),
        "etag": response["ETag"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    started = time.monotonic_ns()
    args.output.mkdir(parents=True, exist_ok=False)
    base = f"s3://{BUCKET}/{PREFIX}"
    terminal_record = fetch(
        base + "/terminal.json",
        TERMINAL_SHA,
        432,
        args.output / "terminal.json",
    )
    terminal = json.loads((args.output / "terminal.json").read_bytes())
    if (
        terminal.get("status") != "complete"
        or terminal.get("generation") != 131
        or terminal.get("generation_manifest_sha256") != MANIFEST_SHA
    ):
        raise ValueError("generation preparation terminal differs")
    manifest_record = fetch(
        base + "/generation.json",
        MANIFEST_SHA,
        5_640,
        args.output / "generation.json",
    )
    manifest = json.loads((args.output / "generation.json").read_bytes())
    if (
        manifest.get("schema") != "borsuk-v132-live-serving-generation-v1"
        or manifest.get("generation") != 131
    ):
        raise ValueError("generation authority differs")
    records = {"terminal": terminal_record, "generation": manifest_record}
    for role, (section, name, relative) in LOCAL.items():
        record = manifest[section][name]
        records[role] = fetch(
            record["uri"],
            record["sha256"],
            record["bytes"],
            args.output / relative,
            record["etag"],
        )
    records["v131_replay"] = fetch(
        V131_REPLAY,
        V131_REPLAY_SHA,
        V131_REPLAY_BYTES,
        args.output / "replay.jsonl",
    )
    summary = {
        "schema": "borsuk-v132-live-inputs-v1",
        "records": records,
        "elapsed_ns": time.monotonic_ns() - started,
    }
    (args.output / "prepare-summary.json").write_text(
        json.dumps(summary, sort_keys=True, separators=(",", ":"))
    )
    print(
        json.dumps(
            {
                "input_count": len(records),
                "download_bytes": sum(record["bytes"] for record in records.values()),
                "elapsed_ns": summary["elapsed_ns"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
