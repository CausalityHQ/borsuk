#!/usr/bin/env python3
"""Publish one immutable 100k live-serving generation from sealed V122/V131 inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
import subprocess
from typing import Any

import boto3

BUCKET = "borsuk-bench-453182569524-euc1"
V122 = (
    "research/v122-deep-image-100k/afe07cb5a9ba8518263375595f589639fdf3f4f1/"
    "runs/v122-20260924T011355Z/a0001"
)
V131 = (
    "research/v131-source-replay/56f7071716681fde524432d29a436015584df773/"
    "runs/v131-20260924T052935Z/a0001"
)
V122_TERMINAL_SHA = "5475dfdb8608b80c2dbdc3550721d9f5d106fb79c613cbc83cb40a7a29a4a89c"
V131_TERMINAL_SHA = "7a68269ddd56f940c0896b75e42b87d7e498cbd2634d869ee1be4e89d8a21534"
SOURCE_SHA = "da3ad1295d6031818b7ccb817529c6102e9f0ec93bb4ad0792e2b7ab3cd21e69"
SQ8_SHA = "c20dcb8058d2409791c6c584d9f078491d4c239acbe7c19350be7757d533e8df"
GENERATION = 131
ROWS = 100_000
DIMENSIONS = 96
PAGE_ROWS = 256


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def canonical(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def read_checked(
    s3: Any, prefix: str, path: str, terminal: dict[str, Any]
) -> tuple[bytes, dict[str, Any]]:
    expected = terminal["artifacts"][path]
    key = f"{prefix}/artifacts/{path}"
    result = s3.get_object(Bucket=BUCKET, Key=key)
    body = result["Body"].read()
    if len(body) != expected["bytes"] or digest(body) != expected["sha256"]:
        raise ValueError(f"sealed artifact differs: {path}")
    return body, {
        "uri": f"s3://{BUCKET}/{key}",
        "bytes": len(body),
        "sha256": digest(body),
        "etag": result["ETag"],
    }


def terminal_checked(
    s3: Any, prefix: str, expected_sha: str, expected_schema: str
) -> dict[str, Any]:
    raw = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/terminal.json")["Body"].read()
    if digest(raw) != expected_sha:
        raise ValueError("sealed terminal SHA-256 differs")
    value = json.loads(raw)
    if (
        value.get("schema") != expected_schema
        or value.get("status") != "complete"
        or value.get("exit_code") != 0
    ):
        raise ValueError("sealed source is incomplete")
    return value


def publish(s3: Any, prefix: str, name: str, body: bytes) -> dict[str, Any]:
    key = f"{prefix}/{name}"
    result = s3.put_object(Bucket=BUCKET, Key=key, Body=body, IfNoneMatch="*")
    received = s3.get_object(Bucket=BUCKET, Key=key)
    check = received["Body"].read()
    if check != body or result["ETag"] != received["ETag"]:
        raise ValueError(f"published object differs: {name}")
    return {
        "uri": f"s3://{BUCKET}/{key}",
        "bytes": len(body),
        "sha256": digest(body),
        "etag": received["ETag"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if (
        not args.dry_run
        and subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
    ):
        raise ValueError("publisher requires a clean source commit")
    prefix = args.output_prefix.removeprefix(f"s3://{BUCKET}/").rstrip("/")
    if (
        prefix == args.output_prefix
        or not prefix.startswith("research/v132-live-s3/")
        or commit not in prefix
        or not prefix.endswith("/generation-131-a0001")
    ):
        raise ValueError("immutable output prefix differs")
    s3 = boto3.Session(profile_name="causality", region_name="eu-central-1").client(
        "s3"
    )
    old = terminal_checked(
        s3, V122, V122_TERMINAL_SHA, "borsuk-v122-deep-image-100k-spot-v1"
    )
    source = terminal_checked(
        s3, V131, V131_TERMINAL_SHA, "borsuk-v131-source-replay-spot-v1"
    )
    inputs: dict[str, dict[str, Any]] = {}
    router_raw, inputs["router_old_manifest"] = read_checked(
        s3, V122, "router/manifest.json", old
    )
    router = json.loads(router_raw)
    if (
        router["schema"] != "borsuk-source-router-v2"
        or router["generation"] != 1
        or router["geometry"]["rows"] != ROWS
        or router["geometry"]["dimensions"] != DIMENSIONS
        or router["geometry"]["page_rows"] != PAGE_ROWS
        or router["source_sha256"] != SOURCE_SHA
        or router["sq8_sha256"] != SQ8_SHA
    ):
        raise ValueError("sealed router identity differs")
    for name in ("summaries", "books", "codes", "low", "step"):
        body, inputs[f"router_{name}"] = read_checked(
            s3, V122, f"router/{name}.bin", old
        )
        if (
            len(body) != router["sections"][name]["bytes"]
            or digest(body) != router["sections"][name]["sha256"]
        ):
            raise ValueError(f"router section differs: {name}")
    sq8, inputs["sq8"] = read_checked(s3, V122, "built/sq8.bin", old)
    if digest(sq8) != SQ8_SHA or len(sq8) != ROWS * (DIMENSIONS + 12):
        raise ValueError("sealed SQ8 body differs")
    for path in ("queries.jsonl", "truth.npy", "evidence.jsonl"):
        _, inputs[path] = read_checked(s3, V122, path, old)
    tier, inputs["source_tier"] = read_checked(
        s3, V131, "built/source-tier.bin", source
    )
    id_map, inputs["source_id_map"] = read_checked(
        s3, V131, "built/source-id-map.bin", source
    )
    source_hash = bytes.fromhex(SOURCE_SHA)
    if (
        tier[:8] != b"BORSST02"
        or struct.unpack_from("<IIQQ", tier, 8) != (2, DIMENSIONS, ROWS, GENERATION)
        or tier[32:64] != source_hash
    ):
        raise ValueError("source tier generation differs")
    if (
        id_map[:8] != b"BORSMAP1"
        or struct.unpack_from("<I", id_map, 8)[0] != 1
        or struct.unpack_from("<QQ", id_map, 16) != (ROWS, GENERATION)
        or id_map[32:64] != source_hash
        or id_map[64:96] != bytes.fromhex(inputs["source_tier"]["sha256"])
    ):
        raise ValueError("source map generation differs")
    router["generation"] = GENERATION
    page_bytes = PAGE_ROWS * (DIMENSIONS + 12)
    page_digests = b"".join(
        hashlib.sha256(sq8[start : start + page_bytes]).digest()
        for start in range(0, len(sq8), page_bytes)
    )
    mirror_digests = b"".join(
        hashlib.sha256(sq8[start : start + 4096]).digest()
        for start in range(0, len(sq8), 4096)
    )
    page_manifest = canonical(
        {
            "schema": "borsuk-v115-sq8-page-authority-v2",
            "generation": GENERATION,
            "rows": ROWS,
            "dimensions": DIMENSIONS,
            "page_rows": PAGE_ROWS,
            "object_sha256": SQ8_SHA,
            "page_digest_sha256": digest(page_digests),
        }
    )
    if args.dry_run:
        print(
            json.dumps(
                {
                    "source_commit": commit,
                    "input_count": len(inputs),
                    "router_manifest_sha256": digest(canonical(router)),
                    "page_digest_sha256": digest(page_digests),
                    "mirror_digest_sha256": digest(mirror_digests),
                    "page_manifest_sha256": digest(page_manifest),
                    "published": False,
                },
                sort_keys=True,
            )
        )
        return
    derived = {
        "router_manifest": publish(
            s3, prefix, "router-manifest.json", canonical(router)
        ),
        "page_digests": publish(s3, prefix, "page-digests.bin", page_digests),
        "mirror_digests": publish(s3, prefix, "mirror-digests.bin", mirror_digests),
        "page_manifest": publish(s3, prefix, "page-manifest.json", page_manifest),
    }
    authority = {
        "schema": "borsuk-v132-live-serving-generation-v1",
        "generation": GENERATION,
        "source_commit": commit,
        "source_sha256": SOURCE_SHA,
        "sq8_sha256": SQ8_SHA,
        "upstream_terminal_sha256": {
            "v122": V122_TERMINAL_SHA,
            "v131": V131_TERMINAL_SHA,
        },
        "inputs": inputs,
        "derived": derived,
    }
    result = publish(s3, prefix, "generation.json", canonical(authority))
    terminal = publish(
        s3,
        prefix,
        "terminal.json",
        canonical(
            {
                "schema": "borsuk-v132-live-serving-generation-preparation-v1",
                "status": "complete",
                "generation": GENERATION,
                "source_commit": commit,
                "generation_manifest_sha256": result["sha256"],
                "upstream_terminal_sha256": authority["upstream_terminal_sha256"],
            }
        ),
    )
    print(
        json.dumps(
            {
                "generation": result,
                "terminal": terminal,
                "prefix": f"s3://{BUCKET}/{prefix}",
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
