#!/usr/bin/env python3
"""Stage and run the claim-ineligible V23 incidence development screen once."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import os
import pathlib
import re
import subprocess
import tempfile
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

PROFILE = "causality"
REGION = "eu-central-1"
INDEX_ID = "index-bcda7bb66812e162d45077e6"
ROLE_ORDER = (
    "tree-receipt",
    "incidence-tree",
    "posting-receipt",
    "incidence-postings-one",
    "incidence-postings-two",
    "d2-report",
    "query-parquet",
)
LOWER_SHA1 = re.compile(r"[0-9a-f]{40}\Z")
LOWER_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


@dataclasses.dataclass(frozen=True)
class FrozenArtifact:
    role: str
    uri: str
    digest_algorithm: str
    digest: str
    encoded_bytes: int
    basename: str


TREE_PREFIX = (
    "s3://borsuk-bench-453182569524-euc1/research/v23-leaf-page-incidence/"
    "a321c473cb38a3b38c4757a50acf14e144b0441b0ca4bbbe7a8c7f3baaef78cc/"
    "v23-incidence-tree-20260831T120514Z/"
)
POSTING_PREFIX = (
    "s3://borsuk-bench-453182569524-euc1/research/v23-leaf-page-incidence/"
    "7f9d1350948112ecef393dc5c6994cef642ce965639c7f682d47aabfb87976a2/"
    "v23-incidence-posting-20260831T152007Z/"
)
D2_PREFIX = (
    "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/"
    "r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/0000/attempts/0001/"
)
FROZEN_ARTIFACTS = (
    FrozenArtifact(
        "tree-receipt",
        TREE_PREFIX + "tree-receipt.json",
        "sha256",
        "c1af5ab84ef20797ffe52fa0a93872008df817c142957f009895c8b7fc853a99",
        26_106,
        "tree-receipt.json",
    ),
    FrozenArtifact(
        "incidence-tree",
        TREE_PREFIX + "incidence-tree.bin",
        "blake3",
        "aa72bf926c6fcbd17890188d8b3bd3b35393d9c392bffc032e75328ea47fae64",
        40_369_836,
        "incidence-tree.bin",
    ),
    FrozenArtifact(
        "posting-receipt",
        POSTING_PREFIX + "posting-receipt.json",
        "sha256",
        "cca5b1f895fd633ad5e6fab0288f6838d3efa9087f83809fc0c2032736ff6aca",
        13_407_759,
        "posting-receipt.json",
    ),
    FrozenArtifact(
        "incidence-postings-one",
        POSTING_PREFIX + "incidence-postings-one.bin",
        "blake3",
        "b5f6b1009e67d8286f012d80d4eea0f52d2516db70ddbad88e1e4477e3ae7c61",
        51_502_404,
        "incidence-postings-one.bin",
    ),
    FrozenArtifact(
        "incidence-postings-two",
        POSTING_PREFIX + "incidence-postings-two.bin",
        "blake3",
        "ad75479318297d9c95e0f8f71220e7a5f2d283440be762238ea0bb8959f6897d",
        59_186_088,
        "incidence-postings-two.bin",
    ),
    FrozenArtifact(
        "d2-report",
        D2_PREFIX + "bench_v23_d2_report.json",
        "sha256",
        "bb8f97360827abd0f18964982c9729c083888ad02ad4cc08d1ba6779100f409a",
        25_725_198,
        "bench_v23_d2_report.json",
    ),
    FrozenArtifact(
        "query-parquet",
        "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/datasets/"
        "deep-image-96/attempts/0001/materialized/test.parquet",
        "sha256",
        "296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4",
        3_843_448,
        "test.parquet",
    ),
)


def _digest_file(path: pathlib.Path, algorithm: str) -> str:
    if algorithm == "sha256":
        digest: Any = hashlib.sha256()
    elif algorithm == "blake3":
        import blake3

        digest = blake3.blake3()
    else:
        raise ValueError("artifact digest algorithm differs")
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return str(digest.hexdigest())


def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError("artifact URI differs")
    return parsed.netloc, parsed.path.lstrip("/")


def _default_s3_client() -> Any:
    import boto3

    return boto3.Session(profile_name=PROFILE, region_name=REGION).client("s3")


def _validate_inputs(
    binary: pathlib.Path, source_commit: str, source_archive_sha256: str, output: pathlib.Path
) -> None:
    if not binary.is_absolute() or not binary.is_file():
        raise ValueError("screen binary differs")
    if LOWER_SHA1.fullmatch(source_commit) is None:
        raise ValueError("source commit differs")
    if LOWER_DIGEST.fullmatch(source_archive_sha256) is None:
        raise ValueError("source archive SHA-256 differs")
    if not output.is_absolute() or output.exists():
        raise ValueError("screen output path differs")
    if tuple(artifact.role for artifact in FROZEN_ARTIFACTS) != ROLE_ORDER:
        raise ValueError("frozen artifact role order differs")


def run_screen(
    *,
    binary: pathlib.Path,
    source_commit: str,
    source_archive_sha256: str,
    output: pathlib.Path,
    s3_client: Any | None = None,
    scratch_parent: pathlib.Path | None = None,
) -> None:
    binary = binary.resolve()
    output = output.resolve()
    _validate_inputs(binary, source_commit, source_archive_sha256, output)
    client = s3_client if s3_client is not None else _default_s3_client()
    scratch = pathlib.Path(
        tempfile.mkdtemp(
            prefix="v23-incidence-screen-",
            dir=None if scratch_parent is None else str(scratch_parent),
        )
    )
    local_paths = {artifact.role: scratch / artifact.basename for artifact in FROZEN_ARTIFACTS}
    partial_output = scratch / "screen-result.json"
    try:
        for artifact in FROZEN_ARTIFACTS:
            bucket, key = _split_s3_uri(artifact.uri)
            local = local_paths[artifact.role]
            client.download_file(bucket, key, str(local))
            if local.stat().st_size != artifact.encoded_bytes:
                raise ValueError(f"{artifact.role} length differs")
            if _digest_file(local, artifact.digest_algorithm) != artifact.digest:
                raise ValueError(f"{artifact.role} digest differs")

        command = [str(binary)]
        for artifact in FROZEN_ARTIFACTS:
            command.extend([f"--{artifact.role}", str(local_paths[artifact.role])])
        command.extend(
            [
                "--source-commit",
                source_commit,
                "--source-archive-sha256",
                source_archive_sha256,
                "--index-id",
                INDEX_ID,
                "--output",
                str(partial_output),
            ]
        )
        for artifact in FROZEN_ARTIFACTS:
            command.extend(
                [
                    f"--{artifact.role}-uri",
                    artifact.uri,
                    f"--{artifact.role}-{artifact.digest_algorithm}",
                    artifact.digest,
                    f"--{artifact.role}-bytes",
                    str(artifact.encoded_bytes),
                ]
            )
        command.append("--execute-development-screen")
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=7_200,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or f"exit {completed.returncode}"
            raise RuntimeError(f"incidence development screen failed: {detail}")
        if not partial_output.is_file():
            raise RuntimeError("incidence development screen produced no result")
        output.parent.mkdir(parents=True, exist_ok=True)
        os.replace(partial_output, output)
    finally:
        partial_output.unlink(missing_ok=True)
        for artifact in FROZEN_ARTIFACTS:
            local_paths[artifact.role].unlink(missing_ok=True)
        scratch.rmdir()


def parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--execute-development-screen", action="store_true")
    values = parser.parse_args(arguments)
    if not values.execute_development_screen:
        parser.error("--execute-development-screen is required")
    return values


def main(arguments: Sequence[str] | None = None) -> int:
    values = parse_args(arguments)
    run_screen(
        binary=values.binary,
        source_commit=values.source_commit,
        source_archive_sha256=values.source_archive_sha256,
        output=values.output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
