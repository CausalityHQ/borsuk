#!/usr/bin/env python3
"""Stage exact immutable V23 incidence inputs outside scientific sandboxes."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import pathlib
import re
import sys
import urllib.parse
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

_MANIFEST_SCHEMA = "borsuk-v23-incidence-manifest-v1"
_RECEIPT_SCHEMA = "borsuk-v23-incidence-staging-receipt-v1"
_SOURCE_COMMIT = "c339a546f8f9370cb2e6e9fb3b0fd4bdefa3cb05"
_SOURCE_ARCHIVE_SHA256 = (
    "77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d"
)
_INDEX_ID = "index-bcda7bb66812e162d45077e6"
_DATASET_ID = "deep-image-96"
_ROLE = re.compile(r"[a-z0-9][a-z0-9-]*\Z")
_LOWER_HEX = frozenset("0123456789abcdef")
_MANIFEST_KEYS = {
    "algorithm",
    "claim_eligible",
    "dataset_id",
    "index_id",
    "ordered_inputs",
    "parent_receipt_sha256",
    "phase",
    "schema",
    "source_archive_sha256",
    "source_commit",
}
_IDENTITY_KEYS = {
    "digest",
    "digest_algorithm",
    "encoded_bytes",
    "generation",
    "role",
    "uri",
}


class S3Client(Protocol):
    """Narrow exact-object surface accepted by the stager."""

    def get_object(self, **request: str) -> Mapping[str, Any]: ...


def _valid_digest(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in _LOWER_HEX for character in value)
    )


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _read_manifest(path: pathlib.Path) -> tuple[bytes, list[dict[str, object]]]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("manifest file authority differs")
    raw = path.read_bytes()
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("manifest JSON differs") from error
    if raw != _canonical_json_bytes(value):
        raise ValueError("manifest canonical bytes differ")
    if type(value) is not dict or set(value) != _MANIFEST_KEYS:  # noqa: E721
        raise ValueError("manifest schema differs")
    if (
        value["schema"] != _MANIFEST_SCHEMA
        or value["claim_eligible"] is not False
        or value["source_commit"] != _SOURCE_COMMIT
        or value["source_archive_sha256"] != _SOURCE_ARCHIVE_SHA256
        or value["index_id"] != _INDEX_ID
        or value["dataset_id"] != _DATASET_ID
        or type(value["ordered_inputs"]) is not list
        or not value["ordered_inputs"]
    ):
        raise ValueError("manifest authority differs")
    identities: list[dict[str, object]] = []
    seen_roles: set[str] = set()
    seen_uris: set[str] = set()
    for item in value["ordered_inputs"]:
        if type(item) is not dict or "identity" not in item:  # noqa: E721
            raise ValueError("manifest input schema differs")
        identity = item["identity"]
        if type(identity) is not dict or set(identity) != _IDENTITY_KEYS:  # noqa: E721
            raise ValueError("manifest identity schema differs")
        role = identity["role"]
        uri = identity["uri"]
        if (
            type(role) is not str
            or _ROLE.fullmatch(role) is None
            or type(uri) is not str
            or type(identity["digest_algorithm"]) is not str
            or identity["digest_algorithm"] not in {"sha256", "blake3"}
            or not _valid_digest(identity["digest"])
            or type(identity["encoded_bytes"]) is not int
            or identity["encoded_bytes"] <= 0
            or type(identity["generation"]) is not str
            or not identity["generation"]
        ):
            raise ValueError("manifest identity role or authority differs")
        if role in seen_roles or uri in seen_uris:
            raise ValueError("manifest duplicate role or URI")
        seen_roles.add(role)
        seen_uris.add(uri)
        identities.append(identity)
    return raw, identities


def _s3_request(identity: Mapping[str, object]) -> dict[str, str]:
    parsed = urllib.parse.urlsplit(str(identity["uri"]))
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.startswith("/")
        or parsed.path == "/"
        or parsed.query
        or parsed.fragment
        or ".." in pathlib.PurePosixPath(parsed.path).parts
    ):
        raise ValueError("object URI differs")
    generation = str(identity["generation"])
    request = {
        "Bucket": parsed.netloc,
        "ChecksumMode": "ENABLED",
        "Key": parsed.path[1:],
    }
    if generation.startswith("s3-version:") and generation != "s3-version:":
        request["VersionId"] = generation.removeprefix("s3-version:")
    elif generation.startswith("unversioned-sha256:") and _valid_digest(
        generation.removeprefix("unversioned-sha256:")
    ):
        pass
    else:
        raise ValueError("object generation authority differs")
    return request


def _registered_s3_sha256(identity: Mapping[str, object]) -> str | None:
    generation = str(identity["generation"])
    if generation.startswith("unversioned-sha256:"):
        return generation.removeprefix("unversioned-sha256:")
    if identity["digest_algorithm"] == "sha256":
        return str(identity["digest"])
    return None


def _new_digest(algorithm: str) -> Any:
    if algorithm == "sha256":
        return hashlib.sha256()
    try:
        import blake3
    except ImportError as error:
        raise RuntimeError("the pinned blake3 module is required") from error
    return blake3.blake3()


def _write_exclusive(path: pathlib.Path, payload: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise OSError("exclusive write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _cleanup(
    staging_directory: pathlib.Path,
    receipt_path: pathlib.Path,
    known_names: Sequence[str],
) -> None:
    if receipt_path.exists():
        if receipt_path.is_symlink() or not receipt_path.is_file():
            raise ValueError("staging receipt cleanup authority differs")
        receipt_path.unlink()
    if not staging_directory.exists():
        return
    if staging_directory.is_symlink() or not staging_directory.is_dir():
        raise ValueError("staging cleanup directory differs")
    actual = {entry.name for entry in staging_directory.iterdir()}
    expected = set(known_names)
    if not actual.issubset(expected):
        raise ValueError("unexpected staging entry")
    for name in known_names:
        path = staging_directory / name
        if path.exists():
            if path.is_symlink() or not path.is_file():
                raise ValueError("staging cleanup target differs")
            path.unlink()
    staging_directory.rmdir()


def stage_manifest(
    manifest_path: pathlib.Path,
    staging_directory: pathlib.Path,
    receipt_path: pathlib.Path,
    client: S3Client,
) -> bytes:
    """Stage every exact S3 object in one canonical incidence manifest."""

    if (
        not manifest_path.is_absolute()
        or not staging_directory.is_absolute()
        or not receipt_path.is_absolute()
        or staging_directory == receipt_path.parent
        and receipt_path.name in {"", ".", ".."}
    ):
        raise ValueError("staging path authority differs")
    manifest_raw, identities = _read_manifest(manifest_path)
    if staging_directory.exists() or receipt_path.exists():
        raise FileExistsError("staging output already exists")
    staging_directory.mkdir(mode=0o700)
    known_names = tuple(
        name
        for identity in identities
        for name in (f".{identity['role']}.partial", str(identity["role"]))
    )
    observed: list[dict[str, object]] = []
    try:
        for identity in identities:
            role = str(identity["role"])
            request = _s3_request(identity)
            response = client.get_object(**request)
            if response.get("ContentLength") != identity["encoded_bytes"]:
                raise ValueError("object length differs")
            if response.get("VersionId") != request.get("VersionId"):
                raise ValueError("object generation differs")
            if "Body" not in response:
                raise ValueError("object body is absent")
            registered_sha256 = _registered_s3_sha256(identity)
            if registered_sha256 is not None:
                try:
                    observed_checksum = base64.b64decode(
                        response.get("ChecksumSHA256", ""), validate=True
                    ).hex()
                except (ValueError, TypeError) as error:
                    raise ValueError("object S3 checksum differs") from error
                metadata = response.get("Metadata")
                if (
                    observed_checksum != registered_sha256
                    or type(metadata) is not dict
                    or metadata.get("borsuk-sha256") != registered_sha256
                ):
                    raise ValueError("object S3 checksum differs")
            partial = staging_directory / f".{role}.partial"
            final = staging_directory / role
            digest = _new_digest(str(identity["digest_algorithm"]))
            count = 0
            descriptor = os.open(
                partial,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                0o600,
            )
            try:
                body = response["Body"]
                while True:
                    chunk = body.read(1024 * 1024)
                    if not chunk:
                        break
                    if not isinstance(chunk, bytes):
                        raise ValueError("object body concrete type differs")
                    count += len(chunk)
                    digest.update(chunk)
                    offset = 0
                    while offset < len(chunk):
                        written = os.write(descriptor, chunk[offset:])
                        if written <= 0:
                            raise OSError("staging write made no progress")
                        offset += written
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
                close = getattr(response.get("Body"), "close", None)
                if close is not None:
                    close()
            if count != identity["encoded_bytes"]:
                raise ValueError("object length differs")
            if digest.hexdigest() != identity["digest"]:
                raise ValueError("object digest differs")
            os.rename(partial, final)
            observed.append(
                {
                    **identity,
                    "relative_path": role,
                }
            )
        receipt_value = {
            "claim_eligible": False,
            "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
            "ordered_objects": observed,
            "schema": _RECEIPT_SCHEMA,
        }
        receipt_bytes = _canonical_json_bytes(receipt_value)
        _write_exclusive(receipt_path, receipt_bytes)
        return receipt_bytes
    except Exception:
        _cleanup(staging_directory, receipt_path, known_names)
        raise


def parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    parser.add_argument("--staging-directory", required=True, type=pathlib.Path)
    parser.add_argument("--receipt", required=True, type=pathlib.Path)
    parser.add_argument("--profile", choices=("causality",), default="causality")
    parser.add_argument("--region", choices=("eu-central-1",), default="eu-central-1")
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    parsed = parse_args(arguments)
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError("boto3 is required for credentialed staging") from error
    client = boto3.Session(
        profile_name=parsed.profile, region_name=parsed.region
    ).client("s3")
    stage_manifest(parsed.manifest, parsed.staging_directory, parsed.receipt, client)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
