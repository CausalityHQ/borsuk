#!/usr/bin/env python3
"""Stage exact V24 local-manifest inputs before offline scientific execution."""

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

_MANIFEST_SCHEMA = "borsuk-v24-local-manifest-v1"
_PREPARATION_MANIFEST_SCHEMA = "borsuk-v24-preparation-manifest-v1"
_RECEIPT_SCHEMA = "borsuk-v24-staging-receipt-v1"
_PHASES = {
    "witness-training",
    "posting-construction",
    "development-evaluation",
    "holdout-binding",
    "holdout-evaluation",
}
_PHASE_ROLES = {
    "witness-training": ("construction-rows-parquet",),
    "posting-construction": (
        "training-result",
        "witness-graph",
        "witnesses-arrow",
        "page-rows-parquet",
    ),
    "development-evaluation": (
        "witness-graph",
        "witness-postings",
        "query-parquet",
        "neighbors-parquet",
    ),
    "holdout-binding": (
        "development-result",
        "query-parquet",
        "neighbors-parquet",
    ),
    "holdout-evaluation": (
        "holdout-truth",
        "witness-graph",
        "witness-postings",
        "query-parquet",
        "neighbors-parquet",
    ),
}
_IDENTITY_KEYS = {
    "digest",
    "digest_algorithm",
    "encoded_bytes",
    "generation",
    "role",
    "uri",
}
_ROLE = re.compile(r"[a-z0-9][a-z0-9-]*\Z")
_LOWER_HEX = frozenset("0123456789abcdef")


class S3Client(Protocol):
    """Narrow exact-object API used only by the credentialed stager."""

    def get_object(self, **request: str) -> Mapping[str, Any]: ...


def _valid_digest(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in _LOWER_HEX for character in value)
    )


def _registered_role(role: str) -> bool:
    fixed = {
        "construction-manifest",
        "construction-rows-parquet",
        "dataset-meta",
        "training-manifest",
        "training-result",
        "witnesses-arrow",
        "witness-graph",
        "posting-manifest",
        "page-rows-parquet",
        "page-roster",
        "witness-postings",
        "query-parquet",
        "neighbors-parquet",
        "parent-receipt",
        "preflight-receipt",
        "development-result",
        "holdout-truth",
        "holdout-result",
    }
    if role in fixed:
        return True
    for prefix in ("training-shard-", "page-body-"):
        suffix = role.removeprefix(prefix)
        if (
            suffix != role
            and len(suffix) == 5
            and suffix.isascii()
            and suffix.isdigit()
        ):
            return True
    return False


def _relative_path(role: str) -> str:
    fixed = {
        "construction-manifest": "construction-manifest.json",
        "construction-rows-parquet": "construction-rows.parquet",
        "dataset-meta": "dataset-meta.json",
        "training-manifest": "training-manifest.json",
        "training-result": "training-result.json",
        "witnesses-arrow": "witnesses.arrow",
        "witness-graph": "witness-graph.arrow",
        "posting-manifest": "posting-manifest.json",
        "page-rows-parquet": "page-rows.parquet",
        "page-roster": "page-roster.json",
        "witness-postings": "witness-postings.arrow",
        "query-parquet": "queries.parquet",
        "neighbors-parquet": "neighbors.parquet",
        "parent-receipt": "parent-receipt.json",
        "preflight-receipt": "preflight-receipt.json",
        "development-result": "development-result.json",
        "holdout-truth": "holdout-binding.json",
        "holdout-result": "holdout-result.json",
    }
    if role in fixed:
        return fixed[role]
    if role.startswith("training-shard-"):
        return f"{role}.parquet"
    if role.startswith("page-body-"):
        return f"{role}.page"
    raise ValueError("manifest identity role differs")


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _read_manifest(
    path: pathlib.Path,
    expected_sha256: str,
) -> tuple[bytes, tuple[dict[str, object], ...]]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("manifest file authority differs")
    raw = path.read_bytes()
    if (
        not _valid_digest(expected_sha256)
        or hashlib.sha256(raw).hexdigest() != expected_sha256
    ):
        raise ValueError("manifest digest differs")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("manifest JSON differs") from error
    if raw != _canonical_json_bytes(value):
        raise ValueError("manifest canonical bytes differ")
    if type(value) is not dict or value.get("claim_eligible") is not False:  # noqa: E721
        raise ValueError("manifest authority differs")
    generation = value.get("generation")
    if type(generation) is not str or not generation:  # noqa: E721
        raise ValueError("manifest generation differs")
    schema = value.get("schema")
    if schema == _MANIFEST_SCHEMA:
        if (
            value.get("phase") not in _PHASES
            or type(value.get("inputs")) is not list
            or not value["inputs"]
        ):  # noqa: E721
            raise ValueError("manifest authority differs")
        registered = [(identity, None, None) for identity in value["inputs"]]
    elif schema == _PREPARATION_MANIFEST_SCHEMA:
        expected_keys = {
            "claim_eligible",
            "d1_report_sha256",
            "dataset_id",
            "generation",
            "index_id",
            "page_uri",
            "pages",
            "physical_row_count",
            "roster",
            "schema",
            "shards",
            "source_archive_sha256",
            "source_row_count",
        }
        if (
            set(value) != expected_keys
            or value["dataset_id"] != "deep-image-96"
            or type(value["index_id"]) is not str
            or not value["index_id"]
            or not _valid_digest(value["source_archive_sha256"])
            or not _valid_digest(value["d1_report_sha256"])
            or type(value["source_row_count"]) is not int
            or value["source_row_count"] <= 0
            or type(value["physical_row_count"]) is not int
            or value["physical_row_count"] < value["source_row_count"]
            or type(value["shards"]) is not list
            or not value["shards"]
            or type(value["pages"]) is not list
            or not value["pages"]
            or type(value["roster"]) is not dict
            or type(value["page_uri"]) is not str
            or not value["page_uri"].endswith("/")
        ):
            raise ValueError("preparation manifest authority differs")
        registered = []
        next_ordinal = 0
        for index, shard in enumerate(value["shards"]):
            if (
                type(shard) is not dict
                or set(shard) != {"identity", "ordinal_end", "ordinal_start", "rows"}
                or shard["ordinal_start"] != next_ordinal
                or type(shard["ordinal_end"]) is not int
                or type(shard["rows"]) is not int
                or shard["rows"] <= 0
                or shard["ordinal_end"] - next_ordinal != shard["rows"]
            ):
                raise ValueError("preparation shard authority differs")
            registered.append(
                (shard["identity"], f"training-shard-{index:05}", "sha256")
            )
            next_ordinal = shard["ordinal_end"]
        if next_ordinal != value["source_row_count"]:
            raise ValueError("preparation source count differs")
        registered.append((value["roster"], "page-roster", "sha256"))
        primary_rows = 0
        physical_rows = 0
        for index, page in enumerate(value["pages"]):
            if (
                type(page) is not dict
                or set(page)
                != {
                    "generation_checksum",
                    "identity",
                    "page_ordinal",
                    "primary_rows",
                    "replica_rows",
                }
                or page["page_ordinal"] != index
                or type(page["primary_rows"]) is not int
                or page["primary_rows"] <= 0
                or type(page["replica_rows"]) is not int
                or page["replica_rows"] < 0
                or type(page["generation_checksum"]) is not list
                or len(page["generation_checksum"]) != 32
                or any(
                    type(byte) is not int or byte < 0 or byte > 255
                    for byte in page["generation_checksum"]
                )
                or not any(page["generation_checksum"])
            ):
                raise ValueError("preparation page authority differs")
            registered.append((page["identity"], f"page-body-{index:05}", "blake3"))
            primary_rows += page["primary_rows"]
            physical_rows += page["primary_rows"] + page["replica_rows"]
        if (
            primary_rows != value["source_row_count"]
            or physical_rows != value["physical_row_count"]
        ):
            raise ValueError("preparation page counts differ")
    else:
        raise ValueError("manifest schema differs")
    identities: list[dict[str, object]] = []
    roles: set[str] = set()
    uris: set[str] = set()
    for identity, expected_role, expected_algorithm in registered:
        if type(identity) is not dict or set(identity) != _IDENTITY_KEYS:  # noqa: E721
            raise ValueError("manifest identity schema differs")
        role = identity["role"]
        uri = identity["uri"]
        if (
            type(role) is not str
            or _ROLE.fullmatch(role) is None
            or not _registered_role(role)
            or type(uri) is not str
            or not uri
            or identity["digest_algorithm"]
            != ("sha256" if expected_algorithm is None else expected_algorithm)
            or expected_role is not None
            and role != expected_role
            or not _valid_digest(identity["digest"])
            or type(identity["encoded_bytes"]) is not int
            or identity["encoded_bytes"] <= 0
            or type(identity["generation"]) is not str
            or identity["generation"] != value["generation"]
        ):
            raise ValueError("manifest identity role, generation, or authority differs")
        if role in roles or uri in uris:
            raise ValueError("manifest duplicate role or URI")
        roles.add(role)
        uris.add(uri)
        identities.append(identity)
    if (
        schema == _MANIFEST_SCHEMA
        and tuple(identity["role"] for identity in identities)
        != _PHASE_ROLES[value["phase"]]
    ):
        raise ValueError("manifest phase roles differ")
    if schema == _PREPARATION_MANIFEST_SCHEMA:
        for identity in identities:
            if identity["role"].startswith("page-body-") and identity["uri"] != (
                value["page_uri"] + "pages/" + identity["digest"]
            ):
                raise ValueError("preparation page URI differs")
    return raw, tuple(identities)


def manifest_phase(path: pathlib.Path, expected_sha256: str) -> str:
    """Return the authenticated V24 phase name after complete role validation."""

    raw, _ = _read_manifest(path, expected_sha256)
    value = json.loads(raw)
    return (
        "input-preparation"
        if value["schema"] == _PREPARATION_MANIFEST_SCHEMA
        else str(value["phase"])
    )


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
    request = {
        "Bucket": parsed.netloc,
        "ChecksumMode": "ENABLED",
        "Key": parsed.path[1:],
    }
    return request


def _new_digest(algorithm: str) -> Any:
    if algorithm == "sha256":
        return hashlib.sha256()
    if algorithm == "blake3":
        try:
            import blake3
        except ImportError as error:
            raise RuntimeError("blake3 is required for V24 page staging") from error
        return blake3.blake3()
    raise ValueError("V24 digest algorithm differs")


def _digest_file(path: pathlib.Path, algorithm: str) -> str:
    digest = _new_digest(algorithm)
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


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


def _cleanup_staging(
    root: pathlib.Path, receipt: pathlib.Path, names: Sequence[str]
) -> None:
    if receipt.exists():
        if receipt.is_symlink() or not receipt.is_file():
            raise ValueError("staging receipt cleanup authority differs")
        receipt.unlink()
    if not root.exists():
        return
    if root.is_symlink() or not root.is_dir():
        raise ValueError("staging cleanup root differs")
    if not {entry.name for entry in root.iterdir()}.issubset(set(names)):
        raise ValueError("unexpected staging entry")
    for name in names:
        path = root / name
        if path.exists():
            if path.is_symlink() or not path.is_file():
                raise ValueError("staging cleanup target differs")
            path.unlink()
    root.rmdir()


def validate_inventory(
    manifest_path: pathlib.Path,
    manifest_sha256: str,
    staging_directory: pathlib.Path,
    receipt_path: pathlib.Path,
) -> tuple[str, ...]:
    """Reauthenticate a complete staged directory against its V24 manifest."""

    manifest_raw, identities = _read_manifest(manifest_path, manifest_sha256)
    if (
        staging_directory.is_symlink()
        or not staging_directory.is_dir()
        or receipt_path.is_symlink()
        or not receipt_path.is_file()
    ):
        raise ValueError("staged inventory authority differs")
    receipt_raw = receipt_path.read_bytes()
    try:
        receipt = json.loads(receipt_raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("staged inventory receipt differs") from error
    if (
        receipt_raw != _canonical_json_bytes(receipt)
        or type(receipt) is not dict  # noqa: E721
        or set(receipt)
        != {
            "claim_eligible",
            "manifest_sha256",
            "ordered_objects",
            "schema",
        }
        or receipt["claim_eligible"] is not False
        or receipt["manifest_sha256"] != hashlib.sha256(manifest_raw).hexdigest()
        or receipt["schema"] != _RECEIPT_SCHEMA
        or type(receipt["ordered_objects"]) is not list
        or len(receipt["ordered_objects"]) != len(identities)
    ):
        raise ValueError("staged inventory receipt differs")
    for identity, observed in zip(identities, receipt["ordered_objects"], strict=True):
        expected = {
            **identity,
            "relative_path": _relative_path(str(identity["role"])),
        }
        if type(observed) is not dict:  # noqa: E721
            raise ValueError("staged inventory receipt differs")
        transport_version = observed.get("transport_version_id")
        if transport_version is not None and (
            type(transport_version) is not str or not transport_version
        ):
            raise ValueError("staged inventory transport version differs")
        without_transport = {
            key: value
            for key, value in observed.items()
            if key != "transport_version_id"
        }
        if without_transport != expected or set(observed) - set(expected) not in (
            set(),
            {"transport_version_id"},
        ):
            raise ValueError("staged inventory receipt differs")
    expected_names = tuple(
        _relative_path(str(identity["role"])) for identity in identities
    )
    if tuple(sorted(entry.name for entry in staging_directory.iterdir())) != tuple(
        sorted(expected_names)
    ):
        raise ValueError("staged inventory differs")
    for identity in identities:
        path = staging_directory / _relative_path(str(identity["role"]))
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_size != identity["encoded_bytes"]
            or _digest_file(path, str(identity["digest_algorithm"]))
            != identity["digest"]
        ):
            raise ValueError("staged inventory object differs")
    return tuple(str(identity["role"]) for identity in identities)


def stage_manifest(
    manifest_path: pathlib.Path,
    manifest_sha256: str,
    staging_directory: pathlib.Path,
    receipt_path: pathlib.Path,
    client: S3Client,
) -> bytes:
    """Download and authenticate every exact input in one V24 phase manifest."""

    if staging_directory.exists() or receipt_path.exists():
        raise FileExistsError("staging output already exists")
    manifest_raw, identities = _read_manifest(manifest_path, manifest_sha256)
    staging_directory.mkdir(mode=0o700)
    staged_objects: list[dict[str, object]] = []
    names = tuple(
        name
        for identity in identities
        for relative_path in (_relative_path(str(identity["role"])),)
        for name in (f".{relative_path}.partial", relative_path)
    )
    try:
        for identity in identities:
            request = _s3_request(identity)
            response = client.get_object(**request)
            if response.get("ContentLength") != identity["encoded_bytes"]:
                raise ValueError("object length differs")
            transport_version = response.get("VersionId")
            if transport_version is not None and (
                type(transport_version) is not str or not transport_version
            ):
                raise ValueError("object transport version differs")
            checksum = response.get("ChecksumSHA256")
            if identity["digest_algorithm"] == "sha256" and checksum is not None:
                try:
                    observed = base64.b64decode(checksum, validate=True).hex()
                except (TypeError, ValueError) as error:
                    raise ValueError("object S3 checksum differs") from error
                if observed != identity["digest"]:
                    raise ValueError("object S3 checksum differs")
            body = response.get("Body")
            if body is None:
                raise ValueError("object body is absent")
            relative_path = _relative_path(str(identity["role"]))
            partial = staging_directory / f".{relative_path}.partial"
            final = staging_directory / relative_path
            digest = _new_digest(str(identity["digest_algorithm"]))
            count = 0
            descriptor = os.open(
                partial,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                0o600,
            )
            try:
                while True:
                    chunk = body.read(1024 * 1024)
                    if not chunk:
                        break
                    if not isinstance(chunk, bytes):
                        raise ValueError("object body concrete type differs")
                    digest.update(chunk)
                    count += len(chunk)
                    offset = 0
                    while offset < len(chunk):
                        written = os.write(descriptor, chunk[offset:])
                        if written <= 0:
                            raise OSError("staging write made no progress")
                        offset += written
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
                close = getattr(body, "close", None)
                if close is not None:
                    close()
            if count != identity["encoded_bytes"]:
                raise ValueError("object length differs")
            if digest.hexdigest() != identity["digest"]:
                raise ValueError("object digest differs")
            os.rename(partial, final)
            staged_object = {
                **identity,
                "relative_path": relative_path,
            }
            if transport_version is not None:
                staged_object["transport_version_id"] = transport_version
            staged_objects.append(staged_object)
        receipt_value = {
            "claim_eligible": False,
            "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
            "ordered_objects": staged_objects,
            "schema": _RECEIPT_SCHEMA,
        }
        receipt_bytes = _canonical_json_bytes(receipt_value)
        _write_exclusive(receipt_path, receipt_bytes)
        validate_inventory(
            manifest_path,
            manifest_sha256,
            staging_directory,
            receipt_path,
        )
        return receipt_bytes
    except Exception:
        _cleanup_staging(staging_directory, receipt_path, names)
        raise


def parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--staging-directory", required=True, type=pathlib.Path)
    parser.add_argument("--receipt", required=True, type=pathlib.Path)
    parser.add_argument("--profile", choices=("causality",), default="causality")
    parser.add_argument("--region", required=True)
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    parsed = parse_args(arguments)
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError("boto3 is required for credentialed V24 staging") from error
    client = boto3.Session(
        profile_name=parsed.profile,
        region_name=parsed.region,
    ).client("s3")
    stage_manifest(
        parsed.manifest,
        parsed.manifest_sha256,
        parsed.staging_directory,
        parsed.receipt,
        client,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
