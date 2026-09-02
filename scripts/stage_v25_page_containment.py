#!/usr/bin/env python3
"""Credentialed staging for exact V25 containment artifacts."""

from __future__ import annotations

import dataclasses
import hashlib
import pathlib
import urllib.parse
from collections.abc import Sequence
from typing import Protocol


@dataclasses.dataclass(frozen=True)
class RegisteredInput:
    role: str
    uri: str
    sha256: str
    encoded_bytes: int
    file_name: str
    generation: str


class S3Downloader(Protocol):
    def download_file(self, bucket: str, key: str, filename: str) -> None: ...


def _sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stage_exact_inputs(
    client: S3Downloader,
    registered: Sequence[RegisteredInput],
    destination: pathlib.Path,
) -> list[pathlib.Path]:
    if not registered:
        raise ValueError("V25 registered input inventory is empty")
    created = not destination.exists()
    if created:
        destination.mkdir(parents=False)
    if not destination.is_dir() or any(destination.iterdir()):
        raise ValueError("V25 staging directory differs")
    roles: set[str] = set()
    uris: set[str] = set()
    names: set[str] = set()
    staged: list[pathlib.Path] = []
    try:
        for item in registered:
            parsed = urllib.parse.urlparse(item.uri)
            name = pathlib.Path(item.file_name)
            if (
                not item.role
                or not item.generation
                or parsed.scheme != "s3"
                or not parsed.netloc
                or not parsed.path.lstrip("/")
                or name.name != item.file_name
                or item.file_name in {"", ".", ".."}
                or len(item.sha256) != 64
                or any(character not in "0123456789abcdef" for character in item.sha256)
                or item.encoded_bytes <= 0
                or item.role in roles
                or item.uri in uris
                or item.file_name in names
            ):
                raise ValueError("V25 registered input authority differs")
            roles.add(item.role)
            uris.add(item.uri)
            names.add(item.file_name)
            target = destination / item.file_name
            client.download_file(parsed.netloc, parsed.path.lstrip("/"), str(target))
            staged.append(target)
            if target.stat().st_size != item.encoded_bytes or _sha256(target) != item.sha256:
                raise ValueError("V25 staged input bytes differ")
        return staged
    except Exception:
        for path in staged:
            if path.exists() or path.is_symlink():
                path.unlink()
        if created:
            destination.rmdir()
        raise
