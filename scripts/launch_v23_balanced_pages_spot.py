#!/usr/bin/env python3
"""Exact-input staging and fail-closed Spot lifecycle for balanced-page cells."""

from __future__ import annotations

import dataclasses
import hashlib
import pathlib
import stat
from collections.abc import Mapping, Sequence
from typing import Protocol

_LOWER_HEX = frozenset("0123456789abcdef")
_TERMINALS = frozenset({"complete", "quality", "stopped", "failed"})


@dataclasses.dataclass(frozen=True)
class RegisteredObject:
    """One immutable object staged before the offline child starts."""

    role: str
    uri: str
    sha256: str
    encoded_bytes: int
    basename: str


@dataclasses.dataclass(frozen=True)
class SpotLaunchRequest:
    """One preregistered same-region interruptible instance request."""

    region: str
    ami_id: str
    instance_type: str
    subnet_id: str
    security_group_ids: tuple[str, ...]
    instance_profile_arn: str
    user_data: str


class ObjectStorage(Protocol):
    def download(self, uri: str, destination: pathlib.Path) -> None: ...


class SpotCloud(Protocol):
    def launch_spot(self, request: SpotLaunchRequest) -> str: ...

    def wait_terminal(self, instance_id: str) -> str: ...

    def terminate(self, instance_id: str) -> None: ...


def _valid_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in _LOWER_HEX for character in value)


def _regular_file(path: pathlib.Path) -> bool:
    try:
        return stat.S_ISREG(path.lstat().st_mode)
    except FileNotFoundError:
        return False


def _sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_registered_objects(objects: Sequence[RegisteredObject]) -> None:
    if not objects:
        raise ValueError("registered object inventory is empty")
    roles: set[str] = set()
    uris: set[str] = set()
    basenames: set[str] = set()
    for registered in objects:
        if (
            type(registered.role) is not str
            or not registered.role
            or not registered.uri.startswith("s3://borsuk-v23-eu-west-1/")
            or not _valid_sha256(registered.sha256)
            or type(registered.encoded_bytes) is not int
            or registered.encoded_bytes <= 0
            or not registered.basename
            or pathlib.PurePath(registered.basename).name != registered.basename
            or registered.basename in {".", ".."}
            or registered.role in roles
            or registered.uri in uris
            or registered.basename in basenames
        ):
            raise ValueError("registered object authority differs")
        roles.add(registered.role)
        uris.add(registered.uri)
        basenames.add(registered.basename)


def stage_registered_inputs(
    storage: ObjectStorage,
    objects: Sequence[RegisteredObject],
    directory: pathlib.Path,
) -> tuple[pathlib.Path, ...]:
    """Download and authenticate only the exact registered object inventory."""

    _validate_registered_objects(objects)
    if not directory.is_absolute():
        raise ValueError("staging directory must be absolute")
    directory.mkdir(mode=0o700, parents=False, exist_ok=True)
    if any(directory.iterdir()):
        raise ValueError("staging directory is not empty")
    staged: list[pathlib.Path] = []
    try:
        for registered in objects:
            destination = directory / registered.basename
            partial = directory / f".{registered.basename}.partial"
            storage.download(registered.uri, partial)
            if (
                not _regular_file(partial)
                or partial.stat().st_size != registered.encoded_bytes
                or _sha256(partial) != registered.sha256
            ):
                raise ValueError(f"{registered.role} digest or length differs")
            partial.replace(destination)
            staged.append(destination)
    except BaseException:
        for path in tuple(directory.iterdir()):
            if _regular_file(path):
                path.unlink()
        raise
    return tuple(staged)


def validate_spot_request(request: SpotLaunchRequest) -> None:
    """Validate the frozen same-region Spot request without fallback."""

    if (
        request.region != "eu-west-1"
        or not request.ami_id.startswith("ami-")
        or not request.instance_type
        or not request.subnet_id.startswith("subnet-")
        or not request.security_group_ids
        or any(not group.startswith("sg-") for group in request.security_group_ids)
        or not request.instance_profile_arn.startswith("arn:aws:iam::")
        or not request.user_data.startswith("#!/bin/sh\n")
    ):
        raise ValueError("Spot request authority differs")


def ec2_run_instances_payload(request: SpotLaunchRequest) -> dict[str, object]:
    """Create the exact one-instance Spot API payload."""

    validate_spot_request(request)
    return {
        "ImageId": request.ami_id,
        "InstanceType": request.instance_type,
        "MinCount": 1,
        "MaxCount": 1,
        "SubnetId": request.subnet_id,
        "SecurityGroupIds": list(request.security_group_ids),
        "IamInstanceProfile": {"Arn": request.instance_profile_arn},
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {"SpotInstanceType": "one-time"},
        },
        "UserData": request.user_data,
    }


def launch_spot_cell(cloud: SpotCloud, request: SpotLaunchRequest) -> str:
    """Launch once, observe one terminal, and always terminate that instance."""

    validate_spot_request(request)
    instance_id = cloud.launch_spot(request)
    if not instance_id.startswith("i-"):
        raise ValueError("instance identity differs")
    try:
        terminal = cloud.wait_terminal(instance_id)
        if terminal not in _TERMINALS:
            raise ValueError("terminal classification differs")
        return terminal
    finally:
        cloud.terminate(instance_id)


def payload_is_spot(payload: Mapping[str, object]) -> bool:
    """Expose a small assertion helper for launch adapters."""

    options = payload.get("InstanceMarketOptions")
    return isinstance(options, dict) and options.get("MarketType") == "spot"

