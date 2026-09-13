#!/usr/bin/env python3
"""Spot-only orchestration for the V38 boundary-spill fail-fast campaign."""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import json
import os
import pathlib
import re
import shlex
import subprocess
import sys
import threading
import time
import urllib.parse
from collections.abc import Callable
from typing import Any

EXPECTED_AWS_ACCOUNT = "453182569524"
PROFILE = "causality"
REGION = "eu-central-1"
AMI_ID = "ami-07bcecd13a160173f"
INSTANCE_TYPE = "c7g.8xlarge"
INSTANCE_PROFILE = "borsuk-bench-profile"
SECURITY_GROUP_ID = "sg-0b1fd3e4fbde4af0d"

BUILD_MEMORY_LIMIT_BYTES = 3 * 1_073_741_824
CEILING_MEMORY_LIMIT_BYTES = 256 * 1_048_576
MEMORY_PSI_FULL_LIMIT = 0.75
BUILD_SCIENCE_TIMEOUT_SECONDS = 600
BUILD_WRAPPER_TIMEOUT_SECONDS = 720
BUILD_PROGRESS_TIMEOUT_SECONDS = 120
CEILING_SCIENCE_TIMEOUT_SECONDS = 120
CEILING_WRAPPER_TIMEOUT_SECONDS = 180
CEILING_PROGRESS_TIMEOUT_SECONDS = 30

_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_LOWER_GIT_SHA1 = re.compile(r"[0-9a-f]{40}\Z")
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_INSTANCE_ID = re.compile(r"i-[0-9a-f]{8,17}\Z")


@dataclasses.dataclass(frozen=True)
class SpotTarget:
    """One preregistered public subnet in an independent availability zone."""

    availability_zone: str
    subnet_id: str


SPOT_TARGETS = (
    SpotTarget("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
    SpotTarget("eu-central-1b", "subnet-00243d923761c047c"),
    SpotTarget("eu-central-1a", "subnet-034528fbd6977848f"),
)


@dataclasses.dataclass(frozen=True)
class V38SpotPlan:
    """Exact immutable launch authority for one V38 phase."""

    phase: str
    run_id: str
    source_commit: str
    source_archive_uri: str
    source_archive_sha256: str
    source_archive_bytes: int
    binary_uri: str
    binary_sha256: str
    binary_bytes: int
    manifest_uri: str
    manifest_sha256: str
    manifest_bytes: int
    output_prefix: str


@dataclasses.dataclass(frozen=True)
class V38MonitorSample:
    """One complete cgroup observation for the sole scientific process group."""

    phase: str
    science_elapsed_seconds: float
    wrapper_elapsed_seconds: float
    last_progress_seconds: float
    memory_current_bytes: int
    memory_peak_bytes: int
    memory_anon_bytes: int
    memory_file_bytes: int
    memory_kernel_bytes: int
    psi_full_avg10: float
    swap_start_bytes: int
    swap_end_bytes: int


class V38WrapperMonitor:
    """Continuously preserve aggregate-slice evidence across worker lifecycle."""

    def __init__(
        self,
        phase: str,
        memory_cgroup: pathlib.Path,
        *,
        poll_seconds: float = 0.1,
    ) -> None:
        phase_input_roles(phase)
        if poll_seconds <= 0:
            raise ValueError("V38 wrapper monitor interval differs")
        self.phase = phase
        self.memory_cgroup = memory_cgroup
        self.poll_seconds = poll_seconds
        self.started = time.monotonic()
        self.swap_start = _read_v38_cgroup_value(
            memory_cgroup / "memory.swap.current"
        )
        self.current = 0
        self.peak = 0
        self.swap_end = self.swap_start
        self.anon = 0
        self.file_bytes = 0
        self.kernel = 0
        self.peak_psi = 0.0
        self._error: BaseException | None = None
        self._finished: V38MonitorSample | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._update()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()

    def _update(self) -> None:
        current = _read_v38_cgroup_value(self.memory_cgroup / "memory.current")
        peak = _read_v38_cgroup_value(self.memory_cgroup / "memory.peak")
        swap_end = _read_v38_cgroup_value(
            self.memory_cgroup / "memory.swap.current"
        )
        anon, file_bytes, kernel = _read_v38_memory_stat(
            self.memory_cgroup / "memory.stat"
        )
        psi = _v38_psi_full_avg10()
        with self._lock:
            self.current = current
            self.peak = max(self.peak, peak)
            self.swap_end = swap_end
            self.anon = anon
            self.file_bytes = file_bytes
            self.kernel = kernel
            self.peak_psi = max(self.peak_psi, psi)

    def _poll(self) -> None:
        while not self._stop.wait(self.poll_seconds):
            try:
                self._update()
            except BaseException as error:
                self._error = error
                return

    def finish(self, science: V38MonitorSample | None) -> V38MonitorSample:
        """Stop sampling and merge final wrapper evidence with native science."""

        if self._finished is not None:
            return self._finished
        self._stop.set()
        self._thread.join()
        if self._error is not None:
            raise self._error
        self._update()
        with self._lock:
            self._finished = V38MonitorSample(
                phase=self.phase,
                science_elapsed_seconds=(
                    0.0 if science is None else science.science_elapsed_seconds
                ),
                wrapper_elapsed_seconds=time.monotonic() - self.started,
                last_progress_seconds=(
                    0.0 if science is None else science.last_progress_seconds
                ),
                memory_current_bytes=self.current,
                memory_peak_bytes=max(
                    self.peak,
                    0 if science is None else science.memory_peak_bytes,
                ),
                memory_anon_bytes=self.anon,
                memory_file_bytes=self.file_bytes,
                memory_kernel_bytes=self.kernel,
                psi_full_avg10=max(
                    self.peak_psi,
                    0.0 if science is None else science.psi_full_avg10,
                ),
                swap_start_bytes=self.swap_start,
                swap_end_bytes=self.swap_end,
            )
        return self._finished


@dataclasses.dataclass(frozen=True)
class V38WorkerInvocation:
    """Strict local capabilities admitted to the remote worker."""

    root: pathlib.Path
    plan: pathlib.Path
    manifest: pathlib.Path
    binary: pathlib.Path
    instance_id: str


@dataclasses.dataclass(frozen=True)
class V38StagedArtifact:
    """One authenticated phase input with a local capability path."""

    role: str
    path: pathlib.Path
    uri: str
    sha256: str
    blake3: str
    encoded_bytes: int


@dataclasses.dataclass(frozen=True)
class V38StagedOutput:
    """One phase output with its preregistered URI and local path."""

    role: str
    path: pathlib.Path
    uri: str


@dataclasses.dataclass(frozen=True)
class V38StagedPhase:
    """Authenticated local capabilities for exactly one V38 phase."""

    phase: str
    run_id: str
    source_commit: str
    workers: int
    inputs: tuple[V38StagedArtifact, ...]
    outputs: tuple[V38StagedOutput, ...]


def parse_v38_worker_args(arguments: list[str]) -> V38WorkerInvocation:
    """Parse the explicit local-only worker boundary without abbreviation."""

    iterator = iter(arguments)
    try:
        next(iterator)
    except StopIteration as error:
        raise ValueError("V38 worker program name is absent") from error
    execute = False
    values: dict[str, str] = {}
    allowed = {"--root", "--plan", "--manifest", "--binary", "--instance-id"}
    for flag in iterator:
        if flag == "--execute-v38-worker":
            if execute:
                raise ValueError("V38 worker execute flag is duplicated")
            execute = True
            continue
        if flag not in allowed or flag in values:
            raise ValueError("V38 worker flag differs")
        try:
            value = next(iterator)
        except StopIteration as error:
            raise ValueError("V38 worker flag value is absent") from error
        if value.startswith("--"):
            raise ValueError("V38 worker flag value differs")
        values[flag] = value
    if not execute or set(values) != allowed:
        raise ValueError("V38 worker capabilities differ")
    paths = [
        pathlib.Path(values[flag])
        for flag in ("--root", "--plan", "--manifest", "--binary")
    ]
    if (
        any(not path.is_absolute() for path in paths)
        or len(set(paths)) != len(paths)
        or _INSTANCE_ID.fullmatch(values["--instance-id"]) is None
    ):
        raise ValueError("V38 worker local authority differs")
    return V38WorkerInvocation(
        root=paths[0],
        plan=paths[1],
        manifest=paths[2],
        binary=paths[3],
        instance_id=values["--instance-id"],
    )


def _s3_uri(value: str, *, prefix: bool = False) -> tuple[str, str]:
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.startswith("/")
        or parsed.path == "/"
        or parsed.query
        or parsed.fragment
        or ".." in pathlib.PurePosixPath(parsed.path).parts
    ):
        raise ValueError("V38 S3 authority differs")
    key = parsed.path[1:]
    if key.endswith("/") != prefix:
        raise ValueError("V38 S3 prefix authority differs")
    return parsed.netloc, key


def phase_input_roles(phase: str) -> tuple[str, ...]:
    """Return the only local input roles admitted to one executable phase."""

    roles = {
        "preflight-spill": ("v38-authority",),
        "build-spill": (
            "v38-authority",
            "v37-authority",
            "v37-build-manifest",
            "v37-local-result",
            "v37-terminal",
            "v37-tree",
            "v37-ownership",
            "source",
        ),
        "evaluate-ceiling": (
            "v38-ceiling-authority",
            "v38-construction-result",
            "spill-relation",
            "spill-postings",
            "gt100",
        ),
    }
    try:
        return roles[phase]
    except KeyError as error:
        raise ValueError("V38 phase differs") from error


def phase_output_roles(phase: str) -> tuple[str, ...]:
    """Return the exact local output roles admitted to one executable phase."""

    outputs = {
        "preflight-spill": (),
        "build-spill": ("spill-relation", "spill-postings"),
        "evaluate-ceiling": ("ceiling",),
    }
    try:
        return outputs[phase]
    except KeyError as error:
        raise ValueError("V38 phase differs") from error


def _validate_identity(value: object, role: str) -> str:
    if type(value) is not dict or set(value) != {
        "blake3",
        "encoded_bytes",
        "role",
        "sha256",
        "uri",
    }:
        raise ValueError("V38 artifact identity differs")
    if (
        value.get("role") != role
        or type(value.get("encoded_bytes")) is not int
        or value["encoded_bytes"] <= 0
        or _LOWER_SHA256.fullmatch(str(value.get("sha256"))) is None
        or _LOWER_SHA256.fullmatch(str(value.get("blake3"))) is None
        or type(value.get("uri")) is not str
    ):
        raise ValueError("V38 artifact authority differs")
    _s3_uri(value["uri"])
    return value["uri"]


def canonical_v38_phase_manifest_bytes(value: object) -> bytes:
    """Validate and encode one exact V38 phase manifest."""

    if type(value) is not dict or set(value) != {
        "claim_eligible",
        "inputs",
        "outputs",
        "phase",
        "run_id",
        "schema",
        "source_commit",
        "workers",
    }:
        raise ValueError("V38 phase manifest differs")
    phase = value.get("phase")
    roles = phase_input_roles(str(phase))
    output_roles = phase_output_roles(str(phase))
    inputs = value.get("inputs")
    outputs = value.get("outputs")
    if (
        value.get("schema") != "borsuk-v38-spot-phase-manifest-v1"
        or value.get("claim_eligible") is not False
        or _TOKEN.fullmatch(str(value.get("run_id"))) is None
        or _LOWER_GIT_SHA1.fullmatch(str(value.get("source_commit"))) is None
        or type(value.get("workers")) is not int
        or value["workers"] not in (1, 2, 4, 8, 16, 32)
        or type(inputs) is not list
        or type(outputs) is not list
        or [item.get("role") if type(item) is dict else None for item in inputs]
        != list(roles)
        or [item.get("role") if type(item) is dict else None for item in outputs]
        != list(output_roles)
    ):
        raise ValueError("V38 phase manifest authority differs")
    uris: set[str] = set()
    for identity, role in zip(inputs, roles, strict=True):
        uri = _validate_identity(identity, role)
        if uri in uris:
            raise ValueError("V38 phase input roles overlap")
        uris.add(uri)
    for output, role in zip(outputs, output_roles, strict=True):
        if type(output) is not dict or set(output) != {"role", "uri"}:
            raise ValueError("V38 phase output authority differs")
        if output["role"] != role or type(output["uri"]) is not str:
            raise ValueError("V38 phase output authority differs")
        _s3_uri(output["uri"])
        if output["uri"] in uris:
            raise ValueError("V38 phase input/output roles overlap")
        uris.add(output["uri"])
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        + b"\n"
    )


def stage_v38_phase_inputs(
    raw: bytes,
    root: pathlib.Path,
    download: Callable[[str, str, pathlib.Path], None],
) -> V38StagedPhase:
    """Stage and authenticate only the exact objects named by one manifest."""

    manifest = _canonical_json_object(raw, "phase manifest")
    if canonical_v38_phase_manifest_bytes(manifest) != raw or root.exists():
        raise ValueError("V38 staged manifest authority differs")
    input_root = root / "inputs"
    output_root = root / "outputs"
    input_root.mkdir(parents=True, mode=0o700)
    output_root.mkdir(mode=0o700)
    import blake3

    staged_inputs = []
    for identity in manifest["inputs"]:
        path = input_root / identity["role"]
        bucket, key = _s3_uri(identity["uri"])
        download(bucket, key, path)
        sha256 = hashlib.sha256()
        blake3_digest = blake3.blake3()
        encoded_bytes = 0
        with path.open("rb") as source:
            while chunk := source.read(1_048_576):
                encoded_bytes += len(chunk)
                sha256.update(chunk)
                blake3_digest.update(chunk)
        if (
            encoded_bytes != identity["encoded_bytes"]
            or sha256.hexdigest() != identity["sha256"]
            or blake3_digest.hexdigest() != identity["blake3"]
        ):
            raise ValueError(f"V38 {identity['role']} staged bytes differ")
        staged_inputs.append(
            V38StagedArtifact(
                role=identity["role"],
                path=path,
                uri=identity["uri"],
                sha256=identity["sha256"],
                blake3=identity["blake3"],
                encoded_bytes=identity["encoded_bytes"],
            )
        )
    outputs = tuple(
        V38StagedOutput(
            role=output["role"],
            path=output_root / output["role"],
            uri=output["uri"],
        )
        for output in manifest["outputs"]
    )
    return V38StagedPhase(
        phase=manifest["phase"],
        run_id=manifest["run_id"],
        source_commit=manifest["source_commit"],
        workers=manifest["workers"],
        inputs=tuple(staged_inputs),
        outputs=outputs,
    )


def build_v38_binary_command(
    binary: pathlib.Path, staged: V38StagedPhase
) -> list[str]:
    """Build the strict local-only Rust CLI invocation."""

    command = [
        str(binary),
        "--execute-v38-local",
        "--mode",
        staged.phase,
        "--workers",
        str(staged.workers),
    ]
    for item in staged.inputs:
        command.extend(
            [
                f"--{item.role}",
                str(item.path),
                f"--{item.role}-uri",
                item.uri,
                f"--{item.role}-sha256",
                item.sha256,
                f"--{item.role}-blake3",
                item.blake3,
                f"--{item.role}-bytes",
                str(item.encoded_bytes),
            ]
        )
    for output in staged.outputs:
        command.extend([f"--{output.role}-output", str(output.path)])
    return command


def build_v38_spot_plan(**values: Any) -> V38SpotPlan:
    """Validate one immutable launch plan without consulting mutable AWS state."""

    plan = V38SpotPlan(**values)
    phase_input_roles(plan.phase)
    if (
        _TOKEN.fullmatch(plan.run_id) is None
        or _LOWER_GIT_SHA1.fullmatch(plan.source_commit) is None
        or _LOWER_SHA256.fullmatch(plan.source_archive_sha256) is None
        or _LOWER_SHA256.fullmatch(plan.binary_sha256) is None
        or _LOWER_SHA256.fullmatch(plan.manifest_sha256) is None
        or min(plan.source_archive_bytes, plan.binary_bytes, plan.manifest_bytes) <= 0
    ):
        raise ValueError("V38 Spot plan authority differs")
    for uri in (plan.source_archive_uri, plan.binary_uri, plan.manifest_uri):
        _s3_uri(uri)
    _s3_uri(plan.output_prefix, prefix=True)
    if len({plan.source_archive_uri, plan.binary_uri, plan.manifest_uri}) != 3:
        raise ValueError("V38 Spot plan object roles overlap")
    return plan


def build_v38_launch_specs(
    plan: V38SpotPlan, *, user_data: str
) -> list[dict[str, Any]]:
    """Build one Spot-only request per preregistered availability zone."""

    build_v38_spot_plan(**dataclasses.asdict(plan))
    if not user_data.startswith("#!/") or "shutdown -h now" not in user_data:
        raise ValueError("V38 worker user data differs")
    market = {
        "MarketType": "spot",
        "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate",
            "SpotInstanceType": "one-time",
        },
    }
    specs = []
    for ordinal, target in enumerate(SPOT_TARGETS):
        token = json.dumps(
            {"plan": dataclasses.asdict(plan), "target": dataclasses.asdict(target)},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        specs.append(
            {
                "ImageId": AMI_ID,
                "InstanceType": INSTANCE_TYPE,
                "MinCount": 1,
                "MaxCount": 1,
                "ClientToken": "borsuk-v38-"
                + hashlib.sha256(token + bytes([ordinal])).hexdigest()[:48],
                "SubnetId": target.subnet_id,
                "Placement": {"AvailabilityZone": target.availability_zone},
                "SecurityGroupIds": [SECURITY_GROUP_ID],
                "IamInstanceProfile": {"Name": INSTANCE_PROFILE},
                "InstanceInitiatedShutdownBehavior": "terminate",
                "InstanceMarketOptions": market,
                "MetadataOptions": {
                    "HttpEndpoint": "enabled",
                    "HttpPutResponseHopLimit": 1,
                    "HttpTokens": "required",
                },
                "BlockDeviceMappings": [
                    {
                        "DeviceName": "/dev/xvda",
                        "Ebs": {
                            "DeleteOnTermination": True,
                            "Encrypted": True,
                            "Iops": 3_000,
                            "Throughput": 250,
                            "VolumeSize": 200,
                            "VolumeType": "gp3",
                        },
                    }
                ],
                "UserData": user_data,
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": plan.run_id},
                            {"Key": "borsuk-purpose", "Value": f"v38-{plan.phase}"},
                        ],
                    }
                ],
            }
        )
    return specs


def classify_v38_monitor_sample(sample: V38MonitorSample) -> str | None:
    """Classify the first registered stop without inferring process outcome."""

    if sample.phase == "build-spill":
        limits = (3 * 1_073_741_824, 600, 720, 120)
    elif sample.phase == "evaluate-ceiling":
        limits = (256 * 1_048_576, 120, 180, 30)
    elif sample.phase == "preflight-spill":
        limits = (256 * 1_048_576, 120, 180, 30)
    else:
        raise ValueError("V38 monitor phase differs")
    memory, science, wrapper, progress = limits
    if sample.science_elapsed_seconds > science:
        return "science-timeout"
    if sample.wrapper_elapsed_seconds > wrapper:
        return "wrapper-timeout"
    if sample.last_progress_seconds > progress:
        return "progress-timeout"
    if max(sample.memory_current_bytes, sample.memory_peak_bytes) > memory:
        return "memory-stop"
    if sample.psi_full_avg10 > MEMORY_PSI_FULL_LIMIT:
        return "psi-stop"
    if sample.swap_end_bytes != sample.swap_start_bytes:
        return "swap-stop"
    return None


def _v38_psi_full_avg10() -> float:
    for line in pathlib.Path("/proc/pressure/memory").read_text().splitlines():
        if line.startswith("full "):
            for field in line.split()[1:]:
                if field.startswith("avg10="):
                    return float(field.removeprefix("avg10="))
    raise RuntimeError("V38 memory PSI authority is absent")


def _read_v38_cgroup_value(path: pathlib.Path) -> int:
    return int(path.read_text().strip())


def _read_v38_memory_stat(path: pathlib.Path) -> tuple[int, int, int]:
    values = {
        key: int(value)
        for key, value in (
            line.split(maxsplit=1) for line in path.read_text().splitlines()
        )
    }
    return values["anon"], values["file"], values["kernel"]


def run_v38_native_process(
    command: list[str],
    stdout_path: pathlib.Path,
    progress_path: pathlib.Path,
    *,
    phase: str,
    poll_seconds: float = 1.0,
    memory_cgroup: pathlib.Path,
    terminate_command: list[str] | None = None,
) -> tuple[int, V38MonitorSample]:
    """Run and pressure-monitor the sole native process group."""

    phase_input_roles(phase)
    if not command or poll_seconds <= 0 or stdout_path == progress_path:
        raise ValueError("V38 native process authority differs")
    current = _read_v38_cgroup_value(memory_cgroup / "memory.current")
    peak = _read_v38_cgroup_value(memory_cgroup / "memory.peak")
    swap_start = _read_v38_cgroup_value(memory_cgroup / "memory.swap.current")
    swap_end = swap_start
    anon, file_bytes, kernel = _read_v38_memory_stat(memory_cgroup / "memory.stat")
    peak_psi = _v38_psi_full_avg10()
    started = time.monotonic()
    last_progress = started
    progress_bytes = 0
    previous_progress: dict[str, object] | None = None
    trigger_sample: V38MonitorSample | None = None
    with stdout_path.open("xb") as stdout, progress_path.open("xb") as progress:
        process = subprocess.Popen(
            command,
            stdout=stdout,
            stderr=progress,
            start_new_session=True,
        )
        try:
            while process.poll() is None:
                now = time.monotonic()
                progress_raw = progress_path.read_bytes()
                if len(progress_raw) < progress_bytes:
                    raise ValueError("V38 native progress truncated")
                pending = progress_raw[progress_bytes:]
                complete_bytes = pending.rfind(b"\n") + 1
                if complete_bytes:
                    for line in pending[:complete_bytes].splitlines(keepends=True):
                        previous_progress = validate_v38_progress_bytes(
                            line, phase, previous_progress
                        )
                    progress_bytes += complete_bytes
                    last_progress = now
                current = _read_v38_cgroup_value(memory_cgroup / "memory.current")
                peak = max(
                    peak,
                    _read_v38_cgroup_value(memory_cgroup / "memory.peak"),
                )
                swap_end = _read_v38_cgroup_value(
                    memory_cgroup / "memory.swap.current"
                )
                anon, file_bytes, kernel = _read_v38_memory_stat(
                    memory_cgroup / "memory.stat"
                )
                peak_psi = max(peak_psi, _v38_psi_full_avg10())
                sample = V38MonitorSample(
                    phase=phase,
                    science_elapsed_seconds=now - started,
                    wrapper_elapsed_seconds=now - started,
                    last_progress_seconds=now - last_progress,
                    memory_current_bytes=current,
                    memory_peak_bytes=peak,
                    memory_anon_bytes=anon,
                    memory_file_bytes=file_bytes,
                    memory_kernel_bytes=kernel,
                    psi_full_avg10=peak_psi,
                    swap_start_bytes=swap_start,
                    swap_end_bytes=swap_end,
                )
                if classify_v38_monitor_sample(sample) is not None:
                    trigger_sample = sample
                    if terminate_command is not None:
                        subprocess.run(terminate_command, check=False)
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    break
                time.sleep(poll_seconds)
            returncode = process.wait()
        except BaseException:
            if terminate_command is not None:
                subprocess.run(terminate_command, check=False)
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            raise
    ended = time.monotonic()
    if trigger_sample is not None:
        return 1, trigger_sample
    current = _read_v38_cgroup_value(memory_cgroup / "memory.current")
    peak = max(peak, _read_v38_cgroup_value(memory_cgroup / "memory.peak"))
    swap_end = _read_v38_cgroup_value(memory_cgroup / "memory.swap.current")
    anon, file_bytes, kernel = _read_v38_memory_stat(memory_cgroup / "memory.stat")
    peak_psi = max(peak_psi, _v38_psi_full_avg10())
    sample = V38MonitorSample(
        phase=phase,
        science_elapsed_seconds=ended - started,
        wrapper_elapsed_seconds=ended - started,
        last_progress_seconds=ended - last_progress,
        memory_current_bytes=current,
        memory_peak_bytes=peak,
        memory_anon_bytes=anon,
        memory_file_bytes=file_bytes,
        memory_kernel_bytes=kernel,
        psi_full_avg10=peak_psi,
        swap_start_bytes=swap_start,
        swap_end_bytes=swap_end,
    )
    if classify_v38_monitor_sample(sample) is not None and returncode == 0:
        returncode = 1
    return returncode, sample


def _v38_slice_unit(run_id: str) -> str:
    if _TOKEN.fullmatch(run_id) is None:
        raise ValueError("V38 slice run identity differs")
    suffix = hashlib.sha256(run_id.encode()).hexdigest()[:24]
    return f"borsuk-v38-{suffix}.slice"


def build_v38_science_service_command(
    native_command: list[str], staged: V38StagedPhase
) -> list[str]:
    """Wrap native science in its phase-scoped systemd capability boundary."""

    if not native_command or not staged.inputs:
        raise ValueError("V38 native science paths differ")
    input_root = staged.inputs[0].path.parent
    output_root = (
        staged.outputs[0].path.parent
        if staged.outputs
        else input_root.parent / "outputs"
    )
    if (
        any(item.path.parent != input_root for item in staged.inputs)
        or any(item.path.parent != output_root for item in staged.outputs)
        or pathlib.Path(native_command[0]).parent == input_root
        or pathlib.Path(native_command[0]).parent == output_root
    ):
        raise ValueError("V38 native science paths differ")
    sandbox_root = pathlib.Path(
        os.path.commonpath((input_root, output_root, pathlib.Path(native_command[0])))
    )
    if sandbox_root == pathlib.Path("/"):
        raise ValueError("V38 native science sandbox differs")
    memory_limit = (
        BUILD_MEMORY_LIMIT_BYTES
        if staged.phase == "build-spill"
        else CEILING_MEMORY_LIMIT_BYTES
    )
    runtime_limit = 600 if staged.phase == "build-spill" else 120
    unit = f"borsuk-v38-science-{staged.run_id}.service"
    command = [
        "systemd-run",
        "--wait",
        "--collect",
        "--pipe",
        f"--unit={unit}",
        f"--slice={_v38_slice_unit(staged.run_id)}",
        "--quiet",
        "--property=NoNewPrivileges=true",
        "--property=PrivateNetwork=true",
        "--property=IPAddressDeny=any",
        "--property=RestrictAddressFamilies=AF_UNIX",
        "--property=ProtectSystem=strict",
        "--property=ProtectHome=true",
        "--property=ProtectControlGroups=true",
        "--property=PrivateDevices=true",
        "--property=RestrictSUIDSGID=true",
        "--property=CapabilityBoundingSet=",
        "--property=InaccessiblePaths=/run",
        "--property=PrivateTmp=true",
        f"--property=TemporaryFileSystem={sandbox_root}:ro",
        f"--property=MemoryMax={memory_limit}",
        "--property=MemorySwapMax=0",
        f"--property=RuntimeMaxSec={runtime_limit}",
        f"--property=BindReadOnlyPaths={input_root} {native_command[0]}",
        f"--property=BindPaths={output_root}",
    ]
    command.extend(native_command)
    return command


def _v38_systemd_control_group(unit: str) -> pathlib.Path:
    result = subprocess.run(
        ["systemctl", "show", "--property=ControlGroup", "--value", unit],
        check=True,
        capture_output=True,
        text=True,
    )
    control_group = result.stdout.strip()
    if (
        not control_group.startswith("/")
        or control_group == "/"
        or ".." in pathlib.PurePosixPath(control_group).parts
    ):
        raise RuntimeError("V38 aggregate cgroup is absent")
    return pathlib.Path("/sys/fs/cgroup") / control_group.lstrip("/")


def execute_v38_native_science(
    native_command: list[str],
    staged: V38StagedPhase,
    stdout_path: pathlib.Path,
    progress_path: pathlib.Path,
) -> tuple[int, V38MonitorSample]:
    """Execute native science inside the network-free capability sandbox."""

    if not staged.inputs:
        raise ValueError("V38 native science input is absent")
    input_root = staged.inputs[0].path.parent
    output_root = input_root.parent / "outputs"
    for item in staged.inputs:
        item.path.chmod(0o444)
    input_root.parent.chmod(0o711)
    input_root.chmod(0o555)
    output_root.chmod(0o733 if staged.outputs else 0o555)
    command = build_v38_science_service_command(native_command, staged)
    service = f"borsuk-v38-science-{staged.run_id}.service"
    slice_unit = _v38_slice_unit(staged.run_id)
    cgroup = _v38_systemd_control_group(slice_unit)
    try:
        return run_v38_native_process(
            command,
            stdout_path,
            progress_path,
            phase=staged.phase,
            memory_cgroup=cgroup,
            terminate_command=[
                "systemctl",
                "kill",
                "--kill-who=all",
                "--signal=TERM",
                service,
            ],
        )
    finally:
        subprocess.run(["systemctl", "reset-failed", service], check=False)


def _terminal_object(value: object, role: str, uri: str) -> None:
    if (
        type(value) is not dict
        or set(value) != {"encoded_bytes", "role", "sha256", "uri"}
        or value.get("role") != role
        or type(value.get("encoded_bytes")) is not int
        or value["encoded_bytes"] <= 0
        or _LOWER_SHA256.fullmatch(str(value.get("sha256"))) is None
        or value.get("uri") != uri
    ):
        raise ValueError("V38 terminal object differs")


def canonical_v38_terminal_bytes(
    value: object, plan: V38SpotPlan, expected_status: str
) -> bytes:
    """Validate and encode one exact terminal bound to its immutable plan."""

    build_v38_spot_plan(**dataclasses.asdict(plan))
    if expected_status == "failed":
        keys = {
            "binary_bytes",
            "binary_sha256",
            "binary_uri",
            "claim_eligible",
            "instance_id",
            "manifest_bytes",
            "manifest_sha256",
            "manifest_uri",
            "monitor",
            "phase",
            "reason",
            "run_id",
            "schema",
            "source_archive_bytes",
            "source_archive_sha256",
            "source_archive_uri",
            "source_commit",
            "status",
        }
        reasons = {
            "authority-failure",
            "boot-failure",
            "controller-failure",
            "memory-stop",
            "progress-timeout",
            "psi-stop",
            "science-timeout",
            "swap-stop",
            "worker-exit",
            "wrapper-timeout",
        }
        monitor = value.get("monitor") if type(value) is dict else None
        try:
            sample = V38MonitorSample(**monitor) if type(monitor) is dict else None
        except TypeError as error:
            raise ValueError("V38 failed terminal monitor differs") from error
        invalid_sample = sample is not None and (
            sample.phase != plan.phase
            or any(
                number < 0
                for number in (
                    sample.science_elapsed_seconds,
                    sample.wrapper_elapsed_seconds,
                    sample.last_progress_seconds,
                    sample.memory_current_bytes,
                    sample.memory_peak_bytes,
                    sample.memory_anon_bytes,
                    sample.memory_file_bytes,
                    sample.memory_kernel_bytes,
                    sample.psi_full_avg10,
                    sample.swap_start_bytes,
                    sample.swap_end_bytes,
                )
            )
            or sample.memory_peak_bytes < sample.memory_current_bytes
            or max(
                sample.memory_anon_bytes,
                sample.memory_file_bytes,
                sample.memory_kernel_bytes,
            )
            > sample.memory_peak_bytes
        )
        monitored_reasons = {
            "memory-stop",
            "progress-timeout",
            "psi-stop",
            "science-timeout",
            "swap-stop",
        }
        reason = value.get("reason") if type(value) is dict else None
        if (
            type(value) is not dict
            or set(value) != keys
            or value.get("schema") != "borsuk-v38-spot-failed-terminal-v1"
            or value.get("claim_eligible") is not False
            or value.get("status") != "failed"
            or value.get("reason") not in reasons
            or value.get("phase") != plan.phase
            or value.get("run_id") != plan.run_id
            or value.get("source_commit") != plan.source_commit
            or value.get("source_archive_uri") != plan.source_archive_uri
            or value.get("source_archive_sha256") != plan.source_archive_sha256
            or value.get("source_archive_bytes") != plan.source_archive_bytes
            or value.get("binary_uri") != plan.binary_uri
            or value.get("binary_sha256") != plan.binary_sha256
            or value.get("binary_bytes") != plan.binary_bytes
            or value.get("manifest_uri") != plan.manifest_uri
            or value.get("manifest_sha256") != plan.manifest_sha256
            or value.get("manifest_bytes") != plan.manifest_bytes
            or _INSTANCE_ID.fullmatch(str(value.get("instance_id"))) is None
            or invalid_sample
            or (
                reason in monitored_reasons
                and (
                    sample is None
                    or classify_v38_monitor_sample(sample) != reason
                )
            )
            or (
                reason == "worker-exit"
                and sample is not None
                and classify_v38_monitor_sample(sample) is not None
            )
        ):
            raise ValueError("V38 failed terminal authority differs")
        return (
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            + b"\n"
        )
    if expected_status != "complete":
        raise ValueError("V38 terminal status differs")
    expected_keys = {
        "artifacts",
        "binary_bytes",
        "binary_sha256",
        "binary_uri",
        "claim_eligible",
        "instance_id",
        "manifest_bytes",
        "manifest_sha256",
        "manifest_uri",
        "monitor",
        "phase",
        "progress",
        "result",
        "run_id",
        "schema",
        "source_archive_bytes",
        "source_archive_sha256",
        "source_archive_uri",
        "source_commit",
        "status",
    }
    if type(value) is not dict or set(value) != expected_keys:
        raise ValueError("V38 terminal schema differs")
    if (
        value.get("schema") != "borsuk-v38-spot-terminal-v1"
        or value.get("claim_eligible") is not False
        or value.get("status") != expected_status
        or expected_status != "complete"
        or value.get("phase") != plan.phase
        or value.get("run_id") != plan.run_id
        or value.get("source_commit") != plan.source_commit
        or value.get("source_archive_uri") != plan.source_archive_uri
        or value.get("source_archive_sha256") != plan.source_archive_sha256
        or value.get("source_archive_bytes") != plan.source_archive_bytes
        or value.get("binary_uri") != plan.binary_uri
        or value.get("binary_sha256") != plan.binary_sha256
        or value.get("binary_bytes") != plan.binary_bytes
        or value.get("manifest_uri") != plan.manifest_uri
        or value.get("manifest_sha256") != plan.manifest_sha256
        or value.get("manifest_bytes") != plan.manifest_bytes
        or _INSTANCE_ID.fullmatch(str(value.get("instance_id"))) is None
    ):
        raise ValueError("V38 terminal authority differs")
    monitor = value.get("monitor")
    try:
        sample = V38MonitorSample(**monitor) if type(monitor) is dict else None
    except TypeError as error:
        raise ValueError("V38 terminal monitor differs") from error
    if (
        sample is None
        or sample.phase != plan.phase
        or any(
            number < 0
            for number in (
                sample.science_elapsed_seconds,
                sample.wrapper_elapsed_seconds,
                sample.last_progress_seconds,
                sample.memory_current_bytes,
                sample.memory_peak_bytes,
                sample.memory_anon_bytes,
                sample.memory_file_bytes,
                sample.memory_kernel_bytes,
                sample.psi_full_avg10,
                sample.swap_start_bytes,
                sample.swap_end_bytes,
            )
        )
        or sample.memory_peak_bytes < sample.memory_current_bytes
        or max(
            sample.memory_anon_bytes,
            sample.memory_file_bytes,
            sample.memory_kernel_bytes,
        )
        > sample.memory_peak_bytes
        or classify_v38_monitor_sample(sample) is not None
    ):
        raise ValueError("V38 terminal monitor differs")
    artifacts = value.get("artifacts")
    roles = phase_output_roles(plan.phase)
    if (
        type(artifacts) is not list
        or len(artifacts) != len(roles)
        or any(
            _validate_identity(artifact, role) != f"{plan.output_prefix}{role}"
            for artifact, role in zip(artifacts, roles, strict=True)
        )
    ):
        raise ValueError("V38 terminal artifacts differ")
    _terminal_object(value["progress"], "progress", f"{plan.output_prefix}progress.json")
    _terminal_object(value["result"], "local-result", f"{plan.output_prefix}local-result.json")
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        + b"\n"
    )


def _canonical_json_object(raw: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"V38 {label} JSON differs") from error
    if (
        type(value) is not dict
        or json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        + b"\n"
        != raw
    ):
        raise ValueError(f"V38 {label} bytes differ")
    return value


def _bound_phase_manifest(raw: bytes, plan: V38SpotPlan) -> dict[str, object]:
    if len(raw) != plan.manifest_bytes or hashlib.sha256(raw).hexdigest() != plan.manifest_sha256:
        raise ValueError("V38 phase manifest digest differs")
    manifest = _canonical_json_object(raw, "phase manifest")
    if (
        canonical_v38_phase_manifest_bytes(manifest) != raw
        or manifest["phase"] != plan.phase
        or manifest["run_id"] != plan.run_id
        or manifest["source_commit"] != plan.source_commit
        or any(
            output.get("uri") != f"{plan.output_prefix}{output.get('role')}"
            for output in manifest["outputs"]
        )
    ):
        raise ValueError("V38 phase manifest binding differs")
    return manifest


def _plan_from_terminal(terminal: dict[str, object]) -> V38SpotPlan:
    result = terminal.get("result")
    result_uri = result.get("uri") if type(result) is dict else None
    if type(result_uri) is not str or not result_uri.endswith("local-result.json"):
        raise ValueError("V38 predecessor result URI differs")
    return build_v38_spot_plan(
        phase=terminal.get("phase"),
        run_id=terminal.get("run_id"),
        source_commit=terminal.get("source_commit"),
        source_archive_uri=terminal.get("source_archive_uri"),
        source_archive_sha256=terminal.get("source_archive_sha256"),
        source_archive_bytes=terminal.get("source_archive_bytes"),
        binary_uri=terminal.get("binary_uri"),
        binary_sha256=terminal.get("binary_sha256"),
        binary_bytes=terminal.get("binary_bytes"),
        manifest_uri=terminal.get("manifest_uri"),
        manifest_sha256=terminal.get("manifest_sha256"),
        manifest_bytes=terminal.get("manifest_bytes"),
        output_prefix=result_uri.removesuffix("local-result.json"),
    )


def _validate_v38_preflight_evidence(evidence: object) -> None:
    keys = {
        "coordinate_generator",
        "dimensions",
        "leaf_count",
        "predecessor_replay_allowance_ns",
        "projected_construction_elapsed_ns",
        "projected_peak_bytes",
        "projected_scoring_elapsed_ns",
        "projected_spill_post_scoring_elapsed_ns",
        "proposal_coordinate_scores",
        "proposal_coordinates_per_second",
        "relation_rows",
        "rows",
        "scalar_fused_comparisons",
        "scalar_fused_max_ulp_delta",
    }
    if type(evidence) is not dict or set(evidence) != keys:
        raise ValueError("V38 preflight evidence schema differs")
    integers = keys - {"coordinate_generator"}
    if any(type(evidence[key]) is not int or evidence[key] < 0 for key in integers):
        raise ValueError("V38 preflight evidence type differs")
    if (
        evidence["coordinate_generator"] != "splitmix23-f32-v1"
        or evidence["rows"] != 65_536
        or evidence["dimensions"] != 192
        or evidence["leaf_count"] != 16
        or evidence["proposal_coordinate_scores"] != 188_743_680
        or evidence["proposal_coordinates_per_second"] < 78_080_000
        or evidence["scalar_fused_comparisons"] != 15_360
        or evidence["scalar_fused_max_ulp_delta"] != 0
        or evidence["predecessor_replay_allowance_ns"] != 120_000_000_000
        or evidence["projected_peak_bytes"] != 2_783_657_984
        or evidence["projected_scoring_elapsed_ns"] > 300_000_000_000
        or evidence["projected_construction_elapsed_ns"] > 450_000_000_000
        or evidence["projected_construction_elapsed_ns"]
        != evidence["projected_scoring_elapsed_ns"]
        + evidence["projected_spill_post_scoring_elapsed_ns"]
        + evidence["predecessor_replay_allowance_ns"]
        or not 65_536 <= evidence["relation_rows"] <= 81_920
    ):
        raise ValueError("V38 preflight admission evidence differs")


def validate_v38_preflight_admission(
    *,
    build_plan: V38SpotPlan,
    build_manifest_raw: bytes,
    preflight_manifest_raw: bytes,
    preflight_result_raw: bytes,
    preflight_terminal_raw: bytes,
) -> dict[str, object]:
    """Admit construction only from the exact successful reduced preflight."""

    if build_plan.phase != "build-spill":
        raise ValueError("V38 preflight successor phase differs")
    build_manifest = _bound_phase_manifest(build_manifest_raw, build_plan)
    terminal = _canonical_json_object(preflight_terminal_raw, "preflight terminal")
    preflight_plan = _plan_from_terminal(terminal)
    if preflight_plan.phase != "preflight-spill":
        raise ValueError("V38 preflight phase differs")
    canonical_v38_terminal_bytes(terminal, preflight_plan, "complete")
    if (
        preflight_plan.source_commit != build_plan.source_commit
        or preflight_plan.source_archive_uri != build_plan.source_archive_uri
        or preflight_plan.source_archive_sha256 != build_plan.source_archive_sha256
        or preflight_plan.source_archive_bytes != build_plan.source_archive_bytes
        or preflight_plan.binary_uri != build_plan.binary_uri
        or preflight_plan.binary_sha256 != build_plan.binary_sha256
        or preflight_plan.binary_bytes != build_plan.binary_bytes
    ):
        raise ValueError("V38 preflight executable binding differs")
    preflight_manifest = _bound_phase_manifest(preflight_manifest_raw, preflight_plan)
    result = _canonical_json_object(preflight_result_raw, "preflight result")
    terminal_result = terminal["result"]
    if (
        type(terminal_result) is not dict
        or len(preflight_result_raw) != terminal_result.get("encoded_bytes")
        or hashlib.sha256(preflight_result_raw).hexdigest()
        != terminal_result.get("sha256")
        or set(result)
        != {"claim_eligible", "inputs", "mode", "preflight_evidence", "schema"}
        or result.get("schema") != "borsuk-v38-local-result-v1"
        or result.get("claim_eligible") is not False
        or result.get("mode") != "preflight-spill"
        or result.get("inputs") != preflight_manifest["inputs"]
        or preflight_manifest["workers"] != build_manifest["workers"]
        or preflight_manifest["inputs"] != build_manifest["inputs"][:1]
    ):
        raise ValueError("V38 preflight successor binding differs")
    _validate_v38_preflight_evidence(result.get("preflight_evidence"))
    return result


def validate_v38_ceiling_predecessor(
    *,
    ceiling_plan: V38SpotPlan,
    ceiling_manifest_raw: bytes,
    build_plan: V38SpotPlan,
    build_manifest_raw: bytes,
    build_result_raw: bytes,
    build_terminal_raw: bytes,
) -> dict[str, object]:
    """Admit ceiling evaluation only from one sealed construction chain."""

    if ceiling_plan.phase != "evaluate-ceiling" or build_plan.phase != "build-spill":
        raise ValueError("V38 ceiling predecessor phase differs")
    if (
        ceiling_plan.source_commit != build_plan.source_commit
        or ceiling_plan.source_archive_uri != build_plan.source_archive_uri
        or ceiling_plan.source_archive_sha256 != build_plan.source_archive_sha256
        or ceiling_plan.source_archive_bytes != build_plan.source_archive_bytes
        or ceiling_plan.binary_uri != build_plan.binary_uri
        or ceiling_plan.binary_sha256 != build_plan.binary_sha256
        or ceiling_plan.binary_bytes != build_plan.binary_bytes
    ):
        raise ValueError("V38 ceiling executable predecessor differs")
    ceiling_manifest = _bound_phase_manifest(ceiling_manifest_raw, ceiling_plan)
    build_manifest = _bound_phase_manifest(build_manifest_raw, build_plan)
    terminal = _canonical_json_object(build_terminal_raw, "build terminal")
    if _plan_from_terminal(terminal) != build_plan:
        raise ValueError("V38 build terminal plan differs")
    canonical_v38_terminal_bytes(terminal, build_plan, "complete")
    result = _canonical_json_object(build_result_raw, "construction result")
    terminal_result = terminal["result"]
    if (
        type(terminal_result) is not dict
        or len(build_result_raw) != terminal_result.get("encoded_bytes")
        or hashlib.sha256(build_result_raw).hexdigest() != terminal_result.get("sha256")
        or set(result)
        != {"artifacts", "claim_eligible", "construction_evidence", "inputs", "mode", "schema"}
        or result.get("schema") != "borsuk-v38-local-result-v1"
        or result.get("claim_eligible") is not False
        or result.get("mode") != "build-spill"
        or result.get("inputs") != build_manifest["inputs"]
        or result.get("artifacts") != terminal["artifacts"]
    ):
        raise ValueError("V38 construction predecessor differs")
    _validate_v38_construction_evidence(result.get("construction_evidence"))
    import blake3

    expected_result = {
        "blake3": blake3.blake3(build_result_raw).hexdigest(),
        "encoded_bytes": len(build_result_raw),
        "role": "v38-construction-result",
        "sha256": hashlib.sha256(build_result_raw).hexdigest(),
        "uri": terminal_result["uri"],
    }
    ceiling_inputs = ceiling_manifest["inputs"]
    if (
        ceiling_inputs[1] != expected_result
        or ceiling_inputs[2:] [:2] != result["artifacts"]
        or ceiling_manifest["source_commit"] != build_plan.source_commit
    ):
        raise ValueError("V38 ceiling artifact predecessor differs")
    return result


def _local_identity(item: V38StagedArtifact) -> dict[str, object]:
    return {
        "blake3": item.blake3,
        "encoded_bytes": item.encoded_bytes,
        "role": item.role,
        "sha256": item.sha256,
        "uri": item.uri,
    }


def _validate_v38_construction_evidence(value: object) -> None:
    keys = {
        "accepted_alternates",
        "capacity_rejected",
        "exhausted",
        "maximum_posting_population",
        "owners_one",
        "owners_two",
        "proposed_alternates",
        "total_assignments",
    }
    if (
        type(value) is not dict
        or set(value) != keys
        or type(value.get("exhausted")) is not bool
        or any(type(value.get(key)) is not int for key in keys - {"exhausted"})
    ):
        raise ValueError("V38 construction evidence schema differs")
    accepted = value["accepted_alternates"]
    rejected = value["capacity_rejected"]
    proposed = value["proposed_alternates"]
    if (
        proposed != 1_000_000
        or not 0 <= accepted <= 250_000
        or not 0 <= rejected <= proposed
        or accepted + rejected > proposed
        or value["owners_one"] + value["owners_two"] != 1_000_000
        or value["owners_two"] != accepted
        or value["total_assignments"] != 1_000_000 + accepted
        or not 1 <= value["maximum_posting_population"] <= 10_240
        or value["exhausted"] == (accepted == 250_000)
    ):
        raise ValueError("V38 construction evidence formula differs")


def _validate_v38_ceiling_result(
    result: object, staged: V38StagedPhase
) -> None:
    keys = {
        "aggregate_certified_upper_ppm",
        "aggregate_feasible_ppm",
        "aggregate_gate_ppm",
        "authority",
        "certificates",
        "claim_eligible",
        "disposition",
        "exact_query_count",
        "gt_neighbors",
        "minimum_certified_upper_ppm",
        "minimum_feasible_ppm",
        "minimum_gate_ppm",
        "passed",
        "schema",
        "selected_postings",
        "total_solver_visits",
    }
    if type(result) is not dict or set(result) != keys:
        raise ValueError("V38 native ceiling result schema differs")
    integer_fields = keys - {
        "authority",
        "certificates",
        "claim_eligible",
        "disposition",
        "passed",
        "schema",
    }
    if (
        result.get("schema") != "borsuk-v38-boundary-spill-ceiling-v1"
        or result.get("claim_eligible") is not False
        or type(result.get("passed")) is not bool
        or any(type(result.get(key)) is not int for key in integer_fields)
        or result.get("gt_neighbors") != 100
        or result.get("selected_postings") != 14
        or result.get("aggregate_gate_ppm") != 998_000
        or result.get("minimum_gate_ppm") != 800_000
    ):
        raise ValueError("V38 native ceiling result authority differs")
    authority_keys = {
        "aggregate_gate_ppm",
        "construction_result",
        "development_ground_truth",
        "gt_neighbors",
        "maximum_certificate_bytes",
        "maximum_query_solver_nodes",
        "maximum_solver_nodes",
        "minimum_gate_ppm",
        "posting_summary",
        "query_count",
        "relation",
        "schema",
        "selected_postings",
    }
    authority = result.get("authority")
    if type(authority) is not dict or set(authority) != authority_keys:
        raise ValueError("V38 ceiling authority schema differs")
    expected_inputs = [_local_identity(item) for item in staged.inputs]
    authority_raw = staged.inputs[0].path.read_bytes()
    if (
        _canonical_json_object(authority_raw, "ceiling authority") != authority
        or authority_raw
        != json.dumps(
            authority, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        + b"\n"
        or authority.get("schema")
        != "borsuk-v38-boundary-spill-ceiling-authority-v1"
        or authority.get("construction_result") != expected_inputs[1]
        or authority.get("relation") != expected_inputs[2]
        or authority.get("posting_summary") != expected_inputs[3]
        or authority.get("development_ground_truth") != expected_inputs[4]
        or authority.get("gt_neighbors") != 100
        or authority.get("selected_postings") != 14
        or authority.get("aggregate_gate_ppm") != 998_000
        or authority.get("minimum_gate_ppm") != 800_000
        or authority.get("query_count") != 1_000
        or authority.get("maximum_certificate_bytes") != 256
        or authority.get("maximum_query_solver_nodes") != 250_000
        or authority.get("maximum_solver_nodes") != 25_000_000
    ):
        raise ValueError("V38 ceiling authority binding differs")
    certificates = result.get("certificates")
    if type(certificates) is not list or len(certificates) != 1_000:
        raise ValueError("V38 ceiling certificates differ")
    feasible_hits = 0
    upper_hits = 0
    minimum_feasible = 100
    minimum_upper = 100
    exact_queries = 0
    solver_visits = 0
    certificate_keys = {
        "certified_upper_hits",
        "exact",
        "feasible_hits",
        "query_ordinal",
        "selected_postings",
        "solver_visits",
    }
    for query_ordinal, certificate in enumerate(certificates):
        if type(certificate) is not dict or set(certificate) != certificate_keys:
            raise ValueError("V38 ceiling certificate schema differs")
        selected = certificate.get("selected_postings")
        if (
            type(certificate.get("query_ordinal")) is not int
            or certificate["query_ordinal"] != query_ordinal
            or type(selected) is not list
            or len(selected) != 14
            or any(type(posting) is not int for posting in selected)
            or selected != sorted(set(selected))
            or not all(0 <= posting < 123 for posting in selected)
            or type(certificate.get("feasible_hits")) is not int
            or type(certificate.get("certified_upper_hits")) is not int
            or not 0
            <= certificate["feasible_hits"]
            <= certificate["certified_upper_hits"]
            <= 100
            or type(certificate.get("exact")) is not bool
            or (
                certificate["exact"]
                and certificate["feasible_hits"]
                != certificate["certified_upper_hits"]
            )
            or type(certificate.get("solver_visits")) is not int
            or not 0 <= certificate["solver_visits"] <= 250_000
            or len(
                json.dumps(
                    certificate,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode()
                + b"\n"
            )
            > 256
        ):
            raise ValueError("V38 ceiling certificate differs")
        feasible_hits += certificate["feasible_hits"]
        upper_hits += certificate["certified_upper_hits"]
        minimum_feasible = min(minimum_feasible, certificate["feasible_hits"])
        minimum_upper = min(minimum_upper, certificate["certified_upper_hits"])
        exact_queries += int(certificate["exact"])
        solver_visits += certificate["solver_visits"]
    aggregate_feasible = feasible_hits * 1_000_000 // 100_000
    aggregate_upper = upper_hits * 1_000_000 // 100_000
    minimum_feasible *= 10_000
    minimum_upper *= 10_000
    feasible = aggregate_feasible >= 998_000 and minimum_feasible >= 800_000
    rejected = aggregate_upper < 998_000 or minimum_upper < 800_000
    disposition = (
        "layout-feasible"
        if feasible
        else "layout-rejected"
        if rejected
        else "indeterminate"
    )
    if (
        result.get("aggregate_feasible_ppm") != aggregate_feasible
        or result.get("aggregate_certified_upper_ppm") != aggregate_upper
        or result.get("minimum_feasible_ppm") != minimum_feasible
        or result.get("minimum_certified_upper_ppm") != minimum_upper
        or result.get("exact_query_count") != exact_queries
        or result.get("total_solver_visits") != solver_visits
        or solver_visits > 25_000_000
        or result.get("passed") is not feasible
        or result.get("disposition") != disposition
    ):
        raise ValueError("V38 ceiling aggregate differs")


def validate_v38_local_result_bytes(
    raw: bytes, staged: V38StagedPhase
) -> dict[str, object]:
    """Authenticate the native phase result before publication."""

    result = _canonical_json_object(raw, "native result")
    expected_inputs = [_local_identity(item) for item in staged.inputs]
    if result.get("claim_eligible") is not False:
        raise ValueError("V38 native result claim authority differs")
    if staged.phase == "preflight-spill":
        if (
            set(result)
            != {"claim_eligible", "inputs", "mode", "preflight_evidence", "schema"}
            or result.get("schema") != "borsuk-v38-local-result-v1"
            or result.get("mode") != staged.phase
            or result.get("inputs") != expected_inputs
        ):
            raise ValueError("V38 native preflight result differs")
        _validate_v38_preflight_evidence(result.get("preflight_evidence"))
    elif staged.phase == "build-spill":
        if (
            set(result)
            != {"artifacts", "claim_eligible", "construction_evidence", "inputs", "mode", "schema"}
            or result.get("schema") != "borsuk-v38-local-result-v1"
            or result.get("mode") != staged.phase
            or result.get("inputs") != expected_inputs
            or type(result.get("artifacts")) is not list
            or result["artifacts"]
            != [
                {
                    **_encoded_file_identity(output.role, output.uri, output.path),
                }
                for output in staged.outputs
            ]
        ):
            raise ValueError("V38 native construction result differs")
        _validate_v38_construction_evidence(result.get("construction_evidence"))
    elif staged.phase == "evaluate-ceiling":
        if (
            len(staged.inputs) != 5
            or len(staged.outputs) != 1
            or staged.outputs[0].path.read_bytes() != raw
        ):
            raise ValueError("V38 native ceiling result differs")
        _validate_v38_ceiling_result(result, staged)
    else:
        raise ValueError("V38 native result phase differs")
    return result


def validate_v38_progress_bytes(
    raw: bytes, phase: str, previous: dict[str, object] | None
) -> dict[str, object]:
    """Validate one strictly advancing native progress snapshot."""

    value = _canonical_json_object(raw, "progress")
    if phase in {"preflight-spill", "build-spill"}:
        completed_key = "completed_rows"
        total_key = "total_rows"
        schema = "borsuk-v38-preflight-progress-v1"
        expected_total = 65_536 if phase == "preflight-spill" else 1_000_000
    elif phase == "evaluate-ceiling":
        completed_key = "completed_queries"
        total_key = "total_queries"
        schema = "borsuk-v38-ceiling-progress-v1"
        expected_total = 1_000
    else:
        raise ValueError("V38 progress phase differs")
    if (
        set(value) != {completed_key, "schema", total_key}
        or value.get("schema") != schema
        or type(value.get(completed_key)) is not int
        or type(value.get(total_key)) is not int
        or not 0 < value[completed_key] <= value[total_key]
        or value[total_key] != expected_total
        or (
            previous is not None
            and (
                value[completed_key] <= previous[completed_key]
                or value[total_key] != previous[total_key]
            )
        )
    ):
        raise ValueError("V38 native progress differs")
    return value


def _encoded_file_identity(role: str, uri: str, path: pathlib.Path) -> dict[str, object]:
    import blake3

    sha256 = hashlib.sha256()
    blake3_digest = blake3.blake3()
    encoded_bytes = 0
    with path.open("rb") as source:
        while chunk := source.read(1_048_576):
            encoded_bytes += len(chunk)
            sha256.update(chunk)
            blake3_digest.update(chunk)
    if encoded_bytes == 0:
        raise ValueError(f"V38 {role} output is empty")
    return {
        "blake3": blake3_digest.hexdigest(),
        "encoded_bytes": encoded_bytes,
        "role": role,
        "sha256": sha256.hexdigest(),
        "uri": uri,
    }


def publish_v38_worker_success(
    plan: V38SpotPlan,
    staged: V38StagedPhase,
    *,
    instance_id: str,
    result_raw: bytes,
    final_progress_raw: bytes,
    monitor: V38MonitorSample,
    upload_once: Callable[[str, str, bytes], None],
    publish_terminal: bool = True,
) -> bytes:
    """Publish authenticated artifacts and result before the terminal marker."""

    if (
        staged.phase != plan.phase
        or staged.run_id != plan.run_id
        or staged.source_commit != plan.source_commit
        or monitor.phase != plan.phase
    ):
        raise ValueError("V38 staged worker authority differs")
    validate_v38_local_result_bytes(result_raw, staged)
    final_progress = validate_v38_progress_bytes(final_progress_raw, staged.phase, None)
    completed_key = "completed_queries" if staged.phase == "evaluate-ceiling" else "completed_rows"
    total_key = "total_queries" if staged.phase == "evaluate-ceiling" else "total_rows"
    if final_progress[completed_key] != final_progress[total_key]:
        raise ValueError("V38 final progress differs")
    bucket, prefix = _s3_uri(plan.output_prefix, prefix=True)
    artifacts = []
    for output in staged.outputs:
        if output.uri != f"{plan.output_prefix}{output.role}":
            raise ValueError("V38 worker output URI differs")
        body = output.path.read_bytes()
        identity = _encoded_file_identity(output.role, output.uri, output.path)
        upload_once(bucket, prefix + output.role, body)
        artifacts.append(identity)
    upload_once(bucket, prefix + "local-result.json", result_raw)
    upload_once(bucket, prefix + "progress.json", final_progress_raw)
    terminal = {
        "artifacts": artifacts,
        "binary_bytes": plan.binary_bytes,
        "binary_sha256": plan.binary_sha256,
        "binary_uri": plan.binary_uri,
        "claim_eligible": False,
        "instance_id": instance_id,
        "manifest_bytes": plan.manifest_bytes,
        "manifest_sha256": plan.manifest_sha256,
        "manifest_uri": plan.manifest_uri,
        "monitor": dataclasses.asdict(monitor),
        "phase": plan.phase,
        "progress": {
            "encoded_bytes": len(final_progress_raw),
            "role": "progress",
            "sha256": hashlib.sha256(final_progress_raw).hexdigest(),
            "uri": f"{plan.output_prefix}progress.json",
        },
        "result": {
            "encoded_bytes": len(result_raw),
            "role": "local-result",
            "sha256": hashlib.sha256(result_raw).hexdigest(),
            "uri": f"{plan.output_prefix}local-result.json",
        },
        "run_id": plan.run_id,
        "schema": "borsuk-v38-spot-terminal-v1",
        "source_archive_bytes": plan.source_archive_bytes,
        "source_archive_sha256": plan.source_archive_sha256,
        "source_archive_uri": plan.source_archive_uri,
        "source_commit": plan.source_commit,
        "status": "complete",
    }
    terminal_raw = canonical_v38_terminal_bytes(terminal, plan, "complete")
    if publish_terminal:
        upload_once(bucket, prefix + "ATTEMPT_TERMINAL.json", terminal_raw)
    return terminal_raw


def execute_v38_worker(
    plan: V38SpotPlan,
    *,
    root: pathlib.Path,
    manifest_raw: bytes,
    binary: pathlib.Path,
    instance_id: str,
    download: Callable[[str, str, pathlib.Path], None],
    execute: Callable[
        [list[str], V38StagedPhase, pathlib.Path, pathlib.Path],
        tuple[int, V38MonitorSample],
    ],
    upload_once: Callable[[str, str, bytes], None],
    finalize_monitor: Callable[[V38MonitorSample | None], V38MonitorSample] | None = None,
) -> bytes:
    """Run one authenticated native phase and clean every named local file."""

    manifest = _bound_phase_manifest(manifest_raw, plan)
    if root.exists():
        raise ValueError("V38 worker root already exists")
    stdout_path = root / "local-result.json"
    progress_path = root / "progress.log"
    final_progress_path = root / "progress.json"
    staged: V38StagedPhase | None = None
    terminal_raw: bytes | None = None
    try:
        staged = stage_v38_phase_inputs(manifest_raw, root, download)
        command = build_v38_binary_command(binary, staged)
        returncode, sample = execute(command, staged, stdout_path, progress_path)
        stop = classify_v38_monitor_sample(sample)
        if stop is not None:
            error = RuntimeError(f"V38 native worker crossed {stop}")
            error.v38_monitor = sample
            raise error
        if returncode != 0:
            stderr_tail = progress_path.read_bytes()[-8_192:].decode(errors="replace")
            error = RuntimeError(
                f"V38 native worker exited {returncode}; stderr-tail={stderr_tail!r}"
            )
            error.v38_monitor = sample
            raise error
        result_raw = stdout_path.read_bytes()
        validate_v38_local_result_bytes(result_raw, staged)
        previous = None
        lines = progress_path.read_bytes().splitlines(keepends=True)
        if not lines:
            raise ValueError("V38 native progress is absent")
        for line in lines:
            previous = validate_v38_progress_bytes(line, staged.phase, previous)
        final_progress_raw = lines[-1]
        final_progress_path.write_bytes(final_progress_raw)
        terminal_raw = publish_v38_worker_success(
            plan,
            staged,
            instance_id=instance_id,
            result_raw=result_raw,
            final_progress_raw=final_progress_raw,
            monitor=sample,
            upload_once=upload_once,
            publish_terminal=False,
        )
        if finalize_monitor is not None:
            sample = finalize_monitor(sample)
            stop = classify_v38_monitor_sample(sample)
            if stop is not None:
                error = RuntimeError(f"V38 wrapper crossed {stop}")
                error.v38_monitor = sample
                raise error
            terminal = json.loads(terminal_raw)
            terminal["monitor"] = dataclasses.asdict(sample)
            terminal_raw = canonical_v38_terminal_bytes(terminal, plan, "complete")
    finally:
        for identity in manifest["inputs"]:
            (root / "inputs" / identity["role"]).unlink(missing_ok=True)
        for output in manifest["outputs"]:
            (root / "outputs" / output["role"]).unlink(missing_ok=True)
        for path in (stdout_path, progress_path, final_progress_path):
            path.unlink(missing_ok=True)
        for directory in (root / "inputs", root / "outputs", root):
            try:
                directory.rmdir()
            except FileNotFoundError:
                pass
    if terminal_raw is None:
        raise RuntimeError("V38 worker terminal is absent")
    bucket, prefix = _s3_uri(plan.output_prefix, prefix=True)
    upload_once(bucket, prefix + "ATTEMPT_TERMINAL.json", terminal_raw)
    return terminal_raw


def _aws_error_code(error: BaseException) -> str | None:
    response = getattr(error, "response", None)
    if type(response) is not dict or type(response.get("Error")) is not dict:
        return None
    code = response["Error"].get("Code")
    return code if type(code) is str else None


def _read_v38_exact_s3_object(
    s3_client: Any,
    uri: str,
    *,
    expected_bytes: int | None = None,
    expected_sha256: str | None = None,
    maximum_bytes: int = 16 * 1_048_576,
) -> bytes:
    bucket, key = _s3_uri(uri)
    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
        ExpectedBucketOwner=EXPECTED_AWS_ACCOUNT,
        ChecksumMode="ENABLED",
    )
    length = response.get("ContentLength")
    if (
        type(length) is not int
        or not 0 < length <= maximum_bytes
        or (expected_bytes is not None and length != expected_bytes)
    ):
        raise ValueError("V38 S3 object length differs")
    raw = response["Body"].read(length + 1)
    if len(raw) != length:
        raise ValueError("V38 S3 object body length differs")
    if expected_sha256 is not None and hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("V38 S3 object digest differs")
    return raw


def validate_v38_terminal_bytes(
    raw: bytes, plan: V38SpotPlan, expected_status: str
) -> dict[str, object]:
    """Authenticate one canonical terminal and its exact disposition."""

    terminal = _canonical_json_object(raw, "terminal")
    if (
        terminal.get("status") != expected_status
        or canonical_v38_terminal_bytes(terminal, plan, expected_status) != raw
    ):
        raise ValueError("V38 terminal bytes differ")
    return terminal


def _admit_v38_build_preflight(
    plan: V38SpotPlan,
    s3_client: Any,
    terminal_uri: str,
    build_manifest_raw: bytes,
) -> None:
    if not terminal_uri.endswith("/ATTEMPT_TERMINAL.json"):
        raise ValueError("V38 preflight terminal URI differs")
    terminal_raw = _read_v38_exact_s3_object(
        s3_client, terminal_uri, maximum_bytes=1_048_576
    )
    terminal = _canonical_json_object(terminal_raw, "preflight terminal")
    result = terminal.get("result")
    if (
        type(result) is not dict
        or type(terminal.get("manifest_uri")) is not str
        or type(terminal.get("manifest_bytes")) is not int
        or type(terminal.get("manifest_sha256")) is not str
        or type(result.get("uri")) is not str
        or type(result.get("encoded_bytes")) is not int
        or type(result.get("sha256")) is not str
    ):
        raise ValueError("V38 preflight terminal authority differs")
    preflight_manifest_raw = _read_v38_exact_s3_object(
        s3_client,
        terminal["manifest_uri"],
        expected_bytes=terminal["manifest_bytes"],
        expected_sha256=terminal["manifest_sha256"],
    )
    preflight_result_raw = _read_v38_exact_s3_object(
        s3_client,
        result["uri"],
        expected_bytes=result["encoded_bytes"],
        expected_sha256=result["sha256"],
    )
    validate_v38_preflight_admission(
        build_plan=plan,
        build_manifest_raw=build_manifest_raw,
        preflight_manifest_raw=preflight_manifest_raw,
        preflight_result_raw=preflight_result_raw,
        preflight_terminal_raw=terminal_raw,
    )


def _admit_v38_ceiling_build(
    plan: V38SpotPlan,
    s3_client: Any,
    terminal_uri: str,
    ceiling_manifest_raw: bytes,
) -> None:
    if not terminal_uri.endswith("/ATTEMPT_TERMINAL.json"):
        raise ValueError("V38 build terminal URI differs")
    terminal_raw = _read_v38_exact_s3_object(
        s3_client, terminal_uri, maximum_bytes=1_048_576
    )
    terminal = _canonical_json_object(terminal_raw, "build terminal")
    build_plan = _plan_from_terminal(terminal)
    result = terminal.get("result")
    if (
        build_plan.phase != "build-spill"
        or type(result) is not dict
        or type(result.get("uri")) is not str
        or type(result.get("encoded_bytes")) is not int
        or type(result.get("sha256")) is not str
    ):
        raise ValueError("V38 build predecessor authority differs")
    build_manifest_raw = _read_v38_exact_s3_object(
        s3_client,
        build_plan.manifest_uri,
        expected_bytes=build_plan.manifest_bytes,
        expected_sha256=build_plan.manifest_sha256,
    )
    build_result_raw = _read_v38_exact_s3_object(
        s3_client,
        result["uri"],
        expected_bytes=result["encoded_bytes"],
        expected_sha256=result["sha256"],
    )
    validate_v38_ceiling_predecessor(
        ceiling_plan=plan,
        ceiling_manifest_raw=ceiling_manifest_raw,
        build_plan=build_plan,
        build_manifest_raw=build_manifest_raw,
        build_result_raw=build_result_raw,
        build_terminal_raw=terminal_raw,
    )


def run_v38_spot_phase(
    plan: V38SpotPlan,
    *,
    preflight_terminal_uri: str | None = None,
    build_terminal_uri: str | None = None,
    ec2_client: Any,
    s3_client: Any,
    sleep: Callable[[float], None],
    monotonic: Callable[[], float],
) -> str:
    """Launch one Spot worker, preserve its terminal, and terminate it."""

    bucket, prefix = _s3_uri(plan.output_prefix, prefix=True)
    terminal_uri = f"s3://{bucket}/{prefix}ATTEMPT_TERMINAL.json"
    failure_sealed = False

    def upload_once(upload_bucket: str, upload_key: str, body: bytes) -> None:
        s3_client.put_object(
            Bucket=upload_bucket,
            Key=upload_key,
            Body=body,
            ExpectedBucketOwner=EXPECTED_AWS_ACCOUNT,
            IfNoneMatch="*",
            ChecksumAlgorithm="SHA256",
        )

    def seal_failure(instance: str, reason: str) -> None:
        nonlocal failure_sealed
        publish_v38_worker_failure(
            plan,
            instance_id=instance,
            reason=reason,
            upload_once=upload_once,
        )
        failure_sealed = True

    def accept_terminal(raw: bytes, instance: str) -> str:
        nonlocal failure_sealed
        failure_sealed = True
        terminal = _canonical_json_object(raw, "terminal")
        status = terminal.get("status")
        if status not in {"complete", "failed"}:
            raise ValueError("V38 terminal status differs")
        validate_v38_terminal_bytes(raw, plan, status)
        if terminal["instance_id"] != instance:
            raise ValueError("V38 terminal instance differs")
        if status != "complete":
            raise RuntimeError(f"V38 worker failed at {terminal_uri}")
        return terminal_uri

    def read_terminal() -> bytes | None:
        try:
            return _read_v38_exact_s3_object(
                s3_client, terminal_uri, maximum_bytes=1_048_576
            )
        except (KeyError, FileNotFoundError):
            return None
        except Exception as error:
            if _aws_error_code(error) in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    if read_terminal() is not None:
        raise ValueError("V38 terminal already exists")
    if plan.phase == "build-spill" and preflight_terminal_uri is None:
        raise ValueError("V38 preflight terminal is required")
    if plan.phase == "evaluate-ceiling" and build_terminal_uri is None:
        raise ValueError("V38 build terminal is required")
    if (
        plan.phase == "evaluate-ceiling"
        and build_terminal_uri is not None
        and not build_terminal_uri.endswith("/ATTEMPT_TERMINAL.json")
    ):
        raise ValueError("V38 build terminal URI differs")
    if plan.phase == "preflight-spill" and (
        preflight_terminal_uri is not None or build_terminal_uri is not None
    ):
        raise ValueError("V38 predecessor terminal is forbidden")
    manifest_raw = _read_v38_exact_s3_object(
        s3_client,
        plan.manifest_uri,
        expected_bytes=plan.manifest_bytes,
        expected_sha256=plan.manifest_sha256,
    )
    _bound_phase_manifest(manifest_raw, plan)
    if plan.phase == "build-spill":
        if build_terminal_uri is not None:
            raise ValueError("V38 build predecessor terminal differs")
        _admit_v38_build_preflight(
            plan, s3_client, preflight_terminal_uri, manifest_raw
        )
    elif plan.phase == "evaluate-ceiling":
        if preflight_terminal_uri is not None:
            raise ValueError("V38 ceiling preflight terminal is forbidden")
        _admit_v38_ceiling_build(plan, s3_client, build_terminal_uri, manifest_raw)
    instance_id = None
    capacity_errors = {
        "InsufficientInstanceCapacity",
        "InsufficientHostCapacity",
        "MaxSpotInstanceCountExceeded",
        "SpotMaxPriceTooLow",
    }
    for spec in build_v38_launch_specs(plan, user_data=build_v38_worker_script(plan)):
        try:
            response = ec2_client.run_instances(**spec)
        except Exception as error:
            if _aws_error_code(error) in capacity_errors:
                continue
            raise
        instances = response.get("Instances")
        if type(instances) is not list or len(instances) != 1:
            raise ValueError("V38 Spot launch response differs")
        candidate = instances[0].get("InstanceId")
        if type(candidate) is not str or _INSTANCE_ID.fullmatch(candidate) is None:
            raise ValueError("V38 Spot instance identity differs")
        instance_id = candidate
        break
    if instance_id is None:
        raise RuntimeError("V38 Spot capacity unavailable")
    wrapper_timeout = 720 if plan.phase == "build-spill" else 180
    started = monotonic()
    terminated = False
    try:
        while monotonic() - started <= wrapper_timeout:
            raw = read_terminal()
            if raw is not None:
                return accept_terminal(raw, instance_id)
            response = ec2_client.describe_instances(InstanceIds=[instance_id])
            reservations = response.get("Reservations")
            if type(reservations) is not list or len(reservations) != 1:
                raise ValueError("V38 Spot instance status differs")
            instances = reservations[0].get("Instances")
            if type(instances) is not list or len(instances) != 1:
                raise ValueError("V38 Spot instance status differs")
            state = instances[0].get("State")
            name = state.get("Name") if type(state) is dict else None
            if name in {"shutting-down", "terminated", "stopped", "stopping"}:
                seal_failure(instance_id, "boot-failure")
                raise RuntimeError("V38 worker failed at boot-failure")
            if name not in {"pending", "running"}:
                raise ValueError("V38 Spot instance state differs")
            sleep(5)
        ec2_client.terminate_instances(InstanceIds=[instance_id])
        terminated = True
        raw = read_terminal()
        if raw is not None:
            return accept_terminal(raw, instance_id)
        seal_failure(instance_id, "wrapper-timeout")
        raise TimeoutError("V38 Spot wrapper timed out")
    except BaseException:
        if not failure_sealed:
            try:
                seal_failure(instance_id, "controller-failure")
            except BaseException:
                pass
        raise
    finally:
        if not terminated:
            ec2_client.terminate_instances(InstanceIds=[instance_id])


def run_v38_causality_phase(
    plan: V38SpotPlan,
    *,
    preflight_terminal_uri: str | None = None,
    build_terminal_uri: str | None = None,
    session_factory: Callable[..., Any] | None = None,
) -> str:
    """Run one phase only through the pinned causality account and region."""

    if session_factory is None:
        import boto3

        session_factory = boto3.Session
    session = session_factory(profile_name=PROFILE, region_name=REGION)
    if getattr(session, "region_name", None) != REGION:
        raise ValueError("V38 AWS region differs")
    sts_client = session.client("sts", region_name=REGION)
    identity = sts_client.get_caller_identity()
    if type(identity) is not dict or identity.get("Account") != EXPECTED_AWS_ACCOUNT:
        raise ValueError("V38 AWS account differs")
    ec2_client = session.client("ec2", region_name=REGION)
    s3_client = session.client("s3", region_name=REGION)
    return run_v38_spot_phase(
        plan,
        preflight_terminal_uri=preflight_terminal_uri,
        build_terminal_uri=build_terminal_uri,
        ec2_client=ec2_client,
        s3_client=s3_client,
        sleep=time.sleep,
        monotonic=time.monotonic,
    )


def _load_v38_spot_plan_bytes(raw: bytes) -> V38SpotPlan:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V38 worker plan JSON differs") from error
    expected = {field.name for field in dataclasses.fields(V38SpotPlan)}
    if type(value) is not dict or set(value) != expected:
        raise ValueError("V38 worker plan schema differs")
    plan = build_v38_spot_plan(**value)
    if json.dumps(value, sort_keys=True, separators=(",", ":")).encode() != raw:
        raise ValueError("V38 worker plan bytes differ")
    return plan


def _v38_worker_failure_reason(error: BaseException) -> str:
    message = str(error)
    for reason in (
        "memory-stop",
        "progress-timeout",
        "psi-stop",
        "science-timeout",
        "swap-stop",
        "wrapper-timeout",
    ):
        if reason in message:
            return reason
    if "worker exited" in message:
        return "worker-exit"
    return "authority-failure"


def publish_v38_worker_failure(
    plan: V38SpotPlan,
    *,
    instance_id: str,
    reason: str,
    monitor: V38MonitorSample | None = None,
    upload_once: Callable[[str, str, bytes], None],
) -> bytes:
    """Seal one exact claim-ineligible failed terminal."""

    terminal = {
        "binary_bytes": plan.binary_bytes,
        "binary_sha256": plan.binary_sha256,
        "binary_uri": plan.binary_uri,
        "claim_eligible": False,
        "instance_id": instance_id,
        "manifest_bytes": plan.manifest_bytes,
        "manifest_sha256": plan.manifest_sha256,
        "manifest_uri": plan.manifest_uri,
        "monitor": dataclasses.asdict(monitor) if monitor is not None else None,
        "phase": plan.phase,
        "reason": reason,
        "run_id": plan.run_id,
        "schema": "borsuk-v38-spot-failed-terminal-v1",
        "source_archive_bytes": plan.source_archive_bytes,
        "source_archive_sha256": plan.source_archive_sha256,
        "source_archive_uri": plan.source_archive_uri,
        "source_commit": plan.source_commit,
        "status": "failed",
    }
    raw = canonical_v38_terminal_bytes(terminal, plan, "failed")
    bucket, prefix = _s3_uri(plan.output_prefix, prefix=True)
    upload_once(bucket, prefix + "ATTEMPT_TERMINAL.json", raw)
    return raw


def run_v38_worker_invocation(
    invocation: V38WorkerInvocation,
    s3_client: Any,
    *,
    finalize_monitor: Callable[
        [V38MonitorSample | None], V38MonitorSample | None
    ]
    | None = None,
) -> bytes:
    """Adapt exact boot files and S3 calls to the pure worker boundary."""

    plan = _load_v38_spot_plan_bytes(invocation.plan.read_bytes())
    bucket, prefix = _s3_uri(plan.output_prefix, prefix=True)

    def upload_once(upload_bucket: str, key: str, body: bytes) -> None:
        s3_client.put_object(
            Bucket=upload_bucket,
            Key=key,
            Body=body,
            ExpectedBucketOwner=EXPECTED_AWS_ACCOUNT,
            IfNoneMatch="*",
            ChecksumAlgorithm="SHA256",
        )

    manifest_raw = invocation.manifest.read_bytes()
    binary_sha256 = hashlib.sha256()
    binary_bytes = 0
    try:
        with invocation.binary.open("rb") as binary_file:
            while chunk := binary_file.read(1_048_576):
                binary_bytes += len(chunk)
                binary_sha256.update(chunk)
        if (
            binary_bytes != plan.binary_bytes
            or binary_sha256.hexdigest() != plan.binary_sha256
        ):
            raise ValueError("V38 worker binary authority differs")
    except Exception as error:
        diagnostic = f"{type(error).__name__}: {error}\n".encode()[:65_536]
        try:
            upload_once(bucket, prefix + "WORKER_FAILURE.log", diagnostic)
        except Exception:
            pass
        publish_v38_worker_failure(
            plan,
            instance_id=invocation.instance_id,
            reason="authority-failure",
            upload_once=upload_once,
        )
        raise
    def download(bucket: str, key: str, path: pathlib.Path) -> None:
        response = s3_client.get_object(
            Bucket=bucket,
            Key=key,
            ExpectedBucketOwner=EXPECTED_AWS_ACCOUNT,
            ChecksumMode="ENABLED",
        )
        expected = response.get("ContentLength")
        if type(expected) is not int or expected <= 0:
            raise ValueError("V38 staged object length differs")
        written = 0
        with path.open("xb") as output:
            while chunk := response["Body"].read(1_048_576):
                output.write(chunk)
                written += len(chunk)
        if written != expected:
            raise ValueError("V38 staged object length differs")

    def execute(
        command: list[str],
        staged: V38StagedPhase,
        stdout_path: pathlib.Path,
        progress_path: pathlib.Path,
    ) -> tuple[int, V38MonitorSample]:
        return execute_v38_native_science(
            command, staged, stdout_path, progress_path
        )

    try:
        if finalize_monitor is None:
            wrapper_monitor = V38WrapperMonitor(
                plan.phase,
                _v38_systemd_control_group(_v38_slice_unit(plan.run_id)),
            )
            finalize_monitor = wrapper_monitor.finish
        return execute_v38_worker(
            plan,
            root=invocation.root,
            manifest_raw=manifest_raw,
            binary=invocation.binary,
            instance_id=invocation.instance_id,
            download=download,
            execute=execute,
            upload_once=upload_once,
            finalize_monitor=finalize_monitor,
        )
    except Exception as error:
        failure_monitor = getattr(error, "v38_monitor", None)
        preserved_stop = (
            classify_v38_monitor_sample(failure_monitor)
            if isinstance(failure_monitor, V38MonitorSample)
            else None
        )
        if finalize_monitor is not None and preserved_stop is not None:
            try:
                finalize_monitor(failure_monitor)
            except Exception:
                pass
        elif finalize_monitor is not None:
            try:
                failure_monitor = finalize_monitor(failure_monitor)
            except Exception as monitor_error:
                error = monitor_error
                failure_monitor = None
        stop = (
            classify_v38_monitor_sample(failure_monitor)
            if isinstance(failure_monitor, V38MonitorSample)
            else None
        )
        if stop is not None and stop not in str(error):
            resource_error = RuntimeError(f"V38 wrapper crossed {stop}")
            resource_error.v38_monitor = failure_monitor
            error = resource_error
        diagnostic = f"{type(error).__name__}: {error}\n".encode()[:65_536]
        bucket, prefix = _s3_uri(plan.output_prefix, prefix=True)
        try:
            upload_once(bucket, prefix + "WORKER_FAILURE.log", diagnostic)
        except Exception:
            pass
        publish_v38_worker_failure(
            plan,
            instance_id=invocation.instance_id,
            reason=_v38_worker_failure_reason(error),
            monitor=(
                failure_monitor
                if isinstance(failure_monitor, V38MonitorSample)
                else None
            ),
            upload_once=upload_once,
        )
        raise error


def build_v38_worker_script(plan: V38SpotPlan) -> str:
    """Return the bounded bootstrap for one immutable V38 phase."""

    build_v38_spot_plan(**dataclasses.asdict(plan))
    memory_limit = (
        BUILD_MEMORY_LIMIT_BYTES
        if plan.phase == "build-spill"
        else CEILING_MEMORY_LIMIT_BYTES
    )
    wrapper_runtime_limit = 720 if plan.phase == "build-spill" else 180
    output_bucket, output_key_prefix = _s3_uri(plan.output_prefix, prefix=True)
    plan_raw = json.dumps(
        dataclasses.asdict(plan), sort_keys=True, separators=(",", ":")
    ).encode()
    encoded_plan = base64.b64encode(plan_raw).decode()
    cleanup_paths = " ".join(
        f'"$phase_root/{kind}/{role}"'
        for kind, roles in (
            ("inputs", phase_input_roles(plan.phase)),
            ("outputs", phase_output_roles(plan.phase)),
        )
        for role in roles
    )
    return f"""#!/bin/bash
set -euo pipefail
umask 077
root=/var/lib/borsuk-v38
scratch="$root/scratch"
source_root="$root/source"
phase_root="$root/phase"
slice_unit={shlex.quote(_v38_slice_unit(plan.run_id))}
archive_path="$scratch/source.tar.zst"
binary_path="$scratch/v38-boundary-spill"
manifest_path="$scratch/manifest.json"
plan_path="$scratch/plan.json"
boot_log="$scratch/boot.log"
boot_failure_log="$scratch/boot-failure.log"
mkdir -p "$root"
if test "${{V38_IN_SLICE:-0}}" != 1; then
  systemctl start "$slice_unit"
  systemctl set-property --runtime "$slice_unit" MemoryMax={memory_limit} MemorySwapMax=0 MemoryAccounting=yes
  exec systemd-run --wait --collect --pipe \
    --unit=borsuk-v38-wrapper-{plan.run_id} --slice="$slice_unit" \
    --property=ProtectSystem=strict --property=PrivateTmp=true \
    --property=NoNewPrivileges=true --property=ReadWritePaths="$root" \
    --property=MemoryMax={memory_limit} --property=MemorySwapMax=0 \
    --property=RuntimeMaxSec={wrapper_runtime_limit} \
    env V38_IN_SLICE=1 /bin/bash "$0"
fi
dnf install -y python3.12
mkdir -p "$scratch" "$source_root"
printf '%s' {shlex.quote(encoded_plan)} | base64 -d >"$plan_path"
exec >"$boot_log" 2>&1
cleanup() {{
  status=$?
  trap - EXIT INT TERM
  if test "$status" -ne 0 && test -f "$boot_log"; then
    tail -c 65536 "$boot_log" >"$boot_failure_log"
    aws s3api put-object --bucket {shlex.quote(output_bucket)} \
      --key {shlex.quote(output_key_prefix + "BOOT_FAILURE.log")} \
      --body "$boot_failure_log" --if-none-match '*' \
      --checksum-algorithm SHA256 >/dev/null || true
  fi
  for path in {cleanup_paths} "$phase_root/local-result.json" "$phase_root/progress.log" "$phase_root/progress.json" "$archive_path" "$binary_path" "$manifest_path" "$plan_path" "$boot_log" "$boot_failure_log"; do
    test ! -e "$path" || unlink "$path"
  done
  rmdir "$phase_root/inputs" "$phase_root/outputs" "$phase_root" "$scratch" 2>/dev/null || true
  shutdown -h now
  exit "$status"
}}
trap cleanup EXIT INT TERM
aws s3 cp {shlex.quote(plan.source_archive_uri)} "$archive_path" --only-show-errors
aws s3 cp {shlex.quote(plan.binary_uri)} "$binary_path" --only-show-errors
aws s3 cp {shlex.quote(plan.manifest_uri)} "$manifest_path" --only-show-errors
test "$(stat -c %s "$archive_path")" = {plan.source_archive_bytes}
test "$(sha256sum "$archive_path" | cut -d' ' -f1)" = {plan.source_archive_sha256}
test "$(stat -c %s "$binary_path")" = {plan.binary_bytes}
test "$(sha256sum "$binary_path" | cut -d' ' -f1)" = {plan.binary_sha256}
test "$(stat -c %s "$manifest_path")" = {plan.manifest_bytes}
test "$(sha256sum "$manifest_path" | cut -d' ' -f1)" = {plan.manifest_sha256}
chmod 0555 "$binary_path"
tar --zstd -xf "$archive_path" -C "$source_root"
imds_token=$(curl --fail --silent --show-error --request PUT --header 'X-aws-ec2-metadata-token-ttl-seconds: 21600' http://169.254.169.254/latest/api/token)
instance_id=$(curl --fail --silent --show-error --header "X-aws-ec2-metadata-token: $imds_token" http://169.254.169.254/latest/meta-data/instance-id)
[[ "$instance_id" =~ ^i-[0-9a-f]{{8,17}}$ ]]
env PYTHONPATH="$source_root/.v38-python" python3.12 \\
  "$source_root/scripts/run_v38_boundary_spill_spot.py" \\
  --execute-v38-worker --root "$phase_root" --plan "$plan_path" \\
  --manifest "$manifest_path" --binary "$binary_path" --instance-id "$instance_id"
"""


def main(arguments: list[str] | None = None) -> int:
    """Run only the explicit authenticated remote-worker boundary."""

    try:
        invocation = parse_v38_worker_args(sys.argv if arguments is None else arguments)
        import boto3
        from botocore.config import Config

        s3_client = boto3.client(
            "s3",
            region_name=REGION,
            config=Config(
                connect_timeout=10,
                read_timeout=60,
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )
        run_v38_worker_invocation(invocation, s3_client)
    except Exception as error:
        print(f"V38 worker failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
