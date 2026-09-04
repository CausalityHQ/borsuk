"""Strict two-phase Spot orchestration for the V30 S3 qualification."""

from __future__ import annotations

import json
import shlex
from collections.abc import Callable
from dataclasses import dataclass


def _digest(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _s3(value: str) -> bool:
    return value.startswith("s3://") and value.count("/") >= 3 and not value.endswith("//")


@dataclass(frozen=True)
class SpotTarget:
    availability_zone: str
    subnet_id: str
    instance_type: str
    image_id: str
    security_group_id: str
    instance_profile_arn: str


@dataclass(frozen=True)
class V30ConstructionPlan:
    attempt_id: str
    source_commit: str
    source_archive_uri: str
    source_archive_sha256: str
    source_archive_bytes: int
    corpus_manifest_uri: str
    corpus_manifest_sha256: str
    corpus_manifest_bytes: int
    output_prefix: str
    expected_rows: int
    roots: int
    leaves: int
    routing_leaf_beam: int
    training_rows: int
    page_rows: int


@dataclass(frozen=True)
class V30EvaluationPlan:
    attempt_id: str
    source_commit: str
    source_archive_uri: str
    source_archive_sha256: str
    source_archive_bytes: int
    construction_manifest_uri: str
    construction_manifest_sha256: str
    construction_manifest_bytes: int
    query_uri: str
    query_sha256: str
    query_bytes: int
    truth_uri: str
    truth_sha256: str
    truth_bytes: int
    page_s3_prefix: str
    output_prefix: str
    source_rows: int
    query_start: int
    query_count: int
    leaf_beam: int
    page_count: int


@dataclass(frozen=True)
class V30Observation:
    state: str
    system_status: str
    instance_status: str
    rss_bytes: int
    psi_full_avg10: float
    swap_bytes: int
    progress: int
    terminal: bytes | None


def _validate_target(target: SpotTarget) -> None:
    if (
        not target.availability_zone.startswith("eu-central-1")
        or not target.subnet_id.startswith("subnet-")
        or not target.instance_type.startswith(("r7g.", "r8g."))
        or not target.image_id.startswith("ami-")
        or not target.security_group_id.startswith("sg-")
        or not target.instance_profile_arn
    ):
        raise ValueError("V30 Spot target differs")


def _validate_common(
    attempt_id: str,
    source_commit: str,
    source_uri: str,
    source_sha256: str,
    source_bytes: int,
    output_prefix: str,
) -> None:
    if (
        not attempt_id.startswith("v30-")
        or len(source_commit) != 40
        or not all(character in "0123456789abcdef" for character in source_commit)
        or not _s3(source_uri)
        or not _digest(source_sha256)
        or source_bytes <= 0
        or not _s3(output_prefix)
        or not output_prefix.endswith("/")
    ):
        raise ValueError("V30 Spot common authority differs")


def _validate_construction(plan: V30ConstructionPlan) -> None:
    _validate_common(
        plan.attempt_id,
        plan.source_commit,
        plan.source_archive_uri,
        plan.source_archive_sha256,
        plan.source_archive_bytes,
        plan.output_prefix,
    )
    geometry = (
        plan.expected_rows,
        plan.roots,
        plan.leaves,
        plan.training_rows,
        plan.page_rows,
    )
    if (
        not _s3(plan.corpus_manifest_uri)
        or not _digest(plan.corpus_manifest_sha256)
        or plan.corpus_manifest_bytes <= 0
        or geometry
        not in {
            (100_000, 16, 256, 8_192, 512),
            (100_000, 16, 256, 8_192, 128),
            (9_990_000, 1_024, 32_768, 262_144, 512),
        }
        or plan.routing_leaf_beam
        != {100_000: 192, 9_990_000: 512}[plan.expected_rows]
    ):
        raise ValueError("V30 construction authority differs")


def _validate_evaluation(plan: V30EvaluationPlan) -> None:
    _validate_common(
        plan.attempt_id,
        plan.source_commit,
        plan.source_archive_uri,
        plan.source_archive_sha256,
        plan.source_archive_bytes,
        plan.output_prefix,
    )
    artifacts = (
        (plan.construction_manifest_uri, plan.construction_manifest_sha256, plan.construction_manifest_bytes),
        (plan.query_uri, plan.query_sha256, plan.query_bytes),
        (plan.truth_uri, plan.truth_sha256, plan.truth_bytes),
    )
    expected_page_prefix = plan.construction_manifest_uri.removesuffix(
        "manifest.json"
    ) + "pages"
    if (
        any(not _s3(uri) or not _digest(digest) or length <= 0 for uri, digest, length in artifacts)
        or not plan.construction_manifest_uri.endswith("/manifest.json")
        or not _s3(plan.page_s3_prefix)
        or plan.page_s3_prefix.endswith("/")
        or plan.page_s3_prefix != expected_page_prefix
        or plan.source_rows not in {100_000, 9_990_000}
        or plan.query_start < 0
        or plan.query_count != 32
        or plan.leaf_beam != {100_000: 192, 9_990_000: 512}[plan.source_rows]
        or not 1 <= plan.page_count <= 16
    ):
        raise ValueError("V30 evaluation authority differs")


def _shell(words: list[str]) -> str:
    return " ".join(shlex.quote(word) for word in words)


def _split_s3(uri: str) -> tuple[str, str]:
    remainder = uri.removeprefix("s3://")
    bucket, separator, key = remainder.partition("/")
    if not separator or not bucket or not key:
        raise ValueError("V30 S3 authority differs")
    return bucket, key


def build_v30_corpus_manifest(source_bytes: bytes, *, expected_rows: int) -> bytes:
    """Derive a query-blind V30 prefix manifest from authenticated dataset inputs."""
    try:
        source = json.loads(source_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V30 source manifest JSON differs") from error
    if (
        type(source) is not dict
        or source.get("dataset_id") != "deep-image-96"
        or type(source.get("ordered_inputs")) is not list
        or type(expected_rows) is not int
        or expected_rows <= 0
    ):
        raise ValueError("V30 source manifest authority differs")
    selected: list[dict[str, object]] = []
    source_row = 0
    for item in source["ordered_inputs"]:
        if type(item) is not dict or item.get("authority_kind") != "training-shard":
            continue
        identity = item.get("identity")
        rows = item.get("rows")
        if (
            type(identity) is not dict
            or type(rows) is not int
            or rows <= 0
            or item.get("dimensions") != 96
            or item.get("ordinal_start") != source_row
            or item.get("ordinal_end") != source_row + rows
            or item.get("physical_schema")
            != "emb:fixed-size-list<element:f32;96>:non-null"
            or identity.get("digest_algorithm") != "sha256"
            or not _digest(identity.get("digest", ""))
            or type(identity.get("encoded_bytes")) is not int
            or identity["encoded_bytes"] <= 0
            or not _s3(identity.get("uri", ""))
        ):
            raise ValueError("V30 source training shard differs")
        take = min(rows, expected_rows - source_row)
        selected.append(
            {
                "encoded_bytes": identity["encoded_bytes"],
                "physical_row_count": rows,
                "row_count": take,
                "row_start": source_row,
                "sha256": identity["digest"],
                "uri": identity["uri"],
            }
        )
        source_row += take
        if source_row == expected_rows:
            break
        if take != rows:
            raise ValueError("V30 source prefix coverage differs")
    if source_row != expected_rows:
        raise ValueError("V30 source prefix coverage differs")
    return (
        json.dumps(
            {
                "dataset_id": "deep-image-96",
                "schema_version": 1,
                "shards": selected,
                "source_rows": expected_rows,
            },
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


def _monitored_command(command: str, *, rss_limit_bytes: int, wall_seconds: int) -> list[str]:
    if rss_limit_bytes <= 0 or wall_seconds <= 0 or "\n" in command:
        raise ValueError("V30 monitored command differs")
    return [
        f"rss_limit_bytes={rss_limit_bytes}",
        f"wall_seconds={wall_seconds}",
        "swap_limit_bytes=268435456",
        "swap_start_kib=$(awk '/^SwapTotal:/ {total=$2} /^SwapFree:/ {free=$2} END {print total-free}' /proc/meminfo)",
        f'setsid {command} >"$root/TERMINAL.json" 2>>"$root/worker.log" &',
        "child=$!",
        "started=$(date +%s)",
        "stop_reason=",
        "while kill -0 \"$child\" 2>/dev/null; do",
        "  rss_bytes=$(ps -eo pgid=,rss= | awk -v group=\"$child\" '$1 == group {total += $2} END {printf \"%.0f\", total * 1024}')",
        "  psi_full_avg10=$(awk '/^full / {for (i=1;i<=NF;i++) if ($i ~ /^avg10=/) {split($i,a,\"=\"); print a[2]}}' /proc/pressure/memory)",
        "  swap_now_kib=$(awk '/^SwapTotal:/ {total=$2} /^SwapFree:/ {free=$2} END {print total-free}' /proc/meminfo)",
        "  swap_bytes=$(( (swap_now_kib - swap_start_kib) * 1024 ))",
        "  (( swap_bytes < 0 )) && swap_bytes=0",
        "  progress=$(( $(date +%s) - started ))",
        "  printf '{\"progress\":%d,\"psi_full_avg10\":%s,\"rss_bytes\":%d,\"state\":\"running\",\"swap_bytes\":%d}\\n' \"$progress\" \"$psi_full_avg10\" \"$rss_bytes\" \"$swap_bytes\" >\"$root/HEARTBEAT.json\"",
        "  aws s3api put-object --bucket \"$output_bucket\" --key \"${output_key}HEARTBEAT.json\" --body \"$root/HEARTBEAT.json\" --checksum-algorithm SHA256 >/dev/null",
        "  if (( rss_bytes > rss_limit_bytes || swap_bytes > swap_limit_bytes || progress > wall_seconds )) || awk -v pressure=\"$psi_full_avg10\" 'BEGIN {exit !(pressure > 0.50)}'; then",
        "    stop_reason=resource-stop",
        "    kill -TERM -- \"-$child\" 2>/dev/null || true",
        "    break",
        "  fi",
        "  sleep 30",
        "done",
        "set +e",
        "wait \"$child\"",
        "child_status=$?",
        "set -e",
        "[[ -z \"$stop_reason\" ]] || exit 75",
        "(( child_status == 0 )) || exit \"$child_status\"",
    ]


def _construction_script(plan: V30ConstructionPlan) -> str:
    output_bucket, output_key = _split_s3(plan.output_prefix)
    command = _shell(
        [
            "/opt/borsuk/v30_s3_build",
            "--execute",
            "--corpus-manifest-s3",
            plan.corpus_manifest_uri,
            "--s3-region",
            "eu-central-1",
            "--corpus-manifest-sha256",
            plan.corpus_manifest_sha256,
            "--corpus-manifest-bytes",
            str(plan.corpus_manifest_bytes),
            "--source-commit",
            plan.source_commit,
            "--expected-rows",
            str(plan.expected_rows),
            "--roots",
            str(plan.roots),
            "--leaves",
            str(plan.leaves),
            "--routing-leaf-beam",
            str(plan.routing_leaf_beam),
            "--training-rows",
            str(plan.training_rows),
            "--page-rows",
            str(plan.page_rows),
            "--output-s3-prefix",
            plan.output_prefix,
            "--scratch-dir",
            "/data/v30-build",
        ]
    )
    return "\n".join(
        [
            "#!/bin/bash",
            "set -Eeuo pipefail",
            "umask 077",
            "export AWS_REGION=eu-central-1",
            "ulimit -c 0",
            "shutdown --poweroff +240",
            "root=/run/v30",
            'source_dir="$root/source"',
            'archive="$root/source.tar.zst"',
            'mkdir -p "$root" "$source_dir"',
            'exec >"$root/worker.log" 2>&1',
            f"output_bucket={shlex.quote(output_bucket)}",
            f"output_key={shlex.quote(output_key)}",
            "terminal=failed",
            "put_once() { aws s3api put-object --bucket \"$output_bucket\" --key \"$output_key$2\" --body \"$1\" --if-none-match '*' --checksum-algorithm SHA256 >/dev/null; }",
            "finish() { status=$?; trap - EXIT; set +e; if [[ \"$terminal\" != complete ]]; then printf '{\"claim_eligible\":false,\"status\":\"failed\",\"worker_status\":%d}\\n' \"$status\" >\"$root/FAILED.json\"; put_once \"$root/worker.log\" worker.log || true; put_once \"$root/FAILED.json\" FAILED.json || true; fi; shutdown -h now; }",
            "trap finish EXIT",
            "dnf install -y gcc gcc-c++ tar zstd",
            "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable",
            f"aws s3 cp --only-show-errors {shlex.quote(plan.source_archive_uri)} \"$archive\"",
            f'test "$(stat -c %s "$archive")" -eq {plan.source_archive_bytes}',
            f"printf '%s  %s\\n' {plan.source_archive_sha256} \"$archive\" | sha256sum --check --status",
            'tar --zstd -xf "$archive" -C "$source_dir"',
            'cd "$source_dir"',
            f'test "$(cat .borsuk-source-commit)" = {plan.source_commit}',
            "/root/.cargo/bin/cargo build --release --locked -p borsuk --example v30_s3_build",
            "install -D -m 0555 target/release/examples/v30_s3_build /opt/borsuk/v30_s3_build",
            *_monitored_command(command, rss_limit_bytes=192 * 1024**3, wall_seconds=14_400),
            'put_once "$root/worker.log" worker.log',
            'put_once "$root/TERMINAL.json" TERMINAL.json',
            "terminal=complete",
        ]
    )


def _evaluation_script(plan: V30EvaluationPlan) -> str:
    manifest_prefix = plan.construction_manifest_uri.removesuffix("manifest.json")
    command = _shell(
        [
            "python3",
            "scripts/run_v30_untouched_quality.py",
            "--execute",
            "--qualifier",
            "/opt/borsuk/v30_s3_qualify",
            "--manifest",
            "/run/v30/manifest.json",
            "--manifest-sha256",
            plan.construction_manifest_sha256,
            "--manifest-bytes",
            str(plan.construction_manifest_bytes),
            "--artifact-dir",
            "/run/v30/resident",
            "--query-parquet",
            "/run/v30/test.parquet",
            "--query-sha256",
            plan.query_sha256,
            "--query-bytes",
            str(plan.query_bytes),
            "--truth-parquet",
            "/run/v30/neighbors.parquet",
            "--truth-sha256",
            plan.truth_sha256,
            "--truth-bytes",
            str(plan.truth_bytes),
            "--s3-page-prefix",
            plan.page_s3_prefix,
            "--source-rows",
            str(plan.source_rows),
            "--query-start",
            str(plan.query_start),
            "--query-count",
            str(plan.query_count),
            "--leaf-beam",
            str(plan.leaf_beam),
            "--page-count",
            str(plan.page_count),
        ]
    )
    output_bucket, output_key = _split_s3(plan.output_prefix)
    return "\n".join(
        [
            "#!/bin/bash",
            "set -Eeuo pipefail",
            "umask 077",
            "export AWS_REGION=eu-central-1",
            "ulimit -c 0",
            "shutdown --poweroff +120",
            "root=/run/v30",
            'source_dir="$root/source"',
            'archive="$root/source.tar.zst"',
            'mkdir -p "$root/resident" "$source_dir"',
            'exec >"$root/worker.log" 2>&1',
            f"output_bucket={shlex.quote(output_bucket)}",
            f"output_key={shlex.quote(output_key)}",
            "terminal=failed",
            "put_once() { aws s3api put-object --bucket \"$output_bucket\" --key \"$output_key$2\" --body \"$1\" --if-none-match '*' --checksum-algorithm SHA256 >/dev/null; }",
            "finish() { status=$?; trap - EXIT; set +e; if [[ \"$terminal\" != complete ]]; then printf '{\"claim_eligible\":false,\"status\":\"failed\",\"worker_status\":%d}\\n' \"$status\" >\"$root/FAILED.json\"; put_once \"$root/worker.log\" worker.log || true; put_once \"$root/FAILED.json\" FAILED.json || true; fi; shutdown -h now; }",
            "trap finish EXIT",
            "dnf install -y gcc gcc-c++ python3-pip tar zstd",
            "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable",
            f"aws s3 cp --only-show-errors {shlex.quote(plan.source_archive_uri)} \"$archive\"",
            f'test "$(stat -c %s "$archive")" -eq {plan.source_archive_bytes}',
            f"printf '%s  %s\\n' {plan.source_archive_sha256} \"$archive\" | sha256sum --check --status",
            'tar --zstd -xf "$archive" -C "$source_dir"',
            'cd "$source_dir"',
            f'test "$(cat .borsuk-source-commit)" = {plan.source_commit}',
            "/root/.cargo/bin/cargo build --release --locked -p borsuk --example v30_s3_qualify",
            "install -D -m 0555 target/release/examples/v30_s3_qualify /opt/borsuk/v30_s3_qualify",
            "curl -LsSf https://astral.sh/uv/0.8.17/install.sh | sh",
            "/root/.local/bin/uv venv --python 3.12 /opt/borsuk/venv",
            "/root/.local/bin/uv pip install --python /opt/borsuk/venv/bin/python --requirement scripts/requirements-format-bench.txt",
            f"aws s3 cp --only-show-errors {shlex.quote(plan.construction_manifest_uri)} \"$root/manifest.json\"",
            f"aws s3 cp --only-show-errors {shlex.quote(plan.query_uri)} \"$root/test.parquet\"",
            f"aws s3 cp --only-show-errors {shlex.quote(plan.truth_uri)} \"$root/neighbors.parquet\"",
            f'test "$(stat -c %s "$root/manifest.json")" -eq {plan.construction_manifest_bytes}',
            f"printf '%s  %s\\n' {plan.construction_manifest_sha256} \"$root/manifest.json\" | sha256sum --check --status",
            f'test "$(stat -c %s "$root/test.parquet")" -eq {plan.query_bytes}',
            f"printf '%s  %s\\n' {plan.query_sha256} \"$root/test.parquet\" | sha256sum --check --status",
            f'test "$(stat -c %s "$root/neighbors.parquet")" -eq {plan.truth_bytes}',
            f"printf '%s  %s\\n' {plan.truth_sha256} \"$root/neighbors.parquet\" | sha256sum --check --status",
            "python3 - <<'PY' >\"$root/resident.tsv\"",
            "import json",
            "from pathlib import Path",
            "value=json.loads(Path('/run/v30/manifest.json').read_bytes())",
            "items=[value['hierarchy']['roots'],value['hierarchy']['leaves'],value['layout']['leaf_ranges'],value['layout']['page_ranges'],*value['pq']['artifacts']]",
            "for item in items: print(item['file'], item['sha256'], item['encoded_bytes'], sep='\\t')",
            "PY",
            "while IFS=$'\\t' read -r file sha size; do",
            "  [[ \"$file\" != */* && \"$file\" != .* && \"$sha\" =~ ^[0-9a-f]{64}$ && \"$size\" =~ ^[1-9][0-9]*$ ]]",
            f"  aws s3 cp --only-show-errors {shlex.quote(manifest_prefix)}\"$file\" \"$root/resident/$file\"",
            "  test \"$(stat -c %s \"$root/resident/$file\")\" -eq \"$size\"",
            "  printf '%s  %s\\n' \"$sha\" \"$root/resident/$file\" | sha256sum --check --status",
            "done <\"$root/resident.tsv\"",
            *_monitored_command(
                command.replace("python3 ", "/opt/borsuk/venv/bin/python ", 1),
                rss_limit_bytes=3 * 1024**3,
                wall_seconds=7_200,
            ),
            'put_once "$root/worker.log" worker.log',
            'put_once "$root/TERMINAL.json" TERMINAL.json',
            "terminal=complete",
        ]
    )


def _spec(target: SpotTarget, user_data: str) -> dict[str, object]:
    _validate_target(target)
    return {
        "ImageId": target.image_id,
        "InstanceType": target.instance_type,
        "MinCount": 1,
        "MaxCount": 1,
        "SubnetId": target.subnet_id,
        "SecurityGroupIds": [target.security_group_id],
        "IamInstanceProfile": {"Arn": target.instance_profile_arn},
        "Placement": {"AvailabilityZone": target.availability_zone},
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "Encrypted": True,
                    "VolumeSize": 200,
                    "VolumeType": "gp3",
                },
            }
        ],
        "InstanceInitiatedShutdownBehavior": "terminate",
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate",
            },
        },
        "UserData": user_data,
    }


def build_v30_construction_spot_specs(
    plan: V30ConstructionPlan, targets: tuple[SpotTarget, ...]
) -> tuple[dict[str, object], ...]:
    _validate_construction(plan)
    if not targets:
        raise ValueError("V30 Spot target inventory differs")
    script = _construction_script(plan)
    return tuple(_spec(target, script) for target in targets)


def build_v30_evaluation_spot_specs(
    plan: V30EvaluationPlan, targets: tuple[SpotTarget, ...]
) -> tuple[dict[str, object], ...]:
    _validate_evaluation(plan)
    if not targets:
        raise ValueError("V30 Spot target inventory differs")
    script = _evaluation_script(plan)
    return tuple(_spec(target, script) for target in targets)


def _optional_s3_bytes(client: object, bucket: str, key: str) -> bytes | None:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except Exception as error:
        response = getattr(error, "response", {})
        code = response.get("Error", {}).get("Code") if type(response) is dict else None
        if isinstance(error, KeyError) or code in {"NoSuchKey", "404"}:
            return None
        raise
    body = response.get("Body") if type(response) is dict else None
    value = body.read() if body is not None else None
    if type(value) is not bytes:
        raise ValueError("V30 S3 object body differs")
    return value


def execute_v30_spot_phase(
    *,
    plan: V30ConstructionPlan | V30EvaluationPlan,
    targets: tuple[SpotTarget, ...],
    ec2_client: object,
    s3_client: object,
    sleep: Callable[[int], None],
    wall_observations: int,
) -> bytes:
    """Launch and monitor exactly one construction or evaluation Spot phase."""
    if isinstance(plan, V30ConstructionPlan):
        specs = build_v30_construction_spot_specs(plan, targets)
        rss_limit_bytes = 192 * 1024**3
    elif isinstance(plan, V30EvaluationPlan):
        specs = build_v30_evaluation_spot_specs(plan, targets)
        rss_limit_bytes = 3 * 1024**3
    else:
        raise TypeError("V30 Spot plan type differs")
    bucket_name, prefix = _split_s3(plan.output_prefix)
    polls = 0

    def launch(spec: dict[str, object]) -> str:
        try:
            response = ec2_client.run_instances(**spec)
        except Exception as error:
            detail = getattr(error, "response", {})
            code = detail.get("Error", {}).get("Code") if type(detail) is dict else None
            if code == "InsufficientInstanceCapacity":
                raise RuntimeError("InsufficientInstanceCapacity") from error
            raise
        instances = response.get("Instances") if type(response) is dict else None
        if type(instances) is not list or len(instances) != 1:
            raise ValueError("V30 Spot launch receipt differs")
        instance_id = instances[0].get("InstanceId")
        if type(instance_id) is not str:
            raise ValueError("V30 Spot instance receipt differs")
        return instance_id

    def observe(instance_id: str) -> V30Observation:
        nonlocal polls
        polls += 1
        reservations = ec2_client.describe_instances(InstanceIds=[instance_id]).get(
            "Reservations", []
        )
        try:
            state = reservations[0]["Instances"][0]["State"]["Name"]
        except (IndexError, KeyError, TypeError) as error:
            raise ValueError("V30 Spot instance state differs") from error
        statuses = ec2_client.describe_instance_status(
            InstanceIds=[instance_id], IncludeAllInstances=True
        ).get("InstanceStatuses", [])
        status = statuses[0] if statuses else {}
        system_status = status.get("SystemStatus", {}).get("Status", "initializing")
        instance_status = status.get("InstanceStatus", {}).get("Status", "initializing")
        terminal = _optional_s3_bytes(s3_client, bucket_name, prefix + "TERMINAL.json")
        if terminal is None:
            terminal = _optional_s3_bytes(s3_client, bucket_name, prefix + "FAILED.json")
        heartbeat = (
            None
            if terminal is not None
            else _optional_s3_bytes(s3_client, bucket_name, prefix + "HEARTBEAT.json")
        )
        if heartbeat is None:
            rss_bytes = 0
            psi = 0.0
            swap_bytes = 0
            progress = 0
        else:
            try:
                value = json.loads(heartbeat)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValueError("V30 heartbeat JSON differs") from error
            if (
                type(value) is not dict
                or set(value)
                != {"progress", "psi_full_avg10", "rss_bytes", "state", "swap_bytes"}
                or value["state"] != "running"
                or type(value["progress"]) is not int
                or type(value["rss_bytes"]) is not int
                or type(value["swap_bytes"]) is not int
                or type(value["psi_full_avg10"]) not in {int, float}
            ):
                raise ValueError("V30 heartbeat schema differs")
            rss_bytes = value["rss_bytes"]
            psi = float(value["psi_full_avg10"])
            swap_bytes = value["swap_bytes"]
            progress = value["progress"]
        return V30Observation(
            state,
            system_status,
            instance_status,
            rss_bytes,
            psi,
            swap_bytes,
            progress,
            terminal,
        )

    return monitor_v30_original_attempt(
        launch=launch,
        specs=specs,
        observe=observe,
        terminate=lambda instance_id: ec2_client.terminate_instances(
            InstanceIds=[instance_id]
        ),
        sleep=sleep,
        wall_observations=wall_observations,
        rss_limit_bytes=rss_limit_bytes,
    )


def monitor_v30_original_attempt(
    *,
    launch: Callable[[dict[str, object]], str],
    specs: tuple[dict[str, object], ...],
    observe: Callable[[str], V30Observation],
    terminate: Callable[[str], None],
    sleep: Callable[[int], None],
    wall_observations: int,
    rss_limit_bytes: int,
) -> bytes:
    """Monitor exactly one launched instance and terminate it on every outcome."""
    if not specs or wall_observations <= 0 or rss_limit_bytes <= 0:
        raise ValueError("V30 monitor authority differs")
    instance_id = ""
    for spec in specs:
        try:
            instance_id = launch(spec)
        except RuntimeError as error:
            if str(error) == "InsufficientInstanceCapacity":
                continue
            raise
        break
    if not instance_id:
        raise RuntimeError("V30 Spot capacity is unavailable")
    if not instance_id.startswith("i-"):
        raise ValueError("V30 Spot instance identity differs")
    last_progress = -1
    stagnant = 0
    try:
        for _ in range(wall_observations):
            observation = observe(instance_id)
            if observation.terminal is not None:
                try:
                    value = json.loads(observation.terminal)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ValueError("V30 terminal JSON differs") from error
                if observation.terminal != json.dumps(
                    value, sort_keys=True, separators=(",", ":")
                ).encode() + b"\n":
                    raise ValueError("V30 terminal canonical bytes differ")
                if (
                    type(value) is not dict
                    or value.get("status") != "passed"
                    or value.get("claim_eligible") is not False
                ):
                    raise RuntimeError("V30 Spot worker failed")
                return observation.terminal
            allowed_health = {"ok", "initializing", "not-applicable"}
            if (
                observation.system_status not in allowed_health
                or observation.instance_status not in allowed_health
            ):
                raise RuntimeError("V30 Spot health differs")
            if (
                observation.rss_bytes < 0
                or observation.rss_bytes > rss_limit_bytes
                or not 0.0 <= observation.psi_full_avg10 <= 0.5
                or observation.swap_bytes < 0
                or observation.swap_bytes > 256 * 1024**2
                or observation.progress < last_progress
            ):
                raise RuntimeError("V30 Spot resource stop")
            if observation.progress == last_progress:
                stagnant += 1
            else:
                stagnant = 0
            if stagnant >= 20:
                raise RuntimeError("V30 Spot progress stop")
            last_progress = observation.progress
            if observation.state not in {"pending", "running"}:
                raise RuntimeError("V30 Spot instance stopped without terminal")
            sleep(30)
        raise TimeoutError("V30 Spot attempt exceeded wall stop")
    finally:
        terminate(instance_id)
