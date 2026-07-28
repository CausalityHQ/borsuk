#!/usr/bin/env bash
# Package the exact local source state, start the dedicated EC2 worker, and
# launch stage-one format qualification in a detached remote tmux session.
set -euo pipefail

PROFILE="${AWS_PROFILE:-causality}"
REGION="${AWS_REGION:-eu-central-1}"
INSTANCE_ID="${BORSUK_BENCH_INSTANCE_ID:-i-0e73bacb470807838}"
BUCKET="${BORSUK_FORMAT_BUCKET:-borsuk-bench-453182569524-euc1}"
EXPECTED_ACCOUNT="${BORSUK_AWS_ACCOUNT:-453182569524}"
RUN_ID="${BORSUK_FORMAT_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
CAMPAIGN="${BORSUK_FORMAT_CAMPAIGN:-qualification}"
CAMPAIGN_ENV=""
RESULT_PREFIX=""
case "$CAMPAIGN" in
  qualification)
    ENTRYPOINT="scripts/bench_format_qualification_aws.sh"
    RESULT_NAMESPACE="format-qualification"
    CHECKPOINT="FORMAT_DECISION_REQUIRED"
    ;;
  tuning)
    ENTRYPOINT="scripts/bench_format_tuning_aws.sh"
    RESULT_NAMESPACE="format-tuning"
    CHECKPOINT="FORMAT_TUNING_COMPLETE"
    ;;
  range-cap)
    ENTRYPOINT="scripts/bench_format_tuning_aws.sh"
    RESULT_NAMESPACE="format-range-cap"
    CHECKPOINT="FORMAT_TUNING_COMPLETE"
    CAMPAIGN_ENV="BORSUK_FORMAT_TUNING_SCOPE=range-cap"
    ;;
  storage-layout)
    ENTRYPOINT="scripts/bench_storage_layout_qualification_aws.sh"
    RESULT_NAMESPACE="layout-qualification"
    CHECKPOINT="LAYOUT_QUALIFICATION_COMPLETE"
    CAMPAIGN_ENV="BORSUK_LAYOUT_EXECUTE=1 BORSUK_RUN_LAYOUT_QUALIFICATION=1 BORSUK_LAYOUT_RUN_ID=$RUN_ID BORSUK_LAYOUT_BUCKET=$BUCKET BORSUK_LAYOUT_DATASETS=/home/ec2-user/borsuk-datasets"
    ;;
  wal-layout)
    ENTRYPOINT="scripts/bench_wal_layout_qualification_aws.sh"
    RESULT_NAMESPACE="layout-qualification/wal-results"
    RESULT_PREFIX="$RESULT_NAMESPACE/$RUN_ID"
    CHECKPOINT="WAL_LAYOUT_QUALIFICATION_COMPLETE"
    CAMPAIGN_ENV="BORSUK_WAL_LAYOUT_EXECUTE=1 BORSUK_RUN_WAL_LAYOUT_QUALIFICATION=1 BORSUK_WAL_LAYOUT_BUCKET=$BUCKET BORSUK_WAL_LAYOUT_DATASETS=/home/ec2-user/borsuk-datasets"
    ;;
  *)
    echo "BORSUK_FORMAT_CAMPAIGN must be qualification, tuning, range-cap, storage-layout, or wal-layout" >&2
    exit 2
    ;;
esac
if [[ -z "$RESULT_PREFIX" ]]; then
  RESULT_PREFIX="$RESULT_NAMESPACE/results/$RUN_ID"
fi
SESSION="borsuk-format-$CAMPAIGN-$RUN_ID"

cd "$(dirname "$0")/.."

account="$(aws --profile "$PROFILE" sts get-caller-identity --query Account --output text)"
if [[ "$account" != "$EXPECTED_ACCOUNT" ]]; then
  echo "AWS account mismatch: got $account, expected $EXPECTED_ACCOUNT" >&2
  exit 2
fi

temp_root="$(mktemp -d "${TMPDIR:-/tmp}/borsuk-format-launch.XXXXXX")"
if [[ -n "${BORSUK_FORMAT_SOURCE_ARCHIVE:-}" ]]; then
  [[ -f "$BORSUK_FORMAT_SOURCE_ARCHIVE" ]] || {
    echo "BORSUK_FORMAT_SOURCE_ARCHIVE is not a file" >&2
    exit 2
  }
  archive="$(
    cd "$(dirname "$BORSUK_FORMAT_SOURCE_ARCHIVE")"
    printf '%s/%s\n' "$PWD" "$(basename "$BORSUK_FORMAT_SOURCE_ARCHIVE")"
  )"
else
  archive="$temp_root/borsuk-source.tar.gz"
  tar_xattr_args=()
  if tar --no-xattrs -cf /dev/null --files-from /dev/null 2>/dev/null; then
    tar_xattr_args+=(--no-xattrs)
  fi

  git ls-files -z --cached --others --exclude-standard -- \
      Cargo.toml Cargo.lock crates scripts \
      docs/research/storage-layout-qualification-protocol.json \
      docs/research/wal-layout-qualification-protocol.json \
    | while IFS= read -r -d '' path; do
        case "$path" in
          */.borsuk-scratch/*|*/target/*|*/.venv*/*) continue ;;
        esac
        [[ -f "$path" ]] && printf '%s\0' "$path"
      done \
    | COPYFILE_DISABLE=1 tar "${tar_xattr_args[@]}" --null -czf "$archive" -T -
fi
source_sha256="$(shasum -a 256 "$archive" | awk '{print $1}')"
source_key="format-qualification/source/$source_sha256.tar.gz"
aws --profile "$PROFILE" --region "$REGION" s3 cp \
  "$archive" "s3://$BUCKET/$source_key" --only-show-errors

state="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].State.Name' \
  --output text)"
instance_type="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].InstanceType' \
  --output text)"
ami_id="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].ImageId' \
  --output text)"
volume_id="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].BlockDeviceMappings[0].Ebs.VolumeId' \
  --output text)"
disk_class="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-volumes \
  --volume-ids "$volume_id" \
  --query 'Volumes[0].join(`-`, [VolumeType, to_string(Size), to_string(Iops), to_string(Throughput)])' \
  --output text)"
local_disk_contract="ebs-$disk_class"
if [[ "$CAMPAIGN" == "wal-layout" ]]; then
  IFS=- read -r volume_type _volume_size volume_iops volume_throughput <<< "$disk_class"
  local_disk_contract="${volume_type}-${volume_iops}iops-${volume_throughput}MBps"
fi
if [[ "$state" == "stopped" ]]; then
  aws --profile "$PROFILE" --region "$REGION" ec2 start-instances \
    --instance-ids "$INSTANCE_ID" >/dev/null
fi
aws --profile "$PROFILE" --region "$REGION" ec2 wait instance-running \
  --instance-ids "$INSTANCE_ID"
aws --profile "$PROFILE" --region "$REGION" ec2 wait instance-status-ok \
  --instance-ids "$INSTANCE_ID"

ssm_online=0
for _ in $(seq 1 60); do
  ping_status="$(aws --profile "$PROFILE" --region "$REGION" ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
    --query 'InstanceInformationList[0].PingStatus' \
    --output text 2>/dev/null || true)"
  if [[ "$ping_status" == "Online" ]]; then
    ssm_online=1
    break
  fi
  sleep 5
done
if [[ "$ssm_online" != "1" ]]; then
  echo "SSM did not become online for $INSTANCE_ID" >&2
  exit 3
fi

workspace="/home/ec2-user/borsuk-format-source-$source_sha256"
remote_script="$(printf '%s\n' \
  'set -euo pipefail' \
  "workspace='$workspace'" \
  "source_uri='s3://$BUCKET/$source_key'" \
  "expected_sha256='$source_sha256'" \
  "remote_archive='/tmp/borsuk-format-source-$source_sha256.tar.gz'" \
  "session='$SESSION'" \
  'if [[ ! -f "$workspace/source.ready" ]]; then' \
  '  rm -rf "$workspace"' \
  '  mkdir -p "$workspace"' \
  '  aws s3 cp "$source_uri" "$remote_archive" --only-show-errors' \
  '  remote_sha256="$(sha256sum "$remote_archive" | awk '"'"'{print $1}'"'"')"' \
  '  [[ "$remote_sha256" == "$expected_sha256" ]]' \
  '  tar -xzf "$remote_archive" -C "$workspace"' \
  '  printf "%s\n" "$expected_sha256" > "$workspace/source.ready"' \
  'fi' \
  '[[ "$(cat "$workspace/source.ready")" == "$expected_sha256" ]]' \
  'chown -R ec2-user:ec2-user "$workspace"' \
  'if sudo -u ec2-user -H tmux has-session -t "$session" 2>/dev/null; then' \
  '  echo "tmux session already running: $session"' \
  '  exit 0' \
  'fi' \
  "sudo -u ec2-user -H tmux new-session -d -s '$SESSION' \"bash -lc 'cd \\\"$workspace\\\" && env AWS_REGION=\\\"$REGION\\\" AWS_DEFAULT_REGION=\\\"$REGION\\\" BORSUK_S3_BUCKET=\\\"$BUCKET\\\" BORSUK_FORMAT_RUN_ID=\\\"$RUN_ID\\\" BORSUK_SOURCE_SHA256=\\\"$source_sha256\\\" BORSUK_INSTANCE_ID=\\\"$INSTANCE_ID\\\" BORSUK_INSTANCE_TYPE=\\\"$instance_type\\\" BORSUK_AMI_ID=\\\"$ami_id\\\" BORSUK_LOCAL_DISK_CLASS=\\\"$local_disk_contract\\\" $CAMPAIGN_ENV bash \\\"$ENTRYPOINT\\\" > \\\"$workspace/launcher.log\\\" 2>&1'\"" \
  "echo 'started tmux session $SESSION'" \
  "echo 'workspace $workspace'" \
  "echo 'results s3://$BUCKET/$RESULT_PREFIX'")"
encoded_remote="$(printf '%s' "$remote_script" | base64 | tr -d '\n')"
command_id="$(aws --profile "$PROFILE" --region "$REGION" ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name AWS-RunShellScript \
  --comment "BORSUK format $CAMPAIGN $RUN_ID" \
  --parameters "commands=echo $encoded_remote | base64 -d | bash" \
  --query 'Command.CommandId' \
  --output text)"
aws --profile "$PROFILE" --region "$REGION" ssm wait command-executed \
  --command-id "$command_id" \
  --instance-id "$INSTANCE_ID"
aws --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
  --command-id "$command_id" \
  --instance-id "$INSTANCE_ID" \
  --query '{Status:Status,Output:StandardOutputContent,Error:StandardErrorContent}' \
  --output json

printf '%s\n' \
  "run_id=$RUN_ID" \
  "session=$SESSION" \
  "source_sha256=$source_sha256" \
  "campaign=$CAMPAIGN" \
  "instance_type=$instance_type" \
  "local_disk_class=$local_disk_contract" \
  "results=s3://$BUCKET/$RESULT_PREFIX" \
  "checkpoint=s3://$BUCKET/$RESULT_PREFIX/$CHECKPOINT"
