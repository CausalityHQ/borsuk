#!/usr/bin/env bash
# Content-addressed detached launcher for the frozen publication-v2 runner.
set -euo pipefail

cd "$(dirname "$0")/.."
PROFILE="${AWS_PROFILE:-causality}"
REGION="${AWS_REGION:-eu-central-1}"
INSTANCE_ID="${BORSUK_BENCH_INSTANCE_ID:-i-0e73bacb470807838}"
BUCKET="${BORSUK_PUBLICATION_BUCKET:-borsuk-bench-453182569524-euc1}"
EXPECTED_ACCOUNT="${BORSUK_AWS_ACCOUNT:-453182569524}"
MANIFEST="docs/research/publication-v2-manifest.json"
SSM_CHUNK_BYTES="${BORSUK_SSM_CHUNK_BYTES:-12000}"
SSM_MAX_IN_FLIGHT="${BORSUK_SSM_MAX_IN_FLIGHT:-8}"
[[ "$SSM_MAX_IN_FLIGHT" =~ ^[1-9][0-9]*$ ]] &&
  ((SSM_MAX_IN_FLIGHT <= 16)) || {
  echo "BORSUK_SSM_MAX_IN_FLIGHT must be an integer from 1 through 16" >&2
  exit 2
}

python3 scripts/publication_protocol.py validate "$MANIFEST"
manifest_field() {
  python3 - "$MANIFEST" "$1" <<'PY'
import json
from pathlib import Path
import sys

value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))[sys.argv[2]]
if not isinstance(value, str) or not value or "\n" in value:
    raise SystemExit(f"invalid manifest string field: {sys.argv[2]}")
print(value)
PY
}
campaign_id="$(manifest_field campaign_id)"
result_prefix="$(manifest_field result_prefix)"
index_prefix="$(manifest_field index_prefix)"
RUN_ID="${BORSUK_PUBLICATION_V2_RUN_ID:-$campaign_id}"
[[ "$RUN_ID" == "$campaign_id" ]] || {
  echo "campaign id mismatch: launcher=$RUN_ID manifest=$campaign_id" >&2
  exit 3
}
SESSION="borsuk-publication-v2-$RUN_ID"
account="$(aws --profile "$PROFILE" --region "$REGION" sts get-caller-identity \
  --query Account --output text)"
[[ "$account" == "$EXPECTED_ACCOUNT" ]] || {
  echo "AWS account mismatch: got $account, expected $EXPECTED_ACCOUNT" >&2
  exit 2
}

launch_temp="$(mktemp -d "${TMPDIR:-/tmp}/borsuk-publication-v2.XXXXXX")"
archive="$launch_temp/borsuk-source.tar.gz"
tar_xattr_args=()
if tar --no-xattrs -cf /dev/null --files-from /dev/null 2>/dev/null; then
  tar_xattr_args+=(--no-xattrs)
fi
git ls-files -z --cached --others --exclude-standard -- \
  Cargo.toml Cargo.lock crates scripts docs/research |
while IFS= read -r -d '' source_item; do
  case "$source_item" in
    */target/*|*/.venv/*|*/.borsuk-scratch/*) continue ;;
  esac
  [[ -f "$source_item" ]] && printf '%s\0' "$source_item"
done | COPYFILE_DISABLE=1 tar "${tar_xattr_args[@]}" --null -czf "$archive" -T -
source_sha256="$(shasum -a 256 "$archive" | awk '{print $1}')"
manifest_sha256="$(shasum -a 256 "$MANIFEST" | awk '{print $1}')"
source_key="publication/v2/source/$source_sha256.tar.gz"
manifest_key="publication/v2/manifests/$manifest_sha256.json"
source_uri="s3://$BUCKET/$source_key"
manifest_uri="s3://$BUCKET/$manifest_key"

state="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].State.Name' --output text)"
instance_type="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].InstanceType' --output text)"
instance_profile_arn="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].IamInstanceProfile.Arn' --output text)"
instance_profile_name="${instance_profile_arn##*/}"
role_arn="$(aws --profile "$PROFILE" iam get-instance-profile \
  --instance-profile-name "$instance_profile_name" \
  --query 'InstanceProfile.Roles[0].Arn' --output text)"
volume_id="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].BlockDeviceMappings[0].Ebs.VolumeId' \
  --output text)"
local_disk_class="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-volumes \
  --volume-ids "$volume_id" \
  --query 'Volumes[0].join(`-`, [VolumeType, to_string(Size), to_string(Iops), to_string(Throughput)])' \
  --output text)"
accelerator="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instance-types \
  --instance-types "$instance_type" \
  --query 'InstanceTypes[0].GpuInfo.Gpus[0].Name' --output text)"
[[ "$accelerator" == "None" ]] && accelerator=none

require_iam_actions() {
  local resource="$1"
  shift
  local denied
  denied="$(aws --profile "$PROFILE" iam simulate-principal-policy \
    --policy-source-arn "$role_arn" \
    --action-names "$@" \
    --resource-arns "$resource" \
    --query 'length(EvaluationResults[?EvalDecision != `allowed`])' \
    --output text)"
  [[ "$denied" == "0" ]] || {
    echo "S3 Vectors IAM preflight failed for $resource" >&2
    exit 5
  }
}
iam_bucket="arn:aws:s3vectors:$REGION:$account:bucket/borsuk-publication-v2-preflight"
iam_index="$iam_bucket/index/vectors"
require_iam_actions "*" s3vectors:ListVectorBuckets
require_iam_actions "$iam_bucket" \
  s3vectors:CreateVectorBucket s3vectors:GetVectorBucket \
  s3vectors:DeleteVectorBucket s3vectors:ListIndexes
require_iam_actions "$iam_index" \
  s3vectors:CreateIndex s3vectors:GetIndex s3vectors:DeleteIndex \
  s3vectors:PutVectors s3vectors:QueryVectors

if [[ "$state" == "stopped" ]]; then
  aws --profile "$PROFILE" --region "$REGION" ec2 start-instances \
    --instance-ids "$INSTANCE_ID" >/dev/null
fi
aws --profile "$PROFILE" --region "$REGION" ec2 wait instance-status-ok \
  --instance-ids "$INSTANCE_ID"

run_ssm_script() {
  local script="$1"
  local parameters command_id
  parameters="$(jq -cn --arg command "$script" '{commands:[$command]}')"
  command_id="$(aws --profile "$PROFILE" --region "$REGION" ssm send-command \
    --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
    --parameters "$parameters" \
    --query 'Command.CommandId' --output text)"
  if ! aws --profile "$PROFILE" --region "$REGION" ssm wait command-executed \
    --command-id "$command_id" --instance-id "$INSTANCE_ID"; then
    aws --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
      --command-id "$command_id" --instance-id "$INSTANCE_ID" \
      --query '{Status:Status,Output:StandardOutputContent,Error:StandardErrorContent}' \
      --output json >&2 || true
    return 1
  fi
}

ssm_batch_pids=()
wait_for_ssm_batch() {
  local pid failed=0
  for pid in "${ssm_batch_pids[@]}"; do
    if ! wait "$pid"; then
      failed=1
    fi
  done
  ssm_batch_pids=()
  ((failed == 0))
}

stage_file_through_ssm() {
  local local_file="$1"
  local remote_file="$2"
  local expected_sha256="$3"
  local digest_label="$4"
  local chunk_dir chunk chunk_name encoded
  chunk_dir="$(mktemp -d "$launch_temp/ssm-chunks.XXXXXX")"
  split -b "$SSM_CHUNK_BYTES" -a 6 "$local_file" "$chunk_dir/chunk."

  run_ssm_script "set -euo pipefail
mkdir -p '$staging_dir'
find '$staging_dir' -maxdepth 1 -type f -name '$(basename "$remote_file").part.*' -delete
rm -f '$remote_file'"

  for chunk in "$chunk_dir"/chunk.*; do
    chunk_name="${chunk##*.}"
    encoded="$(base64 < "$chunk" | tr -d '\n')"
    run_ssm_script "set -euo pipefail
printf '%s' '$encoded' | base64 -d > '$remote_file.part.$chunk_name'" &
    ssm_batch_pids+=("$!")
    if ((${#ssm_batch_pids[@]} >= SSM_MAX_IN_FLIGHT)); then
      wait_for_ssm_batch
    fi
  done
  wait_for_ssm_batch

  run_ssm_script "set -euo pipefail
cat '$remote_file'.part.* > '$remote_file'
actual_sha256=\$(sha256sum '$remote_file' | awk '{print \$1}')
[[ \"\$actual_sha256\" == '$expected_sha256' ]] || {
  echo 'remote $digest_label digest mismatch' >&2
  exit 3
}"
}

staging_dir="/tmp/borsuk-publication-upload-$source_sha256"
staged_source="$staging_dir/source.tar.gz"
staged_manifest="$staging_dir/manifest.json"
stage_file_through_ssm "$archive" "$staged_source" "$source_sha256" source
stage_file_through_ssm "$MANIFEST" "$staged_manifest" "$manifest_sha256" manifest

remote_script="$(printf '%s\n' \
  'set -euo pipefail' \
  "session='$SESSION'" \
  "workspace='/home/ec2-user/borsuk-publication-v2-$source_sha256'" \
  "staged_source='$staged_source'" \
  "staged_manifest='$staged_manifest'" \
  "bucket='$BUCKET'" \
  "source_key='$source_key'" \
  "manifest_key='$manifest_key'" \
  "source_uri='$source_uri'" \
  "source_sha256='$source_sha256'" \
  "manifest_uri='$manifest_uri'" \
  "manifest_sha256='$manifest_sha256'" \
  'if sudo -iu ec2-user tmux list-sessions -F "#S" 2>/dev/null | grep -E "^borsuk-" | grep -v -F "$session" >/dev/null; then' \
  '  echo "another BORSUK campaign is active; refusing contention" >&2; exit 4' \
  'fi' \
  'mkdir -p "$workspace"' \
  'actual="$(sha256sum "$staged_source" | awk '"'"'{print $1}'"'"')"' \
  '[[ "$actual" == "$source_sha256" ]] || { echo "remote source digest mismatch" >&2; exit 3; }' \
  'actual_manifest="$(sha256sum "$staged_manifest" | awk '"'"'{print $1}'"'"')"' \
  '[[ "$actual_manifest" == "$manifest_sha256" ]] || { echo "remote manifest digest mismatch" >&2; exit 3; }' \
  'aws s3api put-object --bucket "$bucket" --key "$source_key" --body "$staged_source" --server-side-encryption AES256 >/dev/null' \
  'aws s3api put-object --bucket "$bucket" --key "$manifest_key" --body "$staged_manifest" --content-type application/json --server-side-encryption AES256 >/dev/null' \
  'tar -xzf "$staged_source" -C "$workspace"' \
  'cp "$staged_manifest" "$workspace/docs/research/publication-v2-manifest.json"' \
  'sudo chown -R ec2-user:ec2-user "$workspace"' \
  "campaign_argv=(env AWS_REGION='$REGION' AWS_DEFAULT_REGION='$REGION' BORSUK_PUBLICATION_BUCKET='$BUCKET' BORSUK_PUBLICATION_V2_RUN_ID='$RUN_ID' BORSUK_PUBLICATION_V2_RESULT_PREFIX='$result_prefix' BORSUK_PUBLICATION_V2_INDEX_PREFIX='$index_prefix' BORSUK_SOURCE_SHA256='$source_sha256' BORSUK_SOURCE_ARCHIVE='$staged_source' BORSUK_PUBLICATION_DATASETS=/home/ec2-user/borsuk-datasets BORSUK_PUBLICATION_HYBRID_DATASETS=/home/ec2-user/borsuk-hybrid-datasets BORSUK_INSTANCE_TYPE='$instance_type' BORSUK_LOCAL_DISK_CLASS='ebs-$local_disk_class' BORSUK_ACCELERATOR='$accelerator' BORSUK_INDEX_STORAGE_CLASS='amazon-s3-standard' BORSUK_PUBLICATION_V2_EXECUTE=1 BORSUK_RUN_PUBLICATION_V2=1 bash scripts/bench_publication_v2_aws.sh)" \
  'printf -v campaign_command "%q " "${campaign_argv[@]}"' \
  'sudo -iu ec2-user tmux new-session -d -s "$session" -c "$workspace"' \
  'sudo -iu ec2-user tmux set-option -t "$session" remain-on-exit on' \
  'sudo -iu ec2-user tmux send-keys -t "$session" -l -- "exec ${campaign_command% }"' \
  'sudo -iu ec2-user tmux send-keys -t "$session" Enter' \
  'printf "started %s\n" "$session"')"
encoded_remote="$(printf '%s' "$remote_script" | base64 | tr -d '\n')"
command_id="$(aws --profile "$PROFILE" --region "$REGION" ssm send-command \
  --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
  --parameters "commands=echo $encoded_remote | base64 -d | bash" \
  --query 'Command.CommandId' --output text)"
aws --profile "$PROFILE" --region "$REGION" ssm wait command-executed \
  --command-id "$command_id" --instance-id "$INSTANCE_ID"
aws --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
  --command-id "$command_id" --instance-id "$INSTANCE_ID" \
  --query '{Status:Status,Output:StandardOutputContent,Error:StandardErrorContent}' \
  --output json

printf '%s\n' \
  "run_id=$RUN_ID" \
  "session=$SESSION" \
  "source_sha256=$source_sha256" \
  "manifest_sha256=$manifest_sha256" \
  "results=s3://$BUCKET/$result_prefix"
