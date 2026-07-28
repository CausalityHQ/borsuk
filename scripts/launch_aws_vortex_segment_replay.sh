#!/usr/bin/env bash
# Package the exact local state and detach a corrected real-segment replay on
# the explicit benchmark worker. This script starts no benchmark implicitly:
# invoking it is the launch action.
set -euo pipefail

PROFILE="${AWS_PROFILE:-causality}"
REGION="${AWS_REGION:-eu-central-1}"
INSTANCE_ID="${BORSUK_BENCH_INSTANCE_ID:-i-0e73bacb470807838}"
BUCKET="${BORSUK_VORTEX_BUCKET:-borsuk-bench-453182569524-euc1}"
EXPECTED_ACCOUNT="${BORSUK_AWS_ACCOUNT:-453182569524}"
RUN_ID="${BORSUK_VORTEX_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
SOURCE_URI="${BORSUK_VORTEX_SOURCE_URI:-s3://$BUCKET/publication/indexes/20260724T092000Z/full-s3/20260724T092000Z/fashion-mnist-784/srht-pq-scan}"
RESULT_PREFIX="${BORSUK_VORTEX_RESULT_PREFIX:-vortex-segment-replay-corrected/results/$RUN_ID}"
DATA_PREFIX="${BORSUK_VORTEX_DATA_PREFIX:-vortex-segment-replay-corrected/data/$RUN_ID}"
SHUTDOWN="${BORSUK_VORTEX_SHUTDOWN:-1}"
SESSION="borsuk-vortex-segment-replay-$RUN_ID"

cd "$(dirname "$0")/.."

for value in "$INSTANCE_ID" "$BUCKET" "$RUN_ID" "$RESULT_PREFIX" "$DATA_PREFIX"; do
  if [[ ! "$value" =~ ^[A-Za-z0-9._/-]+$ ]]; then
    echo "unsafe launcher value: $value" >&2
    exit 2
  fi
done
case "$SOURCE_URI" in
  s3://*/*) ;;
  *)
    echo "BORSUK_VORTEX_SOURCE_URI must be a non-empty s3://bucket/prefix" >&2
    exit 2
    ;;
esac
source_location="${SOURCE_URI#s3://}"
source_bucket="${source_location%%/*}"
source_prefix="${source_location#*/}"
if [[ "$source_bucket" != "$BUCKET" ]]; then
  echo "source and materialized replay must use the same explicit bucket" >&2
  exit 2
fi
for value in "$source_bucket" "$source_prefix"; do
  if [[ ! "$value" =~ ^[A-Za-z0-9._/-]+$ ]]; then
    echo "unsafe source location: $SOURCE_URI" >&2
    exit 2
  fi
done
if [[ "$SHUTDOWN" != "0" && "$SHUTDOWN" != "1" ]]; then
  echo "BORSUK_VORTEX_SHUTDOWN must be 0 or 1" >&2
  exit 2
fi

account="$(aws --profile "$PROFILE" sts get-caller-identity \
  --query Account --output text)"
if [[ "$account" != "$EXPECTED_ACCOUNT" ]]; then
  echo "AWS account mismatch: got $account, expected $EXPECTED_ACCOUNT" >&2
  exit 2
fi
aws --profile "$PROFILE" --region "$REGION" s3api head-bucket --bucket "$BUCKET"
aws --profile "$PROFILE" --region "$REGION" s3api head-bucket --bucket "$source_bucket"
source_parquet_count="$(aws --profile "$PROFILE" --region "$REGION" \
  s3api list-objects-v2 \
  --bucket "$source_bucket" \
  --prefix "$source_prefix/segments/" \
  --query 'length(Contents[?ends_with(Key, `.parquet`)])' \
  --output text)"
if [[ "$source_parquet_count" == "None" || "$source_parquet_count" -lt 1 ]]; then
  echo "source has no segment Parquet objects: $SOURCE_URI" >&2
  exit 4
fi
for fresh_prefix in "$RESULT_PREFIX" "$DATA_PREFIX"; do
  existing="$(aws --profile "$PROFILE" --region "$REGION" s3api list-objects-v2 \
    --bucket "$BUCKET" \
    --prefix "${fresh_prefix%/}/" \
    --max-keys 1 \
    --query 'KeyCount' \
    --output text)"
  if [[ "$existing" != "0" ]]; then
    echo "refusing to overwrite non-empty S3 prefix: s3://$BUCKET/$fresh_prefix" >&2
    exit 3
  fi
done

temp_root="$(mktemp -d "${TMPDIR:-/tmp}/borsuk-vortex-replay-launch.XXXXXX")"
archive="$temp_root/borsuk-source.tar.gz"
LAUNCHED_INSTANCE=0
LAUNCH_COMMITTED=0
cleanup() {
  local status=$?
  rm -f -- "$archive"
  rmdir "$temp_root" 2>/dev/null || true
  if [[ "$LAUNCHED_INSTANCE" == "1" && "$LAUNCH_COMMITTED" != "1" ]]; then
    aws --profile "$PROFILE" --region "$REGION" ec2 stop-instances \
      --instance-ids "$INSTANCE_ID" >/dev/null 2>&1 || true
  fi
  return "$status"
}
trap cleanup EXIT
git ls-files -z --cached --others --exclude-standard -- Cargo.toml Cargo.lock crates scripts \
  | while IFS= read -r -d '' path; do
      case "$path" in
        */.borsuk-scratch/*|*/target/*|*/.venv*/*) continue ;;
      esac
      [[ -f "$path" ]] && printf '%s\0' "$path"
    done \
  | COPYFILE_DISABLE=1 tar --no-xattrs --null -czf "$archive" -T -
source_sha256="$(shasum -a 256 "$archive" | awk '{print $1}')"
source_key="vortex-segment-replay-corrected/source/$source_sha256.tar.gz"
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
volume_id="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].BlockDeviceMappings[0].Ebs.VolumeId' \
  --output text)"
case "$state" in
  running)
    LAUNCHED_INSTANCE=0
    ;;
  stopped)
    LAUNCHED_INSTANCE=1
    aws --profile "$PROFILE" --region "$REGION" ec2 start-instances \
      --instance-ids "$INSTANCE_ID" >/dev/null
    ;;
  *)
    echo "explicit worker $INSTANCE_ID is not safely reusable (state=$state)" >&2
    exit 3
    ;;
esac
disk_class="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-volumes \
  --volume-ids "$volume_id" \
  --query 'Volumes[0].join(`-`, [VolumeType, to_string(Size), to_string(Iops), to_string(Throughput)])' \
  --output text)"
aws --profile "$PROFILE" --region "$REGION" ec2 wait instance-running \
  --instance-ids "$INSTANCE_ID"
aws --profile "$PROFILE" --region "$REGION" ec2 wait instance-status-ok \
  --instance-ids "$INSTANCE_ID"

ssm_online=0
for _ in $(seq 1 60); do
  ping_status="$(aws --profile "$PROFILE" --region "$REGION" \
    ssm describe-instance-information \
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

workspace="/home/ec2-user/borsuk-vortex-replay-source-$source_sha256"
remote_script="$(printf '%s\n' \
  'set -euo pipefail' \
  "workspace='$workspace'" \
  "source_uri='s3://$BUCKET/$source_key'" \
  "session='$SESSION'" \
  'sudo shutdown -c >/dev/null 2>&1 || true' \
  'if ! id ec2-user >/dev/null 2>&1; then' \
  '  echo "ec2-user is required for a persistent benchmark session" >&2; exit 7' \
  'fi' \
  'if ! sudo loginctl enable-linger ec2-user; then' \
  '  echo "failed to enable persistent user manager for ec2-user" >&2; exit 7' \
  'fi' \
  'if [[ "$(loginctl show-user ec2-user --property=Linger --value)" != "yes" ]]; then' \
  '  echo "ec2-user linger verification failed" >&2; exit 7' \
  'fi' \
  'campaign_root=/home/ec2-user/borsuk-vortex-segment-replay' \
  'sudo install -d -o ec2-user -g ec2-user /home/ec2-user/borsuk-vortex-segment-replay' \
  'sudo chown ec2-user:ec2-user "$campaign_root"' \
  'if [[ "$(stat -c "%U:%G" "$campaign_root")" != "ec2-user:ec2-user" ]]; then' \
  '  echo "campaign root ownership repair failed" >&2; exit 7' \
  'fi' \
  'if ! sudo -iu ec2-user test -w "$campaign_root"; then' \
  '  echo "campaign root is not writable by ec2-user" >&2; exit 7' \
  'fi' \
  'tmux_provisioning=preinstalled' \
  'if ! command -v tmux >/dev/null 2>&1; then' \
  '  if command -v dnf >/dev/null 2>&1; then' \
  '    if ! sudo dnf install -y tmux; then' \
  '      echo "failed to install tmux with dnf" >&2; exit 5' \
  '    fi' \
  '    tmux_provisioning=installed-dnf' \
  '  elif command -v yum >/dev/null 2>&1; then' \
  '    if ! sudo yum install -y tmux; then' \
  '      echo "failed to install tmux with yum" >&2; exit 5' \
  '    fi' \
  '    tmux_provisioning=installed-yum' \
  '  else' \
  '    echo "tmux is unavailable and neither dnf nor yum can install it" >&2' \
  '    exit 5' \
  '  fi' \
  'fi' \
  'if ! command -v tmux >/dev/null 2>&1; then' \
  '  echo "tmux remains unavailable after package preflight" >&2; exit 5' \
  'fi' \
  'tmux_version="$(tmux -V | tr " " "_")"' \
  'printf "tmux_preflight version=%s provisioning=%s\n" "$tmux_version" "$tmux_provisioning"' \
  'pip_provisioning=preinstalled' \
  'if ! command -v python3 >/dev/null 2>&1; then' \
  '  echo "python3 is required to provision pinned uv" >&2; exit 6' \
  'fi' \
  'if ! python3 -m pip --version >/dev/null 2>&1; then' \
  '  if command -v dnf >/dev/null 2>&1; then' \
  '    if ! sudo dnf install -y python3-pip; then' \
  '      echo "failed to install python3-pip with dnf" >&2; exit 6' \
  '    fi' \
  '    pip_provisioning=installed-dnf' \
  '  elif command -v yum >/dev/null 2>&1; then' \
  '    if ! sudo yum install -y python3-pip; then' \
  '      echo "failed to install python3-pip with yum" >&2; exit 6' \
  '    fi' \
  '    pip_provisioning=installed-yum' \
  '  else' \
  '    echo "pip is unavailable and neither dnf nor yum can install python3-pip" >&2' \
  '    exit 6' \
  '  fi' \
  'fi' \
  'if ! python3 -m pip --version >/dev/null 2>&1; then' \
  '  echo "python3 pip remains unavailable after package preflight" >&2; exit 6' \
  'fi' \
  'uv_provisioning=preinstalled' \
  'export PATH="/usr/local/bin:$PATH"' \
  'if ! command -v uv >/dev/null 2>&1; then' \
  '  if ! sudo python3 -m pip install --no-cache-dir uv==0.11.28; then' \
  '    echo "failed to install pinned uv 0.11.28 with python3 pip" >&2; exit 6' \
  '  fi' \
  '  uv_provisioning=installed-pip' \
  '  hash -r' \
  'fi' \
  'if ! command -v uv >/dev/null 2>&1; then' \
  '  uv_user_bin="$(python3 -m site --user-base)/bin"' \
  '  export PATH="$uv_user_bin:$PATH"' \
  'fi' \
  'if ! command -v uv >/dev/null 2>&1; then' \
  '  echo "uv remains unavailable on PATH after pinned installation" >&2; exit 6' \
  'fi' \
  'uv_bin_dir="$(dirname "$(command -v uv)")"' \
  'uv_version="$(uv --version | tr " " "_")"' \
  'printf "uv_preflight version=%s provisioning=%s pip=%s path=%s\n" "$uv_version" "$uv_provisioning" "$pip_provisioning" "$uv_bin_dir"' \
  'if sudo -iu ec2-user env PATH="$PATH" tmux list-sessions -F "#S" 2>/dev/null | grep -E "^borsuk-" | grep -v -F "$session" >/dev/null; then' \
  '  echo "another BORSUK campaign is active; refusing benchmark contention" >&2' \
  '  exit 4' \
  'fi' \
  'if [[ ! -f "$workspace/source.ready" ]]; then' \
  '  if [[ -e "$workspace" ]]; then echo "partial source workspace exists" >&2; exit 3; fi' \
  '  mkdir -p "$workspace"' \
  '  aws s3 cp "$source_uri" /tmp/borsuk-vortex-replay-source.tar.gz --only-show-errors' \
  "  if [[ \"\$(sha256sum /tmp/borsuk-vortex-replay-source.tar.gz | awk '{print \$1}')\" != '$source_sha256' ]]; then" \
  '    echo "downloaded source archive digest mismatch" >&2; exit 3' \
  '  fi' \
  '  tar -xzf /tmp/borsuk-vortex-replay-source.tar.gz -C "$workspace"' \
  "  printf '%s\\n' '$source_sha256' > \"\$workspace/source.ready\"" \
  'fi' \
  "if [[ \"\$(cat \"\$workspace/source.ready\")\" != '$source_sha256' ]]; then" \
  '  echo "source workspace digest marker mismatch" >&2; exit 3' \
  'fi' \
  'sudo chown -R ec2-user:ec2-user "$workspace"' \
  'if sudo -iu ec2-user env PATH="$PATH" tmux has-session -t "$session" 2>/dev/null; then' \
  '  echo "tmux session already exists: $session" >&2' \
  '  exit 3' \
  'fi' \
  "campaign_log='/home/ec2-user/borsuk-vortex-segment-replay/$RUN_ID/campaign.log'" \
  "bootstrap_log='/home/ec2-user/$SESSION.bootstrap.log'" \
  "campaign_argv=(env \"PATH=\$uv_bin_dir:\$PATH\" \"AWS_REGION=$REGION\" \"AWS_DEFAULT_REGION=$REGION\" \"BORSUK_S3_BUCKET=$BUCKET\" \"BORSUK_VORTEX_RUN_ID=$RUN_ID\" \"BORSUK_VORTEX_SOURCE_URI=$SOURCE_URI\" \"BORSUK_VORTEX_RESULT_PREFIX=$RESULT_PREFIX\" \"BORSUK_VORTEX_DATA_PREFIX=$DATA_PREFIX\" \"BORSUK_SOURCE_SHA256=$source_sha256\" \"BORSUK_INSTANCE_TYPE=$instance_type\" \"BORSUK_LOCAL_DISK_CLASS=ebs-$disk_class\" \"BORSUK_VORTEX_LAUNCHED_INSTANCE=$LAUNCHED_INSTANCE\" \"BORSUK_VORTEX_SHUTDOWN=$SHUTDOWN\" \"BORSUK_TMUX_VERSION=\$tmux_version\" \"BORSUK_TMUX_PROVISIONING=\$tmux_provisioning\" \"BORSUK_UV_VERSION=\$uv_version\" \"BORSUK_UV_PROVISIONING=\$uv_provisioning\" \"BORSUK_PIP_PROVISIONING=\$pip_provisioning\" bash scripts/bench_vortex_segment_replay_aws.sh)" \
  'printf -v campaign_command "%q " "${campaign_argv[@]}"' \
  'campaign_command="exec ${campaign_command% }"' \
  'sudo -iu ec2-user env PATH="$PATH" tmux new-session -d -s "$session" -c "$workspace"' \
  'sudo -iu ec2-user env PATH="$PATH" tmux set-option -t "$session" remain-on-exit on' \
  'sudo -iu ec2-user env PATH="$PATH" tmux pipe-pane -t "$session" -o "cat >> $bootstrap_log"' \
  'sudo -iu ec2-user env PATH="$PATH" tmux send-keys -t "$session" -l -- "$campaign_command"' \
  'sudo -iu ec2-user env PATH="$PATH" tmux send-keys -t "$session" Enter' \
  'pane_dead=1' \
  'startup_observed=0' \
  'for _ in $(seq 1 20); do' \
  '  pane_dead="$(sudo -iu ec2-user env PATH="$PATH" tmux display-message -p -t "$session:0.0" "#{pane_dead}" 2>/dev/null || printf 1)"' \
  '  if [[ "$pane_dead" == "1" ]]; then break; fi' \
  '  if [[ "$pane_dead" != "0" ]]; then echo "invalid tmux pane state: $pane_dead" >&2; break; fi' \
  '  if [[ -f "$campaign_log" ]]; then startup_observed=1; break; fi' \
  '  sleep 0.5' \
  'done' \
  'if [[ "$pane_dead" == "0" ]]; then startup_observed=1; fi' \
  'if [[ "$pane_dead" != "0" || "$startup_observed" != "1" ]]; then' \
  '  echo "campaign pane exited during startup; pane follows" >&2' \
  '  sudo -iu ec2-user env PATH="$PATH" tmux capture-pane -p -S -200 -t "$session:0.0" >&2 || true' \
  '  echo "campaign bootstrap log follows" >&2' \
  '  tail -200 "$bootstrap_log" >&2 2>/dev/null || true' \
  '  sudo -iu ec2-user env PATH="$PATH" tmux kill-session -t "$session" >/dev/null 2>&1 || true' \
  '  exit 8' \
  'fi' \
  'printf "started tmux session %s pane_dead=%s campaign_log=%s\n" "$session" "$pane_dead" "$campaign_log"' \
  "echo 'workspace $workspace'" \
  "echo 'source $SOURCE_URI'" \
  "echo 'results s3://$BUCKET/$RESULT_PREFIX'")"
encoded_remote="$(printf '%s' "$remote_script" | base64 | tr -d '\n')"
command_id="$(aws --profile "$PROFILE" --region "$REGION" ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name AWS-RunShellScript \
  --comment "BORSUK corrected Vortex segment replay $RUN_ID" \
  --parameters "commands=echo $encoded_remote | base64 -d | bash" \
  --query 'Command.CommandId' \
  --output text)"
aws --profile "$PROFILE" --region "$REGION" ssm wait command-executed \
  --command-id "$command_id" \
  --instance-id "$INSTANCE_ID"
invocation_status="$(aws --profile "$PROFILE" --region "$REGION" \
  ssm get-command-invocation \
  --command-id "$command_id" \
  --instance-id "$INSTANCE_ID" \
  --query Status \
  --output text)"
aws --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
  --command-id "$command_id" \
  --instance-id "$INSTANCE_ID" \
  --query '{Status:Status,Output:StandardOutputContent,Error:StandardErrorContent}' \
  --output json
if [[ "$invocation_status" != "Success" ]]; then
  echo "remote launch command failed with status $invocation_status" >&2
  exit 4
fi
LAUNCH_COMMITTED=1

printf '%s\n' \
  "run_id=$RUN_ID" \
  "session=$SESSION" \
  "instance_id=$INSTANCE_ID" \
  "source_sha256=$source_sha256" \
  "source=$SOURCE_URI" \
  "execution_mode=materialized_arrow" \
  "launched_instance=$LAUNCHED_INSTANCE" \
  "shutdown_when_complete=$SHUTDOWN" \
  "results=s3://$BUCKET/$RESULT_PREFIX" \
  "checkpoint=s3://$BUCKET/$RESULT_PREFIX/VORTEX_SEGMENT_REPLAY_COMPLETE"
