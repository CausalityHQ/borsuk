#!/usr/bin/env bash
# Content-address and detach the fresh Fashion-MNIST product-format A/B on the
# explicit c7g publication worker. This script is the launch action.
set -euo pipefail

PROFILE="${AWS_PROFILE:-causality}"
REGION="${AWS_REGION:-eu-central-1}"
INSTANCE_ID="${BORSUK_BENCH_INSTANCE_ID:-i-0e73bacb470807838}"
BUCKET="${BORSUK_PUBLICATION_BUCKET:-borsuk-bench-453182569524-euc1}"
EXPECTED_ACCOUNT="${BORSUK_AWS_ACCOUNT:-453182569524}"
RUN_ID="${BORSUK_VORTEX_PRODUCT_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
RESULT_PREFIX="${BORSUK_VORTEX_PRODUCT_RESULT_PREFIX:-vortex-product-ab/results/$RUN_ID}"
INDEX_PREFIX="${BORSUK_VORTEX_PRODUCT_INDEX_PREFIX:-vortex-product-ab/indexes/$RUN_ID}"
SHUTDOWN="${BORSUK_VORTEX_PRODUCT_SHUTDOWN:-1}"
SESSION="borsuk-vortex-product-ab-$RUN_ID"

cd "$(dirname "$0")/.."

for value in "$INSTANCE_ID" "$BUCKET" "$RUN_ID" "$RESULT_PREFIX" "$INDEX_PREFIX"; do
  if [[ ! "$value" =~ ^[A-Za-z0-9._/-]+$ ]]; then
    echo "unsafe launcher value: $value" >&2
    exit 2
  fi
done
if [[ "$SHUTDOWN" != "0" && "$SHUTDOWN" != "1" ]]; then
  echo "BORSUK_VORTEX_PRODUCT_SHUTDOWN must be 0 or 1" >&2
  exit 2
fi

account="$(aws --profile "$PROFILE" sts get-caller-identity --query Account --output text)"
if [[ "$account" != "$EXPECTED_ACCOUNT" ]]; then
  echo "AWS account mismatch: got $account, expected $EXPECTED_ACCOUNT" >&2
  exit 2
fi
aws --profile "$PROFILE" --region "$REGION" s3api head-bucket --bucket "$BUCKET"
for prefix in "$RESULT_PREFIX" "$INDEX_PREFIX"; do
  existing="$(aws --profile "$PROFILE" --region "$REGION" s3api list-objects-v2 \
    --bucket "$BUCKET" --prefix "${prefix%/}/" --max-keys 1 \
    --query KeyCount --output text)"
  if [[ "$existing" != "0" ]]; then
    echo "refusing to overwrite non-empty S3 prefix: s3://$BUCKET/$prefix" >&2
    exit 3
  fi
done

temp_root="$(mktemp -d "${TMPDIR:-/tmp}/borsuk-vortex-product-launch.XXXXXX")"
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
source_key="vortex-product-ab/source/$source_sha256.tar.gz"
aws --profile "$PROFILE" --region "$REGION" s3 cp \
  "$archive" "s3://$BUCKET/$source_key" --only-show-errors

state="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].State.Name' --output text)"
instance_type="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].InstanceType' --output text)"
volume_id="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].BlockDeviceMappings[0].Ebs.VolumeId' --output text)"
case "$state" in
  running) LAUNCHED_INSTANCE=0 ;;
  stopped)
    LAUNCHED_INSTANCE=1
    aws --profile "$PROFILE" --region "$REGION" ec2 start-instances \
      --instance-ids "$INSTANCE_ID" >/dev/null
    ;;
  *)
    echo "explicit worker $INSTANCE_ID is not reusable (state=$state)" >&2
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

workspace="/home/ec2-user/borsuk-vortex-product-source-$source_sha256"
remote_script="$(printf '%s\n' \
  'set -euo pipefail' \
  "workspace='$workspace'" \
  "source_uri='s3://$BUCKET/$source_key'" \
  "source_sha='$source_sha256'" \
  "session='$SESSION'" \
  'sudo shutdown -c >/dev/null 2>&1 || true' \
  'id ec2-user >/dev/null 2>&1 || { echo "ec2-user is required" >&2; exit 7; }' \
  'sudo loginctl enable-linger ec2-user' \
  'if [[ "$(loginctl show-user ec2-user --property=Linger --value)" != "yes" ]]; then' \
  '  echo "ec2-user linger verification failed" >&2; exit 7' \
  'fi' \
  'if ! command -v tmux >/dev/null 2>&1; then' \
  '  if command -v dnf >/dev/null 2>&1; then sudo dnf install -y tmux;' \
  '  elif command -v yum >/dev/null 2>&1; then sudo yum install -y tmux;' \
  '  else echo "tmux is unavailable" >&2; exit 5; fi' \
  'fi' \
  'command -v tmux >/dev/null 2>&1 || { echo "tmux installation failed" >&2; exit 5; }' \
  'if sudo -iu ec2-user tmux list-sessions -F "#S" 2>/dev/null | grep -E "^borsuk-" | grep -v -F "$session" >/dev/null; then' \
  '  echo "another BORSUK campaign is active; refusing contention" >&2; exit 4' \
  'fi' \
  'if [[ ! -f "$workspace/source.ready" ]]; then' \
  '  if [[ -e "$workspace" ]]; then echo "partial source workspace exists" >&2; exit 3; fi' \
  '  mkdir -p "$workspace"' \
  '  aws s3 cp "$source_uri" /tmp/borsuk-vortex-product-source.tar.gz --only-show-errors' \
  '  actual="$(sha256sum /tmp/borsuk-vortex-product-source.tar.gz)"' \
  '  actual="${actual%% *}"' \
  '  if [[ "$actual" != "$source_sha" ]]; then echo "source archive digest mismatch" >&2; exit 3; fi' \
  '  tar -xzf /tmp/borsuk-vortex-product-source.tar.gz -C "$workspace"' \
  '  printf "%s\n" "$source_sha" > "$workspace/source.ready"' \
  'fi' \
  'if [[ "$(cat "$workspace/source.ready")" != "$source_sha" ]]; then' \
  '  echo "source workspace digest marker mismatch" >&2; exit 3' \
  'fi' \
  'campaign_root=/home/ec2-user/borsuk-vortex-product-ab' \
  'sudo install -d -o ec2-user -g ec2-user "$campaign_root"' \
  'sudo chown -R ec2-user:ec2-user "$workspace" "$campaign_root"' \
  'sudo -iu ec2-user test -w "$campaign_root"' \
  'if sudo -iu ec2-user tmux has-session -t "$session" 2>/dev/null; then' \
  '  echo "tmux session already exists: $session" >&2; exit 3' \
  'fi' \
  "campaign_log='/home/ec2-user/borsuk-vortex-product-ab/$RUN_ID/campaign.log'" \
  "bootstrap_log='/home/ec2-user/$SESSION.bootstrap.log'" \
  "campaign_argv=(env \"AWS_REGION=$REGION\" \"AWS_DEFAULT_REGION=$REGION\" \"BORSUK_PUBLICATION_BUCKET=$BUCKET\" \"BORSUK_VORTEX_PRODUCT_RUN_ID=$RUN_ID\" \"BORSUK_VORTEX_PRODUCT_RESULT_PREFIX=$RESULT_PREFIX\" \"BORSUK_VORTEX_PRODUCT_INDEX_PREFIX=$INDEX_PREFIX\" \"BORSUK_SOURCE_SHA256=$source_sha256\" \"BORSUK_INSTANCE_TYPE=$instance_type\" \"BORSUK_LOCAL_DISK_CLASS=ebs-$disk_class\" \"BORSUK_VORTEX_PRODUCT_LAUNCHED_INSTANCE=$LAUNCHED_INSTANCE\" \"BORSUK_VORTEX_PRODUCT_SHUTDOWN=$SHUTDOWN\" bash scripts/bench_vortex_product_ab_aws.sh)" \
  'printf -v campaign_command "%q " "${campaign_argv[@]}"' \
  'campaign_command="exec ${campaign_command% }"' \
  'sudo -iu ec2-user tmux new-session -d -s "$session" -c "$workspace"' \
  'sudo -iu ec2-user tmux set-option -t "$session" remain-on-exit on' \
  'sudo -iu ec2-user tmux pipe-pane -t "$session" -o "cat >> $bootstrap_log"' \
  'sudo -iu ec2-user tmux send-keys -t "$session" -l -- "$campaign_command"' \
  'sudo -iu ec2-user tmux send-keys -t "$session" Enter' \
  'pane_dead=1' \
  'startup_observed=0' \
  'for _ in $(seq 1 20); do' \
  '  pane_dead="$(sudo -iu ec2-user tmux display-message -p -t "$session:0.0" "#{pane_dead}" 2>/dev/null || printf 1)"' \
  '  if [[ "$pane_dead" == "1" ]]; then break; fi' \
  '  if [[ -f "$campaign_log" ]]; then startup_observed=1; break; fi' \
  '  sleep 0.5' \
  'done' \
  'if [[ "$pane_dead" == "0" ]]; then startup_observed=1; fi' \
  'if [[ "$pane_dead" != "0" || "$startup_observed" != "1" ]]; then' \
  '  echo "campaign pane exited during startup" >&2' \
  '  sudo -iu ec2-user tmux capture-pane -p -S -200 -t "$session:0.0" >&2 || true' \
  '  tail -200 "$bootstrap_log" >&2 2>/dev/null || true' \
  '  sudo -iu ec2-user tmux kill-session -t "$session" >/dev/null 2>&1 || true' \
  '  exit 8' \
  'fi' \
  'printf "started tmux session %s pane_dead=%s\n" "$session" "$pane_dead"' \
  "echo 'workspace $workspace'" \
  "echo 'parquet_index s3://$BUCKET/$INDEX_PREFIX/parquet/fashion-mnist-784/srht-pq-scan'" \
  "echo 'vortex_index s3://$BUCKET/$INDEX_PREFIX/vortex/fashion-mnist-784/srht-pq-scan'" \
  "echo 'results s3://$BUCKET/$RESULT_PREFIX'")"
encoded_remote="$(printf '%s' "$remote_script" | base64 | tr -d '\n')"
command_id="$(aws --profile "$PROFILE" --region "$REGION" ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name AWS-RunShellScript \
  --comment "BORSUK fresh Vortex product A/B $RUN_ID" \
  --parameters "commands=echo $encoded_remote | base64 -d | bash" \
  --query 'Command.CommandId' --output text)"
aws --profile "$PROFILE" --region "$REGION" ssm wait command-executed \
  --command-id "$command_id" --instance-id "$INSTANCE_ID"
invocation_status="$(aws --profile "$PROFILE" --region "$REGION" \
  ssm get-command-invocation --command-id "$command_id" --instance-id "$INSTANCE_ID" \
  --query Status --output text)"
aws --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
  --command-id "$command_id" --instance-id "$INSTANCE_ID" \
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
  "launched_instance=$LAUNCHED_INSTANCE" \
  "shutdown_when_complete=$SHUTDOWN" \
  "parquet_index=s3://$BUCKET/$INDEX_PREFIX/parquet/fashion-mnist-784/srht-pq-scan" \
  "vortex_index=s3://$BUCKET/$INDEX_PREFIX/vortex/fashion-mnist-784/srht-pq-scan" \
  "results=s3://$BUCKET/$RESULT_PREFIX" \
  "checkpoint=s3://$BUCKET/$RESULT_PREFIX/VORTEX_PRODUCT_AB_COMPLETE"
