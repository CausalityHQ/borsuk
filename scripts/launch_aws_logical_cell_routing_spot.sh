#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-causality}"
REGION="${AWS_REGION:-eu-central-1}"
INSTANCE_TYPE="${BORSUK_ROUTING_INSTANCE_TYPE:-c7g.8xlarge}"
AMI_ID="${BORSUK_ROUTING_AMI_ID:-ami-07bcecd13a160173f}"
SUBNET_ID="${BORSUK_ROUTING_SUBNET_ID:-subnet-034528fbd6977848f}"
SECURITY_GROUP_ID="${BORSUK_ROUTING_SECURITY_GROUP_ID:-sg-0b1fd3e4fbde4af0d}"
KEY_NAME="${BORSUK_ROUTING_KEY_NAME:-borsuk-bench}"
INSTANCE_PROFILE="${BORSUK_ROUTING_INSTANCE_PROFILE:-borsuk-bench-profile}"
RUST_TOOLCHAIN="${BORSUK_ROUTING_RUST_TOOLCHAIN:-1.91.0}"
BUCKET="${BORSUK_ROUTING_BUCKET:-borsuk-bench-453182569524-euc1}"
RUN_ID="${BORSUK_ROUTING_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-v12}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CAMPAIGN="$ROOT_DIR/docs/research/logical-cell-routing-positioned-v12-campaign.json"
CAMPAIGN_ID="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["campaign_id"])' "$CAMPAIGN")"
CAMPAIGN_TIMEOUT_SECONDS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["campaign_timeout_seconds"])' "$CAMPAIGN")"
CLONE_TIMEOUT_SECONDS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["clone_timeout_seconds"])' "$CAMPAIGN")"
SYSTEMD_STOP_SECONDS="$((CLONE_TIMEOUT_SECONDS + 60))"
RESULT_URI="s3://${BUCKET}/research/logical-cell-routing-positioned-v12/${RUN_ID}/results"
INDEX_URI="s3://${BUCKET}/research/logical-cell-routing-positioned-v12/${RUN_ID}/index"

if [[ "${BORSUK_LAUNCH_DRY_RUN:-0}" == "1" ]]; then
  python3 - "$PROFILE" "$REGION" "$INSTANCE_TYPE" "$CAMPAIGN_ID" "$RUN_ID" "$RESULT_URI" "$INDEX_URI" "$CAMPAIGN_TIMEOUT_SECONDS" "$CLONE_TIMEOUT_SECONDS" "$SYSTEMD_STOP_SECONDS" "$RUST_TOOLCHAIN" <<'PY'
import json
import sys
profile, region, instance_type, campaign, run_id, result_uri, index_uri, campaign_timeout, clone_timeout, stop_timeout, rust_toolchain = sys.argv[1:]
print(json.dumps({
    "profile": profile,
    "region": region,
    "instance_type": instance_type,
    "purchase_option": "spot",
    "instance_count": 1,
    "instance_initiated_shutdown_behavior": "terminate",
    "root_volume_delete_on_termination": True,
    "campaign_timeout_seconds": int(campaign_timeout),
    "clone_timeout_seconds": int(clone_timeout),
    "systemd_timeout_stop_seconds": int(stop_timeout),
    "independent_shutdown_deadline": True,
    "rust_toolchain": rust_toolchain,
    "provisions_build_dependencies": True,
    "preserves_campaign_log_on_failure": True,
    "campaign": campaign,
    "run_id": run_id,
    "result_uri": result_uri,
    "index_uri": index_uri,
}, sort_keys=True))
PY
  exit 0
fi

cd "$ROOT_DIR"
[[ -z "$(git status --porcelain)" ]] || { echo "launch requires a clean source tree" >&2; exit 2; }
git fetch --quiet origin main
git merge-base --is-ancestor origin/main HEAD || {
  echo "origin/main is not an ancestor of the launch revision" >&2
  exit 2
}
[[ "$(git rev-parse HEAD)" == "$(git rev-parse origin/main)" ]] || {
  echo "launch revision must already be delivered to origin/main" >&2
  exit 2
}
account="$(aws --profile "$PROFILE" --region "$REGION" sts get-caller-identity --query Account --output text)"
[[ "$account" == "453182569524" ]] || { echo "AWS account mismatch: $account" >&2; exit 2; }

active_borsuk="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --filters 'Name=instance-state-name,Values=pending,running' 'Name=tag:Name,Values=borsuk-*' \
  --query 'Reservations[].Instances[].InstanceId' --output text)"
[[ -z "$active_borsuk" || "$active_borsuk" == None ]] || {
  echo "another BORSUK worker is active; refusing contention: $active_borsuk" >&2
  exit 3
}
python3 scripts/benchmark_s3.py assert-empty --profile "$PROFILE" --uri "$RESULT_URI"
python3 scripts/benchmark_s3.py assert-empty --profile "$PROFILE" --uri "$INDEX_URI"

source_archive="$(mktemp /tmp/borsuk-routing-v12-source.XXXXXX.tar)"
launch_spec="$(mktemp /tmp/borsuk-routing-v12-launch.XXXXXX.json)"
remote_script="$(mktemp /tmp/borsuk-routing-v12-remote.XXXXXX.sh)"
instance_id=""
cleanup() {
  status=$?
  rm -f "$source_archive" "$launch_spec" "$remote_script"
  if (( status != 0 )) && [[ -n "$instance_id" ]]; then
    aws --profile "$PROFILE" --region "$REGION" ec2 terminate-instances \
      --instance-ids "$instance_id" --output json >/dev/null || true
  fi
  exit "$status"
}
trap cleanup EXIT

git archive --format=tar HEAD -o "$source_archive"
source_sha="$(sha256sum "$source_archive" | awk '{print $1}')"
source_key="research/logical-cell-routing-positioned-v12/source/${source_sha}.tar"
aws --profile "$PROFILE" --region "$REGION" s3 cp \
  "$source_archive" "s3://${BUCKET}/${source_key}" --only-show-errors

availability_zone="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-subnets \
  --subnet-ids "$SUBNET_ID" --query 'Subnets[0].AvailabilityZone' --output text)"
spot_price="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-spot-price-history \
  --instance-types "$INSTANCE_TYPE" --product-descriptions 'Linux/UNIX' \
  --availability-zone "$availability_zone" --start-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --max-items 1 --query 'SpotPriceHistory[0].SpotPrice' --output text)"

python3 - "$launch_spec" "$AMI_ID" "$INSTANCE_TYPE" "$KEY_NAME" "$SECURITY_GROUP_ID" "$SUBNET_ID" "$INSTANCE_PROFILE" "$RUN_ID" "$CAMPAIGN_ID" <<'PY'
import json
import sys
path, ami, instance_type, key, security_group, subnet, profile, run_id, campaign = sys.argv[1:]
spec = {
    "ImageId": ami,
    "InstanceType": instance_type,
    "MinCount": 1,
    "MaxCount": 1,
    "KeyName": key,
    "SecurityGroupIds": [security_group],
    "SubnetId": subnet,
    "IamInstanceProfile": {"Name": profile},
    "InstanceMarketOptions": {
        "MarketType": "spot",
        "SpotOptions": {
            "SpotInstanceType": "one-time",
            "InstanceInterruptionBehavior": "terminate",
        },
    },
    "InstanceInitiatedShutdownBehavior": "terminate",
    "MetadataOptions": {
        "HttpEndpoint": "enabled",
        "HttpTokens": "required",
        "HttpPutResponseHopLimit": 2,
    },
    "BlockDeviceMappings": [{
        "DeviceName": "/dev/xvda",
        "Ebs": {
            "DeleteOnTermination": True,
            "Encrypted": True,
            "VolumeSize": 200,
            "VolumeType": "gp3",
        },
    }],
    "TagSpecifications": [{
        "ResourceType": "instance",
        "Tags": [
            {"Key": "Name", "Value": f"borsuk-routing-v12-{run_id}"},
            {"Key": "Project", "Value": "BorsukBenchmark"},
            {"Key": "Campaign", "Value": campaign},
            {"Key": "RunId", "Value": run_id},
            {"Key": "AutoTerminate", "Value": "true"},
        ],
    }],
}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(spec, handle)
PY

instance_id="$(aws --profile "$PROFILE" --region "$REGION" ec2 run-instances \
  --cli-input-json "file://${launch_spec}" --query 'Instances[0].InstanceId' --output text)"
aws --profile "$PROFILE" --region "$REGION" ec2 wait instance-running --instance-ids "$instance_id"
aws --profile "$PROFILE" --region "$REGION" ec2 wait instance-status-ok --instance-ids "$instance_id"
lifecycle="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
  --instance-ids "$instance_id" --query 'Reservations[0].Instances[0].InstanceLifecycle' --output text)"
[[ "$lifecycle" == spot ]] || { echo "launched instance is not Spot: $lifecycle" >&2; exit 4; }

for _ in $(seq 1 60); do
  ping="$(aws --profile "$PROFILE" --region "$REGION" ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=${instance_id}" \
    --query 'InstanceInformationList[0].PingStatus' --output text)"
  [[ "$ping" == Online ]] && break
  sleep 5
done
[[ "$ping" == Online ]] || { echo "Spot worker did not become SSM-online" >&2; exit 4; }

cat >"$remote_script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
run_id='${RUN_ID}'
source_sha='${source_sha}'
source_uri='s3://${BUCKET}/${source_key}'
result_uri='${RESULT_URI}'
index_uri='${INDEX_URI}'
instance_id='${instance_id}'
availability_zone='${availability_zone}'
spot_price='${spot_price}'
campaign_timeout_seconds='${CAMPAIGN_TIMEOUT_SECONDS}'
clone_timeout_seconds='${CLONE_TIMEOUT_SECONDS}'
rust_toolchain='${RUST_TOOLCHAIN}'
workspace="/home/ec2-user/borsuk-routing-v12-source-${source_sha}"
output="/home/ec2-user/borsuk-routing-v12-results-${RUN_ID}"
archive="/tmp/borsuk-routing-v12-source-${source_sha}.tar"
campaign_log="/home/ec2-user/borsuk-routing-v12-${RUN_ID}.campaign.log"

finish() {
  status=\$?
  trap - EXIT
  mkdir -p "\$output"
  if [[ -f "\$campaign_log" ]]; then cp "\$campaign_log" "\$output/campaign.log"; fi
  printf '%s\n' "\$status" > "\$output/runner-exit.txt"
  printf '%s\n' "launch_spot_price_usd_per_hour=\$spot_price" > "\$output/launch-cost.txt"
  if [[ -f "\$workspace/scripts/benchmark_s3.py" ]]; then
    python3 "\$workspace/scripts/benchmark_s3.py" publish --source "\$output" \
      --destination "\$result_uri" --timeout-seconds "\$clone_timeout_seconds" || true
  else
    aws s3 sync --only-show-errors "\$output" "\$result_uri" || true
  fi
  sudo shutdown -h now || true
  exit "\$status"
}
trap finish EXIT
[[ ! -e "\$workspace" && ! -e "\$output" ]] || { echo 'remote paths already exist' >&2; exit 5; }
exec > >(tee -a "\$campaign_log") 2>&1
if pgrep -af 'logical_cell_routing_bench|bench_group_commit|production_bench' >/dev/null; then
  echo 'another BORSUK benchmark process is active' >&2
  exit 5
fi
mkdir -p "\$workspace"
sudo shutdown --poweroff "+\$((campaign_timeout_seconds / 60))"
aws s3 cp "\$source_uri" "\$archive" --only-show-errors
[[ "\$(sha256sum "\$archive" | awk '{print \$1}')" == "\$source_sha" ]] || {
  echo 'source archive checksum mismatch' >&2
  exit 5
}
tar -xf "\$archive" -C "\$workspace"
cd "\$workspace"
export HOME=/home/ec2-user
sudo dnf install -y gcc gcc-c++ make cmake perl pkgconf-pkg-config openssl-devel clang
if [[ ! -x /home/ec2-user/.cargo/bin/rustup ]]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain "\$rust_toolchain"
fi
source /home/ec2-user/.cargo/env
rustup toolchain install "\$rust_toolchain" --profile minimal
rustup default "\$rust_toolchain"
rustc --version
cargo --version
export AWS_REGION='${REGION}' AWS_DEFAULT_REGION='${REGION}'
export CARGO_BUILD_JOBS=32 CARGO_INCREMENTAL=0
export BORSUK_SOURCE_ARCHIVE="\$archive" BORSUK_SOURCE_SHA256="\$source_sha"
export BORSUK_ROUTING_OUTPUT_ROOT="\$output"
export BORSUK_ROUTING_INDEX_ROOT="\$index_uri"
export BORSUK_ROUTING_RESULT_URI="\$result_uri"
export BORSUK_ARCHITECTURE=aarch64 BORSUK_INSTANCE_TYPE='${INSTANCE_TYPE}'
export BORSUK_INSTANCE_ID="\$instance_id" BORSUK_AVAILABILITY_ZONE="\$availability_zone"
export BORSUK_PURCHASE_OPTION=spot BORSUK_RUN_LOGICAL_CELL_ROUTING=1
bash scripts/bench_logical_cell_routing.sh
EOF

remote_base64="$(base64 -w0 "$remote_script")"
command_id="$(aws --profile "$PROFILE" --region "$REGION" ssm send-command \
  --instance-ids "$instance_id" --document-name AWS-RunShellScript \
  --comment "BORSUK positioned V12 logical-cell routing ${RUN_ID}" \
  --parameters "commands=echo ${remote_base64} | base64 -d > /home/ec2-user/borsuk-routing-v12-run.sh && chown ec2-user:ec2-user /home/ec2-user/borsuk-routing-v12-run.sh && chmod 700 /home/ec2-user/borsuk-routing-v12-run.sh && systemd-run --unit=borsuk-routing-v12-${RUN_ID} --uid=ec2-user --working-directory=/home/ec2-user --property=RuntimeMaxSec=${CAMPAIGN_TIMEOUT_SECONDS} --property=TimeoutStopSec=${SYSTEMD_STOP_SECONDS} --collect /bin/bash /home/ec2-user/borsuk-routing-v12-run.sh" \
  --query Command.CommandId --output text)"
aws --profile "$PROFILE" --region "$REGION" ssm wait command-executed \
  --command-id "$command_id" --instance-id "$instance_id"
invocation="$(aws --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
  --command-id "$command_id" --instance-id "$instance_id" \
  --query '{Status:Status,Output:StandardOutputContent,Error:StandardErrorContent}' --output json)"
[[ "$(python3 -c 'import json,sys; print(json.load(sys.stdin)["Status"])' <<<"$invocation")" == Success ]] || {
  printf '%s\n' "$invocation" >&2
  exit 5
}

instance_id_for_output="$instance_id"
instance_id=""
trap cleanup EXIT
printf '%s\n' \
  "run_id=$RUN_ID" \
  "instance_id=$instance_id_for_output" \
  "purchase_option=spot" \
  "spot_price_usd_per_hour=$spot_price" \
  "result_uri=$RESULT_URI" \
  "index_uri=$INDEX_URI" \
  "terminal_marker=$RESULT_URI/LOGICAL_CELL_ROUTING_COMPLETE" \
  "failure_marker=$RESULT_URI/LOGICAL_CELL_ROUTING_FAILED"
