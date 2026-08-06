#!/usr/bin/env bash
# Launch the preregistered group-commit scalability matrix in a detached,
# terminal-marker-monitored tmux session on the dedicated EC2 worker.
set -euo pipefail

PROFILE="${AWS_PROFILE:-causality}"
REGION="${AWS_REGION:-eu-central-1}"
INSTANCE_ID="${BORSUK_BENCH_INSTANCE_ID:-i-0e73bacb470807838}"
BUCKET="${BORSUK_GROUP_COMMIT_BUCKET:-borsuk-bench-453182569524-euc1}"
RUN_ID="${BORSUK_GROUP_COMMIT_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CAMPAIGN="${ROOT_DIR}/docs/research/realistic-group-commit-campaign.json"
RESULT_URI="s3://${BUCKET}/research/group-commit-scalability/${RUN_ID}/results"
INDEX_URI="s3://${BUCKET}/research/group-commit-scalability/${RUN_ID}/index"
SESSION="borsuk-group-commit-${RUN_ID}"

cd "$ROOT_DIR"
account="$(aws --profile "$PROFILE" --region "$REGION" sts get-caller-identity --query Account --output text)"
[[ "$account" == "453182569524" ]] || {
  echo "AWS account mismatch: $account" >&2
  exit 2
}
state="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].State.Name' --output text)"
[[ "$state" == running ]] || { echo "worker is not running: $state" >&2; exit 3; }
instance_type="$(aws --profile "$PROFILE" --region "$REGION" ec2 describe-instances --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].InstanceType' --output text)"
[[ "$instance_type" == c7g.8xlarge ]] || { echo "unexpected instance type: $instance_type" >&2; exit 3; }
source_archive="$(mktemp /tmp/borsuk-group-commit-source.XXXXXX.tar)"
trap 'rm -f "$source_archive"' EXIT
git archive --format=tar HEAD -o "$source_archive"
source_sha="$(sha256sum "$source_archive" | awk '{print $1}')"
manifest_sha="$(sha256sum "$CAMPAIGN" | awk '{print $1}')"
source_key="research/group-commit-scalability/source/${source_sha}.tar"
aws --profile "$PROFILE" --region "$REGION" s3 cp "$source_archive" "s3://${BUCKET}/${source_key}" --only-show-errors

encoded_remote="$(cat <<EOF | base64 -w0
set -euo pipefail
workspace="/home/ec2-user/borsuk-group-commit-source-${source_sha}"
source_uri="s3://${BUCKET}/${source_key}"
session="${SESSION}"
result_uri="${RESULT_URI}"
index_uri="${INDEX_URI}"
active_panes="\$(sudo -iu ec2-user tmux list-panes -a -F '#{pane_dead}|#{pane_current_command}' 2>/dev/null | awk -F'|' '\$1 == 0 && \$2 != "bash" { print }')"
if [[ -n "\$active_panes" ]]; then
  echo 'another BORSUK tmux workload is active; refusing contention' >&2
  printf '%s\n' "\$active_panes" >&2
  exit 4
fi
if pgrep -af 'bench_group_commit_scalability|group_commit_bench|logical_cell_routing_bench' >/dev/null; then
  echo 'another BORSUK benchmark process is active; refusing contention' >&2
  exit 4
fi
if [[ -e "\$workspace" ]]; then
  echo "source workspace already exists: \$workspace" >&2
  exit 5
fi
mkdir -p "\$workspace"
aws s3 cp "\$source_uri" /tmp/borsuk-group-commit-source.tar --only-show-errors
actual="\$(sha256sum /tmp/borsuk-group-commit-source.tar | awk '{print \$1}')"
[[ "\$actual" == "${source_sha}" ]] || { echo 'source archive checksum mismatch' >&2; exit 5; }
tar -xf /tmp/borsuk-group-commit-source.tar -C "\$workspace"
printf '%s\n' "${source_sha}" > "\$workspace/source.ready"
sudo chown -R ec2-user:ec2-user "\$workspace"
dataset_root=/home/ec2-user/borsuk-datasets
dataset_dir="\$dataset_root/cohere-medium-1M"
if [[ ! -f "\$dataset_dir/dataset.json" || ! -f "\$dataset_dir/train.parquet" ]]; then
  sudo -iu ec2-user bash -lc "cd \"\$workspace\" && uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt python scripts/fetch_vdbbench_dataset.py --dataset cohere-medium-1M --output-root \"\$dataset_root\" --execute-download"
fi
sudo -iu ec2-user bash -lc "cd \"\$workspace\" && uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt python scripts/fetch_vdbbench_dataset.py --dataset cohere-medium-1M --output-root \"\$dataset_root\" --check-existing" >/dev/null
[[ -f "\$dataset_dir/dataset.json" && -f "\$dataset_dir/train.parquet" ]] || {
  echo 'pinned Cohere dataset preparation failed' >&2
  exit 7
}
sudo -iu ec2-user tmux new-session -d -s "\$session" -c "\$workspace"
sudo -iu ec2-user tmux set-option -t "\$session" remain-on-exit on
sentinel="/home/ec2-user/borsuk-group-commit-runner-started-${RUN_ID}"
cmd="printf 'started\\n' > \"\$sentinel\" && exec env AWS_REGION='${REGION}' AWS_DEFAULT_REGION='${REGION}' BORSUK_SOURCE_ARCHIVE=/tmp/borsuk-group-commit-source.tar BORSUK_GROUP_COMMIT_SCALABILITY_OUTPUT_ROOT='/home/ec2-user/borsuk-group-commit-results/${RUN_ID}' BORSUK_GROUP_COMMIT_SCALABILITY_INDEX_ROOT=\"\$index_uri\" BORSUK_GROUP_COMMIT_SCALABILITY_RESULT_URI=\"\$result_uri\" BORSUK_GROUP_COMMIT_DATASET='/home/ec2-user/borsuk-datasets/cohere-medium-1M' BORSUK_ARCHITECTURE=aarch64 BORSUK_INSTANCE_TYPE='${instance_type}' BORSUK_RUN_GROUP_COMMIT_SCALABILITY=1 bash scripts/bench_group_commit_scalability.sh"
sudo -iu ec2-user tmux send-keys -t "\$session" -l -- "\$cmd"
sudo -iu ec2-user tmux send-keys -t "\$session" Enter
for _ in \$(seq 1 120); do
  dead="\$(sudo -iu ec2-user tmux display-message -p -t "\$session:0.0" '#{pane_dead}' 2>/dev/null || printf 1)"
  [[ "\$dead" == 0 ]] || break
  if sudo -iu ec2-user test -f "\$sentinel"; then
    break
  fi
  sleep 1
done
[[ "\$dead" == 0 ]] && sudo -iu ec2-user test -f "\$sentinel" || {
  sudo -iu ec2-user tmux capture-pane -p -t "\$session:0.0" -S -100 >&2 || true
  exit 6
}
printf 'started session=%s source_sha256=%s result=%s index=%s\n' "\$session" '${source_sha}' "\$result_uri" "\$index_uri"
EOF
)"
command_id="$(aws --profile "$PROFILE" --region "$REGION" ssm send-command \
  --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
  --comment "BORSUK group-commit scalability ${RUN_ID}" \
  --parameters "commands=echo ${encoded_remote} | base64 -d | bash" \
  --query Command.CommandId --output text)"
aws --profile "$PROFILE" --region "$REGION" ssm wait command-executed --command-id "$command_id" --instance-id "$INSTANCE_ID"
aws --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
  --command-id "$command_id" --instance-id "$INSTANCE_ID" \
  --query '{Status:Status,Output:StandardOutputContent,Error:StandardErrorContent}' --output json

printf '%s\n' \
  "run_id=$RUN_ID" \
  "session=$SESSION" \
  "source_sha256=$source_sha" \
  "manifest_sha256=$manifest_sha" \
  "result_uri=$RESULT_URI" \
  "index_uri=$INDEX_URI"
