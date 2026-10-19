#!/bin/bash
set -euo pipefail
E=/mnt/borsuk-workspace/evidence/fp32-concurrent-full1000-root-a0001
exec 9>/mnt/borsuk-workspace/cargo-target.lock
flock -n 9
exec 8>/mnt/borsuk-workspace/native-benchmark.lock
flock -n 8
source /mnt/borsuk-workspace/environment.sh
unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_CONTAINER_CREDENTIALS_RELATIVE_URI AWS_CONTAINER_CREDENTIALS_FULL_URI
export AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_CONFIG_FILE=/dev/null AWS_EC2_METADATA_DISABLED=false
cd "$E"
set -C
exec > "$E/controller.stdout" 2> "$E/controller.stderr"
trap 'rc=$?; if ! test -e "$E/terminal.json"; then printf "%s\n" "{\"status\":\"INVALID\",\"algorithm_negative\":false,\"completed_cells_preserved\":true,\"automatic_retry\":false}" > "$E/terminal.json"; fi; printf "%s\n" "$rc" > "$E/controller.exit"' EXIT
python3 -B "$E/run-full1000.py"
