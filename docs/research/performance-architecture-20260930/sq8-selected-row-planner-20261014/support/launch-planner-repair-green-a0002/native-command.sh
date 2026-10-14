#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-paired-qualification
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
timeout -k 2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261014/sq8-selected-row-planner-repair-green-a0001/inputs/run-green.sh "$root/run-green.sh" > /dev/null
printf '%s  %s\n' 06e235b1504a8527c87ccc7cbb4cfdf12ec9a4560f2afeba524c855055bed991 "$root/run-green.sh" | sha256sum --strict -c -
bash "$root/run-green.sh"
