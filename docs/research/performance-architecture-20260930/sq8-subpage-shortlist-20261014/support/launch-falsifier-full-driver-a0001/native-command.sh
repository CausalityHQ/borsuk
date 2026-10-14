#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-paired-qualification
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
timeout -k 2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261014/sq8-shortlist-full-driver-a0001/inputs/run-full-driver.sh "$root/run-full-driver.sh" > /dev/null
printf '%s  %s\n' 6c05128cda9afd6f1edbe7798fa71bea9c32c21c24b8dbac3c92eae5ba2cc3b7 "$root/run-full-driver.sh" | sha256sum --strict -c -
bash "$root/run-full-driver.sh"
