#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-paired-qualification
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
timeout -k 2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261014/sq8-shortlist-final-build-admission-red-a0001/inputs/run-final-build-red.sh "$root/run-final-build-red.sh" > /dev/null
printf '%s  %s\n' db847e80fee6b029dd4756c5146d40761f3cab5011f913f06ed308c06e58487e "$root/run-final-build-red.sh" | sha256sum --strict -c -
bash "$root/run-final-build-red.sh"
