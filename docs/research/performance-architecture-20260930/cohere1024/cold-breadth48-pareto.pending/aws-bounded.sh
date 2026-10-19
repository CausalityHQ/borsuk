#!/bin/bash
set -euo pipefail
exec /usr/bin/timeout --kill-after=2s 15s env AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard /mnt/borsuk-retained-root/usr/local/aws-cli/v2/2.36.11/dist/aws --cli-connect-timeout 5 --cli-read-timeout 10 "$@"
