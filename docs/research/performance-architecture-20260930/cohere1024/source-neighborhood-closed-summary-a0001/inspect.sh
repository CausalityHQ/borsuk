#!/bin/bash
# Evidence-only EC2 operation. No native execution, corpus/query/truth download or reduction.
set -euo pipefail
[[ $# == 3 ]] || exit 125
key=$1 expected=$2 bytes=$3
[[ $key == research/semantic-router/20261011/source-neighborhood-data-gates-a0002/smoke/native.log ]]
[[ $expected =~ ^[0-9a-f]{64}$ && $bytes =~ ^[0-9]+$ ]]
(( bytes <= 1048576 ))
export AWS_MAX_ATTEMPTS=1 AWS_PAGER='' AWS_DEFAULT_REGION=eu-central-1
work=$(mktemp -d /tmp/borsuk-closed-summary.XXXXXX)
timeout -k 2 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --checksum-mode ENABLED "$work/native.log" > "$work/get.json"
[[ $(stat -c %s "$work/native.log") == "$bytes" ]]
printf '%s  %s\n' "$expected" "$work/native.log" | sha256sum -c - > "$work/auth.txt"
printf 'BEGIN_AUTHENTICATED_EXISTING_RUST_SUMMARY\n'
cat "$work/native.log"
printf 'END_AUTHENTICATED_EXISTING_RUST_SUMMARY\n'
