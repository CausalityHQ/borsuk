#!/bin/bash
set -euo pipefail
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
root=/mnt/borsuk-scale1m
ev="$root/evidence/independent32-failure-preservation"
mkdir "$ev"
result="$root/closed/s3/independent32-result.jsonl"
test -f "$result"
# Export only the original Rust terminal record; do not inspect partial query rows.
tail -n 1 "$result" > "$ev/native-terminal-record.json"
jq -e '.phase=="terminal" and (.summary.status=="INVALID" or .summary.status=="RESOURCE_REJECT")' "$ev/native-terminal-record.json" > "$ev/terminal-shape-check.txt"
cp --no-clobber "$result" "$ev/independent32-incomplete-result.jsonl"
cp --no-clobber "$root/closed/s3/independent32-config.json" "$ev/independent32-config.json"
cp --no-clobber "$root/evidence/independent32/terminal.json" "$ev/stage-terminal.json"
(cd "$ev"; find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
(cd "$ev"; sha256sum -c SHA256SUMS) > "$root/evidence/independent32-failure-preservation-manifest-checked.log"
for f in "$ev"/*; do timeout --kill-after=2 25 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261011/scale-closure-panels32-a0001/stages/independent32-failure-preservation/"$(basename "$f")" --body "$f" --if-none-match '*'; done
cat "$ev/native-terminal-record.json"
