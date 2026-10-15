#!/bin/bash
# Disposable CLI publication/readback/cleanup only; native reads are separate.
set -euo pipefail
[[ $# == 0 && $EUID == 0 ]] || exit 125
root=/mnt/borsuk-v9-panel
key=research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/v9-source-shortlist-cold-a0001/smoke-probe.json
ev=$root/evidence/smoke-object
mkdir -m 700 "$ev"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
timeout -k 2 15 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --body "$root/config/candidate-generation.json" --if-none-match '*' > "$ev/create.put.json"
etag=$(jq -er '.ETag|select(type=="string")' "$ev/create.put.json")
timeout -k 2 15 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --if-match "$etag" "$ev/readback.json" > "$ev/readback.get.json"
cmp "$root/config/candidate-generation.json" "$ev/readback.json"
timeout -k 2 15 aws s3api delete-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$ev/delete.json"
set +e
timeout -k 2 15 aws s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$ev/absent.stdout" 2> "$ev/absent.stderr"
original=$?
set -e
printf '%s\n' "$original" > "$ev/absent.exit"
[[ $original == 254 ]] || exit 1
grep -Fq '(404)' "$ev/absent.stderr" || exit 1
jq -n '{schema:"borsuk-v9-disposable-object-smoke-v1",status:"CREATED_READBACK_DELETED",operations:"real_AWS_CLI_only",native_publication_claim:false,performance_claim:false}' > "$ev/smoke.json"
