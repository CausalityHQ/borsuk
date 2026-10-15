#!/bin/bash
# Disposable CLI smoke. No ANN: preserve failures, delete only owned probe.
set -euo pipefail
[[ $# == 0 && $EUID == 0 ]] || exit 125
root=/mnt/borsuk-v9-panel
ev=$root/evidence/smoke-object
mkdir -m 700 "$ev"
token=$(cat /proc/sys/kernel/random/uuid)
[[ $token =~ ^[a-f0-9-]{36}$ ]] || exit 125
key=research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/v9-source-shortlist-cold-a0002/smoke-$token.json
printf '%s\n' "$key" > "$ev/key.txt"
printf '%s\n' "$token" > "$ev/owner-token.txt"
manifest=$root/config/candidate-generation.json
[[ -f $manifest && ! -L $manifest ]] || exit 125
printf '%s  %s\n' 460d095ac98f2dba70ff765907ba89d3bd7bbdb03e1cb4781da637ef5d19baf6 "$manifest" | sha256sum --strict -c - > "$ev/body-pin.log"
deadline=$(( $(date +%s)+120 ))
work_deadline=$((deadline-40))
stop=$(cat "$root/machine-stop.epoch")
((deadline<stop-300)) || exit 125
attempted=false
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
aws_at() {
 local bound=$1 cap=$2 left; shift 2
 left=$((bound-$(date +%s)-2)); ((left>0)) || return 125
 ((left<=cap)) || left=$cap
 /usr/bin/timeout -k 2 "$left" aws --cli-connect-timeout 5 --cli-read-timeout 12 "$@"
}
absent() {
 [[ $1 == 254 ]] && grep -Fq '(404)' "$2"
}
cleanup() {
 local rc etag
 if [[ $attempted != true ]]; then return 0; fi
 aws_at "$deadline" 10 s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$ev/cleanup-head.json" 2> "$ev/cleanup-head.stderr"
 rc=$?
 if absent "$rc" "$ev/cleanup-head.stderr"; then return 0; fi
 [[ $rc == 0 ]] || return 125
 jq -e --arg token "$token" '.Metadata["borsuk-smoke-owner"]==$token' "$ev/cleanup-head.json" >/dev/null || return 125
 etag=$(jq -er '.ETag|select(type=="string" and test("^\"[a-f0-9-]+\"$"))' "$ev/cleanup-head.json") || return 125
 aws_at "$deadline" 10 s3api delete-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --if-match "$etag" > "$ev/delete.json" 2> "$ev/delete.stderr" || return "$?"
 aws_at "$deadline" 10 s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$ev/absent.stdout" 2> "$ev/absent.stderr"
 rc=$?
 printf '%s\n' "$rc" > "$ev/absent.exit"
 absent "$rc" "$ev/absent.stderr"
}
finish() {
 original=$?; trap - EXIT
 set +e
 cleanup
 clean=$?
 printf '%s\n' "$original" > "$ev/original.exit"
 printf '%s\n' "$clean" > "$ev/cleanup.exit"
 printf 'SMOKE_KEY=%s ORIGINAL=%s CLEANUP=%s\n' "$key" "$original" "$clean"
 if ((clean!=0)); then touch "$root/evidence/smoke-object-cleanup-unverified"; fi
 if ((original==0 && clean==0)); then
  jq -n '{schema:"borsuk-v9-disposable-object-smoke-v2",status:"CREATED_READBACK_DELETED",operations:"real_AWS_CLI_only",ownership_token_verified:true,cleanup_verified:true,native_publication_claim:false,performance_claim:false}' > "$ev/smoke.json" || exit 126
 else
  jq -n --argjson original "$original" --argjson cleanup "$clean" '{schema:"borsuk-v9-disposable-object-smoke-v2",status:"FAILED",original_exit:$original,cleanup_exit:$cleanup,native_publication_claim:false,performance_claim:false}' > "$ev/smoke.failure.json"
 fi
 if ((original!=0)); then exit "$original"; fi
 ((clean==0)) || exit 125
 exit 0
}
trap finish EXIT
set +e
aws_at "$work_deadline" 15 s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$ev/pre-head.json" 2> "$ev/pre-head.stderr"
pre=$?
set -e
absent "$pre" "$ev/pre-head.stderr" || exit 125
attempted=true
aws_at "$work_deadline" 15 s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --body "$manifest" --metadata "borsuk-smoke-owner=$token" --if-none-match '*' > "$ev/create.put.json"
etag=$(jq -er '.ETag|select(type=="string")' "$ev/create.put.json")
aws_at "$work_deadline" 15 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --if-match "$etag" "$ev/readback.json" > "$ev/readback.get.json"
cmp "$manifest" "$ev/readback.json"
