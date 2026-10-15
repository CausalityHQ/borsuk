#!/bin/bash
# Called once by the chain EXIT trap, before the root watcher terminates EC2.
set -euo pipefail
[[ $# == 2 && $1 =~ ^[0-9]{1,3}$ && $2 =~ ^[a-z][a-z0-9-]*$ ]] || exit 125
original=$1; phase=$2; root=/mnt/borsuk-v9-panel
prefix=research/semantic-router/20261015/source-shortlist-v9-actual-a0001/terminal
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
[[ ! -e $root/evidence/cleanup-unverified ]] || exit 125
support=$(jq -er '.support_sha256|select(test("^[a-f0-9]{64}$"))' "$root/evidence/setup.json")
[[ ! -e $root/evidence/chain-terminal.json ]] || exit 125
jq -n --arg support "$support" --argjson original "$original" --arg phase "$phase" '{schema:"borsuk-v9-actual-correctness-chain-v1",source_commit:"ee598553b227660d9e78500086411481456e1a0e",support_archive_sha256:$support,original_exit:$original,last_phase:$phase,execution_closed:($original==0),cold_performance_claim:false,vendor_win_claim:false,compute_closure_pending:true}' > "$root/evidence/chain-terminal.json"
(cd "$root" && find evidence results config -type f -print0 | sort -z | xargs -0 sha256sum) > "$root/SHA256SUMS"
sync -f "$root/evidence/chain-terminal.json"
sync -f "$root/SHA256SUMS"
sync -f "$root/evidence"
tar -czf "$root/evidence.tar.gz" -C "$root" evidence results config SHA256SUMS
bytes=$(stat -c %s "$root/evidence.tar.gz")
(( bytes>0 && bytes<=16777216 )) || exit 125
sha=$(sha256sum "$root/evidence.tar.gz"); sha=${sha%% *}
timeout -k 2 90 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/evidence.tar.gz" --body "$root/evidence.tar.gz" --if-none-match '*' > "$root/archive.put.json"
jq -n --arg support "$support" --arg sha "$sha" --argjson bytes "$bytes" --argjson original "$original" --arg phase "$phase" '{schema:"borsuk-v9-actual-terminal-publication-v1",source_commit:"ee598553b227660d9e78500086411481456e1a0e",support_archive_sha256:$support,archive_sha256:$sha,archive_bytes:$bytes,original_exit:$original,last_phase:$phase,execution_closed:($original==0),cold_performance_claim:false,vendor_win_claim:false,compute_closure_pending:true}' > "$root/terminal.json"
timeout -k 2 25 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/terminal.json" --body "$root/terminal.json" --if-none-match '*' > "$root/terminal.put.json"
cat "$root/terminal.json"
