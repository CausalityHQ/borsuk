#!/bin/bash
# EC2-only terminal-cell evidence publication; scientific rows remain opaque.
set -euo pipefail
[[ $# == 1 && $EUID == 0 ]] || exit 125
name=$1
case $name in formal-original-a1|formal-candidate-b1|formal-candidate-b2|formal-original-a2) ;; *) exit 125;; esac
root=/mnt/borsuk-v9-panel
ev=$root/evidence/publish-$name
mkdir -m 700 "$ev"
result=$root/results/$name.jsonl
report=$root/results/completed-$name.json
for file in "$result" "$report"; do [[ -f $file && ! -L $file ]] || exit 125; done
bytes=$(stat -c %s "$result"); ((bytes>0 && bytes<=67108864)) || exit 125
(( $(stat -c %s "$report")>0 && $(stat -c %s "$report")<=1048576 )) || exit 125
sha=$(sha256sum "$result" | cut -d ' ' -f 1)
report_sha=$(sha256sum "$report" | cut -d ' ' -f 1)
jq -e --arg sha "$sha" --argjson bytes "$bytes" '.status=="MEASURED" and .complete==true and .population_percentiles_valid==true and .native_terminal.selected_count==1000 and .result_sha256==$sha and .result_bytes==$bytes' "$report" >/dev/null
jq -n --slurpfile native "$root/evidence/$name/supervision.json" --slurpfile completed "$root/evidence/completed-$name/supervision.json" '{native:$native[0],completed:$completed[0]}' > "$ev/supervision.json"
jq -e 'all(.[]; .execution_closed==true and .manager_exit==0 and .native_exit==0 and .timeout_wrapper_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false)' "$ev/supervision.json" >/dev/null
proof_sha=$(sha256sum "$ev/supervision.json" | cut -d ' ' -f 1)
support=$(jq -er '.support_sha256|select(test("^[a-f0-9]{64}$"))' "$root/evidence/setup.json")
stop=$(cat "$root/machine-stop.epoch")
deadline=$(( $(date +%s)+120 ))
(( deadline<stop-300 )) || exit 125
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
put() {
 local file=$1 leaf=$2 cap=$3 left
 left=$((deadline-$(date +%s)-2)); ((left>0)) || return 125
 ((left<=cap)) || left=$cap
 /usr/bin/timeout -k 2 "$left" aws --cli-connect-timeout 5 --cli-read-timeout 40 s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "research/semantic-router/20261015/source-shortlist-v9-cold-a0003/cells/$name/$leaf" --body "$file" --if-none-match '*' > "$ev/$leaf.put.json"
}
put "$result" result.jsonl 45
put "$report" report.json 25
put "$ev/supervision.json" supervision.json 25
printf '%s  %s\n%s  %s\n' "$sha" "$result" "$report_sha" "$report" | sha256sum --strict -c - > "$ev/result-after-publication.sha-check.log"
jq -n --arg name "$name" --arg sha "$sha" --argjson bytes "$bytes" --arg report "$report_sha" --arg proof "$proof_sha" --arg support "$support" '{schema:"borsuk-v9-cold-cell-terminal-v1",status:"CELL_CLOSED",name:$name,result_sha256:$sha,result_bytes:$bytes,report_sha256:$report,supervision_sha256:$proof,support_sha256:$support,complete:true,full_queries:1000,whole_campaign_complete:false,performance_pass_claim:false,vendor_win_claim:false}' > "$ev/terminal.json"
sync -f "$ev/terminal.json"
sync -f "$ev"
put "$ev/terminal.json" terminal.json 25
