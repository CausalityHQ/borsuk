#!/usr/bin/env bash
# EC2-only opaque evidence publication and exact owned-unit cleanup.
set -euo pipefail
[[ $# == 1 && $1 == /mnt/borsuk-source-width64 ]] || exit 125
root=$1
phases=(admission-candidate completed-admission-candidate canary-candidate completed-canary-candidate
 formal-candidate-b1 completed-formal-candidate-b1)
mkdir -m 700 "$root/evidence/terminal"
finish() {
 original=$?
 trap - EXIT
 set +e
 ev=$root/evidence/terminal
 printf '%s\n' "$original" > "$ev/original-campaign.exit"
 cleanup=0; forced=false
 cleanup_deadline=$(( $(date +%s)+120 ))
 for phase in "${phases[@]}"; do
  unit=borsuk-source-width64-$phase.service
  cg=/sys/fs/cgroup/system.slice/$unit
  if [[ -d $cg ]] && ! grep -qx 'populated 0' "$cg/cgroup.events"; then
   forced=true
   printf '1\n' > "$cg/cgroup.kill" || cleanup=1
   left=$((cleanup_deadline-$(date +%s)-2))
   if ((left>0)); then
    ((left<=5)) || left=5
    /usr/bin/timeout -k 2 "$left" systemctl stop "$unit" > "$ev/$phase.stop.stdout" 2> "$ev/$phase.stop.stderr" || cleanup=1
   else cleanup=1; fi
  fi
  if [[ -d $cg ]] && ! grep -qx 'populated 0' "$cg/cgroup.events"; then cleanup=1; fi
 done
 decision=INVALID
 if [[ $original == 0 && $cleanup == 0 && $forced == false && -f $root/evidence/width64.status ]]; then
  if [[ $(cat "$root/evidence/width64.status") == FIXED128_CPU_PROFILE_AND_NATIVE_REDUCTION_CLOSED ]]; then decision=CPU_PROFILE_CLOSED; else cleanup=1; fi
 fi
 terminal=$original
 [[ $cleanup == 0 && $forced == false ]] || terminal=70
 if [[ $decision == INVALID && $terminal == 0 ]]; then terminal=70; fi
 if ! jq -n --arg decision "$decision" --argjson original "$original" --argjson terminal "$terminal" --argjson cleanup "$cleanup" --argjson forced "$forced" '{schema:"borsuk-source-width64-cpu-profile-terminal-v1",decision:$decision,original_campaign_exit:$original,terminal_exit:$terminal,cleanup_error:$cleanup,forced_cleanup:$forced,compute_termination_pending:true,cold_performance_claim:false,vendor_win_claim:false,product_ready:false}' > "$ev/terminal.json.tmp"; then terminal=71;
 elif ! mv "$ev/terminal.json.tmp" "$ev/terminal.json"; then terminal=71; fi
 /usr/bin/timeout -k 2 30 tar -czf "$root/evidence.tar.gz" -C "$root" evidence results dynamic perf-admission perf.path perf.sha256 || terminal=71
 if [[ -f $root/evidence.tar.gz && $(stat -c %s "$root/evidence.tar.gz") -le 335544320 ]]; then
  sha256sum "$root/evidence.tar.gz" > "$root/evidence.tar.gz.sha256" || terminal=71
  sync -f "$root/evidence.tar.gz" || terminal=71
  sync -f "$root/evidence.tar.gz.sha256" || terminal=71
  sync -f "$root" || terminal=71
  export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
  publication_deadline=$(( $(date +%s)+120 ))
  for file in evidence.tar.gz evidence.tar.gz.sha256; do
   left=$((publication_deadline-$(date +%s)-2))
   if ((left>0)); then
    ((left<=60)) || left=60
    /usr/bin/timeout -k 2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "research/semantic-router/20261013/source-width64-cpu-profile-a0001/terminal/$file" --body "$root/$file" --if-none-match '*' > "$root/$file.put.json" 2> "$root/$file.put.stderr" || terminal=71
   else terminal=71; fi
  done
 else terminal=71; fi
 printf '%s\n' "$terminal" > "$root/terminal.exit" || terminal=71
 /usr/bin/timeout -k 2 15 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261013/source-width64-cpu-profile-a0001/terminal/terminal.exit --body "$root/terminal.exit" --if-none-match '*' > "$root/terminal.put.json" 2> "$root/terminal.put.stderr" || terminal=71
 exit "$terminal"
}
trap finish EXIT
bash "$root/run-width64-campaign.sh" "$root"
