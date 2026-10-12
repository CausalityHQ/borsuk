#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 && $1 == /mnt/borsuk-spatial-full1000 ]] || exit 125
root=$1
mkdir -m 700 "$root/evidence/terminal"
finish() {
 original=$?
 trap - EXIT
 set +e
 ev=$root/evidence/terminal
 printf '%s\n' "$original" > "$ev/original-campaign.exit"
 cleanup=0; forced=false
 # Exact names on this campaign's fresh disposable host only.
 for stage in admission canary formal; do
  for arm in radial spatial; do
   for phase in "$stage-$arm" "completed-$stage-$arm"; do
    unit=borsuk-full1000-$phase.service
    cg=/sys/fs/cgroup/system.slice/$unit
    if [[ -d $cg ]] && ! grep -qx 'populated 0' "$cg/cgroup.events"; then
     forced=true
     printf '1\n' > "$cg/cgroup.kill" || cleanup=1
     /usr/bin/timeout -k 2 5 systemctl stop "$unit" > "$ev/$phase.stop.stdout" 2> "$ev/$phase.stop.stderr" || cleanup=1
    fi
    if [[ -d $cg ]] && ! grep -qx 'populated 0' "$cg/cgroup.events"; then cleanup=1; fi
   done
  done
 done
 decision=INVALID
 if [[ $original == 0 && $cleanup == 0 && $forced == false && -f $root/evidence/full1000.status ]]; then
  if [[ $(cat "$root/evidence/full1000.status") == BOTH_FULL1000_NATIVE_OUTPUTS_VALIDATED_COLD_AND_VENDOR_INTERPRETATION_PENDING ]]; then decision=FULL1000_MEASURED; else cleanup=1; fi
 fi
 terminal=$original
 [[ $cleanup == 0 && $forced == false ]] || terminal=70
 if [[ $decision == INVALID && $terminal == 0 ]]; then terminal=70; fi
 jq -n --arg decision "$decision" --argjson original "$original" --argjson terminal "$terminal" --argjson cleanup "$cleanup" --argjson forced "$forced" '{schema:"borsuk-full1000-spatial-campaign-terminal-v1",decision:$decision,original_campaign_exit:$original,terminal_exit:$terminal,cleanup_error:$cleanup,forced_cleanup:$forced,compute_termination_pending:true,cold_performance_claim:false,product_ready:false}' > "$ev/terminal.json.tmp"
 if [[ $? != 0 ]] || ! mv "$ev/terminal.json.tmp" "$ev/terminal.json"; then terminal=71; fi
 # Include closed native results, never requests/truth or source payload bodies.
 /usr/bin/timeout -k 2 30 tar -czf "$root/evidence.tar.gz" -C "$root" evidence results dynamic || terminal=71
 if [[ -f $root/evidence.tar.gz && $(stat -c %s "$root/evidence.tar.gz") -le 67108864 ]]; then
  sha256sum "$root/evidence.tar.gz" > "$root/evidence.tar.gz.sha256" || terminal=71
  sync -f "$root/evidence.tar.gz" || terminal=71
  sync -f "$root/evidence.tar.gz.sha256" || terminal=71
  export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
  for file in evidence.tar.gz evidence.tar.gz.sha256; do
   /usr/bin/timeout -k 2 60 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "research/semantic-router/20261012/full1000-spatial-cold-a0001/terminal/$file" --body "$root/$file" --if-none-match '*' > "$root/$file.put.json" 2> "$root/$file.put.stderr" || terminal=71
  done
 else terminal=71; fi
 printf '%s\n' "$terminal" > "$root/terminal.exit"
 /usr/bin/timeout -k 2 15 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261012/full1000-spatial-cold-a0001/terminal/terminal.exit --body "$root/terminal.exit" --if-none-match '*' > "$root/terminal.put.json" 2> "$root/terminal.put.stderr" || terminal=71
 exit "$terminal"
}
trap finish EXIT
bash "$root/run-full1000-campaign.sh" "$root"
