#!/usr/bin/env bash
# Preserve the original campaign exit; completed objects are never deleted.
set -euo pipefail
[[ $# == 1 && $1 == /mnt/borsuk-retained-returned-publication ]] || exit 125
root=$1
[[ -f $root/input-files.sha256 && -f $root/machine-stop.epoch ]] || exit 125
out=$root/evidence/terminal
mkdir -m 700 "$out"
finish() {
 original=$?
 trap - EXIT
 set +e
 printf '%s\n' "$original" > "$out/original-campaign.exit"
 cleanup=0
 forced=false
 deadline=$(( $(date +%s)+170 ))
 terminal_call() {
  local cap=$1 left
  shift
  left=$((deadline-$(date +%s)-2))
  ((left>0)) || return 125
  ((left<=cap)) || left=$cap
  /usr/bin/timeout -k 2 "$left" "$@"
 }
 # Only these preregistered units belong to this campaign.
 for phase in negative-approval negative-membership admission-spatial canary-copy canary-replay final-radial final-spatial; do
  unit=borsuk-publication-$phase.service
  cg=/sys/fs/cgroup/system.slice/$unit
  terminal_call 5 systemctl show "$unit" -p ActiveState -p SubState -p Result -p ControlGroup > "$out/$phase.show" 2> "$out/$phase.show.stderr"
  if [[ -d $cg ]]; then
   if ! grep -qx 'populated 0' "$cg/cgroup.events"; then
    forced=true
    printf '1\n' > "$cg/cgroup.kill" || cleanup=1
    terminal_call 10 systemctl stop "$unit" > "$out/$phase.stop.stdout" 2> "$out/$phase.stop.stderr" || cleanup=1
   fi
   if [[ -d $cg ]]; then
    cat "$cg/cgroup.events" > "$out/$phase.cgroup.events" || cleanup=1
    grep -qx 'populated 0' "$out/$phase.cgroup.events" || cleanup=1
   fi
  fi
 done
 # Inventory even when no receipt exists: a lost PUT response can leave objects.
 export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
 namespace=research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic
 for dest in publication-admission-spatial-a0001 publication-negative-membership-a0001 publication-canary-spatial-a0001 rebound-radial-returned-a0001 rebound-spatial-returned-a0001 publication-canary-assets-a0001; do
  terminal_call 12 aws --region eu-central-1 s3api list-objects-v2 --bucket borsuk-bench-453182569524-euc1 --prefix "$namespace/$dest/" --no-paginate > "$out/$dest.objects.json" 2> "$out/$dest.objects.stderr"
  rc=$?; printf '%s\n' "$rc" > "$out/$dest.objects.exit"
  if [[ $rc != 0 ]] || ! jq -e '.IsTruncated==false' "$out/$dest.objects.json" > /dev/null; then cleanup=1; fi
  terminal_call 12 aws --region eu-central-1 s3api list-multipart-uploads --bucket borsuk-bench-453182569524-euc1 --prefix "$namespace/$dest/" --no-paginate > "$out/$dest.uploads.json" 2> "$out/$dest.uploads.stderr"
  rc=$?; printf '%s\n' "$rc" > "$out/$dest.uploads.exit"
  # Unknown upload IDs are preserved for exact-key root disposition, never adopted.
  if [[ $rc != 0 ]] || ! jq -e '.IsTruncated==false and ((.Uploads//[])|length)==0' "$out/$dest.uploads.json" > /dev/null; then cleanup=1; fi
 done
 terminal=$original
 if [[ $cleanup != 0 || $forced == true ]]; then terminal=70; fi
 jq -n --argjson original "$original" --argjson terminal "$terminal" --argjson cleanup "$cleanup" --argjson forced "$forced" '{schema:"borsuk-retained-publication-campaign-terminal-v1",original_campaign_exit:$original,terminal_exit:$terminal,closure_error:$cleanup,forced_cleanup:$forced,compute_termination_pending:true,publication_accepted:false,performance_claim:false}' > "$out/terminal.json.tmp"
 if [[ $? != 0 ]] || ! mv "$out/terminal.json.tmp" "$out/terminal.json"; then terminal=71; fi
 (cd "$out" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$out/SHA256SUMS" || terminal=71
 /usr/bin/sync -f "$out/terminal.json" || terminal=71
 /usr/bin/sync -f "$out/SHA256SUMS" || terminal=71
 /usr/bin/sync -f "$out" || terminal=71
 exit "$terminal"
}
trap finish EXIT
bash "$root/run-publication-campaign.sh" "$root"
