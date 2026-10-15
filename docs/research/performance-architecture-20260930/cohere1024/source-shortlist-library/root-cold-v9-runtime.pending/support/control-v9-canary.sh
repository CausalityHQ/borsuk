#!/bin/bash
# EC2 source-bound systemd control canary. No ANN or scientific input opens.
set -euo pipefail
[[ $# == 0 && $EUID == 0 ]] || exit 125
campaign=/mnt/borsuk-v9-panel
testroot=$campaign/control-canary
mkdir -m 700 "$testroot"
mkdir -m 700 "$testroot/evidence" "$testroot/chain.claim"
checks_complete=false
finish() {
 original=$?; trap - EXIT
 set +e
 for name in canary-exit17 canary-client-timeout; do
  state=$(/usr/bin/timeout -k 2 5 systemctl show "borsuk-v9-prep-$name.service" -p ActiveState --value)
  cg=/sys/fs/cgroup/system.slice/borsuk-v9-prep-$name.service
  if [[ $state != inactive && $state != failed ]] || { [[ -d $cg ]] && ! grep -qx 'populated 0' "$cg/cgroup.events"; }; then
   touch "$campaign/evidence/cleanup-unverified"; original=125
  fi
 done
 if [[ -e $testroot/evidence/cleanup-unverified ]]; then touch "$campaign/evidence/cleanup-unverified"; original=125; fi
 if [[ $original == 0 && $checks_complete == true ]]; then
  jq -n '{schema:"borsuk-v9-control-canary-v1",status:"CONTROL_CANARY_PASS",real_systemd:true,original_prep_exit:17,client_timeout_exit:124,duplicate_evidence_unchanged:true,absence_predicate_inputs_mocked:true,ann_executed:false,scientific_bodies_opened:false,performance_claim:false}' > "$campaign/evidence/control-canary.json" || exit 126
 fi
 cp -r "$testroot" "$campaign/evidence/control-canary"
 copied=$?
 if ((copied!=0)); then exit 126; fi
 exit "$original"
}
trap finish EXIT
cp "$campaign/machine-stop.epoch" "$testroot/machine-stop.epoch"
sha256sum "$campaign/run-v9-chain.sh" "$campaign/smoke-v9-object.sh" > "$testroot/source.sha256"
sed -n '/^owned() {$/,/^}$/p' "$campaign/run-v9-chain.sh" > "$testroot/owned-original.sh"
[[ $(grep -c '^owned() {$' "$testroot/owned-original.sh") == 1 && $(tail -n 1 "$testroot/owned-original.sh") == '}' ]] || exit 125
cp "$testroot/owned-original.sh" "$testroot/owned-client-timeout.sh"
# Only the client deadline is shortened; unit properties/ownership/drain stay exact.
perl -0777 -i -pe 'BEGIN{$n=0} $n+=s/"\$\(\(cap\+40\)\)"/"2"/g; END{die "client deadline anchor" unless $n==1}' "$testroot/owned-client-timeout.sh"
cat > "$testroot/invoke.sh" <<'SH'
#!/bin/bash
set -euo pipefail
root=$1; functions=$2; name=$3; cap=$4; command=$5
source "$functions"
owned "$name" "$cap" /bin/bash -c "$command"
SH
set +e
/usr/bin/timeout -k 2 40 /bin/bash "$testroot/invoke.sh" "$testroot" "$testroot/owned-original.sh" canary-exit17 5 'exit 17' > "$testroot/exit17.log" 2>&1
original=$?
set -e
printf '%s\n' "$original" > "$testroot/exit17.exit"
[[ $original == 17 ]]
jq -e '.manager_exit==17 and .owned_cgroup_drained==true and .forced_cleanup==true' "$testroot/evidence/prep-canary-exit17.supervision.json" >/dev/null
set +e
/usr/bin/timeout -k 2 40 /bin/bash "$testroot/invoke.sh" "$testroot" "$testroot/owned-client-timeout.sh" canary-client-timeout 10 'sleep 30' > "$testroot/client-timeout.log" 2>&1
original=$?
set -e
printf '%s\n' "$original" > "$testroot/client-timeout.exit"
[[ $original == 124 ]]
jq -e '.manager_exit==124 and .owned_cgroup_drained==true and .forced_cleanup==true' "$testroot/evidence/prep-canary-client-timeout.supervision.json" >/dev/null
[[ ! -e $testroot/evidence/cleanup-unverified ]]
# Isolate the exact chain by replacing its single fixed root literal only.
perl -pe 'BEGIN{$n=0} $n+=s!^root=/mnt/borsuk-v9-panel$!root=/mnt/borsuk-v9-panel/control-canary!; END{die "root anchor" unless $n==1}' "$campaign/run-v9-chain.sh" > "$testroot/duplicate-chain.sh"
printf 'original-evidence\n' > "$testroot/evidence/sentinel.txt"
(cd "$testroot/evidence" && find . -type f -print0 | sort -z | xargs -0 sha256sum) > "$testroot/evidence.before.sha256"
set +e
/usr/bin/timeout -k 2 5 /bin/bash "$testroot/duplicate-chain.sh" > "$testroot/duplicate.log" 2>&1
original=$?
set -e
printf '%s\n' "$original" > "$testroot/duplicate.exit"
[[ $original != 0 && ! -e $testroot/evidence/chain-terminal.json ]]
(cd "$testroot/evidence" && sha256sum --strict -c ../evidence.before.sha256) > "$testroot/duplicate-evidence-check.log"
awk '/^absent[(][)] [{]$/{on=1} on{print} on && /^[}]$/{exit}' "$campaign/smoke-v9-object.sh" > "$testroot/absence-predicate.sh"
[[ $(wc -l < "$testroot/absence-predicate.sh") == 3 ]]
mkdir "$testroot/predicate"
printf 'An error occurred (404) when calling the HeadObject operation: Not Found\n' > "$testroot/predicate/absent.stderr"
for value in 0 124 254; do
 set +e
 /bin/bash -c 'set -euo pipefail; source "$3"; absent "$1" "$2/absent.stderr"' bash "$value" "$testroot/predicate" "$testroot/absence-predicate.sh"
 observed=$?
 set -e
 printf '%s %s\n' "$value" "$observed" >> "$testroot/predicate.out"
 if [[ $value == 254 ]]; then [[ $observed == 0 ]]; else [[ $observed != 0 ]]; fi
done
printf 'AccessDenied\n' > "$testroot/predicate/absent.stderr"
set +e
/bin/bash -c 'set -euo pipefail; source "$2"; absent 254 "$1/absent.stderr"' bash "$testroot/predicate" "$testroot/absence-predicate.sh"
observed=$?
set -e
[[ $observed != 0 ]]
sha256sum --strict -c "$testroot/source.sha256" > "$testroot/source-after.log"
checks_complete=true
