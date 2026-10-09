#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
root=/mnt/borsuk-admission-smoke
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261009/actual1m-q32-admission-smoke-a0002
cd "$root"
for ((i=0;i<600;i++)); do [[ $(systemctl show cloud-final.service -p MainPID --value) == 0 ]] && break; sleep .1; done
[[ $(systemctl show cloud-final.service -p MainPID --value) == 0 ]]
set +e
systemd-run --unit=borsuk-smoke --slice=borsuk-smoke.slice --wait --pipe -p PrivateNetwork=yes -p RuntimeMaxSec=420 -p TimeoutStopSec=5 -p KillMode=control-group -p "ExecStopPost=/bin/bash $root/sources/service-stop.sh" timeout -k 5 405 bash "$root/cases.sh" "$root/sources" "$root/results" > driver.log 2>&1
rc=$?
set -e
printf '%s\n' "$rc" > original-unit.exit
quiescent_unit() {
 local unit=$1 state main control
 for ((q=0;q<200;q++)); do
  state=$(timeout -k 2 10 systemctl show "$unit" -p ActiveState --value) || return 1
  main=$(timeout -k 2 10 systemctl show "$unit" -p MainPID --value) || return 1
  control=$(timeout -k 2 10 systemctl show "$unit" -p ControlPID --value) || return 1
  [[ $state == inactive || $state == failed ]] && [[ $main == 0 && $control == 0 ]] && return 0
  sleep .05
 done
 return 1
}
stop_owned() {
 local unit=$1 load
 load=$(timeout -k 2 10 systemctl show "$unit" -p LoadState --value) || return 1
 [[ $load == not-found ]] && return 0
 timeout -k 2 15 systemctl stop "$unit" || return 1
 quiescent_unit "$unit"
}
for unit in borsuk-cohort-parity.service borsuk-cohort-transport.service borsuk-smoke.service cloud-final.service; do stop_owned "$unit"; done
mkdir evidence
cp driver.log original-unit.exit sources/source.sha256 cases.sh collector.sh bootstrap.log evidence/
if [[ -d results ]]; then cp -a results evidence/; fi

if (( rc != 0 )); then
 mkdir evidence/failure-current
 for name in run.log shutdown.calls terminal.json final.exit; do
  body=/mnt/borsuk-scale1m/$name
  if [[ -f $body && ! -L $body ]]; then
   bytes=$(stat -c %s "$body"); digest=$(sha256sum "$body"); digest=${digest%% *}
   jq -n --arg name "$name" --arg sha "$digest" --argjson bytes "$bytes" '{name:$name,bytes:$bytes,sha256:$sha,scope:"quiesced inert smoke fixture; prefix may be truncated"}' > "evidence/failure-current/$name.meta.json"
   head -c 4096 "$body" > "evidence/failure-current/$name.prefix"
  fi
 done
fi
if [[ -f /mnt/borsuk-scale1m/evidence-root/service-exit.json ]]; then cp /mnt/borsuk-scale1m/evidence-root/service-exit.json evidence/driver-manager-exit.json; fi
(cd evidence && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum) > artifacts.sha256
(ulimit -f 65536; tar -czf evidence.tar.gz -C evidence .)
bytes=$(stat -c %s evidence.tar.gz); digest=$(sha256sum evidence.tar.gz); digest=${digest%% *}
[[ $bytes -gt 0 && $bytes -le 67108864 ]]
timeout -k 5 60 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" --body evidence.tar.gz --if-none-match '*' > evidence.put.json
timeout -k 5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/artifacts.sha256" --body artifacts.sha256 --if-none-match '*' > manifest.put.json
jq -n --arg instance "$(cat instance.txt)" --arg sha "$digest" --argjson bytes "$bytes" --argjson exit "$rc" '{schema:"borsuk-parity-smoke-closed-v1",instance_id:$instance,original_unit_exit:$exit,evidence:{bytes:$bytes,sha256:$sha},acceptance:"ROOT_REPLAY_REQUIRED",performance_claim:false}' > terminal.json
cp terminal.json /mnt/borsuk-scale1m/terminal.json
timeout -k 5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" --body terminal.json --if-none-match '*'
printf '0\n' > /mnt/borsuk-scale1m/final.exit
sync -f /mnt/borsuk-scale1m/final.exit
shutdown -h +1
