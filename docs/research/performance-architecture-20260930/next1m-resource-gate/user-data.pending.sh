#!/usr/bin/env bash
# DRAFT: exact asset SHA placeholders must be frozen before any launch.
set -Eeuo pipefail
umask 077
root=/mnt/borsuk-scale1m
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261009/actual1m-q32-admission-a0001
mkdir -p "$root"
cd "$root"
mkdir evidence-root
phase=bootstrap
instance=unknown
boot_epoch=$(date +%s)
local_stop_epoch=$((boot_epoch + 3900))
finish() {
 original=$?; trap - EXIT TERM INT HUP PIPE; set +e
 status=$original
 # Seal only after every owned service is inactive; stop failure leaves no success publication.
 for unit in borsuk-cohort-transport.service borsuk-cohort-parity.service; do
  state=$(timeout -k 2 10 systemctl show "$unit" -p ActiveState --value) || { shutdown -h now; exit 96; }
  if [[ $state == active || $state == activating || $state == deactivating ]]; then
   timeout -k 2 30 systemctl stop "$unit" || { shutdown -h now; exit 96; }
  fi
  state=$(timeout -k 2 10 systemctl show "$unit" -p ActiveState --value) || { shutdown -h now; exit 96; }
  [[ $state != active && $state != activating && $state != deactivating ]] || { shutdown -h now; exit 96; }
 done
 # Owned services have closed their inherited pipe writers before logger collection.
 exec >/dev/null 2>&1
 if [[ -n ${log_pid:-} ]]; then wait "$log_pid" || status=96; fi
 systemctl show borsuk-cohort-parity.service > evidence-root/systemd-after.txt 2>&1
 # Admit every quiesced evidence body before copying it.
 log_bytes=$(stat -c %s run.log) || { shutdown -h now; exit 96; }
 existing_bytes=$(du -sb evidence-root | awk '{print $1}') || { shutdown -h now; exit 96; }
 local_bytes=0
 if [[ -d evidence-local ]]; then local_bytes=$(du -sb evidence-local | awk '{print $1}') || { shutdown -h now; exit 96; }; fi
 [[ $log_bytes =~ ^[0-9]+$ && $existing_bytes =~ ^[0-9]+$ && $local_bytes =~ ^[0-9]+$ ]] || { shutdown -h now; exit 96; }
 extra_bytes=0
 for body in prepared-parent/cohort/complete.json prepared-parent/cohort/truth.u64 finalized-config.json; do
  if [[ -e $body || -L $body ]]; then
   [[ -f $body && ! -L $body ]] || { shutdown -h now; exit 96; }
   body_bytes=$(stat -c %s "$body") || { shutdown -h now; exit 96; }
   [[ $body_bytes =~ ^[0-9]+$ ]] || { shutdown -h now; exit 96; }
   (( body_bytes <= 33554432 )) || { shutdown -h now; exit 96; }
   extra_bytes=$((extra_bytes + body_bytes))
  fi
 done
 (( log_bytes + existing_bytes + local_bytes + extra_bytes + 65536 <= 33554432 )) || { shutdown -h now; exit 96; }
 cp support.sha256 evidence-root/support.sha256 || status=96
 cp run.log evidence-root/bootstrap.log || status=96
 printf '%s\n' "$original" > evidence-root/bootstrap.exit || status=96
 if [[ -d evidence-local ]]; then cp -a evidence-local evidence-root/ || status=96; fi
 if [[ -d prepared-parent/cohort && -f prepared-parent/cohort/complete.json ]]; then
  cp prepared-parent/cohort/complete.json evidence-root/native-complete.json || status=96
  cp prepared-parent/cohort/truth.u64 evidence-root/actual-truth.u64 || status=96
 fi
 if [[ -f finalized-config.json ]]; then cp finalized-config.json evidence-root/ || status=96; fi
 (cd evidence-root && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum) > artifacts.sha256 || status=96
 (ulimit -f 65536; tar -czf evidence.tar.gz -C evidence-root .) || { shutdown -h now; exit 96; }
 bytes=$(stat -c %s evidence.tar.gz); digest=$(sha256sum evidence.tar.gz); digest=${digest%% *}
 [[ $bytes -gt 0 && $bytes -le 67108864 ]] || { shutdown -h now; exit 96; }
 timeout -k 5 120 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" --body evidence.tar.gz --if-none-match '*' > evidence.put.json || status=96
 timeout -k 5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/artifacts.sha256" --body artifacts.sha256 --if-none-match '*' > manifest.put.json || status=96
 jq -n --arg instance "$instance" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --arg sha "$digest" --argjson bytes "$bytes" \
 '{schema:"borsuk-actual-cohort-parity-closed-v1",instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,evidence:{bytes:$bytes,sha256:$sha},acceptance:"PROVISIONAL_REQUIRES_EXTERNAL_BOOTSTRAP_EXIT",publication_verified:false,performance_claim:false,ann_run:false}' > terminal.json || status=96
 timeout -k 5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" --body terminal.json --if-none-match '*' || status=96
 printf '%s\n' "$status" > final.exit || { shutdown -h now; exit 96; }
 sync -f final.exit || { rm -f -- final.exit; shutdown -h now; exit 96; }
 # Preserve the original absolute machine shutdown schedule; never postpone it.
 exit "$status"
}
trap finish EXIT
trap 'exit 97' TERM INT HUP
trap 'exit 98' PIPE
# Only the logger inherits this file-size cap; SDK extraction and native outputs do not.
exec > >(ulimit -f 32768; exec cat >run.log) 2>&1
log_pid=$!
# Independent machine stop remains active even if package/transport setup fails.
# Effective local stop is 3900s, leaving 300s before the external 4200s hard cap.
shutdown -h +65
# Minimal tools only: no compiler, Rust toolchain, fitting, query or ANN execution.
timeout -k 10 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
timeout -k 10 240 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip jq time python3
(ulimit -f 71412; curl -fsS --connect-timeout 10 --max-time 180 --max-filesize 73022935 -o awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip)
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
[[ $(stat -c %s awscliv2.zip) == 73022935 ]]
timeout -k 2 15 unzip -l awscliv2.zip > sdk-members.txt
sdk_expanded=$(tail -1 sdk-members.txt | awk '{print $1}')
[[ $sdk_expanded =~ ^[0-9]+$ ]] && (( sdk_expanded <= 536870912 ))
timeout -k 10 120 unzip -q awscliv2.zip
timeout -k 10 120 ./aws/install
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
token=$(curl -fsS --connect-timeout 5 --max-time 10 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
instance=$(curl -fsS --connect-timeout 5 --max-time 10 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
[[ $instance =~ ^i-[0-9a-f]+$ ]]
phase=support
# Fixed support allowlist and 64-KiB per-file write cap precede authentication.
get_support() {
 (ulimit -f 64; timeout -k 5 30 aws s3api get-object --bucket "$bucket" --key "$prefix/inputs/$1" "$1")
 [[ $(stat -c %s "$1") -le 65536 ]]
}
printf '%s  support.sha256\n' PENDING_SUPPORT_MANIFEST_SHA > expected-support.sha256
get_support support.sha256
sha256sum -c expected-support.sha256
for name in transport.py transport-pins.json config-template.json run_actual_cohort_admission.sh service-stop.sh; do
 get_support "$name"
done
sha256sum --strict -c support.sha256
awk '{print $2}' support.sha256 | LC_ALL=C sort > support.names
printf '%s\n' config-template.json run_actual_cohort_admission.sh service-stop.sh transport-pins.json transport.py | LC_ALL=C sort > expected-support.names
cmp support.names expected-support.names
# Manager records the actual cloud-final exit after the last publication command.
mkdir -p /run/systemd/system/cloud-final.service.d
printf '[Service]\nExecStopPost=/bin/bash %s/service-stop.sh bootstrap %s %s/bootstrap-manager.json\n' "$root" "$bucket" "$prefix" > /run/systemd/system/cloud-final.service.d/borsuk-exit.conf
systemctl daemon-reload
systemctl cat cloud-final.service > evidence-root/cloud-final-unit.txt
{ uname -r; cat /etc/os-release; jq --version; bash --version; systemd --version; /usr/bin/time --version; sha256sum /usr/bin/jq; } > evidence-root/environment.txt
df -B1 "$root" > evidence-root/disk-before.txt
available=$(df -B1 --output=avail "$root" | tail -1 | tr -d ' ')
[[ $available =~ ^[0-9]+$ ]] && (( available >= 15331342625 ))
phase=transport
pins_sha=$(sha256sum transport-pins.json); pins_sha=${pins_sha%% *}
set +e
systemd-run --unit=borsuk-cohort-transport --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=900 -p TimeoutStopSec=30 -p KillMode=control-group -p "ExecStopPost=/bin/bash $root/service-stop.sh transport" \
 python3 "$root/transport.py" "$root/transport-pins.json" "$pins_sha" "$root/assets"
transport_rc=$?; set -e
printf '%s\n' "$transport_rc" > evidence-root/transport-unit.exit
(( transport_rc == 0 )) || exit "$transport_rc"
jq -e ' .schema=="borsuk-parity-service-exit-v1" and .exit_code=="exited" and .exit_status=="0" and .service_result=="success" ' evidence-root/transport-exit.json
 jq -e --slurpfile pins transport-pins.json ' .body_decode==false and (.items|length)==13 and ([.items[]|{relative_path,bytes,sha256}] == [$pins[0][]|{relative_path,bytes,sha256}]) and all(.items[]; .whole_body_authenticated==true) ' assets/transport-receipt.json
chmod 0500 assets/bin/prepare_cohere_native_cohort
mkdir prepared-parent
jq --arg assets "$root/assets" --arg parent "$root/prepared-parent" \
 --argjson dev "$(stat -c %d prepared-parent)" --argjson ino "$(stat -c %i prepared-parent)" \
 '.shards |= map(.path=($assets+"/input/"+.publisher_path)) | .output_parent={path:$parent,device:$dev,inode:$ino}' config-template.json > finalized-config.json
config_sha=$(sha256sum finalized-config.json); config_sha=${config_sha%% *}
# Native service plus stop grace and publication must fit the local machine deadline.
now=$(date +%s)
(( now >= boot_epoch && now + 2460 + 60 + 240 <= local_stop_epoch )) || exit 95
printf '%s %s %s\n' "$boot_epoch" "$now" "$local_stop_epoch" > evidence-root/deadline-admission.txt
phase=native-parity
set +e
systemd-run --unit=borsuk-cohort-parity --wait --pipe -p CPUQuota=400% -p AllowedCPUs=0-3 -p MemoryMax=8G -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=2460 -p TimeoutStopSec=30 -p KillMode=control-group -p "ExecStopPost=/bin/bash $root/service-stop.sh" \
 bash "$root/run_actual_cohort_admission.sh" "$root/finalized-config.json" "$config_sha" "$root/assets/bin/prepare_cohere_native_cohort" \
 a3a828beec8d898fdf1d26a3b6673ea16be5407da864222dc9a394c2d28221fb "$root/prepared-parent/cohort" "$root/evidence-local" "$root/assets/historical/requests"
parity_rc=$?; set -e
printf '%s\n' "$parity_rc" > evidence-root/parity-unit.exit
(( parity_rc == 0 )) || exit "$parity_rc"
df -B1 "$root" > evidence-root/disk-after.txt
phase=completion-validation
jq -e ' .schema=="borsuk-parity-service-exit-v1" and .exit_code=="exited" and .exit_status=="0" and .service_result=="success" ' evidence-root/service-exit.json
[[ -f evidence-local/wrapper.exit && $(cat evidence-local/wrapper.exit) == 0 ]]
jq -e '.status=="ADMISSION_VERIFIED" and .intended_exit==0 and .performance_claim==false' evidence-local/terminal.json
phase=complete
