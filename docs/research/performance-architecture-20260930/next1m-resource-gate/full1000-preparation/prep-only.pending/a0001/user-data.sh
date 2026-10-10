#!/usr/bin/env bash
# PENDING: root freezes prefix+support SHA; no launch admission from this source.
set -Eeuo pipefail
umask 077
root=/mnt/borsuk-scale1m
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261010/full1000-preparation-a0001
support_sha=afcb286064a028f05470b8799e1af9074e52c511f67026867cc4f7622fd62295
[[ $prefix =~ ^research/semantic-router/[0-9]{8}/[a-z0-9-]+$ && $support_sha =~ ^[0-9a-f]{64}$ ]] || { echo 'PENDING placeholders not frozen' >&2; exit 64; }
mkdir -p "$root"
cd "$root"
mkdir evidence-root
phase=bootstrap; instance=unknown; prep_closed=0; chain_exit=; chain_disp=
boot_epoch=$(date +%s)
local_stop_epoch=$((boot_epoch + 7200))
bad() { shutdown -h now; exit 96; }
finish() {
 original=$?; trap - EXIT TERM INT HUP PIPE; set +e
 close_deadline=$(( $(date +%s) + 570 ))
 (( close_deadline <= local_stop_epoch - 30 )) || close_deadline=$((local_stop_epoch - 30))
 close_run() {
  local cap=$1 now left; shift
  now=$(date +%s); left=$((close_deadline - now - 5))
  (( left > 0 )) || return 125
  (( cap <= left )) || cap=$left
  timeout -k 5 "$cap" "$@"
 }
 if [[ $prep_closed == 1 && $original == 0 ]]; then status=0
 elif (( original >= 90 && original <= 98 )); then status=$original
 else status=99; fi
 for unit in borsuk-cohort-transport borsuk-cohort-parity; do
  for pass in stop check; do
   state=$(close_run 10 systemctl show "$unit.service" -p ActiveState --value) || bad
   if [[ $pass == stop && ( $state == active || $state == activating || $state == deactivating ) ]]; then close_run 30 systemctl stop "$unit.service" || bad; fi
  done
  [[ $state != active && $state != activating && $state != deactivating ]] || bad
 done
 exec >/dev/null 2>&1
 if [[ -n ${log_pid:-} ]]; then wait "$log_pid" || status=96; fi
 close_run 10 systemctl show borsuk-cohort-parity.service > evidence-root/systemd-after-cohort-parity.txt 2>&1 || status=96
 [[ ! -f evidence-root/chain-outer/manager.show ]] || close_run 15 cp evidence-root/chain-outer/manager.show evidence-root/systemd-after-native-chain.txt
 raw=$(close_run 5 stat -c %s run.log) || bad; P=prepared-parent
 for d in evidence-root evidence-local evidence-chain; do
  if [[ -d $d ]]; then
   special=$(close_run 20 find "$d" ! -type f ! -type d) || bad; [[ -z $special ]] || bad
   n=$(close_run 20 du -sb "$d" | awk '{print $1}') || bad; [[ $n =~ ^[0-9]+$ ]] || bad; raw=$((raw + n))
  fi
 done
 bodies="$P/cohort/complete.json:65536 $P/cohort/truth.u64:80000 finalized-config.json:65536 scratch-launch-binding.json:4096 scratch-launch-binding.json.sha256:256"
 for b in $bodies; do
  IFS=: read -r src cap <<< "$b"
  if [[ -e $src || -L $src ]]; then
   [[ -f $src && ! -L $src ]] || bad
   n=$(close_run 5 stat -c %s "$src") || bad; [[ $n =~ ^[0-9]+$ ]] && (( n <= cap )) || bad; raw=$((raw + n))
  fi
 done
 (( raw + 2097152 <= 268435456 )) || bad
 for b in $bodies; do
  IFS=: read -r src cap <<< "$b"; dst=evidence-root/${src//\//_}
  if [[ -f $src ]]; then close_run 15 cp "$src" "$dst" && cmp -s "$src" "$dst" || status=96; fi
 done
 close_run 15 cp support.sha256 evidence-root/support.sha256 || status=96
 close_run 15 cp run.log evidence-root/bootstrap.log || status=96
 printf '%s\n' "$original" > evidence-root/bootstrap.exit || status=96
 for d in evidence-local evidence-chain; do if [[ -d $d ]]; then close_run 15 cp -a "$d" evidence-root/ || status=96; fi; done
 close_run 20 sync -f evidence-root || status=96
 (cd evidence-root && close_run 20 find . -type f -print0 | LC_ALL=C sort -z | close_run 45 xargs -0 sha256sum) > artifacts.sha256 || status=96
 [[ $(close_run 5 stat -c %s artifacts.sha256) -le 2097152 ]] || status=96
 (ulimit -f 262144; close_run 90 tar -czf evidence.tar.gz -C evidence-root .) || bad
 bytes=$(close_run 5 stat -c %s evidence.tar.gz); digest=$(close_run 30 sha256sum evidence.tar.gz); digest=${digest%% *}
 [[ $bytes -gt 0 && $bytes -le 268435456 ]] || bad
 close_run 150 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" --body evidence.tar.gz --if-none-match '*' > evidence.put.json || status=96
 close_run 30 aws s3api put-object --bucket "$bucket" --key "$prefix/artifacts.sha256" --body artifacts.sha256 --if-none-match '*' > manifest.put.json || status=96
 close_run 20 jq -n --arg instance "$instance" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --arg sha "$digest" --argjson bytes "$bytes" --arg unit "$chain_exit" --arg disp "$chain_disp" \
  '{schema:"borsuk-native-scale-build-bootstrap-closed-v1",instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,evidence:{bytes:$bytes,sha256:$sha},chain:{unit_exit:(if $unit=="" then null else ($unit|tonumber) end),disposition:(if $disp=="" then null else $disp end)},acceptance:"PROVISIONAL_REQUIRES_EXTERNAL_BOOTSTRAP_EXIT",publication_verified:false,scientific_success_asserted:false,performance_claim:false}' > terminal.json || status=96
 close_run 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" --body terminal.json --if-none-match '*' || status=96
 printf '%s\n' "$status" > final.exit || bad
 close_run 20 sync -f final.exit || { rm -f -- final.exit; bad; }
 exit "$status"
}
trap finish EXIT
trap 'exit 97' TERM INT HUP
trap 'exit 98' PIPE
exec > >(trap - EXIT ERR; ulimit -c 0; ulimit -f 32768; exec cat >run.log) 2>&1
log_pid=$!
shutdown -h +120
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
ser() { tr -d ' \n' < "/sys/block/${1##*/}/device/serial"; }
phase=support
get() { (ulimit -f "$2"; timeout -k 5 30 aws s3api get-object --bucket "$bucket" --key "$prefix/inputs/$1" "$1"); [[ -f $1 && ! -L $1 && $(stat -c %s "$1") -le $((${2} * 1024)) ]]; }
printf '%s  support.sha256\n' "$support_sha" > expected-support.sha256
get support.sha256 64
sha256sum -c expected-support.sha256
names='config-template.json derivation-config.json gate-config-template.json run_actual_cohort_admission.sh run_native_scale_build_gate.sh observer-command.sh collect-native-chain-outer.sh run-native-chain-observer.sh service-stop.sh transport-pins.json transport.py validate-scratch-binding.sh'
for n in $names; do get "$n" 64; done
sha256sum --strict -c support.sha256
awk '{print $2}' support.sha256 | LC_ALL=C sort > support.names
printf '%s\n' $names | LC_ALL=C sort > expected-support.names
cmp support.names expected-support.names
mkdir -p /run/systemd/system/cloud-final.service.d
printf '[Service]\nExecStopPost=/bin/bash %s/service-stop.sh bootstrap %s %s/bootstrap-manager.json\n' "$root" "$bucket" "$prefix" > /run/systemd/system/cloud-final.service.d/borsuk-exit.conf
systemctl daemon-reload
systemctl cat cloud-final.service > evidence-root/cloud-final-unit.txt
(( BASH_VERSINFO[0] > 5 || (BASH_VERSINFO[0] == 5 && BASH_VERSINFO[1] >= 1) ))
{ uname -r; cat /etc/os-release; jq --version; bash --version; systemd --version; /usr/bin/time --version; prlimit --version; lsblk --version; sha256sum /usr/bin/jq /usr/bin/prlimit; } > evidence-root/environment.txt 2>&1
phase=scratch
bd=scratch-launch-binding.json
setup_stop=$((boot_epoch + 900))
win() { t=$((setup_stop - $(date +%s))); (( t > 0 )) || return 1; (( t <= $1 )) || t=$1; }
while win 10; do
 (ulimit -f 4; timeout -k 1 "$t" aws s3api get-object --bucket "$bucket" --key "$prefix/inputs/$bd" "$bd" >/dev/null 2>&1) && win 10 && (ulimit -f 1; timeout -k 1 "$t" aws s3api get-object --bucket "$bucket" --key "$prefix/inputs/$bd.sha256" "$bd.sha256" >/dev/null 2>&1) && break
 rm -f -- "$bd" "$bd.sha256"; win 5 && sleep "$t"
done
(( $(date +%s) <= setup_stop )) || exit 90
[[ -f $bd && ! -L $bd && -f $bd.sha256 && ! -L $bd.sha256 && $(stat -c %s "$bd") -le 4096 && $(stat -c %s "$bd.sha256") -le 256 ]] || exit 90
h=$(sha256sum < "$bd"); printf '%s  scratch-launch-binding.json\n' "${h%% *}" | cmp -s - "$bd.sha256" || exit 90
ids=$(timeout -k 1 15 bash "$root/validate-scratch-binding.sh" "$instance" "$bd") || exit 90
[[ $ids =~ ^(vol-[0-9a-f]{8,17})\ (vol-[0-9a-f]{8,17})$ ]] || exit 90; vol=${BASH_REMATCH[1]}; rvol=${BASH_REMATCH[2]}
mapfile -t cand < <(for s in /sys/block/nvme*n1; do [[ $(ser "$s") == "${vol/-/}" ]] && basename "$s"; done)
(( ${#cand[@]} == 1 )) || exit 90
dev=/dev/${cand[0]}
ident() { { echo "== $1 $dev $vol"; lsblk -b -J -o NAME,SERIAL,SIZE,TYPE,FSTYPE,UUID,MOUNTPOINTS,PKNAME; ser "$dev"; echo; blkid -p "$dev" 2>&1 || echo "blkid_rc=$?"; findmnt -J; df -B1 "$root"; } > "evidence-root/scratch-$1.txt" 2>&1; }
[[ -b $dev && $(blockdev --getsize64 "$dev") == 42949672960 ]] || exit 90
ident before
nl=$(lsblk -rno NAME "$dev" | wc -l) || exit 90
mp=$(lsblk -rno MOUNTPOINTS "$dev") || exit 90
ft=$(lsblk -rno FSTYPE,PTTYPE "$dev" | tr -d ' \n') || exit 90
ws=$(wipefs -n "$dev") || exit 90
mt=$(findmnt -rno SOURCE) || exit 90
sw=$(swapon --noheadings --show=NAME) || exit 90
[[ $nl == 1 && -z $mp$ft$ws && $mt != *"$dev"* && $sw != *"$dev"* ]] || exit 90
bk=0; blkid -p "$dev" > /dev/null 2>&1 || bk=$?
[[ $bk == 2 ]] || exit 90
out=$(timeout -k 1 20 cmp -n 4194304 "$dev" /dev/zero 2>&1) && rc=0 || rc=$?; printf '%s\nrc=%s\n' "$out" "$rc" > evidence-root/scratch-cmp.txt || exit 90; (( rc <= 1 )) || exit 90
rd=$(lsblk -no PKNAME "$(findmnt -no SOURCE /)") || exit 90
[[ /dev/$rd != "$dev" && $(ser "$rd") == "${rvol/-/}" && $(ser "$dev") == "${vol/-/}" && $(blockdev --getsize64 "$dev") == 42949672960 ]] || exit 90
mkdir prepared-parent
timeout -k 5 300 mkfs.ext4 -q -m 0 -L borsuk-scratch -E nodiscard,lazy_itable_init=1,lazy_journal_init=1 "$dev" < /dev/null
mount -o noatime,nodev,nosuid "$dev" prepared-parent
chmod 0700 prepared-parent
mountpoint -q prepared-parent && [[ $(findmnt -no SOURCE prepared-parent) == "$dev" && $(stat -c %d prepared-parent) != $(stat -c %d .) ]] || exit 90
ident after
avail=$(df -B1 --output=avail prepared-parent | tail -1 | tr -d ' ')
root_avail=$(df -B1 --output=avail . | tail -1 | tr -d ' ')
need=$(jq '[.[].bytes] | add' transport-pins.json)
[[ $avail =~ ^[0-9]+$ && $root_avail =~ ^[0-9]+$ && $need =~ ^[0-9]+$ ]] || exit 90
floor=$((need + 1610612736))
printf 'scratch_avail=%s scratch_floor=30064771072 root_avail=%s transport_need=%s root_reserve=1610612736 root_floor=%s\n' "$avail" "$root_avail" "$need" "$floor" > evidence-root/disk-admission.txt
(( avail >= 30064771072 && root_avail >= floor )) || exit 90
phase=transport
pins_sha=$(sha256sum transport-pins.json); pins_sha=${pins_sha%% *}
set +e
systemd-run --unit=borsuk-cohort-transport --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=900 -p TimeoutStopSec=30 -p KillMode=control-group -p "ExecStopPost=/bin/bash $root/service-stop.sh transport" \
 python3 "$root/transport.py" "$root/transport-pins.json" "$pins_sha" "$root/assets"
rc=$?; set -e
printf '%s\n' "$rc" > evidence-root/transport-unit.exit
(( rc == 0 )) || exit 91
jq -e '.schema=="borsuk-parity-service-exit-v1" and .exit_code=="exited" and .exit_status=="0" and .service_result=="success"' evidence-root/transport-exit.json
jq -e --slurpfile pins transport-pins.json '.body_decode==false and (.items|length)==17 and ([.items[]|{relative_path,bytes,sha256}] == [$pins[0][]|{relative_path,bytes,sha256}]) and all(.items[]; .whole_body_authenticated==true)' assets/transport-receipt.json
chmod 0500 assets/bin/*
phase=prep-config
jq --arg assets "$root/assets" --arg parent "$root/prepared-parent" --argjson dev "$(stat -c %d prepared-parent)" --argjson ino "$(stat -c %i prepared-parent)" \
 '.shards |= map(.path=($assets+"/input/"+.publisher_path)) | .output_parent={path:$parent,device:$dev,inode:$ino}' config-template.json > finalized-config.json
config_sha=$(sha256sum finalized-config.json); config_sha=${config_sha%% *}
admit() { now=$(date +%s); (( now >= boot_epoch && now + $1 <= local_stop_epoch )) || exit 95; printf '%s %s %s %s %s\n' "$2" "$boot_epoch" "$now" "$1" "$local_stop_epoch" >> evidence-root/deadline-admission.txt; }
admit 5700 prep
phase=native-parity
set +e
systemd-run --unit=borsuk-cohort-parity --wait --pipe -p CPUQuota=400% -p AllowedCPUs=0-3 -p MemoryMax=8G -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=2460 -p TimeoutStopSec=30 -p KillMode=control-group -p "ExecStopPost=/bin/bash $root/service-stop.sh" \
 bash "$root/run_actual_cohort_admission.sh" "$root/finalized-config.json" "$config_sha" "$root/assets/bin/prepare_cohere_native_cohort" \
 a3a828beec8d898fdf1d26a3b6673ea16be5407da864222dc9a394c2d28221fb "$root/prepared-parent/cohort" "$root/evidence-local" "$root/assets/historical/requests"
rc=$?; set -e
printf '%s\n' "$rc" > evidence-root/parity-unit.exit
(( rc == 0 )) || exit 92
df -B1 "$root" > evidence-root/disk-root-after.txt
df -B1 prepared-parent > evidence-root/disk-scratch-after-prep.txt
phase=prep-validation
E=evidence-local
jq -e '.schema=="borsuk-parity-service-exit-v1" and .exit_code=="exited" and .exit_status=="0" and .service_result=="success"' evidence-root/service-exit.json
for f in native timeout time tee time-log supervisor-stderr-log native-stderr-log wrapper; do [[ -f $E/$f.exit && $(< "$E/$f.exit") == 0 ]]; done
for f in after closed; do [[ $(< "$E/resource-closure.$f.txt") == true ]]; done
jq -e '.status=="ADMISSION_VERIFIED" and .intended_exit==0 and .performance_claim==false' $E/terminal.json
phase=retain-preparation
# Whole native output seals were checked by the admission wrapper. Retain them create-only.
admit 3000 retain-preparation
retain_deadline=$(( $(date +%s) + 2400 ))
retain_run() {
 local cap=$1 now left; shift
 now=$(date +%s); left=$((retain_deadline - now - 5))
 (( left > 0 )) || return 125
 (( cap <= left )) || cap=$left
 timeout -k 5 "$cap" "$@"
}
jq -r '.outputs[] | [.name,.bytes,.sha256] | @tsv' prepared-parent/cohort/complete.json > evidence-root/prepared-outputs.tsv
while IFS=$'\t' read -r name bytes expected; do
 case "$name" in corpus.f32|corpus.ids.jsonl|queries.f32|queries.ids.jsonl|truth.u64) ;; *) exit 94;; esac
 file="prepared-parent/cohort/$name"
 [[ -f $file && ! -L $file && $(stat -c %s "$file") == "$bytes" ]] || exit 94
 stamp=$(stat -c '%d:%i:%s:%y:%z' "$file")
 actual=$(retain_run 120 sha256sum < "$file"); [[ ${actual%% *} == "$expected" ]] || exit 94
 # PutObject's 5GiB ceiling also bounds each admitted JSONL output here.
 (( bytes <= 5368709120 )) || exit 94
 retain_run 540 aws s3api put-object --bucket "$bucket" --key "$prefix/prepared/$name" --body "$file" --if-none-match '*' > "evidence-root/put-$name.json"
 [[ $(stat -c '%d:%i:%s:%y:%z' "$file") == "$stamp" ]] || exit 94
 retain_run 20 aws s3api head-object --bucket "$bucket" --key "$prefix/prepared/$name" > "evidence-root/head-$name.json"
 jq -e --argjson bytes "$bytes" '.ContentLength==$bytes' "evidence-root/head-$name.json"
done < evidence-root/prepared-outputs.tsv
# Marker is published last. ETags and length are transport evidence, not independent SHA proof.
retain_run 60 aws s3api put-object --bucket "$bucket" --key "$prefix/prepared/complete.json" --body prepared-parent/cohort/complete.json --if-none-match '*' > evidence-root/put-complete.json
prep_closed=1
phase=complete
exit 0
