#!/usr/bin/env bash
# PENDING: root freezes prefix+support SHA; no launch admission from this source.
set -Eeuo pipefail
umask 077
root=/mnt/borsuk-scale1m
bucket=borsuk-bench-453182569524-euc1
prefix=PENDING_ROOT_FRESH_PREFIX
support_sha=PENDING_ROOT_SUPPORT_SHA
[[ $prefix =~ ^research/semantic-router/[0-9]{8}/[a-z0-9-]+$ && $support_sha =~ ^[0-9a-f]{64}$ ]] || { echo 'PENDING placeholders not frozen' >&2; exit 64; }
mkdir -p "$root"
cd "$root"
mkdir evidence-root
phase=bootstrap; instance=unknown; prep_closed=0; chain_exit=; chain_disp=
boot_epoch=$(date +%s)
local_stop_epoch=$((boot_epoch + 7200))
bad() { shutdown -h now; exit 96; }
retain_panel_result() { return 96; }
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
 if [[ $prep_closed == 1 && ( $original == 0 || $original == 2 || $original == 3 ) ]]; then status=$original
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
 if [[ -n $chain_exit && ( $chain_exit == 0 || $chain_exit == 2 || $chain_exit == 3 ) ]]; then
  [[ -f evidence-root/chain-outer/manager.show && ! -L evidence-root/chain-outer/manager.show ]] || status=96
 fi
 if [[ -f evidence-root/chain-outer/manager.show ]]; then close_run 15 cp evidence-root/chain-outer/manager.show evidence-root/systemd-after-native-chain.txt || status=96; fi
 raw=$(close_run 5 stat -c %s run.log) || bad; P=prepared-parent
 for d in evidence-root evidence-local evidence-chain; do
  if [[ -d $d ]]; then
   special=$(close_run 20 find "$d" ! -type f ! -type d) || bad; [[ -z $special ]] || bad
   n=$(close_run 20 du -sb "$d" | awk '{print $1}') || bad; [[ $n =~ ^[0-9]+$ ]] || bad; raw=$((raw + n))
  fi
 done
 bodies="$P/cohort/complete.json:65536 $P/cohort/truth.u64:80000 gate-config.json:65536 $P/derived/derivation.json:65536 $P/generation/manifest.json:65536 $P/publication-receipt.json:65536 scratch-launch-binding.json:4096 scratch-launch-binding.json.sha256:256"
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
  if [[ -f $src ]]; then close_run 15 cp "$src" "$dst" && close_run 15 cmp -s "$src" "$dst" || status=96; fi
 done
 close_run 15 cp support.sha256 evidence-root/support.sha256 || status=96
 close_run 15 cp run.log evidence-root/bootstrap.log || status=96
 printf '%s\n' "$original" > evidence-root/bootstrap.exit || status=96
 for d in evidence-local evidence-chain; do if [[ -d $d ]]; then close_run 15 cp -a "$d" evidence-root/ || status=96; fi; done
 # Before chain launch, preserve the original setup failure without fabricating a missing-result failure.
 if [[ -n $chain_exit || -e $root/prepared-parent/query/baseline-result.jsonl || -L $root/prepared-parent/query/baseline-result.jsonl ]]; then
  closed_phase=$phase
  retain_panel_result || status=96
  phase=$closed_phase
 fi
 close_run 20 sync -f evidence-root || status=96
 (cd evidence-root && close_run 20 find . -type f -print0 | LC_ALL=C sort -z | close_run 45 xargs -0 sha256sum) > artifacts.sha256 || status=96
 manifest_bytes=$(close_run 5 stat -c %s artifacts.sha256) || manifest_bytes=
 [[ $manifest_bytes =~ ^[1-9][0-9]*$ ]] && (( manifest_bytes <= 2097152 )) || status=96
 (ulimit -f 262144; close_run 90 tar -czf evidence.tar.gz -C evidence-root .) || bad
 bytes=$(close_run 5 stat -c %s evidence.tar.gz) || bad
 digest=$(close_run 30 sha256sum evidence.tar.gz) || bad; digest=${digest%% *}
 [[ $bytes =~ ^[1-9][0-9]*$ && $digest =~ ^[0-9a-f]{64}$ ]] && (( bytes <= 268435456 )) || bad
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
names='derivation-config.json gate-config.json run_native_scale_build_gate.sh observer-command.sh collect-native-chain-outer.sh run-native-chain-observer.sh service-stop.sh transport-pins.json transport.py validate-scratch-binding.sh retain-panel-result.sh retain-generation-source.sh'
for n in $names; do get "$n" 64; done
sha256sum --strict -c support.sha256
awk '{print $2}' support.sha256 | LC_ALL=C sort > support.names
printf '%s\n' $names | LC_ALL=C sort > expected-support.names
cmp support.names expected-support.names
source "$root/retain-panel-result.sh"
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
jq -e --slurpfile pins transport-pins.json '.body_decode==false and (.items|length)==10 and ([.items[]|{relative_path,bytes,sha256}] == [$pins[0][]|{relative_path,bytes,sha256}]) and all(.items[]; .whole_body_authenticated==true)' assets/transport-receipt.json
chmod 0500 assets/bin/*

# Embedded successor bootstrap block; source only, not executed.
# $root is the fresh owned instance root; all10 transport bodies authenticated first.
phase=map-prepared
[[ -d $root/assets/prepared && ! -L $root/assets/prepared && ! -e $root/prepared-parent/cohort && ! -L $root/prepared-parent/cohort ]] || exit 91
mkdir -m 0700 "$root/prepared-parent/cohort"
map_deadline=$(( $(date +%s) + 600 ))
map_run() {
 local now left; now=$(date +%s); left=$((map_deadline-now-5))
 (( left > 0 )) || return 125
 timeout --signal=TERM --kill-after=5 "$left" "$@"
}
map_one() {
 local name=$1 bytes=$2 expected=$3 src dst actual
 src=$root/assets/prepared/$name; dst=$root/prepared-parent/cohort/$name
 [[ -f $src && ! -L $src && $(stat -c %h "$src") == 1 && $(stat -c %s "$src") == "$bytes" && ! -e $dst && ! -L $dst ]] || return 1
 map_run cp --no-clobber --reflink=never -- "$src" "$dst" || return
 [[ -f $dst && ! -L $dst && $(stat -c %h "$dst") == 1 && $(stat -c %s "$dst") == "$bytes" ]] || return 1
 actual=$(map_run sha256sum -- "$dst") || return
 [[ ${actual%% *} == "$expected" ]] || return 1
 chmod 0400 "$dst" || return
 map_run sync -f "$dst" || return
}
map_one corpus.f32 4096000000 1a491c060ec8b668b4983042dc1ac3971456327f41f28fd6fd2449df24cf3e38 || exit 91
map_one queries.f32 4096000 8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e || exit 91
map_one corpus.ids.jsonl 200873446 008191ae3103d78db682276008190cc858a361b19e487f6780c02cea5dd70067 || exit 91
map_one queries.ids.jsonl 196017 f0e6e974c3dfe45fae7db3a9a5146891541d1f3c0d605674d115b6c580a87f68 || exit 91
map_one truth.u64 80000 0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f || exit 91
map_one complete.json 6238 e0503d41493157a6a0a48627515e62ec452386d0496827028bb9571d406e3a17 || exit 91
map_run sync -f "$root/prepared-parent/cohort" || exit 91

phase=native-chain
config_sha=$(sha256sum gate-config.json); config_sha=${config_sha%% *}
now=$(date +%s)
(( now >= boot_epoch && now + 3680 + 1800 + 600 <= local_stop_epoch )) || exit 95
printf '%s %s %s\n' "$now" 3680 "$local_stop_epoch" > evidence-root/chain-deadline-admission.txt
set +e
bash "$root/run-native-chain-observer.sh" "$root" "$config_sha"
chain_exit=$?
set -e
[[ $chain_exit == 0 || $chain_exit == 2 || $chain_exit == 3 ]] || exit 96
chain_disp=LOCAL_PANEL_CLOSED_EXTERNAL_REPLAY_REQUIRED
# Preserve authenticated generation objects before machine termination.
# This transport is not retained publication or cold measurement acceptance.
if [[ $chain_exit == 0 ]]; then
 phase=retain-generation
 now=$(date +%s)
 (( now + 1800 + 600 <= local_stop_epoch )) || exit 95
 bash "$root/retain-generation-source.sh" "$root" "$bucket" "$prefix" "$((now+1800))" || exit 94
fi
# Result preservation occurs in finish after the exact observer has closed.
prep_closed=1
phase=local-panel-complete
exit "$chain_exit"
