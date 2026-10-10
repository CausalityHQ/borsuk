#!/usr/bin/env bash
# PENDING: root freezes prefix+support SHA; no launch admission from this source.
set -Eeuo pipefail
umask 077
root=/mnt/borsuk-scale1m
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261010/actual1m-scale-chain-canary-a0005
support_sha=274702e3891e4bd1b0d7927431d5227d23044c04391568ca11dbfb0627ccc8c0
[[ $prefix =~ ^research/semantic-router/[0-9]{8}/[a-z0-9-]+$ && $support_sha =~ ^[0-9a-f]{64}$ ]] || { echo 'PENDING placeholders not frozen' >&2; exit 64; }
mkdir -p "$root"
cd "$root"
mkdir evidence-root
phase=bootstrap; instance=unknown; chain_closed=0; chain_exit=; chain_disp=
boot_epoch=$(date +%s)
local_stop_epoch=$((1791593411 + 3000)); (( boot_epoch >= 1791593411 - 5 && boot_epoch <= 1791593411 + 600 )) || { shutdown -h now; exit 95; }
bad() { shutdown -h now; exit 96; }
finish() {
 original=$?; set +x; unset BASH_XTRACEFD; trap - EXIT TERM INT HUP PIPE; set +e
 if [[ $chain_closed == 1 && $original == "$chain_exit" ]]; then status=$original
 elif (( original >= 90 && original <= 98 )); then status=$original
 else status=99; fi
 for unit in borsuk-cohort-transport borsuk-cohort-parity borsuk-native-chain; do
  for pass in stop check; do
   state=$(timeout -k 2 10 systemctl show "$unit.service" -p ActiveState --value) || bad
   if [[ $pass == stop && ( $state == active || $state == activating || $state == deactivating ) ]]; then timeout -k 2 30 systemctl stop "$unit.service" || bad; fi
  done
  [[ $state != active && $state != activating && $state != deactivating ]] || bad
 done
 exec >/dev/null 2>&1
 if [[ -n ${log_pid:-} ]]; then wait "$log_pid" || status=96; fi
 for u in cohort-parity native-chain; do systemctl show "borsuk-$u.service" > "evidence-root/systemd-after-$u.txt" 2>&1; done
 raw=$(stat -c %s run.log) || bad; P=prepared-parent
 for d in evidence-root evidence-local evidence-chain; do
  if [[ -d $d ]]; then
   [[ -z $(find "$d" ! -type f ! -type d) ]] || bad
   n=$(du -sb "$d" | awk '{print $1}') || bad; [[ $n =~ ^[0-9]+$ ]] || bad; raw=$((raw + n))
  fi
 done
 bodies="$P/cohort/complete.json:65536 $P/cohort/truth.u64:65536 $P/derived/derivation.json:65536 $P/generation/manifest.json:65536 $P/generation/page_manifest.json:1048576 $P/generation/plane/manifest.json:1048576 $P/publication-receipt.json:65536 $P/query/baseline-result.jsonl:67108864 finalized-config.json:65536 gate-config-template.json:65536 gate-config.json:65536 chain-argv.json:65536 scratch-launch-binding.json:4096 scratch-launch-binding.json.sha256:256"
 for b in $bodies; do
  IFS=: read -r src cap <<< "$b"
  if [[ -e $src || -L $src ]]; then
   [[ -f $src && ! -L $src ]] || bad
   n=$(stat -c %s "$src") || bad; [[ $n =~ ^[0-9]+$ ]] && (( n <= cap )) || bad; raw=$((raw + n))
  fi
 done
 (( raw + 65536 <= 134217728 )) || bad
 for b in $bodies; do
  IFS=: read -r src cap <<< "$b"; dst=evidence-root/${src//\//_}
  if [[ -f $src ]]; then cp "$src" "$dst" && cmp -s "$src" "$dst" || status=96; fi
 done
 cp support.sha256 evidence-root/support.sha256 || status=96
 cp run.log evidence-root/bootstrap.log || status=96
 printf '%s\n' "$original" > evidence-root/bootstrap.exit || status=96
 for d in evidence-local evidence-chain; do if [[ -d $d ]]; then cp -a "$d" evidence-root/ || status=96; fi; done
 sync -f evidence-root || status=96
 (cd evidence-root && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum) > artifacts.sha256 || status=96
 [[ $(stat -c %s artifacts.sha256) -le 65536 ]] || status=96
 (ulimit -f 262144; tar -czf evidence.tar.gz -C evidence-root .) || bad
 bytes=$(stat -c %s evidence.tar.gz); digest=$(sha256sum evidence.tar.gz); digest=${digest%% *}
 [[ $bytes -gt 0 && $bytes -le 268435456 ]] || bad
 timeout -k 5 150 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" --body evidence.tar.gz --if-none-match '*' > evidence.put.json || status=96
 timeout -k 5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/artifacts.sha256" --body artifacts.sha256 --if-none-match '*' > manifest.put.json || status=96
 jq -n --arg instance "$instance" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --arg sha "$digest" --argjson bytes "$bytes" --arg unit "$chain_exit" --arg disp "$chain_disp" \
  '{schema:"borsuk-native-scale-build-bootstrap-closed-v1",instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,evidence:{bytes:$bytes,sha256:$sha},chain:{unit_exit:(if $unit=="" then null else ($unit|tonumber) end),disposition:(if $disp=="" then null else $disp end)},acceptance:"PROVISIONAL_REQUIRES_EXTERNAL_BOOTSTRAP_EXIT",publication_verified:false,scientific_success_asserted:false,performance_claim:false}' > terminal.json || status=96
 timeout -k 5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" --body terminal.json --if-none-match '*' || status=96
 printf '%s\n' "$status" > final.exit || bad
 sync -f final.exit || { rm -f -- final.exit; bad; }
 exit "$status"
}
trap finish EXIT
trap 'exit 97' TERM INT HUP
trap 'exit 98' PIPE
exec > >(trap - EXIT ERR; ulimit -c 0; ulimit -f 32768; exec cat >run.log) 2>&1
log_pid=$!
remaining_minutes=$(( (local_stop_epoch - $(date +%s)) / 60 )); (( remaining_minutes >= 1 )) || bad; shutdown -h +"$remaining_minutes"
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
names='config-template.json derivation-config.json gate-config-template.json run_actual_cohort_admission.sh run_native_scale_build_gate.sh service-stop.sh transport-pins.json transport.py validate-scratch-binding.sh'
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
setup_stop=$((boot_epoch + 800))
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
exec 3> evidence-root/scratch-trace.log; BASH_XTRACEFD=3; PS4='+scratch:${LINENO}:${FUNCNAME[0]:-main}: '; set -x
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
timeout -k 5 60 mkfs.ext4 -q -m 0 -L borsuk-scratch -E nodiscard,lazy_itable_init=1,lazy_journal_init=1 "$dev" < /dev/null
mount -o noatime,nodev,nosuid "$dev" prepared-parent
chmod 0700 prepared-parent
mountpoint -q prepared-parent && [[ $(findmnt -no SOURCE prepared-parent) == "$dev" && $(stat -c %d prepared-parent) != $(stat -c %d .) ]] || exit 90
ident after
set +x; unset BASH_XTRACEFD
avail=$(df -B1 --output=avail prepared-parent | tail -1 | tr -d ' ')
root_avail=$(df -B1 --output=avail . | tail -1 | tr -d ' ')
need=$(jq '[.[].bytes] | add' transport-pins.json)
[[ $avail =~ ^[0-9]+$ && $root_avail =~ ^[0-9]+$ && $need =~ ^[0-9]+$ ]] || exit 90
floor=$((need + 1610612736))
printf 'scratch_avail=%s scratch_floor=30064771072 root_avail=%s transport_need=%s root_reserve=1610612736 root_floor=%s\n' "$avail" "$root_avail" "$need" "$floor" > evidence-root/disk-admission.txt
(( avail >= 30064771072 && root_avail >= floor )) || exit 90
(( $(date +%s) + 1800 <= local_stop_epoch )) || exit 95
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

phase=canary
get canary-tail.sh 64
printf '%s  canary-tail.sh\n' ba736c3d4b6c05bd6b6c242f795ad2ac138580ab66a15df9856efe23b0fb9aae | sha256sum --strict -c -
get canary-coordinator.sh 64
printf '%s  canary-coordinator.sh\n' bff35749d957c1e76635b1170c2ef390fe97dadf5b3dc72712b65451397e6f35 | sha256sum --strict -c -
get wrapper-canary.sh 64
printf '%s  wrapper-canary.sh\n' bf2cb012c3880c420bf9c3b80334e42a7d1469caa3c339042cde8607d3093a1f | sha256sum --strict -c -
bash "$root/canary-tail.sh" "$root" "$bucket" "$prefix" "$instance" "$boot_epoch" "$local_stop_epoch" 96b9a7b8a74af189954aa04dc589b0e4fbf9e0d725f3f0fa7ca5ab05e6204a24
exit 99
