#!/bin/bash
set -Eeuo pipefail
umask 077
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 DEBIAN_FRONTEND=noninteractive
root=/mnt/borsuk-reducer
bucket=borsuk-bench-453182569524-euc1
prefix=research/native-library-check/20261010/scale-prefix-parity-a0003
mkdir -p "$root/evidence"
cd "$root"
phase=bootstrap
started=$(date +%s)
systemd-run --unit=borsuk-parity-host-stop --on-active=9000s /usr/sbin/shutdown -h now
finish() {
 original=$?
 trap - EXIT TERM
 set +e
 code=$original
 phase_at_exit=$phase
 timeout -k 2 30 systemctl show native-parity-gates -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup >evidence/manager.show || code=96
 owned_cgroup=$(cat evidence/cgroup-path.txt) || code=96
 zero_receipts() {
  [[ $(cat evidence/launcher.exit) == 0 && $(cat evidence/final-exit) == 0 ]]
 }
 exact_absent() {
  [[ $owned_cgroup == /system.slice/native-parity-gates.service && ! -e /sys/fs/cgroup$owned_cgroup ]]
 }
 if [[ $owned_cgroup != /system.slice/native-parity-gates.service ]]; then
  code=96
 else
  if [[ -d /sys/fs/cgroup$owned_cgroup ]]; then
   timeout -k 2 30 systemctl stop native-parity-gates >evidence/stop.stdout 2>evidence/stop.stderr
   stop_rc=$?
   printf '%s\n' "$stop_rc" >evidence/stop.exit || code=96
   if (( stop_rc != 0 )); then
    if (( stop_rc != 124 && stop_rc != 137 )) &&
       [[ ! -s evidence/stop.stdout ]] &&
       [[ $(cat evidence/stop.stderr) == 'Failed to stop native-parity-gates.service: Unit native-parity-gates.service not loaded.' ]] &&
       exact_absent && zero_receipts; then
     printf '%s\n' unloaded-during-stop >evidence/stop-reconciled || code=96
    else code=96; fi
   fi
  fi
  if [[ -e /sys/fs/cgroup$owned_cgroup/cgroup.events ]]; then
   if cat "/sys/fs/cgroup$owned_cgroup/cgroup.events" >evidence/drain.events; then
    grep -Fxq 'populated 0' evidence/drain.events || code=96
   elif exact_absent && zero_receipts; then
    printf '%s\n' owned-cgroup-disappeared-during-read >evidence/drain.events || code=96
   else code=96; fi
  elif exact_absent && zero_receipts; then
   printf '%s\n' owned-cgroup-absent >evidence/drain.events || code=96
  else code=96; fi
  # A successful closure always requires both payload receipts, including after stop.
  if (( code == 0 )); then zero_receipts || code=96; fi
 fi
 cp bootstrap.log evidence/bootstrap.log || code=96
 printf '%s\n' "$original" >evidence/bootstrap-original-exit
 printf '%s\n' "$code" >evidence/collection-exit
 (cd evidence && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum >SHA256SUMS) || code=96
 tar -czf evidence.tar.gz evidence || code=96
 timeout -k 2 90 aws s3api put-object --bucket "$bucket" --key "$prefix/output/evidence.tar.gz" --body evidence.tar.gz --if-none-match '*' >archive-put.json || code=96
 archive_sha=$(sha256sum evidence.tar.gz | cut -d ' ' -f1) || code=96
 archive_bytes=$(stat -c %s evidence.tar.gz) || code=96
 token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token) || code=96
 instance=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id) || code=96
 write_terminal() {
  jq -cn --arg source 'afb70da60727ba80147a97e71fce738ab15e6456' --arg phase "$phase_at_exit" --arg instance "$instance" --arg sha "$archive_sha" --argjson bytes "${archive_bytes:-0}" --argjson original "$original" --argjson exit "$code" '{schema:"borsuk-scale-parity-native-qualification-v1",source_commit:$source,instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,archive:{bytes:$bytes,sha256:$sha},performance_claim:false}' >terminal.json
 }
 ready=0
 if write_terminal && sync -f terminal.json && sync -f "$root"; then ready=1; else code=96; fi
 if [[ $ready == 0 ]]; then
  if write_terminal && sync -f terminal.json && sync -f "$root"; then ready=1; fi
 fi
 if [[ $ready == 1 ]]; then
  timeout -k 2 60 aws s3api put-object --bucket "$bucket" --key "$prefix/output/terminal.json" --body terminal.json --if-none-match '*' >terminal-put.json || code=96
 fi
 shutdown -h now
 exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >bootstrap.log 2>&1
phase=install
timeout -k 10 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
timeout -k 10 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip jq time tar gzip util-linux binutils build-essential pkg-config libssl-dev cmake python3.12 python3-dev
curl -fsSL --connect-timeout 10 --max-time 180 -o aws.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  aws.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
test "$(stat -c %s aws.zip)" = 73022935
timeout -k 5 120 unzip -q aws.zip
timeout -k 5 120 ./aws/install
phase=download
for name in source.tar.gz source-files.sha256 source-manifest.json gates.sh; do
 timeout -k 2 60 aws s3api get-object --bucket "$bucket" --key "$prefix/input/$name" "$name" >"$name.get.json"
done
printf '%s  source.tar.gz\n' 8c16e336a801da9be04fddc3044d0d680aa8e31ca64f771c5c46cfe9992c6525 | sha256sum -c -
printf '%s  source-manifest.json\n' 5f26cb5ba194695d6aa5c9c08b0e8206324ba923bcb6478636c0b385c06f869e | sha256sum -c -
printf '%s  source-files.sha256\n' a249fbfabfd0eeabc7037493e8dd01f883a4d3cec6a3f478bf543a74b8013d58 | sha256sum -c -
printf '%s  gates.sh\n' e8f69df7d7bacb6b7ddd08c1bf4e0a6d93773ff7e110b699eb223be2a45ea436 | sha256sum -c -
mkdir source
tar -xzf source.tar.gz -C source
phase=toolchain
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"
curl -fsSL --connect-timeout 10 --max-time 180 -o rustup-init.sh https://sh.rustup.rs
timeout -k 10 600 sh rustup-init.sh -y --profile minimal --default-toolchain 1.98.0
timeout -k 10 180 rustup component add clippy --toolchain 1.98.0
phase=native
(( $(date +%s) - started + 7200 + 300 <= 9000 )) || exit 95
set +e
systemd-run --unit=native-parity-gates --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=7200 -p TimeoutStopSec=30 -p KillMode=control-group -p LimitCORE=0 -p WorkingDirectory="$root/source" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$PATH" /bin/bash "$root/gates.sh" >evidence/launcher.stdout 2>evidence/launcher.stderr
native_exit=$?
set -e
printf '%s\n' "$native_exit" >evidence/launcher.exit
(( native_exit == 0 )) || exit "$native_exit"
phase=complete
