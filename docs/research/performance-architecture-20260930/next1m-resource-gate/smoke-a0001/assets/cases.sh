#!/usr/bin/env bash
# Disposable-host mechanics only; never a native correctness or performance claim.
set -Eeuo pipefail
umask 077
src=$1; out=$2
[[ $EUID == 0 && $src == /* && $out == /* && ! -e $out ]]
mkdir "$out"
cg=$(systemctl show borsuk-smoke.slice -p ControlGroup --value)
[[ $cg == /* && $cg != *..* ]]
cg=/sys/fs/cgroup$cg
for file in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max memory.events pids.events; do
 cat "$cg/$file" > "$out/$file.before"
done
[[ $(cat "$cg/cpu.max") == '100000 100000' && $(cat "$cg/cpuset.cpus.effective") == 0 && $(cat "$cg/memory.max") == 268435456 && $(cat "$cg/memory.swap.max") == 0 && $(cat "$cg/pids.max") == 128 ]]
[[ $(readlink /proc/self/ns/net) != "$(readlink /proc/1/ns/net)" ]]
(cd "$src" && sha256sum --strict -c source.sha256) > "$out/source-check.txt"
cp "$src/source.sha256" "$out/source.sha256"
root=/mnt/borsuk-scale1m
[[ ! -e $root ]]
mkdir "$root"
[[ $(stat -c %d "$root") == "$(stat -c %d "$out")" ]]
sed -n '/^finish() {$/,/^}$/p' "$src/user-data.sh" > "$out/bootstrap-finish.sh"
sed -n '/^get_support() {$/,/^}$/p' "$src/user-data.sh" > "$out/get-support.sh"
[[ -s $out/bootstrap-finish.sh && -s $out/get-support.sh ]]
sha256sum "$out/bootstrap-finish.sh" "$out/get-support.sh" > "$out/fragments.sha256"
printf '%s\n' 'UNRUN: full wrapper/native paths; real S3 publication; corpus/query/truth.' > "$out/limits.txt"
uname -a > "$out/host.txt"
systemctl cat cloud-final.service > "$out/cloud-final.original.txt"
systemctl show cloud-final.service > "$out/cloud-final.original.properties"
[[ $(systemctl show cloud-final.service -p Type --value) == oneshot ]]
[[ $(systemctl show cloud-final.service -p RemainAfterExit --value) == yes ]]
cloud-init --version > "$out/cloud-init.version" 2>&1
sha256sum /usr/lib/python3/dist-packages/cloudinit/signal_handler.py > "$out/cloud-init.signal-handler.sha256"
for ((i=0;i<200;i++)); do
 [[ $(systemctl show cloud-final.service -p MainPID --value) == 0 ]] && break
 sleep .05
done
[[ $(systemctl show cloud-final.service -p MainPID --value) == 0 ]]
[[ $(systemctl show cloud-final.service -p SubState --value) == exited || $(systemctl show cloud-final.service -p ActiveState --value) == inactive ]]
printf 'SOURCE_BOUND_SETUP_ONLY\n' > "$out/state.txt"
sed -n '/^finish() {$/,/^}$/p' "$src/run_actual_cohort_admission.sh" > "$out/wrapper-finish.sh"
[[ -s $out/wrapper-finish.sh ]]
mkdir "$out/bin"
cat > "$out/bin/shutdown" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >> /mnt/borsuk-scale1m/shutdown.calls
SH
cat > "$out/bin/aws" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ ${1:-} == --region ]]; then
 [[ $# -ge 7 && $2 == eu-central-1 && $3 == --cli-connect-timeout && $4 == 10 && $5 == --cli-read-timeout && $6 == 60 ]]
 shift 6
fi
[[ ${1:-} == s3api ]]
op=$2; shift 2
body= key= last=
while (($#)); do
 last=$1
 case $1 in --body) body=$2; shift;; --key) key=$2; shift;; esac
 shift
done
root=/mnt/borsuk-scale1m
if [[ $op == get-object ]]; then head -c "${MOCK_GET_BYTES:?}" /dev/zero > "$last"; exit; fi
[[ $op == put-object && -f $body && -n $key ]]
mkdir -p "$root/put"
[[ ! -e $root/put/${key##*/} ]]
cp "$body" "$root/put/${key##*/}"
printf '%s\n' "$key" >> "$root/put.calls"
if [[ ${key##*/} == terminal.json ]]; then
 touch "$root/terminal.accepted"
 case ${CASE_MODE:?} in lost) exit 96;; term) while [[ ! -e $root/put.release ]]; do sleep .05; done;; esac
fi
printf '{}\n'
SH
chmod 0500 "$out/bin/aws" "$out/bin/shutdown"
cat > "$out/cloud-child.sh" <<'SH'
#!/usr/bin/env bash
set -Eeuo pipefail
src=$1; out=$2; mode=$3
root=/mnt/borsuk-scale1m
export PATH="$out/bin:$PATH" CASE_MODE=$mode
cd "$root"
touch ready
while [[ ! -e go ]]; do sleep .05; done
(
 source "$out/wrapper-finish.sh"
 evidence=$root/evidence-local; config_sha=fixture; elf_sha=fixture; output=fixture
 stage=fixture; failed_line=0; verified=1
 true; finish
)
source "$out/bootstrap-finish.sh"
phase=complete
case $mode in normal) instance=i-00000000000000001;; lost) instance=i-00000000000000002;; term) instance=i-00000000000000003;; esac
bucket=borsuk-bench-453182569524-euc1; prefix=research/mechanical-smoke
trap finish EXIT
true
SH
chmod 0500 "$out/cloud-child.sh"
wait_file() { for ((i=0;i<200;i++)); do [[ -e $1 ]] && return; sleep .05; done; return 1; }
owned_drop=/run/systemd/system/cloud-final.service.d/borsuk-smoke.conf
hook_drop=/run/systemd/system/cloud-final.service.d/borsuk-exit.conf
[[ ! -e $owned_drop && ! -e $hook_drop ]]
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
cleanup() {
 trap - EXIT
 for unit in borsuk-cohort-transport.service borsuk-cohort-parity.service cloud-final.service; do
  stop_owned "$unit" || exit 2
 done
 rm -- "$owned_drop" "$hook_drop"
 timeout -k 2 10 systemctl daemon-reload || exit 2
}
systemctl stop cloud-final.service
mkdir -p /run/systemd/system/cloud-final.service.d
printf '[Service]\nExecStart=\nExecStart=/bin/bash %s/cloud-child.sh %s %s normal\nSlice=borsuk-smoke.slice\nPrivateNetwork=yes\nTimeoutStartSec=120\nTimeoutStopSec=5\n' "$out" "$src" "$out" > "$owned_drop"
touch "$hook_drop"
trap cleanup EXIT
reset_fixture() {
 if [[ -e $root/evidence-root ]]; then mv "$root" "$out/case.$1.previous"; mkdir "$root"; fi
 mkdir "$root/evidence-root" "$root/evidence-local"
 printf 'fixture\n' > "$root/evidence-local/fixture.txt"
 cp "$src/service-stop.sh" "$root/service-stop.sh"
 cp "$src/source.sha256" "$root/support.sha256"
 printf 'fixture\n' > "$root/run.log"
}
for mode in normal lost term; do
 reset_fixture "$mode"
 printf '[Service]\nExecStart=\nExecStart=/bin/bash %s/cloud-child.sh %s %s %s\nSlice=borsuk-smoke.slice\nPrivateNetwork=yes\nTimeoutStartSec=120\nTimeoutStopSec=5\n' "$out" "$src" "$out" "$mode" > "$owned_drop"
 : > "$hook_drop"
 systemctl daemon-reload
 systemctl reset-failed cloud-final.service || true
 systemctl start --no-block cloud-final.service
 wait_file "$root/ready"
 # Install the actual stop hook while this exact invocation is already running.
 printf '[Service]\nExecStopPost=/bin/bash %s/service-stop.sh bootstrap\n' "$root" > "$hook_drop"
 systemctl daemon-reload
 touch "$root/go"
 wait_file "$root/terminal.accepted"
 if [[ $mode == term ]]; then
  systemctl kill --kill-whom=main --signal=TERM cloud-final.service
  touch "$root/put.release"
 fi
 for ((i=0;i<200;i++)); do
  state=$(systemctl show cloud-final.service -p SubState --value)
  [[ $state == exited || $state == failed || $state == dead ]] && break
  sleep .05
 done
 [[ $state == exited || $state == failed || $state == dead ]]
 systemctl show cloud-final.service > "$root/manager.properties"
 code=$(systemctl show cloud-final.service -p ExecMainCode --value)
 status=$(systemctl show cloud-final.service -p ExecMainStatus --value)
 case $mode in normal) [[ $code == 1 && $status == 0 ]];; lost) [[ $code == 1 && $status == 96 ]];; term) [[ $code == 2 && $status == 15 ]];; esac
 jq -e '.status=="ADMISSION_VERIFIED" and .intended_exit==0' "$root/evidence-local/terminal.json"
 [[ -f $root/put/terminal.json ]]
 systemctl stop cloud-final.service
 mv "$root" "$out/case.$mode"
 mkdir "$root"
done
printf 'CLOUD_MANAGER_CASES_PASSED_REMAINING_CASES_PENDING\n' > "$out/state.txt"

# Exercise the exact bootstrap logger fragment with inert bytes, never native output.
sed -n '/^exec > >(trap - EXIT ERR; ulimit -c 0; ulimit -f 32768; exec cat >run.log) 2>&1$/,/^log_pid=\$!$/p' "$src/user-data.sh" > "$out/bounded-logger.sh"
[[ $(wc -l < "$out/bounded-logger.sh") == 2 ]]
sha256sum "$out/bounded-logger.sh" >> "$out/fragments.sha256"
for mode in logger-normal logger-overflow; do
 reset_fixture "$mode"
 set +e
 (cd "$root"; export PATH="$out/bin:$PATH" CASE_MODE=normal
  source "$out/bootstrap-finish.sh"
  phase=complete; instance=i-00000000000000004
  bucket=borsuk-bench-453182569524-euc1; prefix=research/mechanical-smoke
  trap finish EXIT
  source "$out/bounded-logger.sh"
  case $mode in logger-normal) head -c 256 /dev/zero;; logger-overflow) head -c 33554433 /dev/zero;; esac)
 logger_case_rc=$?
 set -e
 if [[ $mode == logger-normal ]]; then
  [[ $logger_case_rc == 0 && $(stat -c %s "$root/run.log") == 256 ]]
 else
  (( logger_case_rc != 0 ))
  [[ $(stat -c %s "$root/run.log") -le 33554432 ]]
  if [[ -f $root/terminal.json ]]; then
   jq -e '.exit!=0' "$root/terminal.json"
  else
   [[ ! -e $root/put/terminal.json ]]
  fi
 fi
 [[ ! -e $root/shutdown.calls ]]
 mv "$root" "$out/case.$mode"; mkdir "$root"
done

# Run exact wrapper log/supervisor fragment against a labelled Bash dummy, no Rust native.
sed -n '/^stage=log-admission$/,/^stage=resources-after$/p' "$src/run_actual_cohort_admission.sh" > "$out/wrapper-log-fragment.sh"
[[ -s $out/wrapper-log-fragment.sh ]]
sha256sum "$out/wrapper-log-fragment.sh" >> "$out/fragments.sha256"
cat > "$out/log-dummy.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
# This is inert process-I/O mechanics, never a native or ANN fixture.
mkdir "$3"
head -c 2097152 /dev/zero > "$3/unrelated-regular-output"
case $1 in
 normal) printf 'dummy stdout\n'; printf 'dummy stderr\n' >&2;;
 stdout-overflow) head -c 1025 /dev/zero;;
 stderr-overflow) head -c 1048577 /dev/zero >&2;;
esac
SH
chmod 0500 "$out/log-dummy.sh"
for mode in normal stdout-overflow stderr-overflow; do
 evidence="$out/log-$mode"; mkdir "$evidence"
 output="$out/log-output-$mode"; config=$mode; config_sha=dummy
 preparer="$out/log-dummy.sh"
 (source "$out/wrapper-log-fragment.sh")
 [[ $(stat -c %s "$output/unrelated-regular-output") == 2097152 ]]
 [[ $(stat -c %s "$evidence/native.stdout.json") -le 1024 ]]
 for name in native.stderr.txt supervisor.stderr.txt native.time.txt; do
  [[ $(stat -c %s "$evidence/$name") -le 1048576 ]]
 done
 failure=0
 for name in native timeout time tee time-log supervisor-stderr-log native-stderr-log; do
  status=$(cat "$evidence/$name.exit")
  [[ $status =~ ^[0-9]+$ ]]
  (( status == 0 )) || failure=1
 done
 if [[ $mode == normal ]]; then [[ $failure == 0 ]]; else [[ $failure == 1 ]]; fi
 done
sed -n '/^resource_closure() {$/,/^}$/p' "$src/run_actual_cohort_admission.sh" > "$out/resource-closure-fragment.sh"
[[ -s $out/resource-closure-fragment.sh ]]
sha256sum "$out/resource-closure-fragment.sh" >> "$out/fragments.sha256"
for mode in max-increase max-decrease oom-change pids-change; do
 evidence="$out/resource-$mode"; mkdir "$evidence"
 before_max=0; after_max=1; after_oom=0; after_pids=0
 case $mode in max-decrease) before_max=1; after_max=0;; oom-change) after_oom=1;; pids-change) after_pids=1;; esac
 for phase in before after; do
  max=$before_max; oom=0; pids=0
  if [[ $phase == after ]]; then max=$after_max; oom=$after_oom; pids=$after_pids; fi
  jq -cn --arg max "$max" --arg oom "$oom" --arg pids "$pids" \
   '{path:"/fixture",cpu_max:"400000 100000",cpuset_cpus_effective:"0-3",memory_max:"8589934592",memory_swap_max:"0",pids_max:"128",memory_events:("max "+$max+"\noom "+$oom+"\noom_kill 0\noom_group_kill 0"),pids_events:("max "+$pids)}' > "$evidence/resources.$phase.jsonl"
  printf '{}\n' > "$evidence/resources.$phase.effective.json"
 done
 set +e
 (set -e; source "$out/resource-closure-fragment.sh"; resource_closure after)
 closure_rc=$?
 set -e
 if [[ $mode == max-increase ]]; then
  [[ $closure_rc == 0 ]]; jq -e '.valid==true and .memory_max_events[0].delta==1' "$evidence/resource-events.after.json"
 else
  (( closure_rc != 0 )); jq -e '.valid==false' "$evidence/resource-events.after.json"
 fi
 done
reset_fixture receipts
systemd-run --unit=borsuk-cohort-transport --slice=borsuk-smoke.slice --wait --pipe -p PrivateNetwork=yes -p RuntimeMaxSec=30 -p "ExecStopPost=/bin/bash $root/service-stop.sh transport" /bin/true
jq -e '.schema=="borsuk-parity-service-exit-v1" and .exit_code=="exited" and .exit_status=="0" and .service_result=="success"' "$root/evidence-root/transport-exit.json"
# The service shell expands its own positional argument.
# shellcheck disable=SC2016
systemd-run --unit=borsuk-cohort-parity --slice=borsuk-smoke.slice --no-block -p PrivateNetwork=yes -p RuntimeMaxSec=30 -p "ExecStopPost=/bin/bash $root/service-stop.sh" /bin/bash -c 'touch "$1"; exec sleep 100' bash "$root/payload.ready"
wait_file "$root/payload.ready"
systemctl kill --kill-whom=main --signal=TERM borsuk-cohort-parity.service
quiescent_unit borsuk-cohort-parity.service
wait_file "$root/evidence-root/service-exit.json"
jq -e '.schema=="borsuk-parity-service-exit-v1" and .exit_code=="killed"' "$root/evidence-root/service-exit.json"
if jq -e '.schema=="borsuk-parity-service-exit-v1" and .exit_code=="exited" and .exit_status=="0" and .service_result=="success"' "$root/evidence-root/service-exit.json"; then exit 2; fi
stop_owned borsuk-cohort-parity.service
mv "$root" "$out/case.receipts"; mkdir "$root"
archive_ok() {
 local archive=$1 manifest=$2 dir=$3
 mkdir "$dir"
 tar -tzf "$archive" | sed '/\/$/d' | LC_ALL=C sort > "$dir/archive.names"
 duplicates=$(uniq -d "$dir/archive.names")
 [[ -z $duplicates ]]
 if grep -Eq '(^/|(^|/)\.\.(/|$))' "$dir/archive.names"; then return 1; else [[ $? == 1 ]] || return 1; fi
 tar -tvzf "$archive" | awk 'substr($1,1,1)!="-" && substr($1,1,1)!="d" {bad=1} END {exit bad}'
 awk '{print $2}' "$manifest" | LC_ALL=C sort > "$dir/manifest.names"
 cmp "$dir/archive.names" "$dir/manifest.names"
 mkdir "$dir/extracted"
 tar -xzf "$archive" -C "$dir/extracted"
 (cd "$dir/extracted" && sha256sum --strict -c "$manifest")
}
archive_ok "$out/case.normal/evidence.tar.gz" "$out/case.normal/artifacts.sha256" "$out/archive.good"
mkdir "$out/archive.extra-source"
tar -xzf "$out/case.normal/evidence.tar.gz" -C "$out/archive.extra-source"
touch "$out/archive.extra-source/extra"
tar -czf "$out/extra.tar.gz" -C "$out/archive.extra-source" .
# Each verifier command must propagate failure, including inside negative checks.
set +e
(set -e; archive_ok "$out/extra.tar.gz" "$out/case.normal/artifacts.sha256" "$out/archive.extra")
extra_rc=$?
set -e
((extra_rc != 0))
tail -n +2 "$out/case.normal/artifacts.sha256" > "$out/omitted.sha256"
set +e
(set -e; archive_ok "$out/case.normal/evidence.tar.gz" "$out/omitted.sha256" "$out/archive.omitted")
omitted_rc=$?
set -e
((omitted_rc != 0))
reset_fixture support
export PATH="$out/bin:$PATH" CASE_MODE=normal
export bucket=borsuk-bench-453182569524-euc1 prefix=research/mechanical-smoke
cd "$root"
# Exact SHA-bound function extracted above.
# shellcheck source=/dev/null
source "$out/get-support.sh"
export MOCK_GET_BYTES=65536
get_support boundary
[[ $(stat -c %s boundary) == 65536 ]]
export MOCK_GET_BYTES=65537
set +e
(set -e; get_support excess)
cap_rc=$?
set -e
((cap_rc != 0)); [[ $(stat -c %s excess) -le 65536 ]]
mv "$root" "$out/case.support"; mkdir "$root"
for mode in log metadata combined; do
 reset_fixture "$mode"
 mkdir -p "$root/prepared-parent/cohort"
 case $mode in
  log) truncate -s 33554433 "$root/run.log";;
  metadata) truncate -s 33554433 "$root/prepared-parent/cohort/complete.json";;
  combined) truncate -s 17825792 "$root/prepared-parent/cohort/complete.json"; truncate -s 17825792 "$root/prepared-parent/cohort/truth.u64";;
 esac
 if [[ $mode == log ]]; then
  cat > "$root/live-writer.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$$" > "$1/writer.pid"
while :; do printf 'fixture\n' >> "$1/writer.bytes"; sleep .05; done
SH
  systemd-run --unit=borsuk-cohort-parity --slice=borsuk-smoke.slice --no-block -p PrivateNetwork=yes -p RuntimeMaxSec=30 -p KillMode=control-group /bin/bash "$root/live-writer.sh" "$root"
  wait_file "$root/writer.pid"
  writer_pid=$(cat "$root/writer.pid")
  [[ $writer_pid =~ ^[0-9]+$ && -d /proc/$writer_pid ]]
 fi
 set +e
 # Exact SHA-bound function extracted above.
# shellcheck source=/dev/null
 (cd "$root"; source "$out/bootstrap-finish.sh"; export phase=complete instance='i-00000000000000000'; true; finish)
 evidence_rc=$?
 set -e
 if [[ $mode == log ]]; then
  quiescent_unit borsuk-cohort-parity.service
  [[ ! -d /proc/$writer_pid ]]
  sha256sum "$root/writer.bytes" > "$root/writer.after-stop.sha256"
  sleep .1
  sha256sum --strict -c "$root/writer.after-stop.sha256"
 fi
 [[ $evidence_rc == 96 && ! -e $root/put.calls && ! -e $root/evidence.tar.gz && ! -e $root/evidence-root/bootstrap.log ]]
 mv "$root" "$out/case.$mode"; mkdir "$root"
done
printf 'MECHANICS_CASES_PASSED_TRANSPORT_AND_EXTERNAL_CONSOLE_PENDING\n' > "$out/state.txt"
pins=$out/transport-pins.json
zero_sha=$(head -c 4 /dev/zero | sha256sum); zero_sha=${zero_sha%% *}
jq -n --arg sha "$zero_sha" '[range(0;13)|{kind:"s3",bucket:"borsuk-bench-453182569524-euc1",key:("research/inert-fixture/"+tostring),etag:"fixture",relative_path:("fixture/"+tostring),bytes:4,sha256:$sha}]' > "$pins"
pins_sha=$(sha256sum "$pins"); pins_sha=${pins_sha%% *}
export MOCK_GET_BYTES=4
python3 "$src/transport.py" "$pins" "$pins_sha" "$out/transport.good"
jq -e '.body_decode==false and (.items|length)==13 and all(.items[]; .whole_body_authenticated==true and .bytes==4)' "$out/transport.good/transport-receipt.json"
export MOCK_GET_BYTES=5
set +e
python3 "$src/transport.py" "$pins" "$pins_sha" "$out/transport.excess"
transfer_rc=$?
set -e
((transfer_rc != 0)); [[ ! -e $out/transport.excess/fixture/0 && ! -e $out/transport.excess/transport-receipt.json && $(stat -c %s "$out/transport.excess/fixture/0.part") -le 4 ]]
cleanup
trap - EXIT
for file in memory.events pids.events; do cat "$cg/$file" > "$out/$file.after"; cmp "$out/$file.before" "$out/$file.after"; done
mkdir "$root/evidence-root"
cp "$src/service-stop.sh" "$root/service-stop.sh"
jq -n '{status:"MECHANICS_VERIFIED_EXTERNAL_CONSOLE_PENDING",mock_publication:true,real_systemd:true,real_s3_publication:false,http_get_tested:false,full_wrapper_native_tested:false,cloud_final_main_is_fixture:true,poweroff_hook_tested:false,performance_claim:false}' > "$out/result.json"
sync -f "$out"
printf 'MECHANICS_VERIFIED_EXTERNAL_CONSOLE_PENDING\n' > "$out/state.txt"
