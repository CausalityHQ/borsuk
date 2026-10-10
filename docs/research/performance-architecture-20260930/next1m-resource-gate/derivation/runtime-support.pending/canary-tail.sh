#!/usr/bin/env bash
# Borsuk next-1M cloud-final canary tail (plan R6 s.3 + root decision, message 1791585172630928753).
# SOURCE ONLY until the root assembles and launches it. Runs as a CHILD of the original cloud-final user-data, after the
# pinned production prefix (lines 1..159, including `chmod 0500 assets/bin/*`), as:
#   canary-tail.sh ROOT BUCKET PREFIX INSTANCE BOOT_EPOCH LOCAL_STOP_EPOCH FRAGMENT_SHA256
# Exit 0 = the coordinator unit was queued (T1..T4 results are recorded in evidence-root/canary-tail, not in this status);
# 90 = refusal (arguments, pins, T1 stack assertion); 91 = hand-over failure; 92 = T3 cleanup not proven in its step remainder (INVALID, batch stopped);
# 95 = deadline guard. No ANN, no native chain, no performance claim.
# Every step records its own exit status; QUALIFIED requires T1..T4 to be 0 and is decided by the coordinator.
# shellcheck disable=SC2016,SC2329
# (the dpkg-query format is a literal; cg_inner is shipped to the transient unit through declare -f)
set -Eeuo pipefail
umask 077
export LC_ALL=C PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
[[ $# == 7 ]] || exit 90
root=$1 bucket=$2 prefix=$3 instance=$4 boot=$5 stop=$6 frag_sha=$7
[[ $root == /mnt/borsuk-scale1m && $bucket == borsuk-bench-453182569524-euc1 && $prefix =~ ^research/semantic-router/[0-9]{8}/[a-z0-9-]+$ && $instance =~ ^i-[0-9a-f]+$ && $boot =~ ^[0-9]+$ && $stop =~ ^[0-9]+$ && $frag_sha =~ ^[0-9a-f]{64}$ ]] || exit 90
wc_sha=bf2cb012c3880c420bf9c3b80334e42a7d1469caa3c339042cde8607d3093a1f
coord_sha=b2ec593fe50776462dcb403237f1a971361bfa2249e8c303f9d4815ed968700b
probe_sha=6111cbc9f504be3dc43c96dc450f2d30d319ab20ccdcf0acf94d65a518072fc9
want_ci=26.1-0ubuntu1~24.04.1
dropin=/run/systemd/system/cloud-final.service.d/borsuk-exit.conf
[[ $coord_sha =~ ^[0-9a-f]{64}$ ]] || exit 90
cd "$root"
ev=$root/evidence-root/canary-tail
mkdir -m 0700 -- "$ev"
self=$(sha256sum < "${BASH_SOURCE[0]}"); self=${self%% *}
wc_stop=0
step_stop=0
T=0
t3_halt=0
reg() { [[ -f $1 && ! -L $1 ]]; }
sha() { local h; h=$(sha256sum < "$1") || return 1; h=${h%% *}; [[ $h =~ ^[0-9a-f]{64}$ ]] || return 1; printf '%s' "$h"; }
need() { (( stop - EPOCHSECONDS >= $1 )) || exit 95; }
mark() { printf '%s %s\n' "$EPOCHSECONDS" "$1" >> "$ev/times.txt"; }
fin_step() { printf '%s\n' "$2" > "$ev/$1.exit"; mark "$1 end rc=$2"; }
# One recorded positive deadline per window: tmo CAP sets T = min(CAP, step deadline, wrapper-class deadline, machine stop - 20) - now - 1
# (the 1 s is the reserved kill grace); a remainder <= 0 is REFUSED without invoking the command (never `timeout 0`).
tmo() {
 local lim=$step_stop r
 (( wc_stop < lim )) && lim=$wc_stop
 (( stop - 20 < lim )) && lim=$((stop - 20))
 r=$((lim - EPOCHSECONDS - 1))
 (( r > 0 )) || return 1
 T=$(( $1 < r ? $1 : r ))
}
run() { local cap=$1; shift; tmo "$cap" || return 125; timeout -k 1 "$T" "$@"; }
step_begin() { step_stop=$((EPOCHSECONDS + $2)); mark "$1 begin budget=$2 step_deadline=$step_stop"; }
cg_empty() { [[ ! -d $1 ]] || grep -qx 'populated 0' "$1/cgroup.events" 2> /dev/null || [[ ! -d $1 ]]; }

# ---- T1: stack assertion (<= 10 s). Every check ends in an explicit `|| return 1`: errexit is off inside functions called with ||
t1() {
 local frag drop h='' ci_ver=unreadable ci_cli=unreadable sd_ver=unreadable fok=false dok=false
 run 3 systemctl show cloud-final.service -p Type -p RemainAfterExit -p KillMode -p TasksMax -p TimeoutStartUSec -p ExecStart -p ExecStopPost -p FragmentPath -p DropInPaths > "$ev/T1.show.txt" || return 1
 frag=$(run 2 systemctl show cloud-final.service -p FragmentPath --value) || return 1
 if [[ $frag == /* ]] && reg "$frag"; then h=$(sha "$frag") || h=''; [[ $h == "$frag_sha" ]] && fok=true; fi
 drop=$(run 2 systemctl show cloud-final.service -p DropInPaths --value) || return 1
 if [[ $drop == "$dropin" ]] && cmp -s -- "$dropin" <(printf '[Service]\nExecStopPost=/bin/bash %s/service-stop.sh bootstrap %s %s/bootstrap-manager.json\n' "$root" "$bucket" "$prefix"); then dok=true; fi
 # best-effort version evidence last: a refused or failed call is recorded as unreadable, never as a pass
 ci_ver=$(run 2 dpkg-query -W -f='${Version}' cloud-init 2>&1 | head -c 100) || ci_ver=unreadable
 ci_cli=$(run 3 cloud-init --version 2>&1 | head -c 200) || ci_cli=unreadable
 sd_ver=$(run 1 systemctl --version 2>&1 | head -n1) || sd_ver=unreadable
 jq -n --arg f "$frag" --arg h "$h" --arg want "$frag_sha" --argjson fok "$fok" --argjson dok "$dok" --arg dp "$drop" \
  --arg civ "$ci_ver" --arg wci "$want_ci" --arg cli "$ci_cli" --arg sd "$sd_ver" \
  '{schema:"borsuk-canary-t1-v1",fragment_path:$f,fragment_sha256:$h,fragment_pin:$want,fragment_ok:$fok,drop_in_paths:$dp,drop_in_exact:$dok,
    cloud_init_dpkg:$civ,cloud_init_cli:$cli,systemd:$sd,version_matches_expected:($civ == $wci),expected_version:$wci}' > "$ev/T1.json" || return 1
 [[ $fok == true && $dok == true ]] || return 1
}

# ---- T2: the root's wrapper canary, unchanged and sha-pinned (<= 70 s) ----------------------------------------------------
t2() {
 local rc=0 f
 reg wrapper-canary.sh && [[ $(sha wrapper-canary.sh) == "$wc_sha" ]] || return 1
 [[ ! -e canary-wrapper && ! -L canary-wrapper ]] || return 1
 ( ulimit -f 128; run 70 bash ./wrapper-canary.sh "$root/run_native_scale_build_gate.sh" "$root/assets/bin" "$root/canary-wrapper" > "$ev/T2.out" 2> "$ev/T2.err" ) || rc=$?
 mkdir -m 0700 -- "$ev/wrapper" || return 1
 # small, regular, allowlisted files only (the wrapper canary also leaves a FIFO and large logs behind)
 for f in canary-wrapper/result.json canary-wrapper/SHA256SUMS canary-wrapper/source.sha256 canary-wrapper/*.exit canary-wrapper/*.assert; do
  if reg "$f" && (( $(stat -c %s "$f") <= 8192 )); then cp -p -- "$f" "$ev/wrapper/" || return 1; fi
 done
 (( rc == 0 )) || return 1
 jq -e '.status == "WRAPPER_CHECKS_VERIFIED" and .ann_run == false and .performance_claim == false' canary-wrapper/result.json > /dev/null || return 1
}

# ---- T3: production resource chain properties in a transient unit (<= 20 s) ------------------------------------------------
cg_inner() {
 set -Eeuo pipefail
 local rel d name value key
 local -a args
 rel=$(sed -n 's/^0:://p' /proc/self/cgroup)
 [[ $rel == /system.slice/borsuk-canary-cgroup.service ]]
 d=/sys/fs/cgroup$rel
 while :; do
  args=(--arg path "$d")
  for name in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max memory.events pids.events; do
   value=absent
   if [[ -r $d/$name ]]; then value=$(< "$d/$name"); fi
   key=${name//./_}
   args+=(--arg "$key" "$value")
  done
  jq -cn "${args[@]}" '$ARGS.named'
  [[ $d != /sys/fs/cgroup ]] || break
  d=${d%/*}
 done
 sleep 300 > /dev/null 2>&1 &
 printf '%s\n' "$!" > "$1"
}
IFS= read -r -d '' jq_cg <<'EOF' || true
def cap($k): [.[] | .[$k] | select(. != "absent" and . != "max") | tonumber] | if length > 0 then min else error("missing limit") end;
def ev: split("\n") | map(select(length > 0) | split(" ") | {key: .[0], value: (.[1] | tonumber)}) | from_entries;
.[0] as $leaf |
($leaf.cpu_max == "400000 100000") and ($leaf.cpuset_cpus_effective == "0-3") and
($leaf.memory_max == "8589934592") and ($leaf.memory_swap_max == "0") and ($leaf.pids_max == "128") and
(cap("memory_max") == 8589934592) and (cap("memory_swap_max") == 0) and (cap("pids_max") == 128) and
(($leaf.memory_events | ev) as $m | $m.oom == 0 and $m.oom_kill == 0) and (($leaf.pids_events | ev) as $p | $p.max == 0)
EOF
# t3_drain: stop the OWNED transient unit, kill only its exact cgroup subtree, then PROVE inactive|failed and populated 0 (or removed)
t3_drain() {
 local u=borsuk-canary-cgroup.service cg=/sys/fs/cgroup/system.slice/borsuk-canary-cgroup.service st
 run 4 systemctl stop "$u" || true
 if [[ -d $cg ]]; then
  [[ -w $cg/cgroup.kill ]] && printf '1' > "$cg/cgroup.kill"
  for _ in 1 2 3 4 5 6; do if cg_empty "$cg" || ! tmo 1; then break; fi; sleep 0.5; done
 fi
 st=$(run 2 systemctl show "$u" -p ActiveState --value) || return 1
 run 2 systemctl reset-failed "$u" || true
 [[ $st == inactive || $st == failed ]] && cg_empty "$cg"
}
# t3_clean: the NORMAL cleanup runs inside the same step/shared-120 remainder (its time is reserved before the unit is launched; no
# success path gets extra time). Only if that cannot PROVE the drain is ONE emergency drain attempted, in a finite safety margin taken from
# the 300 s margin bucket solely to stop/clean (never a measurement budget); either way the attempt is INVALID: t3_halt stops T4/T5/coordinator.
t3_clean() {
 local dr=0 er=0 s1 s2
 t3_drain || dr=$?
 if (( dr == 0 )); then return 0; fi
 s1=$step_stop; s2=$wc_stop
 step_stop=$((EPOCHSECONDS + 12)); wc_stop=$step_stop
 mark "T3 EMERGENCY drain (safety margin, not measurement budget) deadline=$step_stop"
 t3_drain || er=$?
 step_stop=$s1; wc_stop=$s2
 printf 'normal_drain_rc=%s emergency_drain_rc=%s attempt=INVALID\n' "$dr" "$er" > "$ev/T3.emergency"
 t3_halt=1
 return 1
}
t3() {
 local rc=0 dr=0 pid full=$step_stop pidf=$ev/cgroup.stray.pid cg=/sys/fs/cgroup/system.slice/borsuk-canary-cgroup.service
 # reserve 8 s of THIS step (inside the shared 120 s) for the drain BEFORE the unit is launched
 step_stop=$((full - 8))
 # the manager expands $ (and %) in command-line words (systemd-run(1); services since v254): the generated function text must never be a command-line word,
 # so the exact generated script is written to a file and the unit runs `bash FILE PIDFILE` (two words without $ or %); the file is also the evidence of the generated source
 { declare -f cg_inner; printf '%s\n' 'cg_inner "$1"'; } > "$ev/cg_inner.sh" || return 1
 ( ulimit -f 128; run 20 systemd-run --quiet --wait --pipe --collect --unit=borsuk-canary-cgroup -p CPUQuota=400% -p AllowedCPUs=0-3 -p MemoryMax=8G -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=8 -p TimeoutStopSec=4 -p KillMode=control-group \
   bash "$ev/cg_inner.sh" "$pidf" > "$ev/cgroup.resources.jsonl" 2> "$ev/cgroup.err" ) || rc=$?
 step_stop=$full
 # cleanup is proven on EVERY path (the original failure and its evidence are kept); unproven cleanup in the normal remainder halts the batch
 t3_clean || dr=$?
 printf '%s\n' "$dr" > "$ev/T3.drain.exit"
 if (( dr != 0 )); then return 1; fi
 (( rc == 0 )) || return 1
 jq -s -e "$jq_cg" "$ev/cgroup.resources.jsonl" > /dev/null || return 1
 pid=$(< "$pidf") || return 1
 [[ $pid =~ ^[0-9]+$ ]] || return 1
 for _ in 1 2 3 4 5 6 7 8; do if ! kill -0 "$pid" 2> /dev/null || ! tmo 1; then break; fi; sleep 0.25; done
 ! kill -0 "$pid" 2> /dev/null || return 1
 [[ ! -d $cg ]] || return 1
}

# ---- T4: controlled-probe IAM checks (<= 20 s). Only the exact error code AccessDenied is denial evidence --------------------
aws_t() { # aws_t NAME s3api-arguments...: stdout/stderr/exit recorded; stderr and stdout are size-capped
 local n=$1 rc=0
 shift
 ( ulimit -f 64; run 3 aws s3api "$@" > "$ev/iam.$n.out" 2> "$ev/iam.$n.err" ) || rc=$?
 printf '%s\n' "$rc" > "$ev/iam.$n.rc"
}
denied() { # denied NAME OPERATION KEY: rc exactly 254 and EXACTLY the AWS CLI v2 explicit-deny line (optional "aws: [ERROR]: " prefix, optional leading blank line); 412, timeouts, other errors, implicit denies and any extra line are refused
 local rc msg acct pre mid
 local -a lines
 rc=$(< "$ev/iam.$1.rc")
 [[ $rc == 254 ]] || return 1
 mapfile -t lines < "$ev/iam.$1.err"
 if (( ${#lines[@]} == 2 )) && [[ -z ${lines[0]} ]]; then msg=${lines[1]}; elif (( ${#lines[@]} == 1 )); then msg=${lines[0]}; else return 1; fi
 msg=${msg#"aws: [ERROR]: "}
 pre="An error occurred (AccessDenied) when calling the $2 operation: User: arn:aws:sts::"
 mid=":assumed-role/borsuk-bench-role/$instance is not authorized to perform: s3:$2 on resource: \"arn:aws:s3:::$bucket/$3\" with an explicit deny in an identity-based policy"
 [[ $msg == "$pre"* && $msg == *"$mid" ]] || return 1
 acct=${msg#"$pre"}; acct=${acct%"$mid"}
 [[ $acct =~ ^[0-9]{12}$ ]]
}
t4() {
 local key=$prefix/inputs/permission-probe.txt pos=$prefix/canary/iam-positive.txt bad=0 b=$ev/iam.probe.get1
 aws_t get1 get-object --bucket "$bucket" --key "$key" "$b"
 [[ $(< "$ev/iam.get1.rc") == 0 ]] && reg "$b" && [[ $(stat -c %s "$b") == 38 && $(sha "$b") == "$probe_sha" ]] || bad=1
 aws_t putdeny put-object --bucket "$bucket" --key "$key" --body "$b" --if-none-match '*'
 denied putdeny PutObject "$key" || bad=1
 aws_t deldeny delete-object --bucket "$bucket" --key "$key"
 denied deldeny DeleteObject "$key" || bad=1
 aws_t get2 get-object --bucket "$bucket" --key "$key" "$ev/iam.probe.get2"
 [[ $(< "$ev/iam.get2.rc") == 0 ]] && reg "$ev/iam.probe.get2" && [[ $(stat -c %s "$ev/iam.probe.get2") == 38 && $(sha "$ev/iam.probe.get2") == "$probe_sha" ]] || bad=1
 printf 'borsuk-canary-iam-positive-v1 %s\n' "$instance" > "$ev/iam.positive.body"
 aws_t putpos put-object --bucket "$bucket" --key "$pos" --body "$ev/iam.positive.body" --if-none-match '*'
 [[ $(< "$ev/iam.putpos.rc") == 0 ]] && jq -e 'type == "object" and (.ETag | type == "string")' "$ev/iam.putpos.out" > /dev/null || bad=1
 jq -n --argjson bad "$bad" --arg key "$key" --arg sha "$probe_sha" '{schema:"borsuk-canary-t4-v1",probe_key:$key,probe_sha256:$sha,controlled_probe_deleted_or_rewritten_by_canary:"only if DELETE/PUT unexpectedly succeeded (then GET#2 fails)",all_checks_passed:($bad == 0)}' > "$ev/T4.json" || return 1
 (( bad == 0 )) || return 1
}

# ---- T5: hand over to the coordinator (<= 15 s, its own deadline) ---------------------------------------------------------
t5() {
 reg canary-coordinator.sh && [[ $(sha canary-coordinator.sh) == "$coord_sha" ]] || return 1
 jq -n --arg b "$bucket" --argjson boot "$boot" --arg c "$coord_sha" --arg f "$frag_sha" --arg i "$instance" --argjson stop "$stop" --arg p "$prefix" --arg t "$self" --arg w "$wc_sha" \
  '{schema:"borsuk-canary-params-v1",bucket:$b,boot_epoch:$boot,coordinator_sha256:$c,fragment_sha256:$f,instance_id:$i,local_stop_epoch:$stop,prefix:$p,tail_sha256:$t,wrapper_canary_sha256:$w}' > "$ev/params.json" || return 1
 ( umask 022; cat > /run/systemd/system/borsuk-canary-coordinator.service <<'EOF'
[Unit]
Description=Borsuk canary coordinator
After=cloud-final.service borsuk-canary-final-0.service borsuk-canary-final-2.service borsuk-canary-final-3.service

[Service]
Type=simple
WorkingDirectory=/mnt/borsuk-scale1m
ExecStart=/bin/bash /mnt/borsuk-scale1m/canary-coordinator.sh
RuntimeMaxSec=480
TimeoutStopSec=15
KillMode=control-group
StandardOutput=journal+console
StandardError=journal+console
EOF
 ) || return 1
 run 8 systemctl daemon-reload || return 1
 run 5 systemctl start --no-block borsuk-canary-coordinator.service || return 1
}

# ---- sequence --------------------------------------------------------------------------------------------------------------
need 900
wc_stop=$((EPOCHSECONDS + 120))
printf 'wrapper_class_start=%s wrapper_class_deadline=%s\n' "$EPOCHSECONDS" "$wc_stop" > "$ev/deadline.txt"
rc=0; step_begin T1 10; t1 || rc=$?; fin_step T1 "$rc"
(( rc == 0 )) || exit 90
rc=0; step_begin T2 70; t2 || rc=$?; fin_step T2 "$rc"
rc=0; step_begin T3 20; t3 || rc=$?; fin_step T3 "$rc"
if (( t3_halt != 0 )); then
 jq -n '{schema:"borsuk-canary-tail-v1",coordinator_queued:false,attempt:"INVALID",halt:"T3 cleanup not proven in the normal step remainder (emergency drain used); T4, T5 and the coordinator not run"}' > "$ev/tail.json"
 exit 92
fi
rc=0; step_begin T4 20; t4 || rc=$?; fin_step T4 "$rc"
need 780
wc_stop=$stop
rc=0; step_begin T5 15; t5 || rc=$?; fin_step T5 "$rc"
(( rc == 0 )) || exit 91
jq -n --arg self "$self" --arg coord "$coord_sha" --arg wc "$wc_sha" --arg frag "$frag_sha" '{schema:"borsuk-canary-tail-v1",tail_sha256:$self,coordinator_sha256:$coord,wrapper_canary_sha256:$wc,fragment_sha256:$frag,coordinator_queued:true}' > "$ev/tail.json"
exit 0
