#!/usr/bin/env bash
# SOURCE ONLY / runtime UNVERIFIED. Execute only on causality EC2.
# One fixed native phase. Root owns config, input/provenance admission and quality.
# shellcheck disable=SC2329 # EXIT trap invokes finish and owned cleanup.
set -Eeuo pipefail
set -o noclobber
umask 077
export LC_ALL=C
die() { printf 'INVALID: %s\n' "$*" >&2; exit 98; }
[[ $# == 3 ]] || die 'usage: PHASE_RECORD PHASE_RECORD_SHA NEW_EVIDENCE'
record=$1 record_sha=$2 evidence=$3
[[ $record_sha =~ ^[0-9a-f]{64}$ && $EUID == 0 ]] || die 'root/record SHA'
canonical() {
    [[ $1 == /* && $1 != *[$'\t\r\n\\']* && $(realpath -e -- "$1") == "$1" ]] || die "noncanonical: $1"
}
new_path() {
    [[ $1 == /* && $1 != */ && ! -e $1 && ! -L $1 && ${1##*/} != . && ${1##*/} != .. ]] || die 'occupied/invalid output'
    canonical "${1%/*}"
}
authenticate() {
    local path=$1 bytes=$2 expected=$3 before digest
    canonical "$path"
    [[ -f $path && ! -L $path && $(stat -c %h "$path") == 1 && $bytes =~ ^[1-9][0-9]*$ && $expected =~ ^[0-9a-f]{64}$ ]] || die 'artifact type/pin'
    before=$(stat -c '%d:%i:%s:%y:%z' "$path")
    [[ $(stat -c %s "$path") == "$bytes" ]] || die 'artifact length'
    digest=$(sha256sum < "$path")
    [[ ${digest%% *} == "$expected" && $(stat -c '%d:%i:%s:%y:%z' "$path") == "$before" ]] || die 'artifact SHA/drift'
}
for tool in jq stat realpath sha256sum cp cmp timeout prlimit systemctl systemd-run awk sed sync find sort xargs sleep; do command -v "$tool" >/dev/null || die "missing $tool"; done
[[ -x /usr/bin/time ]] || die 'GNU time missing'
record_bytes=$(stat -c %s "$record")
(( record_bytes > 0 && record_bytes <= 32768 )) || die 'record cap'
authenticate "$record" "$record_bytes" "$record_sha"
new_path "$evidence"
mkdir -- "$evidence"
cp -- "$record" "$evidence/record.json"
authenticate "$evidence/record.json" "$record_bytes" "$record_sha"
# Canonical JSON also refuses duplicate fields: jq serialization must equal original bytes.
jq -cS . "$evidence/record.json" > "$evidence/record.canonical.json"
cmp "$evidence/record.json" "$evidence/record.canonical.json" || die 'record must be one canonical JSON object'
jq -e '
  def exact($keys): type=="object" and keys==$keys;
  def pin: exact(["bytes","path","sha256"]) and (.bytes|type=="number" and floor==. and .>0)
    and (.path|type=="string") and (.sha256|type=="string" and test("^[0-9a-f]{64}$"));
  exact(["config","executable","output","phase","schema"]) and
  .schema=="borsuk-retained-native-phase-config-v1" and
  (.phase|IN("historical-reduction","local-reduction","retained-publication","s3-panel32","s3-reduction","three-arm-parity")) and
  (.executable|pin and .bytes<=33554432) and (.config|pin and .bytes<=65536) and (.output|type=="string")
' "$evidence/record.json" > "$evidence/record.valid"
phase=$(jq -r .phase "$evidence/record.json")
elf=$(jq -r .executable.path "$evidence/record.json")
elf_bytes=$(jq -r .executable.bytes "$evidence/record.json")
elf_sha=$(jq -r .executable.sha256 "$evidence/record.json")
native_config=$(jq -r .config.path "$evidence/record.json")
native_config_bytes=$(jq -r .config.bytes "$evidence/record.json")
native_config_sha=$(jq -r .config.sha256 "$evidence/record.json")
native_output=$(jq -r .output "$evidence/record.json")
new_path "$native_output"
[[ $native_output != "$evidence/"* && $evidence != "$native_output/"* ]] || die 'output/evidence overlap'
phase_memory=1073741824
phase_seconds=120
case $phase in
  retained-publication)
    expected_sha=076afd99e6dc6c814461161fd3bab0fd7bc1ab4db68c1337c8c98137fef15982
    phase_seconds=1200; native_args=(--retained "$native_config" "$native_config_sha" "$native_output") ;;
  s3-panel32)
    expected_sha=59fe47aa1001b3ca24d1f9ff31444f97fcda72e3e297c8d7d846f5c3d811bfc3
    phase_memory=536870912; phase_seconds=900; native_args=("$native_config" "$native_config_sha" "$native_output") ;;
  three-arm-parity)
    expected_sha=03aca9d786119e15762add1f2f0d9331fc3c697b73214b98c8072a4b1477de4f
    native_args=(--scale-prefix-parity "$native_config" "$native_config_sha" "$native_output") ;;
  *)
    expected_sha=03aca9d786119e15762add1f2f0d9331fc3c697b73214b98c8072a4b1477de4f
    native_args=(--completed-scale "$native_config" "$native_config_sha" "$native_output") ;;
esac
[[ $elf_sha == "$expected_sha" && -x $elf ]] || die 'qualified role executable'
authenticate "$elf" "$elf_bytes" "$elf_sha"
authenticate "$native_config" "$native_config_bytes" "$native_config_sha"
mkdir "$evidence/manager-calls" "$evidence/phases"
stage=observer-admission
owned_unit='' owned_id='' owned_cg='' phase_dir='' observer_cg=''
verified=0 native_rc=98 failed_line=0
finish() {
    local original=$? rc status=INVALID cleanup=0 publication=0
    trap - EXIT ERR
    trap '' HUP INT TERM
    set +e
    cleanup_owned || cleanup=$?
    if [[ -n $observer_cg && -f $evidence/observer.initial ]]; then
        snapshot "$observer_cg" "$evidence/observer.final" || publication=1
        check_limits "$evidence/observer.final" 268435456 1 0 || publication=1
        check_events "$evidence/observer.initial" "$evidence/observer.final" || publication=1
    else
        publication=1
    fi
    rc=98
    if (( verified==1 && original==0 && cleanup==0 && publication==0 )); then
        rc=$native_rc
        status=NATIVE_PHASE_CLOSED
        (( rc==0 )) || status=NATIVE_NONZERO_EXIT
    fi
    jq -n --arg phase "$phase" --arg stage "$stage" --arg status "$status" --arg record_sha256 "$record_sha" \
      --argjson native_exit "$native_rc" --argjson original_exit "$original" --argjson intended_exit "$rc" \
      --argjson cleanup_exit "$cleanup" --argjson failed_line "$failed_line" \
      '{schema:"borsuk-retained-native-phase-v1",phase:$phase,stage:$stage,status:$status,record_sha256:$record_sha256,
        native_exit:$native_exit,original_exit:$original_exit,intended_exit:$intended_exit,cleanup_exit:$cleanup_exit,
        failed_line:$failed_line,actual_outer_manager_exit_required:true,quality_claim:false,performance_claim:false}' \
      > "$evidence/terminal.json" || publication=1
    (cd "$evidence" && find . -type f ! -name closure.sha256 ! -name wrapper.exit -print0 | sort -z | xargs -0 sha256sum --) \
      > "$evidence/closure.sha256" || publication=1
    sync -f "$evidence" || publication=1
    (( publication==0 )) || rc=98
    printf '%s\n' "$rc" > "$evidence/wrapper.exit" || rc=98
    sync -f "$evidence" || rc=98
    exit "$rc"
}
trap finish EXIT
trap 'failed_line=$LINENO' ERR
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
cfg() { [[ $1 == ".phases.$phase.timeout_seconds" ]] || die 'unexpected phase lookup'; printf '%s\n' "$phase_seconds"; }
sample_disk() { stat -f -c 'fs_block=%S blocks=%b free=%f avail=%a' "$evidence" > "$evidence/disk.$1.txt"; }

# Phase mechanics copied from SHA27ea601a; only fixed resource selection and obsolete negative-fixture branch differ.
manager() {
    local remaining budget call rc
    remaining=$((manager_deadline-SECONDS))
    (( remaining >= 2 )) || { printf 'manager deadline exhausted\n' >&2; return 98; }
    budget=$((remaining-1)); (( budget <= 2 )) || budget=2
    call=$evidence/manager-calls/$(< /proc/sys/kernel/random/uuid)
    mkdir -- "$call" || return 98
    printf '%q ' "$@" > "$call/argv" || return 98
    printf 'timeout_seconds=%s kill_grace_seconds=1 total_deadline=%s\n' "$budget" "$manager_deadline" > "$call/deadline" || return 98
    # Applies only to the manager client; its timeout is not evidence of payload termination.
    if prlimit --core=0:0 --fsize=1048576:1048576 -- timeout --signal=TERM --kill-after=1 "$budget" "$@" > "$call/stdout" 2> "$call/stderr"; then rc=0; else rc=$?; fi
    printf '%s\n' "$rc" > "$call/exit" || return 98
    cat "$call/stdout" || return 98
    cat "$call/stderr" >&2 || return 98
    return "$rc"
}

# All controls are restricted to the same random unit, InvocationID and ControlGroup.
owned() {
    local invocation description control_group
    [[ -n $owned_unit && -n $owned_id && -n $owned_cg ]] || return 1
    invocation=$(manager systemctl show "$owned_unit" -p InvocationID --value) || return 1
    description=$(manager systemctl show "$owned_unit" -p Description --value) || return 1
    control_group=$(manager systemctl show "$owned_unit" -p ControlGroup --value) || return 1
    [[ $invocation == "$owned_id" && $description == "$owned_unit" &&
       $control_group == "${owned_cg#/sys/fs/cgroup}" ]]
}
drained() {
    [[ ! -e $owned_cg && ! -L $owned_cg ]] && return 0
    [[ -d $owned_cg && ! -L $owned_cg && -r $owned_cg/cgroup.events ]] || return 1
    local events
    events=$(cat "$owned_cg/cgroup.events") || return 1
    [[ $events == *'populated 0'* ]]
}
capture_owned_drain() {
    local dest=$1 rc=0 invocation description reported
    [[ -n $owned_unit && $owned_id =~ ^[0-9a-f]{32}$ &&
       $owned_cg == "/sys/fs/cgroup/system.slice/$owned_unit" && ! -L $owned_cg ]] || return 1
    if [[ -e $owned_cg ]]; then
        [[ -d $owned_cg && $(realpath -e "$owned_cg") == "$owned_cg" ]] || return 1
        cat "$owned_cg/cgroup.events" > "$dest" 2> "$dest.read.stderr" || rc=$?
        printf '%s\n' "$rc" > "$dest.read.exit" || return 1
        if ((rc==0)); then
            awk '$1=="populated" {if(NF!=2 || $2!="0" || n++) bad=1} END {if(bad || n!=1) exit 1}' "$dest" || return 1
            drain_state=empty
            return 0
        fi
    fi
    # A read failure is acceptable only as authenticated original-path removal.
    [[ ! -e $owned_cg && ! -L $owned_cg ]] || return 1
    invocation=$(manager systemctl show "$owned_unit" -p InvocationID --value) || return 1
    description=$(manager systemctl show "$owned_unit" -p Description --value) || return 1
    reported=$(manager systemctl show "$owned_unit" -p ControlGroup --value) || return 1
    [[ $invocation == "$owned_id" && $description == "$owned_unit" &&
       ( $reported == '' || $reported == "${owned_cg#/sys/fs/cgroup}" ) &&
       ! -e $owned_cg && ! -L $owned_cg ]] || return 1
    drain_state=removed
}
cleanup_owned() {
    local end cleanup_rc=0
    [[ -n $owned_unit ]] || return 0
    manager_deadline=$((SECONDS+40))
    if ! owned; then
        [[ ! -e $owned_cg && ! -L $owned_cg ]] || return 1
        capture_owned_drain "$phase_dir/cleanup.drain.events" || return 1
        jq -n --arg state "$drain_state" --arg path "$owned_cg" --arg id "$owned_id" \
          '{schema:"borsuk-native-pid128-payload-drain-v1",state:$state,path:$path,invocation_id:$id}' > "$phase_dir/cleanup.drain.json" || return 1
        manager systemctl stop "$owned_unit" > "$phase_dir/cleanup.stop.log" 2>&1
        local removed_stop_rc=$?
        printf '%s\n' "$removed_stop_rc" > "$phase_dir/cleanup.stop.exit" || return 1
        ((removed_stop_rc==0)) || return 1
        owned_unit='' owned_id='' owned_cg=''
        return 0
    fi
    snapshot "$owned_cg" "$phase_dir/resources.cleanup" || cleanup_rc=1
    manager systemctl kill --kill-whom=all --signal=TERM "$owned_unit" > "$phase_dir/cleanup.term.log" 2>&1
    printf '%s\n' "$?" > "$phase_dir/cleanup.term.exit" || cleanup_rc=1
    end=$((SECONDS+10)); (( end <= manager_deadline )) || end=$manager_deadline
    while ! drained && (( SECONDS < end )); do sleep 0.05; done
    if ! drained; then
        owned || return 1
        printf '1\n' >| "$owned_cg/cgroup.kill" || return 1
        printf 'cgroup.kill=1\n' > "$phase_dir/cleanup.kill.txt" || cleanup_rc=1
    fi
    end=$manager_deadline
    while ! drained && (( SECONDS < end )); do sleep 0.05; done
    drained || return 1
    capture_owned_drain "$phase_dir/cleanup.drain.events" || return 1
    cleanup_drain_state=$drain_state
    jq -n --arg state "$cleanup_drain_state" --arg path "$owned_cg" --arg id "$owned_id" \
      '{schema:"borsuk-native-pid128-payload-drain-v1",state:$state,path:$path,invocation_id:$id}' > "$phase_dir/cleanup.drain.json" || cleanup_rc=1
    manager systemctl show "$owned_unit" > "$phase_dir/manager.cleanup.txt" || cleanup_rc=1
    manager systemctl stop "$owned_unit" > "$phase_dir/cleanup.stop.log" 2>&1
    local stop_rc=$?
    printf '%s\n' "$stop_rc" > "$phase_dir/cleanup.stop.exit" || cleanup_rc=1
    (( stop_rc == 0 )) || cleanup_rc=1
    owned_unit='' owned_id='' owned_cg=''
    return "$cleanup_rc"
}

snapshot() {
    local dir=$1 output=$2 name value
    local -a args
    while :; do
        args=(--arg path "$dir")
        for name in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max pids.current pids.peak pids.events \
            memory.current memory.peak memory.events memory.swap.current memory.swap.peak memory.swap.events cgroup.events cgroup.threads cgroup.procs; do
            value=absent
            if [[ -r $dir/$name ]]; then value=$(< "$dir/$name") || return 1;
            elif [[ $dir != /sys/fs/cgroup ]]; then printf 'missing counter: %s/%s\n' "$dir" "$name" >&2; return 1; fi
            args+=(--arg "${name//./_}" "$value")
        done
        jq -cn "${args[@]}" '$ARGS.named' || return 1
        [[ $dir != /sys/fs/cgroup ]] || break
        dir=${dir%/*}
    done > "$output"
}
check_limits() {
    jq -es --argjson memory "$2" --argjson cores "$3" --arg cpus "$4" '
      def cap($k): [.[]|.[$k]|select(.!="absent" and .!="max")|tonumber]|min;
      (cap("memory_max")==$memory and cap("memory_swap_max")==0 and cap("pids_max")==128) and
      ([.[]|.cpu_max|select(.!="absent")|split(" ")|select(.[0]!="max")|(.[0]|tonumber)/(.[1]|tonumber)]|min)==$cores and
      .[0].cpuset_cpus_effective==$cpus and (.[0].memory_peak|tonumber)<=$memory and
      (.[0].memory_swap_current|tonumber)==0 and (.[0].memory_swap_peak|tonumber)==0 and
      (.[0].pids_peak|tonumber)<=128' "$1" > "$1.validated"
}
check_events() {
    jq -nes --slurpfile b "$1" --slurpfile a "$2" '
      def events: split("\n")|map(select(length>0)|split(" ")|{key:.[0],value:(.[1]|tonumber)})|from_entries;
      def limits: {path,cpu_max,cpuset_cpus_effective,memory_max,memory_swap_max,pids_max};
      ($a|length)==($b|length) and all(range(0;$a|length); . as $i |
        ($a[$i]|limits)==($b[$i]|limits) and $a[$i].pids_events==$b[$i].pids_events and
        (if $a[$i].memory_events=="absent" then $b[$i].memory_events=="absent" else
          ($a[$i].memory_events|events|{oom,oom_kill,oom_group_kill})==($b[$i].memory_events|events|{oom,oom_kill,oom_group_kill}) end) and
        (if $a[$i].memory_swap_current=="absent" then true else ($a[$i].memory_swap_current|tonumber)==0 and
          ($a[$i].memory_swap_peak|tonumber)==0 end))' > "$2.events-valid"
}

# Reused layered time/timeout/native/log-writer collection; each layer writes its own actual exit.
IFS= read -r -d '' INNER <<'EOF' || true
set -u -o noclobber
dir=$1 errfd=$2
shift 2
bash -c 'printf "%s\n" "$$" > "$1" || exit 98; shift; exec "$@"' _ "$dir/native.pid" "$@" 2>&"$errfd"
rc=$?
printf '%s\n' "$rc" > "$dir/native.exit" || exit 98
exit "$rc"
EOF
IFS= read -r -d '' OUTER <<'EOF' || true
set -u -o noclobber
inner=$1 dir=$2 errfd=$3 secs=$4
shift 4
timeout --signal=TERM --kill-after=10 "$secs" bash -c "$inner" _ "$dir" "$errfd" "$@"
rc=$?
printf '%s\n' "$rc" > "$dir/timeout.exit" || exit 98
exit "$rc"
EOF
IFS= read -r -d '' PAYLOAD <<'EOF' || true
set -u -o pipefail -o noclobber
umask 077
dir=$1 secs=$2 inner=$3 outer=$4
shift 4
printf '%s\n' "$$" > "$dir/ready" || exit 98
until [[ -f $dir/release ]]; do (( SECONDS < 30 )) || exit 98; sleep 0.05; done
exec {ef}> >(exec prlimit --core=0:0 --fsize=1048576:1048576 -- tee "$dir/native.stderr.txt" >/dev/null)
ep=$!
exec {sf}> >(exec prlimit --core=0:0 --fsize=1048576:1048576 -- tee "$dir/supervisor.stderr.txt" >/dev/null)
sp=$!
exec {tf}> >(exec prlimit --core=0:0 --fsize=1048576:1048576 -- tee "$dir/native.time.txt" >/dev/null)
tp=$!
/usr/bin/time -v -o "/dev/fd/$tf" bash -c "$outer" _ "$inner" "$dir" "$ef" "$secs" "$@" 2>&"$sf" |
    prlimit --core=0:0 --fsize=1048576:1048576 -- tee "$dir/native.stdout" >/dev/null
ps=("${PIPESTATUS[@]}")
printf '%s\n' "${ps[0]}" > "$dir/time.exit" || exit 98
printf '%s\n' "${ps[1]}" > "$dir/tee.exit" || exit 98
exec {tf}>&- {sf}>&- {ef}>&-
wait "$tp"; printf '%s\n' "$?" > "$dir/time-log.exit" || exit 98
wait "$sp"; printf '%s\n' "$?" > "$dir/supervisor-stderr-log.exit" || exit 98
wait "$ep"; printf '%s\n' "$?" > "$dir/native-stderr-log.exit" || exit 98
rc=98
if [[ -f $dir/native.exit && $(< "$dir/native.exit") =~ ^[0-9]+$ ]]; then
    native_rc=$(< "$dir/native.exit")
    if (( native_rc <= 255 )); then
        rc=$native_rc
        for f in timeout time; do
            [[ -f $dir/$f.exit && $(< "$dir/$f.exit") == "$native_rc" ]] || rc=98
        done
        for f in tee time-log supervisor-stderr-log native-stderr-log; do
            [[ -f $dir/$f.exit && $(< "$dir/$f.exit") == 0 ]] || rc=98
        done
    fi
fi
printf '%s\n' "$rc" > "$dir/payload.exit" || exit 98
printf 'done\n' > "$dir/done" || exit 98
end=$((SECONDS+30))
until [[ -f $dir/close ]]; do (( SECONDS < end )) || exit 98; sleep 0.05; done
exit "$rc"
EOF
mono_cs() { local up rest; read -r up rest < /proc/uptime; printf '%s\n' "${up/./}"; }
observe() {
    local end=$1 fd=$2 now previous gap pid=none tasks threads current peak line written=0 samples=0 membership procstat
    local -a task_paths
    previous=$(mono_cs)
    while [[ ! -f $phase_dir/done ]]; do
        now=$(mono_cs); gap=$((now-previous)); previous=$now
        (( now < end )) || die 'observer deadline; no retry'
        [[ -d $owned_cg ]] || die 'payload cgroup vanished before final counters'
        current=$(< "$owned_cg/pids.current"); peak=$(< "$owned_cg/pids.peak"); threads=$(< "$owned_cg/cgroup.threads")
        tasks=none
        membership=absent procstat=absent
        if [[ -f $phase_dir/native.pid ]]; then
            pid=$(< "$phase_dir/native.pid")
            [[ $pid =~ ^[1-9][0-9]*$ ]] || die 'native PID'
            if [[ -d /proc/$pid/task ]]; then
                if ! membership=$(< "/proc/$pid/cgroup"); then membership=exited; fi
                if ! procstat=$(< "/proc/$pid/stat"); then procstat=exited; fi
                if [[ $membership == "0::${owned_cg#/sys/fs/cgroup}" ]]; then
                    task_paths=(/proc/"$pid"/task/[0-9]*)
                    tasks=${task_paths[*]}
                else tasks='membership-gone'; fi
            else tasks=exited; fi
        fi
        printf -v line 'mono_cs=%s gap_cs=%s pids.current=%s pids.peak=%s native_pid=%s tasks=%s threads=%s membership=%s procstat=%s\n' \
            "$now" "$gap" "$current" "$peak" "$pid" "$tasks" "${threads//$'\n'/,}" "${membership//$'\n'/,}" "$procstat"
        written=$((written+${#line})); samples=$((samples+1))
        (( written <= 16777216 && samples <= 80000 )) || die 'observer evidence cap'
        printf '%s' "$line" >&"$fd"
        sleep 0.05
    done
    printf 'samples=%s bytes=%s cadence_ms=50 timestamps=proc_uptime_centiseconds\n' "$samples" "$written" > "$phase_dir/observer.txt"
}
run_phase() {
    local name=$1 cap=$2 secs memory cores cpus token manager_rc sample_fd end cg main f load_state native_rc expected_result
    shift 2
    secs=$(cfg ".phases.$name.timeout_seconds")
    [[ $cap == 1048576 ]] || die 'phase stdout cap must remain 1MiB'
    memory=$phase_memory; cores=1; cpus=0
    stage=$name
    phase_dir=$evidence/phases/$name
    mkdir -- "$phase_dir"
    printf '%s\n' "$secs" > "$phase_dir/timeout.seconds"
    sample_disk "phase-$name-before"
    (( SECONDS + secs + 80 < whole_deadline )) || die 'phase cannot fit whole-stage deadline'
    manager_deadline=$((SECONDS+secs+80))
    jq -n --args '$ARGS.positional' -- "$@" > "$phase_dir/argv.json"
    printf 'environment=env-i PATH=/usr/bin:/bin LC_ALL=C HOME=/root\n' > "$phase_dir/environment.txt"
    token=$(< /proc/sys/kernel/random/uuid)
    owned_unit=borsuk-pid128-$token-$name.service
    load_state=$(manager systemctl show "$owned_unit" -p LoadState --value) || die 'unit existence response failed'
    [[ $load_state == not-found ]] || die 'unit already exists'
    printf '%s\n' "$owned_unit" > "$phase_dir/unit"
    set +e
    manager systemd-run --expand-environment=no --quiet --unit="$owned_unit" --description="$owned_unit" --service-type=exec \
        -p CPUQuota="$((cores*100))%" -p AllowedCPUs="$cpus" -p MemoryMax="$memory" -p MemorySwapMax=0 -p TasksMax=128 \
        -p KillMode=control-group -p OOMPolicy=continue -p TimeoutStopSec=10 -p RuntimeMaxSec="$((secs+80))" -p RemainAfterExit=yes \
        -p "BindsTo=${observer_relative##*/}" -p "After=${observer_relative##*/}" \
        /usr/bin/env -i PATH=/usr/bin:/bin LC_ALL=C HOME=/root /bin/bash -c "$PAYLOAD" _ "$phase_dir" "$secs" "$INNER" "$OUTER" "$@" \
        > "$phase_dir/manager.start.stdout" 2> "$phase_dir/manager.start.stderr"
    manager_rc=$?
    set -e
    printf '%s\n' "$manager_rc" > "$phase_dir/manager.start.exit"
    owned_id=$(manager systemctl show "$owned_unit" -p InvocationID --value)
    cg=$(manager systemctl show "$owned_unit" -p ControlGroup --value)
    [[ $owned_id =~ ^[0-9a-f]{32}$ && $cg == /system.slice/"$owned_unit" ]] || die 'manager ownership receipt'
    owned_cg=/sys/fs/cgroup$cg
    owned || die 'unit ownership mismatch'
    [[ $manager_rc == 0 ]] || die 'manager start failed'
    canonical "$owned_cg"
    [[ $owned_cg != "$observer_cg" && $owned_cg != "$observer_cg/"* && $observer_cg != "$owned_cg/"* ]] || die 'observer overlaps payload'
    end=$((SECONDS+20))
    until [[ -f $phase_dir/ready ]]; do (( SECONDS < end )) || die 'payload not ready'; sleep 0.05; done
    main=$(manager systemctl show "$owned_unit" -p MainPID --value)
    [[ $main == "$(< "$phase_dir/ready")" && $(< "/proc/$main/cgroup") == "0::$cg" ]] || die 'ready PID membership'
    manager systemctl show "$owned_unit" > "$phase_dir/manager.initial.txt"
    snapshot "$owned_cg" "$phase_dir/resources.initial"
    check_limits "$phase_dir/resources.initial" "$memory" "$cores" "$cpus"
    [[ -w $owned_cg/cgroup.kill ]] || die 'owned cgroup.kill unavailable'
    exec {sample_fd}> "$phase_dir/samples.txt"
    mono_cs > "$phase_dir/release.uptime_cs"
    printf 'release\n' > "$phase_dir/release"
    observe "$(( $(mono_cs) + (secs+15)*100 ))" "$sample_fd"
    exec {sample_fd}>&-
    snapshot "$owned_cg" "$phase_dir/resources.final"
    # Capture every original manager status before allowing the cgroup holder to exit.
    manager systemctl show "$owned_unit" > "$phase_dir/manager.before-close.txt"
    printf 'close\n' > "$phase_dir/close"
    end=$((SECONDS+30)); (( end <= manager_deadline )) || end=$manager_deadline
    until drained; do (( SECONDS < end )) || die 'owned descendants failed to drain'; sleep 0.05; done
    capture_owned_drain "$phase_dir/drain.events" || die 'original cgroup drain not authenticated'
    mono_cs > "$phase_dir/drain.uptime_cs"
    jq -n --arg state "$drain_state" --arg path "$owned_cg" --arg id "$owned_id" \
      '{schema:"borsuk-native-pid128-payload-drain-v1",state:$state,path:$path,invocation_id:$id}' > "$phase_dir/drain.json"
    # Empty cgroup can precede the manager's final execution fields. Preserve
    # ownership and wait inside the existing phase/manager deadline before stop.
    end=$((SECONDS+5)); (( end <= manager_deadline )) || end=$manager_deadline
    local original_manager_deadline=$manager_deadline
    manager_deadline=$end
    while :; do
        manager systemctl show "$owned_unit" >| "$phase_dir/manager.final.poll"
        [[ $(sed -n 's/^InvocationID=//p' "$phase_dir/manager.final.poll") == "$owned_id" &&
           $(sed -n 's/^Description=//p' "$phase_dir/manager.final.poll") == "$owned_unit" ]] || die 'manager final ownership changed'
        final_cg=$(sed -n 's/^ControlGroup=//p' "$phase_dir/manager.final.poll")
        [[ $final_cg == "${owned_cg#/sys/fs/cgroup}" ||
           ( $final_cg == '' && ! -e $owned_cg && ! -L $owned_cg ) ]] || die 'original terminal cgroup changed'
        if [[ $(sed -n 's/^MainPID=//p' "$phase_dir/manager.final.poll") == 0 &&
              $(sed -n 's/^ExecMainCode=//p' "$phase_dir/manager.final.poll") == 1 &&
              $(sed -n 's/^ActiveState=//p' "$phase_dir/manager.final.poll") =~ ^(active|failed)$ &&
              $(sed -n 's/^SubState=//p' "$phase_dir/manager.final.poll") =~ ^(exited|failed)$ ]]; then break; fi
        (( SECONDS < end )) || die 'manager terminal-state deadline'
        sleep 0.05
    done
    cp "$phase_dir/manager.final.poll" "$phase_dir/manager.final.txt"
    manager_deadline=$original_manager_deadline
    [[ $(sed -n 's/^InvocationID=//p' "$phase_dir/manager.final.txt") == "$owned_id" ]] || die 'manager invocation changed'
    set +e
    manager systemctl stop "$owned_unit" > "$phase_dir/manager.stop.log" 2>&1
    manager_rc=$?
    set -e
    printf '%s\n' "$manager_rc" > "$phase_dir/manager.stop.exit"
    [[ $manager_rc == 0 ]] || die 'manager stop failed'
    drained || die 'manager drain failed'
    owned_unit='' owned_id='' owned_cg=''
    # Record final resource checks before classifying an intended failed payload.
    check_limits "$phase_dir/resources.final" "$memory" "$cores" "$cpus"
    check_events "$phase_dir/resources.initial" "$phase_dir/resources.final"
    [[ -f $phase_dir/native.exit && $(< "$phase_dir/native.exit") =~ ^[0-9]+$ ]] || die "$name: missing/invalid original native exit"
    native_rc=$(< "$phase_dir/native.exit")
    (( native_rc <= 255 )) || die 'native exit range'
    expected_result=exit-code; (( native_rc != 0 )) || expected_result=success
    [[ $(sed -n 's/^Result=//p' "$phase_dir/manager.final.txt") == "$expected_result" &&
       $(sed -n 's/^ExecMainCode=//p' "$phase_dir/manager.final.txt") == 1 &&
       $(sed -n 's/^ExecMainStatus=//p' "$phase_dir/manager.final.txt") == "$native_rc" ]] || die 'manager/native original exit mismatch'
    for f in timeout time payload; do
        [[ -f $phase_dir/$f.exit && $(< "$phase_dir/$f.exit") == "$native_rc" ]] || die "$name: missing/mismatched $f exit"
    done
    for f in tee time-log supervisor-stderr-log native-stderr-log; do
        [[ -f $phase_dir/$f.exit && $(< "$phase_dir/$f.exit") == 0 ]] || die "$name: missing/nonzero $f exit"
    done
    awk -F ': ' -v cap="$((memory/1024))" '/Maximum resident set size \(kbytes\)/ {n++; if ($2<=0 || $2>cap) bad=1} END {exit(n!=1 || bad)}' "$phase_dir/native.time.txt"
    sample_disk "phase-$name-after"
}

observer_relative=$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ $observer_relative == /system.slice/*.service && $observer_relative != *..* ]] || die 'observer must be a system service'
observer_cg=/sys/fs/cgroup$observer_relative
canonical "$observer_cg"
snapshot "$observer_cg" "$evidence/observer.initial"
check_limits "$evidence/observer.initial" 268435456 1 0
whole_deadline=$((SECONDS+phase_seconds+100))
run_phase "$phase" 1048576 "$elf" "${native_args[@]}"
native_rc=$(< "$phase_dir/native.exit")
authenticate "$record" "$record_bytes" "$record_sha"
authenticate "$elf" "$elf_bytes" "$elf_sha"
authenticate "$native_config" "$native_config_bytes" "$native_config_sha"
if (( native_rc==0 )); then
    canonical "$native_output"
    output_bytes=$(stat -c %s "$native_output")
    (( output_bytes>0 && output_bytes<=33554432 )) || die 'native output cap'
    output_sha=$(sha256sum < "$native_output"); output_sha=${output_sha%% *}
    authenticate "$native_output" "$output_bytes" "$output_sha"
    jq -n --arg path "$native_output" --argjson bytes "$output_bytes" --arg sha256 "$output_sha" \
      '{path:$path,bytes:$bytes,sha256:$sha256,quality_interpreted:false}' > "$evidence/output.json"
fi
verified=1
stage='phase-closed'
exit 0
