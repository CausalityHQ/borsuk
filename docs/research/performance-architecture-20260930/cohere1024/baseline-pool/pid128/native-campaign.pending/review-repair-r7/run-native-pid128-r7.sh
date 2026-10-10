#!/usr/bin/env bash
# SOURCE UNVERIFIED. Root repair of preserved b6d99117; no runtime qualification.
# One fresh 100k/Q32 PID128 mechanics fixture, not a performance qualification.
# API: ROOT_CONFIG ROOT_CONFIG_SHA256 NEW_EVIDENCE. Root freezes stage=admission, then stage=widths.
# Admission stops after the real diagnostic ordinal0. Widths requires root's separately pinned admission/gate.
# Run this observer in its own CPU0/100%/256MiB/noSwap/Tasks128 cgroup, outside fresh payload services.
# Observer must be a system.slice service with RemainAfterExit=yes and a unique InvocationID.
# Root captures exit records and drain evidence before stopping the retained observer unit.
# Requires the system manager, cgroup v2 pids.peak/cgroup.kill, GNU tools and jq. No network/transport here.
# Root owns staging, cost/launch, original observer-manager/outer exits, terminal upload and instance/volume closure.
# Hashing warms page cache outside payload accounting. No exact thread-bound or causal claim.
# Timeout may prevent native.exit publication: missing raw native exit is INVALID, never synthesized.
# shellcheck disable=SC2016,SC2329 # jq programs use literal dollars; EXIT invokes closure helpers.
set -Eeuo pipefail
set -o noclobber
umask 077
export LC_ALL=C
unset TOKIO_WORKER_THREADS BORSUK_CPU_THREADS RAYON_NUM_THREADS

die() { printf 'INVALID: %s\n' "$*" >&2; exit 98; }
[[ $# == 3 ]] || die 'usage: ROOT_CONFIG ROOT_CONFIG_SHA256 NEW_EVIDENCE'
(( EUID == 0 )) || die 'root-owned bounded observer and system-manager permission required'
config=$1 config_sha=$2 evidence=$3
readonly ROOT=/mnt/borsuk-pool-pid PARENT=/mnt/borsuk-pool-pid/prepared-parent
readonly BASE=3cee684acc5f8d90bebe26c59262f0741f7a9bad
readonly CORPUS_SHA=3c95fa49a7d3f9d4bf6178f5ac2493e700a30fbcfe91da97a5fcf16a1f5fc09c
readonly RESERVED_SHA=8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e
readonly AUTHORITY='{"source_commit":"bc3082a8210c4370ebadae4ba093a7c211072b62","executable_sha256":"4f502be7d15a064dba525816c63ad471c3ad2fe616ede14712b67cdb85e3cf66","producer_source_sha256":"a7ba542c3ada061002b09e638a68423236d5499d11aae0e0083cd6e52d38adfd","sq8_source_sha256":"16f03aa7187b4f18676aa511339b07b6e7b8776323710a60071e6ea0d62eb795","source_order_source_sha256":"1393f99e71be1e68916b598256d1200d56879b439c05d8c66c098345856c9eb4"}'
readonly ASSETS='[
 ["input/en/0000.parquet",216612385,"2c6abfffa7dd336113251b3e6f3fe4ee16688ead7c67b99593cbadc5589e28b3"],
 ["input/en/0001.parquet",216746705,"1ce373d5730494429a64a0f788c62f71226ded2bc670aaf9399204e91e544c3b"],
 ["assets/bin/prepare_cohere_native_cohort",10015160,"a3a828beec8d898fdf1d26a3b6673ea16be5407da864222dc9a394c2d28221fb"],
 ["assets/bin/build_sq8_source",4273936,"4f502be7d15a064dba525816c63ad471c3ad2fe616ede14712b67cdb85e3cf66"],
 ["assets/bin/build_two_bit_generation",4444504,"73c0d7da26dc5c52f7b548d04590221b13e0c883af5e0a10990bfae7268b01fe"],
 ["assets/bin/publish_two_bit_generation",15964832,"076afd99e6dc6c814461161fd3bab0fd7bc1ab4db68c1337c8c98137fef15982"],
 ["assets/bin/check_cohere_native_baseline",17029632,"59fe47aa1001b3ca24d1f9ff31444f97fcda72e3e297c8d7d846f5c3d811bfc3"]]'
readonly LIMITS='{"max_memory_bytes":536870912,"max_active_queries":1,"max_query_bytes":16773120,"max_query_gets":32,"max_parallel_gets":16,"max_source_bytes":67108864,"max_source_gets":128,"max_parallel_source_gets":16,"max_query_scratch_bytes":594520,"already_pinned_bytes":0}'
readonly JQ_TYPES='def keys_are($k): type=="object" and keys==($k|sort);
 def uint: type=="number" and .>=0 and .<=9007199254740991 and floor==.;
 def sha: type=="string" and test("^[0-9a-f]{64}$");
 def art: keys_are(["path","bytes","sha256"]) and (.path|type=="string" and startswith("/")) and (.bytes|uint and .>0) and (.sha256|sha);'
for tool in jq sha256sum stat realpath timeout tee cmp sync sed awk mkdir find sort xargs prlimit systemd-run systemctl sleep head env cp du cat; do
    command -v "$tool" >/dev/null || die "missing $tool"
done
[[ -x /usr/bin/time && -x /usr/bin/dd && $config_sha =~ ^[0-9a-f]{64}$ ]] || die 'time/dd/SHA required'
canonical() {
    [[ $1 == /* && $1 != *[$'\t\r\n\\']* && $(realpath -e -- "$1") == "$1" ]] || die "noncanonical: $1"
}
new_path() {
    [[ $1 == /* && $1 != */ && ${1##*/} != . && ${1##*/} != .. && ! -e $1 && ! -L $1 ]] || die "occupied: $1"
    canonical "${1%/*}"
}
sha_of() { local digest; digest=$(sha256sum < "$1"); printf '%s\n' "${digest%% *}"; }
authenticate() {
    local path=$1 bytes=$2 sha=$3 stamp
    canonical "$path"
    [[ -f $path && ! -L $path && $bytes =~ ^[1-9][0-9]*$ && $sha =~ ^[0-9a-f]{64}$ ]] || die "file pin: $path"
    stamp=$(stat -c '%d:%i:%s:%y:%z' -- "$path")
    [[ $(stat -c %s -- "$path") == "$bytes" && $(sha_of "$path") == "$sha" && $(stat -c '%d:%i:%s:%y:%z' -- "$path") == "$stamp" ]] || die "identity drift: $path"
    jq -cn --arg path "$path" --argjson bytes "$bytes" --arg sha256 "$sha" --arg stamp "$stamp" \
        '{path:$path,bytes:$bytes,sha256:$sha256,stamp:$stamp}'
}
descriptor() { authenticate "$1" "$(stat -c %s -- "$1")" "$(sha_of "$1")" | jq 'del(.stamp)'; }
small_json() {
    canonical "$1"
    [[ -f $1 && ! -L $1 ]] || die 'JSON regular file'
    local bytes; bytes=$(stat -c %s -- "$1")
    (( bytes > 0 && bytes <= 65536 )) || die "JSON cap: $1"
    one_object "$1" 65536
}
one_object() {
    local path=$1 cap=$2 bytes
    canonical "$path"
    [[ -f $path && ! -L $path ]] || die 'JSON object regular file'
    bytes=$(stat -c %s -- "$path")
    (( bytes > 0 && bytes <= cap )) || die 'JSON object byte cap'
    jq -es 'length==1 and (.[0]|type=="object")' "$path" >/dev/null || die 'exactly one JSON object required'
}
auth_art() {
    local art=$1 path bytes sha
    path=$(jq -er .path <<< "$art"); bytes=$(jq -er .bytes <<< "$art"); sha=$(jq -er .sha256 <<< "$art")
    authenticate "$path" "$bytes" "$sha"
}
new_path "$evidence"
canonical "$ROOT"
[[ $evidence != "$ROOT" && $evidence != "$ROOT/"* && $ROOT != "$evidence/"* ]] || die 'evidence must be outside asset/scratch root'
# Reject admission namespace overlap before any evidence file can be created.
small_json "$config"
authenticate "$config" "$(stat -c %s "$config")" "$config_sha" >/dev/null
if [[ $(jq -er .stage "$config") == widths ]]; then
    admission_namespace=$(jq -er .admission.path "$config")
    canonical "$admission_namespace"
    admission_namespace=${admission_namespace%/*}
    [[ $evidence != "$admission_namespace" && $evidence != "$admission_namespace/"* &&
       $admission_namespace != "$evidence/"* ]] || die 'evidence overlaps sealed admission namespace'
fi
mkdir -- "$evidence"
stage=admission status=INVALID failed_line=0 signal_name='' owned_unit='' owned_id='' owned_cg='' phase_dir=''
completed=()
mkdir -- "$evidence/manager-calls"
manager_deadline=$((SECONDS+40))
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
        printf '1\n' > "$owned_cg/cgroup.kill" || return 1
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
finish() {
    local rc=$? original cleanup=0
    original=$rc
    trap - EXIT ERR; trap '' HUP INT TERM
    set +e
    cleanup_owned || cleanup=$?
    if [[ $status == INVALID || $rc != 0 || $cleanup != 0 ]]; then status=INVALID; rc=98; fi
    # Closure/cleanup failures must differ from the intended negative exit98.
    (( cleanup == 0 )) || rc=97
    if [[ -n ${observer_cg:-} && -f $evidence/observer.initial ]]; then
        snapshot "$observer_cg" "$evidence/observer.closure" || rc=97
        check_limits "$evidence/observer.closure" 268435456 1 0 || rc=97
        check_events "$evidence/observer.initial" "$evidence/observer.closure" || rc=97
    else rc=97; fi
    printf 'original=%s cleanup=%s intended=%s signal=%s line=%s\n' "$original" "$cleanup" "$rc" "$signal_name" "$failed_line" > "$evidence/closure.txt" || rc=97
    jq -n --arg status "$status" --arg stage "$stage" --arg sha "$config_sha" --argjson original "$original" \
        --argjson intended "$rc" --argjson cleanup "$cleanup" --args \
        '{schema:"borsuk-native-pid128-terminal-v1",status:$status,stage:$stage,config_sha256:$sha,
          original_exit:$original,intended_exit:$intended,cleanup_exit:$cleanup,phases_completed:$ARGS.positional,
          original_observer_manager_and_outer_exits_required:true,root_instance_and_volume_closure_required:true,
          performance_claim:false,cold_claim:false,competitor_claim:false,one_million_claim:false}' \
        -- "${completed[@]}" > "$evidence/terminal.json" || rc=97
    (cd "$evidence" && find . -type f ! -path ./closure.sha256 ! -path ./wrapper.exit -print0 | sort -z | xargs -0 sha256sum --) > "$evidence/closure.sha256" || rc=97
    printf '%s\n' "$rc" > "$evidence/wrapper.exit" || rc=97
    sync -f "$evidence" || rc=97
    exit "$rc"
}
trap finish EXIT
trap 'failed_line=$LINENO' ERR
trap 'signal_name=HUP; exit 129' HUP
trap 'signal_name=INT; exit 130' INT
trap 'signal_name=TERM; exit 143' TERM

# Raw leaf + ancestor evidence survives removal of the owned leaf.
snapshot() {
    local dir=$1 output=$2 name value
    local -a args
    while :; do
        args=(--arg path "$dir")
        for name in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max pids.current pids.peak pids.events \
            memory.current memory.peak memory.events memory.swap.current memory.swap.peak memory.swap.events cgroup.events cgroup.threads cgroup.procs; do
            value=absent
            if [[ -r $dir/$name ]]; then value=$(< "$dir/$name");
            elif [[ $dir != /sys/fs/cgroup ]]; then printf 'missing counter: %s/%s\n' "$dir" "$name" >&2; return 1; fi
            args+=(--arg "${name//./_}" "$value")
        done
        jq -cn "${args[@]}" '$ARGS.named'
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
exec {ef}> >(exec prlimit --core=0:0 --fsize=1048576:1048576 -- tee "$dir/native.stderr" >/dev/null)
ep=$!
exec {sf}> >(exec prlimit --core=0:0 --fsize=1048576:1048576 -- tee "$dir/supervisor.stderr" >/dev/null)
sp=$!
exec {tf}> >(exec prlimit --core=0:0 --fsize=1048576:1048576 -- tee "$dir/native.time" >/dev/null)
tp=$!
/usr/bin/time -v -o "/dev/fd/$tf" bash -c "$outer" _ "$inner" "$dir" "$ef" "$secs" "$@" 2>&"$sf" |
    prlimit --core=0:0 --fsize=1048576:1048576 -- tee "$dir/native.stdout" >/dev/null
ps=("${PIPESTATUS[@]}")
printf '%s\n' "${ps[0]}" > "$dir/time.exit" || exit 98
printf '%s\n' "${ps[1]}" > "$dir/tee.exit" || exit 98
exec {tf}>&- {sf}>&- {ef}>&-
wait "$tp"; printf '%s\n' "$?" > "$dir/time-log.exit" || exit 98
wait "$sp"; printf '%s\n' "$?" > "$dir/supervisor-log.exit" || exit 98
wait "$ep"; printf '%s\n' "$?" > "$dir/native-log.exit" || exit 98
rc=0
for f in native timeout time tee time-log supervisor-log native-log; do
    [[ -f $dir/$f.exit && $(< "$dir/$f.exit") == 0 ]] || rc=98
done
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
        (( written <= 67108864 && samples <= 80000 )) || die 'observer evidence cap'
        printf '%s' "$line" >&"$fd"
        sleep 0.05
    done
    printf 'samples=%s bytes=%s cadence_ms=50 timestamps=proc_uptime_centiseconds\n' "$samples" "$written" > "$phase_dir/observer.txt"
}
run_phase() {
    local name=$1 secs=$2 memory=$3 cores=$4 cpus=$5 token manager_rc sample_fd end cg main f load_state
    shift 5
    stage=$name
    phase_dir=$evidence/phases/$name
    mkdir -- "$phase_dir"
    scratch_snapshot "$name" before
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
    if [[ -n ${controlled_negative_identity:-} ]]; then
        [[ $controlled_negative_identity == "$ROOT"/staging-*/native/descendant.identity &&
           -f $controlled_negative_identity && ! -L $controlled_negative_identity &&
           -f $controlled_negative_identity.procstat && ! -L $controlled_negative_identity.procstat &&
           $(< "$phase_dir/timeout.exit") =~ ^(124|137)$ ]] || die 'controlled timeout identity unavailable'
        child_pid=$(sed -n 's/^pid=//p' "$controlled_negative_identity")
        [[ $child_pid =~ ^[1-9][0-9]*$ && $(sed -n 's/^0:://p' "$controlled_negative_identity") == "${owned_cg#/sys/fs/cgroup}" ]] || die 'controlled descendant identity'
        cp "$controlled_negative_identity" "$phase_dir/child.identity"
        cp "$controlled_negative_identity.procstat" "$phase_dir/child.initial.procstat"
        cat "/proc/$child_pid/stat" > "$phase_dir/child.after-timeout.procstat"
        cat "/proc/$child_pid/cgroup" > "$phase_dir/child.after-timeout.cgroup"
        [[ $(< "$phase_dir/child.after-timeout.cgroup") == "0::${owned_cg#/sys/fs/cgroup}" ]] || die 'descendant no longer in original cgroup'
        child_start=$(awk '{s=$0;sub(/^[0-9]+ \(.*\) /,"",s);split(s,a," ");print a[20]}' "$phase_dir/child.initial.procstat")
        [[ $child_start =~ ^[1-9][0-9]*$ ]] || die 'descendant start time'
        awk -v pid="$child_pid" -v start="$child_start" '{n++; if(n!=1) exit 1; s=$0;sub(/^[0-9]+ \(.*\) /,"",s);split(s,a," ");
          if($1!=pid || a[1]!~/^(R|S|D)$/ || a[3]!=pid || a[4]!=pid || a[20]!=start) exit 1} END {if(n!=1) exit 1}' "$phase_dir/child.after-timeout.procstat" || die 'descendant not alive in separate session after timeout'
        mono_cs > "$phase_dir/child.witness.uptime_cs"
        jq -n --argjson pid "$child_pid" --arg start "$child_start" --arg cg "${owned_cg#/sys/fs/cgroup}" --arg id "$owned_id" \
          '{schema:"borsuk-native-pid128-timeout-witness-v1",pid:$pid,start_time_ticks:$start,control_group:$cg,invocation_id:$id,alive_after_timeout:true,separate_process_group_and_session:true}' > "$phase_dir/child.witness.json"
    fi
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
    [[ $(sed -n 's/^Result=//p' "$phase_dir/manager.final.txt") == success &&
       $(sed -n 's/^ExecMainCode=//p' "$phase_dir/manager.final.txt") == 1 &&
       $(sed -n 's/^ExecMainStatus=//p' "$phase_dir/manager.final.txt") == 0 ]] || die 'manager native closure'
    for f in native timeout time tee time-log supervisor-log native-log payload; do
        [[ -f $phase_dir/$f.exit && $(< "$phase_dir/$f.exit") == 0 ]] || die "$name: missing/nonzero $f exit"
    done
    awk -F ': ' -v cap="$((memory/1024))" '/Maximum resident set size \(kbytes\)/ {n++; if ($2<=0 || $2>cap) bad=1} END {exit(n!=1 || bad)}' "$phase_dir/native.time"
    scratch_snapshot "$name" after
    completed+=("$name")
}

# Source/manifest authority is external; a pending template is never launch authority.
small_json "$config"
authenticate "$config" "$(stat -c %s -- "$config")" "$config_sha" > "$evidence/root-config.identity"
cp -- "$config" "$evidence/root-config.json"
config_copy=$evidence/root-config.json
authenticate "$config_copy" "$(stat -c %s -- "$config")" "$config_sha" > "$evidence/root-config.copy-identity"
jq -es "$JQ_TYPES
 length==1 and (.[0] | keys_are([\"schema\",\"status\",\"stage\",\"recipe_sha256\",\"input_manifest\",\"cohort_template\",\"derivation_template\",\"scratch\",\"root_gate\",\"admission\",\"admission_closure\"]) and
 .schema==\"borsuk-native-pid128-config-v2\" and .status==\"ROOT_FROZEN\" and (.stage==\"admission\" or .stage==\"widths\") and
 (.recipe_sha256|sha) and ([.input_manifest,.cohort_template,.derivation_template,.root_gate]|all(.[];art)) and
 (.scratch | . as \$s | keys_are([\"caller_scratch_bytes\",\"max_aggregate_scratch_bytes\",\"minimum_free_bytes\",\"total_cap_bytes\",\"evidence_reserve_bytes\",\"evidence_root\",\"phase_peak_bytes\",\"phase_transient_reserve_bytes\"]) and
   ([.caller_scratch_bytes,.max_aggregate_scratch_bytes,.minimum_free_bytes,.total_cap_bytes,.evidence_reserve_bytes]|all(.[];uint and .>0 and .<=17179869184)) and
   (.evidence_root|type==\"string\" and startswith(\"/\")) and
   (.phase_peak_bytes|keys_are([\"prepare\",\"derive\",\"copy\",\"generation\",\"publish\",\"diagnostic\",\"query16\",\"query32\"]) and all(.[];uint and .>0 and .<=\$s.total_cap_bytes)) and
   (.phase_transient_reserve_bytes|keys_are([\"prepare\",\"derive\",\"copy\",\"generation\",\"publish\",\"diagnostic\",\"query16\",\"query32\"]) and all(.[];uint and .>0 and .<=\$s.total_cap_bytes))) and
 (if .stage==\"admission\" then .admission==null and .admission_closure==null else
   (.admission|art) and (.admission_closure|keys_are([\"terminal\",\"manifest\",\"wrapper_exit\",\"outer\"]) and all(.[];art)) end))" "$config_copy" > "$evidence/root-config.validated"
cfg() { jq -er "$1" "$config_copy"; }
mode=$(cfg .stage)
campaign_evidence_root=$(cfg .scratch.evidence_root)
canonical "$campaign_evidence_root"
[[ -d $campaign_evidence_root && $evidence == "$campaign_evidence_root/"* &&
   $ROOT != "$campaign_evidence_root" && $ROOT != "$campaign_evidence_root/"* &&
   $campaign_evidence_root != "$ROOT/"* ]] || die 'disjoint common campaign evidence root required'
payload_fs_device=$(stat -c %d -- "$ROOT") || die 'payload filesystem identity unavailable'
evidence_fs_device=$(stat -c %d -- "$campaign_evidence_root") || die 'evidence filesystem identity unavailable'
[[ $payload_fs_device =~ ^[0-9]+$ && $evidence_fs_device == "$payload_fs_device" ]] || die 'payload and evidence must share filesystem'
recipe=$(realpath -e -- "${BASH_SOURCE[0]}")
recipe_sha=$(cfg .recipe_sha256)
authenticate "$recipe" "$(stat -c %s -- "$recipe")" "$recipe_sha" > "$evidence/recipe.identity"
for key in input_manifest cohort_template derivation_template root_gate; do
    auth_art "$(cfg .$key)"
done > "$evidence/authorities.before"
manifest=$(cfg .input_manifest.path)
cohort_template=$(cfg .cohort_template.path)
derive_template=$(cfg .derivation_template.path)
gate=$(cfg .root_gate.path)
small_json "$manifest"; small_json "$cohort_template"; small_json "$derive_template"; small_json "$gate"
# Preserve the actual provided templates and receipts, including their historical provenance fields.
[[ $(cfg .cohort_template.bytes) == 1722 && $(cfg .cohort_template.sha256) == d55e2c941d3b10432e291c61a7e9d9d084da99e5e9331face0b9a17104c38820 &&
   $(cfg .derivation_template.bytes) == 1025 && $(cfg .derivation_template.sha256) == 0a8ea237c350cece31596cb194ecd14c3e003332c9d28b847389bdae87b7906a ]] || die 'template pins'
jq -e --arg stage "$mode" --arg recipe "$recipe_sha" --slurpfile c "$config_copy" "$JQ_TYPES
 keys_are([\"schema\",\"status\",\"stage\",\"recipe_sha256\",\"staging_receipt_sha256\",\"admission_sha256\",\"admission_terminal_sha256\",\"admission_closure_sha256\",\"admission_outer_sha256\"]) and
 .schema==\"borsuk-native-pid128-root-gate-v2\" and .status==\"PASS\" and .stage==\$stage and .recipe_sha256==\$recipe and
 (if \$stage==\"admission\" then .staging_receipt_sha256==null and
   .admission_terminal_sha256==null and .admission_closure_sha256==null and .admission_outer_sha256==null else
   (.staging_receipt_sha256|sha) and .admission_terminal_sha256==\$c[0].admission_closure.terminal.sha256 and
   .admission_closure_sha256==\$c[0].admission_closure.manifest.sha256 and
   .admission_outer_sha256==\$c[0].admission_closure.outer.sha256 end) and
 .admission_sha256==\$c[0].admission.sha256" "$gate" > "$evidence/root-gate.validated"
jq -e --arg root "$ROOT" --argjson assets "$ASSETS" "$JQ_TYPES
 keys_are([\"schema\",\"status\",\"assets\",\"producer_admission\",\"baseline_admission\"]) and
 .schema==\"borsuk-native-pid128-inputs-v1\" and .status==\"ROOT_FROZEN\" and
 (.producer_admission|art) and (.baseline_admission|art) and
 .producer_admission.sha256==\"f9d3490cf3de71ae837b7f1a47d7907d4bdbb1a707fc947cbf08d7f6fc646984\" and
 .baseline_admission.sha256==\"7ad7d0db4a5e4533aa617f90b9f0d728c0ab0b8e39982f9131e8daf2621dee84\" and
 (.assets|sort_by(.path))==(\$assets|map({path:(\$root+\"/\"+.[0]),bytes:.[1],sha256:.[2]})|sort_by(.path))" "$manifest" > "$evidence/manifest.validated"
jq -r '[.assets[],.producer_admission,.baseline_admission]|.[]|[.path,.bytes,.sha256]|@tsv' "$manifest" > "$evidence/inputs.tsv"
authenticate_inputs() {
    authenticate "$config" "$(stat -c %s -- "$config_copy")" "$config_sha"
    authenticate "$recipe" "$(stat -c %s -- "$recipe")" "$recipe_sha"
    for key in input_manifest cohort_template derivation_template root_gate; do auth_art "$(cfg .$key)"; done
    while IFS=$'\t' read -r path bytes sha; do authenticate "$path" "$bytes" "$sha"; done < "$evidence/inputs.tsv"
}
authenticate_inputs > "$evidence/inputs.before"
bin=$ROOT/assets/bin
for name in prepare_cohere_native_cohort build_sq8_source build_two_bit_generation publish_two_bit_generation check_cohere_native_baseline; do
    [[ -x $bin/$name ]] || die "ELF not executable: $name"
done
[[ $(stat -f -c %T /sys/fs/cgroup) == cgroup2fs ]] || die 'cgroup v2 required'
observer_relative=$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ $observer_relative == /* && $observer_relative != / && $observer_relative != *..* ]] || die 'bounded observer cgroup required'
observer_cg=/sys/fs/cgroup$observer_relative
canonical "$observer_cg"
[[ ${INVOCATION_ID:-} =~ ^[0-9a-f]{32}$ ]] || die 'original observer InvocationID required'
[[ $observer_relative =~ ^/system.slice/borsuk-pid128-observer-[a-z0-9-]+\.service$ ]] || die 'original observer service path'
jq -n --arg id "$INVOCATION_ID" --arg cg "$observer_relative" \
    '{schema:"borsuk-native-pid128-observer-identity-v1",invocation_id:$id,control_group:$cg}' > "$evidence/observer.identity.json"
snapshot "$observer_cg" "$evidence/observer.initial"
check_limits "$evidence/observer.initial" 268435456 1 0
mkdir -- "$evidence/phases"
jq -n --args '$ARGS.positional' -- "$@" > "$evidence/invocation.json"
printf 'base=%s\nrecipe_sha256=%s\n' "$BASE" "$recipe_sha" > "$evidence/source.txt"
whole_deadline=12600
[[ $mode == admission ]] || whole_deadline=2400
scratch_snapshot() {
    local name=$1 label=$2 total reserve admitted free
    # Count ALL retained gate/phase evidence. Endpoints do not measure transient peaks.
    find "$campaign_evidence_root" -type l -print > "$phase_dir/scratch.$label.symlinks"
    [[ ! -s $phase_dir/scratch.$label.symlinks ]] || die 'symlink in campaign evidence'
    du -sb -- "$ROOT" "$campaign_evidence_root" > "$phase_dir/scratch.$label.tsv"
    total=$(awk '{n+=$1} END {printf "%.0f\n",n}' "$phase_dir/scratch.$label.tsv")
    reserve=$(( $(cfg ".scratch.phase_transient_reserve_bytes.$name") + $(cfg .scratch.evidence_reserve_bytes) ))
    admitted=$((total+reserve))
    free=$(( $(stat -f -c %a "$ROOT") * $(stat -f -c %S "$ROOT") ))
    printf 'endpoint_bytes=%s transient_and_evidence_reserve_bytes=%s admitted_bytes=%s phase_cap_bytes=%s total_cap_bytes=%s free_bytes=%s measured_peak=false\n' \
        "$total" "$reserve" "$admitted" "$(cfg ".scratch.phase_peak_bytes.$name")" "$(cfg .scratch.total_cap_bytes)" "$free" > "$phase_dir/scratch.$label.txt"
    (( admitted <= $(cfg ".scratch.phase_peak_bytes.$name") && admitted <= $(cfg .scratch.total_cap_bytes) &&
       free >= reserve && free >= $(cfg .scratch.minimum_free_bytes) )) || die 'root-frozen scratch admission cap'
}

# Existing wrapper's exact binary32 decode and independent inverse guard.
IFS= read -r -d '' JQ_F32 <<'EOF' || true
def f32dec: . as $b
  | (($b / 8388608 | floor) % 256) as $e
  | ($b % 8388608) as $m
  | (if $b >= 2147483648 then -1 else 1 end) as $s
  | if $e == 255 then error("nonfinite f32")
    elif $e == 0 then $s * $m * pow(2; -149)
    else $s * ($m + 8388608) * pow(2; $e - 150) end;
def magbits: . as $a
  | if $a == 0 then 0
    elif $a < pow(2; -126) then $a * pow(2; 149)
    else ($a | log2 | floor) as $x | ($x + 127) * 8388608 + ($a / pow(2; $x) - 1) * 8388608 end;
def f32ok: . as $b | (f32dec) as $v
  | ((($v | fabs) | magbits) == ($b % 2147483648)) and ((($b >= 2147483648) == ($v < 0)) or ($v == 0));
EOF
etag_of() { # object_store 0.14.1 LocalFileSystem, quoted hex inode-mtime_microseconds-size.
    local fields ino size sec time frac
    fields=$(stat -c '%i %s %Y %y' -- "$1")
    read -r ino size sec _ time _ <<< "$fields"
    [[ $time =~ ^[0-9:]+\.([0-9]{9})$ ]] || die 'mtime precision'
    frac=${BASH_REMATCH[1]}
    printf '"%x-%x-%x"\n' "$ino" "$((sec*1000000 + 10#${frac:0:6}))" "$size"
}
cohort=$PARENT/cohort
derived=$PARENT/derived
store=$PARENT/store
generation=$PARENT/generation
publication=$PARENT/publication-receipt.json
configs=$PARENT/configs

validate_result() {
    local result=$1 native_config=$2 count=$3 dir=$4 bytes prefix_bytes sealed_bytes prefix_sha sealed_sha actual seal_line records
    canonical "$result"
    small_json "$native_config"
    bytes=$(stat -c %s -- "$result")
    (( bytes > 0 && bytes <= 67108864 )) || die 'native result byte cap'
    # Drop traces/timing payloads one record at a time: closure never calculates a performance metric.
    jq -c 'if .phase=="query" then {phase,ordinal,truth_opened,underfill,returned_count,returned}
      elif .phase=="recall" then {phase,ordinal,underfill,returned_count,hits10,recall10} else
      {phase,success,truth_opened,count,selected_count,population_count,requires_successful_sync,requires_successful_directory_sync,
       prefix_bytes,prefix_sha256,requests_sha256,generation_root_sha256,summary,
       binary_sha256,config_sha256,fetch_parallelism,execution,cohort_receipt_sha256,derivation_receipt_sha256,
       native_source_sha256,native_sq8_sha256,native_order_sha256,truth_sha256,producer_authority} end' "$result" > "$dir/result.closure-rows.jsonl"
    jq -es --argjson n "$count" --arg sha "$(sha_of "$native_config")" --slurpfile c "$native_config" '
      . as $r | $c[0] as $c |
      ([range(0;length)|select($r[.].phase=="all_queries_sealed")][0]) as $seal |
      ([.[]|select(.phase=="identity")]|length)==1 and .[0].phase=="identity" and .[0].config_sha256==$sha and
      .[0].binary_sha256=="59fe47aa1001b3ca24d1f9ff31444f97fcda72e3e297c8d7d846f5c3d811bfc3" and
      .[0].fetch_parallelism==$c.fetch_parallelism and .[0].execution==$c.execution and
      ([.[]|select(.phase=="bound_inputs")]|length)==1 and
      all(.[]|select(.phase=="bound_inputs"); .cohort_receipt_sha256==$c.cohort_receipt.sha256 and
        .derivation_receipt_sha256==$c.derivation_receipt.sha256 and .producer_authority==$c.producer_authority and
        .native_source_sha256==$c.native_source.source_sha256 and .native_sq8_sha256==$c.native_source.sq8_sha256 and
        .native_order_sha256==$c.native_source.source_order_sha256 and .requests_sha256==$c.requests.sha256 and
        .truth_sha256==$c.truth.sha256 and .generation_root_sha256==$c.generation_root_sha256) and
      ([.[]|select(.phase=="source_binding" and .success==true)]|length)==1 and
      ([.[]|select(.phase=="generation_open" and .success==true)]|length)==1 and
      ([.[]|select(.phase=="query")|.ordinal])==(if $n==1 then [0] else [range(0;32)] end) and
      all(.[]|select(.phase=="query"); .underfill==false and .returned_count==10 and .truth_opened==false and
        (.returned|length)==10 and ([.returned[].id]|unique|length)==10 and
        all(.returned[].id; type=="number" and floor==. and .>=0 and .<100000)) and
      ([.[]|select(.phase=="all_queries_sealed")]|length)==1 and
      ([$r[0:$seal][]|select(.phase=="query")]|length)==$n and
      ([$r[($seal+1):][]|.phase])==([range(0;$n)|"recall"]+["terminal"]) and
      ([$r[($seal+1):][]|select(.phase=="recall")|.ordinal])==(if $n==1 then [0] else [range(0;32)] end) and
      all($r[($seal+1):][]|select(.phase=="recall"); .underfill==false and .returned_count==10 and
        (.hits10|type=="number" and floor==. and .>=0 and .<=10) and .recall10==(.hits10/10)) and
      all($r[0:($seal+1)][]; .truth_opened==false) and
      $r[$seal].count==$n and $r[$seal].selected_count==$n and $r[$seal].population_count==32 and
      $r[$seal].requires_successful_sync==true and $r[$seal].requires_successful_directory_sync==true and
      ([.[]|select(.phase=="terminal")]|length)==1 and .[-1].phase=="terminal" and
      (.[-1].summary | .status=="MEASURED" and .complete==true and .queries==$n and .executed_count==$n and
        .selected_count==$n and .population_count==32 and .underfilled_queries==0 and .all_queries_sealed==true and
        .requests_sha256==$c.requests.sha256 and .truth_sha256==$c.truth.sha256 and
        .generation_root_sha256==$c.generation_root_sha256 and .execution==$c.execution and
        .population_percentiles_valid==false and .full_cohort_qualification==false and .physical_s3_measured==false and .external_gate_required==true and
        .prefix_bytes==$r[$seal].prefix_bytes and .prefix_sha256==$r[$seal].prefix_sha256)' \
        "$dir/result.closure-rows.jsonl" > "$dir/result.validated"
    jq -s '.[-1].summary' "$dir/result.closure-rows.jsonl" > "$dir/summary.json"
    prefix_bytes=$(jq -er .prefix_bytes "$dir/summary.json"); sealed_bytes=$(jq -er .sealed_bytes "$dir/summary.json")
    prefix_sha=$(jq -er .prefix_sha256 "$dir/summary.json"); sealed_sha=$(jq -er .sealed_sha256 "$dir/summary.json")
    [[ $prefix_bytes =~ ^[1-9][0-9]*$ && $sealed_bytes =~ ^[1-9][0-9]*$ && $prefix_sha =~ ^[0-9a-f]{64}$ && $sealed_sha =~ ^[0-9a-f]{64}$ ]] || die 'prefix seal shape'
    (( prefix_bytes < sealed_bytes && sealed_bytes < bytes )) || die 'seal bounds'
    # Parse ONE object per raw line with constant aggregate state, then bind offsets
    # to the actual seal line. Hashes at arbitrary interior byte positions do not suffice.
    jq -Rn 'reduce inputs as $line ({records:0,seals:0,seal_line:0};
      ($line|fromjson) as $row | if ($row|type)!="object" then error("JSONL object required") else
        .records+=1 | if $row.phase=="all_queries_sealed" then .seals+=1 | .seal_line=.records else . end end)
      | if .seals==1 then . else error("one seal line required") end' "$result" > "$dir/raw-record-boundaries.json"
    records=$(jq -er .records "$dir/raw-record-boundaries.json")
    seal_line=$(jq -er .seal_line "$dir/raw-record-boundaries.json")
    awk -v line="$seal_line" -v records="$records" -v bytes="$bytes" \
        -v prefix="$prefix_bytes" -v sealed="$sealed_bytes" '
      NR==line {before=total; after=total+length($0)+1; found++}
      {total+=length($0)+1}
      END {if (found!=1 || NR!=records || total!=bytes || before!=prefix || after!=sealed) exit 1;
           printf "prefix_bytes=%s sealed_bytes=%s records=%s final_newline=true\n",before,after,NR}' \
        "$result" > "$dir/seal-boundaries.validated" || die 'raw JSONL seal boundaries'
    actual=$(head -c "$prefix_bytes" "$result" | sha256sum); [[ ${actual%% *} == "$prefix_sha" ]] || die 'query prefix SHA'
    actual=$(head -c "$sealed_bytes" "$result" | sha256sum); [[ ${actual%% *} == "$sealed_sha" ]] || die 'durable seal SHA'
    jq -S . "$dir/summary.json" > "$dir/summary.sorted.json"
    one_object "$dir/native.stdout" 1048576
    jq -S . "$dir/native.stdout" > "$dir/stdout.sorted.json"
    cmp "$dir/summary.sorted.json" "$dir/stdout.sorted.json"
    authenticate "$result" "$bytes" "$(sha_of "$result")" > "$dir/result.identity"
}

immutable_snapshot() {
    local path
    find "$cohort" "$derived" "$generation" "$store" "$configs" -type l -print > "$evidence/$1.symlinks"
    [[ ! -s $evidence/$1.symlinks ]] || die 'symlink in immutable generation'
    find "$cohort" "$derived" "$generation" "$store" "$configs" -type f -print | sort > "$evidence/$1.paths"
    printf '%s\n' "$publication" >> "$evidence/$1.paths"
    while IFS= read -r path; do
        authenticate "$path" "$(stat -c %s -- "$path")" "$(sha_of "$path")"
    done < "$evidence/$1.paths"
}
verify_state() {
    local state=$1 label=$2
    immutable_snapshot "$label" > "$evidence/$label.jsonl"
    jq -S -s 'sort_by(.path)' "$evidence/$label.jsonl" > "$evidence/$label.sorted"
    jq -S '.immutable_files|sort_by(.path)' "$state" > "$evidence/$label.expected"
    cmp "$evidence/$label.sorted" "$evidence/$label.expected"
    [[ $(stat -c '%d:%i' "$PARENT") == "$(jq -er .parent_identity "$state")" ]] || die 'parent identity changed'
    [[ $(etag_of "$(jq -er .sq8.path "$state")") == "$(jq -er .sq8.etag "$state")" ]] || die 'SQ8 ETag changed'
}

authenticate_admission_closure() {
    local admission_path=$1 admission_dir key path terminal manifest wrapper outer raw_manager raw_exit raw_drain
    local admission_sha terminal_sha manifest_sha wrapper_sha outer_unit outer_id outer_cg
    admission_dir=${admission_path%/*}
    canonical "$admission_dir"
    [[ $admission_path == "$admission_dir/admission.json" &&
       $admission_dir == "$campaign_evidence_root/"* ]] || die 'admission evidence location'
    for key in terminal manifest wrapper_exit outer; do
        path=$(cfg ".admission_closure.$key.path")
        [[ $path == "$campaign_evidence_root/"* ]] || die 'closure outside counted campaign evidence'
        (( $(cfg ".admission_closure.$key.bytes") <= 4194304 )) || die 'closure artifact cap'
        auth_art "$(cfg ".admission_closure.$key")" > "$evidence/admission-$key.identity"
    done
    terminal=$(cfg .admission_closure.terminal.path)
    manifest=$(cfg .admission_closure.manifest.path)
    wrapper=$(cfg .admission_closure.wrapper_exit.path)
    outer=$(cfg .admission_closure.outer.path)
    [[ $terminal == "$admission_dir/terminal.json" && $manifest == "$admission_dir/closure.sha256" &&
       $wrapper == "$admission_dir/wrapper.exit" && $outer != "$admission_dir/"* ]] || die 'closure artifact roles'
    small_json "$terminal"; small_json "$outer"
    admission_sha=$(cfg .admission.sha256)
    terminal_sha=$(cfg .admission_closure.terminal.sha256)
    manifest_sha=$(cfg .admission_closure.manifest.sha256)
    wrapper_sha=$(cfg .admission_closure.wrapper_exit.sha256)
    jq -e --slurpfile a "$admission_path" '
      .schema=="borsuk-native-pid128-terminal-v1" and .status=="SOURCE_ADMITTED_ROOT_GATE_REQUIRED" and
      .stage=="closure" and .original_exit==0 and .intended_exit==0 and .cleanup_exit==0 and
      .config_sha256==$a[0].config_sha256 and .original_observer_manager_and_outer_exits_required==true and
      .performance_claim==false and .cold_claim==false and .competitor_claim==false and .one_million_claim==false' \
        "$terminal" > "$evidence/admission-terminal.validated"
    [[ $(stat -c %s "$wrapper") == 2 && $(< "$wrapper") == 0 ]] || die 'admission wrapper exit'
    # Authenticate the exact immutable closure roster, not merely selected hashes.
    awk -v a="$admission_sha" -v t="$terminal_sha" '
      {digest=substr($0,1,64); path=substr($0,69);
       if (length(digest)!=64 || digest ~ /[^0-9a-f]/ || substr($0,65,4)!="  ./" ||
           path !~ /^[A-Za-z0-9._\/-]+$/ || path ~ /(^|\/)\.\.?($|\/)/ || seen[path]++) exit 1;
       if (path=="admission.json") {if (digest!=a) exit 1; admissions++}
       if (path=="terminal.json") {if (digest!=t) exit 1; terminals++}
       print "./" path}
      END {if (admissions!=1 || terminals!=1) exit 1}' "$manifest" > "$evidence/admission-closure.paths" || die 'admission closure roster'
    sort "$evidence/admission-closure.paths" > "$evidence/admission-closure.sorted"
    (cd "$admission_dir" && find . -type l -print) > "$evidence/admission-closure.symlinks"
    [[ ! -s $evidence/admission-closure.symlinks ]] || die 'admission closure symlink'
    (cd "$admission_dir" && find . -type f ! -path ./closure.sha256 ! -path ./wrapper.exit -print | sort) > "$evidence/admission-closure.actual"
    cmp "$evidence/admission-closure.sorted" "$evidence/admission-closure.actual"
    (cd "$admission_dir" && sha256sum --check --strict --status "$manifest") || die 'admission closure body hashes'
    jq -e --arg recipe "$recipe_sha" --arg admission "$admission_sha" --arg terminal "$terminal_sha" \
      --arg manifest "$manifest_sha" --arg wrapper "$wrapper_sha" --slurpfile t "$terminal" "$JQ_TYPES
      keys_are([\"schema\",\"status\",\"recipe_sha256\",\"config_sha256\",\"admission_sha256\",\"terminal_sha256\",\"manifest_sha256\",\"wrapper_exit_sha256\",\"unit\",\"invocation_id\",\"control_group\",\"manager_show\",\"outer_exit_file\",\"drain_proof\",\"actual_outer_exit\",\"drained\"]) and
      .schema==\"borsuk-native-pid128-outer-closure-v2\" and .status==\"CLOSED\" and
      .recipe_sha256==\$recipe and .config_sha256==\$t[0].config_sha256 and
      .admission_sha256==\$admission and .terminal_sha256==\$terminal and .manifest_sha256==\$manifest and .wrapper_exit_sha256==\$wrapper and
      (.unit|type==\"string\" and test(\"^borsuk-pid128-observer-[a-z0-9-]+\\\\.service$\")) and
      (.invocation_id|type==\"string\" and test(\"^[0-9a-f]{32}$\")) and .control_group==(\"/system.slice/\"+.unit) and
      ([.manager_show,.outer_exit_file,.drain_proof]|all(.[];art and .bytes<=65536)) and
      .actual_outer_exit==0 and .drained==true" "$outer" > "$evidence/admission-outer.validated"
    for key in manager_show outer_exit_file drain_proof; do
        path=$(jq -er ".$key.path" "$outer")
        [[ $path == "$campaign_evidence_root/"* && $path != "$admission_dir/"* ]] || die 'external closure proof location'
        auth_art "$(jq -c ".$key" "$outer")" > "$evidence/admission-outer-$key.identity"
    done
    raw_manager=$(jq -er .manager_show.path "$outer")
    raw_exit=$(jq -er .outer_exit_file.path "$outer")
    raw_drain=$(jq -er .drain_proof.path "$outer")
    outer_unit=$(jq -er .unit "$outer"); outer_id=$(jq -er .invocation_id "$outer"); outer_cg=$(jq -er .control_group "$outer")
    one_object "$admission_dir/observer.identity.json" 65536
    jq -e --arg id "$outer_id" --arg cg "$outer_cg" '
      keys==["control_group","invocation_id","schema"] and
      .schema=="borsuk-native-pid128-observer-identity-v1" and
      .invocation_id==$id and .control_group==$cg' "$admission_dir/observer.identity.json" \
        > "$evidence/admission-observer-identity.validated"
    for key in initial before-admission final; do
        jq -es --arg path "/sys/fs/cgroup$outer_cg" \
            'length>0 and .[0].path==$path' "$admission_dir/observer.$key" \
            > "$evidence/admission-observer-$key.validated"
    done
    small_json "$raw_drain"
    jq -e --arg path "/sys/fs/cgroup$outer_cg" --arg id "$outer_id" "$JQ_TYPES
      keys_are([\"schema\",\"state\",\"path\",\"invocation_id\",\"events\"]) and
      .schema==\"borsuk-native-pid128-drain-v1\" and .path==\$path and .invocation_id==\$id and
      ((.state==\"removed\" and .events==null) or (.state==\"empty\" and (.events|art) and .events.bytes<=65536))" "$raw_drain" > "$evidence/admission-drain.validated"
    drain_state=$(jq -er .state "$raw_drain")
    awk -F= -v unit="$outer_unit" -v id="$outer_id" -v cg="$outer_cg" -v state="$drain_state" '
      $1=="Id" {if ($2!=unit || seen[$1]++) bad=1}
      $1=="Description" {if ($2!=unit || seen[$1]++) bad=1}
      $1=="InvocationID" {if ($2!=id || seen[$1]++) bad=1}
      $1=="ControlGroup" {if (($2!=cg && !($2=="" && state=="removed")) || seen[$1]++) bad=1}
      $1=="Result" {if ($2!="success" || seen[$1]++) bad=1}
      $1=="ExecMainCode" {if ($2!="1" || seen[$1]++) bad=1}
      $1=="ExecMainStatus" {if ($2!="0" || seen[$1]++) bad=1}
      END {if (bad || length(seen)!=7) exit 1}' "$raw_manager" || die 'original observer manager failure'
    [[ $(stat -c %s "$raw_exit") == 2 && $(< "$raw_exit") == 0 ]] || die 'actual enclosing observer exit'
    if [[ $drain_state == empty ]]; then
        auth_art "$(jq -c .events "$raw_drain")" > "$evidence/admission-drain-events.identity"
        drain_events_path=$(jq -er .events.path "$raw_drain")
        [[ $drain_events_path == "$campaign_evidence_root/"* && $drain_events_path != "$admission_dir/"* ]] || die 'drain event proof location'
        awk '$1=="populated" {if (NF!=2 || $2!="0" || n++) bad=1} END {if (bad || n!=1) exit 1}' "$drain_events_path" || die 'observer drain evidence'
    fi
}

if [[ $mode == admission ]]; then
    stage=prepare-config
    new_path "$PARENT"
    mkdir -- "$PARENT" "$configs"
    canonical "$PARENT"
    parent_identity=$(stat -c '%d:%i' "$PARENT")
    free=$(( $(stat -f -c %a "$ROOT") * $(stat -f -c %S "$ROOT") ))
    (( free >= $(cfg .scratch.minimum_free_bytes) )) || die 'scratch free-space admission'
    printf 'free_bytes=%s total_cap_bytes=%s\n' "$free" "$(cfg .scratch.total_cap_bytes)" > "$evidence/scratch.admission"
    jq --argjson dev "$(stat -c %d "$PARENT")" --argjson ino "$(stat -c %i "$PARENT")" \
        '.output_parent.device=$dev | .output_parent.inode=$ino' "$cohort_template" > "$configs/cohort.json"
    [[ $(jq -r '.output_parent|"\(.device):\(.inode)"' "$configs/cohort.json") == "$parent_identity" ]] || die 'parent numeric JSON roundtrip'
    run_phase prepare 2400 8589934592 4 0-3 "$bin/prepare_cohere_native_cohort" "$configs/cohort.json" "$(sha_of "$configs/cohort.json")" "$cohort"
    stage='cohort-receipt'
    receipt=$cohort/complete.json
    small_json "$receipt"
    jq -e --arg csha "$(sha_of "$configs/cohort.json")" --arg corpus "$CORPUS_SHA" --arg reserved "$RESERVED_SHA" \
        --slurpfile c "$configs/cohort.json" '
      .schema=="borsuk-cohere-native-cohort-receipt-v3" and .status=="COMPLETE" and .config.sha256==$csha and
      .dataset==$c[0].dataset and .revision==$c[0].revision and .output_parent==$c[0].output_parent and .resources==$c[0].resources and
      .geometry=={corpus_rows:100000,query_rows:32,dimensions:1024,k:10,corpus_intervals:[{start:0,end:100000}],
        reserved_query_interval:{start:100000,end:101000},query_source_ordinals:[100000,100032]} and
      .reserved_queries_sha256==$reserved and (.outputs|map(.name)|sort)==["corpus.f32","corpus.ids.jsonl","queries.f32","queries.ids.jsonl","truth.u64"] and
      all(.outputs[]; if .name=="corpus.f32" then .bytes==409600000 and .sha256==$corpus
        elif .name=="queries.f32" then .bytes==131072 elif .name=="truth.u64" then .bytes==2560 else .bytes>0 end)' \
        "$receipt" > "$evidence/cohort.validated"
    jq -r '.outputs[]|[.name,.bytes,.sha256]|@tsv' "$receipt" > "$evidence/cohort.outputs.tsv"
    while IFS=$'\t' read -r name bytes sha; do authenticate "$cohort/$name" "$bytes" "$sha"; done < "$evidence/cohort.outputs.tsv" > "$evidence/cohort.outputs.identity"
    corpus_sha=$(jq -er '.outputs[]|select(.name=="corpus.f32")|.sha256' "$receipt")
    # Native producer formula: source + 2*source + 2*order + SQ8 + two receipts + caller + reserve.
    scratch_required=$((409600000*3 + 800000*2 + 103600000 + 65536*2 + $(cfg .scratch.caller_scratch_bytes) + 67108864))
    retained=$(( $(jq '[.assets[].bytes]|add' "$manifest") + $(jq '[.outputs[]|select(.name!="corpus.f32")|.bytes]|add' "$receipt") ))
    (( $(cfg .scratch.caller_scratch_bytes) >= retained + $(cfg .scratch.evidence_reserve_bytes) &&
       $(cfg .scratch.max_aggregate_scratch_bytes) >= scratch_required &&
       $(cfg .scratch.total_cap_bytes) >= $(cfg .scratch.max_aggregate_scratch_bytes) )) || die 'root scratch accounting excludes retained roster/evidence reserve'
    jq --arg sha "$corpus_sha" --slurpfile root "$config_copy" \
      '.original_corpus.sha256=$sha | .resources.caller_scratch_bytes=$root[0].scratch.caller_scratch_bytes |
       .resources.max_aggregate_scratch_bytes=$root[0].scratch.max_aggregate_scratch_bytes' "$derive_template" > "$configs/derive.json"
    run_phase derive 3600 8589934592 4 0-3 "$bin/build_sq8_source" --derive "$configs/derive.json" "$(sha_of "$configs/derive.json")" "$derived"
    stage='derivation-receipt'
    derivation=$derived/derivation.json
    small_json "$derivation"
    jq -e --arg dsha "$(sha_of "$configs/derive.json")" --arg corpus "$corpus_sha" --argjson authority "$AUTHORITY" \
        --argjson scratch "$scratch_required" "$JQ_TYPES
      keys_are([\"schema\",\"producer_config_sha256\",\"status\",\"recipe\",\"query_or_truth_used\",\"original_corpus\",\"rows\",\"dimensions\",\"corpus_intervals\",\"outputs\",\"low_f32_bits\",\"step_f32_bits\",\"producer_authority\",\"source_identity_qualification\",\"admission\"]) and
      .schema==\"borsuk-native-scale-derivation-receipt-v2\" and .producer_config_sha256==\$dsha and .status==\"COMPLETE\" and
      .recipe==\"normalize_then_flat_fit_then_sq8\" and .query_or_truth_used==false and
      .original_corpus=={bytes:409600000,sha256:\$corpus} and .rows==100000 and .dimensions==1024 and .corpus_intervals==[{start:0,end:100000}] and
      .producer_authority==\$authority and .source_identity_qualification==\"external_frozen_prerequisite_not_self_certified\" and
      (.outputs|keys)==[\"normalized\",\"source_order\",\"sq8\"] and .outputs.normalized.bytes==409600000 and
      .outputs.source_order.bytes==800000 and .outputs.sq8.bytes==103600000 and all(.outputs[]; keys_are([\"bytes\",\"sha256\"]) and (.sha256|sha)) and
      (.low_f32_bits|length)==1024 and all(.low_f32_bits[]; uint and .<4294967296 and (.%2147483648)<2139095040) and
      (.step_f32_bits|length)==1024 and all(.step_f32_bits[]; uint and .>0 and .<2139095040) and
      .admission.aggregate_scratch_upper_bound_bytes==\$scratch and .admission.peak_payload_upper_bound_bytes<=2147483648" \
        "$derivation" > "$evidence/derivation.validated"
    jq -e "$JQ_F32 (.low_f32_bits + .step_f32_bits) | all(.[]; f32ok)" "$derivation" > "$evidence/f32-roundtrip.validated"
    jq -r '[["normalized.f32",.outputs.normalized.bytes,.outputs.normalized.sha256],["order.u64",.outputs.source_order.bytes,.outputs.source_order.sha256],["sq8.bin",.outputs.sq8.bytes,.outputs.sq8.sha256]]|.[]|@tsv' "$derivation" > "$evidence/derived.outputs.tsv"
    while IFS=$'\t' read -r name bytes sha; do authenticate "$derived/$name" "$bytes" "$sha"; done < "$evidence/derived.outputs.tsv" > "$evidence/derived.outputs.identity"
    sq8_sha=$(jq -er .outputs.sq8.sha256 "$derivation")
    sq8_key=semantic/objects/$sq8_sha
    staged=$store/$sq8_key
    [[ $(stat -c %h "$derived/sq8.bin") == 1 ]] || die 'SQ8 source hardlinks'
    authenticate "$derived/sq8.bin" 103600000 "$sq8_sha" > "$evidence/sq8.before"
    mkdir -- "$store" "$store/semantic" "$store/semantic/objects"
    run_phase copy 300 8589934592 4 0-3 /usr/bin/dd "if=$derived/sq8.bin" "of=$staged" bs=1048576 conv=excl,fsync status=none
    sync -f "$store"
    [[ $(stat -c %h "$staged") == 1 ]] || die 'staged SQ8 hardlinks'
    authenticate "$derived/sq8.bin" 103600000 "$sq8_sha" > "$evidence/sq8.after"
    cmp "$evidence/sq8.before" "$evidence/sq8.after"
    authenticate "$staged" 103600000 "$sq8_sha" > "$evidence/staged.before-etag"
    sq8_etag=$(etag_of "$staged")
    authenticate "$staged" 103600000 "$sq8_sha" > "$evidence/staged.after-etag"
    cmp "$evidence/staged.before-etag" "$evidence/staged.after-etag"
    jq --arg raw "$derived/normalized.f32" --arg order "$derived/order.u64" --arg sq8 "$derived/sq8.bin" --arg key "$sq8_key" --arg etag "$sq8_etag" \
      "$JQ_F32 {discovery:\"semantic\",semantic_profile:\"scale1m\",order:{path:\$order,sha256:.outputs.source_order.sha256},
       raw:\$raw,raw_sha256:.outputs.normalized.sha256,sq8:\$sq8,sq8_sha256:.outputs.sq8.sha256,rows:100000,dimensions:1024,generation:1,base_epoch:0,
       low:(.low_f32_bits|map(f32dec)),step:(.step_f32_bits|map(f32dec)),sq8_object_key:\$key,sq8_etag:\$etag}" "$derivation" > "$configs/generation.json"
    small_json "$configs/generation.json"
    run_phase generation 2700 8589934592 4 0-3 "$bin/build_two_bit_generation" "$configs/generation.json" "$(sha_of "$configs/generation.json")" 1073741824 "$generation"
    [[ $(stat -c %s "$phase_dir/native.stdout") == 65 ]] || die 'generation SHA stdout'
    root_sha=$(< "$phase_dir/native.stdout")
    authenticate "$generation/manifest.json" "$(stat -c %s "$generation/manifest.json")" "$root_sha" > "$evidence/generation.identity"
    one_object "$generation/plane/manifest.json" 4194304
    jq -e --arg sha "$(jq -er .outputs.source_order.sha256 "$derivation")" '.source_order_sha256==$sha' "$generation/plane/manifest.json" > "$evidence/plane.validated"
    jq -n --arg generation "$generation" --arg sha "$root_sha" --arg store "$store" --argjson limits "$LIMITS" \
      '{schema:"borsuk-two-bit-local-publication-config-v1",root:{path:$generation,sha256:$sha},store_root:$store,prefix:"semantic/index",limits:$limits}' > "$configs/publication.json"
    run_phase publish 1800 8589934592 4 0-3 "$bin/publish_two_bit_generation" "$configs/publication.json" "$(sha_of "$configs/publication.json")" "$publication"
    small_json "$publication"
    jq -e --arg sha "$(sha_of "$configs/publication.json")" --arg root "$root_sha" '
      .schema=="borsuk-two-bit-local-publication-receipt-v1" and .config_sha256==$sha and .prefix=="semantic/index" and
      .root_sha256==$root and .generation==1 and .control_epoch==1 and .metadata_prefix==("semantic/index/generations/"+$root)' "$publication" > "$evidence/publication.validated"
    meta=$(jq -er .metadata_prefix "$publication")
    authenticate "$store/$meta/manifest.json" "$(stat -c %s "$generation/manifest.json")" "$root_sha" > "$evidence/published-root.identity"
    small_json "$store/semantic/index/head.json"
    jq -e --arg sha "$root_sha" '.schema=="borsuk-two-bit-head-v2" and .root_sha256==$sha and .generation==1 and .epoch==1 and .fence==null' "$store/semantic/index/head.json" > "$evidence/head.validated"
    [[ $(etag_of "$staged") == "$sq8_etag" ]] || die 'SQ8 ETag changed during publication'
    for label in diagnostic query16 query32; do
        width=32; [[ $label != query16 ]] || width=16
        execution='{"mode":"full"}'; [[ $label != diagnostic ]] || execution='{"mode":"diagnostic_panel","ordinals":[0],"trace":false}'
        mkdir -- "$PARENT/$label" "$PARENT/$label/scratch"
        jq -n --argjson cohort "$(descriptor "$receipt")" --argjson derive "$(descriptor "$derivation")" \
          --argjson authority "$AUTHORITY" --argjson requests "$(descriptor "$cohort/queries.f32")" --argjson truth "$(descriptor "$cohort/truth.u64")" \
          --slurpfile d "$derivation" --arg store "$store" --arg meta "$meta" --arg root "$root_sha" --arg scratch "$PARENT/$label/scratch" \
          --argjson width "$width" --argjson execution "$execution" '
          {schema:"borsuk-cohere-native-baseline-config-v7",dataset:"CohereLabs/wikipedia-2023-11-embed-multilingual-v3",
           revision:"ade45fb52bd549f5e8c065636fe4160a43c2af36",metric:"cosine",tie_rule:"corpus_ordinal_ascending",
           corpus_intervals:[{start:0,end:100000}],reserved_query_interval:{start:100000,end:101000},
           cohort_receipt:$cohort,derivation_receipt:$derive,producer_authority:$authority,corpus_source_first:0,query_source_first:100000,
           rows:100000,dimensions:1024,count:32,k:10,profile:"scale1m",backend:{kind:"local",store_root:$store},
           generation_prefix:$meta,generation_root_sha256:$root,scratch_parent:$scratch,requests:$requests,truth:$truth,
           native_source:{source_sha256:$d[0].outputs.normalized.sha256,sq8_sha256:$d[0].outputs.sq8.sha256,source_order_sha256:$d[0].outputs.source_order.sha256},
           max_memory_bytes:536870912,fetch_parallelism:$width,serving:{mode:"baseline"},execution:$execution}' > "$configs/$label.json"
    done
    jq -S 'del(.fetch_parallelism,.scratch_parent)' "$configs/query16.json" > "$evidence/query16.common.json"
    jq -S 'del(.fetch_parallelism,.scratch_parent)' "$configs/query32.json" > "$evidence/query32.common.json"
    cmp "$evidence/query16.common.json" "$evidence/query32.common.json"
    immutable_snapshot before-diagnostic > "$evidence/immutable.before-diagnostic.jsonl"
    run_phase diagnostic 900 536870912 1 0 "$bin/check_cohere_native_baseline" "$configs/diagnostic.json" "$(sha_of "$configs/diagnostic.json")" "$PARENT/diagnostic/result.jsonl"
    validate_result "$PARENT/diagnostic/result.jsonl" "$configs/diagnostic.json" 1 "$phase_dir"
    immutable_snapshot after-diagnostic > "$evidence/immutable.after-diagnostic.jsonl"
    cmp "$evidence/immutable.before-diagnostic.jsonl" "$evidence/immutable.after-diagnostic.jsonl"
    authenticate_inputs > "$evidence/inputs.after"
    cmp "$evidence/inputs.before" "$evidence/inputs.after"
    # Admission is emitted only after the observer checks. Its final closure and
    # actual external manager exit remain REQUIRED by widths, not self-certified here.
    snapshot "$observer_cg" "$evidence/observer.before-admission"
    check_limits "$evidence/observer.before-admission" 268435456 1 0
    check_events "$evidence/observer.initial" "$evidence/observer.before-admission"
    jq -n --arg recipe "$recipe_sha" --arg config "$config_sha" --arg manifest "$(cfg .input_manifest.sha256)" \
        --arg parent "$parent_identity" --slurpfile files "$evidence/immutable.after-diagnostic.jsonl" \
        --argjson diagnostic "$(descriptor "$PARENT/diagnostic/result.jsonl")" --argjson q16 "$(descriptor "$configs/query16.json")" \
        --argjson q32 "$(descriptor "$configs/query32.json")" --arg path "$staged" --arg etag "$sq8_etag" '
        {schema:"borsuk-native-pid128-admission-v1",status:"SOURCE_ADMITTED",recipe_sha256:$recipe,config_sha256:$config,
         input_manifest_sha256:$manifest,parent_identity:$parent,immutable_files:$files,diagnostic:$diagnostic,
         query16:$q16,query32:$q32,sq8:{path:$path,etag:$etag},root_acceptance_required:true,performance_claim:false}' > "$evidence/admission.json"
    status=SOURCE_ADMITTED_ROOT_GATE_REQUIRED
else
    stage='admitted-state'
    auth_art "$(cfg .admission)" > "$evidence/admission.identity"
    admission=$(cfg .admission.path)
    (( $(cfg .admission.bytes) <= 4194304 )) || die 'admission receipt cap'
    one_object "$admission" 4194304
    authenticate_admission_closure "$admission"
    original_admission=$admission
    admission=$evidence/admission.baseline.json
    cp "$original_admission" "$admission"
    authenticate "$admission" "$(cfg .admission.bytes)" "$(cfg .admission.sha256)" > "$evidence/admission.baseline.identity"
    auth_art "$(cfg .admission)" > "$evidence/admission.after-copy.identity"
    jq -e --arg recipe "$recipe_sha" --arg manifest "$(cfg .input_manifest.sha256)" --arg configs "$configs" "$JQ_TYPES
      .schema==\"borsuk-native-pid128-admission-v1\" and .status==\"SOURCE_ADMITTED\" and .recipe_sha256==\$recipe and
      .input_manifest_sha256==\$manifest and (.query16|art) and (.query32|art) and
      .query16.path==(\$configs+\"/query16.json\") and .query32.path==(\$configs+\"/query32.json\") and
      (.immutable_files|type==\"array\" and length>10)" "$admission" > "$evidence/admission.validated"
    verify_state "$admission" before-widths
    canonical "$PARENT/query16/scratch"; canonical "$PARENT/query32/scratch"
    # Create-only marker also refuses retry after an interrupted first width.
    printf '%s\n' "$config_sha" > "$PARENT/widths.started"
    for width in 16 32; do
        native_config=$(jq -er ".query$width.path" "$admission")
        auth_art "$(jq -c ".query$width" "$admission")" > "$evidence/query$width.config.identity"
        new_path "$PARENT/query$width/result.jsonl"
        [[ -z $(find "$PARENT/query$width/scratch" -mindepth 1 -print -quit) ]] || die 'query scratch is not fresh'
        run_phase "query$width" 900 536870912 1 0 "$bin/check_cohere_native_baseline" "$native_config" "$(sha_of "$native_config")" "$PARENT/query$width/result.jsonl"
        validate_result "$PARENT/query$width/result.jsonl" "$native_config" 32 "$phase_dir"
        verify_state "$admission" "after-query$width"
        authenticate_inputs > "$evidence/inputs.after-query$width"
        cmp "$evidence/inputs.before" "$evidence/inputs.after-query$width"
        # Recheck the original pinned admission/closure in a new create-only evidence directory.
        auth_art "$(cfg .admission)" > "$evidence/admission.after-query$width.identity"
        authenticate "$admission" "$(cfg .admission.bytes)" "$(cfg .admission.sha256)" > "$evidence/admission.baseline.after-query$width.identity"
        saved_evidence=$evidence
        mkdir "$saved_evidence/admission-closure-after-query$width"
        evidence=$saved_evidence/admission-closure-after-query$width
        authenticate_admission_closure "$original_admission"
        evidence=$saved_evidence
    done
    status=PID128_MECHANICS_CLOSED_ROOT_REPLAY_REQUIRED
fi
stage=closure
snapshot "$observer_cg" "$evidence/observer.final"
check_limits "$evidence/observer.final" 268435456 1 0
check_events "$evidence/observer.initial" "$evidence/observer.final"
sync -f "$PARENT"
exit 0
