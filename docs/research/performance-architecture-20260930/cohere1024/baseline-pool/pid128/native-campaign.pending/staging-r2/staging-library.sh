# Source-bound helpers ONLY. Source exclusively inside the remote staging observer.
# Caller supplies all validated state; this library has no experiment entrypoint.
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
die() { printf 'INVALID: %s\n' "$*" >&2; exit 98; }
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
owned() {
    local invocation description control_group
    [[ -n $owned_unit && -n $owned_id && -n $owned_cg ]] || return 1
    invocation=$(manager systemctl show "$owned_unit" -p InvocationID --value) || return 1
    description=$(manager systemctl show "$owned_unit" -p Description --value) || return 1
    control_group=$(manager systemctl show "$owned_unit" -p ControlGroup --value) || return 1
    [[ $invocation == "$owned_id" && $description == "$owned_unit" &&
       $control_group == "${owned_cg#/sys/fs/cgroup}" ]]
}
drained() { [[ ! -d $owned_cg ]] || [[ $(< "$owned_cg/cgroup.events") == *'populated 0'* ]]; }
cleanup_owned() {
    local end cleanup_rc=0
    [[ -n $owned_unit ]] || return 0
    manager_deadline=$((SECONDS+40))
    owned || { printf 'ownership unavailable; root must reconcile %s\n' "$owned_unit" >&2; return 1; }
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
    printf 'populated=0\n' > "$phase_dir/cleanup.drained.txt" || cleanup_rc=1
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
    printf 'original=%s cleanup=%s intended=%s signal=%s line=%s\n' "$original" "$cleanup" "$rc" "$signal_name" "$failed_line" > "$evidence/closure.txt" || rc=98
    jq -n --arg status "$status" --arg stage "$stage" --arg sha "$config_sha" --argjson original "$original" \
        --argjson intended "$rc" --argjson cleanup "$cleanup" --args \
        '{schema:"borsuk-native-pid128-terminal-v1",status:$status,stage:$stage,config_sha256:$sha,
          original_exit:$original,intended_exit:$intended,cleanup_exit:$cleanup,phases_completed:$ARGS.positional,
          original_observer_manager_and_outer_exits_required:true,root_instance_and_volume_closure_required:true,
          performance_claim:false,cold_claim:false,competitor_claim:false,one_million_claim:false}' \
        -- "${completed[@]}" > "$evidence/terminal.json" || rc=98
    (cd "$evidence" && find . -type f ! -path ./closure.sha256 ! -path ./wrapper.exit -print0 | sort -z | xargs -0 sha256sum --) > "$evidence/closure.sha256" || rc=98
    printf '%s\n' "$rc" > "$evidence/wrapper.exit" || rc=98
    sync -f "$evidence" || rc=98
    exit "$rc"
}
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
    manager systemd-run --quiet --unit="$owned_unit" --description="$owned_unit" --service-type=exec \
        -p CPUQuota="$((cores*100))%" -p AllowedCPUs="$cpus" -p MemoryMax="$memory" -p MemorySwapMax=0 -p TasksMax=128 \
        -p KillMode=control-group -p OOMPolicy=continue -p TimeoutStopSec=10 -p RuntimeMaxSec="$((secs+80))" -p RemainAfterExit=yes \
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
    printf 'release\n' > "$phase_dir/release"
    observe "$(( $(mono_cs) + (secs+15)*100 ))" "$sample_fd"
    exec {sample_fd}>&-
    snapshot "$owned_cg" "$phase_dir/resources.final"
    # Capture every original manager status before allowing the cgroup holder to exit.
    manager systemctl show "$owned_unit" > "$phase_dir/manager.before-close.txt"
    printf 'close\n' > "$phase_dir/close"
    end=$((SECONDS+30)); (( end <= manager_deadline )) || end=$manager_deadline
    until drained; do (( SECONDS < end )) || die 'owned descendants failed to drain'; sleep 0.05; done
    printf 'populated=0\n' > "$phase_dir/drained.txt"
    # Empty cgroup can precede the manager's final execution fields. Preserve
    # ownership and wait inside the existing phase/manager deadline before stop.
    end=$((SECONDS+5)); (( end <= manager_deadline )) || end=$manager_deadline
    local original_manager_deadline=$manager_deadline
    manager_deadline=$end
    while :; do
        manager systemctl show "$owned_unit" >| "$phase_dir/manager.final.poll"
        [[ $(sed -n 's/^InvocationID=//p' "$phase_dir/manager.final.poll") == "$owned_id" &&
           $(sed -n 's/^ControlGroup=//p' "$phase_dir/manager.final.poll") == "${owned_cg#/sys/fs/cgroup}" ]] || die 'manager final ownership changed'
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
    [[ $(sed -n 's/^Result=//p' "$phase_dir/manager.final.txt") == success &&
       $(sed -n 's/^ExecMainCode=//p' "$phase_dir/manager.final.txt") == 1 &&
       $(sed -n 's/^ExecMainStatus=//p' "$phase_dir/manager.final.txt") == 0 ]] || die 'manager native closure'
    check_limits "$phase_dir/resources.final" "$memory" "$cores" "$cpus"
    check_events "$phase_dir/resources.initial" "$phase_dir/resources.final"
    for f in native timeout time tee time-log supervisor-log native-log payload; do
        [[ -f $phase_dir/$f.exit && $(< "$phase_dir/$f.exit") == 0 ]] || die "$name: missing/nonzero $f exit"
    done
    awk -F ': ' -v cap="$((memory/1024))" '/Maximum resident set size \(kbytes\)/ {n++; if ($2<=0 || $2>cap) bad=1} END {exit(n!=1 || bad)}' "$phase_dir/native.time"
    scratch_snapshot "$name" after
    completed+=("$name")
}
cfg() { jq -er "$1" "$config_copy"; }
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
etag_of() { # object_store 0.14.1 LocalFileSystem, quoted hex inode-mtime_microseconds-size.
    local fields ino size sec time frac
    fields=$(stat -c '%i %s %Y %y' -- "$1")
    read -r ino size sec _ time _ <<< "$fields"
    [[ $time =~ ^[0-9:]+\.([0-9]{9})$ ]] || die 'mtime precision'
    frac=${BASH_REMATCH[1]}
    printf '"%x-%x-%x"\n' "$ino" "$((sec*1000000 + 10#${frac:0:6}))" "$size"
}
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
    local admission_path=$1 admission_dir key path terminal manifest wrapper outer raw_manager raw_exit raw_events
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
      keys_are([\"schema\",\"status\",\"recipe_sha256\",\"config_sha256\",\"admission_sha256\",\"terminal_sha256\",\"manifest_sha256\",\"wrapper_exit_sha256\",\"unit\",\"invocation_id\",\"control_group\",\"manager_show\",\"outer_exit_file\",\"drain_events\",\"actual_outer_exit\",\"populated_zero\"]) and
      .schema==\"borsuk-native-pid128-outer-closure-v1\" and .status==\"CLOSED\" and
      .recipe_sha256==\$recipe and .config_sha256==\$t[0].config_sha256 and
      .admission_sha256==\$admission and .terminal_sha256==\$terminal and .manifest_sha256==\$manifest and .wrapper_exit_sha256==\$wrapper and
      (.unit|type==\"string\" and test(\"^borsuk-pid128-observer-[a-z0-9-]+\\\\.service$\")) and
      (.invocation_id|type==\"string\" and test(\"^[0-9a-f]{32}$\")) and .control_group==(\"/system.slice/\"+.unit) and
      ([.manager_show,.outer_exit_file,.drain_events]|all(.[];art and .bytes<=65536)) and
      .actual_outer_exit==0 and .populated_zero==true" "$outer" > "$evidence/admission-outer.validated"
    for key in manager_show outer_exit_file drain_events; do
        path=$(jq -er ".$key.path" "$outer")
        [[ $path == "$campaign_evidence_root/"* && $path != "$admission_dir/"* ]] || die 'external closure proof location'
        auth_art "$(jq -c ".$key" "$outer")" > "$evidence/admission-outer-$key.identity"
    done
    raw_manager=$(jq -er .manager_show.path "$outer")
    raw_exit=$(jq -er .outer_exit_file.path "$outer")
    raw_events=$(jq -er .drain_events.path "$outer")
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
    awk -F= -v unit="$outer_unit" -v id="$outer_id" -v cg="$outer_cg" '
      $1=="Id" {if ($2!=unit || seen[$1]++) bad=1}
      $1=="Description" {if ($2!=unit || seen[$1]++) bad=1}
      $1=="InvocationID" {if ($2!=id || seen[$1]++) bad=1}
      $1=="ControlGroup" {if ($2!=cg || seen[$1]++) bad=1}
      $1=="Result" {if ($2!="success" || seen[$1]++) bad=1}
      $1=="ExecMainCode" {if ($2!="1" || seen[$1]++) bad=1}
      $1=="ExecMainStatus" {if ($2!="0" || seen[$1]++) bad=1}
      END {if (bad || length(seen)!=7) exit 1}' "$raw_manager" || die 'original observer manager failure'
    [[ $(stat -c %s "$raw_exit") == 2 && $(< "$raw_exit") == 0 ]] || die 'actual enclosing observer exit'
    awk '$1=="populated" {if (NF!=2 || $2!="0" || n++) bad=1} END {if (bad || n!=1) exit 1}' \
        "$raw_events" || die 'observer drain evidence'
}
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
