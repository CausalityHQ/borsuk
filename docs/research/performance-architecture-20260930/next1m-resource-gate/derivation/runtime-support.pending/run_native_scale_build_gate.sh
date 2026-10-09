#!/usr/bin/env bash
# One native 1M/Q32 build gate: derive -> SQ8 full-copy stage -> generation -> LocalFileSystem publish -> Q32 baseline.
# Root owns the cgroup scope, config freeze, remote canary and run; this wrapper authenticates, runs one native
# process at a time (no retry, stop at first failure, nothing deleted) and retains every original exit/log/limit.
# The baseline output is opaque: it is never decoded, thresholded or interpreted here.
set -Eeuo pipefail
set -o noclobber
umask 077
export LC_ALL=C

die() { printf 'INVALID: %s\n' "$*" >&2; exit 98; }
[[ $# == 3 ]] || die 'usage: CONFIG CONFIG_SHA NEW_EVIDENCE'
config=$1 config_sha=$2 evidence=$3

# Closed first-admission (04bccf4e) seals of the regenerated preparer outputs, the root-supplied derivation config
# (ea45653c) and the fixed 1M/Q32 cohort identity. The gate config must repeat them exactly.
readonly DERIVE_CONFIG_BYTES=1145
readonly DERIVE_CONFIG_SHA=2737d3e9e8b6c741bffd322e34b4ddf5aa8e318a8cfb4fafc00e262288b96a2d
readonly RESERVED_SHA=8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e
readonly SEALS='{"corpus.f32":{"bytes":4096000000,"sha256":"1a491c060ec8b668b4983042dc1ac3971456327f41f28fd6fd2449df24cf3e38"},
 "queries.f32":{"bytes":131072,"sha256":"664f5b269756a1de5a77c4ec359e56ccbe85c87603fa01fc5d87cc3f02e52667"},
 "corpus.ids.jsonl":{"bytes":200873446,"sha256":"008191ae3103d78db682276008190cc858a361b19e487f6780c02cea5dd70067"},
 "queries.ids.jsonl":{"bytes":6200,"sha256":"e27fa2d4d0a22a4f9f4dbb9eb2aeeeb0681f48c7e2193e3aa57a2b49cb5ddeb4"},
 "truth.u64":{"bytes":2560,"sha256":"36e83267ebfe5efb86db28c778d18897563a8c6c0784933e2b8d6d05cf31bb93"}}'
readonly DATASET=CohereLabs/wikipedia-2023-11-embed-multilingual-v3
readonly REVISION=ade45fb52bd549f5e8c065636fe4160a43c2af36
readonly STORE_PREFIX=semantic/index # same LocalFileSystem layout the qualified preflight used
readonly BASELINE_MEMORY=536870912   # check_cohere_native_baseline MEMORY_CAP, unchanged

for tool in jq sha256sum stat realpath timeout tee head cmp sync sed awk cp mkdir prlimit find sort xargs du mountpoint tr; do
    command -v "$tool" >/dev/null || die "missing $tool"
done
[[ -x /usr/bin/time && -x /usr/bin/dd && $config_sha =~ ^[0-9a-f]{64}$ ]] || die 'time/dd/config SHA'

canonical() {
    [[ $1 == /* && $1 != *[$'\t\r\n\\']* && $(realpath -e -- "$1") == "$1" ]] || die "noncanonical path: $1"
}
new_path() {
    [[ $1 != */ && ${1##*/} != . && ${1##*/} != .. && ! -e $1 && ! -L $1 ]] || die "occupied/invalid namespace: $1"
    canonical "${1%/*}"
    [[ -d ${1%/*} && $1 != *[$'\t\r\n\\']* ]] || die "namespace parent: $1"
}
new_path "$evidence"
mkdir -- "$evidence"
stage=admission
failed_line=0
verified=0
baseline_exit=
baseline_invoked=0
signal_name=
completed=()
finish() {
    local rc=$? status=INVALID wf=0 original signal=$signal_name
    original=$rc
    trap '' HUP INT TERM
    trap - EXIT ERR
    set +e
    # An explicit signal keeps 129/130/143 and its name. Otherwise only a fully closed chain may carry the baseline's own
    # exit (0, 2 or 3) out as the wrapper exit; every other raw status (jq, cmp, cp, redirection, ...) becomes 98 and is
    # retained separately as original_exit.
    if [[ -n $signal ]]; then
        :
    elif (( verified == 1 && rc == 0 )) && [[ $baseline_exit == 0 ]]; then
        status=NATIVE_CHAIN_CLOSED
    elif (( verified == 1 && (rc == 2 || rc == 3) )) && [[ $rc == "$baseline_exit" ]]; then
        status=BASELINE_NONZERO_EXIT
    else
        rc=98
    fi
    printf 'stage=%s line=%s original_exit=%s exit=%s signal=%s\n' "$stage" "$failed_line" "$original" "$rc" "$signal" \
        > "$evidence/closure.txt" || { wf=1; rc=98; }
    # Seal pre-status evidence; wrapper.exit and terminal.json record the later intent.
    (cd -- "$evidence" && find . -type f ! -path ./closure.sha256 -print0 | sort -z | xargs -0 sha256sum --) \
        > "$evidence/closure.sha256" || { wf=1; rc=98; }
    sync -f "$evidence" || { wf=1; rc=98; }
    printf '%s\n' "$rc" > "$evidence/wrapper.exit" || { wf=1; rc=98; }
    (( wf == 0 )) || status=INVALID
    jq -n --arg status "$status" --arg stage "$stage" --argjson intended_exit "$rc" \
        --arg config_sha256 "$config_sha" --arg baseline_exit "$baseline_exit" --arg evidence "$evidence" \
        --argjson baseline_invoked "$baseline_invoked" --argjson original_exit "$original" --arg signal "$signal" \
        --argjson failed_line "$failed_line" \
        --args '{schema:"borsuk-native-scale-build-gate-local-v1",status:$status,stage:$stage,intended_exit:$intended_exit,
          original_exit:$original_exit,signal:(if $signal == "" then null else $signal end),failed_line:$failed_line,
          config_sha256:$config_sha256,baseline_native_exit:(if $baseline_exit == "" then null else ($baseline_exit|tonumber) end),
          phases_completed:$ARGS.positional,evidence:$evidence,
          wrapper_exit_scope:"intended_exit",actual_manager_and_outer_exit_required:true,
          scope:"local exact1M/Q32 derive, stage, generation, publish and baseline run; output opaque",
          baseline_invoked:($baseline_invoked == 1),actual_query_completion_requires_external_replay:true,
          instance_termination_verified:false,performance_claim:false}' \
        -- ${completed[@]+"${completed[@]}"} > "$evidence/terminal.json" || { wf=1; rc=98; }
    sync -f "$evidence" || { wf=1; rc=98; }
    (( wf == 0 )) || printf 'INVALID: evidence publication failed (original_exit=%s signal=%s)\n' "$original" "${signal:-none}" >&2
    # Root must require normal manager termination and the original outer wait exits, including write failures.
    exit "$rc"
}
trap finish EXIT
trap 'failed_line=$LINENO' ERR
trap 'signal_name=HUP; stage=signal-HUP; exit 129' HUP
trap 'signal_name=INT; stage=signal-INT; exit 130' INT
trap 'signal_name=TERM; stage=signal-TERM; exit 143' TERM
jq -n --args '$ARGS.positional' -- "$@" > "$evidence/invocation.json"

authenticate() {
    local path=$1 bytes=$2 expected=$3 stamp digest
    canonical "$path"
    [[ -f $path && ! -L $path && $bytes =~ ^[1-9][0-9]*$ && $expected =~ ^[0-9a-f]{64}$ ]] || die "regular pinned file: $path"
    stamp=$(stat -c '%d:%i:%s:%y:%z' -- "$path")
    [[ $(stat -c %s -- "$path") == "$bytes" ]] || die "length: $path"
    digest=$(sha256sum < "$path")
    [[ ${digest%% *} == "$expected" && $(stat -c '%d:%i:%s:%y:%z' -- "$path") == "$stamp" ]] || die "SHA/metadata drift: $path"
    jq -cn --arg path "$path" --argjson bytes "$bytes" --arg sha256 "$expected" --arg stamp "$stamp" \
        --argjson device "$(stat -c %d -- "$path")" --argjson inode "$(stat -c %i -- "$path")" \
        '{path:$path,bytes:$bytes,sha256:$sha256,stamp:$stamp,device:$device,inode:$inode}'
}

# ---- jq programs (kept in variables so they can be compile-checked without running the gate) -------------------
IFS= read -r -d '' JQ_CONFIG <<'EOF' || true
def uint: type == "number" and . >= 0 and floor == .;
def sha: type == "string" and test("^[0-9a-f]{64}$");
def abs: type == "string" and startswith("/") and length <= 4096;
def keys_are($k): type == "object" and ((keys) == ($k | sort));
def art: keys_are(["bytes","path","sha256"]) and (.path | abs) and (.bytes | uint and . > 0) and (.sha256 | sha);
def phase($max): keys_are(["stdout_cap_bytes","timeout_seconds"]) and
  (.stdout_cap_bytes | uint and . >= 1 and . <= 1048576) and (.timeout_seconds | uint and . >= 11 and . <= $max);
length == 1 and (.[0] |
  keys_are(["baseline","binaries","deadline_seconds","derive_config","disk_min_available_bytes","disk_proposal_bytes",
            "generation","namespaces","phases","prepared","producer_authority","publish","schema","scratch_root"]) and
  .schema == "borsuk-native-scale-build-gate-config-v1" and
  (.scratch_root | abs) and
  (.disk_proposal_bytes | uint and . >= 27581982055 and . <= 42949672960) and
  (.disk_min_available_bytes | uint and . > 0 and . <= 42949672960) and
  (.deadline_seconds | uint and . >= 60 and . <= 9600) and
  (.binaries | keys_are(["baseline","derive","generation","publish"]) and all(.[]; art)) and
  ([.binaries[].path] | unique | length) == 4 and ([.binaries[].sha256] | unique | length) == 4 and
  (.derive_config | art and .bytes == $dbytes and .sha256 == $dsha) and
  (.producer_authority | keys_are(["executable_sha256","producer_source_sha256","source_commit","source_order_source_sha256","sq8_source_sha256"]) and
    (.source_commit | type == "string" and test("^[0-9a-f]{40}$")) and
    ([.executable_sha256,.producer_source_sha256,.source_order_source_sha256,.sq8_source_sha256] | all(.[]; sha))) and
  .binaries.derive.sha256 == .producer_authority.executable_sha256 and
  (.prepared | keys_are(["complete","outputs"]) and (.complete | art and .bytes <= 65536) and
    (.outputs | keys_are($seals | keys) and
      all(to_entries[]; (.value | art) and .value.bytes == $seals[.key].bytes and .value.sha256 == $seals[.key].sha256))) and
  (.namespaces | keys_are(["derive_dir","generation_dir","publish_receipt","query_dir","store_dir"]) and
    all(.[]; abs) and ([.[]] | unique | length) == 5) and
  (.generation | keys_are(["max_memory_bytes"]) and (.max_memory_bytes | uint and . > 0 and . <= 8589934592)) and
  (.publish | keys_are(["limits"]) and
    (.limits | keys_are(["already_pinned_bytes","max_active_queries","max_memory_bytes","max_parallel_gets","max_parallel_source_gets",
                         "max_query_bytes","max_query_gets","max_query_scratch_bytes","max_source_bytes","max_source_gets"]) and
      all(.[]; uint) and .max_memory_bytes > 0 and .max_parallel_gets == 16 and .max_parallel_source_gets == 16)) and
  (.baseline | keys_are(["fetch_parallelism"]) and .fetch_parallelism == 16) and
  (.phases | keys_are(["baseline","derive","generation","publish","stage"]) and (.derive | phase(3600)) and
    (.stage | phase(300)) and (.generation | phase(2700)) and (.publish | phase(1800)) and (.baseline | phase(900))))
EOF

IFS= read -r -d '' JQ_DERIVE_CONFIG <<'EOF' || true
def uint: type == "number" and . >= 0 and floor == .;
$g[0] as $c |
((keys) == (["corpus_intervals","dimensions","original_corpus","producer_authority","resources","rows","schema"] | sort)) and
.schema == "borsuk-native-scale-derivation-config-v1" and
.original_corpus == {path: $c.prepared.outputs["corpus.f32"].path, bytes: $seals["corpus.f32"].bytes, sha256: $seals["corpus.f32"].sha256} and
.rows == 1000000 and .dimensions == 1024 and
.corpus_intervals == [{start:0,end:100000},{start:101000,end:1001000}] and
.producer_authority == $c.producer_authority and
((.resources | keys) == (["caller_payload_bytes","caller_scratch_bytes","max_aggregate_scratch_bytes","max_payload_bytes","temporary_reserve_bytes"] | sort)) and
(.resources | all(.[]; uint))
EOF

IFS= read -r -d '' JQ_COMPLETE <<'EOF' || true
.schema == "borsuk-cohere-native-cohort-receipt-v3" and .status == "COMPLETE" and
.dataset == $dataset and .revision == $revision and
.geometry.corpus_rows == 1000000 and .geometry.query_rows == 32 and .geometry.dimensions == 1024 and .geometry.k == 10 and
.geometry.corpus_intervals == [{start:0,end:100000},{start:101000,end:1001000}] and
.geometry.reserved_query_interval == {start:100000,end:101000} and
.geometry.query_source_ordinals == [100000,100032] and
.reserved_queries_sha256 == $reserved and
(.outputs | length) == 5 and
([.outputs[] | {key:.name, value:{bytes:.bytes, sha256:.sha256}}] | from_entries) == $seals
EOF

IFS= read -r -d '' JQ_DERIVATION <<'EOF' || true
def uint: type == "number" and . >= 0 and floor == .;
def sha: type == "string" and test("^[0-9a-f]{64}$");
def seal($bytes): ((keys) == ["bytes","sha256"]) and .bytes == $bytes and (.sha256 | sha);
def finite_bits: uint and . < 4294967296 and (. % 2147483648) < 2139095040;
def positive_bits: uint and . >= 1 and . < 2139095040;
$dc[0] as $c |
((keys) == (["admission","corpus_intervals","dimensions","low_f32_bits","original_corpus","outputs","producer_authority",
              "producer_config_sha256","query_or_truth_used","recipe","rows","schema","source_identity_qualification",
              "status","step_f32_bits"] | sort)) and
.schema == "borsuk-native-scale-derivation-receipt-v2" and
.producer_config_sha256 == $dsha and
.status == "COMPLETE" and .recipe == "normalize_then_flat_fit_then_sq8" and .query_or_truth_used == false and
.original_corpus == {bytes: $seals["corpus.f32"].bytes, sha256: $seals["corpus.f32"].sha256} and
.rows == 1000000 and .dimensions == 1024 and .corpus_intervals == $c.corpus_intervals and
(.outputs | ((keys) == ["normalized","source_order","sq8"]) and
  (.normalized | seal(4096000000)) and (.source_order | seal(8000000)) and (.sq8 | seal(1036000000))) and
.producer_authority == $c.producer_authority and
.source_identity_qualification == "external_frozen_prerequisite_not_self_certified" and
(.low_f32_bits | length == 1024 and all(.[]; finite_bits)) and
(.step_f32_bits | length == 1024 and all(.[]; positive_bits)) and
(.admission | type == "object")
EOF

# IEEE-754 binary32 bits -> the exactly equal JSON double (every f32 is an exact f64), and an independent inverse
# used as a round-trip guard on the decoded low/step before any generation config is written.
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

IFS= read -r -d '' JQ_GEN_CHECK <<'EOF' || true
(.low_f32_bits + .step_f32_bits) | length == 2048 and all(.[]; f32ok)
EOF

IFS= read -r -d '' JQ_GEN_CONFIG <<'EOF' || true
.low_f32_bits as $low | .step_f32_bits as $step | .outputs as $o |
{discovery:"semantic", semantic_profile:"scale1m",
 order:{path:$order, sha256:$o.source_order.sha256},
 raw:$raw, raw_sha256:$o.normalized.sha256, sq8:$sq8, sq8_sha256:$o.sq8.sha256,
 rows:1000000, dimensions:1024, generation:1, base_epoch:0,
 low:($low | map(f32dec)), step:($step | map(f32dec)),
 sq8_object_key:$key, sq8_etag:$etag}
EOF

IFS= read -r -d '' JQ_PUBLISH_CONFIG <<'EOF' || true
{schema:"borsuk-two-bit-local-publication-config-v1",
 root:{path:$generation, sha256:$root}, store_root:$store, prefix:$prefix, limits:.publish.limits}
EOF

IFS= read -r -d '' JQ_PUBLISH_RECEIPT <<'EOF' || true
((keys) == (["config_sha256","control_epoch","generation","metadata_prefix","prefix","root_sha256","schema"] | sort)) and
.schema == "borsuk-two-bit-local-publication-receipt-v1" and .config_sha256 == $csha and .prefix == $prefix and
.root_sha256 == $root and .generation == 1 and (.control_epoch | type == "number") and
(.metadata_prefix | type == "string" and test("^[A-Za-z0-9_./-]{1,512}$"))
EOF

IFS= read -r -d '' JQ_BASELINE_CONFIG <<'EOF' || true
. as $g | $d[0] as $r |
{schema:"borsuk-cohere-native-baseline-config-v7", dataset:$dataset, revision:$revision,
 metric:"cosine", tie_rule:"corpus_ordinal_ascending",
 corpus_intervals:[{start:0,end:100000},{start:101000,end:1001000}], reserved_query_interval:{start:100000,end:101000},
 cohort_receipt:$g.prepared.complete,
 derivation_receipt:{path:$dpath, bytes:$dbytes, sha256:$dsha},
 producer_authority:$g.producer_authority,
 corpus_source_first:0, query_source_first:100000, rows:1000000, dimensions:1024, count:32, k:10,
 profile:"scale1m", backend:{kind:"local", store_root:$store},
 generation_prefix:$meta, generation_root_sha256:$root, scratch_parent:$scratch,
 requests:$g.prepared.outputs["queries.f32"], truth:$g.prepared.outputs["truth.u64"],
 native_source:{source_sha256:$r.outputs.normalized.sha256, sq8_sha256:$r.outputs.sq8.sha256,
                source_order_sha256:$r.outputs.source_order.sha256},
 max_memory_bytes:$memory, fetch_parallelism:$g.baseline.fetch_parallelism, serving:{mode:"baseline"},
 execution:{mode:"full"}}
EOF

IFS= read -r -d '' JQ_EFFECTIVE <<'EOF' || true
def cap($key): [.[]|.[$key]|select(. != "absent" and . != "max")|tonumber] |
  if length > 0 then min else error("missing effective limit") end;
. as $all |
{path:.[0].path,cpuset:.[0].cpuset_cpus_effective,
 memory_max_bytes:cap("memory_max"),swap_max_bytes:cap("memory_swap_max"),pids_max:cap("pids_max"),
 cpu_quota_cores:([.[]|.cpu_max|select(. != "absent")|split(" ")|select(.[0] != "max")|
    (.[0]|tonumber)/(.[1]|tonumber)] | if length > 0 then min else error("missing CPU quota") end)} |
if (.memory_max_bytes == 8589934592 and .swap_max_bytes == 0 and
    .pids_max == 128 and .cpu_quota_cores == 4 and .cpuset == "0-3" and
    ($all[0].memory_current|tonumber) <= 8589934592 and
    ($all[0].memory_peak|tonumber) <= 8589934592 and ($all[0].memory_swap_current|tonumber) == 0)
then . else error("effective resource ceiling") end
EOF

# memory.events "max" (reclaim pressure) must only be monotonic; every OOM counter and every pids.events counter must be
# unchanged. Limits and cgroup chain must be identical between consecutive samples.
IFS= read -r -d '' JQ_EVENTS <<'EOF' || true
def uint: type == "number" and . >= 0 and floor == .;
def limits: {path,cpu_max,cpuset_cpus_effective,memory_max,memory_swap_max,pids_max};
def events: if . == "absent" then {} else
  split("\n") | map(select(length > 0) | split(" ")|{key:.[0],value:(.[1]|tonumber)}) | from_entries end;
[range(0;$before|length) | . as $i |
  ($before[$i].memory_events|events) as $b | ($after[$i].memory_events|events) as $a |
  {path:$before[$i].path,before:$b.max,after:$a.max,delta:(($a.max // 0)-($b.max // 0))}] as $max |
{from:$from,to:$to,memory_max_events:$max,valid:(
  all($max[]; if .path == "/sys/fs/cgroup" and .before == null and .after == null then .delta == 0
    else (.before|uint) and (.after|uint) and .delta >= 0 end) and
  ($before|length) == ($after|length) and all(range(0;$before|length);
  . as $i | ($before[$i]|limits) == ($after[$i]|limits) and
  ($before[$i].memory_events|events|{oom,oom_kill,oom_group_kill}) ==
    ($after[$i].memory_events|events|{oom,oom_kill,oom_group_kill}) and
  ($before[$i].pids_events|events) == ($after[$i].pids_events|events)))}
EOF

# ---- admission: config, binaries, prepared outputs ---------------------------------------------------------------
canonical "$config"
[[ -f $config ]] || die 'regular config'
config_bytes=$(stat -c %s -- "$config")
(( config_bytes > 0 && config_bytes <= 65536 )) || die 'config byte cap'
authenticate "$config" "$config_bytes" "$config_sha" > "$evidence/config.original.jsonl"
cp -- "$config" "$evidence/config.json"
authenticate "$evidence/config.json" "$config_bytes" "$config_sha" > "$evidence/config.copy.jsonl"
jq -e -s --argjson seals "$SEALS" --arg dsha "$DERIVE_CONFIG_SHA" --argjson dbytes "$DERIVE_CONFIG_BYTES" \
    "$JQ_CONFIG" "$evidence/config.json" > "$evidence/config.validated.txt"
# Observational measurements: a failed tool query is recorded in the text; failing to write the record is fatal.
tools=$({
    jq --version || printf 'jq --version failed rc=%s\n' "$?"
    bash --version | head -n 1 || printf 'bash --version failed rc=%s\n' "$?"
    sha256sum --version | head -n 1 || printf 'sha256sum --version failed rc=%s\n' "$?"
    date -u +%Y-%m-%dT%H:%M:%SZ || printf 'date failed rc=%s\n' "$?"
    uname -a || printf 'uname failed rc=%s\n' "$?"
} 2>&1)
printf '%s\n' "$tools" > "$evidence/tools.txt"

cfg() { jq -er "$1" "$evidence/config.json"; }
scratch_root=$(cfg .scratch_root)
derive_dir=$(cfg .namespaces.derive_dir)
store_dir=$(cfg .namespaces.store_dir)
generation_dir=$(cfg .namespaces.generation_dir)
publish_receipt=$(cfg .namespaces.publish_receipt)
query_dir=$(cfg .namespaces.query_dir)
deadline_seconds=$(cfg .deadline_seconds)
disk_proposal_bytes=$(cfg .disk_proposal_bytes)
disk_min_available_bytes=$(cfg .disk_min_available_bytes)
derive_elf=$(cfg .binaries.derive.path)
generation_elf=$(cfg .binaries.generation.path)
publish_elf=$(cfg .binaries.publish.path)
baseline_elf=$(cfg .binaries.baseline.path)
derive_config=$(cfg .derive_config.path)
generation_memory=$(cfg .generation.max_memory_bytes)

canonical "$scratch_root"
[[ -d $scratch_root ]] && mountpoint -q -- "$scratch_root" || die 'scratch_root must be a dedicated mountpoint'
[[ $evidence != "$scratch_root" && $evidence != "$scratch_root/"* && $scratch_root != "$evidence/"* ]] || die 'evidence/scratch overlap'
scratch_ns=("$derive_dir" "$store_dir" "$generation_dir" "$publish_receipt" "$query_dir")
for ns in "${scratch_ns[@]}"; do
    new_path "$ns"
    [[ ${ns%/*} == "$scratch_root" ]] || die "namespace must be a direct child of scratch_root: $ns"
done
for elf in "$derive_elf" "$generation_elf" "$publish_elf" "$baseline_elf"; do
    canonical "$elf"
    [[ -x $elf ]] || die "native executable: $elf"
done
jq -r '[.binaries[], .derive_config, .prepared.complete, .prepared.outputs[]] | .[] | [.path,.bytes,.sha256] | @tsv' \
    "$evidence/config.json" > "$evidence/inputs.tsv"
authenticate_inputs() {
    local path bytes sha
    authenticate "$config" "$config_bytes" "$config_sha"
    while IFS=$'\t' read -r path bytes sha; do authenticate "$path" "$bytes" "$sha"; done < "$evidence/inputs.tsv"
}

# ---- cgroup evidence --------------------------------------------------------------------------------------------
resources() {
    local label=$1 relative dir name value key
    local -a args
    [[ $(stat -f -c %T /sys/fs/cgroup) == cgroup2fs ]] || die 'cgroup v2 required'
    relative=$(sed -n 's/^0:://p' /proc/self/cgroup)
    [[ $relative == /* && $relative != *..* && $relative != *$'\n'* ]] || die 'cgroup membership'
    dir=/sys/fs/cgroup${relative%/}
    canonical "$dir"
    while :; do
        args=(--arg path "$dir")
        for name in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max memory.current memory.peak \
                memory.swap.current memory.events pids.events pids.current pids.peak; do
            value=absent
            if [[ -r $dir/$name ]]; then
                value=$(< "$dir/$name")
            elif [[ $dir != /sys/fs/cgroup && $name != pids.current && $name != pids.peak ]]; then
                die "missing cgroup evidence: $dir/$name"
            fi
            key=${name//./_}
            args+=(--arg "$key" "$value")
        done
        jq -cn "${args[@]}" '$ARGS.named'
        [[ $dir != /sys/fs/cgroup ]] || break
        dir=${dir%/*}
    done > "$evidence/resources.$label.jsonl"
    jq -e -s "$JQ_EFFECTIVE" "$evidence/resources.$label.jsonl" > "$evidence/resources.$label.effective.json"
}
resource_closure() {
    local label=$1 previous=$2
    cmp -- "$evidence/resources.$previous.effective.json" "$evidence/resources.$label.effective.json"
    jq -n --arg from "$previous" --arg to "$label" \
        --slurpfile before "$evidence/resources.$previous.jsonl" --slurpfile after "$evidence/resources.$label.jsonl" \
        "$JQ_EVENTS" > "$evidence/resource-events.$label.json"
    jq -e '.valid' "$evidence/resource-events.$label.json" > "$evidence/resource-closure.$label.txt"
}
# Observational only: a failed measurement is recorded in the sample and never gates the run, but the sample file itself
# is evidence, so failing to write it is fatal.
sample_disk() {
    local label=$1 ns body
    body=$({
        printf 'label=%s\n' "$label"
        stat -f -c 'fs_block=%S blocks=%b free=%f avail=%a' -- "$scratch_root" || printf 'stat-f-failed rc=%s\n' "$?"
        for ns in "${scratch_ns[@]}"; do
            if [[ -e $ns ]]; then du -sb -- "$ns" || printf 'du-failed rc=%s %s\n' "$?" "$ns"; else printf 'absent %s\n' "$ns"; fi
        done
    } 2>&1)
    printf '%s\n' "$body" > "$evidence/disk.$label.txt"
}

# ---- one native process at a time -------------------------------------------------------------------------------
IFS= read -r -d '' INNER <<'EOF' || true
set -u -o noclobber
dir=$1 errfd=$2
shift 2
"$@" 2>&"$errfd"
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
eff_timeout=0
effective_timeout() {
    local want left
    want=$(cfg ".phases.$1.timeout_seconds")
    left=$(( deadline_seconds - SECONDS ))
    # The configured timeout is never shortened or extended: a phase that cannot fit (plus kill-after) is refused up front.
    (( left > want + 10 )) || die "whole-run deadline cannot fit phase $1 timeout $want (+10s kill-after): $left s left"
    eff_timeout=$want
}
# Limit only the log writers; the native process keeps its own output allowance.
run_phase() {
    local name=$1 cap=$2 dir native_stderr_fd supervisor_stderr_fd time_log_fd
    local native_stderr_pid supervisor_stderr_pid time_log_pid started
    local -a pipeline
    shift 2
    dir=$evidence/phases/$name
    mkdir -p -- "$evidence/phases"
    mkdir -- "$dir"
    stage=$name
    effective_timeout "$name"
    jq -n --args '$ARGS.positional' -- "$@" > "$dir/argv.json"
    printf '%s\n' "$eff_timeout" > "$dir/timeout.seconds"
    exec {native_stderr_fd}> >(trap - EXIT ERR; exec prlimit --core=0:0 --fsize=1048576:1048576 -- tee -- "$dir/native.stderr.txt" >/dev/null)
    native_stderr_pid=$!
    exec {supervisor_stderr_fd}> >(trap - EXIT ERR; exec prlimit --core=0:0 --fsize=1048576:1048576 -- tee -- "$dir/supervisor.stderr.txt" >/dev/null)
    supervisor_stderr_pid=$!
    exec {time_log_fd}> >(trap - EXIT ERR; exec prlimit --core=0:0 --fsize=1048576:1048576 -- tee -- "$dir/native.time.txt" >/dev/null)
    time_log_pid=$!
    started=$EPOCHREALTIME
    # Each supervisor records its own original child status; a killed layer leaves a missing receipt.
    set +e
    /usr/bin/time -v -o "/dev/fd/$time_log_fd" bash -c "$OUTER" _ "$INNER" "$dir" "$native_stderr_fd" "$eff_timeout" "$@" \
        2>&"$supervisor_stderr_fd" | prlimit --core=0:0 --fsize="$cap:$cap" -- tee -- "$dir/native.stdout" >/dev/null
    pipeline=("${PIPESTATUS[@]}")
    set -e
    printf '%s %s\n' "$started" "$EPOCHREALTIME" > "$dir/wall.txt"
    printf '%s\n' "${pipeline[0]}" > "$dir/time.exit"
    printf '%s\n' "${pipeline[1]}" > "$dir/tee.exit"
    exec {time_log_fd}>&- {supervisor_stderr_fd}>&- {native_stderr_fd}>&-
    # Later writers inherit earlier pipe descriptors, so collect them in reverse creation order.
    set +e
    wait "$time_log_pid"
    printf '%s\n' "$?" > "$dir/time-log.exit"
    wait "$supervisor_stderr_pid"
    printf '%s\n' "$?" > "$dir/supervisor-stderr-log.exit"
    wait "$native_stderr_pid"
    printf '%s\n' "$?" > "$dir/native-stderr-log.exit"
    set -e
}
exit_of() { # phase file -> numeric exit, refusing a missing/garbled receipt
    local file=$evidence/phases/$1/$2.exit
    [[ -f $file && ! -L $file ]] || die "phase $1: missing $2 exit"
    [[ $(< "$file") =~ ^[0-9]+$ ]] || die "phase $1: malformed $2 exit"
    printf '%s\n' "$(< "$file")"
}
require_clean_exits() { # phase [allowed native exit...]: stdout/log writers must be clean; supervisors mirror native
    local name=$1 native f
    native=$(exit_of "$name" native)
    for f in tee time-log supervisor-stderr-log native-stderr-log; do
        [[ $(exit_of "$name" "$f") == 0 ]] || die "phase $name: nonzero $f exit"
    done
    [[ $(exit_of "$name" timeout) == "$native" && $(exit_of "$name" time) == "$native" ]] || die "phase $name: supervisor exit differs from native"
    awk -F ': ' '/Maximum resident set size \(kbytes\)/ {n++; if ($2 <= 0 || $2 > 8388608) bad=1}
        END {exit (n != 1 || bad)}' "$evidence/phases/$name/native.time.txt" || die "phase $name: GNU time RSS evidence"
    printf '%s\n' "$native"
}
finish_phase() {
    local name=$1
    resources "after-$name"
    resource_closure "after-$name" "$prev_label"
    prev_label=after-$name
    sample_disk "after-$name"
    completed+=("$name")
}
require_success() {
    [[ $(require_clean_exits "$1") == 0 ]] || die "phase $1: native exit nonzero"
}

# ---- start: resources, inputs, disk -----------------------------------------------------------------------------
stage=inputs-before
resources before
prev_label=before
authenticate_inputs > "$evidence/inputs.before.jsonl"
# Semantic parse of the derive config only after it was authenticated as a regular, exact-size, exact-SHA file
# (authenticate_inputs above), and before any native derive call.
jq -e --slurpfile g "$evidence/config.json" --argjson seals "$SEALS" "$JQ_DERIVE_CONFIG" "$derive_config" \
    > "$evidence/derive-config.validated.txt"
jq -e --argjson seals "$SEALS" --arg dataset "$DATASET" --arg revision "$REVISION" --arg reserved "$RESERVED_SHA" \
    "$JQ_COMPLETE" "$(cfg .prepared.complete.path)" > "$evidence/complete.validated.txt"
# disk_proposal_bytes is the TOTAL cap (it may already include the retained prepared outputs); disk_min_available_bytes is
# the root-computed FRESH free space required now. They are recorded separately and never derived from each other.
prepared_bytes=$(jq -n --argjson seals "$SEALS" '[$seals[].bytes] | add')
fs_avail=$(( $(stat -f -c %a -- "$scratch_root") * $(stat -f -c %S -- "$scratch_root") ))
fs_total=$(( $(stat -f -c %b -- "$scratch_root") * $(stat -f -c %S -- "$scratch_root") ))
printf 'total_cap_bytes=%s fresh_free_required_bytes=%s fs_total=%s fs_avail=%s prepared_five_output_bytes=%s total_cap_minus_prepared=%s\n' \
    "$disk_proposal_bytes" "$disk_min_available_bytes" "$fs_total" "$fs_avail" "$prepared_bytes" \
    "$(( disk_proposal_bytes - prepared_bytes ))" > "$evidence/disk.admission.txt"
(( fs_avail >= disk_min_available_bytes && fs_total >= disk_proposal_bytes )) || die 'scratch disk admission'
sample_disk before

# ---- 1 derive ---------------------------------------------------------------------------------------------------
run_phase derive "$(cfg .phases.derive.stdout_cap_bytes)" \
    "$derive_elf" --derive "$derive_config" "$DERIVE_CONFIG_SHA" "$derive_dir"
require_success derive
finish_phase derive
stage=derive-outputs
canonical "$derive_dir"
[[ $(find "$derive_dir" -mindepth 1 -maxdepth 1 -printf '%f\n' | sort | tr '\n' ' ') == 'derivation.json normalized.f32 order.u64 sq8.bin ' ]] \
    || die 'unexpected derive output roster'
derivation=$derive_dir/derivation.json
[[ -f $derivation && ! -L $derivation ]] || die 'derivation receipt must be a regular file before it is read or parsed'
derivation_bytes=$(stat -c %s -- "$derivation")
(( derivation_bytes > 0 && derivation_bytes <= 65536 )) || die 'derivation receipt byte cap'
derivation_sha=$(sha256sum < "$derivation")
derivation_sha=${derivation_sha%% *}
jq -e --slurpfile dc "$derive_config" --arg dsha "$DERIVE_CONFIG_SHA" --argjson seals "$SEALS" \
    "$JQ_DERIVATION" "$derivation" > "$evidence/derivation.validated.txt"
jq -e "$JQ_F32 $JQ_GEN_CHECK" "$derivation" > "$evidence/derivation.f32-roundtrip.txt"
jq -r '[["normalized.f32",.outputs.normalized.bytes,.outputs.normalized.sha256],
        ["order.u64",.outputs.source_order.bytes,.outputs.source_order.sha256],
        ["sq8.bin",.outputs.sq8.bytes,.outputs.sq8.sha256]] | .[] | @tsv' "$derivation" > "$evidence/derive-outputs.tsv"
while IFS=$'\t' read -r name bytes sha; do
    authenticate "$derive_dir/$name" "$bytes" "$sha"
done < "$evidence/derive-outputs.tsv" > "$evidence/derive-outputs.authenticated.jsonl"
authenticate "$derivation" "$derivation_bytes" "$derivation_sha" > "$evidence/derivation.authenticated.jsonl"
normalized=$derive_dir/normalized.f32
order=$derive_dir/order.u64
sq8=$derive_dir/sq8.bin
sq8_bytes=$(jq -er .outputs.sq8.bytes "$derivation")
sq8_sha=$(jq -er .outputs.sq8.sha256 "$derivation")

# ---- 2 SQ8 full-copy stage (create-only; every file retained) --------------------------------------------------
etag_of() { # object_store 0.14.1 LocalFileSystem: quoted lowercase hex inode-mtime_microseconds-size
    local fields ino size sec time frac
    fields=$(stat -c '%i %s %Y %y' -- "$1")
    read -r ino size sec _ time _ <<< "$fields"
    [[ $time =~ ^[0-9:]+\.([0-9]{9})$ ]] || die "mtime precision: $1"
    frac=${BASH_REMATCH[1]}
    printf '"%x-%x-%x"\n' "$ino" $(( sec * 1000000 + 10#${frac:0:6} )) "$size"
}
stage=stage_prepare
sq8_key=semantic/objects/$sq8_sha
staged=$store_dir/$sq8_key
[[ $(stat -c %h -- "$sq8") == 1 ]] || die 'derived SQ8 must be single-link'
authenticate "$sq8" "$sq8_bytes" "$sq8_sha" > "$evidence/sq8.source.before.jsonl"
mkdir -- "$store_dir" "$store_dir/semantic" "$store_dir/semantic/objects"
sync -f "$store_dir"
run_phase stage "$(cfg .phases.stage.stdout_cap_bytes)" \
    /usr/bin/dd "if=$sq8" "of=$staged" bs=1048576 conv=excl,fsync status=none
require_success stage
sync -f "$store_dir"
finish_phase stage
stage=stage_verify
[[ -f $staged && ! -L $staged && $(stat -c %h -- "$staged") == 1 ]] || die 'staged SQ8 must be a single-link regular file'
authenticate "$staged" "$sq8_bytes" "$sq8_sha" > "$evidence/sq8.staged.jsonl"
sq8_etag=$(etag_of "$staged")
authenticate "$staged" "$sq8_bytes" "$sq8_sha" > "$evidence/sq8.staged.after-etag.jsonl"
cmp -- "$evidence/sq8.staged.jsonl" "$evidence/sq8.staged.after-etag.jsonl"
authenticate "$sq8" "$sq8_bytes" "$sq8_sha" > "$evidence/sq8.source.after.jsonl"
cmp -- "$evidence/sq8.source.before.jsonl" "$evidence/sq8.source.after.jsonl"
jq -n --arg path "$staged" --arg key "$sq8_key" --arg etag "$sq8_etag" --arg sha256 "$sq8_sha" --argjson bytes "$sq8_bytes" \
    '{path:$path,object_key:$key,etag:$etag,sha256:$sha256,bytes:$bytes,full_copy:true,create_only:true}' > "$evidence/sq8.staged.json"

# ---- 3 generation -----------------------------------------------------------------------------------------------
stage=generation-config
mkdir -- "$evidence/configs"
jq -c --arg order "$order" --arg raw "$normalized" --arg sq8 "$sq8" --arg key "$sq8_key" --arg etag "$sq8_etag" \
    "$JQ_F32 $JQ_GEN_CONFIG" "$derivation" > "$evidence/configs/generation.json"
generation_config=$evidence/configs/generation.json
generation_config_bytes=$(stat -c %s -- "$generation_config")
(( generation_config_bytes > 0 && generation_config_bytes <= 65536 )) || die 'generation config byte cap'
generation_config_sha=$(sha256sum < "$generation_config")
generation_config_sha=${generation_config_sha%% *}
run_phase generation "$(cfg .phases.generation.stdout_cap_bytes)" \
    "$generation_elf" "$generation_config" "$generation_config_sha" "$generation_memory" "$generation_dir"
require_success generation
finish_phase generation
stage=generation-verify
[[ $(stat -c %s -- "$evidence/phases/generation/native.stdout") == 65 ]] || die 'generation stdout must be one SHA line'
root_sha=$(< "$evidence/phases/generation/native.stdout")
[[ $root_sha =~ ^[0-9a-f]{64}$ ]] || die 'generation root SHA'
canonical "$generation_dir"
manifest_bytes=$(stat -c %s -- "$generation_dir/manifest.json")
authenticate "$generation_dir/manifest.json" "$manifest_bytes" "$root_sha" > "$evidence/generation.root.jsonl"
[[ -f $generation_dir/plane/manifest.json && ! -L $generation_dir/plane/manifest.json ]] || die 'plane manifest must be a regular file before it is parsed'
plane_bytes=$(stat -c %s -- "$generation_dir/plane/manifest.json")
(( plane_bytes > 0 && plane_bytes <= 1048576 )) || die 'plane manifest byte cap'
jq -e --arg order_sha "$(jq -er .outputs.source_order.sha256 "$derivation")" '.source_order_sha256 == $order_sha' \
    "$generation_dir/plane/manifest.json" > "$evidence/generation.plane-binds-order.txt"

# ---- 4 ordinary LocalFileSystem publish -------------------------------------------------------------------------
stage=publish-config
jq -c --arg generation "$generation_dir" --arg root "$root_sha" --arg store "$store_dir" --arg prefix "$STORE_PREFIX" \
    "$JQ_PUBLISH_CONFIG" "$evidence/config.json" > "$evidence/configs/publish.json"
publish_config=$evidence/configs/publish.json
publish_config_bytes=$(stat -c %s -- "$publish_config")
(( publish_config_bytes > 0 && publish_config_bytes <= 65536 )) || die 'publish config byte cap'
publish_config_sha=$(sha256sum < "$publish_config")
publish_config_sha=${publish_config_sha%% *}
run_phase publish "$(cfg .phases.publish.stdout_cap_bytes)" \
    "$publish_elf" "$publish_config" "$publish_config_sha" "$publish_receipt"
require_success publish
finish_phase publish
stage=publish-verify
[[ $(stat -c %s -- "$evidence/phases/publish/native.stdout") == 0 ]] || die 'publisher success log must be empty'
[[ -f $publish_receipt && ! -L $publish_receipt ]] || die 'publish receipt must be a regular file before it is parsed'
publish_receipt_bytes=$(stat -c %s -- "$publish_receipt")
(( publish_receipt_bytes > 0 && publish_receipt_bytes <= 65536 )) || die 'publish receipt byte cap'
jq -e --arg csha "$publish_config_sha" --arg prefix "$STORE_PREFIX" --arg root "$root_sha" \
    "$JQ_PUBLISH_RECEIPT" "$publish_receipt" > "$evidence/publish.validated.txt"
metadata_prefix=$(jq -er .metadata_prefix "$publish_receipt")
authenticate "$staged" "$sq8_bytes" "$sq8_sha" > "$evidence/sq8.staged.after-publish.jsonl"
cmp -- "$evidence/sq8.staged.jsonl" "$evidence/sq8.staged.after-publish.jsonl"

# ---- 5 Q32 baseline (opaque output, original exit exposed) ------------------------------------------------------
stage=baseline-config
mkdir -- "$query_dir" "$query_dir/scratch"
sync -f "$query_dir"
jq -c --slurpfile d "$derivation" --arg dpath "$derivation" --argjson dbytes "$derivation_bytes" --arg dsha "$derivation_sha" \
    --arg store "$store_dir" --arg meta "$metadata_prefix" --arg root "$root_sha" --arg scratch "$query_dir/scratch" \
    --arg dataset "$DATASET" --arg revision "$REVISION" --argjson memory "$BASELINE_MEMORY" \
    "$JQ_BASELINE_CONFIG" "$evidence/config.json" > "$evidence/configs/baseline.json"
baseline_config=$evidence/configs/baseline.json
baseline_config_bytes=$(stat -c %s -- "$baseline_config")
(( baseline_config_bytes > 0 && baseline_config_bytes <= 65536 )) || die 'baseline config byte cap'
baseline_config_sha=$(sha256sum < "$baseline_config")
baseline_config_sha=${baseline_config_sha%% *}
baseline_result=$query_dir/baseline-result.jsonl
baseline_invoked=1
run_phase baseline "$(cfg .phases.baseline.stdout_cap_bytes)" \
    "$baseline_elf" "$baseline_config" "$baseline_config_sha" "$baseline_result"
baseline_exit=$(require_clean_exits baseline)
[[ $baseline_exit == 0 || $baseline_exit == 2 || $baseline_exit == 3 ]] || die "baseline native exit $baseline_exit is not a plain 0/2/3 exit"
finish_phase baseline
stage=baseline-result
if [[ -f $baseline_result && ! -L $baseline_result && $(stat -c %s -- "$baseline_result") -gt 0 ]]; then
    baseline_result_bytes=$(stat -c %s -- "$baseline_result")
    baseline_result_sha=$(sha256sum < "$baseline_result")
    authenticate "$baseline_result" "$baseline_result_bytes" "${baseline_result_sha%% *}" > "$evidence/baseline-result.authenticated.jsonl"
else
    [[ $baseline_exit != 0 ]] || die 'baseline exit 0 without a nonempty result file'
    printf 'baseline result file absent or empty after nonzero native exit %s\n' "$baseline_exit" > "$evidence/baseline-result.absent.txt"
fi

# ---- closure ----------------------------------------------------------------------------------------------------
stage=closure
authenticate_inputs > "$evidence/inputs.after.jsonl"
cmp -- "$evidence/inputs.before.jsonl" "$evidence/inputs.after.jsonl"
while IFS=$'\t' read -r name bytes sha; do
    authenticate "$derive_dir/$name" "$bytes" "$sha"
done < "$evidence/derive-outputs.tsv" > "$evidence/derive-outputs.closed.jsonl"
cmp -- "$evidence/derive-outputs.authenticated.jsonl" "$evidence/derive-outputs.closed.jsonl"
authenticate "$derivation" "$derivation_bytes" "$derivation_sha" > "$evidence/derivation.closed.jsonl"
cmp -- "$evidence/derivation.authenticated.jsonl" "$evidence/derivation.closed.jsonl"
authenticate "$staged" "$sq8_bytes" "$sq8_sha" > "$evidence/sq8.staged.closed.jsonl"
cmp -- "$evidence/sq8.staged.jsonl" "$evidence/sq8.staged.closed.jsonl"
authenticate "$generation_dir/manifest.json" "$manifest_bytes" "$root_sha" > "$evidence/generation.root.closed.jsonl"
cmp -- "$evidence/generation.root.jsonl" "$evidence/generation.root.closed.jsonl"
for ns in "${scratch_ns[@]}"; do canonical "$ns"; done
# Observational inventory of every retained file; nothing is deleted.
# A failing find is recorded in inventory.status; failing to create or write the inventory files is fatal.
: > "$evidence/inventory.tsv"
: > "$evidence/inventory.stderr.txt"
set +e
find "$derive_dir" "$store_dir" "$generation_dir" "$publish_receipt" "$query_dir" -type f -printf '%s\t%p\n' 2>> "$evidence/inventory.stderr.txt" \
    | sort -t "$(printf '\t')" -k2 >> "$evidence/inventory.tsv"
inventory_status=("${PIPESTATUS[@]}")
set -e
printf 'find=%s sort=%s\n' "${inventory_status[0]}" "${inventory_status[1]}" > "$evidence/inventory.status"
[[ ${inventory_status[1]} == 0 ]] || die 'inventory write failed'
sync -f "$scratch_root"
resources closed
resource_closure closed "$prev_label"
sample_disk closed
verified=1
stage=native_chain_closed
exit "$baseline_exit"
