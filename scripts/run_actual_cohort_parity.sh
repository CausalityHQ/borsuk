#!/usr/bin/env bash
# One offline actual100k/Q32 parity cell; root owns staging and external closure.
set -Eeuo pipefail
set -o noclobber
umask 077
export LC_ALL=C

die() { printf 'INVALID: %s\n' "$*" >&2; exit 98; }
[[ $# == 8 ]] || die 'usage: CONFIG CONFIG_SHA QUALIFIED_PREPARER ELF_SHA NEW_OUTPUT EVIDENCE_DIR HISTORICAL_REQUESTS HISTORICAL_TRUTH'
config=$1 config_sha=$2 preparer=$3 elf_sha=$4 output=$5 evidence=$6 requests=$7 truth=$8
qualified_sha=a3a828beec8d898fdf1d26a3b6673ea16be5407da864222dc9a394c2d28221fb
corpus_sha=3c95fa49a7d3f9d4bf6178f5ac2493e700a30fbcfe91da97a5fcf16a1f5fc09c
requests_sha=8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e
truth_sha=479064239b698a2b8094c7838b1bb01af6eff5ea6eee4736692971849fdcfb2c
for tool in jq sha256sum stat realpath timeout tee head cmp sync sed awk cp mkdir; do
    command -v "$tool" >/dev/null || die "missing $tool"
done
[[ -x /usr/bin/time && $config_sha =~ ^[0-9a-f]{64}$ && $elf_sha == "$qualified_sha" ]] || die 'time/config SHA/qualified ELF pin'

canonical() {
    [[ $1 == /* && $1 != *[$'\t\r\n\\']* && $(realpath -e -- "$1") == "$1" ]] || die "noncanonical path: $1"
}
new_path() {
    [[ $1 != */ && ${1##*/} != . && ${1##*/} != .. && ! -e $1 && ! -L $1 ]] || die "occupied/invalid namespace: $1"
    canonical "${1%/*}"
    [[ -d ${1%/*} && $1 != *[$'\t\r\n\\']* ]] || die "namespace parent: $1"
}
new_path "$evidence"
new_path "$output"
[[ $output != "$evidence" && $output != "$evidence/"* && $evidence != "$output/"* ]] || die 'overlapping namespaces'
mkdir -- "$evidence"
stage=admission
failed_line=0
verified=0
finish() {
    local rc=$? status=INVALID
    trap '' HUP INT TERM
    trap - EXIT ERR
    set +e
    (( rc == 0 && verified == 1 )) && status=PARITY_VERIFIED
    [[ $status == PARITY_VERIFIED || $rc != 0 ]] || rc=98
    printf 'stage=%s line=%s exit=%s\n' "$stage" "$failed_line" "$rc" > "$evidence/closure.txt" || rc=98
    # Seal pre-status evidence; wrapper.exit and terminal.json record the later intent.
    (cd -- "$evidence" && for file in *; do
        [[ $file == closure.sha256 ]] || sha256sum -- "$file" || exit 98
    done) > "$evidence/closure.sha256" || rc=98
    sync -f "$evidence" || rc=98
    printf '%s\n' "$rc" > "$evidence/wrapper.exit" || rc=98
    (( rc == 0 )) || status=INVALID
    jq -n --arg status "$status" --arg stage "$stage" --argjson intended_exit "$rc" \
        --arg config_sha256 "$config_sha" --arg elf_sha256 "$elf_sha" --arg output "$output" \
        '{schema:"borsuk-actual-cohort-parity-local-v1",status:$status,stage:$stage,intended_exit:$intended_exit,
          config_sha256:$config_sha256,elf_sha256:$elf_sha256,output:$output,
          wrapper_exit_scope:"intended_exit",actual_manager_and_outer_exit_required:true,
          scope:"local native process, body parity and resource closure only",
          publication_verified:false,instance_termination_verified:false,performance_claim:false}' \
        > "$evidence/terminal.json" || rc=98
    # Root must require normal manager termination and original outer wait exits, including write failures.
    exit "$rc"
}
trap finish EXIT
trap 'failed_line=$LINENO' ERR
trap 'stage=signal-HUP; exit 129' HUP
trap 'stage=signal-INT; exit 130' INT
trap 'stage=signal-TERM; exit 143' TERM
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
canonical "$config"
[[ -f $config ]] || die 'regular config'
config_bytes=$(stat -c %s -- "$config")
(( config_bytes > 0 && config_bytes <= 65536 )) || die 'config byte cap'
authenticate "$config" "$config_bytes" "$config_sha" > "$evidence/config.original.jsonl"
cp -- "$config" "$evidence/config.json"
authenticate "$evidence/config.json" "$config_bytes" "$config_sha" > "$evidence/config.copy.jsonl"
# Only paths and actual output-parent identity vary from the preregistered template.
jq -e -s '
  def uint: type == "number" and . >= 0 and floor == .;
  length == 1 and (.[0] |
    (.output_parent | keys == ["device","inode","path"] and
      (.device|uint) and (.inode|uint and . > 0) and (.path|type == "string")) and
    ([.shards[].path] | length == 2 and all(.[]; type == "string")) and
    (del(.output_parent,.shards[].path) == {
      schema:"borsuk-cohere-native-cohort-config-v3",
      dataset:"CohereLabs/wikipedia-2023-11-embed-multilingual-v3",
      revision:"ade45fb52bd549f5e8c065636fe4160a43c2af36",embedding_column:"emb",document_id_column:"_id",
      geometry:{corpus:100000,queries:32,dimensions:1024,k:10},
      corpus_intervals:[{start:0,end:100000}],reserved_query_interval:{start:100000,end:101000},
      shards:[{publisher_path:"en/0000.parquet",bytes:216612385,rows:100000,
        sha256:"2c6abfffa7dd336113251b3e6f3fe4ee16688ead7c67b99593cbadc5589e28b3"},
        {publisher_path:"en/0001.parquet",bytes:216746705,rows:100000,
        sha256:"1ce373d5730494429a64a0f788c62f71226ded2bc670aaf9399204e91e544c3b"}],
      resources:{batch_rows:256,max_batch_bytes:268435456,max_footer_bytes:1048576,max_id_bytes:1024,
        max_row_group_compressed_bytes:268435456,max_row_group_rows:10000,
        max_row_group_uncompressed_bytes:1073741824,modeled_memory_bytes:7516192768,
        cpu_limit:4,actual_memory_bytes:8589934592,swap_bytes:0,scratch_bytes:4294967296,
        timeout_seconds:2400,truth_block_rows:256,caller_memory_bytes:67108864,
        caller_scratch_bytes:0,temporary_reserve_bytes:67108864}}))
' "$evidence/config.json" > "$evidence/config.validated.txt"
parent=$(jq -er '.output_parent.path' "$evidence/config.json")
canonical "$parent"
parent_identity=$(jq -r '.output_parent | "\(.device):\(.inode)"' "$evidence/config.json")
[[ -d $parent && ${output%/*} == "$parent" && $(stat -c '%d:%i' -- "$parent") == "$parent_identity" ]] || die 'output parent identity'
evidence_identity=$(stat -c '%d:%i' -- "$evidence")
jq -r '.shards[] | [.path,.bytes,.sha256] | @tsv' "$evidence/config.json" > "$evidence/shards.tsv"
authenticate_inputs() {
    local path bytes sha
    authenticate "$config" "$config_bytes" "$config_sha"
    authenticate "$preparer" 10015160 "$qualified_sha"
    while IFS=$'\t' read -r path bytes sha; do authenticate "$path" "$bytes" "$sha"; done < "$evidence/shards.tsv"
    authenticate "$requests" 4096000 "$requests_sha"
    authenticate "$truth" 80000 "$truth_sha"
}

resources() {
    local phase=$1 relative dir name value key
    local -a args
    [[ $(stat -f -c %T /sys/fs/cgroup) == cgroup2fs ]] || die 'cgroup v2 required'
    relative=$(sed -n 's/^0:://p' /proc/self/cgroup)
    [[ $relative == /* && $relative != *..* && $relative != *$'\n'* ]] || die 'cgroup membership'
    dir=/sys/fs/cgroup${relative%/}
    canonical "$dir"
    while :; do
        args=(--arg path "$dir")
        for name in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max memory.current memory.peak memory.swap.current memory.events pids.events; do
            value=absent
            if [[ -r $dir/$name ]]; then
                value=$(< "$dir/$name")
            elif [[ $dir != /sys/fs/cgroup ]]; then
                die "missing cgroup evidence: $dir/$name"
            fi
            key=${name//./_}
            args+=(--arg "$key" "$value")
        done
        jq -cn "${args[@]}" '$ARGS.named'
        [[ $dir != /sys/fs/cgroup ]] || break
        dir=${dir%/*}
    done > "$evidence/resources.$phase.jsonl"
    jq -e -s '
      def cap($key): [.[]|.[$key]|select(. != "absent" and . != "max")|tonumber] |
        if length > 0 then min else error("missing effective limit") end;
      . as $all |
      {path:.[0].path,cpuset:.[0].cpuset_cpus_effective,
       memory_max_bytes:cap("memory_max"),swap_max_bytes:cap("memory_swap_max"),pids_max:cap("pids_max"),
       cpu_quota_cores:([.[]|.cpu_max|select(. != "absent")|split(" ")|select(.[0] != "max")|
          (.[0]|tonumber)/(.[1]|tonumber)] | if length > 0 then min else error("missing CPU quota") end)} |
      if (.memory_max_bytes > 0 and .memory_max_bytes <= 8589934592 and .swap_max_bytes == 0 and
          .pids_max > 0 and .pids_max <= 128 and .cpu_quota_cores > 0 and .cpu_quota_cores <= 4 and
          (.cpuset|test("^[0-3](-[0-3])?(,[0-3](-[0-3])?)*$")) and
          ($all[0].memory_current|tonumber) <= 8589934592 and
          ($all[0].memory_peak|tonumber) <= 8589934592 and ($all[0].memory_swap_current|tonumber) == 0)
      then . else error("effective resource ceiling") end
    ' "$evidence/resources.$phase.jsonl" > "$evidence/resources.$phase.effective.json"
}
stage=inputs-before
resources before
authenticate_inputs > "$evidence/inputs.before.jsonl"
[[ -x $preparer ]] || die 'preparer executable'
new_path "$output"
stage=native
# Each supervisor records its own original child status; a killed layer leaves a missing receipt.
set +e
# Positional arguments are expanded by the nested supervisor shells.
# shellcheck disable=SC2016
/usr/bin/time -v -o "$evidence/native.time.txt" bash -c '
    set -u -o noclobber
    timeout --signal=TERM --kill-after=10 2400 bash -c '\''
        set -u -o noclobber
        "$1" "$2" "$3" "$4" 2> "$5/native.stderr.txt"
        rc=$?
        printf "%s\n" "$rc" > "$5/native.exit" || exit 98
        exit "$rc"
    '\'' _ "$@"
    rc=$?
    printf "%s\n" "$rc" > "$5/timeout.exit" || exit 98
    exit "$rc"
' _ "$preparer" "$config" "$config_sha" "$output" "$evidence" \
    2> "$evidence/supervisor.stderr.txt" | tee -- "$evidence/native.stdout.json"
pipeline=("${PIPESTATUS[@]}")
set -e
printf '%s\n' "${pipeline[0]}" > "$evidence/time.exit"
printf '%s\n' "${pipeline[1]}" > "$evidence/tee.exit"
stage=resources-after
resources after
stage=original-exits
for name in native timeout time tee; do
    [[ -f $evidence/$name.exit && $(stat -c %s -- "$evidence/$name.exit") == 2 && $(< "$evidence/$name.exit") == 0 ]] || die "missing/nonzero $name exit"
done
awk -F ': ' '/Maximum resident set size \(kbytes\)/ {n++; if ($2 <= 0 || $2 > 8388608) bad=1}
    END {exit (n != 1 || bad)}' "$evidence/native.time.txt"
cmp -- "$evidence/resources.before.effective.json" "$evidence/resources.after.effective.json"
jq -e -n --slurpfile before "$evidence/resources.before.jsonl" --slurpfile after "$evidence/resources.after.jsonl" '
  def events: if . == "absent" then {} else
    split("\n") | map(split(" ")|{key:.[0],value:(.[1]|tonumber)}) | from_entries end;
  ($before|length) == ($after|length) and all(range(0;$before|length);
    . as $i | $before[$i].path == $after[$i].path and
    ($before[$i].memory_events|events|{max,oom,oom_kill,oom_group_kill}) ==
      ($after[$i].memory_events|events|{max,oom,oom_kill,oom_group_kill}) and
    ($before[$i].pids_events|events) == ($after[$i].pids_events|events))
' > "$evidence/resource-closure.txt"
stage=receipt
canonical "$output"
output_identity=$(stat -c '%d:%i:%y:%z' -- "$output")
[[ -f $output/complete.json && ! -L $output/complete.json ]] || die 'missing regular native receipt'
receipt_bytes=$(stat -c %s -- "$output/complete.json")
(( receipt_bytes > 0 && receipt_bytes <= 65536 )) || die 'receipt byte cap'
[[ $(stat -c %s -- "$evidence/native.stdout.json") -le 1024 ]] || die 'native stdout byte cap'
receipt_sha=$(jq -er -s 'if length == 1 and (.[0] | keys == ["receipt_sha256","status"] and
    .status == "COMPLETE" and (.receipt_sha256|type == "string" and test("^[0-9a-f]{64}$")))
    then .[0].receipt_sha256 else error("native stdout seal") end' "$evidence/native.stdout.json")
authenticate "$output/complete.json" "$receipt_bytes" "$receipt_sha" > "$evidence/receipt.authenticated.jsonl"
jq -e -s --slurpfile config "$evidence/config.json" --slurpfile limits "$evidence/resources.before.effective.json" \
    --slurpfile inputs "$evidence/inputs.before.jsonl" --arg config_path "$config" --arg config_sha "$config_sha" \
    --argjson config_bytes "$config_bytes" --arg reserved "$requests_sha" '
  def uint: type == "number" and . >= 0 and floor == .;
  length == 1 and (.[0] | . as $r | $config[0] as $c |
    .schema == "borsuk-cohere-native-cohort-receipt-v3" and .status == "COMPLETE" and
    .dataset == $c.dataset and .revision == $c.revision and
    .columns == {embedding:"emb",document_id:"_id"} and
    .config == {path:$config_path,bytes:$config_bytes,sha256:$config_sha} and
    .output_parent == $c.output_parent and .resources == $c.resources and .reserved_queries_sha256 == $reserved and
    .geometry == {corpus_rows:100000,query_rows:32,dimensions:1024,k:10,
      corpus_intervals:[{start:0,end:100000}],reserved_query_interval:{start:100000,end:101000},
      query_source_ordinals:[100000,100032]} and
    .values == "original publisher Float32 bits, little endian, unnormalized" and
    .metric == "cosine" and .truth_ties == "ascending corpus ordinal" and .readiness == "data identity only" and
    .truth_arithmetic == "sequential f64 dot and squared-norm sums over original f32; 1-dot/(sqrt(cnorm2)*sqrt(qnorm2))" and
    .truth_method == "independent single corpus block-major exhaustive scan with bounded per-query top-k heaps; no ANN inputs" and
    .runtime_limits == ($limits[0] | del(.path,.cpuset,.pids_max) + {enforcement:"cgroup-v2"}) and
    (.observed_peak_rss_at_receipt_bytes|uint and . > 0 and . <= 8589934592) and
    (.elapsed_seconds_at_receipt|type == "number" and . >= 0 and . < 2400) and
    (.modeled_peak_bytes|uint and . >= 533127328 and . <= 7516192768) and
    .resource_accounting.resident_peak_bytes == 533127328 and
    .resource_accounting.source_bytes == 433359090 and .resource_accounting.output_cap_bytes == 1133767168 and
    .resource_accounting.caller_scratch_bytes == 0 and .resource_accounting.temporary_and_failure_reserve_bytes == 67108864 and
    .resource_accounting.failure_outputs_retained_within_output_cap == true and
    (.sources|length) == 2 and all(range(0;2); . as $i | $r.sources[$i] as $s |
      ($s|{path,publisher_path,bytes,sha256,rows}) == $c.shards[$i] and
      ($s.footer_bytes|uint and . > 0 and . <= 1048576) and
      any($inputs[]; .path == $s.path and .device == $s.device and .inode == $s.inode)) and
    ([.outputs[].name]|sort) == ["corpus.f32","corpus.ids.jsonl","queries.f32","queries.ids.jsonl","truth.u64"] and
    all(.outputs[]; keys == ["bytes","name","sha256"] and (.bytes|uint and . > 0 and . <= 723968000) and
      (.sha256|type == "string" and test("^[0-9a-f]{64}$"))) and
    ([.outputs[].bytes]|add) <= 1133767168)
' "$output/complete.json" > "$evidence/receipt.validated.txt"
jq -r '.outputs[] | [.name,.bytes,.sha256] | @tsv' "$output/complete.json" > "$evidence/outputs.tsv"
stage=outputs
while IFS=$'\t' read -r name bytes sha; do
    authenticate "$output/$name" "$bytes" "$sha"
done < "$evidence/outputs.tsv" > "$evidence/outputs.authenticated.jsonl"
shopt -s nullglob dotglob
outputs=("$output/"*)
(( ${#outputs[@]} == 6 )) || die 'unexpected native output files'
stage=parity
authenticate "$output/corpus.f32" 409600000 "$corpus_sha" > "$evidence/corpus.parity.jsonl"
requests_prefix=$(head -c 131072 -- "$requests" | sha256sum)
truth_prefix=$(head -c 2560 -- "$truth" | sha256sum)
requests_prefix=${requests_prefix%% *}
truth_prefix=${truth_prefix%% *}
jq -n --arg requests "$requests" --arg truth "$truth" --arg requests_sha "$requests_prefix" --arg truth_sha "$truth_prefix" \
    '{method:"head -c | sha256sum after whole historical body authentication",
      requests:{path:$requests,prefix_bytes:131072,prefix_sha256:$requests_sha},
      truth:{path:$truth,prefix_bytes:2560,prefix_sha256:$truth_sha}}' > "$evidence/prefixes.json"
authenticate "$output/queries.f32" 131072 "$requests_prefix" > "$evidence/requests.parity.jsonl"
authenticate "$output/truth.u64" 2560 "$truth_prefix" > "$evidence/truth.parity.jsonl"
stage=closure
authenticate_inputs > "$evidence/inputs.after.jsonl"
cmp -- "$evidence/inputs.before.jsonl" "$evidence/inputs.after.jsonl"
canonical "$parent"
canonical "$evidence"
canonical "$output"
[[ $(stat -c '%d:%i' -- "$parent") == "$parent_identity" &&
   $(stat -c '%d:%i' -- "$evidence") == "$evidence_identity" &&
   $(stat -c '%d:%i:%y:%z' -- "$output") == "$output_identity" ]] || die 'namespace closure drift'
authenticate "$output/complete.json" "$receipt_bytes" "$receipt_sha" > "$evidence/receipt.closed.jsonl"
cmp -- "$evidence/receipt.authenticated.jsonl" "$evidence/receipt.closed.jsonl"
sync -f "$output"
verified=1
stage=local-parity-verified
