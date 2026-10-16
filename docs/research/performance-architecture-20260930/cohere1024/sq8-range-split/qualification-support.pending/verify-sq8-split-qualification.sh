#!/usr/bin/env bash
# UNVERIFIED source-only verifier for the frozen a0003 compiler campaign.
# Usage: verify-sq8-split-qualification.sh CLOSED_ATTEMPT_DIR EXPECTED_SOURCE_PACKAGE_DIR EXPECTED_SOURCE_COMMIT
# See /data/orchestration/borsuk-split-v2-verifier-contract.json for the offline layout.
# The root supplies original SSM/download evidence and safely extracted metadata.
# This script never downloads, extracts, builds, or executes a retained ELF.
set -Eeuo pipefail
export LC_ALL=C TZ=UTC
umask 077
work=
cleanup() {
 local rc=$?
 trap - EXIT
 [[ -z $work ]] || rm -rf -- "$work"
 if (( rc != 0 )); then printf 'Compiler qualification verification FAILED (exit %s)\n' "$rc" >&2; fi
 exit "$rc"
}
trap cleanup EXIT
die() { printf '%s\n' "$*" >&2; exit 1; }
[[ $# == 3 ]] || die 'Expected CLOSED_ATTEMPT_DIR EXPECTED_SOURCE_PACKAGE_DIR EXPECTED_SOURCE_COMMIT'
closed=$1 expected=$2 revision=$3
[[ $revision =~ ^[0-9a-f]{40}$ ]] || die 'Invalid expected source commit'
for directory in "$closed" "$expected"; do
 [[ $directory == /* && -d $directory && ! -L $directory && $(realpath -e -- "$directory") == "$directory" ]] || die 'Directories must be canonical absolute paths'
 [[ $directory != *[$'\t\r\n']* ]] || die 'Control character in directory'
done
[[ $closed != "$expected" && $closed != "$expected/"* && $expected != "$closed/"* ]] || die 'Expected originals and attempt must be separate'
[[ ! -e $closed/root-verification.json && ! -L $closed/root-verification.json ]] || die 'Verification output already exists'

regular() {
 local path=$1 cap=$2 size
 [[ -f $path && ! -L $path && $(realpath -e -- "$path") == "$path" ]] || die "Not a canonical regular file: $path"
 size=$(stat -c %s -- "$path")
 (( size <= cap )) || die "File exceeds byte cap: $path"
}
same() { cmp -s -- "$1" "$2" || die "Different bytes: $1 and $2"; }
text() {
 regular "$1" 1048576
 [[ $(stat -c %s -- "$1") == $((${#2}+1)) && $(cat -- "$1") == "$2" ]] || die "Unexpected text: $1"
}
sha() { sha256sum -- "$1" | cut -d ' ' -f 1; }
tree() {
 local base=$1 limit=$2 total_cap=$3 path relative count=0 total=0 device
 device=$(stat -c %d -- "$base")
 # Bound traversal output too; a huge or unreadable tree fails before parsing it.
 find -P "$base" -mindepth 1 -print0 | head -c 2097153 > "$work/tree.paths"
 (( $(stat -c %s "$work/tree.paths") <= 2097152 )) || die 'Too many path bytes'
 while IFS= read -r -d '' path; do
  count=$((count+1)); (( count <= limit )) || die 'Too many artifacts'
  relative=${path#"$base/"}
  [[ $relative =~ ^[A-Za-z0-9_./-]+$ && /$relative/ != *'/../'* && /$relative/ != *'/./'* && $relative != *//* ]] || die "Unsafe artifact path: $path"
  [[ ! -L $path && $(realpath -e -- "$path") == "$path" && $(stat -c %d -- "$path") == "$device" ]] || die "Link, escape or mount: $path"
  if [[ -d $path ]]; then continue; fi
  regular "$path" 16777216
  total=$((total+$(stat -c %s -- "$path")))
  (( total <= total_cap )) || die 'Metadata byte cap exceeded'
 done < "$work/tree.paths"
}
work=$(mktemp -d "$closed/.root-verification.XXXXXXXX")
for directory in extracted binaries; do
 [[ -d $closed/$directory && ! -L $closed/$directory && $(realpath -e -- "$closed/$directory") == "$closed/$directory" ]] || die "Missing directory: $directory"
done
tree "$closed/extracted" 4096 134217728
tree "$expected" 32 33554432
metadata=$closed/extracted
native=$metadata/evidence/sq8-split
manager=$metadata/evidence/sq8-split-manager
setup=$metadata/evidence/setup
remote=/mnt/borsuk-paired-qualification
remote_native=$remote/evidence/sq8-split
remote_manager=$remote/evidence/sq8-split-manager
for directory in "$native" "$manager" "$setup"; do [[ -d $directory ]] || die "Missing metadata: $directory"; done
for file in get-command-invocation.json terminal.json archive.sha256; do regular "$closed/$file" 1048576; done
regular "$closed/qualification-evidence.tar.gz" 33554432
for file in check_cohere_native_baseline compare_native_replay; do regular "$closed/binaries/$file" 67108864; done
find -P "$closed" -mindepth 1 -maxdepth 1 -printf '%f\n' | head -c 4097 > "$work/closed.paths"
(( $(stat -c %s "$work/closed.paths") <= 4096 )) || die 'Attempt directory entry cap'
printf '%s\n' "${work##*/}" archive.sha256 binaries extracted get-command-invocation.json qualification-evidence.tar.gz terminal.json | sort > "$work/closed.expected"
sort "$work/closed.paths" > "$work/closed.actual"
same "$work/closed.expected" "$work/closed.actual"
find -P "$metadata" "$expected" -type f -print0 | sort -z | xargs -0 sha256sum > "$work/originals.sha256"
for file in get-command-invocation.json terminal.json archive.sha256 qualification-evidence.tar.gz binaries/check_cohere_native_baseline binaries/compare_native_replay; do
 sha256sum -- "$closed/$file" >> "$work/originals.sha256"
done

# All jq programs live in this one literal, allowing compilation without evidence.
# shellcheck disable=SC2016
jq_program=$(cat <<'JQ'
def scope($raw):
 ($raw | split("\n")) as $lines
 | if $lines[-1] != "" then error("unterminated roster") else $lines[:-1] end
 | if length == 22 and (unique|length) == 22
      and all(.[]; length <= 512 and test("^[A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)+$"))
   then . else error("invalid 22-test roster") end
 | [.[] | select(test("^(sq8_s3_range::tests::split_[A-Za-z0-9_]+|two_bit_generation::source_walk_tests::generation_sq8_split_admission_and_serving_parity|tests::(cold_sq8_split_config_version_caps_trace_and_identity|actual_sq8_split_config_reaches_serving_and_preserves_parity|sq8_split_multi_query_reservation_and_direct_memory_boundaries))$"))] as $split
 | [.[] | select(startswith("sq8_split_panel::tests::"))] as $reader
 | {total:length,split:($split|length),reader:($reader|length),other:(length-($split|length)-($reader|length)),
    scope:{split:$split,reader:$reader,other:(. - $split - $reader)}};
def manifest($raw):
 ($raw|split("\n")) as $lines
 | if $lines[-1] == "" then $lines[:-1] else error("unterminated manifest") end
 | map(capture("^(?<sha>[0-9a-f]{64})  (?<path>[A-Za-z0-9_./-]+)$")) as $files
 | if ($files|length) == ($lines|length)-1
      and all($files[]; (.path|startswith("/")|not)
        and (.path|split("/")|all(. != "" and . != "." and . != "..")))
      and ($files|map(.path)|unique|length) == ($files|length)
   then $files else error("invalid or duplicate manifest path") end;
def no_duplicate_keys:
 reduce inputs as $event ({seen:{},active:[],events:0};
  .events += 1 | if .events > 20000 or ($event[0]|length) > 16 then error("JSON complexity cap") else . end
  | if ($event|length) == 2 then
     $event[0] as $path
     | (if ($path|length) == 0 then [0] else [range(1;($path|length)+1)] end) as $lengths
     | reduce $lengths[] as $length (.;
        $path[:$length] as $prefix
        | if $length > 0 and $prefix == .active[:$length] then . else
           ($prefix|tojson) as $key
           | if .seen[$key] then error("duplicate JSON key/path") else .seen[$key] = true end end)
     | .active = $path[:-1]
     | if ($path|length) == 0 then .seen = {} | .events = 0 else . end
    else .active = $event[0][:[0,($event[0]|length)-2]|max]
     | if ($event[0]|length) == 1 then .seen = {} | .events = 0 else . end end)
 | true;
$ARGS.named as $a
| if $a.action == "unique" then no_duplicate_keys
elif $a.action == "single" then length == 1
elif $a.action == "identity" then
 keys == ["command_id","instance_id","terminal_prefix"]
 and (.command_id|test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))
 and (.instance_id|test("^i-[0-9a-f]{17}$"))
 and (.terminal_prefix|test("^research/semantic-router/[0-9]{8}/sq8-split-[a-z0-9-]+/terminal/$"))
elif $a.action == "identity-values" then [.command_id,.instance_id,.terminal_prefix]|@tsv
elif $a.action == "pins" then
 .schema == "borsuk-split-input-freeze-v1" and .source_commit == $a.commit
 and .source_files == 480 and .native_files == 447 and .required_exact_tests == 22
 and .groups == {lib:6,bin:3,example:9,repair:4}
 and (.sha256|keys) == ($a.files|split(",")|sort) and (.bytes|keys) == (.sha256|keys)
 and all(.sha256[]; type == "string" and test("^[0-9a-f]{64}$"))
 and all(.bytes[]; type == "number" and floor == . and . > 0 and . <= 16777216)
elif $a.action == "pin" then [.sha256[$a.file],(.bytes[$a.file]|tostring)]|@tsv
elif $a.action == "ssm" then
 .CommandId == $a.command and .InstanceId == $a.instance and .Status == "Success" and .ResponseCode == 0
 and .DocumentName == "AWS-RunShellScript" and .PluginName == "aws:runShellScript"
 and (.StandardOutputContent|type) == "string"
elif $a.action == "ssm-identity" then
 .CommandId == $a.command and .InstanceId == $a.instance
 and .DocumentName == "AWS-RunShellScript" and .PluginName == "aws:runShellScript"
 and (.ResponseCode|type == "number" and floor == . and . >= -1 and . <= 255)
elif $a.action == "ssm-exit" then .ResponseCode
elif $a.action == "ssm-stdout" then .StandardOutputContent
elif $a.action == "equal" then . == $original[0]
elif $a.action == "setup" then
 .schema == "borsuk-scale-closure-native-setup-v1" and .source_commit == $a.commit
 and .setup_exit == 0 and .source_authenticated == true and .native_qualified == false
elif $a.action == "inventory" then
 manifest($manifest) as $files | ($files|map({key:.path,value:.sha})|from_entries) as $by_path
 | type == "object" and length == 447 and ($files|length) == 480
 and all(to_entries[]; (.value|type) == "string" and (.value|test("^[0-9a-f]{64}$")) and $by_path[.key] == .value)
 and all(["crates/borsuk/src/sq8_s3_range.rs","crates/borsuk/src/two_bit_generation.rs",
    "crates/borsuk/src/bin/check_cohere_native_baseline.rs","crates/borsuk/examples/compare_native_replay.rs",
    "crates/borsuk/examples/compare_native_replay/sq8_split_panel.rs"][]; . as $key | $by_path[$key] != null)
 and $by_path["scripts/check_rust_test_build.sh"] != null
elif $a.action == "native-manifest" then to_entries|sort_by(.key)[]|"\(.value)  \(.key)"
elif $a.action == "inventory-counts" then . == {source_files:480,native_source_files:447}
elif $a.action == "roster" then scope($roster)
elif $a.action == "roster-equal" then . == scope($roster)
elif $a.action == "terminal" then
 scope($roster) as $s
 | .schema == "borsuk-sq8-split-qualification-v2" and .source_commit == $a.commit
 and .inner_sha256 == $a.inner and .input_map_sha256 == $a.map and .required_exact_tests_sha256 == $a.roster_sha
 and .required_exact_tests == $s.total and .required_split_tests == $s.split
 and .required_reader_tests == $s.reader and .required_other_tests == $s.other and .required_test_scope == $s.scope
 and $s.split == 9 and $s.reader == 6 and $s.other == 7
 and .profiles == ["debug","release"] and .terminal_prefix == $a.prefix
 and .supervisor_exit == 0 and .manager_exit == 0 and .native_exit == 0
 and .unit_owned == true and .owned_cgroup_drained == true and .forced_cleanup == false
 and .unit_status_verified == true and .resource_envelope_verified == true
 and .native_qualified == true and .binaries_verified == true
 and .full_qualification == false and .scientific_performance_claim == false and .compute_closure_pending == true
 and .builder_removal_mutation_falsifier == "EXTERNAL NOT_RUN"
 and (.evidence_archive_sha256|type == "string" and test("^[0-9a-f]{64}$"))
 and .binary_roster.schema == "borsuk-qualification-binaries-v1"
 and (.binary_roster.binaries|map(.name)) == ["check_cohere_native_baseline","compare_native_replay"]
 and all(.binary_roster.binaries[];
    .key == ($a.prefix+"binaries/"+.name)
    and (.size_bytes|type == "number" and floor == . and . >= 1 and . <= 67108864)
    and (.sha256|type == "string" and test("^[0-9a-f]{64}$"))
    and .upload_exit == 0 and .verified == true
    and .upload_receipt == ("evidence/sq8-split-manager/"+.name+".put.json"))
elif $a.action == "archive-sha" then .evidence_archive_sha256
elif $a.action == "binary-roster" then . == $original[0].binary_roster
elif $a.action == "binary-rows" then .binary_roster.binaries[]|[.name,.key,(.size_bytes|tostring),.sha256]|@tsv
elif $a.action == "put" then type == "object" and (.ETag|type == "string" and length > 0 and length <= 512 and (test("[\u0000-\u001f\u007f]")|not))
elif $a.action == "artifact" then
 [split("\n")[]|fromjson?|select(.reason == "compiler-artifact" and .target.name == $a.target
    and .profile.test == true and (.target.kind|index($a.kind)) != null and (.executable|type) == "string")|.executable]
 | if length == 1 then .[0] else error("expected one compiler test artifact") end
elif $a.action == "output" then
 {schema:"borsuk-root-split-qualification-v1",verdict:"VALID_FOR_COMPILER_CORRECTNESS_ONLY",
 source_commit:$a.commit,command_id:$a.command,instance_id:$a.instance,terminal_prefix:$a.prefix,
 evidence_archive_sha256:$a.archive,input_map_sha256:$a.map,inner_sha256:$a.inner,supervisor_sha256:$a.outer,
 required_exact_tests_sha256:$a.roster_sha,required_exact_tests:22,profiles:["debug","release"],
 native_stages:($a.native_stages|tonumber),manager_stages:($a.manager_stages|tonumber),
 binaries:$original[0].binary_roster.binaries,full_qualification:false,scientific_performance_claim:false,
 builder_removal_mutation_falsifier:"EXTERNAL NOT_RUN",compute_closure_pending:true,
 late_archive_publication_evidence:"Original SSM Success/code0 and identical published terminal; late stages are outside their own archive"}
else error("unknown verifier jq action") end
JQ
)
jq_run() {
 local flags=$1 action=$2; shift 2
 local -a defaults=()
 case $action in
  inventory) defaults=(--arg roster '' --argjson original '[]') ;;
  roster|roster-equal|terminal) defaults=(--arg manifest '' --argjson original '[]') ;;
  equal|binary-roster|output) defaults=(--arg manifest '' --arg roster '') ;;
  *) defaults=(--arg manifest '' --arg roster '' --argjson original '[]') ;;
 esac
 jq "$flags" "${defaults[@]}" --arg action "$action" "$@" "$jq_program"
}
jq_check() { jq_run -e "$@" > /dev/null; }
jq_read() { jq_run -er "$@"; }
json() {
 regular "$1" 1048576
 jq_check unique --stream -n "$1"
 jq_check single -s "$1"
}
for file in input-pin-summary.json root-expectations.json native-source.json; do json "$expected/$file"; done
for file in get-command-invocation.json terminal.json; do json "$closed/$file"; done
while IFS= read -r -d '' file; do json "$file"; done < <(find -P "$metadata" -type f -name '*.json' -print0)
pins=$expected/input-pin-summary.json
identity=$expected/root-expectations.json
terminal=$closed/terminal.json
jq_check identity "$identity"
IFS=$'\t' read -r command instance prefix < <(jq_read identity-values "$identity")
files=(QUALIFICATION_ADMITTED admission-inputs.sha256 native-source.json qualify-sq8-split.sh required-exact-tests.txt source-files.sha256 source.tar.gz supervise-sq8-split.sh)
file_csv=$(IFS=,; printf '%s' "${files[*]}")
jq_check pins --arg commit "$revision" --arg files "$file_csv" "$pins"
declare -A pin_sha=()
for file in "${files[@]}"; do
 regular "$expected/$file" 16777216
 IFS=$'\t' read -r digest bytes < <(jq_read pin --arg file "$file" "$pins")
 [[ $(stat -c %s -- "$expected/$file") == "$bytes" && $(sha "$expected/$file") == "$digest" ]] || die "Expected pin mismatch: $file"
 pin_sha[$file]=$digest
 if [[ $file != source.tar.gz ]]; then same "$expected/$file" "$metadata/$file"; fi
done
# Only this frozen runtime has the stage/command contract checked below.
[[ ${pin_sha[qualify-sq8-split.sh]} == fe29a4c6b1c3ce622724dd82efed0dce02d0ac8039ee45ab897cff5c7cd05721
 && ${pin_sha[supervise-sq8-split.sh]} == efa676b136c1388fa2778e92bfef90344a325f80c4090c445fb26797c70dbc33 ]] || die 'Unrecognized runtime contract'
inner_sha=${pin_sha[qualify-sq8-split.sh]}
outer_sha=${pin_sha[supervise-sq8-split.sh]}
map_sha=${pin_sha[admission-inputs.sha256]}
roster_sha=${pin_sha[required-exact-tests.txt]}
roster=$expected/required-exact-tests.txt
jq_check ssm-identity --arg command "$command" --arg instance "$instance" "$closed/get-command-invocation.json"
ssm_exit=$(jq_read ssm-exit "$closed/get-command-invocation.json")
if (( ssm_exit > 0 )); then printf 'Original SSM command failed: %s\n' "$ssm_exit" >&2; exit "$ssm_exit"; fi
text "$expected/QUALIFICATION_ADMITTED" "$revision"
: > "$work/admission-inputs.sha256"
for file in QUALIFICATION_ADMITTED source.tar.gz source-files.sha256 native-source.json required-exact-tests.txt qualify-sq8-split.sh supervise-sq8-split.sh; do
 printf '%s  %s/%s\n' "${pin_sha[$file]}" "$remote" "$file" >> "$work/admission-inputs.sha256"
done
same "$work/admission-inputs.sha256" "$expected/admission-inputs.sha256"
jq_check inventory --rawfile manifest "$expected/source-files.sha256" "$expected/native-source.json"
jq_read native-manifest "$expected/native-source.json" > "$work/native-source.sha256"
jq_read roster --rawfile roster "$roster" -n > "$work/roster.json"
mapfile -t names < "$roster"
[[ ${names[18]} == paired_partition_coverage::tests::paired_real_d1024_public_build_tail_prepare_independent_reopen
 && ${names[19]} == tests::actual_native_scale_runner_reducer_seal_count_mismatches
 && ${names[20]} == tests::leaf_coverage_actual_orchestration_refuses_terminal_and_sync_before_truth
 && ${names[21]} == tests::leaf_coverage_actual_reducer_rejects_rehashed_nontruth_return_outside_sq8 ]] || die 'Original repair roster changed'
jq_check terminal --arg commit "$revision" --arg inner "$inner_sha" --arg map "$map_sha" --arg roster_sha "$roster_sha" --arg prefix "$prefix" --rawfile roster "$roster" "$terminal"
jq_check ssm --arg command "$command" --arg instance "$instance" "$closed/get-command-invocation.json"
jq_read ssm-stdout "$closed/get-command-invocation.json" > "$work/ssm-stdout.txt"
# The frozen native SSM preflight emits exactly this successful outer SHA check
# before the supervisor prints its terminal object. Authenticate, then remove
# only that one line; the remainder still must be exactly one matching JSON doc.
[[ $(head -n 1 "$work/ssm-stdout.txt") == "$remote/supervise-sq8-split.sh: OK" ]] || die 'Unexpected native preflight stdout'
tail -n +2 "$work/ssm-stdout.txt" > "$work/ssm-stdout.json"
json "$work/ssm-stdout.json"
jq_check equal --slurpfile original "$terminal" "$work/ssm-stdout.json"

archive=$closed/qualification-evidence.tar.gz
regular "$archive" 33554432
archive_sha=$(jq_read archive-sha "$terminal")
text "$closed/archive.sha256" "$archive_sha  $remote/qualification-evidence.tar.gz"
[[ $(sha "$archive") == "$archive_sha" ]] || die 'Original archive digest mismatch'
# Listing and comparison read the archive; neither operation extracts members.
timeout -k 2 30 tar --list --verbose --numeric-owner --quoting-style=literal --gzip --file "$archive" | head -c 1048577 > "$work/archive.list"
(( $(stat -c %s "$work/archive.list") <= 1048576 )) || die 'Archive listing cap'
awk '
 NF != 6 || length($1) != 10 || $1 !~ /^[-d][rwxStTs-]+$/ || $3 !~ /^[0-9]+$/ {exit 1}
 {p=$6; sub(/\/$/,"",p)}
 p !~ /^[A-Za-z0-9_.\/-]+$/ || p ~ /^\// || p ~ /(^|\/)\.\.?($|\/)/ || p ~ /\/\// || seen[p]++ {exit 1}
 p ~ /^evidence\/sq8-split\/binaries\// || p ~ /(^|\/)(check_cohere_native_baseline|compare_native_replay)$/ {exit 1}
 {if (++count > 4096 || $3 > 16777216 || (total += $3) > 134217728) exit 1; print p}
 END {if (!count) exit 1}
' "$work/archive.list" | sort > "$work/archive.paths"
find -P "$metadata" -mindepth 1 -printf '%P\n' | sort > "$work/extracted.paths"
same "$work/archive.paths" "$work/extracted.paths"
compare_rc=0
timeout -k 2 30 tar --compare --gzip --file "$archive" --directory "$metadata" > "$work/archive.compare" 2> "$work/archive.compare.stderr" || compare_rc=$?
(( compare_rc <= 1 )) && [[ ! -s $work/archive.compare.stderr ]] || die 'Archive comparison failed'
if (( compare_rc == 1 )); then [[ -s $work/archive.compare ]] || die 'Archive comparison failed without differences'; fi
# Safe extraction may change ownership, permissions or timestamps, never bytes.
if grep -Ev '^[A-Za-z0-9_./-]+: (Uid|Gid|Mode|Mod time) differs$' "$work/archive.compare" > /dev/null; then die 'Extracted metadata differs from archive contents'; fi
[[ ! -e $native/binaries/check_cohere_native_baseline && ! -e $native/binaries/compare_native_replay ]] || die 'Binary payload in metadata'

for file in admission-inputs.sha256 source-files.sha256 native-source.json required-exact-tests.txt; do same "$expected/$file" "$native/$file"; done
for file in admission-inputs.sha256 source-files.sha256 native-source.json required-exact-tests.txt qualify-sq8-split.sh supervise-sq8-split.sh; do same "$expected/$file" "$setup/$file"; done
same "$expected/source-files.sha256" "$setup/base-source-files.sha256"
same "$expected/native-source.json" "$setup/base-native-source.json"
same "$work/native-source.sha256" "$setup/native-source.sha256"
jq_check setup --arg commit "$revision" "$setup/terminal.json"
json "$native/source-inventory.log"
json "$manager/required-roster-validate.log"
jq_check inventory-counts "$native/source-inventory.log"
jq_check roster-equal --rawfile roster "$roster" "$manager/required-roster-validate.log"
same "$roster" "$manager/required-roster-read.log"
text "$manager/required-roster-sha.log" "$roster_sha  $remote_manager/required-roster-read.log"
text "$native/source-commit.txt" "$revision"
text "$native/input-map.sha256" "$map_sha  $remote/admission-inputs.sha256"
same "$native/input-map.sha256" "$manager/input-map.sha256"
text "$manager/inner.sha256" "$inner_sha  $remote/qualify-sq8-split.sh"
text "$manager/supervisor-sha.log" "$outer_sha  $remote/supervise-sq8-split.sh"
for file in qualify-sq8-split.sh supervise-sq8-split.sh required-exact-tests.txt; do printf '%s  %s\n' "${pin_sha[$file]}" "$file"; done > "$work/runtime-files.sha256"
same "$work/runtime-files.sha256" "$setup/runtime-files.sha256"

ok_manifest() { sed -E 's/^[0-9a-f]{64}  (.*)$/\1: OK/' "$1"; }
ok_manifest "$expected/source-files.sha256" > "$work/source-only.ok"
same "$work/source-only.ok" "$setup/source-setup.log"
same "$work/source-only.ok" "$setup/source-overlay.log"
ok_manifest "$work/native-source.sha256" > "$work/native-only.ok"
same "$work/native-only.ok" "$setup/native-source-overlay.log"
cat "$work/source-only.ok" > "$work/source-all.ok"
ok_manifest "$expected/admission-inputs.sha256" >> "$work/source-all.ok"
ok_manifest "$native/input-map.sha256" >> "$work/source-all.ok"
same "$work/source-all.ok" "$native/source-before.log"
same "$work/source-all.ok" "$native/source-after.log"
regular "$native/source-comparison.log" 1048576
[[ ! -s $native/source-comparison.log ]] || die 'Source comparison reported differences'
text "$native/source-after-exit.txt" 0
grep -Eq '^\s*Exit status: 0$' "$native/source-after-time.txt"
text "$native/native-original-exit.txt" 0
text "$native/native-exit.txt" 0
text "$native/builder-removal-falsifier.txt" 'EXTERNAL NOT_RUN: root builder-removal mutation falsifier; not executed by this driver.'
[[ ! -e $native/failed-stage.txt ]] || die 'Failed native stage recorded'

recorded_command() {
 local file=$1; shift
 printf '%q ' "$@" > "$work/command.txt"
 printf '\n' >> "$work/command.txt"
 same "$work/command.txt" "$file"
}
recorded_command "$native/source-after-command.txt" timeout -k 5 120 sha256sum --strict --check "$remote_native/source-files.sha256" "$remote_native/admission-inputs.sha256" "$remote_native/input-map.sha256"
declare -A native_stages=() manager_stages=()
: > "$work/stages.tsv"
stage() {
 local directory=$1 name=$2 cap=$3 grace=${4:-} suffix
 if [[ $directory == "$native" ]]; then
  [[ ! ${native_stages[$name]+yes} ]] || die 'Duplicate expected native stage'
  native_stages[$name]=1
  printf '%s\t%s\t%s\n' "$name" "$cap" "$grace" >> "$work/stages.tsv"
 else
  [[ ! ${manager_stages[$name]+yes} ]] || die 'Duplicate expected manager stage'
  manager_stages[$name]=1
 fi
 for suffix in command.txt started.txt finished.txt timeout-seconds.txt time.txt exit.txt; do regular "$directory/$name-$suffix" 16777216; done
 regular "$directory/$name.log" 16777216
 text "$directory/$name-exit.txt" 0
 grep -Eq '^\s*Exit status: 0$' "$directory/$name-time.txt" || die "Missing successful timing: $name"
 grep -Eq '^[1-9][0-9]{0,4}$' "$directory/$name-timeout-seconds.txt" || die "Invalid stage timeout: $name"
 (( $(cat "$directory/$name-timeout-seconds.txt") <= cap )) || die "Stage exceeds cap: $name"
}
run_native() { local name=$1 cap=$2 grace=$3; shift 3; stage "$native" "$name" "$cap" "$grace"; recorded_command "$native/$name-command.txt" "$@"; }
run_manager() { local name=$1 cap=$2; shift 2; stage "$manager" "$name" "$cap"; recorded_command "$manager/$name-command.txt" "$@"; }
run_native input-map-sha 10 5 sha256sum --strict --check "$remote_native/input-map.sha256"
ok_manifest "$native/input-map.sha256" > "$work/input-map.ok"
same "$work/input-map.ok" "$native/input-map-sha.log"
run_native admission-input-sha 120 5 sha256sum --strict --check "$remote/admission-inputs.sha256"
ok_manifest "$expected/admission-inputs.sha256" > "$work/admission.ok"
same "$work/admission.ok" "$native/admission-input-sha.log"
stage "$native" source-inventory 10 5
run_native source-before 120 5 sha256sum --strict --check "$remote_native/source-files.sha256" "$remote_native/admission-inputs.sha256" "$remote_native/input-map.sha256"
run_native rustc-version 10 5 rustc -vV
run_native rustc-cfg 10 5 rustc --print cfg
grep -Fxq 'panic="unwind"' "$native/rustc-cfg.log"
run_native cargo-version 10 5 cargo -vV

test_list() {
 local file=$1 count
 regular "$file" 16777216
 awk '/: test$/ {if ($0 !~ /^[A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*: test$/ || seen[$0]++) exit 1; n++}
  /: benchmark$/ {exit 1}
  /^[0-9]+ tests?, [0-9]+ benchmarks?$/ {if (++summaries > 1 || $1 != n || $3 != 0) exit 1}
  END {if (!n || summaries != 1) exit 1}' "$file" || die "Invalid declared/listed test counts: $file"
 count=$(grep -c ': test$' "$file")
 printf '%s\n' "$count"
}
test_result() {
 local file=$1 count=$2 filtered=${3:-'[0-9]+'} summary
 summary=$(awk '/^test result:/ {line=$0} END {print line}' "$file")
 [[ $summary =~ ^test\ result:\ ok\.\ $count\ passed\;\ 0\ failed\;\ 0\ ignored\;\ 0\ measured\;\ $filtered\ filtered\ out\;\ finished\ in\ [0-9.]+s$ ]] || die "Selected/passed/ignored mismatch: $file"
}
resolve_binary() {
 local label=$1 profile=$2 target=$3 kind=$4 directory=deps executable
 [[ $kind != example ]] || directory=examples
 stage "$native" "$label-artifact" 10 5
 json "$native/$label-artifact.log"
 awk 'length($0)>1048576 {exit 1} /^[[:space:]]*\{/ {if (++n>10000) exit 1; print}' "$native/$label-compile.log" > "$work/compiler-json.log"
 jq_check unique --stream -n "$work/compiler-json.log"
 executable=$(jq_read artifact -Rs --arg target "$target" --arg kind "$kind" "$native/$label-compile.log")
 [[ $executable =~ ^/mnt/borsuk-paired-qualification/source/target/$profile/$directory/[A-Za-z0-9_-]+$ ]] || die 'Unexpected test executable path'
 text "$native/$label-executable.txt" "$executable"
 # Artifact selection is independently derived from the original compile log.
 [[ $(jq -er . "$native/$label-artifact.log") == "$executable" ]] || die 'Artifact selection mismatch'
 run_native "$label-elf" 10 5 readelf -h "$executable"
 grep -Eq '^\s*Magic:.*7f 45 4c 46' "$native/$label-elf.log"
 run_native "$label-sha" 10 5 sha256sum "$executable"
 local hash_line hash
 hash_line=$(cat "$native/$label-sha.log"); hash=${hash_line%% *}
 [[ $hash =~ ^[0-9a-f]{64}$ ]] || die 'Invalid test ELF SHA'
 text "$native/$label-sha.log" "$hash  $executable"
 cat "$native/$label-sha.log" >> "$work/binaries.sha256"
 binary=$executable
}
binary_check() {
 local label=$1 manifest=$2 log=$3
 run_native "$label" 10 5 sha256sum --strict --check "$manifest"
 ok_manifest "$log" > "$work/binary-check.ok"
 same "$work/binary-check.ok" "$native/$label.log"
}
exact_test() {
 local label=$1 name=$2 total=${3:-} selected
 run_native "$label-selected" 10 5 "$binary" --list --exact "$name"
 selected=$(test_list "$native/$label-selected.log")
 [[ $selected == 1 ]] || die 'Exact selector did not select one required test'
 grep -Fxq "$name: test" "$native/$label-selected.log"
 run_native "$label" 300 5 "$binary" --test-threads=1 --exact "$name" --nocapture
 if [[ -n $total ]]; then test_result "$native/$label.log" 1 "$((total-1))"; else test_result "$native/$label.log" 1; fi
}
regression() {
 local label=$1 inventory=$2 filter=${3:-} cap=600 selected total
 [[ $label != *-example-full ]] || cap=1800
 run_native "$label-selected" 10 5 "$binary" --list "$filter"
 selected=$(test_list "$native/$label-selected.log")
 total=$(test_list "$inventory")
 awk -v prefix="$filter" '/: test$/ && index($0,prefix)==1 {print}' "$inventory" | sort > "$work/expected-selection"
 grep ': test$' "$native/$label-selected.log" | sort > "$work/actual-selection"
 same "$work/expected-selection" "$work/actual-selection"
 run_native "$label" "$cap" 5 "$binary" --test-threads=1 "$filter" --nocapture
 test_result "$native/$label.log" "$selected" "$((total-selected))"
}
: > "$work/binaries.sha256"
run_native repair-example-compile 4200 30 cargo test --locked -p borsuk --example compare_native_replay --no-run --message-format=json
resolve_binary repair-example debug compare_native_replay example
for n in 1 2 3 4; do exact_test "repair-example-exact-$n" "${names[n+17]}"; done
binary_check repair-example-sha-after "$remote_native/repair-example-sha.log" "$native/repair-example-sha.log"
for profile in debug release; do
 flags=(); [[ $profile != release ]] || flags=(--release)
 for kind in lib bin example; do
  case $kind in
   lib) target=borsuk; selector=(--lib) ;;
   bin) target=check_cohere_native_baseline; selector=(--bin "$target") ;;
   example) target=compare_native_replay; selector=(--example "$target") ;;
  esac
  label=$profile-$kind
  run_native "$label-compile" 4200 30 cargo test --locked -p borsuk "${selector[@]}" "${flags[@]}" --no-run --message-format=json
  resolve_binary "$label" "$profile" "$target" "$kind"
 done
 for kind in lib bin example; do
  label=$profile-$kind; binary=$(cat "$native/$label-executable.txt")
  binary_check "$label-sha-before" "$remote_native/$label-sha.log" "$native/$label-sha.log"
  run_native "$label-inventory" 10 5 "$binary" --list
  test_list "$native/$label-inventory.log" > /dev/null
 done
 for kind in lib bin example; do
  label=$profile-$kind; binary=$(cat "$native/$label-executable.txt")
  total=$(test_list "$native/$label-inventory.log")
  case $kind in lib) offset=0; count=6 ;; bin) offset=6; count=3 ;; example) offset=9; count=13 ;; esac
  for ((n=1;n<=count;n++)); do
   name=${names[offset+n-1]}
   [[ $(grep -Fxc "$name: test" "$native/$label-inventory.log") == 1 ]] || die 'Required test absent from inventory'
   exact_test "$label-exact-$n" "$name" "$total"
  done
  binary_check "$label-sha-exact-after" "$remote_native/$label-sha.log" "$native/$label-sha.log"
 done
 binary=$(cat "$native/$profile-lib-executable.txt")
 regression "$profile-sq8-s3-range" "$native/$profile-lib-inventory.log" 'sq8_s3_range::tests::'
 regression "$profile-source-walk" "$native/$profile-lib-inventory.log" 'two_bit_generation::source_walk_tests::'
 binary_check "$profile-lib-sha-after" "$remote_native/$profile-lib-sha.log" "$native/$profile-lib-sha.log"
 for kind in bin example; do
  binary=$(cat "$native/$profile-$kind-executable.txt")
  if [[ $kind == example ]]; then grep -q '^source_cover::.*: test$' "$native/$profile-$kind-inventory.log"; fi
  regression "$profile-$kind-full" "$native/$profile-$kind-inventory.log"
  binary_check "$profile-$kind-sha-after" "$remote_native/$profile-$kind-sha.log" "$native/$profile-$kind-sha.log"
 done
done
run_native release-production-build 4200 30 cargo build --locked -p borsuk --release --bin check_cohere_native_baseline --example compare_native_replay
for name in check_cohere_native_baseline compare_native_replay; do
 artifact=target/release/$name
 [[ $name != compare_native_replay ]] || artifact=target/release/examples/$name
 run_native "production-$name-elf" 10 5 readelf -h "$artifact"
 grep -Eq '^\s*Magic:.*7f 45 4c 46' "$native/production-$name-elf.log"
 run_native "production-$name-sha" 10 5 sha256sum "$artifact"
 cat "$native/production-$name-sha.log" >> "$work/binaries.sha256"
 run_native "production-$name-retain" 30 5 cp -- "$artifact" "$remote_native/binaries/$name"
 run_native "production-$name-retained-parity" 10 5 cmp -- "$artifact" "$remote_native/binaries/$name"
 [[ ! -s $native/production-$name-retain.log && ! -s $native/production-$name-retained-parity.log ]] || die 'Production retain/parity log is not empty'
done
run_native workspace-clippy 4200 30 cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_native workspace-test-build 4200 30 env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=2 CARGO_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh
same "$work/binaries.sha256" "$native/binaries.sha256"
binary_check binaries-final-sha "$remote_native/binaries.sha256" "$native/binaries.sha256"
same "$work/stages.tsv" "$native/stages.tsv"

for file in supervisor-original-exit.txt manager-exit.txt native-exit.txt show-exit.txt; do text "$manager/$file" 0; done
text "$manager/owned.txt" true
text "$manager/drained.txt" true
text "$manager/forced-cleanup.txt" false
text "$manager/final-stage.txt" systemd-run
owner=$(cat "$manager/owner.txt")
[[ $owner =~ ^borsuk-sq8-split-qualification.service:$revision:$inner_sha:[0-9a-f-]{36}$ ]] || die 'Invalid unit owner'
text "$native/cgroup-path.txt" /system.slice/borsuk-sq8-split-qualification.service
for property in "Description=$owner" 'ControlGroup=/system.slice/borsuk-sq8-split-qualification.service' Result=success ExecMainCode=1 ExecMainStatus=0; do
 [[ $(grep -Fxc "$property" "$manager/unit-stop-status.txt") == 1 ]] || die 'Unverified successful unit status'
done
invocation=$(sed -n 's/^InvocationID=//p' "$manager/unit-stop-status.txt")
[[ $invocation =~ ^[0-9a-f]{32}$ ]] || die 'Invalid invocation ID'
if ! grep -Fxq LoadState=not-found "$manager/systemd-after.log"; then
 grep -Fxq "InvocationID=$invocation" "$manager/systemd-after.log"
 grep -Fxq "Description=$owner" "$manager/systemd-after.log"
fi
for phase in before after; do
 if [[ $(cat "$manager/cgroup.events.$phase") != absent ]]; then grep -Fxq 'populated 0' "$manager/cgroup.events.$phase"; fi
done
for property in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do same "$native/$property.before" "$native/$property.after"; done
text "$native/cpu.max.before" '1600000 100000'
text "$native/cpuset.cpus.effective.before" 0-15
text "$native/memory.max.before" 60129542144
text "$native/memory.swap.max.before" 0
text "$native/pids.max.before" 512
grep -Fxq 'oom 0' "$native/memory.events.after"
grep -Fxq 'oom_kill 0' "$native/memory.events.after"
text "$native/memory.swap.peak.after" 0
regular "$manager/resource-check.log" 1048576
[[ ! -s $manager/resource-check.log ]] || die 'Resource comparison differences'
run_manager input-map-sha 10 sha256sum --strict --check "$remote_manager/input-map.sha256"
same "$work/input-map.ok" "$manager/input-map-sha.log"
run_manager admission-input-sha 120 sha256sum --strict --check "$remote/admission-inputs.sha256"
same "$work/admission.ok" "$manager/admission-input-sha.log"
run_manager required-roster-read 10 head -c 65537 -- "$remote/required-exact-tests.txt"
run_manager required-roster-sha 10 sha256sum "$remote_manager/required-roster-read.log"
stage "$manager" required-roster-validate 10
run_manager inner-sha 10 sha256sum --strict --check "$remote_manager/inner.sha256"
ok_manifest "$manager/inner.sha256" > "$work/inner.ok"
same "$work/inner.ok" "$manager/inner-sha.log"
run_manager supervisor-sha 10 sha256sum "$remote/supervise-sq8-split.sh"
run_manager load-before 10 systemctl show borsuk-sq8-split-qualification.service -p LoadState --value
text "$manager/load-before.log" not-found
runtime_limit=$(sed -n 's/.* -p RuntimeMaxSec=\([1-9][0-9]*\) .*/\1/p' "$manager/systemd-run-command.txt")
[[ $runtime_limit =~ ^[1-9][0-9]{1,3}$ ]] || die 'Invalid unit runtime'
(( runtime_limit > 30 && runtime_limit <= 4230 )) || die 'Unit runtime exceeds envelope'
stage "$manager" systemd-run "$((runtime_limit+15))"
stop_hook="/bin/sh -c 'exec /usr/bin/systemctl show borsuk-sq8-split-qualification.service -p Description -p InvocationID -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > $remote_manager/unit-stop-status.txt'"
native_deadline=$(cat "$manager/native-deadline.epoch")
[[ $native_deadline =~ ^[1-9][0-9]{9}$ ]] || die 'Invalid native deadline'
text "$native/native-deadline.epoch" "$native_deadline"
recorded_command "$manager/systemd-run-command.txt" systemd-run --expand-environment=no --unit=borsuk-sq8-split-qualification.service --wait --pipe \
 -p Description="$owner" -p WorkingDirectory="$remote/source" -p CPUQuota=1600% -p CPUQuotaPeriodSec=100ms \
 -p AllowedCPUs=0-15 -p MemoryMax=60129542144 -p MemorySwapMax=0 -p TasksMax=512 \
 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec="$runtime_limit" -p ExecStopPost="$stop_hook" \
 --setenv=RUSTUP_HOME="$remote/.rustup" --setenv=CARGO_HOME="$remote/.cargo" \
 --setenv=PATH="$remote/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
 /bin/bash "$remote/qualify-sq8-split.sh" "$remote" "$revision" "$native_deadline" "$map_sha"
run_manager systemd-after 10 systemctl show borsuk-sq8-split-qualification.service -p LoadState -p Description -p InvocationID -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup
run_manager admission-input-sha-after 120 sha256sum --strict --check "$remote_manager/input-map.sha256" "$remote/admission-inputs.sha256" "$remote_manager/inner.sha256" "$remote_manager/supervisor-sha.log"
cat "$work/input-map.ok" "$work/admission.ok" "$work/inner.ok" > "$work/manager-after.ok"
ok_manifest "$manager/supervisor-sha.log" >> "$work/manager-after.ok"
same "$work/manager-after.ok" "$manager/admission-input-sha-after.log"
jq_check binary-roster --slurpfile original "$terminal" "$manager/binary-roster.json"
while IFS=$'\t' read -r name key size digest; do
 downloaded=$closed/binaries/$name
 regular "$downloaded" 67108864
 [[ $(stat -c %s -- "$downloaded") == "$size" && $(sha "$downloaded") == "$digest" ]] || die 'Downloaded binary size/SHA mismatch'
 # readelf inspects headers only; the downloaded file is never invoked.
 timeout -k 2 10 readelf --file-header -- "$downloaded" > "$work/$name.elf"
 grep -Eq '^\s*Magic:.*7f 45 4c 46' "$work/$name.elf"
 grep -Eq '^\s*Class:\s+ELF64$' "$work/$name.elf"
 grep -Eq '^\s*Machine:\s+Advanced Micro Devices X86-64$' "$work/$name.elf"
 grep -Eq '^\s*Type:\s+(EXEC|DYN) ' "$work/$name.elf"
 artifact=target/release/$name
 [[ $name != compare_native_replay ]] || artifact=target/release/examples/$name
 text "$native/production-$name-sha.log" "$digest  $artifact"
 run_manager "binary-$name-elf" 10 readelf -h "$remote_native/binaries/$name"
 same "$native/production-$name-elf.log" "$manager/binary-$name-elf.log"
 run_manager "binary-$name-sha" 10 sha256sum "$remote_native/binaries/$name"
 text "$manager/binary-$name-sha.log" "$digest  $remote_native/binaries/$name"
 run_manager "binary-$name-sha-after" 10 sha256sum --strict --check "$remote_manager/binary-$name-sha.log"
 text "$manager/binary-$name-sha-after.log" "$remote_native/binaries/$name: OK"
 text "$manager/$name.put-exit.txt" 0
 json "$manager/$name.put.json"
 jq_check put "$manager/$name.put.json"
 grep -Eq '^\s*Exit status: 0$' "$manager/$name.put-time.txt"
 # The only variable command token is the bounded timeout.
 publication_timeout=$(sed -n 's/^timeout -k 2 \([1-9][0-9]*\) aws .*/\1/p' "$manager/$name.put-command.txt")
 [[ $publication_timeout =~ ^[1-9][0-9]?$ ]] || die 'Invalid binary publication timeout'
 (( publication_timeout <= 90 )) || die 'Invalid binary publication timeout'
 recorded_command "$manager/$name.put-command.txt" timeout -k 2 "$publication_timeout" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --body "$remote_native/binaries/$name" --if-none-match '*'
done < <(jq_read binary-rows "$terminal")
[[ $(find -P "$closed/binaries" -mindepth 1 -maxdepth 1 -printf '%f\n' | sort) == $'check_cohere_native_baseline\ncompare_native_replay' ]] || die 'Unexpected downloaded binary artifact'
# Every emitted exit record, including unanticipated ones, must retain exit0.
while IFS= read -r -d '' file; do text "$file" 0; done < <(find -P "$native" "$manager" -type f -name '*-exit.txt' -print0)
while IFS= read -r -d '' file; do
 label=${file##*/}; label=${label%-command.txt}
 [[ ${native_stages[$label]+yes} || $label == source-after ]] || die "Unrecognized native stage: $label"
done < <(find -P "$native" -type f -name '*-command.txt' -print0)
while IFS= read -r -d '' file; do
 label=${file##*/}; label=${label%-command.txt}
 [[ ${manager_stages[$label]+yes} || $label == archive || $label == check_cohere_native_baseline.put || $label == compare_native_replay.put ]] || die "Unrecognized manager stage: $label"
done < <(find -P "$manager" -type f -name '*-command.txt' -print0)
# Archive/sha/publication happen after their snapshot. The original SSM outcome
# above proves the frozen supervisor reached its checked terminal-last return.
recorded_command "$manager/archive-command.txt" tar -czf "$remote/qualification-evidence.tar.gz" --anchored --no-wildcards --exclude=evidence/sq8-split/binaries/check_cohere_native_baseline --exclude=evidence/sq8-split/binaries/compare_native_replay -C "$remote" evidence QUALIFICATION_ADMITTED admission-inputs.sha256 source-files.sha256 native-source.json required-exact-tests.txt qualify-sq8-split.sh supervise-sq8-split.sh
sha256sum --strict --check "$work/originals.sha256" > "$work/originals-unchanged.log"
jq_read output -n --arg commit "$revision" --arg command "$command" --arg instance "$instance" --arg prefix "$prefix" --arg archive "$archive_sha" --arg map "$map_sha" --arg inner "$inner_sha" --arg outer "$outer_sha" --arg roster_sha "$roster_sha" --arg native_stages "${#native_stages[@]}" --arg manager_stages "${#manager_stages[@]}" --slurpfile original "$terminal" > "$work/result.json"
# Atomic, create-only publication on the same filesystem; no partial marker.
ln -- "$work/result.json" "$closed/root-verification.json"
