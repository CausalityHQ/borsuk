#!/bin/bash
# UNVERIFIED: source/static checks only. Root runs this after collection and closure.
# API: bash verify-collected-gc-session.sh /absolute/launch-directory [EXPECTED_SOURCE_COMMIT]
# Reads compiler/test metadata only; never runs collected scripts or executables.
# jq programs and source/command patterns intentionally contain literal shell syntax.
# shellcheck disable=SC2016
set -euo pipefail
shopt -s inherit_errexit
export LC_ALL=C
fail() { printf 'GC verification failed: %s\n' "$*" >&2; exit 1; }
trap 'printf "GC verification failed at line %s\n" "$LINENO" >&2' ERR
[[ $# == 1 || $# == 2 ]] || fail 'expected launch directory and optional root-frozen source revision'
out=$1
[[ $out == /* && $out != / && -d $out && ! -L $out ]] || fail 'unsafe launch directory'
[[ $(realpath -e -- "$out") == "$out" ]] || fail 'launch directory must be canonical and nonsymlink'
# An override is explicit root authority, never a revision inferred from evidence.
source_commit=${2:-103f49c7f6384b8acd27271134a8cc86c7877247}
[[ $source_commit =~ ^[0-9a-f]{40}$ ]] || fail 'expected source revision'
driver_commit=371754c05ae2173fa55330cb3505436080d358f3
inner_sha=31e528998da425b24e8631e552a62a314c1f33e5617476d289e103bfa260ceeb
outer_sha=c37b35867891659227d61f53d43d90fa16cb7994981dbf828fc074aba0308f3c
base=$out/collected
inputs=$out/inputs
ev=$base/evidence/gc-session
manager=$base/evidence/gc-session-manager
remote=/mnt/borsuk-paired-qualification
remote_ev=$remote/evidence/gc-session
remote_manager=$remote/evidence/gc-session-manager
unit=borsuk-gc-session-qualification.service
smoke=native_s3_conditional_fence_and_gc_smoke
archive_root=''
cleanup() { if [[ -n $archive_root ]]; then rm -rf -- "$archive_root"; fi; }
trap cleanup EXIT

file() {
 local archived
 [[ -f $1 && ! -L $1 ]] || fail "missing/nonregular/symlink: $1"
 [[ $(realpath -e -- "$1") == "$1" ]] || fail "symlink ancestor: $1"
 if [[ -n $archive_root && $1 == "$base/"* ]]; then
  archived=$archive_root/${1#"$base/"}
  [[ -f $archived && ! -L $archived && $(realpath -e -- "$archived") == "$archived" ]] || fail "not archived regular metadata: $1"
  cmp -s "$1" "$archived" || fail "collected/archive bytes differ: $1"
 fi
}
text_is() { file "$1"; printf '%s\n' "$2" | cmp -s - "$1" || fail "unexpected text: $1"; }
equal_files() { file "$1"; file "$2"; cmp -s "$1" "$2" || fail "different files: $1 $2"; }
digest() { local value; file "$1"; value=$(sha256sum -- "$1"); printf '%s' "${value%% *}"; }
json_is() { local path=$1; shift; file "$path"; jq -e "$@" "$path" > /dev/null; }
property_is() {
 file "$1"
 awk -v key="$2" -v value="$3" '
  index($0,key "=")==1 {n++; if ($0!=key "=" value) bad=1}
  END {exit (n!=1 || bad)}' "$1" || fail "property $2: $1"
}
epoch() {
 local value result
 file "$1"
 value=$(cat "$1")
 [[ $value =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] || fail "timestamp: $1"
 text_is "$1" "$value"
 result=$(date -u -d "$value" +%s)
 [[ $(date -u -d "@$result" +%FT%TZ) == "$value" ]] || fail "invalid date: $1"
 printf '%s' "$result"
}
integer() {
 local value
 file "$1"; value=$(cat "$1")
 [[ $value =~ ^(0|[1-9][0-9]{0,10})$ ]] || fail "integer: $1"
 text_is "$1" "$value"
 printf '%s' "$value"
}
command_is() { local path=$1 expected; shift; expected=$(printf '%q ' "$@"); text_is "$path" "$expected"; }

# The original SSM invocation and root cleanup must already be terminal.
for name in instance.id volume.id native-command.id; do file "$out/$name"; done
instance=$(cat "$out/instance.id")
volume=$(cat "$out/volume.id")
command=$(cat "$out/native-command.id")
[[ $instance =~ ^i-[0-9a-f]{17}$ && $volume =~ ^vol-[0-9a-f]{17}$ ]] || fail 'instance/volume identity'
[[ $command =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] || fail 'command identity'
text_is "$out/instance.id" "$instance"
text_is "$out/volume.id" "$volume"
text_is "$out/native-command.id" "$command"
json_is "$out/run-terminal-observation.json" -s --arg i "$instance" --arg c "$command" '
 length==1 and (.[0] | .InstanceId==$i and .CommandId==$c and .Status=="Success" and .ResponseCode==0)'
text_is "$out/compute.closed" INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE
json_is "$out/instance-final.json" -s --arg i "$instance" '
 length==1 and (.[0].Reservations | length==1 and
 (.[0].Instances | length==1 and .[0].InstanceId==$i and .[0].State.Name=="terminated"))'
text_is "$out/volume-final.exit" 254
file "$out/volume-final.stderr"
awk -v expected="An error occurred (InvalidVolume.NotFound) when calling the DescribeVolumes operation: The volume '$volume' does not exist." '
 NF {
  n++; line=$0
  sub(/^aws: \[ERROR\]: /,"",line)
  sub(/ when calling the DescribeVolumes operation \(reached max retries: 0\): /," when calling the DescribeVolumes operation: ",line)
  if(line!=expected) bad=1
 } END {exit(n!=1 || bad)}' "$out/volume-final.stderr" || fail 'exact volume absence receipt'
property_is "$out/external-timer-final.txt" ActiveState inactive

# These pins bind the independently expanded roster below to the exact driver source.
for name in qualify-gc-session.sh supervise-gc-session.sh source-files.sha256 native-source.json required-exact-tests.txt; do
 equal_files "$base/$name" "$inputs/$name"
done
[[ $(digest "$inputs/qualify-gc-session.sh") == "$inner_sha" ]] || fail 'inner driver pin'
[[ $(digest "$inputs/supervise-gc-session.sh") == "$outer_sha" ]] || fail 'supervisor pin'
json_is "$out/terminal.json" -s --arg s "$source_commit" --arg h "$inner_sha" --arg smoke "$smoke" '
 length==1 and (.[0] | .schema=="borsuk-gc-session-qualification-v1" and
 .source_commit==$s and .inner_sha256==$h and .source_files==478 and .native_source_files==445 and
 .expected_gc_inventory==24 and .supervisor_exit==0 and .manager_exit==0 and .native_exit==0 and
 .unit_owned==true and .owned_cgroup_drained==true and .forced_cleanup==false and
 .unit_status_verified==true and .resource_envelope_verified==true and .native_qualified==true and
 .ignored_s3_smoke==$smoke and .real_s3_gc_qualified==false and .production_benchmark_binary==false and
 .scientific_performance_claim==false and .compute_closure_pending==true and
 (.terminal_prefix | test("^research/semantic-router/[0-9]{8}/gc-session-[a-z0-9-]+/terminal/$")) and
 (.evidence_archive_sha256 | test("^[0-9a-f]{64}$")))'
# Terminal is published after the archive; root retains it directly in the launch directory.
archive_sha=$(digest "$out/qualification-evidence.tar.gz")
json_is "$out/terminal.json" --arg sha "$archive_sha" '.evidence_archive_sha256==$sha'
members=$(tar --list --absolute-names --gzip --file "$out/qualification-evidence.tar.gz" --quoting-style=literal)
printf '%s\n' "$members" | awk '
 {
  path=$0; sub(/\/$/,"",path)
  if(path !~ /^[A-Za-z0-9_.@+\/-]+$/ || seen[path]++) bad=1
  n=split(path,part,"/"); for(i=1;i<=n;i++) if(part[i]=="" || part[i]=="." || part[i]=="..") bad=1
  if(path!="evidence" && path!~/^evidence\// && path!~/^(source-files[.]sha256|native-source[.]json|required-exact-tests[.]txt|qualify-gc-session[.]sh|supervise-gc-session[.]sh)$/) bad=1
  count++
 }
 END {exit(bad || count==0)}' || fail 'unsafe/duplicate archive member'
types=$(tar --list --absolute-names --verbose --gzip --file "$out/qualification-evidence.tar.gz" --numeric-owner --quoting-style=literal)
printf '%s\n' "$types" | awk 'substr($0,1,1)!="-" && substr($0,1,1)!="d" {bad=1} END {exit bad}' || fail 'archive links or special files'
archive_root=$(mktemp -d /tmp/borsuk-gc-verifier.XXXXXXXX)
tar --extract --gzip --file "$out/qualification-evidence.tar.gz" --directory "$archive_root" --no-same-owner --no-same-permissions
for name in qualify-gc-session.sh supervise-gc-session.sh source-files.sha256 native-source.json required-exact-tests.txt; do file "$base/$name"; done
for name in source-files.sha256 native-source.json required-exact-tests.txt; do
 equal_files "$ev/$name" "$inputs/$name"
done
text_is "$ev/source-commit.txt" "$source_commit"
text_is "$ev/native-original-exit.txt" 0
text_is "$ev/native-exit.txt" 0
[[ ! -e $ev/failed-stage.txt && ! -L $ev/failed-stage.txt ]] || fail 'failed stage retained'

# Streaming entries preserve duplicate object keys, unlike an ordinary JSON object parse.
json_is "$inputs/native-source.json" -s 'length==1 and (.[0] | type=="object" and length==445)'
json_is "$inputs/native-source.json" --stream -s --rawfile manifest "$inputs/source-files.sha256" '
 ($manifest | split("\n")) as $lines |
 ($lines[0:-1] | map(capture("^(?<sha>[0-9a-f]{64})  (?<path>[A-Za-z0-9_.@+/-]+)$"))) as $files |
 ($files | map({key:.path,value:.sha}) | from_entries) as $by_path |
 [.[] | select(length==2)] as $entries |
 ($lines|length)==479 and $lines[-1]=="" and ($files|length)==478 and ($by_path|length)==478 and
 all($files[]; (.path|startswith("/")|not) and (.path|split("/")|all(.!="" and .!="." and .!=".."))) and
 ($entries|length)==445 and ([$entries[]|.[0][0]]|unique|length)==445 and
 all($entries[]; (.[0]|length)==1 and (.[0][0]|type)=="string" and
 (.[1]|type)=="string" and (.[1]|test("^[0-9a-f]{64}$")) and $by_path[.[0][0]]==.[1])'
required_text=$(awk '/^required=\($/ {inside=1; next} inside && /^\)$/ {done=1; exit}
 inside {sub(/^ /, ""); print} END {if (!done) exit 1}' "$inputs/qualify-gc-session.sh")
text_is "$inputs/required-exact-tests.txt" "$required_text"
mapfile -t required <<< "$required_text"
[[ ${#required[@]} == 7 && $(printf '%s\n' "${required[@]}" | sort -u | wc -l) == 7 ]] || fail 'required exact roster'
for name in "${required[@]}"; do [[ $name =~ ^session_tests::[a-z_]+$ ]] || fail 'unsafe exact test name'; done

# Extract only literal jq arguments from pinned source; never source/eval the driver.
literal_query() {
 awk -v start="$1" '
  !inside && $0 ~ start {inside=1; print substr($0,index($0,"\047")+1); next}
  inside {p=index($0,"\047"); if(p) {printf "%s",substr($0,1,p-1); done=1; exit} print}
  END {if(!done) exit 1}' "$inputs/qualify-gc-session.sh"
}
inventory_query=$(literal_query '^run_stage source-inventory ')
inventory_query+=$'\n'
artifact_query=$(literal_query '^ run_stage "\\$stage-artifact" ')
[[ -n $inventory_query && -n $artifact_query ]] || fail 'source query extraction'

native_start=$(epoch "$ev/native-started.txt")
native_finish=$(epoch "$ev/native-finished.txt")
deadline=$(integer "$ev/native-deadline.epoch")
(( native_start <= native_finish && native_finish <= deadline && deadline <= native_start + 4200 )) || fail 'native time envelope'
previous=$native_start
roster=''
declare -A seen_stages=()
stage_count=0
timing() {
 local directory=$1 name=$2 grace=$3 left=$4 status=${5:-0}
 file "$directory/$name-time.txt"
 awk -v status="$status" '
  /^[[:space:]]*Exit status:/ {n++; if($0 !~ "^[[:space:]]*Exit status: " status "$") bad=1}
  /^[[:space:]]*Maximum resident set size \(kbytes\): [0-9]+$/ {rss++}
  /^[[:space:]]*Elapsed \(wall clock\) time/ {wall++}
  END {exit (n!=1 || rss!=1 || wall!=1 || bad)}' "$directory/$name-time.txt" || fail "time evidence: $name"
 [[ $(grep -Fc "Command being timed: \"timeout -k $grace $left " "$directory/$name-time.txt") == 1 ]] || fail "timed command: $name"
}
stage() {
 local name=$1 cap=$2 grace=$3 left start finish
 shift 3
 [[ $name =~ ^[a-z0-9_-]+$ && ! ${seen_stages[$name]+present} ]] || fail 'empty/duplicate stage'
 seen_stages[$name]=1
 stage_count=$((stage_count+1))
 roster+=$(printf '%s\t%s\t%s' "$name" "$cap" "$grace")
 roster+=$'\n'
 text_is "$ev/$name-exit.txt" 0
 command_is "$ev/$name-command.txt" "$@"
 file "$ev/$name.log"
 start=$(epoch "$ev/$name-started.txt")
 finish=$(epoch "$ev/$name-finished.txt")
 left=$(integer "$ev/$name-timeout-seconds.txt")
 (( previous <= start && start <= finish && finish <= native_finish && left > 0 && left <= cap &&
    left <= deadline - start - grace - 31 )) || fail "stage timing: $name"
 timing "$ev" "$name" "$grace" "$left"
 previous=$finish
}

stage admission-input-sha 10 5 sha256sum "$remote/source-files.sha256" "$remote/native-source.json" "$remote/required-exact-tests.txt" "$remote/qualify-gc-session.sh" "$remote/supervise-gc-session.sh"
admission_sha=''
admission_status=''
for name in source-files.sha256 native-source.json required-exact-tests.txt qualify-gc-session.sh supervise-gc-session.sh; do
 admission_sha+="$(digest "$inputs/$name")  $remote/$name"$'\n'
 admission_status+="$remote/$name: OK"$'\n'
done
text_is "$ev/admission-input-sha.log" "${admission_sha%$'\n'}"
stage source-inventory 10 5 jq --stream -se --rawfile manifest "$remote_ev/source-files.sha256" "$inventory_query" "$remote_ev/native-source.json"
json_is "$ev/source-inventory.log" -s 'length==1 and .[0]=={source_files:478,native_source_files:445}'
stage source-before 120 5 sha256sum --strict --check "$remote_ev/source-files.sha256" "$remote_ev/admission-input-sha.log"
source_status=$(awk '{print substr($0,67) ": OK"}' "$inputs/source-files.sha256")
source_status+=$'\n'"${admission_status%$'\n'}"
text_is "$ev/source-before.log" "$source_status"
equal_files "$ev/source-before.log" "$ev/source-after.log"
stage rustc-version 10 5 rustc -vV
stage rustc-cfg 10 5 rustc --print cfg
[[ $(grep -Fxc 'panic="unwind"' "$ev/rustc-cfg.log") == 1 ]] || fail 'unwind configuration'
[[ $(grep -Fxc 'target_os="linux"' "$ev/rustc-cfg.log") == 1 ]] || fail 'Linux configuration'
stage cargo-version 10 5 cargo -vV

binary=''
binary_hashes=''
authenticate() {
 local name=$1 profile=$2 target=$3 kind=$4 opt=0 assertions=true src sha_line sha
 [[ $profile != release ]] || { opt=3; assertions=false; }
 src=$remote/source/crates/borsuk/tests/$target.rs
 [[ $kind != lib ]] || src=$remote/source/crates/borsuk/src/lib.rs
 stage "$name-artifact" 10 5 jq -Rsc --arg target "$target" --arg kind "$kind" "$artifact_query" "$remote_ev/$name-compile.log"
 # Select exactly as THIS driver did, then verify target, profile and build completion.
 binary=$(jq -eRrs --arg target "$target" --arg kind "$kind" --arg src "$src" --arg opt "$opt" \
  --arg manifest "$remote/source/crates/borsuk/Cargo.toml" --argjson assertions "$assertions" '
  [split("\n")[] | fromjson?] as $messages |
  [$messages[] | select(.reason=="compiler-artifact" and .target.name==$target and
   .profile.test==true and (.target.kind|index($kind))!=null and (.executable|type)=="string")] as $artifacts |
  [$messages[] | select(.reason=="build-finished")] as $finished |
  if ($artifacts|length)==1 and ($finished|length)==1 and $finished[0].success==true and
   ($artifacts[0] | .manifest_path==$manifest and .target.src_path==$src and .target.kind==[$kind] and
    .profile.opt_level==$opt and .profile.debug_assertions==$assertions and .profile.overflow_checks==$assertions)
  then $artifacts[0].executable else error("compile artifact/profile/completion mismatch") end' "$ev/$name-compile.log")
 [[ $binary =~ ^/mnt/borsuk-paired-qualification/source/target/(debug|release)/deps/[a-z0-9_]+-[0-9a-f]{16}$ &&
    $binary == "$remote/source/target/$profile/deps/$target-"* ]] || fail "executable path: $name"
 json_is "$ev/$name-artifact.log" -s --arg b "$binary" 'length==1 and .[0]==$b'
 text_is "$ev/$name-executable.txt" "$binary"
 stage "$name-elf" 10 5 readelf -h "$binary"
 [[ $(grep -Ec '^[[:space:]]*Class:[[:space:]]+ELF64$' "$ev/$name-elf.log") == 1 &&
    $(grep -Ec '^[[:space:]]*Type:[[:space:]]+(DYN|EXEC)[[:space:]]' "$ev/$name-elf.log") == 1 &&
    $(grep -Ec '^[[:space:]]*Magic:[[:space:]]+7f 45 4c 46 02 01 01 ' "$ev/$name-elf.log") == 1 ]] || fail "ELF header: $name"
 stage "$name-sha" 10 5 sha256sum "$binary"
 sha_line=$(cat "$ev/$name-sha.log")
 sha=${sha_line%% *}
 [[ $sha =~ ^[0-9a-f]{64}$ ]] || fail "executable hash: $name"
 text_is "$ev/$name-sha.log" "$sha  $binary"
 binary_hashes+="$sha  $binary"$'\n'
}
sha_after() {
 stage "$1-sha-after" 10 5 sha256sum --strict --check "$remote_ev/$1-sha.log"
 text_is "$ev/$1-sha-after.log" "$binary: OK"
}
summary() {
 local log=$1 expected_passed=$2 expected_ignored=$3 expected_filtered=$4 result pattern
 file "$log"
 result=$(grep '^test result:' "$log")
 pattern='^test result: ok\. ([0-9]+) passed; 0 failed; ([0-9]+) ignored; 0 measured; ([0-9]+) filtered out; finished in [0-9]+\.[0-9]+s$'
 [[ $result =~ $pattern ]] || fail "test summary: $log"
 passed=${BASH_REMATCH[1]}; ignored=${BASH_REMATCH[2]}; filtered=${BASH_REMATCH[3]}
 [[ $passed =~ ^[1-9][0-9]{0,5}$ && $ignored =~ ^(0|[1-9][0-9]{0,5})$ && $filtered =~ ^(0|[1-9][0-9]{0,5})$ ]] || fail 'test counters'
 [[ $expected_passed == any || $passed == "$expected_passed" ]] || fail "passed count: $log"
 [[ $ignored == "$expected_ignored" && ( $expected_filtered == any || $filtered == "$expected_filtered" ) ]] || fail "ignored/filtered count: $log"
}
exact() {
 local stage_name=$1 name=$2 cap=$3 filtered_count=$4 listing
 stage "$stage_name-selected" 10 5 "$binary" --list --exact "$name"
 listing=$(grep ': test$' "$ev/$stage_name-selected.log")
 [[ $listing == "$name: test" ]] || fail "empty/duplicate/wrong selection: $stage_name"
 [[ $(grep -Ec '^1 test, 0 benchmarks$' "$ev/$stage_name-selected.log") == 1 ]] || fail "selection total: $stage_name"
 stage "$stage_name" "$cap" 5 "$binary" --test-threads=1 --exact "$name"
 [[ $(grep '^test .* \.\.\. ' "$ev/$stage_name.log") == "test $name ... ok" ]] || fail "exact result: $stage_name"
 summary "$ev/$stage_name.log" 1 0 "$filtered_count"
}
full_results() {
 local log=$1 want_passed=$2 want_ignored=$3 names unique count
 summary "$log" "$want_passed" "$want_ignored" 0
 names=$(awk '/^test .* \.\.\. / {if($0 !~ /^test [a-zA-Z0-9_:]+ \.\.\. (ok|ignored(, .*)?)$/) exit 1; print $2}' "$log")
 [[ -n $names ]] || fail "no test records: $log"
 count=$(printf '%s\n' "$names" | wc -l)
 unique=$(printf '%s\n' "$names" | sort -u | wc -l)
 (( count == unique && count == passed + ignored )) || fail "duplicate/missing test results: $log"
 [[ $(grep -Ec '^test [a-zA-Z0-9_:]+ \.\.\. ok$' "$log") == "$passed" ]] || fail "passed records: $log"
 actual_tests=$(printf '%s\n' "$names" | sort)
}

# Derived from pinned run_stage sites and loops: 6 + 2*21 + 2*6 + 7 + 2 = 69.
for profile in debug release; do
 flags=()
 [[ $profile != release ]] || flags=(--release)
 name=$profile-gc
 stage "$name-compile" 4200 30 cargo test --locked -p borsuk --test two_bit_gc_delayed_delete "${flags[@]}" --no-run --message-format=json
 authenticate "$name" "$profile" two_bit_gc_delayed_delete test
 stage "$name-inventory" 10 5 "$binary" --list
 inventory=$(sed -n 's/: test$//p' "$ev/$name-inventory.log")
 [[ $(printf '%s\n' "$inventory" | wc -l) == 24 && $(printf '%s\n' "$inventory" | sort -u | wc -l) == 24 ]] || fail "inventory: $profile"
 [[ $(grep -Fxc '24 tests, 0 benchmarks' "$ev/$name-inventory.log") == 1 ]] || fail "inventory total: $profile"
 inventory_sorted=$(printf '%s\n' "$inventory" | sort)
 if [[ $profile == debug ]]; then debug_inventory=$inventory_sorted; else
  [[ $inventory_sorted == "$debug_inventory" ]] || fail 'debug/release inventory mismatch'
 fi
 n=0
 for test_name in "${required[@]}"; do
  [[ $'\n'$inventory$'\n' == *$'\n'"$test_name"$'\n'* ]] || fail 'required test absent from inventory'
  n=$((n+1)); cap=20
  [[ $test_name != "${required[4]}" ]] || cap=60
  exact "$name-exact-$n" "$test_name" "$cap" 23
 done
 stage "$name-full" 300 5 "$binary" --test-threads=1
 full_results "$ev/$name-full.log" 24 0
 [[ $actual_tests == "$inventory_sorted" ]] || fail "full/inventory mismatch: $profile"
 sha_after "$name"
done
for target in two_bit_application_ids two_bit_generation; do
 stage "$target-compile" 4200 30 cargo test --locked -p borsuk --test "$target" --no-run --message-format=json
 authenticate "$target" debug "$target" test
 stage "$target-full" 300 5 "$binary" --test-threads=1
 if [[ $target == two_bit_application_ids ]]; then
  full_results "$ev/$target-full.log" any 1
  [[ $(grep -Ec "^test $smoke \.\.\. ignored(, .*)?$" "$ev/$target-full.log") == 1 ]] || fail 'S3 smoke was not explicitly ignored'
  application_passed=$passed
 else
  full_results "$ev/$target-full.log" any 0
  generation_passed=$passed
 fi
 sha_after "$target"
done
stage library-compile 4200 30 cargo test --locked -p borsuk --lib --no-run --message-format=json
authenticate library debug borsuk lib
exact library-semantic-object-store-parity two_bit_generation::source_walk_tests::semantic_object_store_parity 300 any
sha_after library
text_is "$ev/binaries.sha256" "${binary_hashes%$'\n'}"
stage workspace-clippy 4200 30 cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
stage workspace-test-build 4200 30 env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 CARGO_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
[[ $(grep -Ec '^rust-test-build status=0 elapsed_seconds=[0-9]+ jobs=1$' "$ev/workspace-test-build.log") == 1 ]] || fail 'actual jobs=1 test-build result'
grep -Eq '^[[:space:]]*Finished `test` profile ' "$ev/workspace-test-build.log"
[[ $stage_count == 69 ]] || fail 'internal source-derived roster count'
text_is "$ev/stages.tsv" "${roster%$'\n'}"

# The EXIT trap performs the seventieth check, outside stages.tsv.
text_is "$ev/source-after-exit.txt" 0
command_is "$ev/source-after-command.txt" timeout -k 5 20 sha256sum --strict --check "$remote_ev/source-files.sha256" "$remote_ev/admission-input-sha.log"
after_start=$(epoch "$ev/source-after-started.txt")
after_finish=$(epoch "$ev/source-after-finished.txt")
(( previous <= after_start && after_start <= after_finish && after_finish <= native_finish )) || fail 'source-after ordering'
timing "$ev" source-after 5 20
file "$ev/source-comparison.log"
[[ ! -s $ev/source-comparison.log ]] || fail 'source comparison failure'
text_is "$ev/s3-coverage.txt" "$smoke remains ignored; real S3 GC is not qualified."

for when in before after; do
 text_is "$ev/cpu.max.$when" '800000 100000'
 text_is "$ev/cpuset.cpus.effective.$when" 0-7
 text_is "$ev/memory.max.$when" 30064771072
 text_is "$ev/memory.swap.max.$when" 0
 text_is "$ev/pids.max.$when" 512
 file "$ev/memory.events.$when"
 awk '$1=="oom" {a++; if(NF!=2 || $2!="0") bad=1}
      $1=="oom_kill" {b++; if(NF!=2 || $2!="0") bad=1}
      END {exit (a!=1 || b!=1 || bad)}' "$ev/memory.events.$when" || fail "OOM counters: $when"
done
text_is "$ev/cgroup-path.txt" "/system.slice/$unit"
text_is "$ev/memory.swap.peak.after" 0
pids_peak=$(integer "$ev/pids.peak.after")
(( pids_peak > 0 && pids_peak <= 512 )) || fail 'pids peak'

for name in supervisor-original-exit manager-exit native-exit; do text_is "$manager/$name.txt" 0; done
text_is "$manager/owned.txt" true
text_is "$manager/drained.txt" true
text_is "$manager/forced-cleanup.txt" false
text_is "$manager/final-stage.txt" systemd-run
file "$manager/resource-check.log"
[[ ! -s $manager/resource-check.log ]] || fail 'resource comparison failure'
file "$manager/owner.txt"
owner=$(cat "$manager/owner.txt")
[[ $owner == "$unit:$source_commit:$inner_sha:"* ]] || fail 'unit owner'
owner_id=${owner##*:}
[[ $owner_id =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] || fail 'owner nonce'
text_is "$manager/owner.txt" "$unit:$source_commit:$inner_sha:$owner_id"
file "$manager/unit-stop-status.txt"
invocation=$(sed -n 's/^InvocationID=//p' "$manager/unit-stop-status.txt")
[[ $invocation =~ ^[0-9a-f]{32}$ ]] || fail 'unit invocation'
property_is "$manager/unit-stop-status.txt" Description "$owner"
property_is "$manager/unit-stop-status.txt" ControlGroup "/system.slice/$unit"
property_is "$manager/unit-stop-status.txt" Result success
property_is "$manager/unit-stop-status.txt" ExecMainCode 1
property_is "$manager/unit-stop-status.txt" ExecMainStatus 0
file "$manager/systemd-after.log"
show_exit=$(integer "$manager/show-exit.txt")
text_is "$manager/systemd-after-exit.txt" "$show_exit"
if grep -Fxq 'LoadState=not-found' "$manager/systemd-after.log"; then
 (( show_exit == 0 || show_exit == 1 )) || fail 'unloaded unit status'
 property_is "$manager/systemd-after.log" LoadState not-found
else
 (( show_exit == 0 )) || fail 'unit status command'
 property_is "$manager/systemd-after.log" Description "$owner"
 property_is "$manager/systemd-after.log" InvocationID "$invocation"
 property_is "$manager/systemd-after.log" ControlGroup "/system.slice/$unit"
fi
for when in before after; do
 file "$manager/cgroup.events.$when"
 if [[ $(cat "$manager/cgroup.events.$when") == absent ]]; then
  text_is "$manager/cgroup.events.$when" absent
 else
  awk '$1=="populated" {n++; if(NF!=2 || $2!="0") bad=1} END {exit(n!=1 || bad)}' "$manager/cgroup.events.$when" || fail 'owned cgroup not drained'
 fi
done
for name in stop-exit.txt cgroup-kill-exit.txt owned-drain-command.txt; do
 [[ ! -e $manager/$name && ! -L $manager/$name ]] || fail 'forced cleanup evidence'
done
text_is "$manager/inner.sha256" "$inner_sha  $remote/qualify-gc-session.sh"
text_is "$manager/inner-sha.log" "$remote/qualify-gc-session.sh: OK"
text_is "$manager/supervisor-sha.log" "$outer_sha  $remote/supervise-gc-session.sh"
text_is "$manager/load-before.log" not-found
for name in inner-sha supervisor-sha systemd-run; do text_is "$manager/$name-exit.txt" 0; done
load_exit=$(integer "$manager/load-before-exit.txt")
(( load_exit == 0 || load_exit == 1 )) || fail 'unit existed before launch'
text_is "$manager/native-deadline.epoch" "$deadline"
machine_deadline=$(integer "$manager/machine-deadline.epoch")
external_deadline=$(integer "$manager/external-deadline.epoch")
(( deadline <= machine_deadline - 180 && machine_deadline <= external_deadline - 120 )) || fail 'supervisor deadlines'
text_is "$manager/root-envelope.txt" 'native_seconds=4200 machine_seconds=5400 external_seconds=5520 root_cost_ceiling_usd=0.60'

command_is "$manager/inner-sha-command.txt" sha256sum --strict --check "$remote_manager/inner.sha256"
command_is "$manager/supervisor-sha-command.txt" sha256sum "$remote/supervise-gc-session.sh"
command_is "$manager/load-before-command.txt" systemctl show "$unit" -p LoadState --value
command_is "$manager/systemd-after-command.txt" systemctl show "$unit" -p LoadState -p Description -p InvocationID -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup
file "$manager/systemd-run-command.txt"
run_command=$(cat "$manager/systemd-run-command.txt")
[[ $run_command =~ RuntimeMaxSec=([1-9][0-9]{0,4})[[:space:]] ]] || fail 'unit runtime limit'
runtime=${BASH_REMATCH[1]}
(( runtime > 30 && runtime <= 4230 )) || fail 'unit runtime envelope'
stop_hook="/bin/sh -c 'exec /usr/bin/systemctl show $unit -p Description -p InvocationID -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > $remote_manager/unit-stop-status.txt'"
command_is "$manager/systemd-run-command.txt" systemd-run --expand-environment=no --unit="$unit" --wait --pipe \
 -p Description="$owner" -p WorkingDirectory="$remote/source" -p CPUQuota=800% -p CPUQuotaPeriodSec=100ms \
 -p AllowedCPUs=0-7 -p MemoryMax=30064771072 -p MemorySwapMax=0 -p TasksMax=512 \
 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec="$runtime" \
 -p ExecStopPost="$stop_hook" --setenv=RUSTUP_HOME="$remote/.rustup" --setenv=CARGO_HOME="$remote/.cargo" \
 --setenv=PATH="$remote/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
 /bin/bash "$remote/qualify-gc-session.sh" "$remote" "$source_commit" "$deadline" 24
manager_previous=0
for name in inner-sha supervisor-sha load-before systemd-run systemd-after; do
 start=$(epoch "$manager/$name-started.txt")
 finish=$(epoch "$manager/$name-finished.txt")
 left=$(integer "$manager/$name-timeout-seconds.txt")
 status=$(integer "$manager/$name-exit.txt")
 file "$manager/$name.stderr"
 cap=10
 [[ $name != systemd-run ]] || cap=$((runtime+15))
 (( manager_previous <= start && start <= finish && finish <= machine_deadline &&
    left > 0 && left <= cap && left <= machine_deadline - start - 2 )) || fail "manager timing: $name"
 timing "$manager" "$name" 2 "$left" "$status"
 case $name in
  inner-sha)
   (( deadline <= start + 4200 && machine_deadline <= start + 5400 && external_deadline <= start + 5520 )) || fail 'outer time envelope'
   ;;
  load-before) (( finish <= native_start )) || fail 'load-before ordering' ;;
  systemd-run) (( start <= native_start && native_finish <= finish )) || fail 'native containment' ;;
  systemd-after) (( native_finish <= start )) || fail 'systemd-after ordering' ;;
 esac
 manager_previous=$finish
done

# A single JSON result is emitted only after every check and temporary cleanup succeeds.
cleanup
archive_root=''
jq -n --arg source "$source_commit" --arg driver "$driver_commit" --arg instance "$instance" --arg volume "$volume" \
 --arg command "$command" --arg archive "$archive_sha" --argjson pids "$pids_peak" --argjson app "$application_passed" --argjson generation "$generation_passed" '
 {schema:"borsuk-gc-session-root-verification-v1",source_commit:$source,driver_commit:$driver,
 instance_id:$instance,volume_id:$volume,native_command_id:$command,evidence_archive_sha256:$archive,verifier_exit:0,
 stages:69,source_after_verified:true,checks_including_source_after:70,exact_gc_tests:14,gc_inventory_per_profile:24,
 gc_passed_per_profile:24,gc_ignored_per_profile:0,application_ids_passed:$app,application_ids_ignored:1,
 generation_passed:$generation,semantic_library_selected:1,source_files_unchanged:478,native_source_files:445,
 pids_peak:$pids,compute_closed:true,real_s3_gc_qualified:false,production_benchmark_binary:false,
 scope:"completed compiler and test metadata only",scientific_performance_claim:false}'
