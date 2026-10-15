#!/bin/bash
# UNVERIFIED source-only driver; root owns admission and native execution.
set -euo pipefail
[[ $# == 4 ]] || exit 98
root=$1
revision=$2
deadline=$3
expected_inventory=$4
[[ $root == /mnt/borsuk-paired-qualification && $revision =~ ^[0-9a-f]{40}$ && $deadline =~ ^[1-9][0-9]{9}$ && $expected_inventory == 24 ]] || exit 98
[[ -f $root/QUALIFICATION_ADMITTED && ! -L $root/QUALIFICATION_ADMITTED ]] || exit 98
[[ $(cat "$root/QUALIFICATION_ADMITTED") == "$revision" ]] || exit 98
ev=$root/evidence/gc-session
mkdir "$ev"
cd "$root/source"
export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1
export BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1
unset RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
unset CARGO_BUILD_RUSTC_WRAPPER CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER CARGO_BUILD_RUSTFLAGS
unset CARGO_BUILD_TARGET CARGO_TARGET_DIR BORSUK_TEST_BUILD_COMMAND BORSUK_SOURCE_NEIGHBORHOOD_CRASH_DIR
# Cargo accepts target-specific flag/runner overrides as well as global ones.
for variable in ${!CARGO_TARGET_@}; do
 case $variable in *_RUSTFLAGS|*_RUNNER|*_LINKER) unset "$variable" ;; esac
done
started=$(date +%s)
(( deadline <= started + 4200 )) || deadline=$((started + 4200))
printf '%s\n' "$deadline" > "$ev/native-deadline.epoch"
printf '%s\n' "$revision" > "$ev/source-commit.txt"
date -u +%FT%TZ > "$ev/native-started.txt"
current_stage=preflight
source_admitted=false
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
printf '%s\n' "$cg" > "$ev/cgroup-path.txt"
finish() {
 local original=$? status f after_rc
 trap - EXIT
 set +e
 status=$original
 printf '%s\n' "$original" > "$ev/native-original-exit.txt" || status=94
 if (( original != 0 )); then printf '%s\n' "$current_stage" > "$ev/failed-stage.txt"; fi
 if [[ $source_admitted == true ]]; then
  printf '%q ' timeout -k 5 20 sha256sum --strict --check "$ev/source-files.sha256" "$ev/admission-input-sha.log" > "$ev/source-after-command.txt"
  printf '\n' >> "$ev/source-after-command.txt"
  date -u +%FT%TZ > "$ev/source-after-started.txt"
  /usr/bin/time -v -o "$ev/source-after-time.txt" timeout -k 5 20 sha256sum --strict --check "$ev/source-files.sha256" "$ev/admission-input-sha.log" > "$ev/source-after.log" 2>&1
  after_rc=$?
  printf '%s\n' "$after_rc" > "$ev/source-after-exit.txt"
  date -u +%FT%TZ > "$ev/source-after-finished.txt"
  (( after_rc == 0 )) || { (( status != 0 )) || status=94; }
 fi
 if [[ -f $ev/source-before.log && -f $ev/source-after.log ]]; then
  cmp "$ev/source-before.log" "$ev/source-after.log" > "$ev/source-comparison.log" 2>&1 || { (( status != 0 )) || status=94; }
 fi
 for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do
  if [[ -f /sys/fs/cgroup$cg/$f ]]; then
   cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after" || { (( status != 0 )) || status=94; }
  fi
 done
 printf '%s\n' "$status" > "$ev/native-exit.txt"
 date -u +%FT%TZ > "$ev/native-finished.txt"
 exit "$status"
}
trap finish EXIT
run_stage() {
 local name=$1 cap=$2 grace=$3 left rc
 shift 3
 current_stage=$name
 printf '%s\t%s\t%s\n' "$name" "$cap" "$grace" >> "$ev/stages.tsv" || return 94
 printf '%q ' "$@" > "$ev/$name-command.txt" || return 94
 printf '\n' >> "$ev/$name-command.txt" || return 94
 date -u +%FT%TZ > "$ev/$name-started.txt" || return 94
 left=$(( deadline - $(date +%s) - grace - 31 ))
 (( left <= cap )) || left=$cap
 printf '%s\n' "$left" > "$ev/$name-timeout-seconds.txt" || return 94
 if (( left <= 0 )); then
  printf '%s\n' 'shared native deadline exhausted before stage' > "$ev/$name.log" || return 94
  : > "$ev/$name-time.txt" || return 94
  rc=124
 else
  /usr/bin/time -v -o "$ev/$name-time.txt" timeout -k "$grace" "$left" "$@" > "$ev/$name.log" 2>&1 && rc=0 || rc=$?
 fi
 printf '%s\n' "$rc" > "$ev/$name-exit.txt" || return 94
 date -u +%FT%TZ > "$ev/$name-finished.txt" || return 94
 return "$rc"
}
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max memory.events; do
 cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"
done
[[ $cg == /system.slice/borsuk-gc-session-qualification.service ]]
[[ $(cat "$ev/cpu.max.before") == '800000 100000' ]]
[[ $(cat "$ev/cpuset.cpus.effective.before") == 0-7 ]]
[[ $(cat "$ev/memory.max.before") == 30064771072 ]]
[[ $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 512 ]]
[[ $(uname -s) == Linux ]]
[[ -f $root/source-files.sha256 && ! -L $root/source-files.sha256 ]]
[[ -f $root/native-source.json && ! -L $root/native-source.json ]]
[[ -f $root/required-exact-tests.txt && ! -L $root/required-exact-tests.txt ]]
run_stage admission-input-sha 10 5 sha256sum "$root/source-files.sha256" "$root/native-source.json" "$root/required-exact-tests.txt" "$root/qualify-gc-session.sh" "$root/supervise-gc-session.sh"
cp "$root/source-files.sha256" "$ev/source-files.sha256"
cp "$root/native-source.json" "$ev/native-source.json"
# jq variables are expanded by jq.
# shellcheck disable=SC2016
run_stage source-inventory 10 5 jq --stream -se --rawfile manifest "$ev/source-files.sha256" '
 ($manifest | split("\n")) as $lines
 | ($lines[0:-1] | map(capture("^(?<sha>[0-9a-f]{64})  (?<path>.+)$"))) as $files
 | ($files | map({key:.path, value:.sha}) | from_entries) as $by_path
 | [.[] | select(length == 2)] as $entries
 | if ($lines | length) == 479 and $lines[-1] == ""
  and ($files | length) == 478 and ($by_path | length) == 478
  and all($files[]; (.path | startswith("/") | not)
   and (.path | contains("\\") | not)
   and (.path | split("/") | all(. != "" and . != "." and . != "..")))
  and ($entries | length) == 445
  and ([$entries[] | .[0][0]] | unique | length) == 445
  and all($entries[]; (.[0] | length) == 1 and (.[0][0] | type) == "string"
   and (.[1] | type) == "string" and (.[1] | test("^[0-9a-f]{64}$"))
   and $by_path[.[0][0]] == .[1])
 then {source_files:478, native_source_files:445}
 else error("malformed, duplicate or mismatched source/native inventory") end
' "$ev/native-source.json"
source_admitted=true
run_stage source-before 120 5 sha256sum --strict --check "$ev/source-files.sha256" "$ev/admission-input-sha.log"
run_stage rustc-version 10 5 rustc -vV
run_stage rustc-cfg 10 5 rustc --print cfg
grep -Fxq 'panic="unwind"' "$ev/rustc-cfg.log"
run_stage cargo-version 10 5 cargo -vV
required=(
 session_tests::real_filesystem_absent_pending_candidate_reopens_reads_and_mutations
 session_tests::fence_release_reconciliation_and_unresolved_errors_preserve_continuation
 session_tests::dropped_waiter_during_keep_validation_keeps_lock_and_progress
 session_tests::dropped_waiter_during_list_poll_keeps_lock_and_progress
 session_tests::real_filesystem_list_crosses_sdk_batch_with_small_caps_and_fresh_metadata
 session_tests::session_drop_terminates_recorded_worker_and_runtime_helpers
 session_tests::worker_panic_reports_unknown_progress_and_recreation_recovers
)
printf '%s\n' "${required[@]}" > "$ev/required-exact-tests.txt"
cmp "$ev/required-exact-tests.txt" "$root/required-exact-tests.txt"
printf '%s\n' 'native_s3_conditional_fence_and_gc_smoke remains ignored; real S3 GC is not qualified.' > "$ev/s3-coverage.txt"
resolve_binary() {
 local stage=$1 profile=$2 target=$3 kind=$4 candidate actual
 # shellcheck disable=SC2016
 run_stage "$stage-artifact" 10 5 jq -Rsc --arg target "$target" --arg kind "$kind" '
  [split("\n")[] | fromjson? | select(.reason == "compiler-artifact"
   and .target.name == $target and .profile.test == true
   and (.target.kind | index($kind)) != null and (.executable | type) == "string") | .executable]
  | if length == 1 then .[0] else error("expected exactly one test executable from this compile log") end
 ' "$ev/$stage-compile.log"
 candidate=$(jq -er . "$ev/$stage-artifact.log")
 current_stage=$stage-authenticate
 [[ -f $candidate && ! -L $candidate && -x $candidate ]]
 actual=$(realpath -e -- "$candidate")
 [[ $actual == "$candidate" && $actual == "$root/source/target/$profile/deps/"* ]]
 [[ $(realpath -e -- "target/$profile/deps") == "$root/source/target/$profile/deps" ]]
 printf '%s\n' "$actual" > "$ev/$stage-executable.txt"
 run_stage "$stage-elf" 10 5 readelf -h "$actual"
 run_stage "$stage-sha" 10 5 sha256sum "$actual"
 cat "$ev/$stage-sha.log" >> "$ev/binaries.sha256"
 binary=$actual
}
verify_binary() {
 run_stage "$1" 10 5 sha256sum --strict --check "$2"
}
exact_test() {
 local stage=$1 name=$2 cap=$3 selected
 run_stage "$stage-selected" 10 5 "$binary" --list --exact "$name"
 selected=$(grep -c ': test$' "$ev/$stage-selected.log" || true)
 [[ $selected == 1 ]]
 grep -Fxq "$name: test" "$ev/$stage-selected.log"
 run_stage "$stage" "$cap" 5 "$binary" --test-threads=1 --exact "$name"
 grep -Fxq "test $name ... ok" "$ev/$stage.log"
 grep -Eq '^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured;' "$ev/$stage.log"
}
for profile in debug release; do
 flags=()
 [[ $profile != release ]] || flags=(--release)
 stage=$profile-gc
 run_stage "$stage-compile" 4200 30 cargo test --locked -p borsuk --test two_bit_gc_delayed_delete "${flags[@]}" --no-run --message-format=json
 resolve_binary "$stage" "$profile" two_bit_gc_delayed_delete test
 run_stage "$stage-inventory" 10 5 "$binary" --list
 [[ $(grep -c ': test$' "$ev/$stage-inventory.log") == "$expected_inventory" ]]
 grep -Fxq '24 tests, 0 benchmarks' "$ev/$stage-inventory.log"
 n=0
 for name in "${required[@]}"; do
  n=$((n+1))
  cap=20
  [[ $name != session_tests::real_filesystem_list_crosses_sdk_batch_with_small_caps_and_fresh_metadata ]] || cap=60
  exact_test "$stage-exact-$n" "$name" "$cap"
 done
 run_stage "$stage-full" 300 5 "$binary" --test-threads=1
 grep -Eq '^test result: ok\. 24 passed; 0 failed; 0 ignored; 0 measured;' "$ev/$stage-full.log"
 verify_binary "$stage-sha-after" "$ev/$stage-sha.log"
done
for target in two_bit_application_ids two_bit_generation; do
 run_stage "$target-compile" 4200 30 cargo test --locked -p borsuk --test "$target" --no-run --message-format=json
 resolve_binary "$target" debug "$target" test
 run_stage "$target-full" 300 5 "$binary" --test-threads=1
 grep -Eq '^test result: ok\. [1-9][0-9]* passed; 0 failed;' "$ev/$target-full.log"
 if [[ $target == two_bit_application_ids ]]; then
  grep -Fq 'test native_s3_conditional_fence_and_gc_smoke ... ignored' "$ev/$target-full.log"
 fi
 verify_binary "$target-sha-after" "$ev/$target-sha.log"
done
run_stage library-compile 4200 30 cargo test --locked -p borsuk --lib --no-run --message-format=json
resolve_binary library debug borsuk lib
exact_test library-semantic-object-store-parity two_bit_generation::source_walk_tests::semantic_object_store_parity 300
verify_binary library-sha-after "$ev/library-sha.log"
run_stage workspace-clippy 4200 30 cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_stage workspace-test-build 4200 30 env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 CARGO_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
current_stage=finished
