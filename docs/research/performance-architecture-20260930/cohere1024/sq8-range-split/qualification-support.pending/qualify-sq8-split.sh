#!/bin/bash
# UNVERIFIED source-only driver; root owns admission and native execution.
set -euo pipefail
[[ $# == 4 ]] || exit 98
root=$1
revision=$2
deadline=$3
input_map_sha=$4
[[ $root == /mnt/borsuk-paired-qualification && $revision =~ ^[0-9a-f]{40}$ && $deadline =~ ^[1-9][0-9]{9}$ && $input_map_sha =~ ^[0-9a-f]{64}$ ]] || exit 98
[[ -f $root/QUALIFICATION_ADMITTED && ! -L $root/QUALIFICATION_ADMITTED ]] || exit 98
[[ $(cat "$root/QUALIFICATION_ADMITTED") == "$revision" ]] || exit 98
ev=$root/evidence/sq8-split
mkdir "$ev"
cd "$root/source"
export CARGO_BUILD_JOBS=2 BORSUK_TEST_BUILD_JOBS=2 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1
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
 printf '%s\n' "$original" > "$ev/native-original-exit.txt" || { (( status != 0 )) || status=94; }
 if (( original != 0 )); then printf '%s\n' "$current_stage" > "$ev/failed-stage.txt"; fi
 if [[ $source_admitted == true ]]; then
  printf '%q ' timeout -k 5 120 sha256sum --strict --check "$ev/source-files.sha256" "$ev/admission-inputs.sha256" "$ev/input-map.sha256" > "$ev/source-after-command.txt"
  printf '\n' >> "$ev/source-after-command.txt"
  date -u +%FT%TZ > "$ev/source-after-started.txt"
  /usr/bin/time -v -o "$ev/source-after-time.txt" timeout -k 5 120 sha256sum --strict --check "$ev/source-files.sha256" "$ev/admission-inputs.sha256" "$ev/input-map.sha256" > "$ev/source-after.log" 2>&1
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
 left=$(( deadline - $(date +%s) - grace - 131 ))
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
[[ $cg == /system.slice/borsuk-sq8-split-qualification.service ]]
[[ $(cat "$ev/cpu.max.before") == '1600000 100000' ]]
[[ $(cat "$ev/cpuset.cpus.effective.before") == 0-15 ]]
[[ $(cat "$ev/memory.max.before") == 60129542144 ]]
[[ $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 512 ]]
[[ $(uname -s) == Linux ]]
[[ $(realpath -e -- "$0") == "$root/qualify-sq8-split.sh" ]]
[[ -f $root/admission-inputs.sha256 && ! -L $root/admission-inputs.sha256 ]]
printf '%s  %s\n' "$input_map_sha" "$root/admission-inputs.sha256" > "$ev/input-map.sha256"
run_stage input-map-sha 10 5 sha256sum --strict --check "$ev/input-map.sha256"
# The root pins this seven-entry map externally, avoiding script self-reference.
map_files=(QUALIFICATION_ADMITTED source.tar.gz source-files.sha256 native-source.json required-exact-tests.txt qualify-sq8-split.sh supervise-sq8-split.sh)
[[ $(wc -l < "$root/admission-inputs.sha256") == "${#map_files[@]}" ]]
for f in "${map_files[@]}"; do
 [[ -f $root/$f && ! -L $root/$f ]]
 [[ $(grep -Ec "^[0-9a-f]{64}  $root/${f//./\\.}$" "$root/admission-inputs.sha256") == 1 ]]
done
run_stage admission-input-sha 120 5 sha256sum --strict --check "$root/admission-inputs.sha256"
cp "$root/admission-inputs.sha256" "$ev/admission-inputs.sha256"
cp "$root/source-files.sha256" "$ev/source-files.sha256"
cp "$root/native-source.json" "$ev/native-source.json"
# jq variables are expanded by jq.
# shellcheck disable=SC2016
run_stage source-inventory 10 5 jq --stream -se --rawfile manifest "$ev/source-files.sha256" '
 ($manifest | split("\n")) as $lines
 | ($lines[0:-1] | map(capture("^(?<sha>[0-9a-f]{64})  (?<path>.+)$"))) as $files
 | ($files | map({key:.path, value:.sha}) | from_entries) as $by_path
 | [.[] | select(length == 2)] as $entries
 | ($entries | map({key:(.[0][0] | tostring), value:.[1]}) | from_entries) as $native
 | if ($lines | length) > 1 and $lines[-1] == ""
  and ($files | length) == ($lines | length) - 1 and ($by_path | length) == ($files | length)
  and all($files[]; (.path | startswith("/") | not)
   and (.path | contains("\\") | not)
   and (.path | test("[\u0000-\u001f\u007f]") | not)
   and (.path | split("/") | all(. != "" and . != "." and . != "..")))
  and ($entries | length) > 0 and ($native | length) == ($entries | length)
  and all($entries[]; (.[0] | length) == 1 and (.[0][0] | type) == "string"
   and (.[1] | type) == "string" and (.[1] | test("^[0-9a-f]{64}$"))
   and $by_path[.[0][0]] == .[1])
  and all(["crates/borsuk/src/sq8_s3_range.rs", "crates/borsuk/src/two_bit_generation.rs",
   "crates/borsuk/src/bin/check_cohere_native_baseline.rs", "crates/borsuk/examples/compare_native_replay.rs",
   "crates/borsuk/examples/compare_native_replay/sq8_split_panel.rs"][]; $native[.] != null)
  and $by_path["scripts/check_rust_test_build.sh"] != null
 then {source_files:($files|length), native_source_files:($entries|length)}
 else error("malformed, duplicate, missing or mismatched source/native inventory") end
' "$ev/native-source.json"
current_stage=source-paths
while read -r _ path; do
 [[ -f $path && ! -L $path && $(realpath -e -- "$path") == "$root/source/$path" ]]
done < "$ev/source-files.sha256"
source_admitted=true
run_stage source-before 120 5 sha256sum --strict --check "$ev/source-files.sha256" "$ev/admission-inputs.sha256" "$ev/input-map.sha256"
run_stage rustc-version 10 5 rustc -vV
run_stage rustc-cfg 10 5 rustc --print cfg
grep -Fxq 'panic="unwind"' "$ev/rustc-cfg.log"
run_stage cargo-version 10 5 cargo -vV
required_lib=(
 sq8_s3_range::tests::split_plan_exact_coverage_tail_caps_and_overflow
 sq8_s3_range::tests::split_unsplit_score_bits_ordinals_exclusions_and_charges
 sq8_s3_range::tests::split_http_count_etag_auth_ordered_error_and_drain
 sq8_s3_range::tests::split_http_wrong_etag_fails_closed_and_drains_every_get
 sq8_s3_range::tests::split_refuses_caps_trace_and_window_before_http
 two_bit_generation::source_walk_tests::generation_sq8_split_admission_and_serving_parity
)
required_bin=(
 tests::cold_sq8_split_config_version_caps_trace_and_identity
 tests::actual_sq8_split_config_reaches_serving_and_preserves_parity
 tests::sq8_split_multi_query_reservation_and_direct_memory_boundaries
)
required_example=(
 sq8_split_panel::tests::sq8_split_panel_sealed_six_runs_and_screen
 sq8_split_panel::tests::sq8_split_panel_coherently_resealed_negatives
 sq8_split_panel::tests::sq8_split_panel_late_auth_tail_growth_and_source_pins
 sq8_split_panel::tests::sq8_split_panel_physical_caps_options_and_trace_parity
 sq8_split_panel::tests::sq8_split_panel_stage_clock_and_conservation
 sq8_split_panel::tests::sq8_split_panel_output_collision_and_sync_errors
)
printf '%s\n' "${required_lib[@]}" "${required_bin[@]}" "${required_example[@]}" > "$ev/required-exact-tests.txt"
cmp "$ev/required-exact-tests.txt" "$root/required-exact-tests.txt"
printf '%s\n' 'EXTERNAL NOT_RUN: root builder-removal mutation falsifier; not executed by this driver.' > "$ev/builder-removal-falsifier.txt"
resolve_binary() {
 local stage=$1 profile=$2 target=$3 kind=$4 candidate actual directory=deps
 [[ $kind != example ]] || directory=examples
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
 [[ $actual == "$candidate" && $actual == "$root/source/target/$profile/$directory/"* ]]
 [[ $(realpath -e -- "target/$profile/$directory") == "$root/source/target/$profile/$directory" ]]
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
regression() {
 local stage=$1 filter=${2:-} selected
 run_stage "$stage-selected" 10 5 "$binary" --list "$filter"
 selected=$(grep -c ': test$' "$ev/$stage-selected.log")
 [[ $selected -gt 0 ]]
 run_stage "$stage" 600 5 "$binary" --test-threads=1 "$filter"
 grep -Eq "^test result: ok\. $selected passed; 0 failed; 0 ignored; 0 measured;" "$ev/$stage.log"
}
# Finish debug correctness before paying for the release compile. Within each
# profile, compile and inventory all three harnesses before the first test.
for profile in debug release; do
 flags=()
 [[ $profile != release ]] || flags=(--release)
 for kind in lib bin example; do
  case $kind in
   lib) target=borsuk; selector=(--lib) ;;
   bin) target=check_cohere_native_baseline; selector=(--bin "$target") ;;
   example) target=compare_native_replay; selector=(--example "$target") ;;
  esac
  stage=$profile-$kind
  run_stage "$stage-compile" 4200 30 cargo test --locked -p borsuk "${selector[@]}" "${flags[@]}" --no-run --message-format=json
  resolve_binary "$stage" "$profile" "$target" "$kind"
 done
 for kind in lib bin example; do
  stage=$profile-$kind
  binary=$(cat "$ev/$stage-executable.txt")
  verify_binary "$stage-sha-before" "$ev/$stage-sha.log"
  run_stage "$stage-inventory" 10 5 "$binary" --list
  [[ $(grep -c ': test$' "$ev/$stage-inventory.log") -gt 0 ]]
  case $kind in
   lib) required=("${required_lib[@]}") ;;
   bin) required=("${required_bin[@]}") ;;
   example) required=("${required_example[@]}") ;;
  esac
  for name in "${required[@]}"; do
   [[ $(grep -Fxc "$name: test" "$ev/$stage-inventory.log") == 1 ]]
  done
 done
 # All required names must exist in this profile before the first exact test.
 for kind in lib bin example; do
  stage=$profile-$kind
  binary=$(cat "$ev/$stage-executable.txt")
  case $kind in
   lib) required=("${required_lib[@]}") ;;
   bin) required=("${required_bin[@]}") ;;
   example) required=("${required_example[@]}") ;;
  esac
  n=0
  for name in "${required[@]}"; do
   n=$((n+1))
   exact_test "$stage-exact-$n" "$name" 300
  done
  verify_binary "$stage-sha-exact-after" "$ev/$stage-sha.log"
 done
 binary=$(cat "$ev/$profile-lib-executable.txt")
 regression "$profile-sq8-s3-range" 'sq8_s3_range::tests::'
 regression "$profile-source-walk" 'two_bit_generation::source_walk_tests::'
 verify_binary "$profile-lib-sha-after" "$ev/$profile-lib-sha.log"
 for kind in bin example; do
  binary=$(cat "$ev/$profile-$kind-executable.txt")
  if [[ $kind == example ]]; then
   grep -q '^source_cover::.*: test$' "$ev/$profile-$kind-inventory.log"
  fi
  regression "$profile-$kind-full"
  verify_binary "$profile-$kind-sha-after" "$ev/$profile-$kind-sha.log"
 done
done
run_stage release-production-build 4200 30 cargo build --locked -p borsuk --release --bin check_cohere_native_baseline --example compare_native_replay
mkdir "$ev/binaries"
for artifact in target/release/check_cohere_native_baseline target/release/examples/compare_native_replay; do
 current_stage=production-authenticate
 [[ -f $artifact && ! -L $artifact && -x $artifact ]]
 [[ $(realpath -e -- "$artifact") == "$root/source/$artifact" ]]
 name=${artifact##*/}
 run_stage "production-$name-elf" 10 5 readelf -h "$artifact"
 run_stage "production-$name-sha" 10 5 sha256sum "$artifact"
 cat "$ev/production-$name-sha.log" >> "$ev/binaries.sha256"
 run_stage "production-$name-retain" 30 5 cp -- "$artifact" "$ev/binaries/$name"
 run_stage "production-$name-retained-parity" 10 5 cmp -- "$artifact" "$ev/binaries/$name"
done
run_stage workspace-clippy 4200 30 cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_stage workspace-test-build 4200 30 env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=2 CARGO_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh
verify_binary binaries-final-sha "$ev/binaries.sha256"
current_stage=finished
