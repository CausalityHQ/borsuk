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
ev=$root/evidence/sq8-get-curve
mkdir "$ev"
cd "$root/source"
export CARGO_BUILD_JOBS=1 BORSUK_TEST_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1
export BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1
unset RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
unset CARGO_BUILD_RUSTC_WRAPPER CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER CARGO_BUILD_RUSTFLAGS
unset CARGO_BUILD_TARGET CARGO_TARGET_DIR BORSUK_TEST_BUILD_COMMAND BORSUK_SOURCE_NEIGHBORHOOD_CRASH_DIR
# Cargo accepts target-specific flag/runner overrides as well as global ones.
for variable in ${!CARGO_TARGET_@}; do
 case $variable in *_RUSTFLAGS|*_RUNNER|*_LINKER) unset "$variable" ;; esac
done
started=$(date +%s)
(( deadline <= started + 6600 )) || deadline=$((started + 6600))
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
[[ $cg == /system.slice/borsuk-sq8-get-curve-qualification.service ]]
[[ $(cat "$ev/cpu.max.before") == '1600000 100000' ]]
[[ $(cat "$ev/cpuset.cpus.effective.before") == 0-15 ]]
[[ $(cat "$ev/memory.max.before") == 60129542144 ]]
[[ $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 512 ]]
[[ $(uname -s) == Linux ]]
[[ $(realpath -e -- "$0") == "$root/qualify-sq8-get-curve.sh" ]]
[[ -f $root/admission-inputs.sha256 && ! -L $root/admission-inputs.sha256 ]]
printf '%s  %s\n' "$input_map_sha" "$root/admission-inputs.sha256" > "$ev/input-map.sha256"
run_stage input-map-sha 10 5 sha256sum --strict --check "$ev/input-map.sha256"
# The root pins this seven-entry map externally, avoiding script self-reference.
map_files=(QUALIFICATION_ADMITTED source.tar.gz source-files.sha256 native-source.json required-exact-tests.txt qualify-sq8-get-curve.sh supervise-sq8-get-curve.sh)
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
  and all(["crates/borsuk/examples/compare_native_replay.rs",
   "crates/borsuk/examples/compare_native_replay/sq8_subpage_shortlist.rs",
   "crates/borsuk/examples/compare_native_replay/sq8_count_curve.rs",
   "crates/borsuk/examples/compare_native_replay/sq8_get_curve.rs"][]; $native[.] != null)
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
original_required=(
 sq8_subpage_shortlist::get_curve::tests::strict_config_and_budgets_leave_payloads_unopened
 sq8_subpage_shortlist::get_curve::tests::g30_equivalence_and_more_gets_change_admitted_rows
 sq8_subpage_shortlist::get_curve::tests::partial_tail_exact_byte_cap_and_tied_gaps
 sq8_subpage_shortlist::get_curve::tests::bridge_removal_changes_winner_and_can_rescue_or_lose_truth
 sq8_subpage_shortlist::get_curve::tests::report_preserves_below_floor_and_rejects_resealed_budget_charge_quality_errors
 sq8_subpage_shortlist::get_curve::tests::g30_historical_binding_requires_rank_proof_and_checks_every_query
 sq8_subpage_shortlist::get_curve::tests::retained_control_authenticates_complete_ranks_before_truth_and_rechecks_snapshot
 sq8_subpage_shortlist::get_curve::tests::admission_smoke_and_measurement_schemas_cannot_be_substituted
 sq8_subpage_shortlist::get_curve::tests::create_only_and_invalid_errors_preserve_existing_output
)
[[ $(stat -c %s "$root/required-exact-tests.txt") -le 65536 ]]
run_stage required-roster-read 10 5 head -c 65537 -- "$root/required-exact-tests.txt"
[[ $(stat -c %s "$ev/required-roster-read.log") -le 65536 ]]
run_stage required-roster-sha 10 5 sha256sum "$ev/required-roster-read.log"
required_roster_sha=$(cut -d ' ' -f 1 "$ev/required-roster-sha.log")
grep -Fxq "$required_roster_sha  $root/required-exact-tests.txt" "$root/admission-inputs.sha256"
# The root may admit additional repaired fixtures; preserve the original subset.
# shellcheck disable=SC2016
run_stage required-roster-validate 10 5 jq -Rse '
 split("\n") | (if .[-1] == "" then .[:-1] else . end) as $names
 | if ($names | length > 0)
   and ($names | all(length <= 512 and test("^sq8_subpage_shortlist::get_curve::tests::[A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*$")))
   and (($names | unique | length) == ($names | length))
   then {required_exact_tests:($names|length)} else error("invalid exact GetCurve roster") end
' "$ev/required-roster-read.log"
for name in "${original_required[@]}"; do
 [[ $(grep -Fxc "$name" "$ev/required-roster-read.log") == 1 ]]
done
cp "$ev/required-roster-read.log" "$ev/required-exact-tests.txt"
mapfile -t required_example < "$ev/required-exact-tests.txt"
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
 run_stage "$stage" "$cap" 5 "$binary" --test-threads=1 --exact "$name" --nocapture
 # The exact selector was authenticated above. nocapture may interleave a test's
 # own stdout/panic witnesses with the pretty per-name line; require exit0 and
 # the one-pass/zero-failure/zero-ignore harness summary instead.
 grep -Eq '^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured;' "$ev/$stage.log"
}
regression() {
 local stage=$1 filter=${2:-} selected cap=600
 if [[ $stage == *-example-full ]]; then cap=1800; fi
 run_stage "$stage-selected" 10 5 "$binary" --list "$filter"
 selected=$(grep -c ': test$' "$ev/$stage-selected.log")
 [[ $selected -gt 0 ]]
 run_stage "$stage" "$cap" 5 "$binary" --test-threads=1 "$filter" --nocapture
 grep -Eq "^test result: ok\. $selected passed; 0 failed; 0 ignored; 0 measured;" "$ev/$stage.log"
}
# Inventory every admitted name before executing any fixture in each profile.
for profile in debug release; do
 flags=()
 [[ $profile != release ]] || flags=(--release)
 stage=$profile-example
 run_stage "$stage-compile" 6600 30 cargo test --locked --jobs 1 -p borsuk --example compare_native_replay "${flags[@]}" --no-run --message-format=json
 resolve_binary "$stage" "$profile" compare_native_replay example
 verify_binary "$stage-sha-before" "$ev/$stage-sha.log"
 run_stage "$stage-inventory" 10 5 "$binary" --list
 [[ $(grep -c ': test$' "$ev/$stage-inventory.log") -gt 0 ]]
 for name in "${required_example[@]}"; do
  [[ $(grep -Fxc "$name: test" "$ev/$stage-inventory.log") == 1 ]]
 done
 n=0
 for name in "${required_example[@]}"; do
  n=$((n+1))
  exact_test "$stage-exact-$n" "$name" 300
 done
 verify_binary "$stage-sha-exact-after" "$ev/$stage-sha.log"
 if [[ $profile == debug ]]; then
  regression "$stage-full"
  verify_binary "$stage-sha-after" "$ev/$stage-sha.log"
 fi
done
run_stage release-production-build 6600 30 cargo build --locked --jobs 1 -p borsuk --release --example compare_native_replay
mkdir "$ev/binaries"
artifact=target/release/examples/compare_native_replay
current_stage=production-authenticate
[[ -f $artifact && ! -L $artifact && -x $artifact ]]
[[ $(realpath -e -- "$artifact") == "$root/source/$artifact" ]]
name=${artifact##*/}
run_stage "production-$name-elf" 10 5 readelf -h "$artifact"
run_stage "production-$name-size" 10 5 stat -c %s "$artifact"
run_stage "production-$name-sha" 10 5 sha256sum "$artifact"
cat "$ev/production-$name-sha.log" >> "$ev/binaries.sha256"
run_stage "production-$name-retain" 30 5 cp -- "$artifact" "$ev/binaries/$name"
run_stage "production-$name-retained-parity" 10 5 cmp -- "$artifact" "$ev/binaries/$name"
run_stage workspace-clippy 6600 30 cargo clippy --locked --jobs 1 --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_stage workspace-test-build 6600 30 env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 CARGO_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
verify_binary binaries-final-sha "$ev/binaries.sha256"
current_stage=finished
