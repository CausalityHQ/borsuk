#!/bin/bash
# UNVERIFIED source-only falsifier. Parent owns isolation, admission and execution.
# Usage: SOURCE_DIR EXPECTED_RUNNER_SHA NEW_EVIDENCE_DIR DEADLINE_EPOCH EXPECTED_SOURCE_MANIFEST_SHA
# Essential fifth pin: SHA256 of the required ../source-files.sha256 sidecar
# relative to SOURCE_DIR, independently frozen by parent. Exact manifest format:
# (cd SOURCE_DIR && find . -type f -printf '%P\0' | LC_ALL=C sort -z | xargs -0 -r sha256sum --)
# Stage the external driver outside source; it need not belong to build inputs.
# Only use a disposable remote test source copy, never production/scientific source.
# Source has no .git, symlinks or hardlinks; evidence/target live outside source.
# Offline dependency cache/toolchain must already exist. This launches no jobs.
set -euo pipefail
[[ $# == 5 ]] || exit 98
source_dir=$1 expected_runner_sha=$2 ev=$3 deadline=$4 expected_manifest_sha=$5
[[ $source_dir == /* && $ev == /* && $expected_runner_sha =~ ^[0-9a-f]{64}$ &&
   $expected_manifest_sha =~ ^[0-9a-f]{64}$ && $deadline =~ ^[1-9][0-9]{9}$ ]] || exit 98
[[ $source_dir != / && $(realpath -e -- "$source_dir") == "$source_dir" && -d $source_dir &&
   ! -e $source_dir/.git && ! -L $source_dir/.git &&
   $(realpath -m -- "$ev") == "$ev" && ! -e $ev && ! -L $ev &&
   $ev != "$source_dir" && $ev != "$source_dir/"* && $source_dir != "$ev/"* ]] || exit 98
(( deadline - $(date +%s) > 130 )) || exit 98
exec {source_lock}<"$source_dir"
flock -n "$source_lock" || exit 98
driver=$(realpath -e -- "$0")
mkdir -- "$ev"
runner_rel=crates/borsuk/src/bin/check_cohere_native_baseline.rs
runner=$source_dir/$runner_rel
source_manifest=$(dirname -- "$source_dir")/source-files.sha256
test_name=tests::actual_sq8_split_config_reaches_serving_and_preserves_parity
anchor='        Some(split) => generation.with_sq8_range_split(split)?,'
replacement='        Some(_split) => generation,'
assertion='                        assert_eq!(spans.len(), expected.len());'
current_stage=preflight
saved=false pinned=false success=false oom_before=false reserve=120
export LC_ALL=C CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1
export BORSUK_TEST_BUILD_JOBS=2 BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1
unset BASH_ENV ENV RUSTC RUSTDOC RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTFLAGS
unset CARGO_ENCODED_RUSTFLAGS CARGO_BUILD_RUSTFLAGS CARGO_BUILD_RUSTC CARGO_BUILD_RUSTDOC
unset CARGO_BUILD_RUSTC_WRAPPER CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER CARGO_BUILD_TARGET
unset BORSUK_TEST_BUILD_COMMAND BORSUK_SOURCE_NEIGHBORHOOD_CRASH_DIR
for variable in ${!CARGO_TARGET_@}; do
 case $variable in *_RUSTFLAGS|*_RUNNER|*_LINKER) unset "$variable" ;; esac
done
export CARGO_TARGET_DIR=$ev/target
export RUST_BACKTRACE=0

# Each layer records its own exit; a killed original has no original.exit receipt.
# shellcheck disable=SC2016
run_stage() {
 local name=$1 cap=$2 budget rc
 shift 2
 current_stage=$name
 budget=$((deadline - $(date +%s) - reserve - 5))
 (( budget <= cap )) || budget=$cap
 printf '%q ' "$@" > "$ev/$name.command" || return 94
 printf '\n' >> "$ev/$name.command" || return 94
 printf '%s\n' "$budget" > "$ev/$name.timeout-seconds" || return 94
 date -u +%FT%TZ > "$ev/$name.started" || return 94
 if (( budget <= 0 )); then
  printf '%s\n' 'shared deadline exhausted; command not started' > "$ev/$name.stderr" || return 94
  : > "$ev/$name.stdout" || return 94
  rc=124
 else
  /usr/bin/time -v -o "$ev/$name.time" bash -c '
   receipt=$1; shift
   timeout -k 5 "$@"
   rc=$?
   printf "%s\n" "$rc" > "$receipt" || exit 94
   exit "$rc"
  ' bash "$ev/$name.timeout.exit" "$budget" bash -c '
   receipt=$1; shift
   "$@"
   rc=$?
   printf "%s\n" "$rc" > "$receipt" || exit 94
   exit "$rc"
  ' bash "$ev/$name.original.exit" "$@" > "$ev/$name.stdout" 2> "$ev/$name.stderr" && rc=0 || rc=$?
 fi
 printf '%s\n' "$rc" > "$ev/$name.time.exit" || return 94
 date -u +%FT%TZ > "$ev/$name.finished" || return 94
 return "$rc"
}

# shellcheck disable=SC2016
manifest() {
 run_stage "$1" 110 bash -o pipefail -c '
  cd -- "$1" || exit
  [[ -z $(find . ! -type f ! -type d -print -quit) ]] || exit 98
  [[ -z $(find . -type f -links +1 -print -quit) ]] || exit 98
  find . -type f -printf "%P\0" | LC_ALL=C sort -z | xargs -0 -r sha256sum --
 ' bash "$source_dir"
}

restore() {
 local phase=$1 copy_rc hash_rc tree_rc
 current_stage=$phase
 # Copy first, even when a signal or exhausted caller deadline caused this exit.
 cp -- "$ev/runner.original.rs" "$runner" && copy_rc=0 || copy_rc=$?
 printf '%s\n' "$copy_rc" > "$ev/$phase.copy.exit" || return 94
 sha256sum -- "$runner" > "$ev/$phase.runner.sha256" && hash_rc=0 || hash_rc=$?
 if (( hash_rc == 0 )); then
  [[ $(cut -d ' ' -f1 "$ev/$phase.runner.sha256") == "$expected_runner_sha" ]] &&
   cmp -- "$ev/runner.original.rs" "$runner" && hash_rc=0 || hash_rc=95
 fi
 printf '%s\n' "$hash_rc" > "$ev/$phase.runner-identity.exit" || return 94
 manifest "$phase.tree" && cmp -- "$ev/source.before.stdout" "$ev/$phase.tree.stdout" && tree_rc=0 || tree_rc=$?
 printf '%s\n' "$tree_rc" > "$ev/$phase.tree-identity.exit" || return 94
 (( copy_rc == 0 && hash_rc == 0 && tree_rc == 0 ))
}

finish() {
 local original_rc=$? failed_stage=$current_stage restore_rc=0 oom_rc=95 status=95 disposition=INVALID
 trap - EXIT
 trap '' HUP INT TERM
 set +e
 reserve=0
 if [[ $saved == true ]]; then
  restore final-restore && restore_rc=0 || restore_rc=$?
 elif [[ $pinned == true ]]; then
  manifest restore-tree && cmp -- "$ev/source.before.stdout" "$ev/restore-tree.stdout" && restore_rc=0 || restore_rc=$?
 fi
 if [[ $oom_before == true ]]; then
  cp -- "$memory_events" "$ev/memory.events.after" &&
   awk 'NR == FNR {before[$1]=$2; next}
    ($1 == "oom" || $1 == "oom_kill" || $1 == "oom_group_kill") && $2 != before[$1] {bad=1}
    END {exit bad}' "$ev/memory.events.before" "$ev/memory.events.after" && oom_rc=0 || oom_rc=$?
 fi
 printf '%s\n' "$oom_rc" > "$ev/oom-check.exit" || exit 95
 if (( original_rc == 0 && restore_rc == 0 && oom_rc == 0 )) && [[ $success == true ]]; then
  status=0 disposition=BUILDER_REMOVAL_CAUGHT
 fi
 printf '%s\n' "$original_rc" > "$ev/driver.original.exit" || exit 95
 printf '%s\n' "$restore_rc" > "$ev/driver.restoration.exit" || exit 95
 printf '%s\n' "$status" > "$ev/driver.exit" || exit 95
 date -u +%FT%TZ > "$ev/driver.finished" || exit 95
 # Verdict is last, after restoration, positive rerun and all receipt hashes.
 find "$ev" -maxdepth 1 -type f ! -name receipts.sha256 ! -name verdict.json -print0 |
  sort -z | xargs -0 -r sha256sum -- > "$ev/receipts.sha256" || exit 95
 jq -n --arg verdict "$disposition" --arg failed_stage "$failed_stage" \
  --arg runner_sha "$expected_runner_sha" --arg manifest_sha "$expected_manifest_sha" \
  --arg test "$test_name" --argjson exit "$status" --argjson original_exit "$original_rc" \
  --argjson restoration_exit "$restore_rc" --argjson deadline "$deadline" \
  '{verdict:$verdict,exit:$exit,original_exit:$original_exit,restoration_exit:$restoration_exit,
    last_stage:$failed_stage,test:$test,runner_sha256:$runner_sha,
    source_manifest_sha256:$manifest_sha,deadline_epoch:$deadline,
    interpretation:"execution falsifier only; INVALID never rejects an algorithm"}' > "$ev/verdict.json" || exit 95
 exit "$status"
}
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
cd -- "$source_dir"
date -u +%FT%TZ > "$ev/driver.started"
printf '%s\n' "$deadline" > "$ev/deadline.epoch"
printf '%s\n' "$source_dir" > "$ev/source-directory.txt"
sha256sum -- "$driver" > "$ev/driver.sha256"
cgroup=$(awk -F: '$1 == 0 {print $3}' /proc/self/cgroup)
memory_events=/sys/fs/cgroup$cgroup/memory.events
printf '%s\n' "$cgroup" > "$ev/cgroup.txt"
[[ -n $cgroup && -f $memory_events ]]
cp -- "$memory_events" "$ev/memory.events.before"
oom_before=true
[[ -f $source_manifest && ! -L $source_manifest ]]
printf '%s\n' "$source_manifest" > "$ev/source-manifest-path.txt"
cp -- "$source_manifest" "$ev/source.expected.sha256"
[[ $(sha256sum "$ev/source.expected.sha256" | cut -d ' ' -f1) == "$expected_manifest_sha" ]]
manifest source.before
cmp -- "$ev/source.expected.sha256" "$ev/source.before.stdout"
pinned=true
[[ -f $runner && ! -L $runner && $(stat -c %h "$runner") == 1 ]]
sha256sum -- "$runner" > "$ev/runner.original.sha256"
[[ $(cut -d ' ' -f1 "$ev/runner.original.sha256") == "$expected_runner_sha" ]]
[[ $(grep -Fxc "$anchor" "$runner") == 1 && $(grep -Fxc "$assertion" "$runner") == 1 ]]
[[ $(grep -Fxc '    async fn actual_sq8_split_config_reaches_serving_and_preserves_parity() {' "$runner") == 1 ]]
mutation_line=$(grep -nFx "$anchor" "$runner" | cut -d: -f1)
assertion_line=$(grep -nFx "$assertion" "$runner" | cut -d: -f1)
assertion_column=$(awk -v n="$assertion_line" 'NR == n {match($0, /[^[:space:]]/); print RSTART}' "$runner")
printf '%s:%s:%s\n' "$runner_rel" "$assertion_line" "$assertion_column" > "$ev/expected-panic-location.txt"
printf '%s\n' "$mutation_line" > "$ev/mutation-line.txt"
cp -- "$runner" "$ev/runner.original.rs"
cmp -- "$runner" "$ev/runner.original.rs"
saved=true
run_stage rustc-version 10 rustc -vV
run_stage cargo-version 10 cargo -vV
run_stage rustc-cfg 10 rustc --print cfg
grep -Fxq 'panic="unwind"' "$ev/rustc-cfg.stdout"

compile() {
 local phase=$1 candidate
 run_stage "$phase.compile" 3600 cargo test --locked --offline -p borsuk \
  --bin check_cohere_native_baseline --no-run --message-format=json
 # shellcheck disable=SC2016
 run_stage "$phase.artifact" 10 jq -se --arg runner "$runner" '
  [.[] | select(.reason == "compiler-artifact" and .profile.test == true
    and .target.name == "check_cohere_native_baseline" and .target.src_path == $runner
    and (.target.kind | index("bin")) != null and (.executable | type) == "string")]
  | if length == 1 and .[0].fresh == false then .[0].executable
    else error("expected one freshly compiled bin-test ELF") end
 ' "$ev/$phase.compile.stdout"
 candidate=$(jq -er . "$ev/$phase.artifact.stdout")
 [[ -f $candidate && -x $candidate && ! -L $candidate &&
    $(realpath -e -- "$candidate") == "$candidate" && $candidate == "$CARGO_TARGET_DIR/debug/deps/"* ]]
 binary=$candidate
 printf '%s\n' "$binary" > "$ev/$phase.executable.txt"
 run_stage "$phase.elf" 10 readelf -h "$binary"
 run_stage "$phase.elf-sha" 10 sha256sum -- "$binary"
 run_stage "$phase.inventory" 10 "$binary" --list --exact "$test_name" --color never
 [[ $(grep -c ': test$' "$ev/$phase.inventory.stdout") == 1 ]]
 grep -Fxq "$test_name: test" "$ev/$phase.inventory.stdout"
 grep -Fxq '1 test, 0 benchmarks' "$ev/$phase.inventory.stdout"
}

exact_test() {
 local phase=$1 expected_rc=$2 rc
 run_stage "$phase.test" 300 "$binary" --exact "$test_name" --test-threads=1 --nocapture --color never && rc=0 || rc=$?
 [[ $rc == "$expected_rc" && $(cat "$ev/$phase.test.original.exit") == "$expected_rc" &&
    $(cat "$ev/$phase.test.timeout.exit") == "$expected_rc" ]]
 grep -Fxq 'running 1 test' "$ev/$phase.test.stdout"
 if (( expected_rc == 0 )); then
  grep -Eq '^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured;' "$ev/$phase.test.stdout"
 else
  grep -Eq '^test result: FAILED\. 0 passed; 1 failed; 0 ignored; 0 measured;' "$ev/$phase.test.stdout"
  [[ $(grep -c ' panicked at ' "$ev/$phase.test.stderr") == 1 ]]
  awk -v name="$test_name" -v rel="$runner_rel:$assertion_line:$assertion_column:" \
   -v abs="$runner:$assertion_line:$assertion_column:" '
   index($0, "thread \047" name "\047") == 1 &&
     (substr($0, length($0)-length(rel)+1) == rel || substr($0, length($0)-length(abs)+1) == abs) {
    if ((getline) == 1 && $0 == "assertion `left == right` failed" &&
        (getline) == 1 && $0 == "  left: 1" && (getline) == 1 && $0 == " right: 5") found++
   }
   END {exit found != 1}
  ' "$ev/$phase.test.stderr"
 fi
 run_stage "$phase.elf-after" 10 sha256sum --strict --check "$ev/$phase.elf-sha.stdout"
}

compile baseline
exact_test baseline 0
manifest baseline-source
cmp -- "$ev/source.before.stdout" "$ev/baseline-source.stdout"
current_stage=mutation
awk -v anchor="$anchor" -v replacement="$replacement" '
 $0 == anchor {printf "%-*s\n", length($0), replacement; next} {print}
' "$ev/runner.original.rs" > "$ev/runner.mutant.rs"
[[ $(wc -c < "$ev/runner.original.rs") == "$(wc -c < "$ev/runner.mutant.rs")" ]]
if diff -u -- "$ev/runner.original.rs" "$ev/runner.mutant.rs" > "$ev/mutation.diff"; then
 exit 95
else
 [[ $? == 1 ]]
fi
cp -- "$ev/runner.mutant.rs" "$runner"
sha256sum -- "$runner" > "$ev/runner.mutant.sha256"
manifest mutant-source
awk -v path="$runner_rel" -v original="$expected_runner_sha" \
 -v sha="$(cut -d ' ' -f1 "$ev/runner.mutant.sha256")" '
 $0 == original "  " path {$0 = sha "  " path} {print}
' "$ev/source.before.stdout" > "$ev/source.mutant.expected.sha256"
cmp -- "$ev/source.mutant.expected.sha256" "$ev/mutant-source.stdout"
compile mutant
exact_test mutant 101
manifest mutant-source-after
cmp -- "$ev/source.mutant.expected.sha256" "$ev/mutant-source-after.stdout"
restore before-positive-restore
compile restored
exact_test restored 0
success=true
