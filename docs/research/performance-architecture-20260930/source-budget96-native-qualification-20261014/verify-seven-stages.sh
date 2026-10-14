#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-source-budget96.pending/launch-header-window-green-a0001
source=8910ef50c1cce155eb396d71a2e05dd3b9e906a0
[[ $(cat "$out/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
jq -e '.Status=="Success" and .ResponseCode==0 and .CommandId=="3765c057-fdfd-4231-b429-d008f078b461" and .InstanceId=="i-02fb16ed561141e40"' "$out/run-terminal-observation.json" >/dev/null
count=0
while IFS= read -r stage; do
 ev=$out/$stage-collected
 (cd "$ev" && sha256sum --strict --check SHA256SUMS) > "$ev/independent-manifest-check.log"
 jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
 cmp "$ev/source-before.log" "$ev/source-after.log"
 [[ $(wc -l < "$ev/source-before.log") == 470 ]]
 [[ $(cat "$ev/cpu.max.before") == '400000 100000' && $(cat "$ev/cpuset.cpus.effective.before") == 0-3 && $(cat "$ev/memory.max.before") == 15032385536 && $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 512 && $(cat "$ev/memory.swap.peak.after") == 0 ]]
 grep -qx 'oom_kill 0' "$ev/memory.events.after"
 case $stage in
 abba-test|abba-refusals|example-regressions)
  n=$(grep -c ': test$' "$ev/native-list.log")
  [[ $(grep -Ec "^test result: ok[.] $n passed; 0 failed; 0 ignored; 0 measured;" "$ev/native.log") == 1 ]]
  while IFS= read -r name; do [[ $(grep -Fxc "test $name ... ok" "$ev/native.log") == 1 ]]; done < <(sed -n 's/: test$//p' "$ev/native-list.log")
  if [[ $stage == example-regressions ]]; then [[ $n == 198 ]]; else
   [[ $n == 1 ]]
   name=source_budget_abba_replays_distinct_cover_with_identical_quality_and_sq8
   [[ $stage == abba-test ]] || name=source_budget_abba_refuses_config_drift_before_results_and_wrong_late_cover
   grep -Fqx "tests::$name: test" "$ev/native-list.log"
  fi;;
 workspace-clippy) grep -F 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious' "$ev/native-command.txt" >/dev/null;;
 workspace-test-build) grep -F 'env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh' "$ev/native-command.txt" >/dev/null;;
 esac
 count=$((count+1))
 printf '%s\tVERIFIED\n' "$stage"
done < /data/target/borsuk-source-budget96.pending/assets-header-window-green/stage-roster.txt
[[ $count == 7 ]]
printf '%s\n' SEVEN_NATIVE_GATES_EXACT_TESTS_SOURCE_RESOURCES_AND_COMPUTE_CLOSURE_VERIFIED > "$out/independent-verification.txt"
cat "$out/independent-verification.txt"
