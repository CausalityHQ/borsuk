#!/usr/bin/env bash
set -euo pipefail
cd /home/rb/worktrees/borsuk-prod-ready-v9
out=/data/target/borsuk-leaf-metric-coverage.pending
source=$(cat "$out/candidate.commit")
test "$source" = 240a570dff26daa7ec556159c9f9ecd370ed0dce
: > "$out/source-git-match.log"
while read -r expected path; do actual=$(git show "$source:$path" | sha256sum); test "${actual%% *}" = "$expected"; printf '%s  %s\n' "$expected" "$path" >> "$out/source-git-match.log"; done < "$out/source-files.sha256"
cmp "$out/source-files.sha256" "$out/source-git-match.log"
test "$(wc -l < "$out/source-git-match.log")" = 463
jq -e 'length==430' "$out/native-source.json" > /dev/null
for dir in tests-r3-collected sealed-loss-r3-collected leaf-metric-release-r3-collected leaf-metric-clippy-r3-collected leaf-metric-test-build-r3-collected; do
 (cd "$out/$dir" && sha256sum -c SHA256SUMS) > "$out/$dir.verified.log"
 cmp "$out/source-expected-current.log" "$out/$dir/source-before.log"
 cmp "$out/source-expected-current.log" "$out/$dir/source-after.log"
 jq -e --arg source "$source" '.source_commit==$source and .manager_exit==0 and .native_exit==0 and .owned_cgroup_drained==true' "$out/$dir/terminal.json" > /dev/null
 test "$(cat "$out/$dir/cpu.max.before")" = '400000 100000'
 test "$(cat "$out/$dir/cpuset.cpus.effective.before")" = '0-3'
 test "$(cat "$out/$dir/memory.max.before")" = 8589934592
 test "$(cat "$out/$dir/memory.swap.max.before")" = 0
 test "$(cat "$out/$dir/pids.max.before")" = 512
 grep -qx 'oom 0' "$out/$dir/memory.events.after"
 grep -qx 'oom_kill 0' "$out/$dir/memory.events.after"
 test "$(cat "$out/$dir/memory.swap.peak.after")" = 0
done
grep -qx 'env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=2 CARGO_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh' "$out/leaf-metric-test-build-r3-collected/native-command.txt"
grep -q '^rust-test-build status=0 .* jobs=2$' "$out/leaf-metric-test-build-r3-collected/native.log"
grep -q '^test result: ok. 8 passed; 0 failed; 0 ignored;' "$out/tests-r3-collected/native.log"
grep -q '^test result: ok. 4 passed; 0 failed; 0 ignored;' "$out/sealed-loss-r3-collected/native.log"
jq -e '.Status=="Success" and .ResponseCode==0' "$out/remaining-r3.ssm-terminal.json" > /dev/null
jq -e '.Reservations[0].Instances[0].InstanceId=="i-0defaf8a335f362fd" and .Reservations[0].Instances[0].State.Name=="terminated"' "$out/instance.terminated.json" > /dev/null
test "$(cat "$out/terminate-wait.exit")" = 0
test "$(cat "$out/root-volume-after.exit")" = 254
grep -q 'InvalidVolume.NotFound' "$out/root-volume-after.stderr"
grep -q 'vol-08e774940109dda3a' "$out/root-volume-after.stderr"
grep -qx 'ActiveState=inactive' "$out/external-timer.closed.txt"
printf '%s\n' 'ALL_FIVE_GATES_AUTHENTICATED_SOURCE463_NATIVE430_TERMINATED_VOLUME_DELETED'
