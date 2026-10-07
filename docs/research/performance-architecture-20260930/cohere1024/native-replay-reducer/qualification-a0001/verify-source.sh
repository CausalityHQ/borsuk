#!/bin/bash
set -euo pipefail
cd /home/rb/worktrees/borsuk-prod-ready-v9
base=$(/usr/bin/git rev-parse HEAD)
test "$base" = 750487e825f9e11f4a9ec212379066577749755f
export GIT_INDEX_FILE=/tmp/borsuk-reducer-integration/private-index
/usr/bin/git -c core.preloadIndex=false read-tree "$base"
for path in crates/borsuk/Cargo.toml crates/borsuk/examples/compare_native_replay.rs; do
  blob=$(/usr/bin/git rev-parse "8bc3d18911e62a73e9a9edcb4cd61e47baa27caf:$path")
  /usr/bin/git update-index --add --cacheinfo "100644,$blob,$path"
done
tree=$(/usr/bin/git write-tree)
printf '%s\n' "$tree" > /tmp/borsuk-reducer-integration/native-tree
while IFS=$'\t' read -r path expected; do
  actual=$(/usr/bin/git show "$tree:$path" | sha256sum | cut -d' ' -f1)
  test "$actual" = "$expected"
  printf '%s\t%s\n' "$path" "$actual"
done < /tmp/borsuk-reducer-gates-a0001/native-source.tsv > /tmp/borsuk-reducer-integration/native-source-verified.tsv
test "$(wc -l < /tmp/borsuk-reducer-integration/native-source-verified.tsv)" = 414
jq -cS . /tmp/borsuk-reducer-gates-a0001/collected/native-source.json | tr -d '\n' | sha256sum > /tmp/borsuk-reducer-integration/native-identity-check.txt
test "$(cut -d' ' -f1 /tmp/borsuk-reducer-integration/native-identity-check.txt)" = 70c4080b954ff4b23f3cb026712763f9f1abdc2dfc923df8384b7a0d3753ea02
for stage in reducer-tests reducer-release workspace-clippy workspace-test-build; do
  test "$(cat "/tmp/borsuk-reducer-gates-a0001/collected/$stage.native-exit")" = 0
  test "$(cat "/tmp/borsuk-reducer-gates-a0001/collected/$stage.tee-exit")" = 0
done
for name in nearest_rank_and_sequential_qps_literal_golden four_authenticated_native_runs_and_completed_no_win invalid_tampered_out_of_order_and_semantic_mismatch bounded_files_reject_links_fifo_oversize_and_duplicate_evidence; do
  test "$(grep -Fxc "test tests::$name ... ok" /tmp/borsuk-reducer-gates-a0001/collected/reducer-tests.log)" = 1
done
grep -Fq 'test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;' /tmp/borsuk-reducer-gates-a0001/collected/reducer-tests.log
grep -Fq 'Executable unittests examples/compare_native_replay.rs' /tmp/borsuk-reducer-gates-a0001/collected/workspace-test-build.log
grep -Fq 'rust-test-build status=0' /tmp/borsuk-reducer-gates-a0001/collected/workspace-test-build.log
printf 'SOURCE414_GATES4_TESTS4_VERIFIED\n'
