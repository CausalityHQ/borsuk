# a0002: split-builder mutation sensitivity gate passed

Frozen native source8e7076951936b0434b7947afd5b23e192c98fe70, original source-bound probe38b3a919/83df03d1 unchanged. Repaired outer wrapper576ce2b1a282990336eeac0ed65d8149191ca1b4 (SHA8ee3aab70d6a2b13bf87da7698641c03cfb1009d5b856fabf4d70e22958b55b6). Original command32289c66-2396-43d1-9347-b5499a545f91 on Spoti-0fa657acfbd424555 finished Success0.

- Baseline: freshly compiled exact test selected1, passed1, exit0.
- Change: remove exactly the production split-builder application, with padding preserving source positions; no algorithm/test assertion changes.
- Mutant: freshly compiled same test selected1, failed1, original/timeout/time exit101; exact span assertion at check_cohere_native_baseline.rs:4228:25, left1/right5.
- Restore: every original source byte restored; freshly compiled same test selected1, passed1, exit0. Full480-file manifests equal the independently pinned original before baseline and after both restorations. Mutant manifests equal the independently constructed one-path change before and after the failure.
- Original wrapper/probe/publication/shutdown request all exit0. Root independently verified all endpoints, source/runtime/whole-archive pins, actual compiler artifacts and resource snapshots.
- Initial live unit witness binds Description nonce, InvocationID8149d75f09a34188a1e4876f355c2a2d and exact cgroup. Final successful active/exited status retains that owner/invocation; empty cgroup property is accompanied by independent absent/empty subtree evidence. Stop completes with unit not-found/inactive and expected subtree absent; no null status is used to claim the probe ran.
- CPU16/Allowed0-15/56GiB/noSwap/pids512 verified before/after; OOM0. Exact instance terminated, vol-06f9e1fd2c126acc0 absent254, external timer inactive; original watcher11037 exit0/BGoff.

Original a0001 remains outer executionINVALID95. Its inner0/101/0 datapoints and original empty-ControlGroup record remain retained, not rewritten. The new wrapper repairs only observed status/ownership retention and stop evidence, preserving scientific/algorithm/assertion limits.

Archive: s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261016/sq8-builder-mutation-a0002/terminal/evidence.tar.gz SHA2556f6cd0cb30c9808deaf9443eb8119854721576c60d984994434ea7084b207. The source-stage UNVERIFIED label in the immutable script/worker contract describes the original handoff; this later root record qualifies only the executed coupling/ownership lifecycle.

This closes the external builder-removal correctness prerequisite. It does not measure ANN performance, real-input quality or competitor throughput. Still required before scientific six cells: real-input admission, separate disposable staging/canary smoke, native-validated disabled/historical parity, independent final headers/ELFs/protocol freeze and explicit prospective performance admission. No new scientific run was started by this closeout.
