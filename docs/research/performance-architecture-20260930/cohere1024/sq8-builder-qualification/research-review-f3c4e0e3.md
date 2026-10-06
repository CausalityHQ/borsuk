## Verdict: hold for one fix. After that it can go, with no other code changes.

The new mode itself is sound: the build command, exit/tee gating, resources, cgroup, ACK, terminate→wait→collect, archive and historical-mode handling all check out. But the code never enforces the claim that the native source is "already qualified", and an unpushed commit that changes native code is being qualified right now.

I made no edits and ran no compiler, cloud, or job commands. I ran only read-only `git` and `ps` commands. Line numbers refer to `scripts/launch_native_workspace_execution_spot.py` at f3c4e0e3 unless another file is named.

### Required before the root freezes

**1. The native source identity is not pinned to the qualified receipt.**
- **What the code checks:** at line 1252, `qualify` only checks `native_source_commit == control_native_source_commit`, and the root writes both values. The `SOURCE_IDENTITY` checks at lines 1265, 1598 and 1743 are all skipped because `SEMANTIC_1M` is set. The N=K tree check in preflight (lines 1309–1323) only compares the tree against whatever N the root wrote.
- **What it should check:** the contract and the qualified reference (`cohort-implementation-gates/a0001/verification.json`, sha `3ba61026…`, NATIVE_QUALIFIED) both name identity `009119a7…` with 411 files. Nothing in the code enforces that.
- **Why it matters now:** the live job `--cohere1024-implementation a0003` is running at commit c0936ebe, which changes `two_bit_generation.rs` (+95 lines) and `semantic_unit_router.rs`. If that lands on main before the freeze commit K, a root manifest with N = control = that commit passes every check. The paid run would then build from source that was qualified differently, and nothing would flag it.
- **Smallest fix:**
  - Add the constant `COHERE_SQ8_BUILDER_IDENTITY = '009119a720892f06139fd717f6e55ae9e3b865ebb3667b143a966002ef734e64'`, following the `STARTUP_WAVE8_IDENTITY` pattern at line 675.
  - At line 1252, add `assert manifest['source_identity_sha256'] == COHERE_SQ8_BUILDER_IDENTITY`.
  - Optionally add the same check in `validate_receipt` (line 1598) so replays are bound too.
  - The self-check fixture (lines 2988–3017) uses a synthetic inventory. It needs `patch.object(module, 'COHERE_SQ8_BUILDER_IDENTITY', worker.source_identity(inventory))` plus one case that must be rejected.

### Should fix (small)

**2. The qualified commit 8a5e0594 is not reachable from any ref.** `git for-each-ref --contains 8a5e0594` returns 0, so that commit is on no local branch and not on origin. The preflight `ls-tree N` at line 1311 depends on an object that garbage collection could eventually remove. 2941dfc9 (on origin/main) has the identical 411-file native tree, which I verified blob for blob, as does origin/main itself. Either:
- set N to 2941dfc9, or
- require N to be on an origin ref, mirroring the check at line 1304.

With fix 1 in place, the identity check carries the real guarantee whichever you choose.

### Lifecycle note (not a code defect)

**3. The shared launch lock is held by the live a0003 controller.** That controller is PID 469836, running from `/tmp/borsuk-cohere1024-qualification-a0003`. Any sq8 launch now fails closed at `flock(LOCK_NB)` (line 4167) before any AWS call, which is correct. The root has to wait for that controller to finish collecting and must not remove the lock. That checkout is separate from the sq8 worktree, so freezing K/F/B does not touch it.

### Checked and fine

- **Command:** the single `run_stage` line (`check_cohere_sq8_builder.sh:46`) is checked against `COHERE_SQ8_BUILDER_STAGES` (line 502) by the recorder's exact-argv assert. Clippy install is skipped only for this mode. `--locked` and a fresh target directory outside the repo are kept.
- **Binary:** the output is `target/release/examples/build_sq8_source`, copied with ELF, size and identity checks. `[[example]]` has no `required-features`. The example also compiles the dev-dependencies (criterion, axum, tower) in release mode, but those were already compiled in check/test mode at N.
- **Resources:** the previous qualification's release stage took 13m28s with 4.36 GB max RSS and no OOM at the same 8 GiB / 200% / 512 tasks / jobs=1. That leaves plenty of room inside 7200 s. Its `memory.peak` of 8 GiB is page cache.
- **Freeze ancestry:** clean B, F with K as its only parent and only CONFIG changed, F on origin, empty B, and committed archive/support authority. The archive covers everything outside `docs/research/`. The preflight native-file filter matches `source_hashes` in `check_native_startup_build.py`.
- **Historical modes:** every new branch is gated on the sq8 flag except one: the test-only `'wait-failure'` case added to the shared `_lifecycle_self_check` (line 2017). There are no main-branch changes to these files since 2941dfc9, and `git merge-tree` against origin/main is clean.
- **Missing evidence:** the root ran only the sq8 self-check. One historical self-check, for example `--cohere-cohort-implementation --self-check`, would show the shared self-check changes didn't break the older modes.

I left LIVE exec12009 / i-07515496bdaec3142 and the a0003 controller untouched. I also saved a short memory note on these findings.
