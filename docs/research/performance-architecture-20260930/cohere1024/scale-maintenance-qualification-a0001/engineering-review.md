# Review 2073fefb6d264b68
Candidate a07b979286293fdf5e6e205302b1808de3e09b84
Reviewer cf22f13191574f58

Reviewed committed `a07b979286293fdf5e6e205302b1808de3e09b84` against `344740c89b696407518f62e3c1470ff56e722f71`. These are source findings; no code or tests were executed.

**Three blockers:**

1. **[P1] Crash recovery can exceed the admitted scratch cap.**  
   [canonical_source.rs:389](/home/rb/worktrees/borsuk-scale-profile-maintenance-native/crates/borsuk/src/canonical_source.rs:389) creates temporary staging beneath the job directory. Process death leaves that directory behind. Retry cleanup at [two_bit_compaction.rs:730](/home/rb/worktrees/borsuk-scale-profile-maintenance-native/crates/borsuk/src/two_bit_compaction.rs:730) removes only `input`, `generation`, and `sq8.bin`, then admits another complete preparation.

   At 1M/D1024, abandoned canonical plus merged raw/IDs can retain approximately **8.208 GB**. Repeating preparation can therefore exceed the **11.052 GB** disk model before generation construction. This is an inherited defect in the newly admitted lifecycle.

   **Smallest fix:** give preparation a recognized, job-owned staging location and reclaim it under the existing lock before retry allocation. Add an orphan-staging recovery fixture; normal error unwinding does not simulate process death.

2. **[P1] Newly enabled Fresh1m maintenance can seal writes into an impossible target.**  
   For a Fresh1m base, deleting one existing row leaves `upper_rows == 1_000_000`, so [preflight passes and sealing occurs](/home/rb/worktrees/borsuk-scale-profile-maintenance-native/crates/borsuk/src/two_bit_compaction.rs:609). The actual 999,999-row output then fails [exact build admission](/home/rb/worktrees/borsuk-scale-profile-maintenance-native/crates/borsuk/src/two_bit_compaction.rs:779), because Fresh1m requires exactly one million rows. Writes remain sealed. Retrying explicitly with the valid Scale1m profile is rejected by the [captured-profile check](/home/rb/worktrees/borsuk-scale-profile-maintenance-native/crates/borsuk/src/two_bit_compaction.rs:551).

   **Smallest scoped fix:** retain the early Fresh1m maintenance refusal unless the caller explicitly selects a compatible target profile before starting. Otherwise, exact population admission must precede irreversible sealing. Add a refusal fixture that verifies mutations remain writable.

3. **[P2] The earlier disk check invalidates an existing recovery test.**  
   [semantic_object_store_parity](/home/rb/worktrees/borsuk-scale-profile-maintenance-native/crates/borsuk/src/two_bit_generation.rs:5672) deliberately supplies a 262,144-byte disk cap, then asserts prepared input exists. The new [prospective disk check](/home/rb/worktrees/borsuk-scale-profile-maintenance-native/crates/borsuk/src/two_bit_compaction.rs:599) rejects before creating that job. The assertion at line 5690 is therefore inconsistent with the candidate’s control flow.

   **Smallest fix:** inject a failure after preparation to preserve the intended unready-job recovery coverage. Explicitly run:
   `cargo test --locked -p borsuk --lib two_bit_generation::source_walk_tests::semantic_object_store_parity`  
   The listed `--test two_bit_generation` command runs a different test target.

**Nonblocking observations:** The metadata-prefix correction and remaining mutation-budget clamp are present. Profile identity is checked across binary roots, empty roots, journals, and prepared targets. I found no new authentication bypass in those paths. Historical Native100k/Fresh1m selection arithmetic remains unchanged.

**Verdict: hold for these fixes and root-owned qualification.** Compiler, runtime, RSS, recall, and performance remain unverified.

