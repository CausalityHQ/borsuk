What this change does: `--derive` composes the unchanged normalization, ordinary flat fitting, and SQ8 APIs. Receipt v2 binds the corpus independently of query count, so its validation logic supports reuse across Q32/Q1000 with identical corpus bytes and intervals.

Reviewed exact commit `5098c6aaaa4af9a7e1e9dbc6c6dbc4a21efaf2e1` against its stated parent. Findings are limited to this delta.

1. **[P2] Cleanup can hide the original publication failure.**  
   At [build_sq8_source.rs:711](/home/rb/worktrees/borsuk-scale-cohort-streaming-native/crates/borsuk/examples/build_sq8_source.rs:711), final directory/parent synchronization failure enters revocation, but either subsequent `?` replaces that original error. An unlink failure also leaves a parseable `COMPLETE` receipt; failed synchronization of the unlink leaves its durability uncertain. The process still fails, so this is not an exit-zero bypass.

   **Minimal fix:** retain the original synchronization error and attach both cleanup outcomes. Explicitly report uncertain revocation. Add deterministic fault injection after receipt installation covering directory sync, parent sync, unlink, and revocation sync. The current hooks/tests stop before this branch and cannot establish its behavior. Acceptance must require the **original derivation process exit 0**, regardless of any surviving receipt.

2. **[P2 — test gap] No test connects an actual producer receipt to the generation consumer.**  
   The [actual-chain test](/home/rb/worktrees/borsuk-scale-cohort-streaming-native/crates/borsuk/examples/build_sq8_source.rs:746) exercises real APIs, but only four D2 rows and stops at producer output. The [baseline receipt fixture](/home/rb/worktrees/borsuk-scale-cohort-streaming-native/crates/borsuk/src/bin/check_cohere_native_baseline.rs:2766) fabricates receipt v2, authority, and calibration. Consequently, the Q32/Q1000 reuse and calibration tests can pass despite a future producer/consumer serialization mismatch.

   **Minimal fix:** add one bounded D1024 integration fixture that consumes unmodified `--derive` output, builds a generation using those exact calibration bits, and validates the receipt/generation binding. Keep one negative case changing a valid calibration coefficient by one bit. This is a missing integration check, not an observed runtime failure or a substitute for actual-input parity.

3. **[P3] Reducer rejection directs operators to the wrong schema.**  
   [compare_native_replay.rs:1923](/home/rb/worktrees/borsuk-scale-cohort-streaming-native/crates/borsuk/examples/compare_native_replay.rs:1923) says “scale reduction config v3 required,” while the accepted schema is now v4.

   **Minimal fix:** update the diagnostic and assert it in the obsolete-schema rejection test.

The main composition looks correct by source inspection:

- All four bodies receive full seals; fitted order and SQ8 outputs are independently rehashed.
- Calibration comparison uses exact f32 bits against authenticated manifest bytes, before generation execution.
- Query/truth fields are rejected by the derivation config.
- Payload accounting includes the retained order, and scratch accounting includes partial/pending outputs.
- Consumer authority reaches the reducer’s exact frozen expected-input comparison. Reports retain `qualified:false` and no performance-win claim.

These remain **external prerequisites**, not receipt-certified facts: immutable inputs and directory namespace; independently frozen executable/build provenance; original process exit status; enforced RSS, filesystem capacity, and resource closure. In particular, `compiled_authority` passes through the configured commit and embeds three source-file hashes—it does not independently prove the complete compiled dependency/source closure.

**Verdict:** fix finding 1 before accepting derivation results; close finding 2 before treating the producer-to-consumer path as tested. No concrete compile/API mismatch found by inspection. `git diff --check` passed; no builds or native tests were run. The seven remote gates, actual-input parity, and 1M cold benchmark remain pending.
