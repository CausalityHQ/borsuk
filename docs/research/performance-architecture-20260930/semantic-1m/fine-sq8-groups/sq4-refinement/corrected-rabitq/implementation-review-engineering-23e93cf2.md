# Corrected four-bit implementation engineering review

Exact source: `23e93cf251c82ba1b155189d72ca372ab7433d31`.

Consultation `dbd613eeac254d1e`, group `cdd27a4f71294272`; original completed exit 0. Reviewer gpt-6-astra. Read-only static review; no native qualification.

**Engineering critic: HOLD release/freeze of `23e93cf251c82ba1b155189d72ca372ab7433d31`.** Reviewed against `128a27266091945b65e93a67d683194cd977c7d9`, including the prior method critiques. I established no P0/P1 implementation defect through static inspection. Native qualification remains outstanding.

1. **P2 — The unchanged SQ8 control loses its rank-100/101 boundary evidence.**  
   The corrected path calls the existing scorer at [fine_sq8_groups.rs:3821](/home/rb/worktrees/borsuk-corrected-fourbit-native-codec/crates/borsuk/src/fine_sq8_groups.rs:3821). That scorer truncates to 100 results; its boundary metadata is emitted only when `plane.histogram` exists, whereas corrected construction explicitly sets it to `None`. Candidate and decoded-cosine gaps therefore have no corresponding SQ8 gap for comparison.

   **Falsifying check:** the existing 135-row fixture should require independently calculated rank-100/101 score bits and their gap for all three controls. SQ8 currently lacks these fields. Its test only checks roster and result count; the ranking-oracle loop excludes SQ8 at [CLI test:2128](/home/rb/worktrees/borsuk-corrected-fourbit-native-codec/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:2128).

   **Smallest same-worker repair:** capture the SQ8 boundary before truncation without changing its scoring kernel, and extend this fixture’s scalar oracle to SQ8.

2. **P2 — Corrected freeze and terminal sync failures are untested.**  
   The new failure test injects only `fail_sync = Some(1)` at [fine_sq8_groups.rs:7325](/home/rb/worktrees/borsuk-corrected-fourbit-native-codec/crates/borsuk/src/fine_sq8_groups.rs:7325): the first rotation-file sync. It never reaches the corrected freeze or terminal sync. The pretruth corruption hooks run after successful freeze publication and cannot establish those failure guarantees.

   **Falsifying fixture:** inject failure at the freeze’s file and directory syncs, then at terminal sync. Freeze failures must return an error, record INVALID, and leave truth unopened. Terminal failure must remain unsuccessful even if best-effort INVALID rewriting succeeds.

   **Smallest same-worker repair:** extend the existing fault-injection test to those publication points. This is a coverage gap, not an observed incorrect recovery path.

The principal earlier requirements are present in the source: separate f32 source reconstruction, reciprocal correction, exact dyadic threshold comparison, winning-prefix replay, persisted matrix authentication and aggregate Gram certificate, full-batch query admission before construction, and inclusion of the new module in source identity. The hard-coded population digest and byte count match `closed-populations.json`. Ordered intervals and fetched-ID hashes are checked; all 128 results and the persisted closure are authenticated before truth opens.

**Release evidence remains insufficient.** The nine tests are authored but unrun; compilation, required Clippy, and `scripts/check_rust_test_build.sh` have no passing evidence here. The same worker should address the two gaps and qualify the resulting exact revision on an authorized build host before source-bound admission and canary execution.

The historical histogram REJECT remains unchanged. No corrected recall, lifecycle, scalability, latency/QPS, or vendor-comparison result is established.

No files edited, Cargo/native execution, corpus/query/GT reads, cloud actions, or additional reviews launched. `git diff --check` passed; that is not compilation evidence.

