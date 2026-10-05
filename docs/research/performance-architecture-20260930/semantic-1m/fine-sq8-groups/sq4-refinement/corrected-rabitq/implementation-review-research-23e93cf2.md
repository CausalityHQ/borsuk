# Corrected four-bit implementation research review

Exact source: `23e93cf251c82ba1b155189d72ca372ab7433d31`.

Consultation `1f479f27fe1845c3`, group `cdd27a4f71294272`; original completed exit0. Opus authentication failed; completed reviewer was GPT-6.1 Sol. Read-only static review; no native qualification.

**Independent research critic: HOLD `23e93cf2` for qualification.** I found one definite P1 blocker; no demonstrated P0 or estimator error. Findings refer to the exact committed source.

1. **P1 — the real-builder binary test cannot compile.** [hierarchical_semantic_cells.rs:2077](/home/rb/worktrees/borsuk-corrected-fourbit-native-codec/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:2077) calls `borsuk::sq8_source::cosine_vector`, declared `pub(crate)` in [sq8_source.rs:22](/home/rb/worktrees/borsuk-corrected-fourbit-native-codec/crates/borsuk/src/sq8_source.rs:22). The binary is a separate crate, so this violates Rust visibility and blocks binary test compilation, all-target Clippy, and workspace test-build qualification.

   **Falsifier:** compile `cargo test --locked -p borsuk --bin hierarchical_semantic_cells --no-run` on this revision; expect E0603. This was **not run here**.

   **Smallest same-worker repair:** replace that call with the existing public `borsuk::two_bit_generation::normalize_two_bit_diagnostic_query(&query)`. It forwards to the exact same normalizer; no API expansion is needed.

The real fixture has two evidence weaknesses, separate from that compilation defect:

- **“Incidental winner” is not established.** [Line 2113](/home/rb/worktrees/borsuk-corrected-fourbit-native-codec/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:2113) checks that some top-100 row is outside nine nominees. That necessarily holds for any 100 distinct results. Changing the query to follow a nominated row still satisfies it. Assert that an independently determined rank-one winner is incidental and appears at the expected rank.
- **The third control lacks a score oracle.** [Lines 2124–2128](/home/rb/worktrees/borsuk-corrected-fourbit-native-codec/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:2124) check SQ8 population and result count, while rank/score comparisons cover only corrected and decoded cosine. Substituting arbitrary SQ8 reference scores would escape these assertions. Compare all SQ8 top-100 IDs and score bits against the unchanged native scorer on the same decoded query and source rows.

Source inspection supports the intended estimator and integration: `λ=1/(u·z)` and unclamped `2−2λ(v·z)` are coherent; exact dyadic threshold ordering, true-tie batching, winning-prefix replay, persisted rotation bits, the column-Gram certificate, and new-module source binding address the prior method findings. The strict population digest matches the supplied authority. Full-batch admission precedes construction, and complete results and payload closure are authenticated before truth acquisition.

**This supports continued qualification, not release or recall acceptance.** Repair the same worker, freeze the resulting revision, and collect actual affected-test, Clippy, and workspace test-build results before the source-bound admission and disposable canary. Nine authored tests remain unrun. The historical histogram REJECT remains unchanged; correction removes radial error algebraically but leaves directional ranking error. Dense rotation and the 396-byte layout still have no qualified lifecycle, scalability, cold-latency, QPS, or vendor-win evidence.

No files edited, builds/native/data/GT/cloud work performed, or additional reviewers launched.

