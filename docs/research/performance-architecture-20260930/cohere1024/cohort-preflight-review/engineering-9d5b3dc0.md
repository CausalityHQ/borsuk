# Cohere preflight source engineering review

Reviewed source only: preparer `9d5b3dc0d1af31ccad32e82d0a83fd73967ee43e`, runner `d869e786e3c343d2c2ebf382d4ba7f0807a7be29`.

Consultation `e3248968663947ba`, dual group `b5fcb1f9b2b24920`. This is the completed engineering critic, not the still-running research critic. Both binaries remain native UNVERIFIED; root directed the same preparer worker to repair the two concrete findings in a separate commit, keeping its eight test names. Original library qualification a0002 is separately closed and qualified.

**HOLD: two blocking preparer defects.** Both reviewed files match their specified commits and contract hashes.

1. **P1 — Predecode allocation admission undercounts dictionary-expanded IDs.**  
   [Preparer:564](/home/rb/worktrees/borsuk-cohere1024-native-cohort-preparer/crates/borsuk/src/bin/prepare_cohere_native_cohort.rs:564) charges `id_uncompressed` once. A dictionary-encoded string can occupy those bytes once yet be repeated across every decoded row. For example, a 64 MiB string repeated across a 128-row batch requires 8 GiB of UTF-8 payload, while the admitted metadata estimate remains much smaller. The batch-memory and individual-ID checks run **after** Arrow constructs the batch, at lines 984–1006; they cannot prevent that allocation.

   **Minimum repair:** conservatively bound expanded ID payload before building the reader—for example, checked `batch_rows * id_uncompressed`, plus offsets—and include it in batch/memory admission. Add a small dictionary-expansion case that must fail during metadata admission. Existing cap tests merely lower limits on ordinary fixtures.

2. **P1 — Temporary source mutation can produce a falsely authenticated cohort.**  
   [Preparer:356](/home/rb/worktrees/borsuk-cohere1024-native-cohort-preparer/crates/borsuk/src/bin/prepare_cohere_native_cohort.rs:356) authenticates length and SHA, but retains no modification/change timestamps across decoding. The final authentication at lines 1070–1076 only rehashes the current bytes. Consequently, `authenticate A → decode modified B → restore A → authenticate A` can produce `COMPLETE` with vectors or IDs from B while recording A’s source hash.

   **Minimum repair:** capture descriptor identity, length, mtime and ctime before initial authentication; require them unchanged after authentication, through decoding and at final authentication. The runner already contains this identity-checking pattern. Add a mutate-and-restore failure case; current tests cover wrong hashes and changed lengths, not this sequence.

The preparer’s original-f32 preservation, row selection, scalar exhaustive cosine/tie handling, and normal marker synchronization sequence look consistent. The runner contains actual native-pipeline test paths, independent literal oracles, request/prefix reauthentication, three-plane charge summation and fixed-denominator underfill checks. I found no additional concrete runner blocker within the permitted files; underlying library memory-accounting guarantees remain outside this inspection.

**Compilation, Clippy, native tests and tiny-pipeline execution: UNVERIFIED.** No files changed, jobs launched, or live qualification touched.

