## Verdict

**GO for the bounded remote compile and full-execution gate.** I found no Rust correctness defect in the decoder, allocation admission, persisted identity/profile, source-order binding or Fresh1m maintenance rejection. All references below are to `98ff6e5e`.

**Do not spend the fresh ReLAION 1M panel until items B1–B3 are done.** None of them is a format or architecture problem. They decide whether a 1M FAIL can be blamed on scale rather than on the rewritten harness or path.

## What I checked and found sound

- **Binary root decoder** (`semantic_unit_router.rs:619-774`):
  - It runs only after the profile cap and the generation SHA are checked.
  - Short-circuit order prevents out-of-bounds reads of the schema area.
  - The header must agree with the caller's profile and geometry, and reserved bytes must be zero.
  - Total length must be exact, and the stored estimate must match a fresh `admit` result.
  - Directory checks cover ordering, contiguous offsets, sizes, unit and row totals, and finite prototype bits. All of this happens before any per-leaf allocation.
  - The encoder layout matches the plan's offsets, and `Discovery::valid` uses the same `512 + L(64+4D)` size formula.
- **Allocation admission** (`:241-316`):
  - My hand computation reproduces the plan: training 391,631,304 B, leaf construction 317,158,184 B, validation 268,342,232 B.
  - The 16-frame recursion bound holds: with k=32 children each holding at least one quota, the largest child is at most `leaves−31`, so 489 needs `⌈488/31⌉ = 16` frames.
  - Counting group sizes before allocating keeps insertion order, empty-group repair order and tie order unchanged.
- **Size caps:**
  - At 1M, at most 977 leaves give a root of at most 3,064,384 B, under the 4 MiB cap.
  - Selected leaves are at most 1,576,960 B, under 2 MiB.
  - Walk units are at most 1,031.
- **Source-order identity:** `two_bit_source.rs:256-259` hashes the same little-endian u64 physical→ID bytes that the scorer binds at `check_semantic_router_scorer.rs:280`. The panel tool writes the `truth.i64` (51,200 B) the scorer expects.
- **Maintenance rejection:**
  - Compaction is checked twice, in the outer function and again under the lock in `compact_owned`.
  - Empty replacement is rejected after the existing argument validation and before any write.
  - The test asserts that no scratch directory is created and the store's object count does not change.
- **Diagnostic trace path:** its scratch charge is about 46 KB and rows-independent (every term is capped). The same deduction already ran at 100k/D768. SQ8 coverage decoding uses stride `D+12` from byte 0, which matches `two_bit_generation.rs:1977`.

## B1–B3: must be done before the fresh panel is consumed

**B1. The key Native100k end-to-end test was not rerun on the final source.**
- Log `06-generation` (12:43) ran on freeze v4. That suite includes `semantic_object_store_parity`, which covers single-pass diagnostic parity, remote staging and the compaction lifecycle.
- Freeze v6 changed `object_native_generation` staging, and v7 changed `publish_empty_with_mode`. Both are exercised by that test.
- Logs 09, 13 and 15 reran only neighbouring targets.
- **Minimum check:** `cargo test --locked -p borsuk --lib two_bit_generation::` on `98ff6e5e` must exit 0. The planned remote full run covers this, but the receipt must show it explicitly.

**B2. There is no positive control through the new scorer.**
- The scorer was rewritten (+~500/−1,428 lines), the root format changed (BORSUSR2), the generation schema is v8, and diagnostics now come from one combined pass.
- All 100k quality evidence is from v7 and the old harness.
- **Fix (no code change):** rebuild ReLAION 100k as v8 Native100k and run the new scorer locally on dev0-63. It must reproduce the historical returned IDs, or at least 628/640. That panel is already consumed, so this costs no fresh queries.
- Without this, a 1M FAIL cannot be separated from a harness or ID-mapping fault.

**B3. Truth is in source-ordinal space, but nothing binds the order to that space.**
- `inverse_order` checks only that the order is a permutation and that its SHA matches the generation (`:159`, `:280`).
- If application IDs are not source ordinals, the run reports about 0 hits as a quality FAIL.
- **Fix:** freeze, in the controller, that the order SHA equals the authenticated source-ordinal order receipt.

## Should-fix: cheap, protects what a failure can tell you

**S1. One query error aborts the whole panel and loses the nomination breakdown.**
- On any per-query error the scorer returns immediately (`check_semantic_router_scorer.rs:307-323`).
- `diagnostic_search_with_store` drops the trace on error (`two_bit_generation.rs:1886-1907`), although `semantic_units` is already filled before the source cover fails.
- So a single source-budget error on query 0 gives no truth-conditioned nomination coverage at all.
- **Fix:** return the trace with the error, record the failure, continue, and keep the run status FAIL/incomplete.

This matters because the source-read cap is new territory at 1M (see S2).

## Scale facts relevant to the falsifier (not code defects)

**S2. The source-read cap never bound at 100k; at 1M it can.**
- A source record is `768/4 + 8 = 200` B, so a page is 51,200 B.
- At 100k the whole plane is 391 pages ≈ 20.0 MB, below the 64 MiB cap. The cap could not bind.
- At 1M there are 3,907 pages, and 64 MiB admits at most 1,310 of them.
- A 1,024-page closure fits only if it forms ≤128 runs, or if merging runs adds ≤286 gap pages.
- A fully scattered 1,024-page closure covers roughly 2,600 pages and fails with InsufficientBudget. How likely that is depends on the frozen 1M physical order, which I could not see.

**S3. The 100k result says little about 1M nomination.**
- The fixed nomination budget is 16 × 64 × 32 = 32,768 rows: 32.8% of 100k but 3.3% of 1M.
- SQ8 admits 84 pages: 21.5% of 100k versus 2.15% of 1M.
- My prior for ≥95% R@10 is low, though uncertain; it depends on layout coherence.
- The arm is still a cheap, decisive falsifier, so this is not a block.

## Minor issues (non-blocking)

- `TwoBitPlanTrace::scratch_bytes` (`two_bit_generation.rs:510-517`) charges pages with the graph bound (2×159). Semantic `ranked_candidate_pages` can hold up to 1,024, so it undercharges by at most about 5.6 KB, and the doc comment at :493 is stale.
- Only compaction and empty replacement are guarded. `apply_two_bit_mutations`, `seal_two_bit_mutations` and the write fence still accept Fresh1m writes that can never be compacted. This is a product gap, not a gate issue.
- `ids[..10]` at `check_semantic_router_scorer.rs:374` panics with no terminal record if fewer than 10 candidates come back. Unlikely at 1M.
- `publish_empty_with_mode` now always reads the previous manifest when one exists, including graph mode with an explicit discovery mode.

The modeled 400,316,456 B, ~308.7 MB and ~617.3 MB figures are still estimates. There is still no 1M RSS, recall or latency measurement, and the full workspace test compile remains unverified (exit 143). I also saved a memory note summarising this review.
