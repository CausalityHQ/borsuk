**BLOCK candidate `98ff6e5` before the 1M evaluation gate. One concrete blocker found.**

**P1 — Every D768 diagnostic query exceeds its scratch allowance.** The scorer sets `max_query_scratch_bytes` to **400,000** at [check_semantic_router_scorer.rs:231](/home/rb/worktrees/borsuk-semantic-1m-binary-root-20261001/crates/borsuk/src/bin/check_semantic_router_scorer.rs:231). The diagnostic path then subtracts the trace charge before preparing the query at [two_bit_generation.rs:1425](/home/rb/worktrees/borsuk-semantic-1m-binary-root-20261001/crates/borsuk/src/two_bit_generation.rs:1425).

For both 100k and 1M rows on the supported 64-bit target:

- Trace charge: **45,576 bytes**, leaving **354,424 bytes**.
- D768 codec requirement: `(192 × 256 + 768) × 8 = 399,360 bytes`, enforced at [rotated_two_bit.rs:233](/home/rb/worktrees/borsuk-semantic-1m-binary-root-20261001/crates/borsuk/src/rotated_two_bit.rs:233).
- Therefore query zero returns `MemoryBudget` before nomination or truth evaluation. This would be an implementation failure, not a scientific falsification.

Minimum failing check, **not executed** under the read-only restriction:

```rust
let codec = RotatedTwoBitCodec::new(&[0.; 768], 20260923).unwrap();
let query = [1.; 768];
assert!(codec.prepare_query(&query, 400_000).is_ok());
assert!(codec.prepare_query(
    &query,
    400_000 - TwoBitPlanTrace::scratch_bytes(1_000_000),
).is_ok()); // fails
```

**Minimum fix:** add the trace allowance to the scorer’s diagnostic scratch budget, preserving 400,000 bytes for codec preparation and charging the total through existing admission. Update the frozen allocation estimate. Add D768 diagnostic-versus-normal search parity; the current parity fixture uses D2 at [two_bit_generation.rs:2216](/home/rb/worktrees/borsuk-semantic-1m-binary-root-20261001/crates/borsuk/src/two_bit_generation.rs:2216).

I found no additional concrete blocker in binary decoding, count-first allocations, persisted profile/source binding, source-order evaluation, or direct Fresh1m maintenance rejection. Maintenance rejection performs metadata reads; it precedes maintenance writes.

The 14 Rust source hashes matched the receipt. Release and Clippy logs authenticate exit 0; workspace compile-only remains **unverified, exit 143**. No builds, corpus execution, AWS actions, children, or edits were performed. The allocation estimates and historical v7 100k results establish no v8 1M RSS or recall qualification.
