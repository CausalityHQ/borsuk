Read-only analysis complete; 896d0423 is clean, with unchanged OWN2 hashes. Smallest credible prospective arrangement: the EXISTING safe crate::simd_control::f64x4 accumulator, keeping checked loads and scalar finalizers.

Exact loop shape inside score_four:
```rust
use crate::simd_control::f64x4;
let [r0, r1, r2, r3] = records;
let mut dots = f64x4::splat(-0.0);
for byte in 0..self.packed_bytes {
    let table = &self.table[byte * 256..(byte + 1) * 256];
    let v0 = table[usize::from(r0.packed[byte])];
    let v1 = table[usize::from(r1.packed[byte])];
    let v2 = table[usize::from(r2.packed[byte])];
    let v3 = table[usize::from(r3.packed[byte])];
    dots = dots + f64x4::from([v0, v1, v2, v3]);
}
let [d0, d1, d2, d3] = dots.to_array();
// Existing four finish_score(rN, dN)? calls remain verbatim/in order.
```
Use Add, not +=: the existing scalar-control facade has Add but not AddAssign. Extract lanes only AFTER the byte loop. Each lane receives one addition per original byte, in order, from literal -0.0; no horizontal operation, multiplication or fused expression enters the loop. Validation, buffered error draining, public callbacks, scalar tails/fallback and final math stay unchanged.

Why different: current source asks LLVM to discover packing among four scalar loop recurrences; authenticated assembly chose four scalar memory-operand adds. This facade supplies explicit lane-wise vector addition. Cargo.lock pins wide 0.7.33; simd_control.rs:10 re-exports it and lexical_simd.rs already uses it. I matched the cached .crate SHA to Cargo.lock and inspected vector files to that archive. Without compile-time AVX, wide f64x4 is two f64x2 values; its Add delegates to both, and f64x2 Add selects safe_arch add_m128d for SSE2. This supports a reasonable packed candidate, NOT an emission/speed claim. No new dependency, compiler flag or BORSUK unsafe code is required; the existing dependency's internal unsafe remains within the already-used trust boundary.

Mechanism caveat: this changes this kernel from scalar-source auto-vectorization to the existing explicit safe-vector backend. It needs YOUR prospective repair/arm decision; it cannot retroactively satisfy 896's closed gate. If only std scalar/array auto-vectorization is allowed, I cannot reasonably force packed emission. A [-0.0;4] accumulator with a fixed inner lane zip/add loop is plausible but may unroll/scalarize back to the failed structure; it is not a reliable source-only fix.

Scratch: accumulator 32 bytes, fixed 32-byte contribution and final lane array; no heap/table/batching growth. Keep the 512-byte charge/fallback. The vector's 32-byte alignment and spills still require exact production caller+kernel assembly comparison to certify <=512 incremental stack.

Existing witnesses suffice for the proposed arithmetic change: rotated_two_bit::tests::four_row_literal_scalar_oracle_matrix (every score bit, distinct lanes/all256 words, dimension matrix, cancellation=>2, signed zeros) and four_row_validation_and_scratch_boundaries; two_bit_generation::source_walk_tests::four_row_authenticated_plan_parity_and_errors (all49 unique winners, mixed-zero fold, lane2/late-tail prefixes), four_row_stateful_callback_and_buffered_error_priority, and four_row_tiny_builder_serving_parity. Requalify exact repaired source and retain packed-addition/serial-order/no-horizontal/no-FMA/stack gates before timing.

No edits, native execution, consultation, child or review. HOLD.
