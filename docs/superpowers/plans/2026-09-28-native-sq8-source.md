# Native SQ8 source creation

Implement the frozen min/max SQ8 codes from immutable normalized LE f32 source
and approved ordinal permutation. Admit caller-owned order plus bitset and
O(D) workspace before allocating; stream calibration/hash, seek one row per
physical ordinal, write new body and return SHA/calibration only after sync.
Reject wrong identity, non-unit/nonfinite/zero vectors, duplicates, bad geometry,
insufficient memory and overwrite. No query/GT input, layout fitting or format
change. Use f32 ties-to-even codes and sequential reconstructed squared norm;
NumPy SIMD norm parity is not assumed. Existing quality artifacts remain frozen.
First run one failing public-boundary test on Spark, implement, rerun, then
compare a deterministic reference fixture with varying coordinates and ties.
No paid jobs or full suite. Native semantic order and lifecycle remain next gaps.
