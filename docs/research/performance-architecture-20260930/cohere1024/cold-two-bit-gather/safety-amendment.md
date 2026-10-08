# Gather safety amendment

Prospective decision: retain the within-row gather candidate (A). Both completed
critics agree its checked tile/index/store bounds are locally sound. Engineering
recommends A; research predicts serial-add dependency will prevent a useful win
and recommends across-row batching. That speed prediction has no executed
codegen or timing evidence. Test the existing distinct candidate at the frozen
cheap primitive gate; do not change production callbacks or batch rows here.

OWN3 adds lib.rs: forbid(unsafe_code) becomes deny(unsafe_code), with reasoned
allowances only on private avx2_dot and gather_dot. Restrict explicit SIMD to
x86_64. The safe runtime-detection wrapper returns Option; tests use it without
unsafe allowances. The target-feature method keeps its single explicit bounded
gather/store unsafe block. Public validation, arithmetic, scratch and callback
order stay unchanged. No dependency, production selector or allocation added.

Required witnesses: forced AVX2 execution, checked truncated-table panic before
gather, malformed record/scalars refuse before touching an unusable table,
existing literal score-bit and callback-prefix oracles in debug and release.
Root must verify actual portable compiler flags, emitted gather and sequential
adds/no FMA, CPU model/microcode, and public fallback/non-x86 evidence before
claiming portable qualification. Scalar helper parity alone is insufficient.

Ignored primitive prints its complete receipt then asserts ACCEPT outside the
panic catcher. Harness success cannot hide REJECT/INVALID. Root independently
classifies the full JSON and resource receipt; nonzero harness status alone
cannot distinguish algorithm rejection from execution invalidity. Both arms
use symmetric opaque function pointers and black_box inputs/outputs.

All original primitive/cold thresholds and resources remain unchanged. A 20%
kernel saving at 13.65% sampled CPU share implies only about 2.73% total CPU
saving under unchanged work, not a cold-tail win. Source/native/codegen/speed
remain UNVERIFIED. Same held worker received source-only amendment; no paid
job, integration, native execution or repeated vendor reference was launched.
