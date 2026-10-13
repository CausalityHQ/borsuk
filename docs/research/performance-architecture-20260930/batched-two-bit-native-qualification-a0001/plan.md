# Bounded two-bit batch scorer

Measured basis: closed CPU profile ae7401d4, fixed128 queries, PreparedTwoBit::score18.74% of sampled process CPU in the source planner. No predicted speed win. The current planner scores each selected unit's at-most32 records separately.

One causal change: score at most8 records together, visiting one LUT byte bank across lanes before advancing to the next byte. Keep each row's byte-order f64 accumulation, footer validation, final expression, source callback order/first invalid-record refusal, unit maxima, trace and physical/logical plan unchanged. No format/routing/quantization/cache/authentication change. Portable scalar lanes first; no new dependency, unsafe intrinsic or runtime knob.

Bounded scratch:8 borrowed record slices plus scale/inverse-norm/dot/output f64 lanes =8*(size_of(&[u8])+4*size_of(f64))=384B on64-bit. Query admission charges LUT plus max(rotated-query bytes,batch buffers), so D768/D1024 bounds stay399360/532480. D1/D5 increase to2432/4480. No dynamic per-unit or corpus-sized allocation.

TDD first: the real codec must refuse preparing a D1 query with the old2056B cap before a batch kernel is admitted; accept2432B. Record native compile0/exact runtime RED101 on EC2 before production changes. Then implement batch and add exact scalar/reference bit parity over generic dimensions, all0..8 partial widths, signed zero/cancellation, malformed footer/length and bounded-output negatives. Integrate into source nomination with first-error/callback preservation; exercise real planner trace/physical-plan parity.

Root-owned qualification on causality EC2 Spot only, jobs1: affected lib test compile, exact new tests, affected codec/source-walk regressions, release baseline, workspace correctness/suspicious Clippy, actual shim-unset test-build. Source archive/native map/count before/after and owned cgroup/resource/terminal/compute closure evidence required. No local Cargo/rustc/native tests, new children or paid review.

After qualification: actual frozen-input fast correctness/recall falsifier and separate canary; then freeze a matched full1000 cold comparison against unchanged width64 with a narrowly bound codec+generation source delta. Require exact scores/IDs/trace/charges for this numerical-equivalent optimization. Retain smaller speed gains; never claim a win from synthetic kernels or this profile. Full product/vendor/lifecycle/100M goal remains unproven.
