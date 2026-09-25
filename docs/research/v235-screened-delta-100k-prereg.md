# V235 conservative screened mutation delta, ReLAION-100k

**Decision tested:** whether a two-pass mutation scorer removes enough
ordered FP64 work to improve loaded query latency while returning
exactly the same IDs as the V232 full scan. It screens all 10,000
pending rows with a resident FP32 blocked dot product, then computes
the original ascending-coordinate FP64 score only for rows that could
beat the unmasked base top-k threshold. If the base contributes fewer
than k rows or the screen is not finite, the row receives an exact
score. Equal-bound rows are rescored, retaining score-descending,
ID-ascending tie order.

The screen slack is dimension-dependent:
`8γ_(2D+3)(f32) + 1e-12`, where `γ_n = n·2^-24/(1−n·2^-24)`.
For D=768 this exceeds 0.0007 cosine units. The factor eight covers
query conversion to FP32, a sequential FP32 product/sum, the shared
FP64 norm inverse and ordered exact-score rounding. The FP16 row
values are exactly representable in FP32; query and row norms are
finite and positive. If `n·2^-24≥0.25`, pruning is disabled. The
existing `formal/AdaptiveRerank.lean` proves the conditional interval
rule; it does not prove this IEEE error calculation or the Rust
implementation. Before any production default, require adversarial
numeric checks and a reviewed floating-point bound. This cell tests
empirical ID parity and latency, not a completed machine-checked
floating-point theorem.

One `causality` c7i.4xlarge Spot cell uses the authenticated V218
ReLAION-100k D768 graph/FP16/PQ/map, fixed requests and returned IDs.
Development queries 0–255 and previously used method-held-out queries
256–999, cosine k=100, exact GT100, ef/shortlist 2,048/2,048 and
10,000 every-tenth-row same-ID/same-FP16-vector upserts are fixed.
Run a row-major decoded full-scan control then the screened candidate,
each sequentially and with eight loaded workers from the same binary
and compiler flags. Seal per-query ID/timing and loaded timing streams
to S3 before downloading GT100. Recompute loaded p50/p90/p95/p99 from
sealed raw; report exact rows rescored, QPS, packing time, RSS,
overlay-owned bytes, vector GETs and estimated Spot cost.

Pass only if 1,000/1,000 complete returned ID lists match control,
per-split GT100 hits are at least V218 (25,537 and 74,234), combined
p05≥99, loaded p95≤75% of control, throughput≥1.5× control,
one-time packing≤1 s, peak RSS≤512 MiB, overlay bytes≤64 MiB,
10,000,000 rows screened and zero vector-body GETs. Record exact
rescore count and p95/max per query without tuning to it. Any ID
mismatch or numerical bound counterexample rejects pruning; a speed
failure sends the design to a materially different mutation tier or
latency-derived compaction trigger before another 1M run. A pass
promotes one 1M HTTP gate against V233 and V230, then real-S3 1M
mutation publication. No vendor or 10M/100M claim follows here.

Retain the immutable source archive, attempt reservation, original
terminal, all artifact hashes and sealed raws. Spot interruption
invalidates the complete two-arm cell. The worker syncs evidence and
shuts down; the controller confirms termination. Incomplete
measurement files are not inspected.
