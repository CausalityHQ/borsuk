# V212 Rust source-PQ plus resident FP16 100k falsifier

V211's frozen 32,768-neighborhood → PQ64 top-4,096 → FP16 top-100
method passed same-panel 100k quality but exceeded its 10-ms
Python-stage p95. Test that **same** budget and source-only artifacts
in Rust before deciding whether to reject this candidate family.
Use the already-used ReLAION-100k D768 development first 256 queries,
V114 512 nominees, V163 physical order, V113 source-trained PQ64
books/codes, authenticated 100k source, and V193 full-rank paired
baseline. No per-query or dataset-fitted coefficient is introduced.

Build an authenticated physical FP16 plane and explicit old/new row
mapping from the pinned source/order. For each request, expand the
512 mapped seed ordinals to the 32,768 nearest physical rows by a
multi-source heap, score those old rows with the existing Rust PQ64
kernel, select 4,096 by PQ score and physical-ordinal tie, then score
FP16 and return stable top-100 IDs. Record nearest, PQ, FP16 and
whole-kernel p50/p95/p99, hydration, process RSS and sequential QPS.
Seal returned IDs before opening GT and V193 baseline. The Rust gate
requires at least the paired baseline's aggregate GT100 hits, p05≥98,
whole in-process p95≤10 ms, and zero vector-body GETs. The Python
V211 result is a quality/control comparison, not a transferred Rust
measurement. Any numeric boundary difference is resolved from actual
Rust returned IDs and GT.

Run one Causality Spot cell, discard interruptions, read back every
terminal artifact digest and terminate. Passing authorizes production
generation integration and a fresh-query gate before another 1M
campaign; failure rejects this route in favor of a different
candidate index. No 100M runtime or external-product claim follows.
