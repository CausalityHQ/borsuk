# V196 authenticated resident FP16 rerank: implementation preflight

## Decision and evidence boundary

Test whether a generation-pinned resident FP16 precision plane can
remove V195's second-wave S3 GET failure without making local memory
or CPU latency unacceptable. This is a **production primitive and
used-panel preflight**, not a fresh recall validation. V194's ReLAION-1M
D768 source pseudoquery SHA ranks 2945–3456 were opened by V194/V195.
Keep V194's optional SQ8 plans frozen for its 511 feasible queries and
V195's generic 766-unit floor rescue for ordinal 3321. Keep the SQ8
top-128 shortlist and original V115 quantizer. There is no model, price,
query-ID, candidate radius or physical plan refit.

V195 already measured the counterfactual return quality for this exact
used cohort: **51,022/51,200 FP16 returned GT100 hits**, p05 98,
15/512 queries below 98; the float32 source control was identical.
The resulting SQ8 first-wave plans total **3,883,676,160 bytes and
5,179 GETs**. These numbers become V196 expected-output controls,
not new measurements. The separate V195 FP16 sidecar was rejected at
5,219,725,824 bytes/16,035 GETs, and no live S3 latency has been
measured. V155's used, unpaired ReLAION-1M validation-1000 baseline is
99,567/100,000 returned GT100 hits at 11,134,007,040 planned bytes and
22,126 GETs; its scaled envelope is a screen, not a paired comparison.

## New format and serving primitive

Build an immutable **format-v1** local FP16 plane in the same V164
physical order as SQ8. Each row contains an 8-byte stable ID and 768
little-endian IEEE FP16 coordinates, 1,544 bytes per row. The file has
a versioned 64-byte header with row count, dimensions, generation and
authenticated source SHA-256. The complete artifact SHA-256 is pinned
by the generation manifest. The loader streams and authenticates the
full file and geometry, and allocates only under an
explicit caller resident budget. A query passes physical row ordinals
and stable IDs from the fixed SQ8 shortlist; the resident tier checks
each pair and scores cosine with deterministic `(score, stable ID)`
ties. A mismatch or corrupt artifact fails closed. The serving API
must not silently use a different generation's plane.

The plane costs `N × (8 + 2D) + 64` bytes plus allocator and runtime
overhead: **1,544,000,064 bytes for 1M D768** and
**154,400,000,064 bytes for 100M D768**. Zero-downtime generation
cutover may hold two planes, **308,800,000,128 bytes** at 100M before
router, deltas and workspaces. The budget is a function of vector count,
dimensions, recall tier and concurrent generations; there is no fixed
100M knee. A higher-memory profile is a cost/latency choice that must
be paired against external products later. Mutations use a generation-
pinned resident delta overlay until compaction rewrites the base plane;
the preflight tests the immutable base and records the overlay as
remaining production integration work.

## One Spot preflight and gates

On one Causality Spot worker, authenticate the complete V194/V195
artifacts and original source/order/SQ8. Build the FP16 plane and a
sealed 512-query case file of vectors, physical top-128 ordinals,
stable IDs and V195 expected returned IDs. Hydrate the plane into a
separate Rust serving process with its exact file hash and a 2 GiB
resident-plane budget. The process runs 10 full query repetitions after
one warm-up, returning exact IDs and per-query nanoseconds. Record
process peak RSS, charged memory if the isolated cgroup exposes it,
local bytes and cold hydration wall time separately. Do not mix build,
hydration or S3 input download into per-query rerank latency.

Pass only if every one of the 5,120 returned sets exactly matches
V195's FP16 top-128 IDs, the worst repetition's p95 local rerank is at
most **2 ms** and p99 at most **5 ms**, the resident process peak RSS
is at most **2 GiB**, and cold artifact authentication succeeds without
exceeding that process budget. Report full plane storage, dual-generation
capacity, source-archive/terminal hashes and hardware. Failure means
fix the responsible scorer or memory representation on this used panel;
do not change the recall policy or claim a new holdout result. A pass
licenses a new identity-disjoint 1M source panel followed by a distinct
real-query dataset and live S3 p50/p95/p99 latency, throughput, charged
RAM and cost. It does not itself freeze a production architecture.

Use one immutable source archive, terminal marker and complete artifact
replay. Seal the plane and case file, including V195 expected IDs, before
timing the Rust process. Terminate Spot compute immediately after terminal.
No full local suite runs on the swap-pressured devbox.
