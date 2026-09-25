# V198 frozen real-query 1M D768 screen

## Question and provenance

Does the V197 generic optional-risk physical planner plus resident FP16
rerank match the strongest closed BORSUK returned-quality baseline on
**ReLAION-1M D768 validation-1000 real queries** while reducing planned
SQ8 bytes and GETs? This split has been used in V116/V155 and is
**not fresh**. It is a paired real-query falsification gate before live
S3 transport, not an independent publication result or cross-dataset
generality claim.

Authenticate the frozen V116 1,000 query vectors and V36
validation-GT100 parquet. Use the V115 source router/quantizer, V164
physical order and SQ8 object, V189 fit, V192 prices, V194 radius 32,
V197 `max(672, mandatory-floor)` unit cap on every query, and the same
V196 generation-pinned FP16 plane. The real-query route nominates 512
old-layout entries without leave-one-out; require exact set parity with
V116's sealed source-only nominee list before deriving the 100 mandatory
SQ8-ranked nominees. Score the same radius-32 PQ unit field, plan with
optional-risk weights and `(1000, 50000)` price, and rerank the physical
SQ8 top-128 with resident FP16. Keep optional model, router,
quantizer, page width, GET cap, shortlist and tie rule fixed. Upload
query IDs, features, plans and their hashes as GT-blind seals before
opening the validation GT100.

## Frozen decision

Compare paired on the exact same 1,000 queries to V155's closed cached
sparse exact-source baseline: **99,567/100,000 returned GT100**, p05
**98**, 11,134,007,040 planned bytes and 22,126 GETs. V155's
SQ8-only same-plan arm returned 99,222. V155 uses exact source union
rerank and is a historical architecture; report its extra source
storage/local reads separately rather than claiming an equivalent RAM
profile. Also report V198 SQ8 top100 and FP16 top100 under the **same
new plan**, with per-query returned IDs, GT100 intersections, candidate
and fetched coverage, p05/minimum/below-98 tail, bytes and GETs.

Advance to live S3 only if the optional FP16 arm returns at least
**99,567/100,000 GT100**, p05 at least **98/100**, has zero infeasible
plans, and stays at or below **11,134,007,040 planned SQ8 bytes** and
**22,126 planned SQ8 GETs**. This is a strict same-split screen, not a
statistical claim of unseen-query superiority. The Rust resident scorer
must reproduce all 1,000 FP16 top-100 lists over 10 repetitions with
the V196 local p95≤2 ms, p99≤5 ms and serving-process RSS≤2 GiB
guards. If any arm fails, decompose losses into candidate omission,
physical allocation and precision/rerank before changing the method.

One Causality Spot cell must seal source archive, GT-blind features and
plans, then evaluate and close raw artifacts under a terminal marker.
An interrupted cell is discarded and restarted at a new attempt.
Terminate compute immediately after the terminal. A pass licenses a
paired live S3 gate for this same frozen method, then a genuinely
different real-query embedding dataset before 10M or 100M promotion.
