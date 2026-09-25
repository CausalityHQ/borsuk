# V197 frozen resident FP16 fresh 1M source holdout

## Question and lock

Does the V196 generation-pinned resident FP16 tier, paired with the
frozen generic optional-risk physical planner and dynamic mandatory
floor, clear a new identity-disjoint ReLAION-1M D768 source-query
screen without per-query or per-dataset tuning? This is an offline
recall/transport gate, not live S3 latency or a real-query qualification.

Use SHA-order source pseudoquery ranks **3457–3968** (zero-based
ordinals 3456–3967), 512 IDs disjoint from V194/V195/V196 ranks
2945–3456. Preserve the V115 router/quantizer, V164 physical order,
V189 fit data, V192 prices and V194 candidate radius 32. Freeze the
V194 optional-risk model and prices `(1000, 50000)`, physical unit
24,960 bytes, ≤32 GETs per query. For each query compute its mandatory
minimum unit count under 32 GETs; set the unit cap to
`max(672, mandatory minimum)`. This rule uses only the GT-blind
candidate/mandatory roster and applies to **every** query, with no
special ordinal. Keep exact GT100 closed until all features, plans,
query IDs and plan-source hashes are uploaded and sealed.

Read the same 1M-row resident FP16 plane format as V196 and rerank the
SQ8 physical top-128 shortlist to 100. The plane is built from the
authenticated source and pinned to the physical generation; no
second-wave S3 FP16 sidecar is charged. Compare paired, on these same
512 queries, against SQ8 top100 under the identical optional-risk
plan and FP16 under the V194 full-rank model with the same dynamic
floor. Do not select a different model, radius, cap, precision or
shortlist size after the GT100 results are opened.

## Decision gates

The optional-risk FP16 arm advances only if it has at least
**50,979/51,200 returned GT100 positions**, empirical p05 at least
**98/100**, zero infeasible plans, at most **5,700,611,604 planned SQ8
bytes** and **11,328 planned SQ8 GETs**. These limits are the frozen
V194 512-query source screen and must not be moved for this panel.
Report minimum and below-98 count and Wilson interval, candidate and
fetched coverage, SQ8 and FP16 returned IDs, and paired full-rank and
SQ8 deltas even if the gate fails. The used V195/V196 reference is
51,022/51,200 returned GT100 positions, p05 98, 15/512 below 98,
3,883,676,160 planned SQ8 bytes and 5,179 GETs; it is an **unpaired
used-panel context**, not the V197 baseline. V155 ReLAION-1M
validation-1000 real queries are also unpaired and used
(99,567/100,000 returned GT100 at 11,134,007,040 planned bytes and
22,126 GETs).

Preregister one Causality Spot measurement cell. Upload the source
archive, GT-blind feature/plan seals, then evaluate. Record complete
query-level IDs, hashes, hardware, process RSS and terminal state;
discard and restart an interrupted cell under a new attempt. Terminate
the worker immediately after the terminal marker. A passing source
screen licenses a distinct real-query dataset and paired live S3
latency/resource/cost gate. A failure requires a root-cause decision
by candidate omission, plan allocation or precision rerank loss before
another method revision.
