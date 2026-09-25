# V195 used V194 panel: rerank and mandatory-floor diagnostic

## Question and firewall

V194 failed on fresh ReLAION-1M D768 source pseudoquery SHA ranks
2945–3456. Those 512 identities are now **used development data**.
This diagnostic may identify a revised generic return representation
and budget rule, but it cannot turn V194 into a validation pass. Keep
V194's authenticated source, V164 order, V115 SQ8 quantizer, candidate
features and optional-risk physical plans frozen. Recompute V194's
exact-source GT100 IDs and SQ8 top-100 returned IDs on remote compute
as an independent check of its 50,692-hit result. Do not change the
candidate radius, model, price, or physical plan on 511 feasible
queries.

## Rerank substrate and resource model

For each feasible optional-risk plan, score its fetched V164-ordered
SQ8 rows and exclude the source query ID. Preserve the SQ8 top-K
shortlist for **K=128, 160, 200, 256**, then rerank that fixed
shortlist by cosine using source float32 vectors converted to IEEE
FP16 and decoded to float32. The FP16 candidate payload is simulated
from the authenticated source; no trained correction or query-specific
exception is permitted. Score exact float32 payload as an upper
control. Count GT100 in the final top100 and GT100 present in the
shortlist. Compare to the V194 SQ8 top100 and fetched physical coverage
per query.

Price a hypothetical immutable FP16 sidecar in the same V164 physical
order: one 32-vector unit is **49,152 bytes**. For each K, take the
physical units holding the shortlisted stable IDs and compute the
minimum contiguous-unit cover using at most
`64 - V194 optional SQ8 GETs` extra range GETs for that query. The
64-GET developmental cap is explicit because the mandatory-floor tail
can consume all 32 V194 SQ8 GETs before a separate rerank read. This
is an optimistic lower bound on sidecar bytes/GETs, since it excludes
object metadata, request overhead, and implementation overhead; never
report it as live S3 cost. Report extra sidecar bytes/GETs and combined
totals against V194's scaled V155 envelope of **5,700,611,604 bytes
and 11,328 GETs**. A sidecar rule must have a physical interval witness,
not only a set of scattered vector IDs.

Independently, for V194's one infeasible query ordinal 3321, compute
the minimum 32-GET mandatory floor from its sealed feature and run the
same frozen optional utility/price with a generic cap
`max(672, mandatory_floor)` and 512 MiB trace allowance. Report its
interval witness, exact physical GT100 coverage, SQ8 top100 and the
same K reranks. This is an admission diagnostic; the extra units above
672 violate V194's frozen per-query gate, so they cannot rescue that
result retroactively. Do not change any other query's plan.

## Decision rule and next validation

The return-format lane is promising only if one predeclared K recovers
at least **200 of V194's 249 SQ8 return losses** across the 511
feasible queries while the combined optimistic planned bytes and GETs,
including the dynamic-floor query, fit the scaled V155 aggregate
envelope and each query uses no more than **64** combined GETs. This
does not meet V155's 32-GET per-query envelope; disclose every query
above 32 and measure its live latency before any production choice. Also
report the exact-float32 control, FP16 gap, sidecar storage size,
and whether the dynamic-floor query becomes feasible. If the sidecar
lower bound fails, reject this layout/GET schedule and investigate a
different representation or locality scheme rather than selecting a
favorable K after seeing results. If it passes, implement a bounded
serving path and freeze it before a new identity-disjoint source panel,
real-query transfer and live S3 performance/cost gate. No V195 number
is publication evidence.

Use one immutable Causality Spot cell. Authenticate V194's complete
terminal and completed artifacts before diagnosis, seal its GT-blind
inputs and exact K/method choices, record resources and terminal hashes,
discard an interrupted cell, and terminate the worker immediately.
Do not run a large diagnostic on the swap-pressured devbox.
