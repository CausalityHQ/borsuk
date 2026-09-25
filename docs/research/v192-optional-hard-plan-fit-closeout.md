# V192 optional utility under hard physical caps: closed fit screen

## Decision

Advance the optional-only rank utility to a **fresh transfer gate**, not
to a production default. On the 32-query internal diagnostic, it
contained the same 3,194/3,200 exact source GT100 rows as a full-rank
control whose price was independently refitted, while planning
75,404,160 fewer bytes and 207 fewer GETs. It gained one hit over the
constant-risk ablation with 71,210,880 fewer bytes and 32 fewer GETs.
Both priced controls had p05 98. The cohort is already examined V189
fit data; no inference about new queries, actual returned Recall@100,
S3 latency or charged serving RAM follows.

The model's earlier total-mass prediction MAE was **worse than a
zero-hit predictor** on the internal fit validation (1.194 versus
1.156 hits/query). The physical-plan result, not that calibration
metric, is the reason for a fresh test. Its relative optional rank
order is unchanged from the old PQ rank feature; the differences are
optional-only fitting, query risk normalization and price.

## Paired developmental measurements

All rows below use **ReLAION-1M D768**, V189 source pseudoqueries,
exact float64 cosine GT100 excluding each query row. They measure
GT100 rows inside planned physical ranges, not returned search
results. Per-query caps are 672 physical 32-row units and 32 GETs;
one unit is 24,960 planned bytes. The aggregate 32-query envelope is
14,274 units and 708 GETs.

| Closed fit split and arm | GT100 hits / 3,200 | p05 hits/query | Planned bytes | Planned GETs |
| --- | ---: | ---: | ---: | ---: |
| Price fit, ordinals 2496–2527: optional risk, fitted price (1000, 50000) | 3,187 | 99 | 195,237,120 | 304 |
| Price fit: full rank, separately fitted price (2000, 50000) | 3,188 | 98 | 306,733,440 | 562 |
| Price fit: constant optional risk, new price | 3,187 | 98 | 294,103,680 | 330 |
| Price fit: V189-style greedy | 3,182 | 96 | 352,809,600 | **943** |
| Diagnostic, ordinals 2528–2559: optional risk | **3,194** | 98 | **221,345,280** | **269** |
| Diagnostic: full rank, refitted price | **3,194** | 98 | 296,749,440 | 476 |
| Diagnostic: constant optional risk | 3,193 | 98 | 292,556,160 | 301 |
| Diagnostic: V189-style greedy | 3,192 | 98 | 355,655,040 | **912** |

The greedy control exceeds the aggregate GET envelope in both portions,
so its quality is not a matched-resource win. The candidate truth
ceiling is 3,197/3,200 on **each** 32-query portion. In the diagnostic,
the optional-risk and full-rank plans have exactly the same hit count
on every query; optional-risk uses fewer bytes on 24 queries and fewer
GETs on 31. Query ordinal 2551 remains at only 96/100 contained hits
despite a 99/100 candidate ceiling and a 672-unit/31-GET optional-risk
plan. On the price-fit portion, ordinal 2527 remains at 93/100 despite
a 99/100 candidate ceiling and a 672-unit/32-GET plan. Nearest-rank
p05 on 32 queries is too unstable to qualify a tail policy.

The earlier truth-aware candidate-restricted hard-cap witness on all
128 V189 fit queries captured 12,779/12,800 with p05 99 at
150,259,200 bytes and 2,359 GETs. That oracle sees GT100 and is
an attainability reference, not a serving comparator. Its existence
and the two residual V192 misses point to optional-page discrimination
and price/resource allocation, not an intrinsic hard-cap or
candidate-universe impossibility on this fit panel.

The strongest measured BORSUK returned-quality baseline is still
**V155 used ReLAION-1M D768 validation-1000**: 99,567/100,000 actual
returned exact-source Recall@100 hits, p05 98, 11,134,007,040 planned
bytes and 22,126 GETs. It is unpaired with V192's source panel. V190's
fresh source panel at ranks 2689–2944 failed with 25,396/25,600 hits,
p05 97, 1,499,596,800 planned bytes and 4,744 GETs, including one
935-unit invalid plan; V192 does not revise that failure. There is
no paired current-revision S3 Vectors or Turbopuffer result.

## Closed authority and resources

One Causality Spot `c7i.2xlarge` worker
`i-0cd8b716894ae0a66` ran source commit
`a22a0d7c6f9cfc71f627bdafa85875750c2048f9` in attempt
`s3://borsuk-bench-453182569524-euc1/research/v192-optional-hard-plan-fit/a22a0d7c6f9cfc71f627bdafa85875750c2048f9/runs/a0001/`.
The source archive SHA-256 is
`25bf85f4bfeea2cd903d296b6f2d4b63629fb3cb6907d12a887e66b205bb841e`;
complete terminal SHA-256 is
`8b5db701cf81545287ab000e10c0a19136bd35ee27170f154985268ef6f18961`;
result SHA-256 is
`b79683695350b4bc21eb4cad14f3588ed5ebaef088a1dd9443b3cc429ef62a01`.
The controller rehashed all terminal-listed S3 artifacts and verified
the instance terminated. `scripts/check_v192_optional_rank_plan_fit.py`
independently matched
all 256 arm/query rows against sealed V189 fit identities and GT100,
mandatory coverage, disjoint interval geometry, unit/GET/byte charges,
candidate ceilings, hit counts, aggregate caps, nearest-rank p05 and
both grid-selected prices. Only complete historical artifacts were
opened.

The offline model/price/plan process used 169.82 CPU seconds and
169.90 wall seconds, peaking at 157,272 KiB RSS on Spot. This includes
two 35-price fit searches plus the four paired arms on 64 queries; it
is not per-query serving latency or charged production RAM. The
devbox ran narrow tests and one 1.45-second sparse truth-oracle check
under its memory pressure; no local full suite was started.

## Next gate

Freeze this policy and price-selection rule for a cross-scale 100k
transfer screen on the already used real-query development cohort,
then a disjoint fresh ReLAION-1M source holdout of at least 512
queries with a tail-failure confidence interval. Include the
full-rank control with its own price fit, constant-risk ablation,
V189-style greedy and truth-aware hard-cap witness on the **same**
queries. The gate must count full GT100, candidate omissions,
aggregate hits, p05, bottom-decile mean, per-query infeasibility,
planned bytes and GETs. A pass advances to actual returned S3
quality, latency, charged RAM and cost under matched workloads and
to at least one distinct data distribution. If the tail still fails,
replace the optional feature with a GT-blind row-level SQ8 score-gap
model before another large-scale campaign.
