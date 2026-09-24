# V178 source oracle physical-budget preregistration

## Decision and authority

V177 passed an unlimited candidate-union ceiling on ReLAION-1M D768 source
pseudoqueries, but 62/128 complete width-8 unions exceed 16 MiB. Before
fitting a utility model, test whether the V164 relaid layout can physically
retain the V155 qualification recall rate under 32 GETs, 16 MiB, and complete
primary coverage. This is a **truth-aware oracle diagnostic** on the already
closed V177 source panel, not a holdout claim, deployable query planner,
returned recall measurement or paired comparison to V155.

Input identities are the V177 complete terminal SHA256
`41acf150e2c76eae346d56fd9db049321348932fd934d8fa58a8ab9fdbc559e7`,
its sealed rosters/labels and prepare seal, V63 old layout, V70 SQ8 and V164
relaid order/terminal. The source labels were computed after roster sealing.
This new diagnostic is preregistered before its plan outputs are calculated;
it cannot be represented as a fresh unseen-query result.

## Three oracle arms

For each of the same 128 source pseudoqueries, map the 100 closed V177 exact
cosine truth stable IDs to physical 32-row SQ8 units through the authenticated
old-SQ8 ID plane, V63 source order and V164 inverse relaid order. Independently
recompute V177's width-8 coverage count and require exact agreement. The
primary-constrained and width-8-weighted arms make every sealed exact-primary
unit mandatory; the unit-transport arm omits that requirement to isolate
the transport limit.

1. **Unit-transport oracle:** weight all 100 truth positions with no mandatory
   primary units. Maximize captured truth with the exact interval DP, 32 GETs
   and 672 complete units (16,773,120 SQ8 bytes, below the 16,777,216-byte
   physical cap). This is an upper bound for whole-unit plans under these caps,
   even if the primary roster changes. It does not bound row-granular formats.
2. **Primary-constrained oracle:** weight all 100 truth positions, including
   positions outside the width-8 candidate union, while covering every sealed
   V177 exact-primary unit. This is an upper bound for whole-unit plans that
   retain those primaries under these caps.
3. **Width-8-weighted oracle witness:** weight only truth positions in the
   V177 width-8 union. It uses the same DP and physical caps. Count *all*
   truth physically fetched in its resulting ranges, including unweighted
   truth inside a bridging interval. This is a feasible truth-aware witness,
   not the exact maximum over all candidate-conditioned policies.

Use the existing `modeled_plan` primary force, which lexically outweighs all
100 truth weights without overflowing the int32 DP. A plan missing any
mandatory unit is reported as primary-infeasible, never treated as zero-cost
success. Independently verify sorted disjoint intervals, mandatory cover,
actual inclusive unit count, GETs and bytes for every plan. Preserve plans,
per-query hits and aggregate fit/holdout counts in a closed terminal.

## Decision rule and interpretation

V155's **used ReLAION-1M D768 validation-1000** exact-source returned
Recall@100 was 99,567/100,000, p05 98, at 11,134,007,040 planned bytes
and 22,126 planned GETs. Its rate translates to a minimum 12,745/12,800
source truth hits here, rounded up. Require p05 at least 98/100 across the
128 source queries (seventh smallest count). The cohorts are different; these
are qualification thresholds, not a paired baseline subtraction.

- If the unit-transport oracle misses 12,745 hits or p05 98, stop promoting
  this whole-unit format under these physical caps and diagnose transport.
- If unit transport passes but the primary-constrained oracle misses either
  gate, change the primary roster or relaid layout before promotion.
- If primary-constrained passes but the width-8-weighted witness misses either
  gate, revisit the candidate generator or costed unit selection. This arm
  does not prove no other width-8 plan could pass.
- If both pass, proceed to source-fit GT-blind utility calibration, sealed
  source holdout reliability and a paired used ReLAION-1M external-query gate.

The resulting plans are oracle diagnostics and must never be served. They
cannot establish product recall, latency, throughput, RAM or an S3
Vectors/Turbopuffer comparison. The production policy remains generic in
`k`, target recall and measured resource costs; no corpus-size knee or fixed
100M memory ceiling is introduced.

Use one Causality Spot attempt, immutable source archive and attempt prefix,
terminal marker before reading outputs, streamed artifact readback and
immediate instance termination. Discard and restart a complete cell under a
new attempt if interrupted.
