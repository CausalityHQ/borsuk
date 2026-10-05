# Closed source-witness representation decision

The original native experiment completed with scientific **FAIL**. The outer infrastructure campaign is **INVALID** because closeout resource validation rejected its final snapshot. Both statuses are preserved. The instance `i-094c252c6d18eebc3` was terminated and waited.

An independent offline replay authenticated the original terminal inventory, all 53 retained native files, unchanged 402-source qualification, four exit-zero native calls, frozen selections, native resource and cleanup receipts, and the original reports. No native measurement was repeated. The authenticated transport is retained in S3 and externally at the path pinned by root-audit.json.

## Measured coverage

Both panels are consumed 64-query splits on FIRST100k, D768 cosine, k100. This measures selected-cell exact-GT coverage; returned recall and physical S3 query performance were not measured.

| Dataset | Earlier same-layout selected coverage | Source witnesses | Source-witness p05 hits/100 | Frozen gate |
|---|---:|---:|---:|---|
| ReLAION | 87.109375% | 76.765625% | 37 | FAIL |
| CoHere | 77.484375% | 61.390625% | 30 | FAIL |

The frozen gate was mean coverage >=98% and p05 >=95 on both panels. Optional returned-ranking diagnostics were correctly skipped after coverage failure. There is no vendor comparison or fresh held-out result.

## Measured work and limits

Sixteen source-only witnesses per cell, scored globally by nearest witness, selected exactly 24 unchanged cells. Mean route CPU was 2.380138 ms on ReLAION and 2.451547 ms on CoHere. Route-only wall p95 was 4.416708 and 8.452654 ms, respectively; these are not end-to-end latency or cold S3 tails. Actual payload query GETs were zero. Hypothetical whole-cell fetch was 24 GETs and mean 7,768,535.9375 / 7,931,633.125 bytes. CPU, payload and memory cannot establish a 100M performance claim.

## Decision

Reject this nearest-source-witness routing arm. It reduces selected-cell coverage on both unchanged layouts, so faster witness scoring cannot recover the lost neighbors. Do not rerun it, widen its frozen beam, or promote it to fresh1M/cold HTTP. Preserve historical balanced-partitioner FAIL separately.

The next Rust intervention must change routing or the layout's geometric neighborhood coverage and pass a cheap native correctness and paired recall falsifier before any scale run. Evaluate a query-specific candidate index rather than repeating centroid/witness proxy selection on these diffuse cells. Keep SQ8 scoring and the paired source identities fixed when testing that causal change. Select the exact intervention from existing research and the closed loss receipts; SIMD is justified only after a viable representation has measured compute dominance.

The closeout guard combines several conditions under one error, and its raw final snapshot is absent. Source calls resource_snapshot with the 256MiB outer cap, including cumulative kernel memory peak. The exact failed field is not recoverable from the original artifacts; do not claim that a specific limit was exceeded. This infrastructure defect does not justify normalizing the whole campaign to GO.

## Post-result layout oracle changes the next intervention

An independent posthoc upper-bound audit authenticated every directory/cell span and the complete 100k source-ID bijection, then counted LEu32 exact truth by cell. For each query, choosing the cells with the largest truth counts maximizes coverage at that GET count; these optima all fit 16MiB. This uses truth, so it is **not a deployable router, new ANN result, or fresh validation**. It bounds what any selector of these unchanged cells could accomplish. An independent recount exactly reproduced both native mean/p05 coverage results.

| Dataset | Perfect 24-cell mean / p05 | Perfect 32-cell mean / p05 |
|---|---:|---:|
| ReLAION | 97.640625% / 87 | 99.421875% / 95 |
| CoHere | 92.625% / 80 | 97.171875% / 88 |

Therefore a routing-only replacement selecting at most 32 of these unchanged whole cells **cannot meet both frozen quality thresholds on CoHere**, even with perfect knowledge of truth. Merely swapping witnesses for a compressed row graph that still fetches the same whole cells is insufficient under that envelope. The next causal design must change physical grouping/overlap, refine fetch granularity, or prospectively change the resource arm. Do not spend a paid run proving an impossible unchanged-layout gate. The 32-GET/16MiB arm remains a historical experiment contract, not a universal product limit.

The oracle audit completed in 4.659s at reported109.3M peak, CPU1/256MiB/no swap. Its first setup attempt assumed the wrong truth width and exited before any result; that source snapshot and terminal metadata are preserved. No native query was rerun.
