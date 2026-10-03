# Architecture decision after the offline locality test

## Closed result

The single truth-free CoHere FIRST1M D768 sealed64 replay closed execution0/scientific **FAIL**, preserving the original gate. It authenticated the closed trace, real router directory/membership, generation/publication authorities and reconstructed old covers; it opened no query-vector or ground-truth body and ran no ANN. This is computed geometry from historical traces, not new serving latency, recall, RSS or a working reordered generation.

| Across64 queries | Query GETs | Query payload bytes |
|---|---:|---:|
| Qualified current arm | 8261 | 2393364520 |
| SOURCE32 arithmetic | 7142 | 2581780520 |
| Virtual semantic-unit reorder, preserving every required old row | **7168** | **4811184040** |

The candidate missed both aggregate thresholds. Every query exceeded this hypothesis's SQ8 byte cap; worst SQ8 payload was50918400B. SOURCE reached66451200B/query. All required rows were preserved. Result593441B; original execution1.17s, GNU-time maxRSS64740KiB, reported cgroup peak50.9M, swap0. These resource measures describe the Python analytical replay only.

Root cause: old nomination expands semantic units to complete physical256-row pages. Sorting units by semantic leaf makes those old neighbors scattered; retaining the old SQ8 gap rows adds another scattered population. Capping each tier at32 GETs bridges large gaps. This falsifies **reorder-only with unchanged closures**, not every hierarchical architecture. Do not tune this frozen gate or rerun it as a new success.

## Decision and implementation specification awaiting approval

Retain current library as the qualified control. Do not promote SOURCE32 arithmetic to measured speedup, spend on its isolated latency tweak, or implement reorder-only. Prefer a bounded **hierarchical directory → semantic cells → local nomination → bounded SQ8 refinement** prototype with changed nomination geometry. Keep current two-bit values and ranking arithmetic initially to isolate routing/layout; one-bit quantization is a separate future arm.

Concrete scope for approval: one100k D768 cosine prototype on ReLAION and CoHere, with authenticated source-order IDs and physical semantic-cell layout; bounded directory pages, explicit fanout and fetch-wave accounting; nomination within the selected cell rather than the obsolete physical-page closure; local refinement blocks colocated with those IDs. Keep data-independent routing/build decisions and split skewed cells by source geometry, without tuning on held-out truth. The first paired test must expose router, boundary coverage, local nomination and final-ranking losses separately. It must also measure actual fetched bytes/GETs, per-stage CPU/RSS, build scratch, and directory dependencies against the unchanged qualified control. Freeze prospective query splits and resource envelopes before any measured arm.

Qualify the exact prototype with affected tests, HTTP build, workspace Clippy and real workspace test compilation on the bounded remote build lane. Cheap authenticated real-fixture preflight and one disposable infrastructure canary precede any paid scientific run. Only a quality/resource Pareto survivor advances to fresh1M cold offered HTTP;10M/100M follow measured scale curves, not latency extrapolation.

Self-contained product work is mandatory: incremental deltas, authenticated pinned publication, restart/recovery and in-process cell-local compaction/GC with measured generation overlap. Current1M experiment profile rejects maintenance; historical100k lifecycle tests do not fill that gap. No production-ready or vendor-win claim until these requirements and matched end-to-end quality/latency/QPS/total-dollar evidence hold.

## Current performance context

Current CoHere FIRST1M D768 cosine k10 sealed64 remains96.71875% recall@10, coldp90/p95541.095/557.409ms,7.597 completed full-span QPS at nominal8. Current fixed48 ReLAION, saturation, lifecycle dollars,10M/100M and matched vendor measurements remain unknown. Prior ReLAION candidate evidence belongs a different source/panel and its frozen whole-pairFAIL remains unchanged.

The published Turbopuffer1M D768 coldp90444ms is a mismatched reference; AWS subsecond cold is directional, with no percentile guarantee. Neither proves parity or superiority. The measured POST CPU ceiling is131.094ms/query mean; eligible SIMD kernel CPU is unknown. Even halving all POST CPU gives only conditionalp90472.595ms, not a measured SIMD gain. Routing/layout/scale should be decided before kernel optimization. See specialist-reconciliation.md and critical-path-evidence.json for sources and100M arithmetic explicitly labeled projections.

Root owns this final protocol/selection decision and integration; bounded implementation remains delegated. No format rewrite or new paid ANN run is authorized by this document. Operator approval of the concrete prototype is the next decision.
