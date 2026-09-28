# Complete native preparation: KILL for cloud promotion

| Dataset/split (first100k, D768 cosine k100) | Mean returned R@100 | p05 returned hits | Paired native SQ8 flat mean | Deficit | Gate |
|---|---:|---:|---:|---:|---|
| ReLAION development0–63 (64 queries) | 99.156% | 97 | 99.516% | 0.359pp | Short pass only |
| CoHere development0–63 (64 queries) | 99.094% | 98 | 99.234% | 0.141pp | Short pass only |
| ReLAION validation256–999 (744 queries) | 98.851% | 97 | 99.569% | **0.718pp** | **KILL: exceeds0.5pp** |
| CoHere validation256–999 | Not run | Not run | Not run | — | Stopped at first hard failure |

All evaluated plans respected32 ranges and16,773,120 bytes. These are verified
offline Rust plans plus the established Python f32 SQ8 scorer, not physical
S3 GETs, cold HTTP latency, throughput, serving memory or a vendor comparison.
The root/SQ8/plan identities, exact counts, per-query samples, phase times and
terminal status are preserved. Original completed source-only preparations
remain immutable on Spark. No paid/cloud run or parameter sweep was started.

## Causal evidence and product decision

ReLAION fetched GT coverage is99.224% mean/p05 97; returned is98.851%. Its
native flat mean99.568548% and p05 99 exactly match the prior approved-layout
native-SQ8 replay. The prior returned mean was99.163978%, a0.405pp loss to flat.
Thus the new complete preparation is not approved by the unchanged gate.
Selection/page coverage is the first visible failing layer; the current plan
output does not separately identify centroid discovery, two-bit nomination and
budgeted page selection. Do not pretend that decomposition has been measured.
Changed source sampling/initialization/arithmetic/physical grouping is the
candidate difference; normalization/quantization is not implicated by the
unchanged flat result. The source-only page oracle's100-hit upper bound did not
predict returned recall and is not a product success.

KILL this native source-fit v1 as a cloud/scale/release default. Preserve the
approved historical Python-layout candidate and its exact roots. No1M run,
CoHere full panel, seed change, relaxed threshold or larger GET budget follows.
The flat fitting complexity also remains unsuitable as a qualified100M build.

Next design decision: one balanced, source-only physical-page partition with
bounded working memory and hierarchical fitting cost, keeping the two-bit/SQ8
serving recipe fixed. First reconcile the failed hierarchy arms and get the
required read-only review; use a cheap development-only coverage/selection
falsifier before full generation promotion. Do not tune on the consumed
validation panel or call its later reuse fresh evidence. Incremental ID/lifecycle
and fresh cold/vendor/scale gates remain unfinished.

## Execution receipts

The original build session3661 exited0; transfer47488 exited0. First campaign
session71019 exited1 before quality scoring: the harness supplied manifest
file instead of generation directory to the planner. Both source preparations
had finished. The corrected score-only session36732 authenticated their raw,
normalized, order, SQ8 and root identities and reused them; it exited0 with a
KILL decision. No preparation was duplicated. The failed logs/phase times are
preserved under `pre-plan-failure`; the correction changed no source algorithm
or benchmark threshold. This is a method failure, not a harness crash.
