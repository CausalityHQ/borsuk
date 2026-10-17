# Native SOURCE capsules and one-pass verification

This is a qualified pre-release library increment, not completion of the ANN product goal.

- Adds generation-pinned flat/capsule layout selection, authenticated capsule publication/import/read paths and tested store/reopen/compaction/GC integration.
- Transfers an opaque verified proof into planning, eliminating repeated authentication and temporary lookup construction. Public corruption checks remain enforced.
- Generic dimension and bounded-admission tests cover D1, D257 and D1024. Native compilation, release, workspace Clippy and full test compilation passed on Causality EC2.

## Results

| Evidence | Quality | Performance interpretation |
|---|---:|---|
| Old/new capsule ABBA, fixed32 | All320 ordered ranks/scorebits/plans/charges equal;309/320 hits | Query CPU lower25–27%, planning time lower59%, serial QPS higher23–51%; p95 improved in one comparison and regressed in the other. Not population percentiles. |
| One-pass capsules, full1000 |9783/10000; all10000 ranks/scorebits and1000 plans/traces/recall rows exactly equal historical flat |p50 199.38ms,p90 253.27ms,p95 281.48ms,p99 360.74ms,serial4.776QPS,114.5MiB peak. |
| Historical flat full1000 |9783/10000 |p95 223.23ms,serial6.123QPS. Historical, not contemporaneous paired timing. |
| S3 Vectors reference full1000 |9571/10000 |p95 211.89ms,serial7.402QPS. Cache conditions differ; no measured vendor win. |

Capsules reduce SOURCE logical GET charges109872→24710, but increase SOURCE verified bytes28967583744→41963275448. SQ8 charges remain30041 GET/10722616576 bytes. These are application charges, not wire/billed counters.

Keep the CPU/GET-efficient capsule option and the flat latency incumbent. Do not claim an overall tail-latency win or change historical gates. Next investigate byte amplification from actual native geometry and independently bounded download/verification/resident-memory tiers. Matched Turbopuffer, full lifecycle/concurrency/large-scale qualification and registry packaging remain open.

All preparation, compilation, native tests and data work ran on persistent Causality EC2/EBS. Delivery uses Git-object integration only and preserves unrelated main assets and the dirty root checkout. Native input equivalence is recorded in native-inputs.tsv and verification.json. Historical source/format/protocol failures and their original results are retained.
