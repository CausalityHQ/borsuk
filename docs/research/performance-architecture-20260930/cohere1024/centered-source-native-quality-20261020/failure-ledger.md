# Research failure ledger

This ledger prevents repeating rejected arms and separates scientific results from invalid execution. Historical artifacts remain immutable. Status as of 2026-10-20.

## Current anchor

The library is not production ready and has no matched vendor win. S3 Vectors is the first comparison target; Turbopuffer follows. Evaluate the generic library using the frozen Cohere1M/D1024/cosine/k10 fixture. Do not embed this workload in library routing, formats or scoring.

Current main before these documents: `999b3ff79098d7297b75b5f865f93e76bbbb1471`. The centered SOURCE candidate is unmerged. See [its decision](decision.md) for exact source and immutable S3 archives.

## Observed outcomes

| Arm or procedure | Evidence and outcome | Decision / condition for revisiting |
|---|---|---|
| Centered SOURCE end-to-end candidate50f76f | Full1000 9901/10000 versus9902; query428 lost one truth ID through changed SQ8 bridge coverage. Common9999 IDs have identical f64 scores. Bytes/query fell ~33%, GET/query169.332. | Valid negative under original nonregression gate; retain Pareto candidate, do not adopt default. A new general coverage rule requires a new preregistered test, not a query428 patch. |
| Current request schedule | Frankfurt GET-only ~$72.81/million queries; excludes compute/lifecycle. S3 Vectors vector-only tariff scenario ~$11.48/million. | Not cost competitive under this scenario. Reduce requests without simply shifting unacceptable cost into bytes/CPU. Scenario is not measured TCO. |
| Fixed-layout range fusion | Offline oracle makes SOURCE free and preserves consumed SQ8/canonical population. At64MiB p95 payload cap, optimistic17.587 reads/query; at256MiB,11.018. | Bound only: no native policy, latency or memory proof. An optimistic pass is not authorization for scale. |
| Progressive high-bit box bounds | 0 of35640 full32 units safely skipped in retained diagnostic. | Stop this box-bound arm; require a materially different bound and cheap falsifier before revisiting. |
| Direct full SOURCE closure | 32GET/256MiB failed942/1000 closed queries;128GET fit with max~227.6MB SQ8. | More requests/bytes; no demonstrated economic win. Do not repeat32GET envelope. |
| Compact lookup24 / prefix68 / AVX2 gather | Native correctness/model work completed; retained primitive measurements showed CPU regressions. | Original portable256 remains incumbent. No SIMD proposal without measured stage bottleneck and same-source/recipe controls. |
| AVX2 implementation evidence | Hardware path required safe borsuk-fma boundary; logical memory charge was not full physical stack/RSS. Combined bin+example build changed dependency features relative to old bin-only control. | Do not use differently featured binaries as a causal cold pair. Preserve actual codegen/frame evidence. |
| Shared Cargo target baseline | Baseline and candidate ELF unexpectedly identical; Cargo fresh=true reused candidate output for old checkout. | Original control invalid; corrected package rebuild/fresh=false and differing ELF required. Never accept command exit alone as source identity. |
| Historical S3 Vectors9617/10000 | Older ENHANCED run had source, timing, retry, resource and cache mismatches with current BORSUK. | Historical only; cannot claim matched superiority. Fresh canonical-input runner required. |
| Historical V139/V146/V149/V150 centroid layouts | Required receipt-level reconciliation is not yet complete in current handoff. | Evidence gap, not blanket proof against centroid layouts. Retrieve exact protocols/failures before selecting a related arm. |

## Execution defects are not algorithm failures

Retain original failed receipts for missing linker/toolchain, sparse inherited missing Git objects, interrupted Spot jobs, unsupported shell pipefail, stale fixture schemas/sidecars, writer caps, omitted imports, wrong strict-reader geometry fields and controller time guards. Classify these as execution INVALID or fixture failures until evidence says otherwise. Fix the demonstrated defect and rerun only affected checks on a recorded revision.

Specific resolved traps:
- Tiny centered residuals must be formed before rotation; H(x)-H(center) cancelled them. Native red/green witnesses preserved.
- Telemetry and reader scratch/mean widths must use the actual codec helper (centered f64 mean8D).
- A panel can pass while full1000 fails; do not stop at convenient subsets.
- QPS uses cohort elapsed; sum of individual wall times is a different diagnostic.
- Application cache disabled does not prove a cold provider cache.
- A launch/queue receipt does not prove a specialist read a brief or a job completed.
- A future-stage guard refusing release is UNSTARTED, not a failed release or qualification pass.

## Revisit checklist

Before repeating any arm: locate its source/config/protocol and original terminal result; state the new causal difference; identify why the previous failure no longer applies; preregister quality, requests, bytes, CPU, memory and total-cost bounds; run the cheapest native falsifier. Unknown evidence stays unknown.
