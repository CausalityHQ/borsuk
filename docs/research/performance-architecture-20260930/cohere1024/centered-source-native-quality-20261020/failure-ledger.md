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
| Historical V139/V146/V149/V150 centroid layouts | Reconciled below: threshold budget tail, linear scan CPU, weak physical hierarchy and unit-to-page slot loss are distinct failures. | Do not repeat frozen policies or interpret surrogate page capture as final recall. |

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
## Reconciled historical centroid-layout failures (2026-10-20)

The root read all four committed closeouts on EC2. V146/V149/V150 terminal objects were fetched from their original S3 paths and independently matched the closeouts' SHA256 pins; decision and small summary artifacts then matched each authenticated terminal's byte lengths and hashes. This does not claim a fresh recount of all raw query rows. V139 has only its historical exploratory local preflight closeout here, not a remote native measurement.

| Arm | Actual rejection and measured distinction | What must differ before revisiting |
|---|---|---|
| V139 threshold admission | ReLAION D768:364/1000 queries exceed16MiB; mean minimum72,544,992B. Threshold has no budget. D96 fetched-range coverage99,297/100,000 was below99.5% screen. Historical local exploratory dry run, no Spot campaign. | Budgeted ranking rather than unbounded threshold; do not infer centroid ranking is useless. |
| V146 flat centroid scan | Exact page plans matched1000/1000. ReLAION1M score+planner p95=11.071522ms rejected10ms gate;96,125,000B logical arrays. Linear ceil(N/32)*D work. | Sublinear discovery with a measured quality/work tradeoff; SIMD or relaxed gates do not remove scaling. |
| V149 hierarchy over consecutive physical pages | D96 used100k split: selected-page capture77.0956% vs95% gate; p95 search+planner0.641749ms vs historical flat0.530383ms. Visited page scores exact, planner rejected0 visited baseline pages. | Better semantic guidance than broad overlapping physical summaries; measure final recall, not only surrogate plan capture. |
| V150 semantic unit graph | Capture73.3549%;970/1000 worse shortfall; p95 CPU0.495937ms vs paired flat0.547917ms. All queries exhausted16P work. Repeated units on same page consumed result slots;13,285 missing pages had evaluated units but no returned unit. | Explicit distinct-page discovery/slot allocation on a fresh split, not M/ef/8P/16P tuning on reused queries. |

V150's committed postterminal diagnostic reports99,523/100,000 truth positions in fetched ranges versus control99,738. This was not returned recall or a preregistered pass. It demonstrates that exact baseline-page capture can be an overly blunt rejection screen; a changed router needs actual returned quality and lower-tail recall. This diagnostic's raw artifact was not independently re-fetched in the current reconciliation.

Original closeouts: [V139](https://github.com/CausalityHQ/borsuk/blob/main/docs/research/v139-centroid-page-preflight-closeout.md), [V146](https://github.com/CausalityHQ/borsuk/blob/main/docs/research/v146-unit-centroid-score-closeout.md), [V149](https://github.com/CausalityHQ/borsuk/blob/main/docs/research/v149-contiguous-hierarchy-closeout.md), [V150](https://github.com/CausalityHQ/borsuk/blob/main/docs/research/v150-unit-centroid-graph-closeout.md).

Authenticated original terminal SHA256s:
- V146:45d98502b274be939c9e9fd4ba4fca2033cdb3a0323cf28a6148ffd88a095086; decision reject-cpu; summary169d9cb71401c183a63c27846063f0b71491cde852b43c0693f3af94bbe36579.
- V149:42d054f0dd0c4ae4dd754bd505a85eeab9f7e11e51d258a52262a022dab0a8f4; decision reject; summary75d01f19d965b676858c59bce43d6e9940265ecc5c633e57a99577a9ef8d51ce.
- V150:c00e15182688a17b94f2182c30f3144d6cb9c2a5e57ecfa0235eb842790a7e07; decision reject; summary6cba22618ffd2208039e275b34b50575e1638e4aaa61411069aac614a05574d9.

Owned reconciliation evidence: EBS evidence/s3-canonical-root-prep-a0001/prior-closeouts/. Original source documents transferred as an exact Git archive, SHA15229044878ee2f043a5fba848f589241fcbe1ef0b0db5a9ffde4bb422d0a90a, under the same S3 campaign prefix as decision.md. No old benchmark or new dataset campaign was rerun.
