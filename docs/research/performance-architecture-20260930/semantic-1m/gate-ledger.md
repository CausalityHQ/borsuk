# Semantic gate ledger

Operator correction: historical RAM, scratch, GET, byte and quality gates belong
to their frozen source, format and protocol. A router/index format change needs
new admission and measurements; it does not inherit a universal product floor.
Preserve every historical GO/FAIL. Local host safety remains in force.

Evidence baseline: `17cc8050c38c9e00e063a1fb02ee1c81660db8fe`. The newer
[remote implementation a0002 decision](implementation-gates/remote-implementation/a0002/decision.md)
and [verification](implementation-gates/remote-implementation/a0002/verification.json)
were inspected from committed `30644c7bd1cbc5a8be8ba32c53b222661ee19d78`;
these two files are outside the baseline checkout. Exact native identity
`c9ecb3ac7138978b45efde523c8e96a88811d559e50906c178fd4cb602fb9ff5`
passed affected tests, release compilation, workspace Clippy and real workspace
test compilation remotely. Full workspace **execution**, new 1M ANN quality,
cold latency, throughput and corpus-build RSS remain unknown. Compiler memory
evidence does not qualify serving or corpus-build RSS.

## Latest delivered evidence

The unchanged production native source subsequently completed [actual remote full workspace execution](implementation-gates/remote-full/a0001/verification.json): 2746 passed, 25 ignored, zero failures. The one-file diagnostic scorer repair separately passed affected tests, release, Clippy and real test compilation ([source-bound evidence](implementation-gates/scorer-file-adapter/verification.json)). No full-workspace execution claim is made for the changed whole-tree digest.

| Current gate | Baseline / candidate | Actual result | Remaining product gap | Next decisive test |
| --- | --- | --- | --- | --- |
| ReLAION FIRST100k D768 cosine, consumed development queries 0–63; local FileObjectStore | Frozen historical R@10 628/640; current v8 R@10 628/640 | R@10 98.125% (0 pp delta), R@100 6217/6400 = 97.140625%; all 64 completed. [Closed result](positive-control/v8-a0002/verification.json) | Fresh 1M quality, cold HTTP tails, saturation QPS and lifecycle dollar remain unmeasured | Fixed fresh 1M construction and admitted production scorer |
| Metadata selection | Frozen 32-object deterministic continuation | Selector subprocess 0; original campaign FAIL from final strict peak assertion. [Exact panel input disposition](panel-tools/input-disposition.json) preserves that FAIL | IDs alone establish no vector uniqueness, oracle correctness or ANN quality | Authenticated vector/duplicate audit, exhaustive GT and sealed readback |

| Fresh 1M fixed-panel construction a0001 | Same authenticated FIRST1M parquet/raw authority and 64 selected IDs | FAIL before vector decoding: indexed feature-ID Arrow type rejected. All 10 bodies authenticated; peak service 1,643,614,208 B, no OOM/swap/max events; owned instance terminated. [Closed evidence](panel-tools/remote-construction/a0001/verification.json) | No new queries/truth or ANN measurement | Establish source writer schema, repair only the admission bug with regression, then freeze a distinct construction attempt |

| Fresh 1M fixed-panel construction a0002 | Same source and IDs as failed a0001; only exact indexed UInt64 admission repaired | GO: 64 fixed queries, duplicate audit against indexed 1M + consumed 1000, exhaustive k100 truth + remote replay, sealed readback; all 20 bodies authenticate. [Closed evidence](panel-tools/remote-construction/a0002/verification.json) | No fresh 1M ANN recall/latency/QPS/cost measurement | Admit complete SQ8/id/order and run the frozen current-v8 Fresh1m builder/scorer |

The local quality run's 9.13 s wall and 51,872 KiB process RSS are diagnostic process measurements; neither is a cold S3 latency or total cache-memory result. Historical v7 cold measurements are not matched current-v8 controls. No vendor win is claimed.

## Thresholds and scope

“Frozen experiment” includes format/admission contracts and proposed envelopes
for the named arm; a proposal is not an observed pass. “Current product target”
is a selection objective, not an inherited release gate or a measured vendor win.

| Threshold / originating source | Protocol | Class | Replacement measurement needed after a format/architecture change |
|---|---|---|---|
| Local **8 GiB, zero swap**; monitor host/cgroup PSI and stop/collect sustained pressure. [Local stop receipt](implementation-gates/local-candidate/resource-pre-stop.json.gz), [implementation preregistration](implementation-gates/remote-implementation/preregister.md) | Shared devbox compile/qualification safety; one serial compile lane. The receipt records a pressure stop, not a numerical PSI pass threshold. | Host safety | Retain the safety envelope and pressure monitoring. Record effective limits, RSS/cgroup peak, reclaim/OOM events and terminal status; do not raise local limits to rescue a product experiment. |
| Historical Native100k: centroid input ≤8 MiB, modeled construction payload ≤128 MiB; fit process ≤256 MiB. [Falsifier](../semantic-router-falsifier.md), [fit gate](../semantic-router-fit-gate.md) | FIRST100k D768 original FP16 unit means; historical JSON router | Frozen experiment | Re-prove live construction allocations for the new trainer/layout and measure actual process/cgroup peaks. These are three distinct budgets, not one RSS limit. |
| Historical router root ≤1 MiB; Fresh1m proposal: exactly 1M/D768, root ≤4 MiB, construction payload ≤512 MiB under a separate 1 GiB process ceiling. [Scale audit](../semantic-scale-admission.md), [implementation plan](../semantic-1m-implementation-plan.md) | Old JSON admission versus explicit Fresh1m binary-root/v8 arm | Frozen experiment | Authenticate format/profile and exact lengths; measure build, publication validation, parse and resident memory separately. Larger scales require their own profile and envelope. |
| Scorer **512 MiB** cap; **400,000 B** production preparation **plus 59,608 B** separately charged trace at 1M, total **459,608 B**. [Corrected scratch receipt](implementation-gates/scratch-candidate/verification.json), [implementation preregistration](implementation-gates/remote-implementation/preregister.md) | Corrected c9ec diagnostic arm; scratch accounting is separate from scorer process memory | Frozen experiment | Measure actual RSS and retained trace/order-map allocations. Do not subtract trace storage from production preparation, treat scratch as total query memory, or make this arm's cap universal. |
| First 8 leaves, boundary multiplier 1.15, maximum 16; selected leaves ≤16 reads/2 MiB. [Falsifier](../semantic-router-falsifier.md), [Fresh1m plan](../semantic-1m-implementation-plan.md) | Unchanged nomination heuristic, whole-leaf admission and seed completion | Frozen experiment | Fresh truth coverage and returned recall, selected leaves/units/page closure, resident router bytes and leaf requests/bytes. A new nomination policy needs a distinct frozen arm. |
| Source ≤128 GETs/64 MiB/16 parallel; SQ8 ≤32 GETs/16,773,120 B; combined ≤160 GETs/83,881,984 B, with router/startup I/O additional. [Scorer gate](../semantic-router-scorer-gate.md), [Fresh1m plan](../semantic-1m-implementation-plan.md) | Logical planned ranges in offline scoring; submitted transport attempts separately counted in HTTP | Frozen experiment | Measure logical ranges, submitted/confirmed requests, consumed/verified/wire bytes, retries and source/SQ8 coverage under the new physical layout. Keep these caps fixed for the existing arm; re-freeze a replacement arm before execution. |
| Metadata waves ≤4 objects; aggregate range concurrency ≤8 GETs; conservative payload buffer ≤32 MiB. [Paired gate](../semantic-cold/metadata-waves/paired-gate.md) | Scheduling intervention on identical historical semantic generations/scorer | Frozen experiment | New-format metadata critical path, decode peaks, buffers and concurrent request overlap; do not sum overlapping object waits. |
| All 64 results valid and ≥608/640 R10 hits (95%); report R100 and loss stages separately. [Fresh-panel preregistration](../semantic-1m-fresh-panel-preregister.md), [implementation preregistration](implementation-gates/remote-implementation/preregister.md) | New ReLAION-first 1M development panel; CoHere only after survival; no retuning after failure | Frozen experiment | Fresh returned recall and nomination/source/quantization loss at each scale. Historical 100k recall and consumed panels cannot establish new-scale quality. |
| Positive control: historical 628/640 candidate R10 hits and ordered-result/source parity. [Input audit](positive-control-input-audit.md), [implementation preregistration](implementation-gates/remote-implementation/preregister.md) | Rebuilt v8 Native100k on consumed ReLAION ordinals 0–63; harness wiring check | Frozen experiment | Authenticate raw source, physical-to-source truth mapping and new scorer outputs. Historical hit count is a parity reference, not a new quality floor or vendor comparison. |
| Candidate p90 below fresh control, p95 no higher, both datasets. [Paired gate](../semantic-cold/metadata-waves/paired-gate.md) | FIRST100k consumed development ABBA; cold timer ends at wire response, before validation/cleanup | Frozen experiment | Fresh matched tails at the new format/scale and declared coldness. Preserve the coded historical decision despite earlier cleanup-inclusive prose. |
| .25/.5/1/2/4/8 offered QPS; 64 offers/cell, six workers; lateness ≤125 ms; native ≤512 MiB/shared ≤8 GiB, zero swap/OOM. [Offered gate](../semantic-cold/metadata-waves/offered-gate.md), [config](../semantic-cold/metadata-waves/offered-config.json) | FIRST100k consumed development; full-span QPS includes validation/drain/cleanup; response latency excludes them | Frozen experiment | New-scale offered and completed throughput with failures/drops/aborts, sustained-load saturation, shared peak and concurrency admission. An attained finite cell at 8 offered QPS is not sustainable 8 completed QPS. |
| Product selection: fresh matched recall, cold latency, QPS and lifecycle dollar. Recorded published context: 1M D768 cold p90 <444 ms; 10M D1024 cold p90 <1214 ms at 8 QPS. [Scale audit](../semantic-scale-admission.md) | Vendor publications have different corpus, geometry and coldness/load protocols; no matched vendor measurement here | Current product target — numeric thresholds unset; publications are context | Fresh matched recall, cold latency/tails, completed QPS and lifecycle dollar against each vendor. No numeric product release floor is selected here; do not transplant the contextual goals to 100M. |

## Preserved results

- [Metadata-waves paired a0001](../semantic-cold/metadata-waves/a0001/decision.md)
  remains **GO**: FIRST100k D768/k10 consumed development, 512 successful calls.
  Recall@10 is 98.125% ReLAION and 95.78125% CoHere for both roles; candidate
  p90 is 452.618/436.060 ms versus control 566.179/572.076 ms. It establishes
  neither cleanup-inclusive latency nor sustainable QPS.
- [Metadata-waves offered a0001](../semantic-cold/metadata-waves/offered/a0001/decision.md)
  remains **GO**: all 1,536 calls succeeded. At 8 offered QPS, candidate
  full-span completion is 7.7007/7.7462 QPS. CoHere candidate p99 worsened to
  676.132 ms versus control 624.850 ms. Native candidate peak RSS at that rate
  is 36,720,640/36,237,312 B; shared cgroup peak is 240,787,456 B. These are
  measured 100k development values, not 1M evidence or a lifecycle dollar win.
- [Remote implementation a0001](implementation-gates/remote-implementation/a0001/decision.md)
  remains **FAIL**: missing Clippy component, test-build unrun. The later
  committed a0002 implementation **GO** above does not rewrite this failure or
  establish full workspace execution/ANN performance.

## Scale arithmetic and prospective envelopes

The following bytes come from [semantic-scale-admission.json](../semantic-scale-admission.json),
source `c2e74dfa`, historical D768 layout. **All are arithmetic, not measured
RSS, necessary working sets, latency, quality or costs.** The recursive model
is deliberately loose and belongs to that trainer/accounting revision.

| Rows | Original FP16 centroids, B | Flat f32 prototypes, min–max B | Encoded membership, B | Old modeled construction charge, B |
|---:|---:|---:|---:|---:|
| 1M | 48,000,032 | 1,502,208–3,001,344 | 125,000 | 1,655,665,240 |
| 10M | 480,000,032 | 15,000,576–29,998,080 | 1,250,000 | 104,130,629,360 |
| 100M | 4,800,000,032 | 150,002,688–300,002,304 | 12,500,000 | 9,830,202,793,440 |

The [Fresh1m planning arithmetic](../semantic-1m-plan-arithmetic.json) replaces
the old construction charge with a lifetime-based **400,316,456 B modeled
generation-build bound** and a **3,064,384 B maximum binary root**. Neither is
observed RSS. Serving allocation formulas in the [implementation plan](../semantic-1m-implementation-plan.md)
also remain estimates; corrected diagnostic storage must be charged in addition.
Neither the old 100M model nor the new 1M model sets a universal RAM floor.

| Measurement / prospective envelope | 1M | 10M → 100M |
|---|---|---|
| Stage RSS and payloads | Measure trainer, generation build, publication validation, metadata parse/open, first query and teardown independently under the frozen arm's limits. | Measure the same stages on each exact candidate; record allocator/stacks/page-cache effects and shared peaks. Freeze scale-specific payload and process caps before execution; no numeric cap selected here. |
| Scratch and resident router | Charge production preparation, trace, truth/order maps, encoded/decoded root, membership and selected-leaf/source/SQ8 buffers separately; record peak filesystem scratch during build/validation and its cleanup. | Measure allocation lifetimes, filesystem scratch and resident growth for the actual representation. Prototype/layout arithmetic alone cannot establish resident memory or require full-centroid residency. |
| Router/source GETs and bytes | Record root/authority/membership/leaf plus source/SQ8 totals, physical fragmentation, bridge gaps, failure/retry charges and unknown wire traffic. | Re-measure selected closure and remote I/O per query under fresh quality constraints. Freeze a replacement envelope if the format changes; do not silently extend the existing arm. |
| Generation pins and concurrency | Measure retained old generation plus new generation during swap and overlapping queries; validate pre-allocation rejection and shared memory. | Exercise multiple live pins and concurrent load; derive admitted concurrency from measured memory/quality/latency, not a sum of isolated RSS maxima. |
| Maintenance and lifecycle dollar | Fresh1m maintenance remains unqualified; the [implementation plan](../semantic-1m-implementation-plan.md) requires unsupported compaction to reject before work. Measure build/ingest, mutations, compaction, recovery, GC and swap peaks when supported. | Measure maintenance while serving, rebuild amplification, temporary storage, retention/pins and recovery. Include compute, storage, requests, transfer, retries and idle lifetime in total dollar; quote/cap arithmetic is not billing. |
| Quality, cold latency and QPS | New panel R10/R100 and loss stages first, then cold/open tails and offered/completed/sustained throughput on the frozen survivor. | Fresh matched tests per scale with explicit geometry, cache/coldness, load and failure population. Set prospective product envelopes from the measured curve and lifecycle dollar; published targets remain context. |

Product selection therefore requires fresh matched recall, cold latency,
completion/saturation QPS and lifecycle dollar on the chosen frozen revision.
This ledger changes no experiment's outcome and selects no new production default.

## Fresh 1M quality survivor

[Closed a0001](quality-execution/a0001/decision.md): ReLAION FIRST1M D768 cosine, fixed fresh64; R10 624/640=97.5%, R100 6077/6400=94.953125%. GO against prospective 95% R10 gate, not the historical 98% R100 arm. All21 bodies authenticate and independent truth recount agrees. Build1353.076s under1GiB service with substantial reclaim; no cold HTTP/QPS/cost claim. Next: unchanged-source object-native cold measurement with explicit build/serving envelopes.
