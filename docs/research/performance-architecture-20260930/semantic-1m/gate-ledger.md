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

## 2026-10-02 measured failures and prospective replacements

| Gate and origin | Class | Current evidence / replacement measurement |
| --- | --- | --- |
| Local 8 GiB, zero new swap, sustained full-memory-PSI stop; operator host-safety direction | Host safety | No local Cargo remains; both paid jobs terminated. New Python workers are bounded to 200 MiB synthetic checks. Heavy builds stay remote. |
| Fresh1m 95% R10 / all64, [CoHere preregistration](cohere-quality/preregister.md) | Frozen experiment; practical representative-quality target | CoHere FIRST1M/newfixed64 R10=91.25%, R100=85.359375%, FAIL. Coverage586/640 before final ranking, returned584; no gate relaxation. New top32 coverage arm gets a distinct excluded panel and its own prospective bound. |
| Six client owners / 8 offeredQPS / ABBA/all64, [wave8 preregistration](startup-wave8/preregistration.md) | Frozen experiment | ReLAION control0 two drops at750/875ms, FAIL; wave8 unstarted. New protocol uses eight owners at unchanged8QPS and collects four declared cells after nonfatal science failures. Every failed cell stays FAIL; matched ratio only if allfour qualify. |
| Initial semantic walk <=1031 units, source-qualified Fresh1m implementation | Frozen implementation admission | Today guard remains; top32 may nominate2048 plus up to7 initial-page additions. Coverage-only falsifier is not admission/returned-quality qualification. Any native expansion requires explicit source-qualified2055-unit bound and source/SQ8 loss/resource measurement. |
| Selected top32 payload<=4MiB, nominations<=2048, closurepages<=2048; [new coverage preregistration](cohere-top32-coverage/preregister.md) | New prospective experiment admission | Derived from32 leaves of at most64 FP16 D768 unit records. Measure actual selectedbytes/coverage; these are not universal100M product caps. Passing608/640 closure hits is only a discovery ceiling. |
| Compiler8GiB/zero swap, seven affected/release/Clippy/testcompile gates, [wave8 qualification](startup-wave8/qualification-decision.md) | Host/experiment safety | Actual native399 identity7e4fab qualified, no full-workspace execution claim. Librarydelivery source matches every qualified Git blob. Native ANN RSS/maintenance/100M remain separate measurements. |
| Published1M D768444ms / 10M D10241214ms@8QPS context and practical95%R10, prior source-dated target matrix | Product comparison context | No matched vendor win. Latest ReLAION successful-only control p90=632.183ms/p95=777.308ms with two drops; candidate unmeasured. Published p95 and saturation unknown. CoHere cold latency/QPS unknown. Fresh matched end-to-end tails/completions/lifecycle dollars are next evidence; do not extrapolate100M latency. |

100M source/layout arithmetic remains a projection. Current1M build/scoring process peaks, cgroup peaks, scratch and router bytes do not prove generation-pin/swap/maintenance RSS,10M/100M latency or total dollar. Preserve historical GO/FAIL rows above.

## 2026-10-02 eight-owner matched result

[Closed paired a0002](startup-wave8/paired/a0002/decision.md) supersedes the earlier candidate-UNMEASURED checkpoint for the new protocol; the earlier six-owner FAIL remains unchanged. ReLAION FIRST1M D768 cosine/fixed64 reservoir1000–1063/k10/8 offered QPS/eight owners: all256 successful, every cell624/640=97.5%R10 and exact ordered source-reference parity. Candidate coldp90=461.848/452.096ms and p95=466.883/459.379ms; actualbracketing controlp90=601.051/497.652ms,p95=648.377/501.403ms. Each candidate beats BOTH controls; p90reduction7.19–24.78%,p956.88–29.15%.

| Requirement | Current measured attainment | Remaining decisive evidence |
| --- | --- | --- |
| Frozen paired all64/95%R10/dispatch/identity/resource/cleanup | PASS allfourcells; recall delta0pp, no drops/errors | Retain qualified width8 candidate; no identical repeat required |
| Published1M D768444ms context | Candidate misses by8.096–17.848ms under this disclosed different cold protocol | One causal stage-driven intervention; matched vendor comparison still absent |
| Throughput | 8 offered QPS/all64completed per cell; finite cleanup/drain-inclusive7.651–7.717successes/s for candidates | Sustained-load/saturation curve and cost per successful query unmeasured |
| Bounded serving memory | Native peak38.33MB; shared cgroup actualpeak178,855,936B/OOMswap0 | Generation swap/pins/maintenance/concurrency and10M/100M RSS not established |
| CoHere representative quality | Originalfresh64 R1091.25%/R10085.359375%, FAIL unchanged | Newly excluded64 panel, fixed top32 nomination/page-closure ceiling before native change |
| Physical S3/lifecycle dollar | HttpService calls and consumed payload measured; confirmed wire/billed request totals and total dollar unknown | Source-qualified physical accounting and build/maintenance/storage/recovery cost curve |

This is a matched internal native improvement and a runnable blob-native cold milestone. It proves neither a measured vendor win nor100M production readiness. Published vendor p95/saturation remain unknown; warm is separate.

## 2026-10-02 root-reuse pair: fixed FAIL, valid candidate cells

[Closed root-reuse a0001](startup-wave8/root-reuse/paired/a0001/decision.md), source34eccb89: ReLAION FIRST1M D768 cosine, fixed64 reservoir1000–1063, k10, eight cold owners/8 offered QPS. Candidate cells each64/64, 624/640=97.5%R10; measured coldp90/p95=455.282/463.225ms and448.433/480.708ms. Native process peaks39,424,000/38,014,976B. Finite completed/s7.685/7.708 includes drain/cleanup; saturation unmeasured. Actual control0 dropped ordinal10 with allownersoccupied,63/64; control3 passed64/64 atp90479.245/p95487.676ms. Frozen allfourcells gate FAIL, matched ratio UNMEASURED. Candidate product viability is retained, not retroactively converted into a campaign GO.

| Threshold | Origin/class | Decision/replacement evidence |
|---|---|---|
| All256 success, each candidate bothp90/p95 better than BOTHactualcontrols | Root-reuse prospective paired protocol; frozen experiment | FAIL from controlcapacitydrop. No identical automatic repeat. Any future capacity comparison must declare a new admission protocol before measurement. |
| >=608/640 each, source/scorer ordered parity, identity/resource/cleanup | Frozen current arm and practical quality context | Candidate624/640each and parity verified. Dropped control remains unsuccessful in all-offer population; no success-conditioning pass. |
| 444ms coldp90 published context | Product comparator context, not universal release guarantee | Candidate misses11.282/4.433ms. No measured vendorwin/p95/QPS/billedcost claim. |
| Top32 source-nomination/pageclosure>=608/640 on newCoHere64 | Distinct prospective discovery-only falsifier | Frozen configuration; no returnedquality/latency claim. Both policies sealbeforeONEGT; prior91.25%R10 FAIL preserved. |
| Local8GiB/no-new-swap/PSI safety | Host safety | No localCargo; heavy qualification alreadyremoteclosed0. Current independent maintenance planning is read-only. |

100M layout/RSS/scratch, pinned generations, incremental maintenance and totaldollar remain open. Fresh1m compaction currently rejects pre-sideeffect; allowing a benchmark does not qualify the production lifecycle.


## 2026-10-02 publication delivery and fixed48 next gate

[Publication a0002](bounded-publication/implementation-gates/a0002/decision.md)
closed0 and is integrated. It removes whole-SOURCE-plane hydration from local
publication validation and reserves live metadata before upload buffers. Exact399
source identities match qualified dc6151a4;29 focused tests, HTTP release,
workspace Clippy and real workspace test compilation pass. All14 bodies
authenticate and the Spot worker is terminated. This is a substantive library
increment, not a new recall, cold-latency, QPS or dollar measurement.

| Threshold | Origin/class | Current decision and replacement evidence |
|---|---|---|
| Compilation8GiB/no swap/jobs1/CPU200% | Qualification safety; not serving memory | PASS original10273; actual cgroup peak8GiB/max4451/OOM0/swap0. Changed-source full-workspace test execution remains separate. |
| Fixed48/3072 nominated units/3079 seed walk/512 closure pages/4096 source scores | [Prospective fixed48 protocol](fixed48/decision.md); new arm admission | Combined routing and truth-free scorer are integrated and qualified: original86025 CLOSED0, terminated Spot, full399 addf62bc and all16 bodies authenticate; seven scoped gates pass,28 focused tests. [Root verification](fixed48/implementation-gates/a0001/root-verification.json). Full workspace execution and scientific quality remain unmeasured. Next measure retained traces/source/SQ8 loss and actual memory; no historical cap relaxed. |
| Fresh64/128 consumed exclusions/population8996872/no replacement | Frozen metadata selector; experiment admission | ONE metadata selection completed,64 unique eligible locators and all body identities independently verified. Vector duplicate audit, ground truth and ANN measurements remain pending. |
| Reproduction12GiB memory/16GiB scratch/1800s/no swap | [Prospective artifact prerequisite](fixed48/artifact-reproduction-preregister.md); new remote experiment admission | Old measured scratch7,257,693,686B/cgroup7,932,358,656B informs headroom only. Reproduce original payload identities with the already qualified builder once; retain bodies. No query/truth or performance result. Helper/controller synthetic checks and immutable archived-builder authority pass. Original71180/a0001 remains FAIL. Distinct70059/a0002 CLOSED0 with all44 terminal bodies authenticated,30 retained artifacts and terminated instance. [Closed GO](fixed48/artifact-reproduction/a0002/decision.md). Preparation cgroup peak7,818,833,920B/scratch7,244,150,300B,swap0/OOM0; no serving RSS or ANN quality claim. These retained bodies admit the separate fresh scientific gate; no builder rerun. |
| Practical95% mean R10 and published cold comparison context | Product target; unchanged prior source-dated references | Latest ReLAION FIRST1M fresh64 candidates97.5%R10,p90448.433–455.282ms; matched campaign FAIL due control drop. CoHere top32 discovery page coverage606/640=94.6875%,below608; not returned recall. Fresh fixed48 measurements unknown. |

Convergence: delivered bounded publication; actual fixed48 quality delta,
end-to-end p90/p95, sustainable QPS and lifecycle cost are UNKNOWN. Both vendor
win and10M/100M bounded lifecycle remain unproven. Next decisive test is the
source-qualified fixed48 fresh64 one-pass production scorer; discovery and
returned-quality loss are separate gates. No consumed-panel retuning/rescore.
