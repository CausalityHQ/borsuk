# Proposed cheapest next causal falsifier — HOLD

Keep the quality incumbent at leaf limit 64. Test the hypothesis that the current fetch scheduling envelope, rather than nomination breadth alone, sustains the cold SOURCE tail. This is an inference from closed timings, not an established diagnosis. No experiment is authorized or launched by this document.

## Closed observations selecting the question

[stage-summary.json](stage-summary.json) reduces all 272 closed panel queries from A1/B1/B2/A2. The input SHA-256 values match the report and root proof. For each named stage, duration is `end_ns - start_ns`; percentiles use nearest rank. Residual is query wall minus the union of discovery/SOURCE/planning/SQ8 intervals. Residual includes work outside those intervals; it is neither CPU nor network time.

| Cell | SOURCE share of query wall | SOURCE median / p95 ms | SQ8 p95 ms | Residual p95 ms |
|---|---:|---:|---:|---:|
| A1 | 44.95% | 71.733 / 262.927 | 102.934 | 125.655 |
| B1 | 45.53% | 66.677 / 259.996 | 122.858 | 99.904 |
| B2 | 44.10% | 62.013 / 261.958 | 96.442 | 105.685 |
| A2 | 49.72% | 71.425 / 263.685 | 101.817 | 110.639 |

SOURCE dominates aggregate measured interval wall, but reducing breadth leaves its p95 near 260 ms. Planning is only 3.6–4.5% of total query wall and discovery below 0.9%, so a planning/discovery-only optimization has a small aggregate ceiling. SQ8 and residual remain substantial; SOURCE alone cannot explain every slow query. B1 ordinal 468, for example, has about 70.7 ms SOURCE but 255.0 ms residual. Ordinal 375 is a tail in both A cells and both B cells. Do not sum stage percentiles or infer stage CPU from these intervals.

## Simulated perspectives, not expert testimony or evidence

| Simulated perspective | Argument and prediction | Objection and falsifier |
|---|---|---|
| Retrieval | Preserve leaf64 and shortlist16; a scheduling-only change should preserve IDs, score bits, ordinals, nomination plans, shortlists, charges, and 674/680 panel hits. | A scheduler can expose ordering/cancellation bugs. Any semantic/reference mismatch invalidates this performance comparison; never accept a quality trade for this test. |
| Systems | Repeated near-260 ms SOURCE tails despite lower work suggest testing request-burst scheduling. Halving the existing concurrency knob might reduce contention/tail exposure without a build. | The closed rows cannot separate S3 latency, queuing, offload, or scheduling. A limit of 64 may add a second SOURCE wave and worsen time. Flat or worse paired SOURCE tails falsify the proposed benefit at this setting. |
| Evaluator | Reuse the frozen 68 and ABBA order, retain every slow cell and actual retry, and require exact reference agreement before interpreting performance. | Two repetitions and a selected panel are weak population evidence. A single favorable pooled number or post-hoc tail subset cannot establish the mechanism or justify rollout. |

## One intervention, no implementation change

If root separately approves a future run, compare C=`fetch_parallelism:128` with D=`fetch_parallelism:64`, both at leaf64. This selector already exists: the qualified runner source `crates/borsuk/src/bin/check_cohere_native_baseline.rs` (SHA-256 `c1ccf0ebe18ad1fc7cd2d0266464d9df8d6bcedda734a8e2cbb18d2d64c64a4d`) accepts 16/32/64/128 at lines 681–700 and 1137–1140. Lines 1770–1777 apply it to SOURCE and SQ8 max parallel GETs; line 698 also sets the blocking-thread ceiling. Lines 4594–4645 contain existing 64/128 cap checks. These were read, not executed here.

This is a **fetch scheduling envelope** intervention, not an isolated SOURCE-network change: the selector couples request concurrency, runtime blocking capacity, and modeled reservation. Preserve total GET/byte caps and all candidate/quality settings. Record every derived resource-model change; do not label derived reservations identical. Reject the plan before launch if exact reference checks cannot distinguish permissible timing/resource metadata from semantic plans and charges.

Reuse the qualified runner/reader, inputs, own leaf64 closed full reference, fixed68 order, cache0, Q1, Offload2, FP32 shortlist16/no coalescing, retry policy, region/backend/generation, and all other configuration. Freeze fresh config/protocol hashes before measurements. No full1000 campaign, new code, rebuild, new corpus, paid consultation, or parameter sweep is needed to falsify this narrow hypothesis.

The smallest credible tail comparison is one bounded C1/D1/D2/C2 fixed68 attempt, preceded by the existing eight-ordinal admission and separate two-ordinal canary in each arm. A tiny tail-picked subset would make the supposed saving hard to interpret. Root must approve fresh authority, host/resource/cost/lifetime bounds and launch window; previous campaign authority does not authorize this proposed one.

Preregister the mechanistic prediction: both paired SOURCE p95 ratios D/C <=0.90, with both paired end-to-end query p95 ratios <=0.90. Preserve the current screen's pooled wall <=0.90, pooled p90 <=1.00, external wall <=0.95, query CPU <=1.00, external children CPU <=1.05, and GET/verified-byte nonregression gates. These are proposed future gates, not changed gates for the closed breadth experiment. Verify exact leaf64 outputs, plans/trace/shortlist/charges and all native/reader exits first.

If SOURCE p95 stays flat or rises, reject the proposed scheduling benefit; do not escalate to a larger run or invent a network diagnosis. If SOURCE improves but query tails fail, reject end-to-end sufficiency and retain SQ8/residual as unresolved. Even a complete pass supports only a selected-panel scheduling result. Root decides any later full-cohort quality/performance work. On interruption/incomplete cell, preserve original handles/checkpoints and mark that future attempt incomplete; no automatic restart or slow-cell exclusion.

## Verified transport seam and fresh admission requirements

The qualified flat-record/cache0/Offload2 path in `crates/borsuk/src/two_bit_generation.rs:3747–3771` passes `max_parallel_source_gets` directly to `fetch_source_ranges_traced_with_callback`. In `crates/borsuk/src/sq8_s3_range.rs:1720–1748` that value reaches the authenticated fetcher unchanged. Lines 1804–1817 construct download workers with `min(max_parallel, ranges.len())` and schedule up to `min(max_parallel + 2, ranges.len())` pending fetch/authentication futures. Lines 199–210 give the workers a download semaphore of that requested size. Lines 1944–1959 acquire that semaphore immediately before `fetch_exact_range_core` and release it after the body fetch, before authentication. The separate process compute pool has two slots/workers (lines 52, 119–125). Thus Offload2 is not a hidden two-download cap: 128->64 changes SOURCE download admission for plans with more than 64 ranges. Actual simultaneous network requests and any deeper provider limitation remain unmeasured; the hypothesis is about a verified admission change, not proof that all permits were occupied.

The same selector changes SQ8's configured cap, but the frozen SQ8 GET budget is only 32, so neither 64 nor 128 is the limiting SQ8 range count. The SOURCE GET budget remains 128, SOURCE bytes 64 MiB, SQ8 bytes 32 MiB, Q1, and library cap 2 GiB. Runtime blocking-thread ceiling changes 128->64; the Q1 `runtime_reserve` returns zero (runner 670–678), so do not import Q>1 runtime-reservation/header claims.

Fresh admission must bind both config hashes and `identity.fetch_parallelism` (runner 3418–3428), `identity.source_authentication` (3464–3465), and `bound_inputs.fetch_parallelism/source_get_budget/sq8_get_budget/sq8_byte_budget/source_byte_budget/source_authentication/semantic_leaf_limit/source_layout/config_schema/execution` (3495–3535), preserving binary/source/input/generation pins. Recompute `startup.range_state_charged_by_library_bytes` and `startup.source_authentication_extra_library_bytes` (2724–2725), and `startup.fp32_rerank.modeled_memory_bytes`/library cap (2688–2722). Also validate caller-pinned, query-scratch and trace-scratch fields; do not silently reuse old expected-header JSON. Authentication reservation uses `min(ranges, downloads + 2)` (range module 56–62), and scheduler reservation uses admitted pending futures (generation 2123–2126; runner 1675–1697). The score-overlap reservation remains charged. Exact future header totals are deliberately unpinned until root constructs and admits the new configurations; no new modeled values or future protocol digest are invented here.

The SOURCE and SQ8 source hashes were checked against the closed authority, and source reads were static only. These observations justify the seam as a proposed scheduling falsifier while leaving performance unproven.
