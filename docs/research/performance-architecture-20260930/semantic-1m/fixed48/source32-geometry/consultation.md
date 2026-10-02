# Source GET cap investigation

Consultation `fd83927adff84314`, completed exit 0 against `334313ce0957f2bbd6108964800f0e9e6351f78f`. This is planning evidence; replay claims require independent verification before prospective protocol freeze. No candidate latency measurement.

Recommend **one candidate-only change: reduce the HTTP SOURCE GET cap from 128 to 32**, using the existing gap-merging planner.

Inspected exact HEAD `334313ce0957f2bbd6108964800f0e9e6351f78f`; worktree remains clean. No edits, native execution, cloud work, or additional consultations.

1. **SOURCE cover capped at 32 — recommended.** Change `main()`’s `max_source_gets` in [two_bit_http.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/two_bit_http.rs:215). Existing `plan_paged_measured()` → `plan_two_bit_source_cover()` → `cover_pages()` already joins the smallest physical gaps. This requires one limit change and no format, routing, scoring, or planner implementation change.
2. **Coalesce adjacent selected leaves — reserve option.** In [remote_walks()](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:1421), fetch contiguous selected leaves together, then authenticate each leaf slice and restore nomination order. Existing traces contain **11–30 contiguous leaf-ID groups, median 22**, versus 48 GETs. Adjacent-only merging adds no payload bytes, but requires more code and ordering/error-accounting checks.

The recommended intervention targets a measured **109.10 ms median SOURCE stage**. Startup already reuses the root and stages seven metadata objects in one wave; median decode is only 8.95 ms. Discovery fetches whole leaves with concurrency 16. SOURCE then fetches and authenticates its complete cover, planning follows, and SQ8 fetches/authenticates before ranking. These are actual sequential boundaries; marginal quantiles remain nonadditive.

I authenticated the sealed scientific trace body against its measurement receipt and checked all 64 queries against the closed eight-rate HTTP rows: ordered top10 IDs, leaf charges, SOURCE charges and SQ8 ranges agree. Replaying the native `cover_pages()` geometry produced:

| SOURCE geometry, same 64 queries | Current cap 128 | Candidate cap 32 |
|---|---:|---:|
| GETs min / median / max | 24 / 49 / 76 | 24 / 32 / 32 |
| Total GETs | 3,147 | 2,028 |
| Bytes median / max | 17,100,800 / 21,299,200 | 19,507,200 / 32,563,200 |
| Total bytes | 1,101,708,800 | 1,290,124,800 |
| Largest individual range | 5,632,000 | 6,348,800 |

That is **35.6% fewer SOURCE GETs for 17.1% more SOURCE bytes**. This is conditional geometry evidence, not measured latency or recomputed quality. Bridged records do not enlarge the scored candidate set: `plan_walks()` derives completion from the original walks/page closure.

The actionable sequence is:

- **Freeze only this candidate limit change.** Keep the authenticated publication, sealed64 requests, normalization, truth, five owners and process-cold boundary.
- **Cheapest remaining falsifier:** run one disposable authenticated local control/candidate query, ordinal **54**, which exercises the largest extra-byte charge. Require identical discovery trace, SOURCE nomination, SQ8 plan and ordered top10 IDs. Stop on any mismatch, admission failure, incorrect charges or cleanup failure. Reuse `fragmented_paged_source_preserves_trace_and_rank_across_get_caps` for bridge correctness, plus existing range-authentication negatives.
- Then pass the exact64 source-bound runtime admission and separate staging/canary gates, using existing truth. Qualify the changed HTTP target, workspace Clippy and `scripts/check_rust_test_build.sh` before root freezes a paired cold experiment.

Prospective envelope for this panel:

- **Reads:** ≤32 SOURCE GETs, 48 leaf GETs and ≤32 SQ8 GETs: ≤112 query GETs. With unchanged startup accounting, predicted process attempts across64 are **8,166**, versus 9,285.
- **Payload:** SOURCE ≤32,563,200 bytes/query; existing 64 MiB admission remains sufficient. Predicted process-consumed payload totals **2,809,517,224 bytes**, versus 2,621,101,224. Wire bytes and billing remain unknown.
- **RAM:** conservative extra SOURCE-buffer allowance is **22,528,000 bytes per owner**, or **112,640,000 bytes across five owners**, using the existing two-copy model. Preserve native/shared limits and zero-swap/OOM requirements; verify actual peaks.
- **CPU:** scoring work stays fixed; hashing/copying processes 188,416,000 extra bytes. The closed eight-rate cell consumed 12.707 CPU-seconds over 8.424 seconds, about 1.51 cores under the two-core quota. Measure added CPU and throttling before accepting the tradeoff.

Principal risks are larger-body transfer/hash cost, ordered-buffer scheduling stalls, and future workloads whose merged cover exceeds admission. No latency gain is established. Ideal per-query erasure of the entire recorded SOURCE stage yields p90 **407.71 ms**; a 32-GET cap removes only part of that work.

Preserve the historical bulk-conversion **FAIL**, six-owner wave8 **FAIL**, root-reuse whole-pair **FAIL**, and eight-owner wave8 **PASS** separately. Their original gates remain intact. Current a0002 remains viable/PASS; the published-context 444 ms gate remains FAIL, with a **97.095 ms / 21.87%** gap. There is no implementation blocker to this bounded intervention; paired measurement must resolve its transport/CPU tradeoff.
