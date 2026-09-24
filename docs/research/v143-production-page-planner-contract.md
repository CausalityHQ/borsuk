# V143 production page-planner contract and next gate

The first production slice is `crates/borsuk/src/budgeted_page_rank.rs`.
It accepts finite page scores, physical primary ordinals, a global
admission multiplier, and explicit GET/byte caps. It admits primary pages
in first-primary-rank order, then the remaining pages by score and page
ordinal. For each proposed page it computes the minimum-byte cover under
the GET cap by bridging the cheapest gaps; the page is admitted only when
the full cover fits the byte cap. It returns physical ranges, planned
bytes, selected pages, primary retention and target shortfall. No corpus
name or vector-count threshold enters the function. A partial final page
is charged by its actual row count.

Four focused Rust tests cover priority, cheapest-gap coalescing and cap
rejection, a short final page, and signed-zero score ties. A separate
32-query check reconstructed the authenticated V140 D96 f16 centroid
scores from V122 SQ8, then compared this Rust planner with V140's sealed
β=4 raw plans. All 32 selected-page inventories, physical ranges,
planned bytes, shortfalls and primary-retention counts matched. This
verifies the planner on a narrow D96 slice. It does not verify the
production score calculation, complete D96/D768 parity, serving timing
or S3 behavior.

## Generic resource policy

The current 256-row page and 32-GET/16-MiB wave are historical transport
settings, not a universal default. A production page format should select
row count from a measured target physical page size and `D+12` SQ8 bytes
per row, then freeze that geometry in the generation format. Rebuild and
reject old experimental artifacts when geometry changes. The request
policy chooses a byte cap, GET cap and admission multiplier from the
requested recall/service objective and measured cross-corpus tradeoff;
it never switches at a particular `N` or branches on dataset identity.
V140's β=4 passed two used development cohorts but is not a calibrated
mapping from an arbitrary recall target.

With PQ64 codes, two float32 summary blocks per 256-row page, f16 unit
centroids at 32 rows/unit, and a 16-byte resident source-ID map entry,
the modeled payload is `N × (64 + D/32 + 2D/32 + 16)` bytes per pinned
generation. This gives 8.9 GB at 100M D96 or 15.2 GB at 100M D768,
and 17.8/30.4 GB for two complete generations. It excludes format
headers, page rounding, hierarchy, allocation, page cache, scratch and
concurrent request buffers. The operator permits more memory at 100M
when recall and latency justify it. We will price any added hierarchy or
smaller centroid unit by its measured marginal quality/latency benefit,
with a continuous or table-calibrated resource policy rather than a
vector-count knee. Exact-source and SQ8 planes remain authenticated
storage payloads; their full sizes are not silently counted as resident
RAM.

`formal/BudgetedPageAdmission.lean` proves the conditional page-admission
GET/byte invariant, full-page arithmetic, and monotonicity of this
linear payload model. The proof assumes `cover` reports the true
coalesced physical charge. It does not prove Rust refinement, empirical
recall, hardware latency, or charged memory. A latency theorem requires
separately validated bounds on route work, S3 request overhead, per-byte
service rate, concurrency and retries. Recall requires source/GT
assumptions or measured held-out data.

## Scaling risk and ordered validation gates

The flat source router and V140 centroid scorer scan resident summaries
or centroids proportional to `N` for every query. At 100M D768 the
32-row f16 centroid payload alone is about 4.8 GB per generation, so a
flat query scan is not a plausible latency strategy. The new page
planner deliberately consumes scores from a separate routing stage so
a hierarchical scorer can replace the flat scan without changing its
budget contract. The current planner's cover is recomputed for each
candidate; its own large-`N` CPU cost also needs a measured gate before
qualification.

1. On the frozen D96 and D768 generations, compute page scores in Rust,
   compare **all** 1,000 query page sets/ranges to V140, and replay the
   returned source IDs against V141/V142. Preserve the two V142 failed
   non-measurement attempts as arithmetic and packaging diagnostics.
2. Measure complete local route and planner p50/p95/p99, concurrency,
   charged memory and pinned-generation startup on one source revision.
   Optimize page-cover maintenance only if its measured CPU cost is
   material. Do not use the old weighted DP in this path.
3. Run a fresh held-out, matched-layout D96/D768 quality and live-S3
   transport gate. Record actual GET attempts, response bytes, latency,
   throughput, charged RAM and per-instance identity. Freeze defaults
   only after both corpora pass the same policy.
4. At 10M, measure primary-page growth, exact-source capture and flat
   router cost. Replace the per-query full scan with a hierarchical
   source-only route and prove that the bounded candidate set retains
   the quality gate before spending on 100M. At 100M, permit larger RAM
   only against measured recall/latency and cost gains.
