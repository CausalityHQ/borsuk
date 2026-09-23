# Candidate 200-byte page code wave

Status: rejected at the preregistered 1M source-only projection gate.

## Decision and existing evidence

The closed ReLAION-100k OPQ8 plus rotated two-bit actual-read cell reached
99,668 GT100 hits. The later 1M code-byte projection showed that fetching
whole four-page groups at 200 bytes per row reaches only 97,541 GT100
group containment and p05 86 under its fixed ranking. The 96-byte sign
and PQ scorers fail the newer paired 100k score-fidelity gate. The 1M
exact-source data-range diagnostic reaches 98,920 GT100 after scoring
all selected groups, but a 200-byte code wave cannot assume it can fetch
those same rows under 16 MiB. Thus a new 100k final-page screen would not
answer the limiting 1M code-wave question.

## Candidate architecture

Keep the source-trained resident OPQ8 route plane and the frozen 1M query
cohort, group selection, physical page order and 32-range/16,777,216-byte
data wave. Use the authenticated OPQ8 **page** priority, before any 200-byte
fetch, to plan one code wave of at most 32 contiguous byte ranges and
16,777,216 bytes. Each physical page contributes exactly `200 × row_count`
code bytes in a new page-ordered object per base/delta role; bridged pages
consume real code bytes. Read and score only the pages in
that code cover with the historical primary rotated two-bit scorer, then
apply the unchanged top-100-row final-page priority and data-range rule.
The score diagnostic may use exact source vectors on precisely the same
code-covered pages; it cannot silently scan unfetched groups.

The historical group object has a group digest but lacks a separate digest
for every page subrange. The production format therefore needs new
page-ordered objects, an incompatible generation marker and a sealed
per-page digest/offset/length manifest before page-granular S3 reads can
be authenticated. HTTP 206,
Content-Range, length and page digest must be checked on every returned
range. A source-only projection may calculate lengths from sealed page row
counts, but does not qualify the reader or its latency.

## Cheapest decisive gate

First replay the terminal-closed 1M source/layout, OPQ8 plans, OPQ8 page
priority and truth-owner authority. Without constructing a 200-byte 1M
code plane, project exact page-code lengths, run the existing minimum-byte
merged-range admission under 32 GETs/16 MiB, and count contained GT100,
GT10, p05 GT100, sub-90 queries, target and bridged pages, code GETs and
bytes. Independently recompute all 1,000 query covers and truth masks;
freeze and seal the query-only plans before opening truth. A projection
passes to code construction only if GT100 containment is at least 98,651
(500 above the 98,151 final gate), p05 is at least 93, and code budgets
hold for every query. These margins are architectural screening rules,
not proved recall. If the projection fails, reject this page-code-wave
candidate and revise routing/locality rather than widening the budget
after seeing the cohort.

On a passing projection, run one preregistered 1M source-only 200-byte
scorer cell on exactly the projected code cover. The fixed final gates are
GT100 ≥98,151, GT10 ≥9,928, p05 GT100 ≥90, at most 49 sub-90 queries,
and ≤32 GETs/≤16 MiB for **each** code and data wave. Compare to the
strongest exact-source arm on those same code-covered pages and to the
closed 98,920 GT100 full-group source diagnostic without conflating the
different candidate sets. A valid pass is still development-cohort
evidence; it needs actual authenticated reads, an untouched query cohort,
observed latency and the 10M/100M scale gate before production claims.

Use the Causality Spot profile for heavy work, one immutable attempt at a
time from a pushed exact source archive, create-only terminal artifacts,
independent readback and replay, and prompt instance termination after the
terminal marker. Interruption invalidates the measurement cell and requires
a new attempt ordinal from the same frozen revision. Do not inspect
incomplete measurement CSV files.

## Formal boundary

`formal/SourceRangeFidelity.lean` proves 200-byte record and conditional
16-MiB row arithmetic, and a fixed-threshold recall stability theorem under
per-page score error and margin premises. `formal/Opq8Planner.lean` proves
abstract request/byte and conditional latency bounds. The greedy merged-
range Python implementation, page-level authentication, row-score errors,
truth masks, S3 service times and unseen-query recall are separate premises
or measurements. A threshold-model counting theorem for pages near the
admission boundary has since been checked; its code-wave planner
refinement remains necessary.

## Closed decision

The single attempt from source `658f35f8e3378250c1bc3421f59efddd2c6aa02c`
closed complete on Spot `i-07534ad9f9d56ec70`, which was terminated.
Independent validation and terminal readback confirmed 98,468 GT100
contained and p05 91, below the frozen 98,651/93 screening margins.
The 200-byte 1M code plane and scorer cell are stopped. Full evidence,
identities and resource receipts are in the research ledger section
"ReLAION-1M page-local 200-byte code-wave projection a0001."
