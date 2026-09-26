# V252 CoHere 100k: source-only coarse entries

**Decision question:** Can a query-selected graph entry recover V251 ef512's
quality miss without losing its bounded-work advantage? V251 ef512 scored
9,558 base rows at p95 but returned 99,220/100,000 exact GT100 hits;
its smallest quality-passing arm ef1024 scored 14,508 rows, above the
10,000-row scale gate. V250's diverse graph and V251's exact FP16
navigation remain fixed.

The sole new method samples 256 evenly spaced physical source ordinals,
scores those authenticated FP16 rows by cosine for each query, selects the
nearest anchor with ordinal as the deterministic tie break, and starts
ef512 base-layer search there. It skips the usual single-entry upper-layer
descent. The 256 anchor scores are included in the reported work count;
the baseline V251 count excludes its upper-layer descent. No query or
truth data select anchors, and no dataset-dependent parameter is fitted.
This is a cheap one-level coarse-entry falsifier, not a 100M architecture.

Rebuild the same V250/V251 graph and require SHA-256
`688941c7c61c89a39739909af14cb2b4a935a7a4967a168f9503ac34e170d0e7`.
Require V248-identical CoHere-large-10M first100k D768 cosine source,
FP16 plane, PQ books/codes, physical map, query panel and independently
computed GT100 truth by SHA-256. Use the same k100 queries, development
ordinals 0–255 and validation 256–999; both splits were previously used.
Freeze exactly one arm: 256 anchors and ef512. Seal all IDs before scoring.

Require development mean R@100≥0.995 and p05 hits≥98, then validation
mean≥0.995 and p05≥98. Also require p95 scored rows including anchors
≤10,000, eight-worker loaded p95≤35 ms, serving peak RSS≤256 MiB and
zero vector-body GETs. Report p50/p90/p95/p99, QPS, per-query hits and
work, RSS, graph build and preparation resources. Loaded timings are
in-process, not service or vendor latency. A quality pass without the
work/resource pass does not promote scale. If this coarse-entry probe
fails, stop variations on entry count or beam and redesign coarse
partitioning/format before another scale attempt.

One `causality` c7i.4xlarge Spot attempt, immutable reservation/source
archive, narrow graph unit test before measurement, closed terminal with
full artifact SHA-256 readback, interruption discard, and immediate
termination. No 1M or 10M promotion until this gate passes.
