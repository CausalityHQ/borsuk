# V133 physical I/O exploration after V132

The V132 live transport gate rejected the broad candidate SQ8 GET: its
complete p95 was 152.858 ms/query against 75.625 ms/query for the paired
control, while its S3 response was 6.14 times larger on average. This note
tests whether physical neighbors of router nominees could cheaply recover
the candidate's quality. It is **exploratory coverage arithmetic**, not a
preregistered method, a new serving measurement, or a recall result.

Inputs were the complete V122 100k deep-image development evidence
(`evidence.jsonl`, SHA-256
`deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6`)
and physical-row-to-ID layout (`built/layout.npy`, SHA-256
`8b23fb6d2f76704394733f5540f25e36db961386b568495b7bfd73fc52d60aea`).
Both came from the sealed V122 terminal. Every source GT ID was mapped to a
physical 256-row SQ8 page. For each query, select pages within `r` pages of
either its 100 primary physical nominees or all 512 nominees. Count GT IDs
in selected pages; count selected full-page bytes and contiguous intervals.
The final page's shorter payload is immaterial to the displayed rounded
means. No SQ8 or source reranking was run on these new page sets, so their
GT coverage is an **upper bound** on achievable Recall@100.

| Origin and radius (pages) | GT coverage / 100,000 | Mean selected MB/query | p95 selected MB/query | p95 contiguous GETs/query |
| --- | ---: | ---: | ---: | ---: |
| Primary, 0 | 98.568% | 0.586 | 1.106 | 30 |
| Primary, 4 | 99.406% | 3.047 | 5.723 | 15 |
| Primary, 6 | 99.532% | 3.905 | 7.078 | 12 |
| Primary, 8 | 99.642% | 4.626 | 8.046 | 10 |
| All nominees, 0 | 98.625% | 1.395 | 2.239 | 49 |
| All nominees, 2 | 99.527% | 3.867 | 6.138 | 24 |
| All nominees, 3 | 99.631% | 4.691 | 7.244 | 19 |
| V122 capped control, actual ranges | 98.827% | 1.536 | 2.865 | 32 maximum |
| V122 broad candidate, actual ranges | 99.942% | 9.428 | 10.772 | 1 |

The all-nominee radius-0 plan exceeds the 32-GET cap at p95. Radius-2 and
primary radius-6 only *barely* clear the 99.5% coverage target before any
SQ8/source selection loss, so this evidence does not justify either as a
production policy. Radius-3 all-nominee and radius-8 primary offer more
coverage headroom but still need actual reranking, cap checks on every query,
live latency, and fresh cross-dataset validation. Picking the radius that
looks best on this already-used cohort would be dataset-specific tuning.

The generic design question is how to allocate pages from a query's router
evidence under explicit byte, GET, and recall targets, with enough headroom
for returned-score selection. Two alternatives merit a design review:

1. **Train-only calibrated selective pages.** Derive page priority and
   neighborhood expansion from physical layout and router evidence, calibrate
   a shared selection rule on held-out training queries, and freeze it before
   evaluating test splits. Dataset-specific fitted parameters are allowed
   only through that identical algorithm; no evaluation-split adjustment.
   Conditional formal bounds can cover caps and monotonic capture, but
   population recall requires stated query-distribution assumptions and
   fresh measurements.
2. **Elastic resident SQ8.** Authenticate and retain SQ8 bytes where a memory
   budget permits, so high-recall reads avoid remote transfer. Per-generation
   SQ8 payload is `N × (D + 12)` bytes: 10.8 GB at 100M × 96 dimensions,
   78 GB at 100M × 768 dimensions, before router, source tier, maps, cache,
   concurrent generations, or allocator costs. These are formal payload
   formulas, not measured RAM or latency. A fixed all-or-nothing corpus-size
   switch would create the vector-count knee the operator wants to avoid;
   any resident policy must expose a smooth memory/performance tradeoff and
   account for the source and router working sets too.

The next decision needs a concrete policy and a cheap quality falsifier that
uses fresh training-only calibration, followed by an immutable live Spot gate.
V132 does not support a claim about end-to-end router latency, concurrent
throughput, 1M generality, or commercial comparisons.
