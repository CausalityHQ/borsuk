# V135 exact hull serial-route closeout

**Decision: pass the preregistered 100k serial-route screen; do not freeze a
production or scaling default.** The generic affordable-hull shortcut removed
V134's planner bottleneck without changing source-derived votes, the physical
budget, nominee order, or returned results. This is a reused development
cohort and authenticated **local** object replay. It does not measure a live
S3 query, concurrent serving, fresh recall, or 1M/10M/100M behavior.

## Frozen attempt and verification

The source commit is `7f17b45cab5b26083eab80ef72416b1a64932a48`;
the complete source archive is 11,663,940 bytes with SHA-256
`0c9bb30261772beaee3feb37337afe53e55614bfd8cf0d4d704ef3aa136795ee`.
The one Causality `c7i.8xlarge` Spot attempt ran on `i-0ba6b165cb8b7ad25`
and wrote its terminal to
`s3://borsuk-bench-453182569524-euc1/research/v135-hull-route/7f17b45cab5b26083eab80ef72416b1a64932a48/runs/v135-20260924T074030Z/a0001/terminal.json`.
The terminal SHA-256 is
`69a7b836d63e9ac618f9c9e051f170cb52404c471cd73a6347efaa7544d462fe`;
it reports `complete`, exit 0, and 277 seconds. The original launcher exited
0 and independently observed the instance **terminated**. All 12
terminal-listed artifacts were downloaded and checked against their byte
lengths and SHA-256 values. The summary digest is
`79e03837c38881c893260c8cff9a93be889935c7e72227de7f1c7b71097e5868`;
the 1,000-row replay digest is
`098bcb7652d9bda163a9e428627346a94bdad047e29d294fbe64386faef3932d`.

The generation manifest and terminal are the same authenticated V131
generation used by V133/V134: SHA-256
`968ef7d795b53e5869400998ca19c6ab20d9b39830295fac3081c419f63f0f20`
and `0ec4b70965d11c83eec1c5f0ee70c2f181fb2954df1429c8f02f57837842f4a6`.
The dataset is the **deep-image-96-angular random 100k train subset**, with
the already-used **publication-test ordinals 9000–9999**, GT100 within that
subset. The paired control replays sealed fixed capped ranges and does not
include online control routing. The candidate includes production PQ64
routing, exact nominee scoring, weighted planning, authenticated local SQ8
reads, SQ8 scoring, ID resolution, and exact-source ranking.

## Paired result

| Measure | V135 candidate | V135 fixed capped control | V134 candidate, historical |
| --- | ---: | ---: | ---: |
| Exact-source Recall@100, 100,000 GT positions | **99.942%** (99,942 hits) | 98.827% (98,827) | 99.942% |
| Complete serial route p50 / p95 / p99, ms/query | **43.170 / 48.006 / 49.574** | 18.382 / 23.108 / 24.732 | 90.793 / 117.916 / 123.241 |
| Candidate planner p50 / p95 / p99, ms/query | **0.032 / 0.037 / 0.040** | excluded | 67.602 p95 |
| Candidate router p95, ms/query | 1.862 | excluded | 1.799 |
| Candidate exact nominee score p95, ms/query | 0.688 | excluded | 0.734 |
| Authenticated SQ8 read p95, ms/query | 14.022 | 3.661 | 15.212 candidate |
| Exact-source phase p95, ms/query | 15.624 | 15.237 | 16.585 candidate |
| Planned range reads across 1,000 queries | 1,000 | 24,674 | V134 multi-range |
| Maximum candidate SQ8 range bytes/query | 10,800,000 | n/a | n/a |

The candidate beat the control on 335 queries, tied on 665, and lost on zero.
Its p95 is 27.618 ms below the unchanged 75.624654-ms gate, and planner p95
is below the preregistered 5-ms screen. The one-range candidate reads the
whole 10.8-MB SQ8 object because that object fits the 16,777,216-byte cap;
the result cannot be extrapolated to a larger object whose positive-vote hull
exceeds the cap. The 100k fixed control remains faster locally, and the
candidate's higher quality does not turn that latency comparison into a
commercial win.

Independent recount of every replay row reproduced hits, paired wins/ties,
GET totals, bytes, block reads, and nearest-rank p50/p95/p99 timings. Every
query ordinal maps to source query ordinal 9000 plus that ordinal. Every
candidate and control top-100 source-ID order, hit count, and candidate count
matches the authenticated V134 replay; candidate nominee and primary order
differences are zero. Every candidate uses one range and at most 10,800,000
SQ8 bytes; every control uses at most 32 ranges. The result is the same
method and result set with a faster exact planner branch, not a new quality
measurement.

Charged serving cgroup peak, including cache, was **114,860,032 bytes**
(109.54 MiB); current charge after evaluation was 73,322,496 bytes. The
preparation stage took 7.712 seconds and startup authentication took
0.390 seconds. These are measured on this Spot attempt. They exclude
multi-generation overlap and concurrent requests.

## Next gates

1. Measure C1/C8/C32 complete local serving on the same immutable generation
   with explicit charged-memory and throughput limits; then run an actual S3
   transport gate. V132's earlier live-S3 candidate/control p95 values,
   152.858/75.625 ms, belong to a different route revision and remain
   historical negative evidence.
2. Before a scaling claim, replace the flat PQ64 router's constant-fraction
   row scan and the constrained weighted planner path. The V121 9.99M D96
   offline nomination batch averaged 184.76 ms/query; V135 did not change
   that scaling mechanism. Test a source-only routing partition decoupled
   from physical pages with row-level PQ64 scoring after coarse selection,
   on matched ReLAION-1M and deep-image cohorts. Its probe count, resident
   bytes and returned recall must be measured under one generic policy.
3. Promote only a method that passes fresh matched 1M quality, live serving,
   memory and concurrency gates to 10M and 100M, then compare under matched
   disclosed conditions with S3 Vectors and Turbopuffer. Memory may grow
   smoothly with vector count and requested recall; no dataset branch or
   vector-count knee is justified by this gate.

The Lean theorem in `formal/PhysicalIntervalBudget.lean` establishes the
conditional lexicographic optimality of the affordable full-vote hull in its
mathematical model. Focused production Rust tests passed 11/11 on a separate
terminated Spot worker. Neither proof establishes Rust refinement, empirical
recall, hardware latency, or charged memory without tests and measurements.
