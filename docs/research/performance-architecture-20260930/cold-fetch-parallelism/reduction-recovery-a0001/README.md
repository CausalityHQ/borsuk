# Cold fetch width decision

The original ABBA attempt remains EXECUTION_INVALID_CONFIGURATION. All four native query runs completed; a separate qualified Rust reduction recovered their analysis without repeating queries. Both comparisons authenticate all 1000 results, ordered traces and logical charges.

| Run | Width | Recall@10 | p50 ms | p90 ms | p95 ms | p99 ms | Serial reciprocal QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| A1 | 16 | 97.23% | 180.421 | 205.770 | 214.643 | 236.366 | 5.432 |
| B1 | 32 | 97.23% | 127.501 | 150.818 | 161.681 | 188.448 | 7.583 |
| B2 | 32 | 97.23% | 128.024 | 152.109 | 161.274 | 200.922 | 7.514 |
| A2 | 16 | 97.23% | 179.367 | 206.443 | 213.969 | 239.917 | 5.427 |

**Decision:** select width32 for the next cold100k configuration. p95 falls24.63–24.67%, p90 falls26.32–26.71%, and serial reciprocal QPS rises38.46–39.59% in both counterbalanced pairs. Generic defaults and scale envelopes are not changed by this decision. Width changes concurrent execution and ordered-buffer scheduling together; no pure scheduling causal claim.

All arms retain9723/10000 hits, zero underfill/errors,67943 logical GETs and27259427868 verified query bytes. Each process reports about39–51MB peak. CPU1/512MiB/no-swap/pids256 limits were checked before measurements; original cgroup peak/OOM counters were not collected after its reducer failure and remain unknown. Recovery reduction peak2936832B,swap/OOM0,drained,instance terminated and waited.

Width32 recorded stage totals correspond to means: discovery34.46–34.52ms, source39.39–39.86ms, planning6.12–6.14ms, SQ8 fetch/rank51.87–52.53ms. The remaining cold path performs sequential router, compressed-source and SQ8 fetch phases. Optimize that dependency/GET cost before SIMD or caches. No network-only attribution is inferred from these intervals.

## External references

The retained same-corpus S3 Vectors first pass reports recall96.56%,p90=118.479ms,p95=149.056ms,p99=215.429ms,serial reciprocal13.754QPS. Width32 still trails its p90/p95/serial throughput, while having higher recall and lower p99 in these two repetitions. S3 backend cache state is unobservable; this is not a matched guaranteed-cold service comparison or a vendor win. The report does not prove concurrent capacity or total lifecycle-dollar superiority.

Published Turbopuffer cold10M figures are contextual only; our100k population and undisclosed vendor IDs/query split prevent parity claims. Cold-only work continues with local source/payload caches disabled. Warm/local-drive cache experiments are deferred.

## Reproduction and provenance

`compare_native_replay --paired-v2 pair1.config.json CONFIG_SHA NEW_REPORT`, then pair2, using the exact qualified1164808B binary SHA9ad7ce0fe8f461a3b8f7957812c798103f234a83d1df6748d5f04cb870c4694b. Both arm configs require borsuk-completed-native-reduction-config-v1. No Rust source, algorithm, query order, GT, producer binary or measurement limit changed.

Original immutable receipts: main4f341c1996045283165626a61190172c0648b9ce and S3 research/semantic-router/20261008/cold-fetch-abba-a0001. Recovery source/config frozen5684475b8b889b2a5d2f96d56b2f3d700b862dd9; original watcher72174 closed0; instancei-0addf68d4969f7e84 terminated/wait0; evidence12512B SHA50600a14cf728358e8616b2cd2d3502f37c09e408b3d8fe2bcc4c6d1929802d2. Native producer31eca73431901c193aff598f4c6e44d3540594da and reducer0f617199cd521590e8760aca41f36bd730c5a5a7 are distinct qualified source snapshots.
