# CoHere fixed48 offered cold measurement a0002

**Verified scientific PASS; original local controller exit 2 is preserved.** The remote worker completed exit 0. All 38 declared bodies (18,963,646 bytes) authenticate; independent closed validation and offline replay pass. The instance is terminated. Collection was recovered without repeating measurement after temporary collector globals changed the immutable cold baseline roster.

## Measured panel and protocol

CoHere FIRST1M, D768 cosine, k10; the same sealed64 queries and qualified HTTP binary as cold a5. Each rate repeats that panel: 384 successful offers are six repetitions, not 384 independent quality queries. Fresh native process and scratch per call; no persistent native cache. Five owners, eu-central-1a c7i.2xlarge, shared CPU quota 200%, shared memory limit 12 GiB/no swap. Linear `(N-1)*p` percentiles. All rates: 64/64 success, 619/640 hits = 96.71875% recall@10, zero drops/errors/aborts, identity/resource/cleanup/dispatch gates pass.

| Offered QPS | Completed full-span QPS | Cold p50 / p90 / p95 / p99 (ms) | Outcome |
|---:|---:|---|---|
| 0.25 | 0.253327 | 555.28 / 674.80 / 841.32 / 960.39 | PASS |
| 0.5 | 0.505638 | 505.14 / 573.92 / 594.41 / 653.49 | PASS |
| 1 | 1.007025 | 500.35 / 560.92 / 629.60 / 908.42 | PASS |
| 2 | 1.997495 | 504.86 / 552.51 / 743.00 / 996.37 | PASS |
| 4 | 3.931439 | 483.40 / 540.61 / 569.54 / 617.41 | PASS |
| 8 | 7.597044 | 485.78 / 541.10 / 557.41 / 624.97 | PASS |

At 8 QPS offered, 64 completions over 8.424329161 s yield 7.597044 full-span QPS including drain. Scheduled-to-response p90/p95/p99: 542.36/558.80/625.95 ms. Sustained capacity and saturation remain unmeasured.

## Comparator and remaining gap

| Baseline / target | Actual comparison | Scope |
|---|---|---|
| Previous CoHere same sealed64 cold a5: 96.71875% R@10, p90 580.67 ms | Quality delta 0 pp; current 8-offer-QPS p90 541.10 ms | Different run/load; no paired speedup claim |
| Turbopuffer published 1M D768 cold p90 444 ms | MISS by 97.10 ms (+21.87%) | Frozen published context, not matched vendor measurement |
| Turbopuffer published 10M D1024 at 8 QPS | Eight nominal offers/s attained here at 1M D768 | Scale/dimension/corpus/protocol differ; no measured win |
| AWS 90%+ average recall and subsecond cold directional reference | This panel exceeds 95% R@10 and has 541.10 ms cold p90 at 8 offered QPS | AWS reference gives no percentile guarantee |

Published references: [Turbopuffer](https://turbopuffer.com/blog/turbopuffer), [S3 Vectors query](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-query.html). Comparator facts use the source-qualified 2026-09-28 target matrix; vendor p95/throughput per dollar remain unknown. The frozen published-context 444 ms gate remains FAIL for every rate; offered capacity PASS does not change that result.

## Resources and cost scope

Whole profiling service kernel memory peak 205,119,488 B, swap peak 0 B, no OOM. Driver process peak RSS 110,202,880 B; separate native peaks must not be summed as simultaneous RSS. Readonly static repository uses 816,635,904 allocated bytes. Native ABI/binary and source399 remain unchanged.
At each rate, process transport totals: 9,285 HTTP attempts, 2,621,101,224 consumed payload bytes, zero stream/transport failures. These are process payload counters, not physical wire bytes or a billing statement. Actual lifecycle billing is unmeasured; observed Spot quote $0.2152/hour and compute cap $0.30 + $0.15 allowance are admission inputs.

At 8 QPS, component medians: before successful connect 173.21 ms; native query 311.23 ms, discovery 100.15 ms, SOURCE 109.10 ms, planning 12.06 ms, SQ8 83.41 ms. Quantiles are not additive; these locate the next investigation, not a demonstrated causal speedup.

## Decision and next gate

Retain this product-viable candidate and PASS. Repair the collector context without rerunning science. Next: one source-grounded latency intervention with prospective byte/RAM/GET envelope and a fast exact-fixture falsifier, followed by paired cold measurement if executable. Measure sustained/saturation and lifecycle cost separately; obtain fresh ReLAION and 10M/100M scale evidence. Both-vendor production superiority, scale, and maintenance cost remain unproven. Historical architecture caps remain frozen-arm diagnostics, not universal product rejection thresholds.
