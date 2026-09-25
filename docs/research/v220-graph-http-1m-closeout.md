# V220 ReLAION-1M HTTP serving closeout

**Decision: pass the frozen loopback HTTP gate.** The selected V219
4,096/4,096 graph returns exactly the same 100 IDs for every validation
query through an eight-worker HTTP service. This is a product-serving
increment, not an external-network S3 Vectors or Turbopuffer win.

Source commit `5c9e4c52d0c97df7fb2b917ee618de4e66e710f6`, source archive
SHA-256 `f8596bbe4a7530085fd88d18f172e9259ad5edef7ec8203cf53e22759e3479db`.
The one `causality` `c7i.4xlarge` Spot attempt was `a0001` on
`i-025275764f1528a5b` in `eu-central-1c`; the instance is terminated.
Terminal SHA-256 `489b8299e198f01a752803751022732a66ff9741cab8e12ef216b5cd2408934d`
reports complete/exit zero. All seven artifact byte counts and hashes
passed independent readback. The raw HTTP returns were sealed before
the V219 IDs and V198 GT100 witness opened; sealed SHA-256
`2e902fab9b15074b84aaf863f52f30a102be458b532844a8cccdc9730b0bd35c`
passed independent readback. Evidence prefix:
`s3://borsuk-bench-453182569524-euc1/research/v220-graph-http-1m/5c9e4c52d0c97df7fb2b917ee618de4e66e710f6/runs/a0001/`.

Dataset: ReLAION-1M D768, **validation ordinals 0–999 already used**,
k=100 and exact GT100. BORSUK was resident after authenticated hydration.
Eight persistent HTTP/1.1 connections ran on loopback against eight search
workers with one bounded shared queue. All 1,000 raw response IDs have exact
V219 parity; quality is **99,664/100,000 GT100 hits**, p05 **98**.

| Measured quantity | V220 |
| --- | ---: |
| HTTP p50 / p90 / p95 / p99 | 14.896 / 19.809 / 21.224 / 24.208 ms |
| Completed throughput | 497.8 queries/s |
| Peak server RSS | 2,030,342,144 bytes |
| Request / response logical bytes | 14,658,893 / 1,026,973 bytes |
| Peak simultaneous requests, replayed from raw offsets | 8 |
| Vector-body GETs | 0 by construction; the serving path has no object client |

The four percentiles were independently recomputed from the same sealed
1,000 raw samples, along with logical byte totals and observed concurrency.
The service passed its p95 <100 ms, p99 <150 ms, >=100 QPS, <=3 GiB RSS,
exact-ID and quality gates. V219's earlier selected-arm **in-process**
loaded p95 was 18.911 ms; that is a different timing scope and is not a
paired per-query HTTP overhead measurement.

The instance launched 2026-09-25 12:56:17 UTC and its terminal landed
255.932 seconds later. The launch-time c7i.4xlarge Spot quote was
$0.3631/hour, implying **about $0.0258 compute**, an estimate excluding
EBS, S3, termination tail and billing adjustments.

**Next gate:** direct S3 Vectors on the same ReLAION validation panel and
an external-client BORSUK HTTP run at eight concurrent requests from the
same region, with cache semantics and distance metric stated. S3's old
development-split first-pass result is historical context only. Do not
advance to 10M from this loopback result or claim a competitor latency win.
