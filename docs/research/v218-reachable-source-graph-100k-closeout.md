# V218 reachable source graph 100k closeout

**Decision: pass the frozen 100k gate and advance one unchanged graph
construction to ReLAION-1M validation.** This is a source-only graph
change, not a query-label or dataset-specific recall setting. Its
one-edge physical-order cycle and local incoming-edge repair use the
same policy at any corpus size. The PQ64 cosine navigation and FP16
rerank arm remains ef=2,048 / shortlist=2,048.

The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-03d4582153aab8077` in `eu-central-1c` and is terminated. Source
commit `ce317cac8d1eb0a1b8a8610f0a3756b96090514c`, source archive
SHA-256 `45d7f0f9b729315fbf601766c78949a969bc75256e557591538f3251686dbc47`,
terminal SHA-256 `cf44ecb95a9d1cc07ac895a222a407c37db090c499f15a887e887d74c8492efd`.
The terminal reports `complete`, exit zero; its ten artifacts passed
independent size and SHA-256 replay. Immutable evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v218-reachable-graph-100k/ce317cac8d1eb0a1b8a8610f0a3756b96090514c/runs/a0001/`.
The raw IDs were sealed before GT100 and paired baseline download.

Dataset: **ReLAION-100k D768, k=100**, development query ordinals
0–255 (already used) and method-held-out ordinals 256–999 (also used
by earlier BORSUK campaigns). On the same GT100 witness and frozen
ef/shortlist arm:

| Split | V215/V216 paired GT100 hits | V218 PQ GT100 hits | V218 exact FP16 navigation hits | V218 p05 hits/query |
| --- | ---: | ---: | ---: | ---: |
| Development, 256 queries | 25,473/25,600 | **25,537/25,600** | 25,540/25,600 | 99 |
| Method held out, 744 queries | 74,048/74,400 | **74,234/74,400** | 74,246/74,400 | 99 |
| Combined, 1,000 queries | 99,521/100,000 | **99,771/100,000** | 99,786/100,000 | **99** |

V218 PQ had 168 wins, 830 ties and two losses against the paired
V215/V216 arm by query. The exact FP16 navigation diagnostic was timed
separately and did not select or tune the PQ arm. It is 15 GT100 hits
above PQ at 100k, suggesting graph access was the larger loss here.

The authenticated V218 base layer has **100,000/100,000 reachable**
rows, **zero** rows below four in-edges, 6,463,215 directed edges,
minimum in-degree four and maximum outgoing degree 99. The old V214
graph had 1,791 unreachable rows and 4,231 below four in-edges. The
new graph artifact is 26,571,646 bytes (SHA-256
`d8b70919243a7cd6ecb9448ce23f776374738476c1882cbc6a651fb34753af2f`).
Its build took 177.702 s by Rust timer, 2m58.05s wall, peaking at
842,149,888 bytes RSS.

Loaded eight-worker whole in-process PQ request p50/p90/p95/p99 was
**5.750/6.673/6.915/7.380 ms** from the same 1,000-query panel;
completed throughput was **1,353.3 queries/s**. Sequential
p50/p90/p95/p99 was 6.019/7.055/7.342/7.739 ms. The p95 base visit
count was 23,015. Peak serving RSS was **212,135,936 bytes**, including
31,142,500 bytes loaded graph heap, 154,400,000 bytes FP16 plane,
6,400,000 bytes PQ codes, 400,000 bytes PQ norm sidecar, and 3,200,000
bytes of eight visit workspaces. Vector-body GETs were zero. These are
in-process numbers; they exclude network admission, external service
latency and object-store transport.

The worker launched at 11:27:56 UTC and uploaded its terminal at
11:35:24 UTC on 2026-09-25. At the launch-time Spot quote of
$0.3631/instance-hour, the 7m28s interval implies **about $0.0452
compute**, an estimate excluding EBS, S3, termination tail and billing
rounding.

The preregistered structure, returned-quality, p05, loaded p95,
256 MiB RSS and zero-GET gates all passed. The 100k result does not
establish 1M quality. Next, build this source policy at ReLAION-1M D768
and compare the validation-1000 GT100 witness to paired V199's
99,605/100,000 hits and p05 98. A pass then requires a same-workload
cold/no-cache end-to-end product comparison before any S3 Vectors or
Turbopuffer performance claim.
