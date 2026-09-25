# V222 external graph HTTP ReLAION-1M closeout

**Decision:** retain the source-only reachable cosine PQ64 graph with a
resident FP16 rerank plane as the current BORSUK serving architecture.
Both separate-host HTTP passes preserved the frozen V219 results and met
the preregistered latency, throughput and memory gates. The next product
increment should put authenticated generation loading and this query path
behind a usable library API; the Axum service is still an example. Do not
claim a Turbopuffer win or production readiness from this 1M gate.

Source commit `1eee3ac3e8663e95a735c9f61297d5259b28b623`, source
archive SHA-256 `2eab9c3a8b09988104432429287d67f98734f14f6298b2953b071e424a1eaad6`.
The server `i-0936be55574e63f28` and client `i-04c55784701beb4d4`
were separate `causality` c7i.4xlarge Spot hosts in `eu-central-1c`.
Both are terminated. Their terminal SHA-256 values are respectively
`3b6fcdf293449e04dce65235ba803bbb0a6ee4a4ead5f800e2e07210f651d44d`
and `93f6beaf90c11e54dab27b09d56149975ce89a43b9cd1da410c6f4b282f90eb2`.
The controller replayed every terminal artifact by byte count and SHA,
including both sealed raw files, and recorded `gate_pass=true`. Closed
evidence and launch receipts are under
`s3://borsuk-bench-453182569524-euc1/research/v222-external-graph-http-1m/1eee3ac3e8663e95a735c9f61297d5259b28b623/runs/a0001/`;
the immutable closeout SHA-256 is
`81aba13fb991aa18868d607531cc3380d833fe1c5bf1e3d16bfcaa80ab4651e4`.

Dataset and split: ReLAION-1M, 1,000,000 source vectors, 768 dimensions,
**validation ordinals 0–999 already used**, k=100. GT100 is squared
Euclidean; the near-unit source and the BORSUK and S3 indexes use cosine.
The authenticated server hydrated the index before timing, held it
resident without a response cache, and issued zero vector-body GETs by
construction. Eight persistent HTTP/1.1 client connections crossed a
private VPC peer path. Each pass used a fresh Python client process and
included JSON encode, HTTP exchange and JSON decode in its timer.

| Measured cell, 1,000 queries per pass | GT100 hits / 100,000 | p50 / p90 / p95 / p99 (ms) | QPS |
| --- | ---: | ---: | ---: |
| BORSUK V222 VPC peer, first | 99,664 | 14.723 / 19.267 / 20.424 / 22.402 | 506.3 |
| BORSUK V222 VPC peer, immediate repeat | 99,664 | 15.064 / 20.603 / 21.987 / 25.699 | 481.5 |
| S3 Vectors V221 direct SDK, fresh-index first | 90,996 | 67.314 / 176.974 / 206.064 / 303.212 | 80.9 |
| S3 Vectors V221 direct SDK, immediate repeat | 91,057 | 61.511 / 65.857 / 67.063 / 76.901 | 128.6 |

Both BORSUK passes matched **all 1,000 frozen V219 ID lists exactly**;
p05 GT100 hits/query was 98. The V222 raw SHA-256 values are
`d3c54322e4f1cadbf3374a99f683c084899087343d99d3a321e7ba2b1a4afdec`
(first) and `b9aa0c7f9d0e63fa42fa33600e23118886cffedf1b5a5443a6e1c6f310f7d10c`
(repeat). Independent readback recomputed each percentile from the same
1,000 raw durations, wall-time QPS, 14,658,893 logical request bytes,
1,026,973 logical response bytes, zero vector GETs and peak eight
simultaneous requests per pass. Server peak RSS was 2,030,424,064 bytes.

V221 used the same dataset, validation queries, k, client instance class,
AZ and concurrency. Its authenticated raw samples and terminal are
documented in [V221 closeout](v221-s3-validation-1m-closeout.md).
The cells differ in the serving system: BORSUK held its whole index in
server RAM and used private plaintext HTTP; S3 Vectors used regional HTTPS
through the SDK and has opaque internal cache state. V222 recreated client
connections between passes; V221 reused an SDK client pool. Thus these
are directly measured directional product results on a shared workload,
not an equal-cache, equal-transport service comparison. V220's loopback
timer excluded JSON codec work and is a separate timing scope.
Turbopuffer has no authenticated matched tenant measurement; published
cold numbers are context only and cannot establish a win.

The launch-time Spot quote was $0.3631/hour per host. Compute to both
terminal markers is estimated at **$0.035657**; this excludes EBS, S3,
index build, hydration, network and billing adjustments. The V221
client-only compute estimate was $0.100962 and excludes S3 Vectors
service charges, so it is not a service-cost comparison.

**Next gate after restart:** one coherent library API for authenticated
generation loading and graph search, with a narrow correctness/recovery
check and the same 1M serving gate from that exact revision. Then measure
10M and 100M within a declared RAM/recall envelope; do not impose an
arbitrary vector-count knee or extrapolate 1M latency as a measurement.
