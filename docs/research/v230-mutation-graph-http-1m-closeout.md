# V230 same-vector mutation HTTP, ReLAION-1M

**Decision:** The 1M network mutation-serving gate passed. Retain the
authenticated resident graph plus bounded mutation overlay for the
measured 10,000-row envelope. A linear scan of 10,000 × 768 FP16
coordinates per query nearly doubles service latency and halves
throughput, so larger mutation batches need an indexed delta or a
measured compaction policy before 10M/100M promotion. The 10,000-row
point is evidence, not a generic vector-count knee or frozen default.

The sole Causality `c7i.4xlarge` Spot attempt `a0001` used server
`i-0c45987baeb723130` and client `i-000212d3b2a396e9e` in
`eu-central-1c`; both are independently confirmed **terminated**.
Source commit `014d1fb36f9f69004c2c25b0648765bc369c4cd0`, archive SHA-256
`01c28488dc03e83931dc6f160afe01283b670263f69aed8fe841063013bcf592`,
generation root SHA-256
`c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf`.
Server/client terminal SHA-256 values are respectively
`899f2ce478e17c98cd877f5cfda1a0a67d98aa36a1c7fe3a1e7d8a0daaca8233`
and `35e60040ff6264c7abc9644bddd0ce58aeb04daa99bb6b91ee1a3e143af02c20`.
All 16 terminal artifacts and both sealed client raw passes passed
independent byte-length and SHA-256 replay. Closeout SHA-256 is
`103a5b05bc7239ae41766a59ee493590301cce61db2fcd44aeb0a7465b2b21db`.
Evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v230-mutation-graph-http-1m/014d1fb36f9f69004c2c25b0648765bc369c4cd0/runs/a0001/`.

Dataset: ReLAION-1M, 768 dimensions, **validation ordinals 0–999
previously used**, cosine k=100 and exact GT100. The server authenticated
the V223 single root and all immutable blobs, then upserted every
hundredth physical row under the same ID and authenticated FP16 vector:
10,000 rows, unchanged logical corpus and GT100. Search retained
ef/shortlist 4,096/4,096. Eight persistent VPC-peer HTTP/1.1 clients
measured JSON encode, exchange and decode against a fully resident
server without a response cache. Zero vector-body GETs were reported
for every query. This cell tests query serving with source-derived
mutations; V228 separately verified real-S3 collection-head CAS on a
four-row corpus. It does **not** establish a durable 1M mutation
publication or cold per-query S3 retrieval.

| Verified measured cell | GT100 hits / 100,000 | p05 hits/query | p50 / p90 / p95 / p99, ms | QPS |
| --- | ---: | ---: | ---: | ---: |
| V223 authenticated HTTP, first | 99,664 | 98 | 14.828 / 19.302 / 20.820 / 22.564 | 497.0 |
| V223 authenticated HTTP, repeat | 99,664 | 98 | 14.765 / 19.269 / 20.713 / 22.573 | 502.2 |
| **V230 1% same-vector upserts, first** | **99,668** | **99** | **31.269 / 36.714 / 38.789 / 44.062** | **244.1** |
| **V230 1% same-vector upserts, repeat** | **99,668** | **99** | **31.176 / 36.364 / 38.195 / 42.932** | **247.9** |

V230 first and repeat each had 25,509/25,600 development-subset hits
versus V219's 25,508, and 74,159/74,400 remaining-validation hits
versus 74,156. Both passes tied 994 of V219's 1,000 full ID lists.
Sealed raw SHA-256 values are
`b4597049dd6959dbf5516342408a31e863fbe13505987f88811d58a4ccdd5d14`
and `e14e53a2601e0b6f0fe64bdf227b9c4a2431e7db21d93cd4db3acd1967dbf30c`.
Independent raw replay recomputed all four latency percentiles from
each pass's same 1,000 samples. Each pass carried 14,658,893 logical
request bytes and 1,026,973 logical response bytes. Server peak RSS
was 2,071,330,816 bytes; the overlay owned 15,645,000 bytes. V223
server peak RSS was 2,026,614,784 bytes in a separate run, so the
full difference is not attributable solely to overlay allocation.

Both passes passed the preregistered split quality, p05≥98,
p95≤41.640 ms, p99≤45.128 ms, throughput≥200 queries/s, RSS≤3 GiB,
10,000 delta rows and zero vector GET gates. The launch-time Spot
quote was $0.3631/hour per host; estimated compute to both terminals
was $0.03684, including Spot build/input time but excluding EBS, S3,
network charges and billing adjustments. V223 is the strongest BORSUK comparison using the same
panel and peer shape, but it is a separate source revision and run:
timing differences are descriptive. V221 S3 Vectors has lower recall
and unequal internal cache/transport semantics; no authenticated
Turbopuffer tenant result exists. No matched product win is claimed.

**Next single gate:** cheaply falsify an indexed mutation delta on
ReLAION-100k at 10,000 same-vector upserts against this linear overlay,
holding graph, panel, GT100 and resource accounting fixed. Promote
only if it preserves per-split recall and reduces the delta's query
cost enough to justify a 1M end-to-end repeat. Track update/build
cost as well as query latency; a low query time with a full expensive
rebuild on each batch is not a usable mutation method. After that,
exercise real-S3 1M collection-head publication and pinned readers,
then freeze the architecture for 10M and 100M scale gates with RAM
chosen by the recall/latency envelope, not N alone.
