# V233 decoded mutation delta HTTP, ReLAION-1M

**Decision: exactness passed; the preregistered product speed gate
failed.** Retain decoded FP32 mutation storage as a useful exact
implementation, but do not promote this query path as the final 1M
mutation architecture. The 100k V232 in-process p95 reduction did not
translate into the required 1M HTTP reduction. The base graph and
HTTP work remain material parts of the end-to-end latency. Another
representation-only change to the same 10,000-row full scan is unlikely
to reach the frozen 1.5× throughput gate. The next cheap gate should
test a materially different **certified exact-pruning** mutation tier:
use a query-dependent upper bound to skip delta rows whose score cannot
enter the current top k, then exact-score all survivors. A conservative
bound must preserve ID ordering for every query; measure its actual
pruning and overhead on the 100k/10k panel before any further 1M run.

The sole `causality` two-host c7i.4xlarge Spot attempt `a0001` ran in
`eu-central-1c` on server `i-0db4c7928376a7db2` and client
`i-00063955d1f2fc493`, both independently confirmed **terminated**.
Source commit `60dc982cce46bd1baf2fb4bdf163d90be32f1aea`, source
archive SHA-256
`b35418efe4d9c3cfb5d39c4746b41dab3431c5d0d353f6877b3ff4b51df5d0a2`.
Server/client terminal SHA-256 values are respectively
`119df2350b2f5f7a0bd3f4a5fc222718b997d32db433ddacd6fe6b97dc023871`
and `69f8ccbd38ae12666db21d3c035b4daa75878a1fe046aba83bcd9fb0baccd804`;
closeout SHA-256 is
`9cb96ffc7b2bc45e8d3138e86f98776610cf780ffdb359ff57eca75fab74550a`.
The controller completed at exit zero and independently replayed all
16 terminal artifact lengths/hashes, both sealed client raw hashes and
p50/p90/p95/p99 from each raw stream. Evidence:
`s3://borsuk-bench-453182569524-euc1/research/v233-decoded-graph-http-1m/60dc982cce46bd1baf2fb4bdf163d90be32f1aea/runs/a0001/`.

Dataset: ReLAION-1M D768, **previously used** validation ordinals
0–999, cosine k=100 and exact GT100. Both systems authenticate the
same V223 graph root and immutable blobs, use ef/shortlist 4,096/4,096
and upsert every hundredth physical row under the same ID and FP16
vector (10,000 rows). Eight persistent VPC-peer HTTP/1.1 connections
measure JSON encode, exchange and decode against a resident server,
without a response cache. V230 is a separate earlier source revision
and two-host run with the same workload and peer shape; timing changes
are descriptive. This is neither cold per-query object retrieval nor a
durable 1M mutation-publication test.

| Verified cell | GT100 hits / 100,000 | p50 / p90 / p95 / p99, ms | QPS |
| --- | ---: | ---: | ---: |
| V230 linear delta, first | 99,668 | 31.269 / 36.714 / 38.789 / 44.062 | 244.1 |
| V233 decoded delta, first | 99,668 | 23.705 / 30.083 / 32.016 / 34.948 | 319.2 |
| V230 linear delta, repeat | 99,668 | 31.176 / 36.364 / 38.195 / 42.932 | 247.9 |
| V233 decoded delta, repeat | 99,668 | 22.980 / 29.077 / 30.622 / 34.633 | 327.0 |

Both V233 passes matched **1,000/1,000 complete V230 ID lists**, with
25,509/25,600 development-subset and 74,159/74,400 remaining
validation hits, p05=99 and zero vector-body GETs. Sealed first/repeat
raw SHA-256 values are
`7850169e6356c8ea2749bf0a24734c4f3d9cab69af676cdc07fe8c8ea8c8c17e`
and `d8a2f63d47bbca89b152c2a5df706709c03f97e1595fe6bda70325258c07d416`.
Server peak RSS was 2,057,392,128 bytes; overlay-owned resident bytes
were 31,005,000. V230's server peak RSS was 2,071,330,816 bytes and
overlay-owned bytes 15,645,000, measured in a separate process/run.
The V233 launch-time Spot quote was $0.3631/hour per host, with
estimated compute to both terminals $0.04133, excluding EBS, S3,
network charges and billing adjustments.

The first-pass p95 fell 17.5% and QPS rose 30.8%; repeat p95 fell 19.8%
and QPS rose 31.9%. Both missed their preregistered p95≤75% and
QPS≥1.5× V230 limits; quality, memory and GET limits passed. V223's
unmutated HTTP first pass was 99,664 GT100 hits, p95 20.820 ms and
497.0 QPS, but uses a different logical mutation state. No matched S3
Vectors or Turbopuffer win follows from this gate.

**Next single gate:** on the frozen ReLAION-100k D768 10,000-upsert
panel, compare certified exact pruning against the same-run decoded
full scan. Require 1,000/1,000 exact ID lists and no per-split recall
loss; measure rows pruned, bound cost, loaded p50/p90/p95/p99, QPS,
mutation preparation, RSS and resident bytes. Preregister an actual
product-relevant speed gain before running. If the bound prunes too
little or costs too much, reject it before a 1M cell and choose a
different mutation-tier/compaction architecture.
