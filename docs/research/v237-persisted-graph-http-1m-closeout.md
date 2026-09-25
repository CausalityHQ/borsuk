# V237 persisted graph mutation HTTP, ReLAION-1M

**Decision: the preregistered persisted-serving gate passed.** A fresh
server read the real V236 S3 collection head at revision 2, authenticated
its graph root and 10,000-row mutation snapshot, hydrated five graph
blobs, then served the decoded overlay over VPC-peer HTTP. Both passes
matched all 1,000 complete V230 and V233 ID lists. The result closes
the missing 1M publication → reload → network-serving path. It does
not establish a matched win over S3 Vectors or Turbopuffer, or cold
per-query retrieval latency.

The sole two-host `causality` c7i.4xlarge Spot attempt `a0001` used
server `i-0f61260c6d8d5adf1` and client `i-0c3208b84e72db379`
in `eu-central-1c`; both were independently confirmed **terminated**.
Source commit `34c352195bf748fc0d89be21f8adf6febff0449e`, source
archive SHA-256
`9ca7935b1214bad3cb04ae4c324fcf5f0f4a90b8217e6521fdee18f13fb5eda0`.
Original server/client terminal SHA-256 values are respectively
`f3c2a391887d37c482539c32cf1c9d4ebe98f2ca620ff9a29f50b957c9555774`
and `b2c1c47bace4e0a3be6d88d17bf4027d67cfed2a18748d9af379747a18fc6c72`;
closeout SHA-256 is
`ef93c549f81a92a5450566430aa1efcff09585a5d2bab92075e0a1b48e57c996`.
Both terminals exited zero. All 17 artifact lengths and hashes, both
sealed raw streams and same-sample percentiles passed independent
replay. Evidence:
`s3://borsuk-bench-453182569524-euc1/research/v237-persisted-graph-http-1m/34c352195bf748fc0d89be21f8adf6febff0449e/runs/a0001/`.

Dataset/split: ReLAION-1M D768, validation queries 0–999 previously
used in method development, cosine k=100, exact GT100, ef/shortlist
4096/4096. The collection has every 100th physical row upserted under
its same ID and FP16 vector (10,000 rows), preserving the logical
corpus. Eight persistent VPC-peer HTTP/1.1 connections measured client
JSON encode, exchange and decode, with no response cache. Both servers
were resident during query timing, and the V237 server first hydrated
from real S3 into an empty local cache. V233 used local authenticated
artifacts and a source-derived mutation overlay in a separate earlier
run; its cells are the relevant BORSUK serving baseline, with identical
logical data and query shape.

| Verified cell | GT100 hits / 100,000 | p50 / p90 / p95 / p99, ms | QPS |
| --- | ---: | ---: | ---: |
| V233 decoded overlay, first | 99,668 | 23.705 / 30.083 / 32.016 / 34.948 | 319.2 |
| V237 persisted revision 2, first | 99,668 | 19.223 / 24.015 / 25.254 / 27.783 | 395.8 |
| V233 decoded overlay, repeat | 99,668 | 22.980 / 29.077 / 30.622 / 34.633 | 327.0 |
| V237 persisted revision 2, repeat | 99,668 | 19.446 / 24.706 / 26.281 / 29.590 | 382.7 |

V237's 1,000/1,000 ID parity held in each pass. Independent GT100
replay gave 25,509/25,600 hits in queries 0–255, 74,159/74,400 in
queries 256–999 and p05=99 hits/query. First/repeat sealed raw SHA-256
values are
`9f2cc94b57173e9393281e35c23ab78c6e82959d513fd3dcdbf5f2ac01acce55`
and `e116f5c80e2bafc645d132c4e79f3ccc4ce3f3d336dff81b022fa4ed24784169`.
Each pass sent 14,658,893 request bytes and received 1,026,973
response bytes. Query vector-body GETs were zero.

The fresh server's authenticated startup hydration took 35.721 s and
fetched five graph blobs totaling 1,879,697,462 response bytes; its
decoded overlay owned 31,005,000 bytes and peak process RSS was
2,115,366,912 bytes. V233 peak server RSS was 2,057,392,128 bytes in
a separate run. Collection-head, root and snapshot metadata GETs are
not in the graph-blob counter. V237's Spot quote was $0.3676/hour per
host and compute estimate to both terminals was $0.03626, excluding
EBS, S3, network and billing adjustments.

V237 first/repeat p95 were 21.1%/14.2% lower and QPS 24.0%/17.0%
higher than V233, respectively. These differences are descriptive
across separate hosts and source revisions; the loader's startup path
is outside query timing, so this run does not prove that S3 persistence
caused a serving speedup. It does show no measured serving regression
under the frozen V237 gate. The two pinned revisions in V236 peaked
at 4.35 GB RSS because the current reload opens a second 1M graph;
the single serving reader here used 2.12 GB.

**Production decision:** retain the authenticated collection head,
decoded resident mutation overlay and pinned-reader swap as the
current 1M format. Implement same-root base sharing across mutation
revisions to remove the measured swap memory duplication; preserve
old-reader pinning and CAS authentication. Set mutation admission and
compaction from explicit recall, latency and memory budgets, not a
fixed vector-count knee. Qualify that memory fix cheaply before
another 1M campaign. Competitive claims still require paired
no-cache/cold methodology against S3 Vectors and authenticated
Turbopuffer access; published vendor numbers are context only.
