# V246 authenticated V245 graph, S3 hydration and VPC-peer HTTP

**Decision: pass the frozen persisted HTTP gate.** The V245 graph was
published through the production conditional S3 generation head,
authenticated on readback, hydrated into an empty server cache and
served over VPC-peer HTTP. Both passes returned exactly the V245
selected-arm ID lists. This is resident query serving after cold
startup hydration, not cold object-store retrieval per query.

The sole valid two-host `causality` c7i.4xlarge Spot attempt `a0001`
used server `i-03a7fab1878e95024` and client
`i-0c9ec133f87fbee52` in `eu-central-1c`; both are **terminated**.
Source commit `f40baabc9a8851aa57ed5b6fe35115fa795c3192`, archive
SHA-256 `b92fbee85540d2efa6bce92cd99ff2c635db6343df8ccc0febde9130bf3c2148`.
Server/client terminal SHA-256 values are respectively
`f07b5c8bb138d4d8834687f47d42d8e4d0440fe53cbab0b9d87b8d4201f507c3`
and `b389a504f7601d382577f514faf85cd78ea4407ce6247f1f438126041c5dbd5a`;
closeout SHA-256 is
`e758b1d5a8971f32b6464964e19094f643a41b14b3a9384b4075e96acfff731f`.
Both terminals exited zero. The original launcher replayed all terminal
artifact byte counts and SHA-256 hashes, and independently confirmed
termination. The sealed first/repeat raw SHA-256 values are
`966ea785e09071cb5bee7f359cdb7ac3f083ac183ea7c38dd9ab3fc54f9a7712`
and `d4c9598f4c62b18395dc9b0151ae9973a4779253383db31c4a2b7cec748af9e4`.
Independent replay of both sealed streams, the V245 reference and
V198 GT100 reproduced the quality and all latency percentiles below.
Evidence:
`s3://borsuk-bench-453182569524-euc1/research/v246-owner-graph-http-1m/f40baabc9a8851aa57ed5b6fe35115fa795c3192/runs/a0001/`.

Dataset and split: ReLAION-1M D768, cosine k=100, validation
ordinals 0–999 **already used**; development 0–255 and remaining
256–999. The graph uses PQ64 navigation, authenticated FP16 rerank,
ef/shortlist 4096/4096. Eight persistent client HTTP/1.1 connections
queried a server with eight resident workers and no response cache.
Both passes used the same corpus and query panel as V245. The
generation root SHA-256 was
`58dcfbda4e5dec2402043c44e0e7f02b649d1d0a46d69ab1be48c61cf5add3dc`.

| Verified V246 pass | Exact GT100 hits / 100,000 | Dev / 25,600 | Remaining / 74,400 | p05 hits/query | Client p50/p90/p95/p99, ms | Completed QPS |
| --- | ---: | ---: | ---: | ---: | --- | ---: |
| First | 99,662 | 25,506 | 74,156 | 98 | 16.039 / 21.447 / 23.375 / 26.563 | 454.3 |
| Immediate repeat | 99,662 | 25,506 | 74,156 | 98 | 15.506 / 20.427 / 22.035 / 24.516 | 479.7 |

Each pass matched **1,000/1,000 complete ordered V245 ID lists** and
passed the frozen p95≤35 ms, p99≤45 ms, ≥300 QPS and quality gates.
Each sent 14,658,893 request bytes and received 1,026,971 response
bytes. Query vector-body GETs were zero. Server peak RSS was
**2,034,659,328 B**, below 3 GiB. Initial S3 hydration took
34.379 s and fetched exactly five graph blobs totaling
1,879,696,738 response bytes. Head/root metadata GETs are not in
that blob counter. The two-host Spot compute estimate through terminal
was **$0.04354** at $0.3676/hour per host, excluding EBS, S3 storage,
PUT/GET, network and billing rounding.

The earlier V237 persisted **mutation** collection, on a different
graph/revision and Spot hosts, had first-pass 99,668 hits and
p50/p90/p95/p99 19.223/24.015/25.254/27.783 ms at 395.8 QPS,
with 2,115,366,912 B server RSS. V246 differs in topology and has no
mutation overlay, so the apparent latency/QPS difference is
descriptive, not an isolated builder or serving gain. V245's
in-process selected arm had p50/p90/p95/p99
15.107/19.525/20.617/23.047 ms and 498.9 QPS; these are different
timing scopes. Historical direct S3 Vectors V221 first pass on this
panel had 90,996 hits and SDK p50/p90/p95/p99
67.314/176.974/206.064/303.212 ms, with opaque server cache,
regional HTTPS and a different client/source revision. It is context,
not a matched current product latency win. There is no authenticated
Turbopuffer tenant result.

**Next gate:** one direct fresh-index S3 Vectors cell on this same
ReLAION-1M validation panel, cosine k=100, client instance class,
region, eight-request concurrency and ordinal request order. Report
both first and repeat raw per-query latency percentiles, recall,
throughput, bytes, memory and full cost. Label BORSUK's resident-after-
hydration semantics and S3 Vectors' opaque server cache and regional
HTTPS transport honestly. If those paired results qualify the product,
proceed to 10M scale and remove the measured preparation and pinned-
revision RAM duplication; avoid a fixed vector-count memory knee.
