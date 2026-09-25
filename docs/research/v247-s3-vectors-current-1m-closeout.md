# V247 direct S3 Vectors ReLAION-1M comparator

**Decision: the frozen direct comparator completed.** On the same
1M corpus, 1,000 validation queries, cosine k=100 and eight-request
client concurrency, BORSUK V246 returned higher exact GT100 recall,
lower client-observed latency and higher completed throughput in both
passes. The network path, TLS and cache semantics differ: V246 is a
resident BORSUK server reached by VPC-peer plaintext HTTP after S3
hydration; V247 is the regional HTTPS S3 Vectors SDK with opaque
managed cache. These observations do not establish a strict
transport- and cache-matched service win or a Turbopuffer win.

The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-046b42366c929f46c` in `eu-central-1c`, now **terminated**. Its
temporary S3 Vectors index and vector bucket were deleted; the
worker cleanup receipt and a separate `GetVectorBucket` NotFound
check agree. Source commit
`d422a352f83949c93a48a561ad293e694e1cc46f`, source archive
SHA-256 `689cc0d8e875257900f671b8a454923ba7348284c51feea56fccce9d4e1da22d`,
terminal SHA-256
`15be4f7f3710730b2eabb429414ffc748735ac01b33d850acaa5eafa2b33f219`.
The terminal exited zero and marks the claim eligible. Independent
readback checked byte counts and SHA-256 for all five evidence objects;
raw sample Parquet SHA-256 is
`22e9391b1474c80bdcea202ba887140d3357a74fa8d6246128b0f5eca6b64e06`.
Independent raw replay reproduced every percentile, recall count,
response byte count and zero retry count. Evidence:
`s3://borsuk-bench-453182569524-euc1/research/v247-s3-vectors-current-1m/d422a352f83949c93a48a561ad293e694e1cc46f/runs/a0001/`.

Frozen dataset: ReLAION-1M, D768, validation ordinals 0–999
**already used**, development 0–255 and remaining 256–999. Both
products use cosine search and k=100; the shared exact GT ranks by
squared Euclidean distance. The source norms are close to, but not
exactly, one. The BORSUK R@10 values below were independently replayed
against authenticated ranked GT Parquet SHA-256
`bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871`.
BORSUK V246 source commit
`f40baabc9a8851aa57ed5b6fe35115fa795c3192` and closeout SHA-256
`e758b1d5a8971f32b6464964e19094f643a41b14b3a9384b4075e96acfff731f`.

| Measured pass | GT100 hits / 100,000 | GT10 hits / 10,000 | p05 GT100 hits/query | Client p50/p90/p95/p99, ms | Completed QPS |
| --- | ---: | ---: | ---: | --- | ---: |
| BORSUK V246 first, peer HTTP | **99,662** | **9,972** | **98** | **16.039 / 21.447 / 23.375 / 26.563** | **454.3** |
| S3 Vectors V247 fresh-index first, SDK HTTPS | 91,746 | 9,835 | 69 | 64.484 / 126.329 / 145.425 / 250.121 | 95.6 |
| BORSUK V246 immediate repeat | **99,662** | **9,972** | **98** | **15.506 / 20.427 / 22.035 / 24.516** | **479.7** |
| S3 Vectors V247 immediate repeat | 91,810 | 9,833 | 69 | 60.691 / 64.849 / 65.800 / 69.861 | 130.6 |

S3 Vectors' first pass had 23,522/25,600 development and
68,224/74,400 remaining GT100 hits; repeat had 23,540 and 68,270.
BORSUK's two passes had 25,506 and 74,156. The first-pass gap was
**7,916 GT100 hits** (7.916 percentage points) and 137 GT10 hits
(1.37 points). Observed first-pass client p95 was 6.22× lower for
BORSUK and throughput was 4.75× higher; repeat p95 was 2.99× lower.
These ratios describe the disclosed cells, not an isolated algorithm
or service effect. S3 Vectors reported zero SDK retries and one page
per query. Its first/repeat response logical bytes were
5,077,126/5,077,086; BORSUK's were 1,026,971 in each pass, with
14,658,893 request bytes per pass and zero query vector-body GETs.

S3 Vectors ingested 1,000,000 vectors in **844.365 s**, averaging
1,184.3 vectors/s across 2,000 PUT requests and 3,080,770,349
logical upload bytes. The client process peaked at **574,021,632 B
RSS**, with zero swap and zero recorded memory pressure. Server-side
S3 Vectors RAM is undisclosed; BORSUK V246's server peaked at
2,034,659,328 B. The S3 worker ran 956 s; its c7i.4xlarge Spot
compute estimate is **$0.097619**, excluding EBS and billing
adjustments. BORSUK V246's two-host measurement compute estimate
was $0.04354; V245 graph build was a separate $0.1012 estimate.
These do not include equal full lifecycle or idle-service costs.

The AWS Pricing API `AmazonS3` Frankfurt SKUs effective
2026-09-01 give $0.214/GB PUT (`6A5A55ZKRUBC4TUQ`), $0.0000027/query
(`BUG2PPMDF9GYHM8X`), $0.000004185/GB for the first 100k vectors'
processed bytes (`GWZJT9M3FX8WTUFT`), $0.000002093/GB for the next
900k (`TSAPJTJDPHBM56SW`), and $0.064/GB-month storage
(`NPN725YSWYPAU9F8`). Applying those rates to the recorded logical
bytes and 2,000 queries, with 1 GiB per priced GB and the full 956 s
as a conservative storage duration, gives **about $0.633 S3 Vectors
service cost**: $0.614 PUT, $0.0186 query and at most $0.00007
storage. Per-result minimum billing remains below the free 512 KB
returned per query, so returned-data charge is modeled as zero.
Using decimal GB would give about $0.679 instead. This is a
**pricing-model estimate, not an observed AWS bill**; adding the
client Spot estimate yields about $0.730 under the binary convention.
The [AWS S3 pricing page](https://aws.amazon.com/s3/pricing/) defines
the charge components; the exact Frankfurt rates above came from
`Pricing.GetProducts` using the listed SKUs.

**Next gate:** qualify the selected architecture beyond 1M with an
authenticated 10M dataset and exact GT panel, choosing build and
serving memory from measured per-vector components and the selected
recall target rather than a fixed vector-count knee. The 1M Python
source preparation peaked at 7,809,417,216 B and is a separate
scaling constraint; size the machine or make preparation streaming
before launch. Preserve the V245/V246 graph and HTTP contracts and
freeze quality, p50/p90/p95/p99, throughput, GETs/bytes, peak RSS
and cost for the 10M attempt. A direct authenticated Turbopuffer
tenant comparison remains unavailable; published vendor numbers
stay contextual. Lean may prove conditional operation and memory
bounds, while recall and wall-clock latency require measurements.
