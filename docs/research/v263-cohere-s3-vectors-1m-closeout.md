# V263 CoHere first1M direct S3 Vectors closeout

**Decision: complete; BORSUK quality leads, warm latency and throughput do
not.** V263 used the exact CoHere-large-10M canonical first1,000,000
float32 rows, prior-used query ordinals0–999, D768 cosine k100 and GT100
truth of BORSUK V262. One fresh S3 Vectors index was ingested, queried
in ordinal order by eight concurrent SDK clients, and deleted. S3 Vectors'
managed cache is opaque. BORSUK served from authenticated resident memory
over private plaintext HTTP after cold S3 hydration; S3 Vectors used
regional HTTPS. This is a direct same-data comparison, not a strict
transport/cache-matched service win.

| Pass, 1,000 queries | Product | GT100 hits / 100,000 | GT10 hits / 10,000 | p50 / p90 / p95 / p99, ms | QPS |
| --- | --- | ---: | ---: | --- | ---: |
| First | BORSUK V262 | **99,717** | **9,958** | 116.236 / 142.162 / **149.749** / **158.941** | 67.13 |
| Fresh-index first | S3 Vectors V263 | 88,803 | 9,222 | **67.011** / 164.977 / 187.630 / 254.842 | **82.99** |
| Immediate repeat | BORSUK V262 | **99,717** | **9,958** | 116.049 / 141.497 / 147.844 / 157.258 | 67.37 |
| Immediate repeat | S3 Vectors V263 | 88,864 | 9,223 | **61.332** / **65.317** / **66.394** / **80.845** | **128.79** |

V263 first GT100 split hits were development22,474/25,600 (p05 72)
and validation66,329/74,400 (p05 73); repeat was 22,495 (p05 71)
and 66,369 (p05 73). BORSUK both passes were development25,511
(p05 98) and validation74,206 (p05 99). V263 SDK reported zero retries;
response logical bytes were 4,797,206 first and 4,797,216 repeat.
Independent replay of the sealed V263 Parquet samples reproduced all
four percentiles, split hit totals, retry counts and query ordinals.
Independent replay of V262 JSONL and exact GT reproduced its GT10/GT100
counts. First-pass BORSUK recall advantage was 10.914 percentage points;
repeat advantage was 10.853 points. S3 Vectors repeat p95 was 2.23×
lower and throughput 1.91× higher. These ratios describe different
service transports and cache semantics.

S3 Vectors ingested 1,000,000 vectors in 969.951 s with 2,000 PUTs and
3,077,888,890 logical bytes. The sole c7i.4xlarge Spot client
`i-01bee59e1b80f96be` in eu-central-1c ran 1,092 s, peak client RSS
351,797,248 B, no swap or memory pressure, and about $0.11111 compute
at the frozen $0.3663/hour quote. S3 Vectors server memory is opaque.
The temporary vector index and bucket were deleted; the cleanup receipt
and independent `GetVectorBucket` NotFound check agree. No BORSUK EC2
instance remained active.

Using the AWS Pricing API's Frankfurt SKUs effective 2026-09-01 and
binary GiB gives a **pricing-model estimate**, not an observed bill:
$0.61343 PUT, $0.01860 for 2,000 queries, and at most $0.00008 storage
through the 1,092-second worker, about **$0.63211 S3 Vectors service**
plus $0.11111 Spot client. The model assumes full index size for the
whole worker and zero returned-data charge below the free 512 KB/query.
Decimal GB would yield a different estimate; these costs do not match
BORSUK's full production lifecycle. The [AWS S3 pricing page](https://aws.amazon.com/s3/pricing/)
defines the charge components. Pricing API SKUs are
`6A5A55ZKRUBC4TUQ`, `BUG2PPMDF9GYHM8X`, `GWZJT9M3FX8WTUFT`,
`TSAPJTJDPHBM56SW`, and `NPN725YSWYPAU9F8`.

Source commit `82488981badb123e687a9904087d8cac84f6e526`, immutable S3
prefix `research/v263-cohere-s3-vectors-1m/82488981badb123e687a9904087d8cac84f6e526/runs/a0001/`
in `borsuk-bench-453182569524-euc1`. Readback verified terminal
SHA-256 `4e55f1fe1af0c8a6692f7d84559bbd18df0e8dafa09262489aaebd6961f834ee`,
result SHA-256 `65830f5d7e031037a7f6f89c511a2d160be290beae3e94115c60f65e5c6a562d`,
raw sample SHA-256 `98ecd984d29e3fb803fab317c22af2b826d5b1c6b09b75bb8e7df1581e98e139`,
and closeout SHA-256
`535268ec43fbf106894160ca99525e3d3a4a390dfe23ed104a55a4a0b52cddbe`.

**Next decision gate:** BORSUK's V261 loaded in-process p95 was 116.782
ms, so even eliminating V262's HTTP overhead would not reach V263's
66.394 ms repeat p95. Make one material graph-navigation or scoring
redesign, falsify it against the strongest current BORSUK baseline on
the cheap 100k CoHere panel, then promote only a recall-preserving
latency winner to a frozen 1M HTTP cell before 10M. Preserve the
99.5% GT100 quality threshold and report memory, bytes/GETs and cost.
