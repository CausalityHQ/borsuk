# V221 direct S3 Vectors ReLAION-1M validation closeout

**Decision:** the frozen direct S3 Vectors comparator completed and its
quality is below BORSUK V220 on the same GT100 validation panel. Latency
is measured, but V220's resident loopback transport is not a matched
network service comparison; proceed to the frozen external BORSUK gate.

Source commit `a1ed538f8e3386d70be2f41a43f44f74325ea047`, source archive
SHA-256 `a4c191ad6a87cdbefc71c8d284de38baa2e2d1ebc4920c2668296d0ea432890b`.
The one `causality` c7i.4xlarge Spot client was `i-0249de65a9bcbabe0` in
`eu-central-1c`; it is terminated. Terminal SHA-256
`9301c109829af1b86d6b6ad08c274ad2d398f3711b5925d14fd83d6302f164b6`
reports complete, exit zero and claim eligibility. All five terminal
evidence objects passed independent byte and SHA replay. The S3 Vectors
index and bucket were both deleted. Raw sample Parquet SHA-256
`cfd7d6450c37a81903ea259a32e6594df005fa74a9a63ae68f85664215bf595e`
was independently replayed. Evidence prefix:
`s3://borsuk-bench-453182569524-euc1/research/v221-s3-validation-1m/a1ed538f8e3386d70be2f41a43f44f74325ea047/runs/a0001/`.

Dataset: ReLAION-1M D768, **validation ordinals 0–999 already used**,
k=100. The complete authenticated float32 source was ingested into a
fresh S3 Vectors **cosine** index. The shared GT100 is squared Euclidean;
source norms are nearly, but not exactly, one. Eight concurrent SDK
requests ran in ordinal order per pass after a 60-second settle period.
The managed service's cache state is opaque; “first” and “repeat” below
describe only execution order.

| Direct S3 Vectors measure | Fresh-index first pass | Immediate repeated pass |
| --- | ---: | ---: |
| GT100 hits / 100,000 | 90,996 | 91,057 |
| p05 hits/query | 68 | 67 |
| Client SDK p50 / p90 / p95 / p99 | 67.314 / 176.974 / 206.064 / 303.212 ms | 61.511 / 65.857 / 67.063 / 76.901 ms |
| Completed throughput | 80.9 queries/s | 128.6 queries/s |
| Response logical bytes | 5,077,490 | 5,077,450 |
| SDK retry attempts | 0 | 0 |

All percentiles, hit totals, p05, bytes, zero retries and peak eight
simultaneous SDK calls were independently recomputed from the same sealed
2,000-query raw sample file. BORSUK V220 on this panel returned
**99,664/100,000** GT100 hits (p05 98). Its p50/p90/p95/p99 were
14.896/19.809/21.224/24.208 ms, 497.8 QPS, with a resident index
over **loopback HTTP**. Those latency and throughput values are not
apples-to-apples with the S3 regional HTTPS SDK path; no product
latency win is claimed from these two cells.

Ingestion took 887.476 seconds for 1,000,000 vectors and 2,000 PUT
requests, 3,080,770,349 logical input bytes (1,126.8 vectors/s).
Client peak RSS was 552,648 KiB. The worker ran 1,001 seconds with
zero measured swap or memory pressure. The launch-time Spot quote was
$0.3631/hour; the worker's **$0.100962 compute estimate** excludes
S3 Vectors ingestion, storage and query charges, EBS, S3 evidence and
billing adjustments.

**Next gate:** two-host VPC-peer BORSUK HTTP on this same validation panel,
eight concurrent requests and c7i.4xlarge client. Report both passes,
actual cache state, TLS/transport differences, peak server RSS and both
hosts' compute cost. Turbopuffer still lacks authenticated tenant access;
its published numbers cannot establish a matched win.
