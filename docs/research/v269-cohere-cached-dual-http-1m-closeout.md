# V269 CoHere first1M: cached dual graph passes HTTP gate

**Decision: keep the cached dual graph as the current first1M
authenticated serving path.** Both VPC-peer client passes preserved
all1000 ordered k100 lists and99,717/100,000 exact GT100 hits while
meeting the frozen client latency, throughput, hydration and memory
gates. This is a measured product HTTP result after empty-cache
hydration. It is not a strict matched S3 Vectors or Turbopuffer win.

Dataset: CoHere-large-10M canonical first1,000,000 rows, D768 cosine
k100, prior-used test ordinals0–999; development0–255,
validation256–999. Search used the V268 fast FP16 graph plus exact
PQ shortlist score cache, then FP16-reranked the distinct union. The
five persisted artifacts, root SHA-256
`1e483859b96f5270209678e0f76f7cc9e26a80162a24c7fa48a942cf010ec92e`,
query panel and exact GT100 truth match V261/V268. One c7i.4xlarge
Spot server and one same-class client ran in eu-central-1c with eight
persistent HTTP/1.1 VPC-peer connections and no response cache.

| Measured pass, 1,000 queries | Exact GT100 hits / 100,000 | Client p50/p90/p95/p99, ms | QPS |
|---|---:|---:|---:|
| V262 FP64 dual, first, historical | 99,717 | 116.236 / 142.162 / 149.749 / 158.941 | 67.13 |
| V269 cached dual, first | 99,717 | 55.587 / 66.102 / 69.462 / 73.902 | 141.58 |
| V262 FP64 dual, repeat, historical | 99,717 | 116.049 / 141.497 / 147.844 / 157.258 | 67.37 |
| V269 cached dual, repeat | 99,717 | 55.697 / 66.813 / 70.555 / 77.172 | 140.68 |

V269 both passes returned development25,511/25,600 (p05 98) and
validation74,206/74,400 (p05 99). Independent replay of each sealed
raw JSONL reproduced all four same-sample percentiles; the scored
client receipts confirm1000/1000 ordered-list parity to V261, which
V268 had already matched exactly. Each pass sent15,356,605 request
bytes and received738,866 response bytes. Client timing covers JSON
encode, HTTP exchange and JSON decode. Server peak RSS was
2,059,653,120 B. Empty-cache startup fetched exactly five
authenticated blobs totaling1,880,914,634 B in37.102 s; query
vector-body GETs were zero. The first pass meets p95≤110 ms,
p99≤130 ms and QPS≥85; the repeat pass also passes. V262 is a
same-artifact historical baseline on another source revision, with
the same instance class/region/concurrency/transport. It is not an
interleaved same-host control.

The successful attempt `a0002` used source commit
`d347cd5320fb17fd28514258bff5a56783f8263a`, archive SHA-256
`59eb665bc4b50f2c30779859143d7ba5093baeed9573ab8162c357b113c09785`.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v269-cohere-cached-dual-http-1m/d347cd5320fb17fd28514258bff5a56783f8263a/runs/a0002/`.
Server `i-0e39231e4d4eb82ad` and client
`i-098e7dc3d9f5b1436` both wrote complete terminals and are
terminated. Server terminal SHA-256
`c531f5bbcccc74c7152c381268121bb2ba6ff1bc72c68bfd6727989accf9522a`,
client terminal SHA-256
`555b728d77637d80fcd1b2df7792bbb7f30364218d84350d40b5232d0db80907`,
closeout SHA-256
`9410325d8943040189d0f6c1d46e4121f15a9d2072aba61f8d7c00b06c862a8b`.
Independent readback verified every terminal artifact size/SHA-256.
First raw SHA-256
`efa9cd75ab2fe8fe0477159377771ba0b371df810edc36db4e54b1666f05b1c6`,
repeat raw SHA-256
`9b9899d74c58bfaaa7f13ea2aeb39bb658c71d0ec99991106b06300f066470c7`.
Two-host Spot compute through terminal was approximately $0.05935 at
the launch-time $0.3696/host-hour quote, excluding EBS, S3, data
transfer, index build and cleanup tail; this is not a full service
cost or bill. Attempt `a0001` stopped before server readiness on a
Rust compile error, produced no client measurement, and terminated its
only instance. Its sealed failure is recorded in the preregistration.

The direct S3 Vectors V263 run used the same CoHere corpus/panel and
GT100 truth: first88,803 hits with p50/p90/p95/p99
67.011/164.977/187.630/254.842 ms at82.99 QPS; repeat88,864 hits
with61.332/65.317/66.394/80.845 ms at128.79 QPS. V269 has
higher recall and throughput on both passes; its repeat p95 is4.161
ms above V263. BORSUK ran from resident memory after a disclosed
37.102 s cold hydration over private HTTP; S3 Vectors used regional
HTTPS with opaque managed cache. These are direct same-data product
measurements with different cache and transport semantics, so neither
row establishes a strict matched vendor win. There is no authenticated
matched Turbopuffer measurement.

Next gate: freeze this library/format revision for a 10M resource and
quality falsifier with a memory envelope derived from dimensions,
graph degree and worker count. Do not impose an arbitrary corpus-size
knee or present projected latency/recall as a measurement. A vendor
claim additionally needs a disclosed equivalent cold/no-cache query
protocol and direct Turbopuffer access.
