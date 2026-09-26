# V266 CoHere 100k: SQ8 navigation does not reduce whole-query cost

**Decision: reject the block-scaled SQ8 navigation representation and do
not promote it to 1M.** It preserved V265's 99,948/100,000 exact GT100
hits and all1000/1000 ordered k100 lists, but loaded p95 increased
from29.318 to29.775 ms; throughput fell from296.74 to292.77 QPS.
It missed the frozen p95≤26.8 ms and throughput≥330 QPS gates. Peak
serving RSS rose by85.3 MB to301,162,496 B. The result points to
graph work and other whole-query costs as the next design lever; it
does not show that SQ8 is inherently slow on other corpora or hardware.

One `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-01fd50ae0c3d72018` in `eu-central-1c`, now terminated. Source
commit `9da6ef74a6f28f57578e7e8438ff930b9a690266`, archive SHA-256
`4db355f053b0479cb8d511b69def9a08417c2151667f746dcdda9810725f2940`.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v266-cohere-sq8-dual-graph-100k/9da6ef74a6f28f57578e7e8438ff930b9a690266/runs/a0001/`.
Terminal status `complete`, exit0, SHA-256
`3c6ab104a8e3cce530e69e66416050ff52eafd458c38bd96b83cf5530e3b9a03`.
The original launcher replayed every artifact size/SHA-256 and confirmed
termination. Remote release tests passed for graph search, SQ8 SIMD/tail
scoring and FP16-underflow rejection. Source, graph, plane, PQ books/
codes, map, requests and exact truth match V265 byte for byte.

Dataset and split: CoHere-large-10M canonical first100,000 source rows,
D768 cosine k100, authenticated prior-used test ordinals0–999;
development0–255 and validation256–999. The PQ graph ef4096/
shortlist4096, exact-branch graph ef2048, exact-beam FP16 rerank and
final two-list union were fixed. V266 alone used per-32-coordinate
signed SQ8 navigation codes derived from the authenticated FP16 plane.
V265 is an immutable prior-revision baseline under matched artifacts
and hardware, not a same-revision paired repetition.

| Loaded eight-worker in-process | Dev GT100 / 25,600; p05 | Val GT100 / 74,400; p05 | Combined / 100,000 | p50/p90/p95/p99, ms | QPS | Peak RSS, B |
|---|---:|---:|---:|---:|---:|---:|
| V265 cached-norm FP16 navigation | 25,583; 99 | 74,365; 100 | 99,948 | 26.889 / 28.795 / 29.318 / 30.298 | 296.74 | 215,863,296 |
| V266 SQ8 navigation | 25,583; 99 | 74,365; 100 | 99,948 | 27.420 / 29.198 / 29.775 / 30.653 | 292.77 | 301,162,496 |

Independent replay of sealed raw IDs and loaded timing reproduced all
four percentiles and1000/1000 ordered ID parity. V266 sequential
p50/p90/p95/p99 was29.520/31.659/32.119/33.148 ms. Its p95 PQ
graph scores33,637 match V265; SQ8 graph scores21,267 versus V265
FP16 graph21,236. Distinct final union p95 was102 in both. The
retained exact-beam FP64 rerank is additional work outside the graph
visit counts. Derived SQ8 cache was86.4 MB: 76.8 MB codes plus9.6 MB
scales. Cold hydration was1.168 s, zero query vector-body GETs and
zero remote swap. Graph rebuild took173.33 s, peak523,560 KiB.

The instance launched18:28:36 UTC and terminal landed18:40:20 UTC,
elapsed704 s. At the previously recorded $0.3663/hour Spot quote,
compute through terminal was approximately $0.0716, excluding EBS,
S3 and cleanup tail; this is a quote-based estimate, not a bill.
Sealed quality SHA-256
`2b92e57241aac11c34c6b0f3423c36efd776aa411778e93d67fcbfcd5abfc253`,
raw IDs `c9205c8ddf5a77b1bc9c74299ebede297735218a9de72f656d25c318624077d1`,
loaded timing `f64fcd055787937de7e413298ed06af54e77cdc413413a4bb5be41d354df9c91`,
serving summary `5fc48f7397ad05ffc53cc8bf0bcc413cb9b675628ff6a0226408d373329be70c`.

Next single gate: eliminate duplicated exact FP16 scores across the PQ
shortlist and exact graph traversal, using a per-query physical-row
score cache on the same authenticated graph. Keep both routes, their
fixed beams and final ranking. Measure cache hits, exact IDs, recall,
loaded latency, throughput, RSS and cost on the same 100k panel before
any 1M cell. V262 HTTP and V263 S3 Vectors first1M results remain
historical measurements on their own source revisions and cache/
transport conditions.
