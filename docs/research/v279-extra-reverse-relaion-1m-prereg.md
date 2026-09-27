# V279 paired ReLAION-1M transfer gate

Status: preregistered before building either arm. V278 fresh CoHere-1M
terminal SHA-256 `cd5ddfdbc321478361e428ef702e573a14532e7c58cbe265649a8d38de76008e`
passed its frozen gate. V279 tests the unchanged bounded extra reverse-edge
method on a different dataset; there is no parameter sweep.

## Frozen dataset and method

ReLAION-1M, D768 cosine, k100. Source Parquet SHA-256
`2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86`,
1,458,450,077 bytes, and 1,000 validation request rows SHA-256
`c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9`.
The **validation ordinals0–999 are historically used**, but neither this
source format nor their labels selected the V276 method or its bounds. Read
source rows in authenticated Parquet order and assign contiguous internal
IDs0–999,999. Both arms use those same IDs and raw FP32 vectors; exact GT100
is recomputed by FAISS IndexFlatIP on FP32 unit-normalized source and query
vectors. This cosine truth is separate from older squared-L2 GT receipts.
Record derived raw/query/truth SHA-256 before search.

Baseline and candidate are built under the same Rust revision on one
eu-central-1 c7i.4xlarge `causality` Spot host. Both use M32/M0=64,
efConstruction128, worker cap8, same PQ/FP16 and authenticated generation
format. The candidate adds V276's fixed reverse edges: target rows with
in-degree<16, at most16 extra edges per original source row below out-degree96,
preserving all baseline edges. Build each arm once. A three-hour hard stop
covers two 1M builds plus artifact upload; an interruption invalidates the
entire paired cell. Upload terminal artifacts, replay all hashes and sizes,
and terminate the instance immediately.

Search each authenticated generation in a separate process after one warmup,
same 1,000 validation queries sequentially at default
PQef4096/shortlist4096/exactef2048 and diagnostic PQef256/shortlist256/exactef128.
Record raw IDs, visits, latency, same-sample p50/p90/p95/p99, R@100, p05
hits, graph degrees/bytes/reachability, build/search RSS and time, quote/cost,
cache and transport. Both arms are local resident Rust, zero query-time GET.
These are not HTTP service or vendor timings.

## Frozen decision

The default-width candidate must reach R@100≥0.995, lose no GT100 hits or
p05 hits against the paired baseline, and reduce total baseline misses by
≥20% when baseline has at least100 misses. If baseline has fewer than100
misses, there is no material quality need for extra edges: retain baseline
unless candidate is no slower at p95/p99 with no quality loss. For promotion,
default p95/p99 and search RSS must be≤120%/≤120%/≤115% of baseline;
diagnostic misses may not rise and mean visits at both widths≤120% baseline.
All baseline edges must remain; at most16 edges added per source, none when
original degree≥96, max candidate out-degree≤max(baseline max,96), graph
bytes≤115% baseline, minimum in-degree≥4, full reachability and authenticated
open/search. Candidate build time/RSS≤150% paired baseline. A pass permits
one same-revision HTTP serving/resource gate; it does not establish unbiased
fresh-query or vendor superiority. A fail keeps the V271 baseline format.
