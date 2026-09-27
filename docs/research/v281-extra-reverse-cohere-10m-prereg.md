# V281 bounded reverse-edge CoHere10M transfer gate

Status: preregistered before any V281 source archive or Spot reservation.
This is one build of the already fixed V276 bounded reverse-edge method,
followed by authenticated library search. V276/V277 100k and V278 fresh 1M
qualified its CoHere quality effect; V279 rejected it as a generic ReLAION
default. No parameter or recall-target sweep is allowed.

## Frozen source and comparison

CoHere-large-10M all canonical train rows, D768 cosine, k100, 458 sealed
Parquet shards. Staging receipt SHA-256
`0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87`;
canonical test Parquet SHA-256
`5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e`.
The V272 prepared FP32 source SHA-256 was
`2e33abfc666e652455a815b90d88a13b617f6dbb431538d6324ad06eccf06f1f`.
Re-materialize in that authenticated shard order and reject a different
source hash. Use the same prior-used test ordinals0–999, with development
0–255 and validation256–999 reported separately. Normalized FP32 queries
SHA-256 `4394cb0f28238fe713182094dbb90e2dbc52db634f48c1419997e86ad085ddd4`;
V272 exact FAISS FP32 cosine GT100 SHA-256
`9d08b49fef274d5bee2572ed8ed186ff8f2063b759f556748fe83b6e1d21c4f1`.
The completed V272 terminal SHA-256 is
`e75a7b399920b277683406ee370a67204f3e9f01fd215e809c0f606356958352`.

V272 production baseline root SHA-256
`948e8a5555f44011b22681ca2cd20edde93f6fec5e6261e20e3a7b799d467022`;
graph2,678,381,310 bytes and all five blobs18,799,167,806 bytes. V281
must authenticate and search this pinned baseline generation on the same
Spot host and source revision as the candidate search. It need not rebuild
that already sealed graph. The V272 historical quality was99,299/100,000
GT hits, development25,421/25,600 and validation73,878/74,400. Compare
quality against both that immutable receipt and the same-host baseline raw
search; require all1,000 baseline ID lists to match V272 or invalidate the
cell before comparing arms. Exact truth is unchanged.

Build one candidate through the public Rust
`build_graph_generation_reverse_extra` route from the re-materialized source,
with M32/M0=64, efConstruction128, worker cap8, low-in-degree target<16,
at most16 added edges per source and no addition to original source degree
≥96. The generation layout remains the same five authenticated blobs and
root. Publish to a fresh S3 prefix, cold reopen/hydrate in a separate
process, then run the same1,000 queries sequentially after one warmup,
k100, PQef4096/shortlist4096/exactef2048. Baseline and candidate searches
run on the same eu-central-1c r7i.8xlarge Spot host with one worker, no
response cache and zero query-time vector GETs. Record raw IDs, visits and
latencies and same-sample p50/p90/p95/p99, splits and p05 hits, build and
search RSS/time, graph/blobs bytes, hydration GETs/bytes/time, source GETs,
quote and estimated compute/storage cost. In-process timings are not HTTP
or vendor service timings.

## Frozen decision and lifecycle

The quality option passes this transfer only if both splits reach
recall@100≥0.995 and p05 hits≥98, and combined misses fall by at least20%
against the same-host baseline. This is high recall, not a 100% target.
Candidate p95/p99 must be≤150/180 ms and≤120% of same-host baseline;
search RSS≤32 GiB, build peak RSS≤8,000 bytes per row (80 GB at10M),
build time≤150% of V272's24,796-second baseline build, graph bytes≤115%
of V272 baseline, and candidate root must retain the same source, plane,
map, PQ book and code hashes as the baseline. The builder's reachability
and minimum-in-degree≥4 checks must pass. Five authenticated hydration
GETs and zero query-time vector GETs are required. A pass permits exactly
one HTTP serving/resource gate of this quality option; it does not promote
the option as a generic default or establish a vendor win. A fail requires
a material quality-layer decision before further paid10M work.

One `causality` r7i.8xlarge Spot host, encrypted250 GiB gp3, 12-hour hard
stop. Reserve a unique attempt only after code and focused tests pass. A
Spot interruption invalidates this full cell; restart only under a new
attempt ID. Never inspect incomplete measurement artifacts. Sync terminal
artifacts to S3, replay every size/SHA-256 and published root, then
terminate the instance immediately. Record the instance and quote; compute
cost from quote×time and label it an estimate, not a bill.
