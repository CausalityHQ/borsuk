# V278 one-shot 1M bounded extra reverse-edge gate

Status: preregistered before candidate build or measurement. V277 fresh100k
terminal SHA-256 `cc3f97c3e4bfe71b0d3635dc5127a07f8b515bcac532bdff38f6414cc2b96020`
passed its frozen gate. This promotes the unchanged V276 construction method to
one 1M gate, not to production by default.

## Frozen data, method, and host

CoHere-large-10M canonical train rows0–999,999, D768 cosine, k100. Baseline
is authenticated V271 first1M root
`c3a60f9969f8bc0fc6cf2f24831c45d3918bd7090a474c090b5812629851f86a`;
V271 terminal SHA-256
`13775dc6c5176c636fc8442b4bd2bddd2ee375818a98d1328d2c0352423bc80b`.
Source V261 raw first1M SHA-256
`6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005`.
Candidate builds with the same M32/M0=64, efConstruction128, worker cap8,
PQ/FP16 and bounded extra reverse edges as V276: each original source row of
out-degree<96 may gain at most16 edges, targeting rows of in-degree<16. No
baseline edge is displaced. One build, no cap or parameter sweep.

Queries are canonical train rows1,001,000–1,001,999, excluded from the index
and disjoint from V271's prior-used rows1,000,000–1,000,999. These are rows
17,975–18,974 of authenticated shard `train-00000045.parquet`, SHA-256
`1fae833dd9cbdb775b177176a2d301f0ee887988575b1152c13d7d0517cd25f4`;
the source roster SHA-256 is
`0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87`.
Normalize queries to FP32 unit vectors and compute exact GT100 with FAISS
IndexFlatIP over unit-normalized FP32 source. Record derived query and truth
SHA-256 before search in the terminal artifacts.

One eu-central-1 c7i.4xlarge `causality` Spot host, two-hour hard stop, one
terminal attempt. Search each authenticated arm in a separate process after
one warmup on the same 1,000 fresh queries, sequentially at default
PQef4096/shortlist4096/exactef2048 and diagnostic PQef256/shortlist256/exactef128.
Record raw IDs, visits, latency and same-sample p50/p90/p95/p99, R@100, p05
hits, graph structure/bytes, build/search RSS and time, instance/quote/cost,
all source/generation identities, query-time GETs and transport. Both arms are
local resident Rust; query-time GETs must be zero. Upload terminal artifacts
and replay SHA-256/size before terminating compute; discard interrupted cells.

## Frozen decision

Primary default-width gate is inconclusive if baseline has fewer than100
GT100 misses. Otherwise candidate must reduce total misses by ≥20%, have
R@100≥0.9975 and p05 hits no lower, while paired p95/p99 are each≤120% of
baseline and search RSS≤115% baseline. Diagnostic total misses must not rise;
if baseline default-width low-in-degree≤8 GT misses are at least50, candidate must reduce
those by ≥30%. Mean visits at both widths≤120% baseline. All baseline graph
edges must remain; each source gains≤16, sources originally at degree≥96
gain zero, candidate max out-degree≤max(baseline max,96), graph bytes≤115%
baseline, min in-degree≥4, full reachability, and authenticated open/search.
Build time/RSS≤150% of V271's same-class 2,064.570 s / 5,547,312 KiB. A pass
permits a fresh cross-dataset 1M check before any 10M rebuild. This in-process
gate is not HTTP product latency or a vendor comparison.
