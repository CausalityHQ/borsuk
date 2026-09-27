# V277 bounded extra reverse edges: fresh-query closeout

Frozen source `1341344b6a620e9419ee58d6f57d7a115402f02a`; source archive
SHA-256 `b9e9606ee6c7ce7dc667503e67376d3f593ad88385089e6b74f6bc8794c1c9d5`.
Attempt `a0001` completed on eu-central-1 c7i.4xlarge Spot
`i-098c51032d9c6eeea` and was terminated. Terminal SHA-256
`cc3f97c3e4bfe71b0d3635dc5127a07f8b515bcac532bdff38f6414cc2b96020`;
all artifacts replayed. Closeout SHA-256
`c12d10d24ed4c4e4db7ac05312c03191bed96e8671a3ec36ffeb574336317116`.
Spot quote $0.3729/hour, estimated compute through terminal $0.03294.

CoHere-large-10M canonical train first100k, D768 cosine, k100; 1,000 fresh
excluded train rows104,000–104,999, query SHA-256
`0099cdcd57a80d33437a63a3cd9e9fab4bd333ba27ad4d1cedbc2d32e2a932cb`.
FAISS exact FP32 GT100 SHA-256
`1270558d42a3fb69f76b52eaeb44851ed432db0c473d8c390ebdff648b8f77a9`.
Each arm is authenticated, locally resident, sequential, in-process Rust;
zero query-time GET. This is not HTTP service or vendor evidence.

| Width | Measure | V271 baseline | V276 graph |
| --- | --- | ---: | ---: |
| Diagnostic PQef256/shortlist256/exactef128 | GT100 hits / 100,000 | 98,524 | 98,826 |
| Diagnostic | baseline low-in-degree GT misses | 311 | 91 |
| Diagnostic | p05 hits/query | 95 | 96 |
| Diagnostic | mean visits/query | 7,838.426 | 9,324.727 |
| Diagnostic | p50/p90/p95/p99 ms | 2.787/3.218/3.357/3.584 | 3.257/3.739/3.898/4.150 |
| Diagnostic | search RSS KiB | 204,736 | 205,556 |
| Default PQef4096/shortlist4096/exactef2048 | GT100 hits / 100,000 | 99,939 | 99,959 |
| Default | p50/p90/p95/p99 ms | 23.676/25.819/26.403/27.673 | 26.860/29.199/29.881/30.718 |

**Frozen decision: go to one fresh 1M gate.** The original baseline graph is
a subgraph of the candidate. The candidate has 230,231 added edges, at most
16 per source and none on sources originally at degree≥96. Both maximum
out-degrees are 118, minimum candidate in-degree4, all100,000 rows reachable.
Graph bytes 26,783,198→27,704,122 (+3.44%); all-row share with in-degree≤8
19.592%→0.356%. V276's authenticated build remains 241.286 s / 847,204 KiB.
All frozen V277 gates passed. Promotion requires a new 1M build and matched
fresh-query quality, latency, memory, and build/resource evidence; V277 alone
does not establish a product or vendor win.
