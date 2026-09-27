# V276 bounded extra reverse edges: terminal closeout

Frozen source `65684cc922a9a81e654ffb58722691c0f7b2a11b`; source archive
SHA-256 `3a5d6f3638f1fa744a89a76575651f644a7c70c95512a4d90faad24153da3bb9`.
Attempt `a0001` completed on eu-central-1 c7i.4xlarge Spot
`i-0ca4a9052e90aea3c` and was terminated. Terminal SHA-256
`6f83e1877ed0e5eaf44ba5514c7588df6b924476f035cea5a77feffc0d8148a1`;
all 30 artifact hashes and lengths replayed. Closeout SHA-256
`bf7bc08a11285c26dd37a61f6141af6c9207e4172eed7de5c67315f6d66888c0`.
Spot quote $0.3729/hour, estimated compute through terminal $0.06412.

CoHere-large-10M canonical train first 100,000, D768 cosine, k100; 1,000
fresh excluded train rows 103,000–103,999, query SHA-256
`81d455e2b251f6244e5e6092b0880fc7014e5cb88c93c29d8f57685f2d51056b`.
FAISS exact FP32 unit-normalized GT100 SHA-256
`f97e2e861641ff7dd55bd9ccd8ec6220d4b585937accdeaaf53c1ff7174f493e`.
Each arm is authenticated, local resident, sequential, in-process Rust; no
query-time GET. These are not HTTP or vendor measurements.

| Width | Measure | V271 baseline | V276 candidate |
| --- | --- | ---: | ---: |
| Diagnostic PQef256/shortlist256/exactef128 | GT100 hits / 100,000 | 98,421 | 98,711 |
| Diagnostic | baseline low-in-degree GT misses | 303 | 99 |
| Diagnostic | p05 hits/query | 93 | 95 |
| Diagnostic | mean visits/query | 7,824.437 | 9,303.652 |
| Diagnostic | p50/p90/p95/p99 ms | 4.238/4.975/5.175/5.525 | 4.794/5.618/5.873/6.209 |
| Diagnostic | search RSS KiB | 204,996 | 206,000 |
| Default PQef4096/shortlist4096/exactef2048 | GT100 hits / 100,000 | 99,929 | 99,948 |
| Default | p50/p90/p95/p99 ms | 32.225/34.673/35.406/36.215 | 36.517/39.219/39.742/40.909 |

Candidate root SHA-256 `56de9f4768271611683193f4fa5d36795cb5926adabb89efb7bd4c851b1e340b`.
All baseline edges remained. Graph bytes 26,783,198→27,704,122 (+3.44%);
fraction of rows with in-degree ≤8 19.592%→0.356%; minimum in-degree 4;
all 100,000 reachable. Candidate build 241.286 s, peak RSS 847,204 KiB.

**Frozen decision: reject.** The preregistered absolute maximum out-degree
≤96 fails: candidate maximum is 118. The authenticated V271 baseline graph
already has maximum 118, with five rows above 96. Preserving every baseline
edge and imposing absolute maximum 96 were mutually incompatible. No added
edge raised the observed maximum: authenticated graph comparison found 230,231
added edges across 42,588 source rows, at most 16 per source, and zero source
rows with baseline degree ≥96 gained an edge. The quality, latency, memory, and build
gates passed. This is a gate specification error, so V276 cannot authorize
promotion. Validate the same bounded-addition method once on a new query
panel under a corrected, frozen structural rule before a 1M gate.
