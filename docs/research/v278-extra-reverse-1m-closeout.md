# V278 bounded extra reverse edges: fresh CoHere-1M closeout

Frozen source `42c48d5e8e6696d9c991faec0d9b43f00dc1b61e`; source archive
SHA-256 `7ae069010410dc466d2c45738ef066da063a587a474ed9550453c521ac2b1600`.
Attempt `a0001` completed on eu-central-1 c7i.4xlarge Spot
`i-01399f48e170f65ee` and was terminated. Terminal SHA-256
`cd5ddfdbc321478361e428ef702e573a14532e7c58cbe265649a8d38de76008e`;
all 32 artifact hashes and lengths replayed. Closeout SHA-256
`fdfe6a53ea7172e62865d840d11663c0c4904cece84073499b851fa19cba6500`.
Spot quote $0.3729/hour; estimated compute through terminal $0.29221.

CoHere-large-10M canonical train first1M, D768 cosine, k100; 1,000 fresh
excluded train rows1,001,000–1,001,999. FP32 unit query SHA-256
`6e3505fdfc9d6a6c101cba2b7d4c16d5c07eee6ec704fa836385a810e8ca17d4`;
FAISS exact FP32 GT100 SHA-256
`7e24e5a8263b9461eb402f0dabdaef7eb4724c623b5632cdc9f7a7d1c7c65e2d`.
Both arms are authenticated, local resident, sequential, in-process Rust,
zero query-time GET. These are not HTTP service or vendor measurements.

| Width | Measure | V271 baseline | V278 candidate |
| --- | --- | ---: | ---: |
| Default PQef4096/shortlist4096/exactef2048 | GT100 hits / 100,000 | 99,724 | 99,867 |
| Default | R@100 | 0.99724 | 0.99867 |
| Default | p05 hits/query | 99 | 99 |
| Default | baseline low-in-degree GT misses | 176 | 43 |
| Default | mean visits/query | 86,809.475 | 103,316.044 |
| Default | p50/p90/p95/p99 ms | 56.823/67.101/69.822/75.093 | 64.104/76.236/79.258/84.018 |
| Default | search RSS KiB | 1,948,044 | 1,956,808 |
| Diagnostic PQef256/shortlist256/exactef128 | GT100 hits / 100,000 | 96,142 | 96,679 |
| Diagnostic | mean visits/query | 10,468.674 | 12,239.310 |
| Diagnostic | p50/p90/p95/p99 ms | 5.566/6.974/7.334/7.817 | 7.204/9.090/9.568/10.308 |

Candidate root SHA-256 `23e4ad52a3b13b548e640eaf9a7c753098772806a058cb6fb305671be407b495`.
All baseline graph edges remained; 2,141,334 edges were added, at most16
per source and none on sources originally at degree≥96. Both maximum
out-degrees are256; all1M rows reachable. Graph bytes
268,128,138→276,693,474 (+3.19%). Candidate build 2,321.036 s, peak RSS
5,549,664 KiB. Default p95 rose13.5% and p99 rose11.9%; diagnostic p95
rose30.5%, so this is a quality/latency tradeoff, not a universal speedup.

**Frozen decision: go to a fresh cross-dataset 1M gate.** Default misses fell
276→133 (51.8% fewer), R@100 exceeded0.9975, and the preregistered default
latency, RSS, graph, build and structural bounds passed. ReLAION-1M must be
built as paired baseline and candidate under one revision; CoHere cannot
establish generic promotion or a vendor win.

The original launcher successfully replayed every artifact but then rejected
the valid `go_cross_dataset_1m` label because its local accepted-label set
still named `go_1m`. This happened after terminal completion and Spot
termination. The label check was repaired locally, and closeout was written
and read back for the original attempt without starting another instance.
