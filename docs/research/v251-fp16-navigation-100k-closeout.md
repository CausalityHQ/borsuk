# V251 exact FP16 navigation: quality passed; scale work failed

Source commit `0168b667c32907ce2dd62e3648d03f220bc98479`;
one `causality` c7i.4xlarge Spot attempt `a0001`, instance
`i-0088230f7a1da759d`, terminated after the complete terminal.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v251-cohere-fp16-navigation-100k/0168b667c32907ce2dd62e3648d03f220bc98479/runs/a0001/`.
Source archive SHA-256
`80b594ca3c5f63e357df6cb8b0ea4273bb74d02831cf4a9a7ef54ae6ffa64ee0`;
terminal SHA-256
`a92bd99b3c5739819894bed011ad9b766914476892c64b2137987f1f9f940d65`.
The launcher replayed every terminal artifact's size and SHA-256. The
single remote graph-workspace unit test passed 1/1 before measurement.

CoHere-large-10M first100k D768 cosine, query k100, development ordinals
0–255 and validation 256–999; both splits were used previously. V248's
authenticated source F32, FP16 plane, PQ books/codes, map, query panel and
exact GT100 truth are byte-identical. The rebuilt diverse graph SHA-256
`688941c7c61c89a39739909af14cb2b4a935a7a4967a168f9503ac34e170d0e7`
is identical to V250. Sealed V251 raw IDs SHA-256
`ab5448c45517fdad6d162f6fac8b35e5f67ac81c5284c90b6b9f6a8e79a90b87`.
Independent replay of all three arms against the authenticated truth
reproduced every recorded hit total.

| Exact FP16 ef | Development hits / 25,600; p05 | Validation hits / 74,400; p05 | Combined hits / 100,000 | Loaded p50/p90/p95/p99 ms | Loaded QPS | Base scores p95 |
|---|---:|---:|---:|---:|---:|---:|
| 512 | 25,369; 96 | 73,851; 97 | 99,220 | 13.892 / 16.556 / 17.136 / 18.173 | 558.6 | 9,558 |
| 1024 | 25,495; 98 | 74,154; 98 | **99,649** | 22.023 / 25.452 / 26.274 / 27.825 | 353.8 | **14,508** |
| 2048 | 25,559; 99 | 74,286; 99 | 99,845 | 34.099 / 37.903 / 38.803 / 40.798 | 232.0 | 21,236 |

The smallest development-passing arm is ef1024; validation also passes:
mean R@100=0.995898 on development and 0.996694 on validation, p05=98
on both. It passes loaded p95≤35 ms, serving peak RSS≤256 MiB and zero
vector-body GETs. Peak serving RSS was 213,753,856 B; cold hydration
322.375 ms. Rebuilding the identical graph took 171.741 s, peak
535,437,312 B; source/PQ preparation took 46.50 s, peak 1,773,572 KiB.
The graph reached all100,000 rows and had minimum in-degree four.

**Scale promotion failed:** ef1024 p95 scored 14,508 base rows, above the
frozen 10,000-row limit. Ef512 stayed under that limit at 9,558 but failed
quality. Relative to V250's same-graph PQ navigation ef4096, V251 reduced
p95 base scores from 33,637 to 14,508, while combined hits changed from
99,689 to 99,649 and loaded p95 rose from 13.421 to 26.274 ms. The
quality/work crossing has no passing tested arm. These are loaded
**in-process** timings, not network service latency or matched vendor data.

**Decision:** keep 10M closed. Exact FP16 scores repair much of the
navigation waste but do not reach the work bound. The next cheapest
falsifier is a material coarse-entry routing change on this same 100k graph:
deterministic source-only anchors choose initial graph entries by cosine,
then the graph search uses a smaller frozen beam. It must beat V251 ef512
on quality without exceeding 10,000 p95 base scores and must pass the
unchanged dev/validation quality and loaded resource gates. No
dataset-specific query-derived links or threshold tuning are allowed.
