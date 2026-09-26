# V253 global PQ candidates: 100k quality and resource gate passed

Source commit `bd14239fbc22156fc7fe53bfbe0e9a7bdcb6879a`;
one `causality` c7i.4xlarge Spot attempt `a0001`, instance
`i-0e823e3171b1339ed`, terminated after the complete terminal.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v253-cohere-global-pq-100k/bd14239fbc22156fc7fe53bfbe0e9a7bdcb6879a/runs/a0001/`.
Source archive SHA-256
`5a54c05df0b3ba52e57d5ffb71f13bc1c1ad85d8c333350ae23de3e494469101`;
terminal SHA-256
`eaed2fd2640846cc7326616030eec0843314a3b0b926f01d83c54a76492b7d2d`.
The launcher replayed every terminal artifact's size and SHA-256. The
narrow remote PQ cosine unit test passed 1/1 before measurement.

CoHere-large-10M first100k D768 cosine, k100, development ordinals
0–255 and validation 256–999, both previously used. The source F32,
FP16 plane, PQ books/codes, map, query panel and exact GT100 truth match
V248–V252 byte for byte. The graph was rebuilt only as a paired-source
guard and had the identical V250 SHA-256
`688941c7c61c89a39739909af14cb2b4a935a7a4967a168f9503ac34e170d0e7`.
V253 sealed raw IDs SHA-256
`af9ce19afc4aef35b970f72148597de84fe458ac9b3c4ccf133676f6a0270ccc`.
Independent replay of all 1,000 sealed ID sets against the authenticated
truth reproduced the quality summary.

| Method | Development hits / 25,600; p05 | Validation hits / 74,400; p05 | Combined hits / 100,000 | Loaded p50/p90/p95/p99 ms | Loaded QPS | Work/query |
|---|---:|---:|---:|---:|---:|---:|
| V251 exact graph ef1024 | 25,495; 98 | 74,154; 98 | 99,649 | 22.023 / 25.452 / 26.274 / 27.825 | 353.8 | 14,508 p95 base rows |
| V253 global PQ top8192 + FP16 | **25,575; 99** | **74,347; 99** | **99,922** | **18.560 / 18.946 / 19.157 / 19.505** | **418.9** | 100,000 PQ + 8,192 FP16 rows |

V253 development mean R@100=0.999023 and validation=0.999288;
both pass the frozen ≥0.995/p05≥98 gate. Loaded p95≤35 ms,
FP16 rows≤10,000, serving peak RSS≤256 MiB and zero vector-body GETs
also pass. Peak serving RSS was 230,133,760 B; cold hydration
332.579 ms. The identical graph rebuild took 170.688 s and peak
535,052,288 B; source/PQ preparation 49.70 s and peak 1,798,944 KiB.
Loaded timings are in-process, not network service or vendor latency.

**Scale limitation:** every query scores all 100,000 PQ codes. That O(N)
scan is included in the 19.157 ms loaded p95 at 100k and cannot be
extrapolated as a 1M/10M/100M result. The next production architecture
decision is to partition PQ codes by source-derived coarse routing while
retaining the high-quality global shortlist. Prove candidate completeness,
bounded PQ work, loaded latency, memory and exact recall on this same
100k panel before a frozen 1M end-to-end service gate. Keep 10M closed
until the bounded route and 1M gate pass.
