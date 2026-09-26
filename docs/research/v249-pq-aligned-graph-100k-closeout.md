# V249 PQ-aligned topology: quality gate failed

Source commit `6b7884a6a24e6467d82c9ced1a00ca3706cf0aca`; one
`causality` c7i.4xlarge Spot attempt `a0001`, instance
`i-01fb162b4c9c5f061`, terminated after its complete terminal.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v249-cohere-pq-aligned-100k/6b7884a6a24e6467d82c9ced1a00ca3706cf0aca/runs/a0001/`.
Source archive SHA-256
`c9a4aef5ece30f68f2fbfed84d60b8d1d7a0345983a184828abbacece111b81e`;
terminal SHA-256
`1d51817ca1556c011fad29bc0476a9b36103fb60e5b9d6a632766d48fa7bc280`.
Every terminal artifact passed independent length/SHA-256 readback.
The worker checked that source, FP16 plane, PQ books/codes, map, 1,000
test queries and exact 100k GT100 truth matched V248 byte for byte.
The new graph SHA-256 was
`0d7cb4589315bd335e9303bfe98a8501aa642d91d58191b26a0ce49a691a89f4`;
sealed returned IDs SHA-256
`5f85f88fb57aebf990db33d062533b89330288c406bfaabc2f155037552b9ac2`.
These are CoHere-large-10M's first 100k D768 cosine source rows,
query k=100, development ordinals 0–255 and validation 256–999;
both splits were previously used in repository publication work.

| ef/shortlist | V248 → V249 development GT100 hits / 25,600; V249 p05 | V248 → V249 validation GT100 hits / 74,400; V249 p05 | V248 → V249 combined GT100 hits / 100,000 | V249 loaded p50/p90/p95/p99 ms | V249 loaded QPS |
|---|---:|---:|---:|---:|---:|
| 2048/2048 | 24,927 → 24,848; 91 | 72,669 → 72,487; 92 | 97,596 → 97,335 | 7.258 / 7.827 / 8.002 / 8.278 | 1088.5 |
| 4096/4096 | 25,303 → 25,292; 95 | 73,598 → 73,584; 96 | 98,901 → 98,876 | 13.151 / 13.824 / 13.996 / 14.314 | 606.5 |
| 8192/8192 | 25,401 → 25,401; 96 | 73,847 → 73,853; 97 | 99,248 → 99,254 | 24.330 / 25.182 / 25.367 / 25.606 | 327.9 |

Independent replay of both sealed raw ID files against the same saved
truth reproduced every hit count. At 8192/8192, V249 won 32 queries,
tied 940 and lost 28 against V248. Neither arm reaches the frozen
mean R@100≥0.995 and p05≥98 on both splits; the best arm's mean is
0.99254, development p05 96, validation p05 97. V249 quality gate
failed. PQ-aligned topology's +6 combined hits is negligible and does
not justify promotion. Its graph remained fully reachable with minimum
in-degree four and maximum degree 256. Build time was 30.055 s and
peak RSS 543,260,672 B (V248 23.912 s / 533,286,912 B). V249 serving
peak RSS was 217,251,840 B, zero vector-body GETs; source/PQ
preparation took 49.39 s and 1,858,539,520 B peak RSS. These loaded
timings are in-process, eight-worker measurements from different but
same-class Spot instances; the observed p95/QPS difference is not a
causal service-latency result or a vendor comparison.

The V248 100-query diagnostic showed that exact FP16 and exhaustive
PQ top8192 candidates had almost all truth IDs. V249 shows that
aligning graph link geometry with PQ navigation did not remove the
failure. The specific metric-mismatch hypothesis is rejected. The
remaining evidence points to incomplete graph exploration or the
single-entry search path under bounded visits; the exact mechanism is
not yet proven. **Decision:** keep the 10M gate closed, reject this
topology change, and evaluate one materially different, source-only
navigation or coarse-routing method at 100k before any 10M run. A
larger ef sweep is not an acceptable substitute.
