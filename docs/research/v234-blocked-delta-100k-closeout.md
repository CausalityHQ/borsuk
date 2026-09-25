# V234 exact blocked mutation delta, ReLAION-100k

**Decision: reject the exact blocked full scan as the next 1M candidate.**
It preserved every result but cut loaded p95 only 3.0% and raised
throughput only 3.8% on c7i, far short of the frozen 50% p95 and 1.5×
throughput gates. The eight independent FP64 accumulators alone did
not remove enough loaded query work: every query still exact-scores all
10,000 mutation rows. The earlier synthetic ARM loop result was a
useful hypothesis, not a transferable product measurement. Do not run
another 1M cell for this layout alone. The next material candidate
must skip most expensive FP64 scores through a conservative two-pass
screen, or change the mutation/compaction tier.

The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-0baf9ce5dd3ea25e5` in `eu-central-1c`, independently confirmed
**terminated**. Source commit
`6e9059256d903080c293be231f6e78c3f53092ba`, source archive
SHA-256 `2260efc63c89a5d00c3f161ab4e3fc985ee76cc7e0324c2730da427c0624857c`,
original terminal SHA-256
`5d754850070bcd096212b9b42250b51dc9a49248017b7a9901bda5fd3b62d652`,
closeout SHA-256
`319342425e3ff810cb7a1550bc7e750b76f9b2ea17af42d295b7d2e3fed5bc08`.
The terminal exited zero; all 12 artifact lengths/hashes and four
sealed raw streams passed independent replay. Loaded percentiles were
recomputed from each sealed 1,000-query timing stream. Evidence:
`s3://borsuk-bench-453182569524-euc1/research/v234-blocked-delta-100k/6e9059256d903080c293be231f6e78c3f53092ba/runs/a0001/`.

Dataset/split: ReLAION-100k D768, development queries 0–255 and
previously used method-held-out queries 256–999, cosine k=100 and
exact GT100. Both arms used the authenticated V218 graph,
ef/shortlist 2,048/2,048, eight loaded workers, and every tenth
physical row upserted under the same ID and FP16 vector (10,000 rows).
The logical corpus and truth were unchanged. Arms ran sequentially in
separate processes on the same host from one binary and compiler flags.
Times below are **loaded in-process**, not HTTP product latency.

| Verified same-run cell | Row-major decoded control | Eight-row blocked exact |
| --- | ---: | ---: |
| Development GT100 hits / 25,600 | 25,540 | 25,540 |
| Previously used method-held-out GT100 hits / 74,400 | 74,238 | 74,238 |
| Combined GT100 hits / 100,000 | 99,778 | 99,778 |
| Complete ID lists identical to control | — | 1,000 / 1,000 |
| Loaded p50 / p90 / p95 / p99, ms | 11.058 / 12.139 / 12.401 / 12.924 | 10.675 / 11.709 / 12.032 / 12.451 |
| Loaded throughput, queries/s | 711.0 | 738.0 |
| Peak process RSS, bytes | 424,468,480 | 424,153,088 |
| Overlay-owned resident bytes | 30,892,500 | 30,892,500 |
| Mutation preparation + decode/pack, ms | 52.679 + 11.209 | 53.279 + 15.305 |
| Delta rows scored / vector-body GETs | 10,000,000 / 0 | 10,000,000 / 0 |

Both arms had p05=99 and passed split quality, exact parity, memory,
packing-time and zero-GET checks. Sealed control ID/loaded SHA-256 values
were `9b617885e929d6bc61a05746a67181742e0fed9b6890cb9d311115a13fb594a0`
and `b29f49d1fb6c0e383cd6fc80d3d168bb04688a9857f1ddc56ae21baf67f7d3f2`;
blocked values were
`1e00d277f9103f9f26d0ba219ad2af3b8fc07fa6a36c8b22b0aab9e217a63cc4`
and `0d8630909a294f056b61e591bb30b396af7abc194411024508d91302920b53a0`.
The Spot quote was $0.3631/hour; estimated compute to closeout was
$0.03416, excluding EBS, S3 and billing adjustments. No vendor or
1M network comparison is inferred from this in-process cell.

**Next single gate:** on the same frozen 100k/10k panel, compare a
conservative FP32 screen plus ordered FP64 exact rescore against the
same-run V232 decoded full scan. A bound below the current top-k
threshold may skip a row only when it is proven unable to enter the
top k, including score ties and FP rounding. Require full ID-list
parity before performance; measure exact rows rescored, screen cost,
loaded p50/p90/p95/p99, QPS, memory and prep. If numerical safety
cannot be justified, do not enable pruning and choose a bounded
compaction policy instead.
