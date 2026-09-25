# V232 exact decoded mutation delta, ReLAION-100k

**Decision: promote one 1M network serving gate.** Decoding authenticated
FP16 mutation coordinates once to resident FP32 preserved every returned
ID list while making the exact 10,000-row delta scan faster. V231's
approximate index remains rejected because it lost recall. The V232
measurement is loaded **in-process** query time, not HTTP product latency.

The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-02b3a6c82c9cb3193` in `eu-central-1c`, independently confirmed
**terminated**. Source commit
`0a603ba7395045d706227e239085d2ed8856ef5e`, source archive
SHA-256 `b44cd1452af6bffbfece2dcfd2280030d8bd7bf42840ad5cdd9c313f9026f772`,
original terminal SHA-256
`6fed6e54b81879a1e1a8d0c66950b4d2f4d6a9ce045f175a8dcdd4c8fcad64cb`,
closeout SHA-256
`a0a144a927a5eef389b532f0710e8874dbb6d39833d8fb921906eeaf1d5fc200`.
The terminal exited zero. All 12 terminal artifacts and four separately
sealed raw streams passed independent byte-length and SHA-256 replay;
loaded latency percentiles were recomputed from those raw samples.
Evidence:
`s3://borsuk-bench-453182569524-euc1/research/v232-decoded-delta-100k/0a603ba7395045d706227e239085d2ed8856ef5e/runs/a0001/`.

Dataset/split: ReLAION-100k D768, development queries 0–255 and
previously used method-held-out queries 256–999, cosine k=100, exact
GT100. Both arms used the authenticated V218 graph, ef/shortlist
2,048/2,048 and every tenth physical row upserted under its same ID
and vector (10,000 rows). The logical corpus and truth were unchanged.
The two arms ran in separate processes, in order, on one host with
eight loaded workers and no response cache.

| Verified same-run cell | Linear FP16 decode | Resident FP32 decode |
| --- | ---: | ---: |
| Development GT100 hits / 25,600 | 25,540 | 25,540 |
| Previously used method-held-out GT100 hits / 74,400 | 74,238 | 74,238 |
| Combined GT100 hits / 100,000 | 99,778 | 99,778 |
| Complete ID lists identical to linear | — | 1,000 / 1,000 |
| p05 GT100 hits/query | 99 | 99 |
| Loaded p50 / p90 / p95 / p99, ms | 25.717 / 26.840 / 27.167 / 27.695 | 11.370 / 12.390 / 12.692 / 13.185 |
| Loaded throughput, queries/s | 308.9 | 693.9 |
| Peak process RSS, bytes | 415,973,376 | 424,169,472 |
| Overlay-owned resident bytes | 15,532,500 | 30,892,500 |
| Mutation preparation, ms | 58.043 | 56.052 + 12.179 decode |
| Exact delta rows scored / vector-body GETs | 10,000,000 / 0 | 10,000,000 / 0 |

The decoded arm reduced loaded p95 by 53.3% and achieved 2.25× the
loaded throughput with exact ID parity. It met every preregistered
quality, latency, throughput, memory and zero-GET gate. The peak RSS
difference is descriptive because the arms used separate processes.
Launch-time Spot quote was $0.3631/hour; estimated compute to closeout
was $0.03826, excluding EBS, S3 and billing adjustments. V218 baseline
split hits were 25,537 and 74,234. No S3 Vectors or Turbopuffer
product win is inferred from this in-process cell.

**Next single gate:** test the decoded mutation overlay over VPC-peer
HTTP on ReLAION-1M against the V230 10,000-row linear mutation baseline,
with the same frozen 1,000-query panel, k, eight clients and no response
cache. Require identical complete ID lists and per-split quality;
recompute p50/p90/p95/p99 from each sealed network raw stream and report
throughput, request/response bytes, GETs, server RSS and compute cost.
Then test real-S3 1M mutation publication before any 10M/100M claim.
