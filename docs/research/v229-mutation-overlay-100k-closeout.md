# V229 same-vector mutation overlay, ReLAION-100k

**Decision:** Pass the frozen 100k overlay gate and promote to one 1M
end-to-end service test. The bounded brute-force delta remains a
candidate; no 10M/100M mutation policy is inferred from this result.

The sole Causality c7i.4xlarge Spot attempt `a0001` on
`i-06ba114422c4a75ee` is terminated. Source commit
`bb798d12f74f9b717fce81ca2360ace27af9513e`, archive SHA-256
`c3ee26e73d72bcb874f1280ca4c20b231939dab1789a76608abff436f013f864`,
terminal SHA-256 `c50a179b9687347846c76956cd8e29a782763a6f846ae40175e95fa91dff6081`.
The complete terminal and all seven artifacts passed length and
SHA-256 replay. Immutable evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v229-mutation-overlay-100k/bb798d12f74f9b717fce81ca2360ace27af9513e/runs/a0001/`.

Dataset: ReLAION-100k D768, development queries 0–255 and previously
used method-held-out queries 256–999, k=100, exact GT100. The V218
graph, FP16 plane, PQ codes and query arm ef/shortlist=2,048/2,048 were
held fixed. Every hundredth physical row (1,000 rows) was upserted
under its **same ID and same authenticated FP16 vector**. The logical
vector set and GT100 did not change. Before mutation, the new library
loader replayed all 1,000 V218 returned-ID lists exactly. Candidate
raw IDs were sealed before GT100 download.

| Measured in-process cell | V218 selected baseline | V229 1% upserts |
| --- | ---: | ---: |
| Development GT100 hits / 25,600 | 25,537 | 25,538 |
| Method-held-out GT100 hits / 74,400 | 74,234 | 74,235 |
| Combined GT100 hits / 100,000 | 99,771 | 99,773 |
| Full returned-ID-list ties / 1,000 | — | 998 |
| p05 GT100 hits/query | 99 | 99 |
| Loaded eight-worker p50/p90/p95/p99, ms | 5.750/6.673/6.915/7.380 | 7.468/8.418/8.722/9.062 |
| Completed throughput, queries/s | 1,353.3 | 1,047.9 |
| Peak process RSS, bytes | 212,135,936 | 258,904,064 |
| Vector-body GETs | 0 | 0 |

The timings and process RSS are verified measurements from different
runs, not paired per-query service latency or a causal attribution of
the full difference to the overlay. V229's process also held the raw
baseline and request panels, and used a different loader path. The
overlay itself owned 1,564,500 resident bytes: the 12,500-byte base
mask plus 1,000 FP16 rows, IDs and norms. Across 1,000 queries it
scanned exactly 1,000,000 delta rows and masked 20,508 graph shortlist
rows. The base visit p95 remained 23,015. V229 sequential p50/p90/
p95/p99 was 7.668/8.624/8.921/9.382 ms.

The preregistered no-quality-loss-per-split, p05, loaded p95 no more
than twice V218's 6.915250 ms, 256 MiB RSS and zero-GET gates all
passed. The runner separately reported 252,216 KiB maximum RSS; its
measurement source differs from the process's VmHWM report. This is a
100k in-process mutation
falsifier; it does not show 1M network latency or broad update/delete
quality. The next gate must measure authenticated 1M mutation serving
end to end on the frozen validation panel, including recall, p50/p90/
p95/p99, throughput, memory, object bytes/GETs, cache state and cost.
