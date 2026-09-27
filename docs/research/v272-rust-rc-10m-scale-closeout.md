# V272 Rust RC CoHere 10M scale/cost closeout

Status: complete, **no-go on recall**, 2026-09-27 UTC. The sole Spot attempt
`a0001` on `i-01dffc787faf0077b` was terminated. Source commit
`eb16c09004a4d06852600b5f151e44b105ed22b6`, frozen library commit
`aa4182a2e6b78a254bca732663c7528818eb6561`, source archive SHA-256
`e0e61810247bb6a653f62ecc4b73c0650104c244bbaad0cb73b341b46e23455c`.
Terminal SHA-256 `e75a7b399920b277683406ee370a67204f3e9f01fd215e809c0f606356958352`;
closeout SHA-256 `bc7c64a9db65c89d28f09f1839e9fca355382e48800376e9275a6cd66a04b0ac`.
All 19 sealed artifact sizes and hashes were replayed, and the published root
SHA-256 `948e8a5555f44011b22681ca2cd20edde93f6fec5e6261e20e3a7b799d467022`
was read back from S3. Immutable artifacts:
`s3://borsuk-bench-453182569524-euc1/research/v272-rust-rc-10m-scale/eb16c09004a4d06852600b5f151e44b105ed22b6/runs/a0001/`.

The dataset was all 10,000,000 canonical CoHere-large-10M train rows, D768
cosine, from 458 authenticated Parquet shards. The source reads returned
30,726,503,063 bytes; normalized FP32 source SHA-256 was
`2e33abfc666e652455a815b90d88a13b617f6dbb431538d6324ad06eccf06f1f`.
Queries were canonical test rows 0–999, **prior-used**, at k100. Exact ground
truth was FAISS `IndexFlatIP` v1.15.1 on unit-normalized FP32 vectors over all
10M source rows. The Rust library reopened its authenticated S3 generation in
a separate process, hydrated five blobs, then ran one query at a time after a
warmup query. Query-time object GETs were zero. These are **verified in-process
resident API** numbers, not HTTP product latency or a vendor comparison.

| Query split | Exact GT100 hits | Recall@100 | Fifth-percentile hits |
| --- | ---: | ---: | ---: |
| Development, test 0–255, prior-used | 25,421 / 25,600 | 0.993008 | 97 / 100 |
| Validation, test 256–999, prior-used | 73,878 / 74,400 | 0.992984 | 96 / 100 |
| Combined, test 0–999 | 99,299 / 100,000 | 0.992990 | 97 / 100 |

The same 1,000 raw query records give p50/p90/p95/p99
**82.964/110.168/120.329/147.272 ms**. Query execution took 84.374 s,
or 11.852 sequential QPS. Both latency gates (p95 ≤150 ms, p99 ≤180 ms)
passed, as did the search RSS gate: 19,852,156,928 bytes against 32 GiB.
Recall failed on **both** splits against R@100 ≥0.995 and p05 hits ≥98.
The 1M V271 fresh-query result (99,771/100,000 hits, p95 61.314 ms) used a
different query split and corpus size; it is scale context, not a paired
quality improvement claim.

| Lifecycle measure | Verified value |
| --- | ---: |
| Prepare 458 source shards | 6m51s, 30,726,503,063 response bytes |
| Build immutable generation | 6h53m16s, peak 52,436,692 KiB RSS |
| Publish authenticated generation | 4m39s |
| Reopen/hydrate | 5 GETs, 18,799,167,806 bytes, 303.031 s |
| Search process including hydration | 6m29s, peak 19,386,872 KiB RSS |
| Exact truth | 1m55s, peak 76,141,356 KiB RSS |
| Persistent five-blob generation | 18,799,167,806 bytes, plus root/head metadata |

The launch Spot quote was $0.9627/hour; quote times 7.2767 hours through
terminal is **$7.005**, an estimate rather than an AWS bill. At the published
Frankfurt gp3 rate of $0.0952/GiB-month, the 250 GiB volume's same-duration
capacity estimate is **$0.237**. At the Frankfurt S3 Standard first-tier
rate of $0.0245/GB-month, retaining the 18.799 GB generation is about
**$0.461/month**. These estimates exclude request charges, retained source
and research artifacts, account discounts, taxes, and any storage kept after
closeout; no total lifecycle cost or vendor cost win is claimed. Pricing
references: [AWS EBS gp3 Frankfurt](https://aws.amazon.com/de/blogs/germany/migrieren-sie-ihre-amazon-ebs-volumes-von-gp2-zu-gp3-und-sparen-sie-bis-zu-20-der-kosten/),
[AWS S3 pricing](https://aws.amazon.com/s3/pricing/).

**Decision:** `no_go_10m`. Search latency, RSS, and the five authenticated
hydration GETs pass, but quality misses both preregistered split gates.
Do not promote this revision to an HTTP/vendor superiority claim or repeat
10M with a parameter sweep. Choose one generic quality repair and require a
cheap 100k falsifier before another paid 10M run.

The original local launcher exited after the complete terminal because its
published-root readback used `generation/roots/<sha>.json`; the object-store
writer encodes the inner slash as `generation/roots%2F<sha>.json`. The one-line
closeout path was fixed after the frozen run. Recovery replayed every sealed
artifact, checked terminal/launch identity and published head/root, wrote
`closeout.json` once, read it back, and confirmed the instance was terminated.
The benchmark source archive and measurements were not changed.
