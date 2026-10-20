# Exhaustive f64 cosine ordering witness, 2026-10-20

The unchanged qualified native diagnostic compared all 1,000 frozen query
rows against all 1,000,000 frozen corpus rows at D1024, k=10. All 10,000
selected IDs and all 1,000 top-k orderings match the frozen truth file.
No truth bytes were rewritten.

## Method and scope

The kernel reads little-endian f32 vectors, computes norms and cosine in f64,
and breaks equal scores by smaller zero-based corpus-row ID. It authenticates
all corpus, query and truth bytes. The selected prefix length is 1,000, equal
to the complete frozen query file. The raw v1 diagnostic still records
`full_request_equivalence_established: false` and its original prefix-only
limitation. This note reports the observed complete-file ID/order witness;
it does not redefine that field, prove broader request/service equivalence,
or qualify ANN latency, vendor performance, or product readiness.

The original first-eight result and its protocol remain immutable. This was
a separately preregistered 1,000-row diagnostic using the same ELF and config,
not a rerun of the completed input-export/admission campaign.

## Frozen identities

- Native source SHA256: `5b266f6265db8716bf7ab08ac5d3cf9daa943e4f22e192dddbcd14b9eca09fa9`.
- ELF SHA256: `e18412b8602575e2925f14739219f37401c01ce1ccbca2ec4584e5dc27580aa9`.
- Config SHA256: `f055c78fd126a6ef310c4e5c48fc04a69a5e3c405abd07f246c32e398dde1555`.
- Corpus: 4,096,000,000 bytes, SHA256 `31f28fa211f9237f0f7e41f569773cd207cefcdce22d4932ec96aee01ff2c40d`.
- Queries: 4,096,000 bytes, SHA256 `8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e`.
- Truth: 80,000 bytes, SHA256 `0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f`.
- Raw result SHA256: `00ae14bb63a988a21dee92e8620f2ea4c3efe0c8b296ba103d4cd0dd19cee560`.

## Execution and decision

Causality Spot EC2 `i-0665a9359195a75a9`, original SSM
`2fba1c96-6393-418a-97c7-682e3dc63076`, terminal Success/0.
Runtime limits were CPUQuota=100%, MemoryMax=4GiB, MemorySwapMax=0 and
RuntimeMaxSec=7200, with guest and external host-expiry guards.
The job finished in 529.393650352 seconds; time(1) recorded 526.57 user
seconds, 1.13 system seconds, 26,980 KiB peak process RSS and no swaps.
These are diagnostic execution costs, not ANN performance measurements.
Other bounded work on the host makes them unsuitable as an isolated
microbenchmark.

Strict reducer `8625dd02-7311-427d-8db7-d30d3153d322` finished Success/0.
It checks authenticated input identities, all query indices 0..999,
1,000 report rows, overlap ten, exact top-k order, empty position differences
and the unchanged conservative scope flag. Decision:
`RETAIN_FROZEN_TRUTH_ORDERING_WITNESS`.

The archive contains preregistration, frozen source/ELF/config, raw result,
stdout/stderr, time/exit/terminal files and the strict root summary:
`s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261020/truth-full1000-root-a0001/evidence.tar.gz`,
SHA256 `69b0fe33012c729b23c1c999851ff1aa6579f034a70503ed1988d70c98eedcbd`.
