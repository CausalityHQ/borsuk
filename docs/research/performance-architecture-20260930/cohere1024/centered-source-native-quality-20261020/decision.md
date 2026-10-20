# Centered SOURCE: native qualification and cold-quality decision

Date: 2026-10-20. Decision: **retain as a Pareto candidate; do not replace the production default**.

## Frozen source and qualification

Candidate `50f76f39335bb2b4605829b67ad4c12a1f698e76` (parent `060cc7b2101e02f4cf5bb09a412dd81afa3ee74a`) implements normalized-f64 corpus centering, signed Hadamard sign codes and f64 correction factors. It is a heuristic, not a Haar rotation or a RaBitQ accuracy guarantee. Dimensions and memory admission remain generic; Cohere is an evaluation fixture.

112 targeted native runtime tests passed, including mutation/lifecycle, canonical readback, publisher, GC, actual runner-to-strict-reader and resealed identity refusals. Exact workspace Clippy correctness/suspicious, real override-unset workspace test compilation and four release targets passed. This is not a claim that the complete runtime suite passed.

## Real-input gate and full1000 result

Same frozen Cohere Wikipedia corpus (1M, D1024), cosine, k10, original 1000 query/truth bytes. Application payload cache disabled, Q1, two scoring workers. Provider cache state is uncontrolled. Authenticated real-input admission and separate disposable real-service canary passed before the quality campaign; canary-owned prefix absence was independently verified after cleanup.

| Metric | Original control | Centered candidate |
|---|---:|---:|
| Recall against frozen fixture truth | 9902/10000 (99.02%) | 9901/10000 (99.01%) |
| Returned bytes/query | 51,077,441.536 | 34,227,220.480 |
| GET/query | 169.024 | 169.332 |
| SOURCE row bytes, D1024 | 264 | 144 |
| Query p95, one recorded cell | 281.836 ms | 284.950 ms |

Timing cells are unpaired and resource/host conditions differ: they establish no causal latency comparison. Candidate cohort QPS was 4.575. Full1000 native execution and strict reduction both exited zero; the scientific per-query nonregression criterion failed.

Only query428 changed, losing truth ID130763. All 9999 common returned IDs retained exact f64 score bits. The old SQ8 coalesced range included that ID's page862 as bridge overfetch; the new range excluded it. Do not tune routing to this individual query. Any range-fusion experiment must preserve the original consumed population and discard newly fetched gap bytes before ranking.

## S3 Vectors first

The operator prioritizes S3 Vectors before Turbopuffer. No matched current vendor win exists. Historical S3 Vectors 9617/10000 is tied to an older mismatched protocol and cannot establish current parity.

Frankfurt public tariff snapshot: standard S3 read $0.43/million requests. Candidate GETs alone imply **$72.81276/million queries**, excluding compute, storage, writes, retries and lifecycle work. A vector-only 1M-D1024 S3 Vectors tariff scenario is approximately $11.4822/million queries; it is not an observed bill or matched TCO. It gives an optimistic 26.7028 GET/query ceiling before BORSUK's other costs. Include service query/processing/returned-data/storage/ingest and application compute/network costs; do not invent an extra S3 request layer inside managed S3 Vectors.

A source-bound offline interval frontier preserves all consumed SQ8/canonical ranges and optimistically makes SOURCE free. Even this relaxation requires 17.587 GET/query for a 64MiB p95 payload cap, or 11.018 for 256MiB. These are offline oracle lower bounds, not executable-policy performance.

Next falsifiable deliverable: native S3 Vectors runner admission for a bit-preserving logical-ID export of the exact authenticated canonical records, explicit service-mode validation and a fresh bounded protocol. Then exact native qualification, real input admission, disposable canary, and matched full1000 runs with equal projection, retries, concurrency and clocks. No new paid service campaign is authorized by this document.

## Immutable evidence

Bucket: `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261020/centered-source-native-quality-a0001/`

- `qualification.tar.gz`: SHA256 `36fc086653f997cd0ea2d922b69b38c8562d81e97984499d4a86125c831e3ec2`; frozen code bundles, actual executed test ELFs, release ELFs, commands and failure receipts.
- `root-results.tar.gz`: SHA256 `6284a8743f191cfbfc2f039ef4c278b506479e5f330e98e270962fc167e74020`; protocols, full native query results, strict reductions, cost worksheets, source-relative candidate bundle and readiness audit.
- Both create-only uploads and checksum-enabled HEAD verification passed.
- Candidate root `835a36d48ca48d270f29ba2dc562bd8214965058f663d954beabb111ca6d1c13`.
- Full result SHA256 `3a7e6b34a909f46b32fd755241a5f437d6c7ce8ba94cfe627529828a5a6a703a`.
- Full decision SHA256 `0ebc41759f0ef054b3d2285038e5445e068f30796d8178c3900252477152380c`.

The candidate code is reproducible but unmerged. This documentation commit leaves the library source unchanged.
