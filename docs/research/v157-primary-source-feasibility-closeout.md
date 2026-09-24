# V157 exact-primary S3 feasibility closeout

## Decision

The V114 exact-SQ8 primary selection cannot be reproduced for every
ReLAION-1M validation query under a 32-GET, 16,777,216-byte total request
cap by fetching the physical SQ8 pages containing all 512 nominees from S3.
The minimum nomination cover alone exceeds that cap on **294 of 1,000**
queries. Do not wire V156's live query path to silently take the first 100
PQ nominees and call it the V154/V155 method. That alternative needs a fresh
paired returned-quality gate. An authenticated local exact tier, a certified
score bound that fetches fewer nominees, or an explicitly larger charged
I/O allowance are other possible architectures. A row-granular range
schedule would also be a different transport method and needs its own
charged GET/byte proof. Select by recall and resource policy across
datasets, not by corpus name or vector count.

This gate establishes a necessary I/O condition only. It measures neither
S3 latency nor returned Recall@100, and the 100k pass does not show that
nomination plus graph and final fetches fit the full request budget.

## Measured results

The source-only PQ64 roster contains 512 nominees per query. V114 selected
its 100 primary rows by exact SQ8 scoring of all 512; the alternative below
uses the first 100 PQ64 nominees. Each result is a verified, GT-blind
arithmetic measurement on a frozen **used** split. `Overlap` is the number
of common rows in the two 100-row primary sets, not recall.

| Dataset and split | Minimum 32-GET nomination cover, p50 / p95 / p99 / max (bytes) | Queries whose nomination alone fits 16,777,216 B | PQ versus exact-primary row overlap, p50 / p95 / range (of 100) |
| --- | ---: | ---: | ---: |
| deep-image-96-angular random100k train subset; publication-test queries 9000–9999 | 1,437,696 / 2,847,744 / 3,262,464 / 3,760,128 | 1,000 / 1,000 | 93 / 97 / 83–99 |
| ReLAION-1M validation-1000 | 10,183,680 / 112,819,200 / 177,315,840 / 298,321,920 | 706 / 1,000 | 68 / 84 / 38–94 |

At 1M, the high-tail cover is far beyond the cap even before graph edges or
final page reads. The 1M PQ-primary set differs substantially from the
exact-SQ8 baseline, so its returned quality cannot be inferred from the
historical exact-primary Recall@100.

## Provenance and verification

The frozen evaluator, independent checker, and preregistration are source
commit `39793c43b7e6b21d426632e58fe88247fda06354`. One Causality
`c7i.xlarge` Spot attempt, instance `i-03a9167c88a58d11a`, produced
`complete` terminal marker SHA-256
`6658f9bcb6d80eb0314b54990016a096deeb8b80d8db8ca89c81410bc3c15144`
at `s3://borsuk-bench-453182569524-euc1/research/v157-primary-feasibility/39793c43b7e6b21d426632e58fe88247fda06354/runs/a0001/`.
The instance was confirmed terminated after the terminal marker.

The terminal authenticated all seven artifacts; a separate read rehashed
every S3 artifact. Both independent-check artifacts report `pass` for all
1,000 query rows and the complete summaries. Raw per-query SHA-256 values:

| Cell | Raw JSONL SHA-256 | Summary SHA-256 | Independent check SHA-256 |
| --- | --- | --- | --- |
| deep-image-96-angular random100k publication-test 9000–9999 | `a6389909009fee99b0017d3b8c07341a940d8fdc1ae6e0c0409b4e5f273f2195` | `ee4360f456e1696496aa2347e96b82cc7b4717aabe712b588153e6d3e9133d05` | `1d11e590ce84d8c6d1cdf070848eeb351cac33a75689df5371ae24ce2b39ce87` |
| ReLAION-1M validation-1000 | `334194444eb3c3d60fd9d2807e13980c5e9f5083d6498e6f130f9ac4798793cb` | `b174908c09bd4525a7b97b6bc841bc71c046bdf457451f8b7040d010b2bccb7d` | `72123d5c4107b00eb3c7ba828d0f986d4368c60b5e9aabc6e4f391e04d63dd0c` |

The fixed source inputs were V122 `evidence.jsonl` (6,183,526 bytes,
SHA-256 `deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6`)
and V116 `rust-replay.jsonl` (13,455,525 bytes, SHA-256
`3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960`).
The evaluator read only the ordinal, nominee roster, and exact-primary
roster. Its minimum **physical-page** cover algorithm merged the cheapest
page gaps until at most 32 contiguous ranges remained, charging each
bridged 256-row page and the exact short final page. This lower bound
applies to the V156 page-aligned SQ8 schedule, not an arbitrary row-range
redesign.

## Next gate

Preregister a paired 100k **D768** returned-quality replay for PQ-first-100
primary selection against the V114 exact-SQ8-primary baseline, with the
same nominees, graph planner, exact source, transport cap, and query split.
Construct and seal exact GT for that corpus before reducing returned IDs.
The V114 ReLAION-100k correctness cell has no returned GT, while V122's
D96 100k candidate fetched its entire SQ8 body; neither is already a
decisive PQ-primary returned-quality comparison. If the new gate fails,
test a generic exact-primary architecture with an
authenticated bounded cache or a score certificate; its per-query memory
and I/O allowance must be explicit. Promote only a winner to a frozen 1M
paired gate, then a distinct 10M/100M scale gate. Separately measure live
S3 latency and charged bytes/GETs before any production claim.
