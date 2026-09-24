# V161 1M geometric relayout transfer: negative closeout

## Decision

**Killed at the preregistered transfer gate.** The V160 100k combined
balanced-two-means order and 512-row page policy does not transfer as a
qualified 1M method under the same 32-GET/16,777,216-byte envelope.
On **used** ReLAION-1M D768 validation-1000, V161 returned 98.859%
exact-source Recall@100, p05 94%, versus the paired V155 cached sparse
baseline's 99.567%, p05 98%. V161 missed its 99.400%/97% transfer floor,
and lost 256 queries, tied 715 and won 29. Keep V155 as the strongest
measured 1M point; do not promote the V161 layout/plan or open 10M/100M.

## Paired evidence

| Used ReLAION-1M D768 validation-1000 | V155 cached sparse | V161 geometric relayout |
| --- | ---: | ---: |
| Exact-source returned GT100 / 100,000 | 99,567 (99.567%) | 98,859 (98.859%) |
| Exact-source p05 hits/query | 98 | 94 |
| SQ8-only returned GT100 / 100,000 | 99,222 (99.222%) | 96,594 (96.594%) |
| Fetched SQ8 physical GT100 coverage / 100,000 | 99,647 (99.647%) | 96,881 (96.881%) |
| Planned SQ8 bytes, total / 1,000 queries | 11,134,007,040 | 16,493,667,840 |
| Planned GETs, total / 1,000 queries | 22,126 | 19,012 |
| Distinct exact-primary pages, median / p95 | not a same-format comparison | 30 / 56 |

The V161 byte total is 48.14% above V155; its GET count is 14.07% below.
All V161 queries stayed within both per-query caps (maximum 32 GETs and
16,773,120 bytes). Its exact-source union recovered 2,265 GT100 hits over
the SQ8-only return, but the SQ8-only and physical-coverage gaps versus
V155 were 2,628 and 2,766 hits. This locates most of the deficit in
which SQ8 rows the physical plan fetches, before exact-source reranking.
The p95 primary spread of 56 pages exceeds the 32-GET allowance; this
alone does not prove the minimum byte cost of covering all primary pages.
A closed-artifact GT-blind cover and GT-aware stage decomposition is the
next narrow diagnostic before changing the builder or planner.

## Provenance and limits

One Causality `c7i.12xlarge` Spot instance `i-0d11e9c3c08f14ed9` ran from
clean pushed source `b9b5b752b78e85bd9c9b8d015ab8674bc1858e39`,
reported a complete terminal, and was confirmed terminated. Immutable
prefix: `s3://borsuk-bench-453182569524-euc1/research/v161-geometric-relayout/b9b5b752b78e85bd9c9b8d015ab8674bc1858e39/runs/a0001/`.
Source archive SHA-256:
`ba2570073477736af0c9426d287c8f94f84c6f7e614ada8377a983fb69d8eb75`.
Terminal SHA-256:
`7b370d8e3500ea5be069b5a5d0712d247b5c4c6c9b82f8a2dccc0032132676cf`.
The controller streamed all **16** terminal-listed S3 artifacts and matched
every length and SHA-256. The 780,000,000-byte relaid SQ8 object has SHA-256
`f4c38a2451142fa197b68ec44669280e17bb76ca65d97ab4feffd896ca6ee0bf`.
Summary SHA-256:
`6d4b290b0f17e17575e82e9cc3414d4458934deffedef53f3a06dcb0b1360afe`;
independent check SHA-256:
`adfce15d967fd654e52055a3cc65af1a4aff55eac9e28d90c1e13db00d0a8ce9`.
The checker passed all 1,000 rows, including byte-preserving relayout,
recomputed plans, SQ8 and exact-source ranked IDs, GT hits and decision.

Construction peak process RSS was 16,934,360 KiB; scoring peak process
RSS was 7,854,440 KiB. These are offline worker phases, not serving RAM
or a two-generation bound. There were no live S3 query GETs, latency or
throughput measurements. The 100k and 1M layouts used the same source-only
builder family and generic page-width formula, with their respective
source metrics, but both cohorts were already used. A 100k success cannot
be promoted across scale from these results.

## Next decision

Run one closed-artifact decomposition of V161's exact-primary rows,
primary pages, minimum 32-GET page cover and final fetched ranges, paired
against V155's terminal-closed per-query coverage. If the relaid pages
have enough GT100 headroom but the 513/1 interval plan loses it, redesign
admission and preregister a 100k/1M falsifier. If the relaid pages
themselves lack headroom or their minimum cover breaches 16 MiB, reject
this one-wave 512-row balanced-two-means format at 1M and consider a
materially different data representation or charged service profile.
Do not tune page width or cluster count on validation GT. Requested high
recall may use a larger explicit memory cap, but the objective/resource
policy remains generic across N and corpus identity. Lean proves only
conditional arithmetic bounds; measured recall, latency and charged RAM
remain independent gates.
