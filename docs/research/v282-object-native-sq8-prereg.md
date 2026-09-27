# V282 object-native SQ8 serving gate

Status: preregistered design decision, 2026-09-27 UTC. No V282 performance
measurement or vendor win exists. The active V281 10M resident experiment is
preserved as graph-quality evidence and is not a V282 serving attempt.

## Decision

Use a compact source-only router and authenticated, generation-pinned S3 SQ8
page ranges for the first non-resident serving candidate. Do not hydrate the
FP16 or SQ8 vector plane and do not keep query-read pages between queries in
the cold gate. Reuse `PageAuthority`, the conditional one-attempt S3 reader,
`budgeted_page_rank`, and `rank_returned_ranges`. The V155 source-only cached
sparse planner is the fixed first route; no new score calibration or graph
parameter sweep enters this gate. A new generation schema must bind router,
SQ8 page digests, object identity and ETag without a local exact-source file.

The verified ReLAION-1M validation V155 SQ8-only result was 99,222/100,000
GT100 hits with 22,126 **planned** GETs and 11,134,007,040 **planned** bytes
over 1,000 queries. It included no live S3 latency. V199 separately verified
live conditional range transport at 10,047 GETs and 7,388,559,360 received
bytes over 1,000 ReLAION-1M validation queries, but used precomputed plans
and a resident FP16 final scorer. Neither is a same-revision complete product
measurement. V280's 61.694 ms first-pass client p95 used 1.88 GB startup
hydration and 2.06 GB peak RSS; it is a resident reference only.

V239 already falsified a naive PQ-graph candidate-to-page mapping: its
quality-passing 1,024 candidate prefix touched 147 distinct 256-row SQ8
pages at p95, at least 29,352,960 bytes/query on ReLAION-100k. V240's
graph-local order made this 218 pages. The V155 physical planner is retained
because it was measured as a joint quality/read method; simply issuing one
range GET for each graph candidate is excluded.

## Gates

1. **100k falsifier.** Build the unchanged source-only route from corpus rows
   on ReLAION-100k and CoHere first100k, D768 cosine, k100. Seal build and
   query-independent layout before reading the existing 1,000-query panels.
   Report development0–255 and validation256–999 separately, with exact
   GT100 hits, p05 hits/query, planned page containment, GETs and bytes per
   query. Compare the same-corpus resident graph quality controls and the
   V239 page lower bound. Advance only if both datasets retain at least 98%
   mean recall@100 and p05 at least 95 while respecting a 32 GET and
   16,777,216-byte hard per-query plan cap. Used panels make this a falsifier,
   never publication evidence. A failure requires a changed representation or
   physical plan, not a threshold tuned to one dataset.
2. **One 1M cold HTTP gate.** Freeze the winning 100k method and schema, then
   run CoHere first1M D768 cosine k100 with source-disjoint train query
   ordinals 1,001,000–1,001,999 and newly computed exact truth. Freeze their
   digest before measuring. One server and one VPC-peer client in eu-central-1c, eight persistent
   connections, empty vector cache, no response cache. Read only metadata at
   open; count every query-time physical S3 GET, verified byte, failed GET,
   retry, and error. Seal the same raw query samples for recall@100,
   p50/p90/p95/p99 client latency, sustained QPS, RSS and cgroup peak,
   startup reads, object-store bill components, build cost and total elapsed
   cost. Require the 100k quality/physical caps on both development and
   validation splits, exact generation binding, and no full vector-plane
   hydration. At 1M measure memory against a declared budget derived from
   router width, page metadata, concurrent in-flight bytes and pinned
   generations; do not introduce a vector-count knee.
3. **Competitor gate.** Run S3 Vectors and Turbopuffer directly on the same
   corpus, fresh queries, k, recall target, cache state, transport,
   concurrency, region and lifecycle cost where access permits. Require
   BORSUK to beat both at equal or better recall on end-to-end p90/p95 and
   throughput per dollar before saying it outperforms them. Historical V263
   S3 Vectors CoHere first1M used the old query panel and opaque cache, so it
   is context. Turbopuffer's [vendor-published 1M×768 cold p90 444 ms](https://marketplace.turbopuffer.com/blog/turbopuffer)
   uses top-k 10 at 32 QPS on undisclosed data and recall; it is a stronger
   dated context target, never a matched win, until a direct run exists.

The first code increment is `OneAttemptS3::rank_verified_sq8_pages`: it
preflights the GET/byte plan, limits parallel reads, authenticates ETag and
page hashes, ranks SQ8 rows and returns physical/error accounting. It does
not yet build the source-only route, open a complete generation, or qualify
latency. The next implementation step is the new generation root and route
binding, then the 100k falsifier. Lean can prove arithmetic caps and
generation-binding invariants under explicit assumptions; recall, latency,
RSS and vendor superiority require measured samples.
