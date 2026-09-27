# V282 object-native SQ8 serving gate

Status: preregistered design decision, 2026-09-27 UTC. No V282 performance
measurement or vendor win exists. The active V281 10M resident experiment is
preserved as graph-quality evidence and is not a V282 serving attempt.

## Decision

Use a compact source-only router and authenticated, generation-pinned S3 SQ8
page ranges for the first non-resident serving candidate. Do not hydrate the
FP16 or SQ8 vector plane and do not keep query-read pages between queries in
the cold gate. Reuse `PageAuthority`, the conditional one-attempt S3 reader,
`budgeted_page_rank`, and `rank_returned_ranges`. Reuse V155's cached sparse
graph expansion with the top PQ nominees as provisional primary seeds. V155
used SQ8-exact primary seeds scored from a local vector plane, so this cold
route is a new quality candidate and V155 recall cannot be carried over.
No new score calibration or graph parameter sweep enters this gate. A new
generation schema binds router,
SQ8 page digests, object identity and ETag without a local exact-source file.

The verified ReLAION-1M validation V155 SQ8-only result was 99,222/100,000
GT100 hits with 22,126 **planned** GETs and 11,134,007,040 **planned** bytes
over 1,000 queries. Those numbers depended on local SQ8-exact primary scoring
and included no live S3 latency. They are context, not a quality baseline for
the provisional PQ-primary route. V199 separately verified
live conditional range transport at 10,047 GETs and 7,388,559,360 received
bytes over 1,000 ReLAION-1M validation queries, but used precomputed plans
and a resident FP16 final scorer. Neither is a same-revision complete product
measurement. V280's 61.694 ms first-pass client p95 used 1.88 GB startup
hydration and 2.06 GB peak RSS; it is a resident reference only.

Freeze one size-scaled policy before the 100k falsifier: `regions =
ceil(1024 * ceil(rows/256) / 3907)`, PQ shortlist 512, the first 100
PQ nominees as provisional primary seeds for k100, page expansion `beta = 4`, and 32 GET /
16,777,216-byte per-query caps. This retains the
historical V155 1M region fraction, with the same formula on both 100k
datasets. It is a preregistered candidate, not a measured 100k quality claim.
PQ nomination still materializes rows from selected regions, so its CPU work
and scratch scale with `regions × 256`; generation admission charges this
scratch. A 1M pass does not qualify 100M latency. The later 10M/100M gate
must measure whether this routing method remains viable under a declared RAM
budget and change the router if it does not.

Build the physical SQ8 order with the existing corpus-only V120 layout rule,
`ceil(8192 × (rows / 1,000,000)^(1/3))` clusters, capped at `rows`, and
V115's source-only PQ64 training with two summaries per 256-row page.
This uses no query or truth data and applies unchanged to both datasets.

V239 already falsified a naive PQ-graph candidate-to-page mapping: its
quality-passing 1,024 candidate prefix touched 147 distinct 256-row SQ8
pages at p95, at least 29,352,960 bytes/query on ReLAION-100k. V240's
graph-local order made this 218 pages. The V155 physical planner is retained
as a measured page scheduling method, while the changed primary seeds require
new quality evidence; simply issuing one
range GET for each graph candidate is excluded.

Prior centroid arms constrain the control: V139's unbudgeted distance
threshold exceeded 16 MiB on 364/1,000 ReLAION-1M validation queries;
V146's flat centroid scorer plus planner was 11.072 ms p95 on that used
1M split and scales linearly with rows; V149's physical-page hierarchy
captured only 77.096% of the D96 V140 page plan. V150's original semantic
graph captured 73.355% of that plan, but postterminal fetched-range truth
coverage was 99,523/100,000 versus V140's 99,738/100,000 on the used D96
cohort. Exact plan capture alone is therefore too blunt to decide returned
quality. The paired flat control below isolates page discovery; it does not
repeat those failed threshold, hierarchy or full-scan arms as product routes.

## Gates

1. **100k falsifier.** Build the preregistered PQ-primary route from corpus rows
   on ReLAION-100k and CoHere first100k, D768 cosine, k100. Seal build and
   query-independent layout before reading the existing 1,000-query panels.
   Pair the unchanged V282 cached sparse graph plan with one flat-centroid
   diagnostic using identical PQ primaries, SQ8 scorer, 32-GET/16,777,216-byte
   caps and raw queries. The flat arm is a page-discovery oracle, not a 100M
   serving candidate: V146 already measured 11.072 ms p95 for flat score plus
   planning on ReLAION-1M validation and rejected its linear scan. Report
   development0–255 and validation256–999 separately, with exact GT100 hits,
   p05 hits/query, planned page containment, GETs and bytes per query, planner
   time and RSS. Compare the same-corpus resident graph quality controls and
   the V239 page lower bound as context. Advance V282 only if both datasets
   retain at least 98% mean recall@100 and p05 at least 95, with no more than
   0.5 percentage point loss against the paired flat arm and both physical
   caps. If flat passes but V282 fails, change page discovery (balanced
   semantic cells are the next candidate); if both fail, change the PQ
   primary/layout representation before another page-graph attempt. Used
   panels make this a falsifier, never publication evidence.
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
page hashes, ranks SQ8 rows and returns submitted-read/error accounting.
`ObjectNativeGeneration::open` adds a new authenticated root for the router,
centroids, page graph, page sidecar and remote SQ8 object, admits routing
metadata and concurrent query buffers against a declared memory budget, and
limits simultaneous reads. Its `plan_pages` and `search` methods reuse the
cached sparse graph expansion with PQ-primary seeds and the authenticated
range reader.
The caller must supply an authorized root digest; no S3 vector plane is
loaded. These increments do not yet build a complete artifact from user data,
serve HTTP or qualify latency. The next implementation step is a 100k
artifact and local query falsifier, then the cold HTTP gate. Lean can prove arithmetic caps and
generation-binding invariants under explicit assumptions; recall, latency,
RSS and vendor superiority require measured samples.

## Paired replay contract and 100M envelope

`v282_build_routing` hashes the local SQ8 body against the source-only router
before deriving centroids and graph; `v282_seal_generation.py` binds that
build, the page digest authority, object SHA and ETag to one root manifest.
`v282_local_falsifier` checks the trusted root digest and SQ8 body, then
replays the same 1,000 query/GT100 panel through graph and flat discovery. Its
local SQ8 reads are authenticated by page SHA-256 and count **planned**
GETs/bytes; route/rank CPU and RSS are diagnostic, not HTTP/S3 measurements.
The per-query receipt records GT100 hits in the PQ shortlist, GT100 hits in
the fetched ranges, and GT100 hits returned after scanning those ranges. This
separates nomination, page coverage, and quantized scoring losses. There is no
local index inside a fetched range, so local-index loss is zero by construction
in this candidate; a later compressed leaf index requires its own paired
measurement. `v282_summarize_falsifier.py` applies the frozen split and pass
criteria above. A gate failure is a redesign decision, not an invitation to
increase PQ regions or graph expansion on the same panel.

Before 100M promotion, the provisional single-process admission envelope is
64 GiB including two pinned routing generations, up to eight active queries,
32 GETs and 16 MiB of fetched SQ8 per query. The admitted query-buffer term
alone is at least `8 × 3 × 16 MiB = 384 MiB` in the current reader model.
The Opus review's 184 resident bytes/row is an **estimate**, so two 100M
generations project to 36.8 GB before page metadata, allocators and scratch;
the 64 GiB cap is a falsifiable engineering ceiling, not a measured RSS or an
optimal memory point. At 100M require no vector-plane hydration, one parallel
range wave, p90/p95 client latency below 150/200 ms at matched recall, and
lower total dollars per matched successful query than **each** competitor.
These are targets, not
claims. Evaluate at least two admitted router-memory points on the same
fresh panel before selecting a production default. If the measured
recall/latency/cost Pareto curve needs more than 64 GiB, raise or reject the
ceiling explicitly instead of silently lowering recall. The 100k replay and
1M cold HTTP gate may kill this V282 route well before that scale gate.

The completed Fable consultation `7129fbdf56494d7f` proposed fetching the
highest-mass PQ candidate cells and merging unfetched candidates by a
per-query calibrated PQ score. Its V239 coverage premise uses a different
resident graph and the bridge has no measured quality or 100M memory bound;
it is retained as a conditional redesign idea only if V282 page discovery
fails. V139's unbudgeted centroid threshold, V146's full flat scan at 1M,
and V149/V150's weak page-plan capture remain killed as product routes.
