# V36 Funnel-3 Architecture Qualification Design

## Objective

Qualify or reject a dimension-independent, S3-native three-stage vector-query
funnel before changing the production format. The candidate must reach
competitive recall on real high-dimensional data, keep the resident serving
index below 3 GiB at 100 million rows, bound remote work to two waves of at
most 16 GETs and 8 MiB each, and preserve append-only writes with observable
snapshot semantics.

This is a research qualification, not a release claim. BORSUK is pre-release:
the winning design may introduce a breaking format with no legacy reader or
migration path. Sub-second contracts run locally; corpus work runs on bounded
same-region AWS Spot capacity under profile `causality`. No 100M build starts
until one frozen arm passes 1M and 10M without retuning.
Before release, the campaign-only V36 freezer/controller/evaluation modules and
executables move into a separate workspace research crate. The generic library
retains its production Parquet and object-store dependencies but exposes no
frozen-dataset authority, AWS campaign policy, or research checkpoint surface.

Before the full-source materialization, a separately named object-sample
screen uses a frozen, bounded population from query-independent hash-ranked
complete source objects on same-region Spot/NVMe. It is a cheap
architecture falsifier, not the V36 qualification corpus and not release
evidence. It may reject a projection, posting geometry, score, coarse code, or
fine-object layout, but passing it only authorizes the globally hash-selected
1M freeze below. The screen never runs on devbox RAM or persistent disk.

## Evidence and unresolved mechanism

Authenticated results establish both the opportunity and the risk:

- the 100M x 96D production baseline reached 0.992 recall, 44.4 ms cached p95,
  910.5 ms uncached p95, 70.82 GETs, 76.7 MiB/query, and 559 MiB RSS;
- the strongest local 9.99M Deep Image result reached 997,708 ppm aggregate,
  800,000 ppm minimum, and 11.47 ms p99, but did not execute its page stage;
- the strongest exact S3 result reached 320/320 hits at 100K rows with 16
  pages, 2,928,808 returned bytes, and 144.07 ms cold p99;
- V32 at 1M showed 302/320 hits with 16 pages and 320/320 with 64 pages, using
  3.1 versus 12.5 MiB/query and roughly 85--180 ms median;
- the 1M rank-four lower-tail shape diagnostic improved 1,277/1,280 centroid
  owner hits to 1,280/1,280 on a burned cohort, but has no fresh high-D result;
- exact global ADC over the historical width-12 PQ plane reproduced the routed
  result, so that generation was representation-limited rather than router-
  limited;
- the 1M Cohere-768 write cell sustained 46,656 records/s drain-inclusive with
  66 ms write p95, two PUTs and zero GETs per acknowledgement, but did not
  establish distributed compaction or mixed-load behavior.

The repeated failure is candidate starvation combined with lossy coarse
ordering and poor physical locality. Tiny 256-primary leaves create 390,625
leaves at 100M, make boundary ownership brittle, and provide too few samples
for useful covariance estimates. Merely increasing shape complexity cannot
repair an insufficient unique row set. Conversely, larger replicated postings
can exceed byte, write-amplification, or construction budgets. V36 therefore
tests posting geometry, replication, scoring, coarse representation, and fine
layout as separate causal layers instead of assuming any one is the solution.

## Frozen corpus and leakage boundary

The scale-transfer source is
[`andropar/relaion2b-natural-embeddings`](https://huggingface.co/datasets/andropar/relaion2b-natural-embeddings)
at revision `bfc7465dcf1245bd605d35dcaf5d2177bbc2025a`: 514,367,913
physical rows, 2,298 Parquet objects, 787,439,811,692 bytes, and ordered
manifest SHA-256
`76ac61cf2821a331419ad40d5eb94d2cdafccccf39af17af1d328b7a2f0bc6c7`.
Its exact membership, duplicate-ID rule, query roles, schemas, and construction
limits are frozen in `docs/research/v36-funnel-dataset-authority.json`.

The source produces nested 1M/10M/100M 768D corpora after removing query rows.
Development, validation, and holdout contain 1,000 disjoint queries each;
performance contains 10,000. Performance vectors are excluded from corpus and
bound for timing but do not receive GT. Exact GT@100 for the 3,000 quality
queries uses binary64 accumulation in source
dimension with no fused multiply-add and orders by
`(distance,unsigned_feature_row_id)`, matching the frozen dataset authority.
`feature_row_id` maps explicitly to `source_ordinal`, its zero-based position in
the materialized source Parquet order. Duplicate feature IDs keep the first
`(shard_path,row_offset)` occurrence. Construction reservoirs, Lloyd
reductions, and training ties use `source_ordinal`. After primary assignment,
`dense_ordinal` means the separate primary-posting storage position used by
coarse heaps and the fine interval directory. The two names are never aliases.

Before opening the 1M holdout, preregister the finite 10M candidate set and its
deterministic development-only selection rule. The 10M development role alone
selects K, code width, and payload layout; 10M validation may reject but cannot
retune, and the previously unopened 1M holdout then measures scale transfer of
that frozen policy. Because query identities are shared across scales, neither
the 10M result nor the 1M holdout is a second fresh cohort. Holdout rejection
cannot trigger another arm. Reused historical query sets are diagnostic only.
Construction receives no query or truth capability; serving receives no
source-corpus or list-S3 capability.

The Task 0 v1 registry's `performance-gt100` output and query-major
`block_queries=8` GT checkpoint are planned, unmaterialized contracts and are not executable at
100M within the 12-hour cell cap. Before materialization, a breaking v2
authority explicitly removes `performance-gt100` from the output set, replaces
the checkpoint, and binds the complete executed-screen query-exclusion set
while preserving the source and four full-source query-role identities: the
3,000 quality query vectors are
loaded once (about 9 MiB), each source row block is read once, distance work
proceeds in successive eight-query tiles, and the checkpoint binds the
completed source-row prefix plus every quality query's canonical top-101 heap.
The 10,000 performance vectors remain separately authenticated. Resume starts at
the first incomplete source-row block. Source membership, query identities,
distance arithmetic, and GT ordering remain unchanged.

Every object-sample or full-source freeze has at most three Spot attempts.
Population progress commits only after a complete authenticated input object.
The checkpoint stores one immutable Arrow IPC identity run per object with
non-null `feature_row_id:u64` and `row_offset:u64`; its canonical JSON manifest
binds the consecutive ranked-object prefix, object ordinal, physical/distinct/
duplicate counters, and every run identity. A 300-active-second timer republishes
the newest complete boundary but never represents a partial object as durable.
Therefore publication cadence is bounded to 300 seconds while maximum lost work
is one complete object.

The executable identity-run format is `borsuk-v36-prefix-identity-run-v3` and
has no legacy reader. Rows are strictly increasing by unsigned
`feature_row_id`, not by physical row offset. Construction externally sorts by
`(feature_row_id,selected_object_ordinal,row_offset)`, retains the minimum
physical pair for each ID, anti-joins earlier committed feature-ID-ordered runs,
and emits the surviving ID with its winning object ordinal and row offset.
This makes within-object deduplication, cross-object anti-join, and resume a
bounded k-way merge without a second ordering pass. Physical location remains
materialization evidence only; it never decides population-score order.
Canonical Arrow IPC fixes schema metadata, non-null columns, 65,536-row batch
boundaries, no compression or dictionary encoding, and deterministic writer
options. The manifest binds the format, unsigned comparator, minimum-physical
winner rule, selected-object window, per-run row count, and both artifact
digests. Old row-offset-ordered experimental runs are rejected.

GT scheduling still uses successive eight-query tiles inside one source scan,
but the durable unit is a source-row block only after that block has updated all
3,000 quality-query heaps. The checkpoint stores the next source ordinal and a
strict Arrow IPC snapshot of canonical `(role,query_ordinal,rank,feature_row_id,
squared_distance)` top-100 rows. It is published after a complete source block
when 300 active seconds have elapsed; a partially updated block is discarded.
This preserves one corpus read rather than performing 375 query-major reads.

Checkpoint dependencies use immutable campaign-scoped content keys so complete
Parquet outputs are uploaded once and reused across replacement attempts.
One run-scoped newest pointer is the sole checkpoint head; attempt-scoped
terminals retain producer provenance. The next attempt's execution authority
binds the exact pointer length/SHA-256, generation, and manifest identity before
launch. Publish all
immutable dependencies, then the immutable checkpoint manifest, then replace
the pointer using `If-None-Match` for generation zero or `If-Match` against the
previous pointer ETag. Resume accepts only the newest fully authenticated
pointer, manifest, and dependencies; it never falls back past a corrupt newest
generation. Rust revalidates SHA-256, BLAKE3, Arrow schemas, the consecutive
slots of the registered object window, and every recomputed counter before network acquisition. The
sidecar starts at checked `generation + 1`; it never infers a generation from a
directory listing. It reconstructs bounded sorted-run cursors or GT heaps and starts at the first
incomplete object or source-row block. A population checkpoint carries
identities rather than embeddings, so a resumed population scan does not replay
completed objects, while final Parquet materialization reacquires exactly the
authenticated selected window. It never downloads the full registered corpus.
The controller confirms the prior instance is terminated before binding and
launching a replacement. A third interruption is terminal
infrastructure failure; it cannot silently buy a fourth cell.

The object-sample input authority exists before execution and binds the full
2,298-object registry count, 787,439,811,692-byte total, ordered-manifest
SHA-256, source revision, caps, policies, and role seeds. It contains no
consumed-object claim. The post-freeze population receipt is created only after
all sixteen ranked objects authenticate, and records every completed object
even when it contributes no surviving distinct ID. It also binds
the input authority, registry, source archive, executable, source commit,
physical/distinct/duplicate counters, all sixteen consumed identities, the
population cutoff score/feature-ID pair, the immutable 1,100,000-row selected-ID
artifact, pre/post-exclusion counts, and every output artifact. Population
checkpoint schemas contain no physical cutoff. They advance once per complete
object, followed by an explicit selected-population checkpoint; materialized and
ground-truth checkpoints bind that exact selection without recomputation drift. Exact
output artifacts are uploaded before a terminal marker can be
published. Terminal states are closed: `complete`,
`screen-source-insufficient`, `infrastructure`, and `interrupted`; only
infrastructure or interruption may retry.
Before launch, a reduced-shape execution of the exact GT kernel with the
production thread count must project completion below half the registered
active wall cap by scaling measured time with the exact row-times-query ratio;
otherwise launch is forbidden. The authority fixes corpus row groups at 65,536
rows, and each complete row group is an interruption boundary. At the first
complete row group after at least 300 active seconds since the preceding
publication, the freezer publishes every
query's canonical top-101 heap plus the exact next source ordinal,
source/query artifact identities, and arithmetic authority. Resume
authenticates that state before scanning the suffix. More than 900 seconds
without a completed row group or checkpoint is an infrastructure stop.
Uninterrupted and resumed executions must produce identical neighbour IDs and
binary64 distance bits.
The object-sample campaign rejects a Spot rate above $3/hour or a $90 total
campaign projection. The cumulative active-time budget is therefore at most 30
hours across all attempts at the admitted rate; before every attempt, its
effective wall cap is the smaller of 43,200 seconds and the remaining dollar
budget divided by that rate, multiplied by 3,600 seconds/hour. Before a full-source cell, the materialization authority
must add and validate exact per-hour and per-attempt cost ceilings from the
then-current Spot and S3 plan; absence of either ceiling forbids launch.

Bulk cross-language data is strict Arrow IPC or Parquet with exact physical
schema, concrete types, nullability, row count, ordinal semantics, and byte
identity. Small manifests and receipts are canonical JSON with one trailing
newline. Publication binds SHA-256; independently fetched objects additionally
bind BLAKE3. ETag is never treated as a digest.

The bounded screen does not take a path prefix. It orders the 2,298 registered
complete source objects by
`(SHA-256("borsuk-v36-screen-object-v1" || utf8(path) || u64le(length)),path)`
and authenticates all of the first 16 objects. The frozen registry proves those
objects total 5,485,265,954 bytes, below the 6-GiB cap. It validates every row,
resolves duplicate feature IDs by the minimum
`(selected_object_ordinal,row_offset)`, then ranks the remaining identities by
`(SHA-256(SHA-256(utf8("borsuk-v36-prefix-screen-population-row-v2")) ||
ordered_source_manifest_sha256_bytes || u64le(feature_row_id)),feature_row_id)`.
The first 1,100,000 identities form the diagnostic candidate population. This
uses all sixteen query-independently sampled objects instead of a contiguous
physical prefix of roughly five objects. Fewer than 1,100,000 distinct valid
IDs after all sixteen complete objects is terminal source insufficiency.
The cohort-A v2 authority binds `cohort_ordinal=0`,
`selected_object_start=0`, `selected_object_count=16`, the exact selected byte
total, the upstream dataset-authority SHA-256, and no exclusion artifact. A
cohort-B authority binds ordinal 1, start 16, count 16, its distinct exact byte
total, and cohort A's selected-ID artifact. These fields are concrete and
required; there is no inferred cohort or default. Source and identity-run rows
retain the global ranked object ordinal (0--15 for A, 16--31 for B); checkpoint
completion is a window-relative count. Cohort B remains execution-disabled until
the authenticated cohort-A selected-ID payload is loaded and applied during the
external merge. A descriptor alone is not exclusion evidence.

Screen role seeds use population-specific SHA-256 labels under
`borsuk-v36-prefix-screen-{role}-query-v2`. After removing all 13,000 query
identities, corpus rows use the distinct label
`borsuk-v36-prefix-screen-corpus-v2`; they never reuse the full-source corpus
score. All four query artifacts from every executed screen cohort are mandatory
exclusion dependencies of future full-source role selection: their union is
removed before any full-source development, validation, sealed-holdout, or
performance ranking. The full-source authority binds the complete ordered set
of executed-cohort artifact identities and proves zero overlap; omission of
cohort B, when executed, is an authority failure. Different seeds alone are not
treated as separation.

The screen reports
GT@10 exact/near-duplicate rates, nearest-neighbour distance quantiles, and the
centered spectrum energy retained at M192. These diagnostics are repeated on
the globally selected 1M population; a material shift is
`population-indeterminate`, not an architecture rejection. Exact duplicates
have binary64 distance zero; near duplicates have squared L2 at most `1e-6`.
Compare duplicate rates in ppm, nearest-neighbour squared-L2 p10/p50/p90, and
retained-energy ppm. A shift is material when either duplicate rate changes by
more than 20,000 ppm, retained energy changes by more than 20,000 ppm, or a
nonzero distance quantile changes by more than 10%; a zero/nonzero quantile
transition is always material.

The 1M screen is a fail-fast cohort diagnostic, not a 100M performance proxy.
For each geometry it additionally reports the route-containment curve for
target posting occupancies 64, 82, 128, 256, 512, 1,024, 2,048, 4,096, and
8,192 rows under the unchanged 14-object wave and reports actual unique
admitted rows and fractions at every point. The curve, including occupancy 82,
is diagnostic and never a pass/fail or family-rejection gate: 82 only
approximates the nominal 0.115% corpus fraction of fourteen 8,192-row postings
at 100M and does not preserve clustering, replication, scoring, or scale
equivalence. For each query, also report GT@1 containment, exact squared-L2
`gap_100_101 = d101 - d100`, squared-L2 reconstruction error of the true
rank-100 source vector, and absolute coarse-score errors
`abs(d_hat(q,x)-d(q,x))` for the true rank-100 and rank-101 rows. Zero-gap
cases are reported by separate `zero_gap_count` and error summaries; strictly
positive gaps alone contribute finite boundary-score-error/gap quantiles.
Neither vector reconstruction error nor boundary-score error is a certified
rank-error bound. Exact truth therefore retains
rank 101 internally even though the published neighbour contract remains
GT@100. A screen pass advances projection,
geometry, and score families; it does not freeze K, code width, payload layout,
or generic production defaults. Those remain open through measured 10M replay.
A failure is `registered-cohort-failed`; it rejects a family only after the
same causal stage fails on cohort B, or after a mathematically stronger bound
proves the stage impossible. Cohort-A failure followed by cohort-B pass is the
terminal `cohort-discordant` outcome: it neither advances nor rejects, and no
cohort C is permitted. Cohort-B source insufficiency or infrastructure failure
also leaves the family indeterminate. Cohort B has a separately registered
three-attempt, $90 campaign cap. Cohort B uses zero-based ranked source objects
16 through 31
(5,483,342,562 bytes), binds cohort A's exact selected-ID artifact, excludes
every overlapping logical ID before its own row ranking, and is therefore
object- and ID-disjoint without observing A's quality outcomes. It never alone
labels the generic architecture infeasible.

## Qualification hypothesis

### Projection and construction

The object-sample screen compares the existing deterministic SRHT projection,
seed 36, with a bounded-memory **centered** principal subspace, both from source
dimension to `M=192`. The centered arm subtracts the corpus-only binary64 mean,
forms the exact 768-by-768 binary64 covariance of the registered geometry
reservoir, and uses the lockfile-pinned `nalgebra 0.33` single-threaded
`SymmetricEigen::try_new(f64::EPSILON, 30 * source_dimensions)` implementation.
Exhausting that exact iteration cap is a terminal numerical failure. It accepts the
decomposition only when every eigenpair has relative residual at most `1e-10`
and the reconstructed covariance has relative Frobenius error at most `1e-10`.
It orders eigenpairs by descending
eigenvalue then original coordinate ordinal, fixes every eigenvector sign by
making its largest-absolute component (ties by coordinate ordinal) positive,
and reports retained centered second-moment energy at dimensions 64/96/128/192.
Failure to converge is a terminal numerical failure, not an architectural
rejection. Both projections are registered before outcomes. Screen
development ranks projection/geometry/score families by the global
lexicographic rule; screen validation does not try an unregistered rescue and
sealed holdout opens once only after validation passes. A surviving family is
replayed at 10M, where K, code width, and payload layout are first frozen.
A family that fails one screen cohort is recorded for the disjoint-cohort
confirmation rule rather than relabelled as a generic Funnel-3 rejection. The
source-f32 metric
remains the truth authority. A deterministic corpus-only
reservoir and fixed reductions train
`min(4,096,next_power_of_two(ceil(rows/262,144)))` super-cell centroids: 4 at
1M, 64 at 10M, and 512 at 100M. Postings are trained within bounded external
super-cell runs. The super cells are a construction partition only and never
replace the exhaustive posting-score authority. Exact assignment against all
posting centroids is required at 1M and 10M so construction approximation
cannot contaminate the geometry result. At 100M, construction routing may be
introduced only after parity with the frozen exact assignment is measured.

The construction reservoir contains the

`min(rows,1,048,576)`

smallest `(SHA-256("borsuk-v36-geometry-reservoir-v1" ||
little_endian(source_ordinal)),source_ordinal)` rows. Super-cell Lloyd runs exactly 25
iterations; local posting Lloyd runs exactly 10. Initialization is deterministic
farthest-first: the first centroid is the smallest reservoir source ordinal, each
next centroid is the not-yet-selected row that maximizes distance to its nearest
retained centroid, and ties use row ordinal. Assignment ties use centroid ordinal. Sums accumulate binary64 in
increasing row-ordinal order and centroid components round once to binary32.
Empty centroids are repaired in increasing centroid-ordinal order. Each takes
the row farthest from its assigned centroid among rows whose current centroid
owns more than one row, breaking ties by source ordinal; that row is removed
from its previous accumulation before recomputation.

Let `P=ceil(rows/B)` and `R` be the number of non-empty super-cell runs. A
checked precondition rejects `P<R`. Give every run one posting, then apportion
the remaining `P-R` postings by Hamilton allocation over run primary-row
counts: run `i` first receives `floor((P-R)*rows_i/rows)` additional postings,
and leftovers go by decreasing integer remainder then super-cell ordinal.
Checked integer arithmetic and a final `sum(postings_i)==P` assertion are
mandatory. This lower-bounded apportionment, including the skewed
`[999997,1,1,1]` case, and every local Lloyd artifact are serialized before the
exact global primary and closure assignment.

Posting size `B` always means target **primary rows**, not stored assignments.
The exact posting counts are:

| Primary target | 1M | 10M | 100M |
|---|---:|---:|---:|
| 4,096 | 245 | 2,442 | 24,415 |
| 8,192 | 123 | 1,221 | 12,208 |

The historical 256-primary single and double-assignment geometries remain
controls. Candidate geometries use `B in {4,096,8,192}` with either single
assignment or closure replication at `epsilon in {0.05,0.15,0.30}`. There are
ten geometry arms total.

For projected row `x`, primary posting `c0` minimizes squared L2. A posting
centroid `c` is closure-eligible when
`d2(x,c) <= (1+epsilon)^2 * d2(x,c0)`. The primary is retained first. Eligible
centroids are visited by increasing `(distance,posting_ordinal)`. A candidate
replica is rejected when an already retained centroid is closer to the
candidate centroid than `x` is. At most eight total owners are retained.
Construction records mean/p50/p95/p99/max replication, primary and stored
occupancy, distance work, spill work, and exact deterministic reduction
evidence.

An arm is rejected before query evaluation when mean replication exceeds 3,
primary p99 exceeds `2B`, primary maximum exceeds `4B`, stored-assignment p99
exceeds `6B`, or stored-assignment maximum exceeds `8B`. These are gates, not
promised clustering properties. Exact global assignment at 100M would cost
roughly 468.768T MACs for B4096 or 234.394T for B8192 before training, so every
later construction method requires a reduced-shape throughput/cost preflight
and a 12-hour active-cell cap.

The breaking `borsuk-v36-funnel-manifest-v2` embeds the complete
`V36ProjectionArm` rather than a seed alone. The prefix-screen manifest also
binds geometry, posting score, coarse code, fine codec, chunk ceiling, and K;
there is no partial winner that full-source qualification could reinterpret.

### Posting score and traversal

Exhaustive scoring of every posting is the representation authority at 1M and
10M. Every integer prefix through `min(posting_count,1,024)` is evaluated;
`{1,2,4,8,16,32,64,128,256,512,1,024}` is the reported summary.

For B4096/B8192 only, compare five posting scores under the same geometry and
equal 4,736-byte resident summary slot:

1. centroid squared-L2;
2. diagonal Gaussian lower-tail heuristic;
3. rank-two Gaussian lower-tail heuristic;
4. rank-four Gaussian lower-tail heuristic.
5. six-total-prototype squared-L2, consisting of the posting centroid plus five
   deterministic sub-prototypes.

The six-total-prototype arm stores exactly six f32[192] vectors, including the
posting centroid, for 4,608 raw bytes; the remaining 128 bytes in the equal
slot cover population and fixed metadata. It does not mean one centroid plus
six additional f32 vectors. Sub-prototypes use deterministic k-means++ seeded
with ChaCha8 seed 36: the first row is the smallest source ordinal, subsequent
rows are sampled from the source-ordinal-ordered binary64 cumulative squared-
distance distribution using the next canonical u64 draw mapped to `[0,total)`,
and a zero total chooses the smallest unused ordinal. The active sub-prototype
count is `min(5,primary_rows)`; remaining vector slots repeat the posting
centroid bit-for-bit and are excluded from the minimum. They then run ten fixed
Lloyd iterations over unique primary rows with binary64 ordinal-ordered
reductions and `(distance,prototype_ordinal)` ties. Its query
score is `min_j ||q-c_j||^2`. This is the equal-byte non-Gaussian control that
the covariance arms must beat on fresh queries.

These are query-ranking heuristics, not certified enclosing ellipsoids. The
diagonal/rank-two/rank-four arms use the V33/V34 Gaussian distance-moment
algorithm generalized from 96 to 192 dimensions. For unique primary rows in
increasing ordinal order, compute the population mean and covariance with
binary64 sums and divisor `n`. Decompose the complete symmetric covariance,
sort eigenpairs by decreasing eigenvalue then original index, canonicalize
repeated eigenspaces by projected coordinate axes, and choose the sign whose
greatest-absolute component is positive (lowest dimension breaks ties).
Negative eigenvalues below `-max(trace*1e-12,1e-15)` reject the arm; smaller
negative values clamp to zero. After rounding retained eigenvectors/eigenvalues
to binary32, residual diagonal is
`max(0,cov[i,i]-sum_j(lambda[j]*v[j,i]^2))`; a negative residual below
`-max(abs(cov[i,i])*1e-5,1e-7)` rejects the arm.

For rank `r in {0,2,4}`, define `C=diag(residual)+sum_j lambda[j] v[j]v[j]^T`,
`delta=q-mean`, and
`score=delta^T delta+trace(C)-sqrt(2*ln(n))*sqrt(max(0,2*trace(C^2)+4*delta^T C delta))`.
Rank zero is the diagonal arm; centroid scoring is only `delta^T delta`.
All scoring uses fixed dimension/component order in binary64 and
`(score,posting_ordinal)` ties. The persisted maximum arm contains population,
mean f32[192], residual f32[192], four f32 eigenvalues and four f32[192]
directions (4,628 raw bytes); every arm is charged the equal 4,736-byte slot.
The receipt reports used bytes as well as the equal-byte slot. A fresh-query
pass decides whether shape helps; burned V33 results are context only.

A resident HNSW over posting summaries is mandatory after geometry and score
freeze whenever posting count exceeds 1,024. Its topology is built by centroid squared-L2; query traversal
orders candidates by the frozen selected posting score. It uses `M=32`,
`efConstruction=200`, seed 36, and
`efSearch=max(L,e)` for `e in {64,128,256,512,1,024}`. It must reproduce the
exhaustive **selected scorer's** prefix at 999,000 ppm traversal parity. HNSW
is an accelerator, never the representation authority. G2 must pass this
traversal parity at the 10M posting count and project graph bytes plus measured
summary-score work to the exact 100M posting count before G4 can start; an
exhaustive 100M summary scan cannot satisfy the decoded-hot gate by assertion.

### Coarse records and unique-candidate admission

Each immutable-base coarse replica carries both a dense logical row ordinal and
its u64 source feature ID. Unique primary order defines the ordinal; no resident
100M-row physical-locator or ID table is allowed. Every scanned replica counts
toward CPU and transferred bytes, but duplicate or non-live rows are filtered
**during** bounded heap admission. Capacity therefore means K unique live
source IDs, not K assignment records.
Deterministic coarse replacement and prefix order are
`(distance,dense_ordinal)`. Final fine results map ordinals back to their bound
source feature IDs and use `(source_f32_distance,unsigned_feature_row_id)` for
the exact control, or `(codec_distance,unsigned_feature_row_id)` for the serving
codec. Recall compares the returned source feature IDs with the frozen GT IDs.

Coarse evidence runs in this causal order:

1. projected-f32 scoring over the routed rows, diagnostic only;
2. a 44-byte sign control: 24 sign bytes, one f32 residual norm, one u64 dense
   ordinal, and one u64 source feature ID. A zero residual has norm zero and
   all-zero bits. Otherwise bit `i` is one iff residual component `i >= 0`;
   reconstruction component is `+/- norm/sqrt(192)`, and the score is binary64
   squared-L2 from the projected query residual to that reconstruction;
3. residual PQ4-32: 64 three-dimensional subquantizers, 16 centroids each,
   32 code bytes plus ordinal and source ID = 48 bytes;
4. residual PQ4-48: 96 two-dimensional subquantizers, 16 centroids each,
   48 code bytes plus ordinal and source ID = 64 bytes.

Every assignment is encoded relative to the centroid of the **owner posting
whose coarse object contains it**. A replicated row therefore has a distinct
sign/PQ code in each owner and needs no primary-centroid lookup at query time.
The projected-f32 checkpoint still represents the same source row exactly and
deduplicates by source ID. Each arm has one global PQ codebook trained from all
retained assignments of the registered corpus-only reservoir, ordered by
`(source_ordinal,owner_posting_ordinal)`. Each residual-PQ subspace initializes
its 16 centroids by the construction farthest-first rule, runs exactly 20 Lloyd
iterations, accumulates binary64 in increasing source-ordinal order, rounds
centroids once to binary32, and uses codeword-ordinal ties. Coarse K is
`{512,1,024,1,536,2,048}` unique rows. Projected-f32
separates projection/routing loss from coarse-code loss; source-f32 on the
identical final row set separates fine-codec loss.

### Physical S3 layout and fine rerank

The falsifier uses one shared, unreplicated fine plane. Primary dense ordinals
are assigned in `(primary_posting,local_cluster,local_order,source_ordinal)`
order. Within each primary posting, projected rows are partitioned with exactly
`min(primary_rows,ceil(B/64))` centroids initialized by the construction
farthest-first rule and refined by five Lloyd iterations in the selected
f32[192] projection space using the same binary64 reductions and empty-centroid
repair. Rows assigned to each centroid are ordered by
`(distance_to_centroid,source_ordinal)`, split into consecutive blocks of at
most 64, then each block is reordered by an exact nearest-neighbour chain:
smallest source ordinal starts, the nearest unvisited projected row follows,
and ties use source ordinal. Blocks and chains follow centroid ordinal. This
costs at most `7*N*ceil(B/64)*192 + N*64*192` distance-component operations for
`N` primary rows after the occupancy gate: one farthest-first pass per local
centroid, five Lloyd assignments, one final assignment, and chains of at most
64 rows. Measured work and the registered
preflight cap are serialized. These corpus-only local chains make rows that are
likely to be reranked together physically adjacent. Each coarse replica still carries the explicit
dense ordinal and u64 source feature ID: object position is not a global row
identity under replication, fragmentation, or updates. A compact resident
interval directory maps ordinal ranges to complete fine objects, avoiding a
per-row locator table.

Fine layouts are constructed without query access and compare complete,
independently authenticated Arrow IPC objects whose **encoded** ceilings are
64, 256, or 512 KiB. Microclusters are construction units, not request units:
they are greedily co-packed in the frozen local order, and query-time ranged
reads or query-specific repacking are forbidden. An object contains one
non-null fixed-size-list vector column, one non-null u64 dense ordinal, and one
non-null u64 source feature ID. Candidate rows vote for their already
materialized object using a frozen rank-weighted reduction. For zero-based
candidate rank `r` in a heap of `K` unique rows, the object's vote mass adds
`K-r`; integer accumulation is checked, and the object's best rank is the
minimum contributing rank. Complete physical objects, not logical micro-pages,
are ordered by `(negative_vote_mass,best_candidate_rank,object_ordinal)` and admitted until
the normal 14-GET/7-MiB wave is full, while the retry-inclusive hard limit
remains 16 GETs/8 MiB. No arbitrary row-range is treated as authenticated.
Metadata, retries, full encoded bytes, decoded capacity, and encoded-plus-
decoded overlap are charged. The causal oracle reports useful candidate rows,
distinct selected objects, fetched rows, candidate-to-object scattering, and
truth containment at every complete-object boundary. If 998,000 ppm post-I/O
containment is unreachable inside 14 complete GETs and 7 MiB, the layout is
rejected; increasing GETs or bytes is not a rescue.

The 512-KiB screen starts from these conservative SQ8 packing targets. They
include `D+16` raw bytes per row and two f32 quantization parameters per source
dimension, but the builder must still serialize Arrow IPC and reduce the row
count until the **complete encoded object** is at most 512 KiB.

| Source D | Target rows/object | Raw row+quantization bytes | Rows in 14 objects |
|---:|---:|---:|---:|
| 384 | 1,280 | 515,072 | 17,920 |
| 768 | 640 | 507,904 | 8,960 |
| 1,536 | 320 | 508,928 | 4,480 |
| 3,072 | 160 | 518,656 | 2,240 |

These counts prove only arithmetic capacity. The offline oracle must prove that
the K unique candidates and truth rows concentrate into the admitted complete
objects. A layout whose candidates occupy more than 14 useful objects fails
even when their aggregate vector payload is below 7 MiB.

Serving fine codecs are SQ8 and f16, selected independently. Each complete SQ8
Arrow object is one non-null row containing non-null
`codes:List<FixedSizeList<u8,D>>`, `dense_ordinals:List<u64>`,
`source_feature_ids:List<u64>`, `scale:FixedSizeList<f32,D>`, and
`offset:FixedSizeList<f32,D>`; all list children are non-null, the three row
lists have equal registered length, and scale/offset are stored once per
object. Each f16/source-f32 object uses the same one-row nested representation
without scale/offset and with `f16`/`f32` vector children. Thus SQ8 stores D
code bytes per row plus exactly two f32 parameters per dimension per object;
there is no repeated per-row quantization metadata or hidden side object. The
offline source-f32 control reranks the exact same admitted rows. Selection and
layout freeze precede codec evaluation.

Coarse objects are also complete authenticated Arrow IPC files. Each object is
one posting fragment; its manifest, rather than a repeated per-row column,
binds `posting_ordinal:u32`. Its strict columns are non-null
`dense_ordinal:u64`, `source_feature_id:u64`, and the arm's fixed-size-binary
code. Rows are ordered by `dense_ordinal` and greedily fragmented without
exceeding 512 KiB encoded. The resident posting directory binds every fragment's URI,
SHA-256, BLAKE3, length, first ordinal, and row count. The planner visits
postings by `(posting_score,posting_ordinal)` and admits **all** fragments of a
posting atomically only if their combined GETs/bytes fit. At the first posting
that does not fit, it records that posting and the remaining suffix as excluded
and stops; it never skips a dense posting to admit lower-ranked work. Partial
postings are never scored. The
projected-f32 diagnostic scores the exact rows admitted by a candidate code
layout using offline projected rows; it does not introduce f32 serving objects.

## Causal gates and selection

The serialized checkpoints are:

1. exhaustive route-owner containment;
2. selected-scorer containment;
3. graph traversal parity when posting count exceeds 1,024;
4. projected-f32 unique-candidate containment;
5. lossy coarse-code unique-candidate containment;
6. post-I/O admitted-row containment;
7. fine-codec recall and same-row source-f32 recall.

Each checkpoint freezes row ordinals before the next stage. Later stages cannot
change an earlier prefix. Report duplicate scans, unique rows, useful/fetched
bytes, GETs, excluded objects, candidate-to-object scattering, and unreachable
frontiers.

Screen development ranks complete registered families by the global
lexicographic rule. Screen validation evaluates only the leading family;
rejection cannot advance to another development survivor. Sealed screen
holdout opens once only after validation passes. The screen records the leading
projection, geometry, and score, but K, coarse-code width, and object layout
remain diagnostic. Before any 1M holdout opens, G2 preregisters their finite
10M candidate set and lexicographic development-only selection rule. The 10M
development role performs their sole freeze; neither 10M validation nor the 1M
scale-transfer holdout can rescue or retune it.

Development requires 998,000 ppm route, selected-scorer, projected-f32,
coarse-code, and post-I/O containment; fine-codec loss versus same-row f32 is at
most 1,000 ppm. End-to-end development requires 997,000 ppm aggregate recall@10
and 800,000 ppm minimum. Holdout requires 995,000 ppm aggregate and 800,000 ppm
minimum. Per-query p1 is reported; minimum remains a gate because recall@10 is
quantized in 100,000-ppm steps.

The normal planner admits at most 14 complete-object GETs and 7 MiB of payload
in the coarse wave and the same in the fine wave. The hard envelope, including
retries and response metadata, remains 16 GETs and 8 MiB per wave. Widening it
is not a rescue. If retries exceed the reserve, the query and complete
measurement repetition are classified `transport-indeterminate`; they are not
scored as zero recall and cannot select or retune an arm. A campaign may run at
most three separately registered replacement repetitions of the already-frozen
arm before blocking on infrastructure. Selection is lexicographic: pass every authority,
quality, construction and transport gate; minimize total returned bytes, GETs,
distinct-query CPU p99, measured resident bytes, then arm ID.

At 10M replay only the frozen winner and registered diagnostic prefixes. Sweep
the common coarse-wave byte ceiling at every distinct cumulative complete-
object boundary from zero through 7 MiB while retaining the 14-GET normal cap.
`C*(N)` is the smallest such byte ceiling reaching 998,000 ppm aggregate
containment. Posting count `L*`, returned bytes, and GETs per query at that
ceiling are reported but are not substituted for `C*`. With seed 36, 10,000
paired query resamples, and the nearest-rank one-sided 99% quantile, recompute
the aggregate frontiers and
`Chat100=C10*C10/C1`; undefined or unreachable frontiers are infinite failures.
The 99% upper bound of `Chat100` must fit the 7-MiB normal payload budget, and
the 100M projection using measured occupancy and fragment packing must fit 14
normal GETs. This byte frontier avoids treating a two-versus-three-posting integer
jump as a continuous scaling signal. It quantifies query-sampling uncertainty,
not certainty of the 100M scaling model.

## Latency, throughput, and memory contracts

Cache protocols remain distinct:

- decoded-hot compute p99 after 1,024 warmups and 10,000 measurements/query;
- distinct-query shared-cache latency and QPS over 10,000 performance queries;
- cold S3 latency with actual GET/byte/retry counters;
- an NVMe-warm arm only if separately registered, never relabelled as RAM-hot.

Only decoded-hot has a 15 ms p99 gate. Cold S3 has no impossible 15 ms claim.
The 3,000-QPS concurrency-64 gate applies only to the decoded-hot microbenchmark;
distinct-query QPS is reported until a production gate is justified by data.
Queueing from the fixed workspace pool is included in latency.

Every executable measurement must remain below 2 GiB RSS at 1M/10M, and every
100M projection and measurement below 3 GiB. The checked ledger separately
charges projection, every scale's registered super centroids, posting summaries, HNSW if present,
posting/object and fine-interval directories, liveness, active and retiring
generations, decoded cache, encoded-plus-decoded overlap, fixed query
workspaces, runtime, delta coarse records, CSR, compaction old/new arenas, and
retry buffers. Shared `Arc` buffers count once; allocations still reachable
after eviction count until released.

The initial fixed caps are projection 3 MiB, super centroids 4 MiB, posting
summaries 128 MiB, all directories 128 MiB, liveness 64 MiB, decoded cache
256 MiB, sixteen 32-MiB query workspaces, and runtime/headroom 512 MiB. A
workspace must stream object decoding and cannot retain more than its 32-MiB
cap. These
are admission caps, not achieved measurements. Delta coarse plus CSR has a
512-MiB byte cap and recent fine rows have a separate 128-MiB cap; row admission derives from the selected encoded record size
and measured replication rather than a fixed four-million-row promise.

Before physical Arrow directories exist, the checked ledger uses a
non-borrowable lower bound: 80 bytes per complete object identity, 16 bytes per
posting entry, and 16 bytes per fine ordinal interval. Minimum coarse and fine
object counts are the raw projected payload divided upward by the applicable
complete-object ceiling. The materialized directory must report at least this
floor, and its greater measured capacity replaces the floor in resident
admission. The 80-byte object entry is two binary 32-byte digests plus an exact
u64 encoded length and u64 object ordinal; URI keys are derived from the
authenticated generation prefix rather than held as resident strings.

At 100M, raw remote coarse storage before Arrow envelopes is 4.4 GB for the
44-byte sign control, 4.8 GB for PQ4-32, or 6.4 GB for PQ4-48 at single
assignment; multiply by measured mean replication. Mean replication 2--3 gives
8.8--13.2, 9.6--14.4, or 12.8--19.2 GB respectively; cap-eight worst cases are
35.2, 38.4, or 51.2 GB. Fine SQ8 is approximately 39/78/154/308 GB at
384/768/1536/3072D plus metadata; f16 is approximately twice that. S3 capacity
is acceptable, but none of these planes may become resident per-row state.

## Write and compaction qualification

The selected generation is immutable. One process-wide active mutation buffer
seals every 250 ms or at a checked byte limit. Conditional `HEAD` publication,
latest-wins visibility, insert/replace/delete semantics, and outcome-blind
receipts are mandatory. Pending and sealed rows use the selected projection,
coarse representation, replication policy, and fine codec.

Query nodes ingest published coarse slices into a bounded resident delta arena
and posting-to-delta CSR. Admission is byte-based and fails closed before the
3-GiB process bound. A byte-bounded resident mutation overlay contains only IDs
changed since the immutable base, with their latest sequence, live/tombstone
state, and current delta ordinal. A base candidate is admitted only when its
source ID is absent from this overlay. A delta candidate additionally carries
u64 sequence and is admitted only when it equals the overlay's latest live
entry. Thus stale or deleted base rows cannot consume K. Overlay allocation,
hash-table slack, rebuild overlap, and lookup CPU are measured and charged;
there is no resident map for unchanged base IDs. Newly published fine rows are
also ingested into a byte-bounded 128-MiB recent-fine arena, so visibility does
not create query-time microsegment GETs. A continuous coalescer flushes every
five seconds or 64 MiB, whichever comes first, grouping rows by owner posting
and rewriting them once into the selected 64/256/512-KiB fine layout. It must
finish before the recent arena reaches 128 MiB or ingestion stops. Publication
switches atomically from resident rows to the coalesced chunk directory; the
same row is never charged to both query sources. Coalesced delta fine objects
consume the same fine-wave budget as base objects. Initial durable
microsegment bytes, coalesced bytes, retries, and later rewrites all count
toward write amplification. Short G5 compactions may coalesce delta
routing/coarse state and tombstones while sharing already coalesced immutable
fine chunks. They do not prove long-lived base absorption or split/reassignment.
That production lifecycle requires a later sustained rebalancing gate.

Saturation uses offered rates 5k/10k/20k/40k mutations/s for 60 seconds each
and must reach 20k with zero rejects and visibility p99 <=1 s. The mixed cell
restores the frozen base, warms for two minutes, then measures ten minutes at
5k accepted mutations/s, eight writers, and 64 readers with a fixed 70/20/10
insert/replace/delete mix. Read p99 must be <=110% of the no-write baseline;
complete S3 write amplification, including scratch/retry/compaction bytes, is
<=3.0. Exact blocked source-f32 GT over live IDs at every registered snapshot
must meet the holdout recall gates and never return a deleted ID.

## Execution funnel and decision

1. G-1 runs the separately registered bounded 1M sixteen-object row-hash screen on
   same-region Spot/NVMe. It compares SRHT versus the centered principal
   subspace, all five registered posting scores, closure geometry, coarse
   codes, and the actual complete fine-object packer. It stops before the
   787-GB freeze unless at least one family reaches every causal containment
   gate inside the physical
   14-GET/7-MiB envelope. It advances one projection/geometry/score family
   after development, validation, and one sealed holdout. K, code width, and
   layout remain unfrozen; passing is diagnostic only. One-cohort failure is
   confirmed on a disjoint registered cohort before family rejection.
2. G0 proves schemas, deterministic arithmetic, capability isolation, bounded
   unique heaps, transport accounting, scalar/SIMD parity, and resource stops.
3. G1 materializes the globally selected 1M corpus/GT on same-region Spot,
   first proves zero overlap with every screen query identity, then replays the
   advanced projection/geometry/score family across development, validation,
   and holdout. Population shift is classified before interpreting a failure.
4. G2 replays that family at 10M, freezes K/code-width/layout exactly once, and
   applies frontier-growth, quality, construction, RSS, and transport gates
   before any production format work.
5. G3 builds the minimal executable S3 query path at 1M/10M and proves offline
   equality plus honest hot/shared-cache/cold measurements.
6. G4 runs three terminal 100M/768D query repetitions on Spot only if G2/G3
   pass unchanged.
7. G5 qualifies saturation and mixed read/write behavior; a later sustained
   gate qualifies base absorption and rebalancing.

If geometry fails route containment, reject or replace the geometry. If the
selected score fails while centroid passes, reject shape complexity. If
projected-f32 fails, neither wider PQ nor fine rerank may rescue it. If lossy
coarse coding fails against projected-f32, replace the code only. If post-I/O
containment fails, reject the physical layout or byte budget. If a fine codec
loses more than 1,000 ppm, reject that codec independently. If 10M frontier
growth fails, reject Funnel-3 before 100M. Passing the 768D path qualifies only
that dimension band; 384/1536/3072 require their own real-data qualification.
