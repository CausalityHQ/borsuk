# V33 shape-aware rank-fine/fetch-coarse routing design

## Status and decision

V32 proves that Deep Image 1M can return exact top ten with 64 page reads, but
the known perfect-containment route scans 425,822..503,541 PQ rows and projects
76..89 bounded code-object GETs per query. The existing 16-page arm returns only
302/320 exact neighbors in the paired S3 run. Building a new selective reader
around that arm would optimize a quality point already known to fail.

V33 therefore qualifies routing geometry before production storage. Fine leaf
summaries rank candidate ranges; bounded storage groups remain the fetched unit.
The first arms estimate a leaf's minimum member distance from residual moments,
then test small unions of prototypes. Enclosing-sphere distance is a diagnostic
control, not the default: in high dimension large radii can rank diffuse cells
first. Diagonal and low-rank ellipsoid scores are ranking heuristics unless
accompanied by a valid Euclidean lower bound; they never authorize exclusion by
themselves. A triangle is simply three prototypes and has no privileged meaning
in 96 dimensions. Hyperbolic or general quadratic routing is excluded from the
first ladder because it changes the metric, costs more to train and score, and
has no current Euclidean-recall authority.

This is a pre-release format. A winning arm replaces the experimental V32
routing layout; no legacy reader, alias, or migration layer is retained.

## Causal boundaries

The experiment keeps four failures separate:

1. leaf/group routing may omit a truth owner's fetched group;
2. PQ candidate retention may omit a truth row after its group was selected;
3. the page reducer may fail to select a page containing a retained truth row;
4. S3 fetching or exact reranking may fail after the correct identities were
   selected.

Shape scores address only the first boundary. They do not compress vectors,
improve PQ distances, or repair page layout by implication. Each receipt reports
truth-owner group rank, selected population, retained-candidate membership,
truth-page rank, and final exact result separately.

## Cheapest metadata-only falsifier

Before reading a corpus vector, first reuse only authenticated routing metadata,
178-group membership, 128 exposed query vectors, and 1,280 recorded truth-owner
identities for the group-centroid/prototype screen. The subsequent leaf-moment
screen additionally authenticates and reads the two PQ codebooks, base code
plane, fidelity plane, and high code plane. It decodes PQ residuals but reads no
exact vector or page body. Freeze every query-independent proxy summary before
loading the query/truth roles:

- per-leaf mean squared residual radius;
- per-leaf diagonal residual variance;
- up to three representatives per storage group, bounded by its parent count,
  built from population-weighted
  parent centroids by deterministic farthest-first initialization and ten Lloyd
  iterations.

Compare centroid control, the fixed analytic moment estimate, diagonal moment
estimate, and group three-prototype minimum distance. Rank groups and admit the
longest prefix whose next complete group remains within 131,072 rows, capped at
64 groups. Also report a same-byte
smaller-plain-leaf control when available, so prototype gains are not mislabeled
as geometry when they are only extra resolution. For every arm emit the complete
truth-owner rank CDF and cumulative selected rows. The proxy passes to fresh
data only when one arm contains 1,280/1,280 owners and all 128 queries within
131,072 rows, a fourfold target against the 524,288-row control ceiling. A single
miss or worse median/p95/max frontier rejects that arm with no beam, seed, or
formula retuning. This is burned explanatory evidence, not a release result.

The first frozen group-level proxy rejected both the weighted-mean control and
three-parent prototype arm. The next proxy therefore operates at routing-leaf
resolution. It reconstructs each logical row from exactly one authenticated
24-byte base or 48-byte high residual code, selected by the fidelity plane, and
adds the code-parent centroid in f32. It does not normalize the reconstructed
row. For every routing leaf, process logical rows and dimensions in ordinal
order, accumulate an f64 population mean and then a second-pass population
diagonal variance, and round each persisted summary value once to f32. Empty
code parents have no summary. Query and truth capabilities remain closed until
all summaries and their SHA-256 identities are frozen.

For a stored leaf mean `mu`, population `n`, query delta `u=q-mu`, squared
distance `D=sum(u_j^2)`, trace `m=sum(v_j)`, and `a=sqrt(2*ln(n))` (`a=0` for
`n=1`), freeze these raw signed scores without clamping:

- scalar moment:
  `D + m - a*sqrt(2*m*m/96 + 4*m*D/96)`;
- diagonal moment:
  `D + m - a*sqrt(2*sum(v_j*v_j) + 4*sum(u_j*u_j*v_j))`.

The variance term deliberately includes both Gaussian components,
`2*trace(Sigma^2)` and `4*u^T*Sigma*u`; dropping the first would ignore radial
distance variance. There is no beta, epsilon, clamp, per-leaf calibration, or
exposed-query fit. Negative scores remain ordered evidence rather than being
collapsed into artificial zero ties. These are Gaussian moment-closure ranking
heuristics, not Euclidean lower bounds or exact distance distributions. A
storage group's score is the minimum score of its
member routing leaves; groups are ordered by `(score,group_ordinal)` and the
first overflowing complete group ends admission without skipping. The matched
fine-leaf centroid control uses `D` under the identical reduction. All f64
reductions retain ordinal order, reject nonfinite intermediates, canonicalize
signed zero, and use ordinal ties.

Representation gains require equal-byte resolution controls. Shared population
and group metadata are excluded identically. Scalar summaries cost388 bytes per
leaf (96 f32 mean values plus one f32 moment); across4,141 leaves, their16,564
extra bytes fund43 additional384-byte plain centers plus52 padding bytes. Split
the43 largest nonsingleton leaves, ties leaf ordinal. Diagonal summaries cost768
bytes per leaf and are compared with exactly two plain centers per leaf. A split
chooses the maximum-variance coordinate, ties dimension, orders rows by
`(coordinate,logical_ordinal)`, cuts after `floor(n/2)`, and stores each child
mean. Singleton diagonal controls duplicate their sole center. Child slots
inherit the original storage group and require no child directory.

The frozen ladder contains: the historical group mean and three-prototype
evidence; the raw authenticated routing-leaf centroid; reconstructed leaf mean;
scalar moment; diagonal moment; scalar 43-split and diagonal two-center
equal-byte controls. Two report-only diagnostics are the reconstructed-member
sphere bound and a full reconstructed-row group oracle. The oracle ranks a
group by its minimum reconstructed member distance and attributes misses that
no summary of the same reconstruction can reliably explain; it is not an arm
and cannot pass the program. A moment arm can make a shape-efficiency claim only
if it beats both its reconstructed-mean baseline and its matched-byte resolution
control. The sphere diagnostic stores
`R=max_i(||xhat_i-mu||)` and scores
`max(0,||q-mu||-R)^2`; it is a lower bound only for reconstructed members.

The proxy authenticates the exact URI, digest, length, role, and dependency
binding of all five PQ artifacts from the frozen build manifest and reports
positive code reads, unlike the preceding metadata-only prototype run. No
summary construction or query scoring may begin until those five identities are
registered; an absent identity is an authority failure, not permission to infer
one from a local file or object ETag. Tests
cover both widths, fidelity replacement/ranks, code-parent reconstruction,
padding, exhaustive logical coverage, singleton and constant leaves,
isotropic scalar/diagonal agreement, directional anisotropy, negative scores,
group-min reduction, duplicate truth-owner groups, exact byte accounting, and
byte-identical rebuilding. Before f32 persistence, the diagonal identity
`sum(v_j)=m` must differ by at most `1e-12*max(1,m)` under the ordered f64
reduction. Count all ten truth identities even when several map
to one group. Report owner ranks and cumulative rows through each query's last
required group in addition to budgeted selection. Passing requires1,280/1,280
owners,128/128 perfect queries, non-worse p50/p95/max frontier against the
fine-leaf centroid control, and the existing row/group bounds. A
shape-efficiency claim additionally requires dominance over its equal-byte
control. Report each miss against the reconstructed-row oracle so failure of
the approximate population is not mislabeled as failure of the summary shape.

## Query-independent construction

Fine summaries belong to immutable routing leaves; fetched objects remain frozen
bounded groups derived from V32 code-parent populations. Every group is
root-local, contains complete code parents, has 1..8,192 rows, and has one
immutable ordinal. The builder streams source vectors once in logical order and
updates all arms simultaneously. No query, ground-truth row, prior query trace,
or page selection enters construction.

The frozen arm ladder is:

- `centroid`: one f32 mean, the existing control;
- `moment`: centroid distance plus mean residual energy and a frozen global
  extreme-value correction estimating the closest member;
- `diagonal-moment`: the moment score with query-direction residual variance;
- `sphere`: one f32 mean plus the maximum finite Euclidean radius;
- `prototype-2` and `prototype-4`: deterministic farthest-first seeds followed
  by a fixed number of assignment/update passes, ties by logical ordinal;
- `diagonal`: f32 mean and diagonal variance, scored as a ranking feature;
- `low-rank-4`: f32 mean, four deterministic principal directions and four
  scales, scored as a ranking feature.
- `projected-interval-16`: one fixed orthonormal projection shared by all groups
  and 16 enclosing min/max intervals per group. The sum of squared distances to
  those intervals is a conservative projected-space bound after decoded
  quantization-error inflation.

Persist summaries as uncompressed Arrow IPC with exact non-null physical
schemas and SHA-256 authority. Corpus/query/truth inputs remain Parquet or Arrow;
JSON is restricted to small canonical manifests and receipts. Prototype storage
uses f16 only after an f32-vs-f16 differential gate proves identical selected
groups on development. Int8 is a later arm only if f16 passes quality and memory
but misses a declared serving-memory target.

For a query `q`, the moment arms estimate minimum query-to-member distance from
centroid distance, residual energy, population and directional variance. Any
calibration is one global scalar fitted on TRAIN only, never a query-fitted or
per-leaf parameter. Prototype score is the minimum squared-L2 distance to the
prototypes. Sphere score is `max(0, ||q-c||-r)^2`, a valid lower bound to members
only when `r` is the authenticated maximum radius. Moment, diagonal and low-rank
scores rank groups but cannot certify pruning. All reductions use finite f32
inputs, f64 accumulation in dimension order, and `(score, ordinal)` ties.

## Scale arithmetic

At roughly 4.14 routing leaves per thousand rows, one f16 residual-moment scalar
adds about 0.8 MB at 100M and 8.3 MB at 1B. A 96-value int8 diagonal plus scale
adds about 41 MB and 414 MB. These leaf-level values must be admitted against the
complete directory and refresh overlap, not in isolation.

The observed 178 groups per one million rows projects to about 17,800 groups at
100M and 178,000 at 1B if the distribution is stable; hard validation uses the
actual group count. At 178,000 groups:

- sphere mean plus radius is about 34.9 MB at 196 bytes/group;
- two f16 prototypes are about 68.4 MB at 384 bytes/group;
- three f16 prototypes are about 102.5 MB at 576 bytes/group;
- four f16 prototypes are about 136.7 MB at 768 bytes/group;
- f16 mean plus diagonal scale is about 68.4 MB at 384 bytes/group;
- f16 mean plus four f16 directions/scales is about 172.3 MB at roughly
  968 bytes/group.

Those figures exclude Arrow envelopes, offsets, the directory, pages, caches,
active queries, and refresh overlap. The checked complete-process projection
must remain below 3 GiB. The corresponding full group-score work at 1B is about
17.1M dimension terms for one center, 34.2M for two prototypes, and 68.4M for
four. Actual CPU and SIMD throughput are measured; these counts are not latency
claims.

## Fail-fast experiment

Use about 2,000 fresh source-distribution queries that do not overlap any prior
V32 cohort or near-duplicate source group. Hash the query and exact GT@10
artifacts before construction. Split into TRAIN 800, development 600 and sealed
holdout 600. TRAIN fits only global calibration. Development chooses at most one
non-control arm and one fixed row budget from `262144, 131072, 65536`. The
holdout remains sealed until the choice, format, scorer, memory projection, and
gates are committed.

Before any real S3 read, run three stages:

1. **Oracle gap:** using exact truth only for diagnosis, compute the minimum leaf,
   group and row population needed to contain all ten owners. Stop if the layout
   itself cannot meet the target.
2. **Shape route:** score every resident summary and fetch the groups containing
   the frozen leaf prefix. Require 1,000,000-ppm aggregate and minimum truth-owner
   containment, no more than 131,072 rows, no more than 64 objects, and no more
   than 8 MiB encoded code payload for every query. This is an improvement gate
   against the 11.22..12.92 MB useful-code control, not a claim about envelopes.
3. **Resident PQ/page replay:** read only the selected code objects, keep 12,288
   candidates, and report page budgets 16, 32, and 64 without choosing among
   them on holdout. A deployable arm must return exact Recall@10 of 1,000,000 ppm
   aggregate and minimum while reducing bytes or requests relative to the
   measured V32 64-page baseline.

In parallel, replay fixed logical page sizes `480, 240, 120, 60` against the same
candidate order and page reducer. This isolates page-byte savings from routing;
no page size may be chosen from sealed results.

The development screen fails immediately on the first truth-owner miss or bound
violation. Only one surviving arm reaches the sealed holdout. Holdout requires
6,000/6,000 exact neighbors and 600/600 perfect queries. A holdout miss
rejects the arm; there is no beam increase, alternate seed, threshold change, or
second holdout attempt.

## S3 and write architecture after a pass

Group summaries and directories are resident. A query fetches only selected
immutable group code objects, followed by selected vector pages. It never reads
the whole code plane or corpus. Object requests are issued in bounded async
waves, preserve deterministic order, and authenticate length and SHA-256 before
decode. Real S3 latency is measured honestly; no 15 ms cold-S3 requirement is
claimed.

Writes use immutable delta segments. The hot path appends Arrow/Parquet pages and
PQ code objects, builds small per-segment group summaries, and publishes one
manifest atomically. Queries search a bounded number of segments; background
compaction retrains or merges summaries query-independently. This avoids random
updates to a billion-row global tree and keeps write throughput separate from
read qualification.

LSH/random projections remain a candidate delta-segment router because online
assignment is cheap, but they are not mixed into this ladder. Their bucket
duplication, multiprobe count, code/page locality, and recall need a separate
fixed experiment after the group-shape result.

## Stop and release gates

Do not implement the V32 selective production reader until one preregistered
shape proxy passes its explanatory gate and one corpus-derived routing arm has
passed the sealed group, row, code-byte, page, exact-recall, CPU, and complete
memory gates. A rejected proxy remains evidence and advances only to an already
registered distinct representation; it never authorizes parameter retuning.
Do not run 100M or 1B until the fresh 1M holdout passes. Use
`AWS_PROFILE=causality` and Spot for the bounded corpus construction/replay,
preserve immutable terminal evidence, terminate compute immediately, and never
adapt from repeated holdout queries. Product comparisons remain unclaimed until
a frozen architecture is reproduced at scale.
