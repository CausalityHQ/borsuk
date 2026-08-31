# V23 Leaf-to-Page Incidence Falsifier

**Status:** Approved architectural falsifier. This design authorizes a local,
claim-ineligible builder and evaluator. It does not authorize a persistent
production format, paid compute, D3, or a publication claim.

## Evidence and decision

The authenticated V23 page oracle proves that the existing eight-page layout
can cover the target neighborhood: it reached 993,750 ppm aggregate recall and
900,000 ppm minimum-query recall. The failures are in page selection, not page
capacity.

Two later diagnostics isolate the selection failure:

- thirty-two spherical means per page reached only 725,000 ppm aggregate,
  100,000 ppm minimum-query, and 729,559 ppm oracle attainment;
- an exact scan of all 9,990,000 width-12 BVS3 rows reached exactly the same
  671,875 / 100,000 / 676,100 ppm as the routed selector, while a per-page
  minimum-distance reducer was worse at 568,750 / 0 / 572,327 ppm.

The 320-cell probe cap is therefore not causal. Fixed page summaries lose
multimodal neighborhoods, and the row-PQ ordering plus reciprocal-rank
maximum-cover reducer loses page-level evidence even when every selector row is
scanned. The next falsifier must remove both mechanisms.

The selected design is a query-independent 65,536-leaf inverted incidence
router. It learns fine geometric leaves from corpus vectors, aggregates each
leaf's mass directly into page postings, and ranks pages without ranking rows.
It combines a balanced spherical partition tree, an inverted-index heavy-hitter
layout, and reciprocal-rank evidence fusion. Ground truth is never used in
training, posting construction, or score definition.

## Alternatives considered

### Fixed SimHash incidence

A fixed hyperplane hash could map rows to buckets without training, then map
buckets to pages. It is cheaper to build, but a failure would be
underdetermined: bit count, table count, radius, and multi-probe schedule create
a large tuning garden. It is retained only as a possible future positive
screen, not the next decisive falsifier.

### Distributional page sketches

Random-projection envelopes, quantiles, or kernel means could summarize each
page in 64--128 values. They are compact, but they again compress every page
independently. A negative result would repeat the information-loss ambiguity of
the K=32 page prototype experiment.

### Page graph

A graph over page signatures can reduce query work, but it adds an entry and
traversal recall failure before the page score itself is known to work. A graph
is permitted only after a direct incidence score passes quality and CPU gates.

## Immutable dataset authority

The experiment uses Deep Image 10M materialization
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/datasets/deep-image-96/attempts/0001/materialized/`
and the authenticated V23 D2 page corpus rooted at source commit
`c339a546f8f9370cb2e6e9fb3b0fd4bdefa3cb05`, source-archive SHA-256
`77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d`,
and index ID `index-bcda7bb66812e162d45077e6`. The page attempt prefix is
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/0000/attempts/0001/`.

Dataset evaluation objects are fixed before implementation:

- `test.parquet`: 3,843,448 bytes, SHA-256
  `296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4`,
  exactly 10,000 non-null `emb: FixedSizeList<element: Float32, 96>` rows;
- `neighbors.parquet`: 4,003,585 bytes, SHA-256
  `d305fcea7387988941defd2942cca1673693271329f977ba073da888cac3de8d`,
  exactly 10,000 non-null
  `neighbors_id: FixedSizeList<element: Int32, 100>` rows;
- `meta.json`: 93 bytes, SHA-256
  `80e25c7ee4d7eb4e3ae77dc5e96598122ea84557448d2cd067790319c8fbd220`,
  exactly `{dim: 96, k: 100, metric: cosine, n_test: 10000,
  n_train: 9990000, name: deep-image-96}`.

The builder additionally authenticates every ordered training shard and every
D2 page reference/body through one separately frozen manifest. The manifest
must bind URI, length, digest, ordinal range, physical schema, generation,
metric, dimensions, row counts, primary/replica roles, and the exact page
namespace. No latest-object discovery is allowed.

Before scientific execution, a credentialed bootstrap process on a fresh,
phase-dedicated disposable worker consumes only that frozen manifest. It
resolves each exact
S3 URI and registered generation/version, verifies the registered byte length
and SHA-256 or BLAKE3 digest while streaming, and writes the object beneath a
role-specific staging directory. It emits a canonical staging receipt binding
the source manifest digest, ordered object identities, relative paths, and
observed digests. Discovery by prefix, ETag-as-digest, latest-object selection,
and implicit retry after a terminal are forbidden. The scientific processes
have no credentials or object-store client and accept only the staged bytes
plus the exact staging receipt.

The raw training representation is frozen before staging: every ordered shard
must have a registered URI, generation, byte length, digest, inclusive source
ordinal range, row count, and exact Parquet physical schema. The logical row is
one non-null `emb: FixedSizeList<element: Float32, 96>` vector; outer or child
nullability, field-name, dimension, dtype, row-count, range, shard-order, or
materialization drift is an authority failure. Arrow is the typed in-memory
boundary and Parquet is the durable table boundary; page payloads retain their
registered binary codec and are never translated through JSON.

## Leakage boundary

The pipeline is split across fresh disposable workers with disjoint staged
inputs. A worker is assigned exactly one scientific phase and one attempt. Its
credentialed parent downloads only that phase's frozen manifest and registered
objects, proves the complete on-disk inventory, creates empty scratch/output
directories, and then starts the scientific child in fresh network and PID
namespaces with loopback down. The child receives a minimal environment with no
AWS variables or credential paths. It can use the worker's ordinary read-only
OS runtime and source tree because no sealed later-phase scientific bytes are
present on that worker; it cannot fetch missing bytes after network isolation.
The parent never exposes an object-store client to Rust.

This boundary deliberately has no private root filesystem, dynamic-loader
discovery, runtime-library allowlist, bind-mounted loader, or `pivot_root`.
`ldd` and equivalent dependency discovery are forbidden. The authoritative
capability is the complete phase-input inventory plus absence of every
later-phase role on a newly created worker, not an emulated operating system.
Startup probes must observe a different network-namespace inode, fail a network
canary, open every registered phase input, write the registered output
directory, and confirm that the staged inventory contains exactly the manifest
paths and no forbidden role. The receipt binds the executable digest, source
commit/archive, instance and AMI identities, network namespace inode, complete
staging receipt digest, ordered inventory, and probe results. Any unexpected
regular file in a scientific input directory, any forbidden role, or inability
to create the offline child emits `authority-stop`.

The launcher never emits one argument per shard/page. Training receives one
shard-directory path plus its ordered manifest and staging receipt. Posting
construction receives one page-directory path plus its roster, ordered
manifest, and staging receipt. Rust derives every relative object path from the
authenticated manifest, rejects absolute paths, `..`, symlinks, duplicate
paths, missing or unexpected files, and authenticates each body before semantic
use. The phase argv therefore remains bounded independently of corpus size and
page count.

1. **Tree training** may read only the exact dataset/construction manifest and
   raw training shards. It cannot open page labels, page bodies, queries,
   neighbors, or prior results. It emits a content-addressed tree and training
   receipt.
2. **Posting construction** may read only the sealed tree/receipt, page roster,
   and page bodies. It cannot open raw query, neighbor, or prior-result bytes.
   One authenticated page stream emits contributions for both the one-leaf and
   two-leaf arms before releasing each decoded page; a second scientific page
   pass is forbidden. It emits content-addressed one- and two-leaf posting
   artifacts plus a router completion receipt.
3. **Development evaluation** may open only the sealed router, the
   authenticated D2 report, and query ordinals 0--31. The D2 report supplies
   their already-authenticated neighbor-to-page truth. These ordinals are
   already burned by earlier V23 work. They may choose one preregistered ladder
   cell but can never support a claim.
4. **Holdout binding** begins only after the router and chosen cell are sealed.
   It authenticates all 12,800 ordered neighbor IDs in rows 32--159, binds the
   ordered first ten per row as the unchanged recall@10 authority, streams the
   immutable pages independently, and maps those 1,280 evaluated IDs to their
   primary and replica pages. The remaining 11,520 IDs remain schema/order
   authority and cannot be substituted into scoring. The phase emits an
   authenticated truth artifact without changing the router.
5. **Holdout evaluation** runs exactly once on query ordinals 32--159. No
   parameter, score, layout, threshold, or kernel change is permitted after any
   holdout metric is visible.

The orchestrator does not create or stage the posting worker until the
tree-training worker has sealed and uploaded its tree receipt, then terminates
the training instance. It does not create or stage a development worker with
query or neighbor objects until posting construction has sealed the router
bytes, construction manifest, binary digest, and immutable object
generation/version identities into a completion receipt. The subsequent
development worker contains only ordinals 0--31. The holdout worker is created
only after that development receipt seals the chosen cell. Receipt ordering is
a digest chain, not filesystem time: each
receipt embeds the exact parent receipt digest, and every consumer rehashes all
parents and content-addressed objects. K32 outputs and the earlier 32-query
global-ADC result are never accepted as labels or builder inputs.

Independent preregistered training or development cells may run concurrently
only on separate disposable workers with disjoint attempt prefixes and scratch.
They never share mutable files or a process namespace. Holdout remains one
sealed cell on one fresh worker and is never parallelized across tunable cells.

## Corpus-only leaf training

Training reads the 9,990,000 raw f32 corpus rows, not page replicas and not PQ
codes. It retains a deterministic 2,097,152-row reservoir selected by the
smallest `(SplitMix64(source_ordinal xor seed), source_ordinal)` keys. The fixed
seed is the little-endian u64 in the first eight bytes of
`SHA-256(source_archive_digest_raw_32_bytes ||
"borsuk-v23-leaf-page-v1")`; the digest bytes are decoded from the registered
lowercase hexadecimal authority, and the resulting seed is recorded verbatim
in the manifest.

The reservoir is converted once to normalized f16 vectors. A balanced
spherical binary tree of depth 16 creates exactly 65,536 leaves:

1. the first seed is the lowest source ordinal in the node;
2. the second seed is the row farthest from the first, with source ordinal as
   the tie break;
3. four spherical two-means Lloyd iterations use f64 accumulators and
   `(distance, source_ordinal)` ties;
4. after the last iteration, both child centroids are normalized, rounded to
   f16, decoded to f32, and assigned exact post-roundtrip inverse norms;
5. for a normalized row `x`, the authoritative split score is
   `dot(x, decoded_child_1) * inverse_norm_1 -
   dot(x, decoded_child_0) * inverse_norm_0`;
6. rows order by `(score.total_cmp, source_ordinal)` and split at the median,
   so child sizes differ by at most one. The node stores the boundary score
   bits and boundary source ordinal. A later row takes child 0 exactly when its
   `(score.total_cmp, source_ordinal)` is not greater than that boundary;
7. authoritative sums consume rows in increasing source ordinal, form f64
   partials over exactly 4,096 rows, and merge partials through an
   index-ordered binary tree with zero-valued right padding. f16 conversion is
   IEEE round-to-nearest, ties-to-even;
8. empty, non-finite, zero-norm, duplicate-ordinal, and wrong-dimensional
   states are errors, never repairs.

The score's dot products use one exact f32 kernel: eight lanes start at positive
zero, each lane consumes its twelve dimensions in increasing order through
IEEE f32 fused multiply-add, and lanes sum from zero through seven with f32
addition. Every internal node stores its normalized-then-f16-rounded centroid,
post-roundtrip inverse norm, boundary-score f32 bits, and boundary source
ordinal. No affine split-vector representation is stored or accepted. Every
leaf stores the same centroid/inverse-norm pair, u32 population, and f32 mean
squared residual. Training is deterministic across thread counts; a scalar
reference and the optimized kernel must emit byte-identical tree topology and
centroid f16 bits. Scheduler completion order cannot affect a sum.

Farthest-seed scans consume exactly 3,221,225,472 distance dimensions, the four
Lloyd passes consume 25,769,803,776, and final post-f16 child-score
repartitioning consumes 6,442,450,944. The registered training ceiling is
therefore 35,433,480,192 distance dimensions, excluding explicitly measured
normalization and sort work.

The tree is a build-time assignment accelerator. Serving scores all leaf
centroids directly, so tree traversal cannot hide query-time leaf recall.

## Leaf-to-page postings

A second ordered stream authenticates every D2 page body. Primary and replica
rows are normalized and assigned through the frozen tree. The one-assignment
arm applies the stored score/boundary comparator at every node. The
two-assignment arm starts at the root, expands both children of each retained
node, computes cosine distance with each child's stored f16 centroid and
post-roundtrip inverse norm, and retains the best two
`(distance.total_cmp, child node ordinal)` values at every depth. Its outputs
are the two beam-selected leaves, not a claim about the globally nearest two
of 65,536. Each physical page assignment contributes one count to
`(leaf, page)`; replicas therefore retain the redundancy that makes the
eight-page oracle attainable. The canonical decimal record ID must parse to
the source ordinal used in split ties. The builder never sees neighbor IDs.

Posting construction is an external partition/sort, not an unbounded hash
map. The one- and two-leaf arms have separate partition sets. Each contribution
writes one fixed eight-byte `(u16 leaf, u32 page, u16 reserved-zero)` record
into the partition selected by the leaf's high eight bits. Across both arms,
the 10M build writes at most 55,860,333 records consuming 446,882,664 scratch
bytes. Each partition is sorted into at most 64-MiB runs by
`(leaf, page)`, the unsorted partition is unlinked after all runs are durable,
and a bounded k-way merge computes counts. Because records arrive ordered by
leaf after merge, only one leaf's 2,048-entry top-count heap and totals are
resident. Runs are consumed and unlinked in order; scratch is bounded by twice
the combined record plane plus 128 MiB of sort/merge buffers. The training reservoir is
unlinked before this phase. Final postings are streamed directly to their
content-addressed output rather than accumulated in RAM.

Across both arms, posting assignment evaluates at most 94 child-centroid
scores per physical page row: 62 for the width-two beam and 32
for the authoritative one-path split comparisons. For 18,620,111 physical
page rows and 96 dimensions, the ceiling is 168,027,881,664 distance
dimensions. Combined with training, the complete ceiling is
203,461,361,856 distance dimensions.

Before each phase acquires its remaining full inputs, its exact scalar/SIMD
distance kernel, page decoder/hash path, or external sorter runs the applicable
fixed preflight over 65,536 training vectors, 256 registered page bodies, or
1,048,576 contribution records. The posting preflight decodes each of its 256
pages once and produces both arms from that one decode. The end-to-end wall
projection is the sum of distance work, authenticated input bytes, and
55,860,333 sort records, each divided by 80% of its observed throughput. It
must not exceed 5,400 seconds; otherwise the current phase emits
`resource-stop` before its complete stream. The two-hour cap remains an
independent hard stop.

For each leaf, exact counts are sorted by `(count descending, page ordinal
ascending)`. The builder emits the first 2,048 pages and authenticates prefix
views at 512, 1,024, and 2,048 pages. A posting is stored structure-of-arrays as
one u32 page ordinal and one u16 normalized mass:

`mass = round_half_even(65535 * count / leaf_assignment_count)`.

Normalization uses the full pre-truncation leaf count, so discarded mass is
not redistributed. Every leaf records total assignments, retained assignments,
retained-mass ppm, and the total-variation error between exact retained masses
and their u16 representation,
`0.5 * sum_retained(abs(count / leaf_assignment_count - mass / 65535))`.
Zero-mass postings are omitted. A cap/assignment arm is ineligible if any
evaluated leaf retains less than 995,000 ppm, exceeds 5,000 ppm quantization
total variation, or differs from the scalar reference in arithmetic, ordering,
uniqueness, or u16 conversion.

## Query score and selection

Queries are normalized once. The SIMD and scalar kernels compute cosine
distance to all 65,536 f16 leaf centroids using each stored post-roundtrip
inverse norm, and rank leaves by
`(distance, leaf ordinal)`. For a probe count `P`, the page score is the fixed
reciprocal-rank posterior:

`score(page) = sum_{rank=0}^{P-1}
(mass_u16(leaf_rank, page) / 65535) / (rank + 1)`.

The optimized kernel uses
`reciprocal_q32[rank] = round_half_even(2^32 / (rank + 1))` and accumulates
`u16_mass * reciprocal_q32` in u64. Its selected pages must equal an f64 scalar implementation. It
uses structure-of-arrays u64 scores and u32 epochs plus a touched-page list;
clearing or scanning an unbounded map per query is forbidden. Exactly eight
unique pages are selected by `(score descending, page ordinal ascending)`.

Development evaluates this fixed lexicographic ladder for each cap in
512, 1,024, then 2,048 order:

1. one tree-assigned leaf per row with P=32, 64, then 128;
2. the bounded two-leaf tree beam per row with P=32, 64, then 128.

For the two-leaf arm, a physical page assignment contributes once to each of
the row's two beam-selected leaves; each leaf normalizes its own page
distribution.
The first development cell passing every gate is sealed. Thus smaller posting
planes dominate before assignment count and probe count. If no cell passes,
the architecture is rejected and holdout evaluation is forbidden. Holdout runs
only the sealed cell.

## Gates and causal outcomes

Every development cell and the one holdout cell select exactly eight pages and
must satisfy:

- aggregate recall at least 975,000 ppm;
- minimum-query recall at least 800,000 ppm;
- attainment of the exact page-coverage oracle at least 995,000 ppm;
- projected 100M serving bytes at most 3 GiB;
- at most 262,144 posting visits per query for P=128 and 2,048 postings per
  leaf;
- at most 8,192 distinct touched pages per query;
- native warm p99 at most 15,000,000 ns;
- finite, deterministic output and identical scalar/SIMD selected pages.

The quality oracle is recomputed from the independently bound primary and
replica pages of the ten shipped ground-truth neighbors. A query with an
unmapped neighbor, fewer than eight selected pages, or a zero-hit oracle is an
authority failure rather than a low score.

Before router scoring, the holdout page layout itself must reach at least
985,000 ppm aggregate oracle recall and 900,000 ppm minimum-query oracle
recall, matching the original layout-viability gates. Failure is
`holdout-layout-rejected` and says nothing about the incidence router.

Every attempted cell records quality, retained mass, projected RAM, posting
visits, touched pages, determinism, and latency independently. Campaign
classification uses this fixed precedence:

1. Any byte/schema/digest/capability/cross-object error emits
   `authority-stop`; no scientific result is opened.
2. Build RSS, scratch, progress, wall, or failed throughput preflight emits
   `resource-stop`.
3. Scalar/SIMD, thread-count, rounding, ordering, or overflow disagreement
   emits `determinism-stop`.
4. If no cap/assignment arm meets both retained-mass and quantization-error
   gates in every leaf, emit `incidence-retention-rejected`. An ineligible arm
   contributes no probe cells.
5. Evaluate eligible cells in the registered order and seal the first cell
   passing quality, projected RAM, posting visits, touched pages, and p99. If
   none passes, emit `incidence-quality-rejected` when no cell passed quality;
   otherwise emit `incidence-budget-rejected` when no quality-passing cell also
   passed every structural budget; otherwise emit `incidence-kernel-rejected`.
6. Before holdout router scoring, a page oracle below its own viability gates
   emits `holdout-layout-rejected`.
7. On holdout, a quality miss emits `incidence-generalization-rejected`, a
   structural-budget miss emits `incidence-holdout-budget-rejected`, and a p99
   miss emits `incidence-holdout-kernel-rejected`.
8. Only an unchanged holdout cell passing every gate emits
   `incidence-falsifier-passed`.

A pass authorizes production-format design and a fresh D1/D2 qualification
with a larger frozen query cohort. It does not authorize D3 or a better-than-SOTA
claim.

## Build-resource bounds

The training phase admits at most these resident allocations:

| Training allocation | Bytes |
| --- | ---: |
| 2,097,152 x 96 f16 reservoir | 402,653,184 |
| reservoir hash keys and source ordinals | 33,554,432 |
| two u32 membership/index planes | 16,777,216 |
| node accumulators and deterministic reduction workspace | 134,217,728 |
| stream/decode buffers | 134,217,728 |
| allocator and implementation headroom | 268,435,456 |
| **Training RSS ceiling** | **989,855,744** |

After the tree is sealed, the reservoir, keys, and index planes are unlinked or
released before posting construction. Posting construction admits 128 MiB of
sort/merge memory, 64 MiB of tree state, 16 MiB of page buffers, 64 MiB of
output buffers, and 256 MiB of headroom: 553,648,128 bytes. Its external-sort
disk ceiling is 1,027,983,056 bytes. Both phases remain below the independent
2-GiB RSS stop, and scratch is checked against a separately preflighted 2-GiB
free-space floor before input acquisition.

## Serving-memory projection

The conservative 100M projection uses
`ceil(28,282 * 100,000,000 / 9,990,000) = 283,104` pages and the largest
2,048-page posting cap:

| Component | Bytes |
| --- | ---: |
| 65,536 x 96 f16 leaf centroids | 12,582,912 |
| leaf offsets (65,537 u32) | 262,148 |
| leaf populations, residuals, and inverse norms | 786,432 |
| capped page/mass postings (65,536 x 2,048 x 6) | 805,306,368 |
| u64 page-score plus u32 epoch planes | 3,397,248 |
| 262,144-entry u32 touched-page list/top-eight workspace | 1,048,576 |
| projected immutable page references | 90,593,280 |
| two maximum page waves | 3,932,160 |
| conservative runtime reserve | 536,870,912 |
| conservative implementation headroom | 268,435,456 |
| **Total** | **1,723,215,492** |

The total is about 1.605 GiB, leaving more than 1.3 GiB below the registered
3-GiB ceiling. Build-only tree nodes, training reservoir, exact count maps, and
truth-binding state are excluded from serving RAM but must remain below a
separate 2-GiB local RSS stop.

## Native kernel boundary

The custom query kernel has two independently tested stages:

1. a blocked f16-centroid cosine kernel computes 65,536 distances and retains
   the best 128 `(distance, leaf)` pairs;
2. a structure-of-arrays posting kernel multiplies u16 mass by the fixed
   reciprocal-rank table, epoch-accumulates at most 262,144 postings, and selects
   the top eight touched pages.

The scalar implementation is authoritative. Random finite vectors, exact ties,
subnormals, all-zero posting lists, maximum posting lists, duplicate pages,
u64 overflow attempts, and thread-count changes must mutation-lock equality.
Non-finite inputs and any score overflow fail closed. CPU evidence is measured
with the complete resident production representation, one pinned worker, a
fixed warm-up, at least 10,000 timed queries, and raw per-query nanoseconds.

## Artifacts and safety

The builder emits a content-addressed tree, posting plane, exact build report,
and terminal receipt. The evaluator emits one canonical result per cell and a
separate terminal receipt. Every artifact binds source, dataset, construction
manifest, query cohort, algorithm constants, tree and posting digests, role
lengths, scalar/SIMD evidence, memory projection, raw latency digest, and all
recomputed metrics.

Partial files are never evidence. Authority, pressure, timeout, checksum,
schema, or deterministic-equality failures emit only a canonical stop receipt.
Page and training bodies are streamed and discarded; no corpus copy is
persisted. Local execution stops without restart at 2 GiB RSS, memory PSI full
avg10 at least 0.79 once or at least 0.50 for three consecutive five-second
samples, swap growth above 256 MiB, five minutes without authenticated
progress, or a two-hour wall cap. Each stream and any external request cost
require separate execution authorization. D3 remains fenced throughout this
work.

Progress is produced by the authenticated Rust phase, not inferred from file
mtime or launcher activity. Each phase atomically replaces one canonical
newline-delimited `progress.json` containing the complete progress history.
Every record binds the exact phase, monotonically increasing sequence,
completed and total registered work units, the last authenticated input or
sealed output digest, and the SHA-256 of the preceding individual record. Tree
training advances after registered shard/reservoir work and fixed node-count
milestones; posting advances after fixed page, partition, run, and merge
milestones; evaluation advances after fixed query/timing milestones. The
launcher accepts a new snapshot only when its previous bytes are an exact
prefix and every appended canonical record preserves phase and total, strictly
increases sequence and completed work, and matches the preceding-record digest.
This permits a monitor to catch up after missing multiple writes without
accepting a gap or an overwritten history. The terminal receipt binds the
SHA-256 of the complete final snapshot. Writing a heartbeat without completing
a registered unit is forbidden.

The credentialed worker treats diagnostic evidence as a first-class immutable
output. Each transient scientific unit writes stdout and stderr directly to a
regular file under the attempt evidence directory; the parent also snapshots
the unit journal after the unit terminates. Python entrypoints preserve the
complete traceback, and a failed phase writes a canonical failure detail plus
any completed preflight or staging receipts before private scratch cleanup.
The outer worker uses direct file-descriptor append rather than an asynchronous
`tee`, uploads every allowlisted evidence role before writing the terminal
marker, and writes that marker last. Terminal publication therefore never
depends on journald surviving instance termination or on a buffered helper
flushing before upload.

Every advertised pressure stop must be demonstrably available before immutable
scientific inputs are staged. In particular, the namespace/bootstrap preflight
requires readable Linux memory PSI with a concrete `full avg10` field; absence
is a bootstrap failure, not permission to omit the gate. Any exception raised
inside resource monitoring terminates and reaps the original process group
before propagating. A phase that advertises the five-minute progress stop must
emit the registered completed-work chain in production; test-only progress
writers or inferred activity are invalid.

## Success boundary

The falsifier succeeds only if the sealed development cell passes unchanged on
the 128-query holdout, including quality, exact eight-page selection, memory,
posting, touched-page, scalar/SIMD, and native p99 gates. The result remains
claim-ineligible because it is an architecture experiment on an existing
dataset. A subsequent fresh qualification must compare the hardened router
against the current production baseline and external state of the art under the
same cold-read, cost, recall, and resource contract before any superiority
claim is permitted.
