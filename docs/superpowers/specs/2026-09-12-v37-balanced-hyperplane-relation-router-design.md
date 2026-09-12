# V37 Balanced Hyperplane Relation Router Design

## Purpose

V37 is the next one-million-row qualification stage after V36 rejected fixed
centroid ownership, closure, diagonal and low-rank Gaussian scores, and fixed
prototype scores. It tests a different mechanism: query-blind balanced
hyperplane partitions define single-owner coarse postings, while an independent
partition learns a sparse relation from routing leaves to those postings.

The first population is the frozen ReLAION2B cohort A: 1,000,000 non-null
`f32[768]` rows, the registered 1,000 development queries, and exact GT@100.
V37 is claim-ineligible development evidence. It does not open validation,
holdout, 10M, 100M, or a serving page body.

## Causal decomposition

V37 separates two questions that V36 mixed:

1. **Layout ceiling:** can the hundred exact neighbours of every query fit in
   fourteen single-owner postings at all?
2. **Router attainment:** can a bounded query-time algorithm identify those
   postings without truth access?

The ownership layout is evaluated first. Since each row has exactly one owner,
the exact K=14 ceiling for one query is the sum of the fourteen largest GT row
counts by posting. This computation is exact, contains no routing score, and
requires no combinatorial solver. The relation router is not constructed when
this ceiling misses either quality gate.

The registered logical gates remain 998,000 ppm aggregate containment and
800,000 ppm minimum-query containment at exactly fourteen postings. Physical
coarse fragments, GETs, bytes, codecs, fine pages, and reranking remain a later
gate and cannot rescue logical failure.

## Projection and numeric contract

The frozen V36 projection contract supplies the input vectors. The first V37
cell deterministically replays the registered 768-to-192 SRHT (seed 36) over
the authenticated V36 source, so the new result changes partition and routing
only. Projection authority is the exact algorithm/version, source and routing
dimensions, seed, and semantic replay SHA-256 over ordered source ordinals and
projected f32 bits. It is not an invented Arrow-object identity: the resident
projector does not write a projected-corpus object. Source ordinals remain the
geometry identity; row order, ties, and digests never use a local path or task
schedule.

Every coordinate and intermediate must be finite. Squared L2 and dot products
use the existing backend-bound fused SIMD contract with a scalar diagnostic
control; scientific construction and routing require the same registered fused
backend. Dimensions are runtime authority, not a compile-time 768 or 192
assumption. The kernel executes increasing eight-f32 lanes followed by an
increasing-coordinate scalar tail, and its backend and lane reduction are
artifact authority. Persisted normals and thresholds are f32. Binary64 mean
sums accumulate in increasing source-ordinal and coordinate order and round
once at the stated f32 boundary. Persistence canonicalizes negative zero to
positive zero; any non-finite input, intermediate, normal, score, or threshold
is a construction stop.

## Single-owner balanced hyperplane layout

For `N` rows and target primary occupancy `B`, create `P=ceil(N/B)` leaves.
The root owns all rows and `P` leaves. At a node with `n` rows and `p>1`
descendant leaves:

1. Set `p_left=floor(p/2)` and
   `n_left=floor(n*p_left/p)` using checked integer arithmetic.
2. Select the node's smallest 4,096 values of
   `(SHA256("borsuk-v37-node-reservoir-v1\n" || seed_le64 || node_id_le64 ||
   source_ordinal_le64),source_ordinal)`, or every row when the node is smaller.
   Query and truth artifacts are unavailable. Nodes are numbered preorder,
   left child before right child.
3. Initialize label zero with the smallest source ordinal and label one with
   the row farthest from it, ties by source ordinal. Labels remain zero/one
   across exactly eight assignment/update rounds. Distance ties choose label
   zero; means reduce by increasing source ordinal. If one label is empty, move
   the row farthest from the nonempty deployed mean, ties by source ordinal. If
   no distinct finite donor exists, stop as degenerate geometry.
4. Form the normalized hyperplane normal from `right_mean-left_mean`. A zero or
   non-finite norm is a construction stop.
5. Score every node row once with the registered fused backend. The exact first
   `n_left` rows under `(score_bits_total_order,source_ordinal)` enter the left
   child. Persist the deployed f32 normal, left boundary score, and boundary
   source ordinal. Corpus replay uses that ordered pair and reproduces quotas.
   Queries have no corpus ordinal: a lower score chooses left, a higher score
   chooses right, and equality chooses the lower child as primary while
   queueing the other child at zero local margin.
6. Recurse in node ordinal order. A leaf receives one consecutive posting
   ordinal. No row is replicated and every leaf owns its exact quota.

At 1M/B8192 this produces 123 postings whose populations differ by at most one.
The implementation may retain projected rows for the resident diagnostic. The
scale builder uses bounded Arrow shards and external score/ordinal partition
runs; it never retains the source corpus or all node scores in RAM.

## Direct tree router

For a query, score a node's persisted hyperplane once. Traverse the containing
child first. Its path penalty inherits the parent penalty. The sibling path
penalty is the maximum of the parent penalty and squared normalized local
margin. Queue by `(path_penalty,node_id)`.
Best-bin-first traversal emits leaves in deterministic queue order, deduplicates
posting ordinals, and stops after fourteen. Equal margins use node then posting
ordinal. This score is a routing heuristic, not a certified metric bound.

The direct tree must pass the exact same aggregate and minimum gates. Its result
also reports the ceiling-to-attained gap per query. If the ceiling passes but
the direct tree fails, the relation stage is allowed. If the ceiling fails, the
layout is rejected and the relation stage is forbidden.

## Independent leaf-to-posting relations

The relation stage trains one second balanced hyperplane tree with a distinct
registered domain seed and 4,096 routing leaves at 1M. It does not own rows or
objects. Each corpus row routes to one relation leaf and increments the count
for its already frozen ownership posting. No query, GT, neighbour, validation,
or holdout artifact is visible during this pass.

For each relation leaf, order posting records by
`(count descending, posting_ordinal ascending)`. A **relation prefix of length
R** means the first R records in that order; it is not an S3 key prefix, vector
prefix, or tree-path prefix. Store the untruncated leaf population and count so
retained mass can be recomputed. The fixed ordered development cells are the
Cartesian product of R in `{16,32,64}` then relation-leaf probes in
`{8,16,32}`, ordered first by R and then by probes. Truncation occurs only after
full counts are reduced and all cells are frozen before development opens.

At query time, best-bin-first traversal emits at most 32 relation leaves. For
zero-based leaf rank `r`, a relation record contributes the checked integer
vote

`(32-r) * round_ties_even(count * 2^24 / leaf_population)`.

Votes accumulate by posting ordinal. The direct ownership-tree primary posting
is inserted with zero vote and `best_leaf_rank=u32::MAX`; it is only a
deterministic shortage backfill and is not a reserved slot. Candidate postings order by
`(vote descending, best_leaf_rank ascending, posting_ordinal ascending)`.
Exactly fourteen unique postings are selected. A short leaf prefix contributes
all of its records, but fewer than fourteen distinct candidates after primary
insertion rejects the cell as `candidate-shortage`. Traversal visits at most
1,024 nodes. The first complete passing cell is the sole survivor.

## Artifacts and portability

Cross-language tables use Parquet; dense typed arrays use Arrow IPC; manifests,
authority, progress, and results use recursively sorted compact JSON plus one
LF. There is no Rust-struct-memory serialization.

- `ownership-tree.arrow`: nodes, f32 normals, thresholds, children, leaf and
  posting ordinals, numeric/backend authority.
- `ownership.parquet` format v2: non-null `source_ordinal:u64`,
  `feature_row_id:u64`, `posting_ordinal:u32`, and posting-local ordinal,
  sorted by the exact contiguous source ordinal. Feature IDs are globally
  unique. This query-blind identity bridge lets a later truth-only process map
  the original V36 GT IDs without reopening corpus or query vectors. The old
  three-column experimental format is rejected; no compatibility reader exists.
- `relation-tree.arrow`: the independent tree and its registered seed.
- `relations.parquet`: non-null leaf/posting/count/population records before
  prefix encoding.
- `relation-prefixes.arrow`: structure-of-arrays u64 offsets, u32 posting
  ordinals, and u32 Q24 masses in the inclusive range `[0,2^24]` for each
  registered prefix length. A record remains exactly eight bytes.
- canonical JSON manifests/results bind every URI, SHA-256, BLAKE3, encoded
  length, schema, source identity, projection, metric, seed, worker count, and
  predecessor result. A separate ceiling authority binds the V37 construction
  authority, sealed tree and ownership identities, the original V36
  development-GT identity, the exact query count, GT@100, and K=14. It contains
  no source/query path or fetch instruction.

The local construction receipt v2 binds the exact ordered identities of all
six authenticated inputs and both generated artifacts. The ceiling output is
not a free-standing score document: its canonical wrapper binds the exact
ceiling-authority object identity, the authority's four prerequisite
identities, and the recomputed ceiling. Construction stages both output files
before either no-clobber commit. It never rolls back a committed path by name:
that would create a check/unlink race with another writer. A build is
scientifically complete only after both no-clobber commits succeed and the
canonical terminal receipt is emitted. If the second commit loses a race, the
first artifact remains incomplete scratch for the controller's explicit
named-file cleanup and cannot authorize a ceiling run.

Readers reject aliases, optional fallbacks, unknown fields, nulls, non-finite
numbers, invalid graph topology, cycles, duplicate ordinals, wrong ordering,
length/remainder drift, digest drift, and cross-object binding drift. This is a
new pre-release format; no V36 compatibility reader is added.
Every local input is opened once, authenticated through that file descriptor,
and retained as a capability for all later parsing and scientific reads. The
runner never reopens an authenticated input by pathname; cloned handles are
rewound before independent passes. A final device/inode/length/time check still
detects namespace replacement, but replacement cannot substitute scientific
bytes after authentication.

## Bounds and scale projection

At 100M/B8192, ownership has 12,208 leaves and fewer than 24,416 nodes. One
f32[192] normal plus 32 bytes of metadata per internal node is under 9.8 MiB.
A 4,096-leaf relation tree is under 3.3 MiB. Even a future 65,536-leaf relation
tree is about 52 MiB. Its 64-entry prefix with an eight-byte
`(posting:u32,mass_q24:u32)` record is 32 MiB plus offsets. These arrays are
far below the 3 GiB serving bound without object-per-record overhead.

The 1M direct-tree query scores fewer than 245 node hyperplanes. The relation
tree visits at most 1,024 nodes, emits at most 32 leaves, and reads at most
`32*64=2,048` relation records. Every SIMD MAC, node visit, posting touch,
selected posting, and byte is recorded. Warm native logical routing p99 must be
at most 5,000,000 ns after 1,024 warmups and at least 10,000 timed queries.

The resident 1M builder borrows row slices from one contiguous projected
coordinate buffer; descriptor sorting must not clone coordinates. Source
Parquet row-group/column-chunk working sets, descriptor arrays, hash-reservoir
and score tuples, assignments, output writers, worker stacks, and 25% allocator
headroom are charged with checked arithmetic before decoding. Ancestor split
scratch is released before recursion. The former three equal coordinate-buffer
placeholders are not an admission certificate.

The 1M ownership and relation builds each have a 600-second scientific cap,
720-second wrapper cap, 120-second progress timeout, 3 GiB process-group RSS
cap, memory PSI full avg10 stop above 0.75, and stop on any swap growth. The
ownership preflight separately charges the 65,536-row f32[768] Arrow decoder
window plus hard 256 MiB compressed and 256 MiB uncompressed Parquet row-group
caps. Before Parquet metadata construction, a bounded compact-Thrift preflight
rejects footers above 16 MiB, binary fields above 1 MiB, and collection claims
above 65,536 entries. Before Arrow decoding, it reads at most 64 KiB per page
header, rejects compressed or uncompressed page claims above 16 MiB, bounds
page values by the row-group shape, and requires a dictionary's physical values
to fit its declared uncompressed bytes. The same footer, row-group, page, and
dictionary checks protect the feature-ID-only pass and the ceiling's ownership
and GT Parquets; projection and truth decoding cannot bypass them. The ceiling
authority admits at most 1,000 queries before any
per-query GT allocation. The construction projection then charges one resident
`rows*192*4` projected buffer, feature IDs, sorted
`(source_ordinal,index)` descriptors, member indices, 24-byte score tuples,
assignments, the bounded 4,096-entry hash reservoir, the ownership tree, the
streaming ownership writer, and 25% allocator headroom. For the registered 1M
shape the checked total is 2,137,615,120 bytes, including 738,721,792 bytes for
the bounded source decoder (the sequential 64 KiB header scratch is contained
within its compressed-working-set budget) and 67,108,864 bytes for the maximum 32 worker
stacks at 2 MiB each. Relation construction has a
separate phase-specific projection and cannot reuse this number. Each phase
rejects before allocation when its checked sum exceeds 3 GiB. At least
20,000,000 fused coordinate scores per second on a 65,536-row reduced shape are
required to admit the full build. All 100M time and I/O projections remain
provisional until an external-partition preflight passes; arithmetic alone is
not qualification.

Writes route through two shallow trees and append one row to a bounded delta;
they do not calculate graph neighbours or update covariance matrices. Release
qualification must later measure base-plus-delta throughput, overflow GETs,
compaction amplification, and recall decay. A static build result is not a
write-throughput claim.

## Fail-fast order and disposition

1. Unit-test quota, topology, deterministic training, backend-bound SIMD
   behavior, scalar diagnostic agreement, codecs, and relation reductions.
2. A construction-only process authenticates V36 source authority, replays and
   verifies the frozen SRHT digest, then builds and seals the ownership
   tree/table without query or GT capability. A separate truth process receives
   only ceiling authority, tree, ownership v2, and the original V36 GT Parquet.
   It validates GT ordering/types, maps globally unique feature IDs to source
   ordinals, and computes the exact fourteen-posting ceiling. It cannot open a
   source file or query vector.
3. Stop as `layout-rejected` if aggregate is below 998,000 ppm or any query is
   below 800,000 ppm.
4. If the ceiling passes, a query-only process emits direct selections and a
   separate truth process evaluates them. Direct pass skips relations.
5. Direct failure seals `direct-failed`; only then may a construction-only
   relation worker authenticate matching `ceiling-passed` and `direct-failed`
   predecessors before opening corpus/ownership inputs. Query-only selection
   and separate truth evaluation then run the fixed development ladder.
6. Stop as `relation-router-rejected` when no registered cell passes. Do not
   add trees, probes, prefixes, GETs, or bytes after outcomes.
7. Only a logical pass advances to fragment packing and exact 14-GET/7-MiB
   transport simulation. Only that pass can later open a fresh sealed holdout.

The development queries already influenced V36 diagnosis and cannot establish
generalization. They choose at most one preregistered V37 cell. Validation and
holdout remain unavailable until that cell and all artifact identities are
sealed. A failed 1M cell never advances to 10M or 100M.
