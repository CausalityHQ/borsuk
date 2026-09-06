# V35 dimension-independent S3 routing design

## Status and decision

V34 remains a frozen 96-dimensional experiment. Its authenticated low-rank
representation and conservative hierarchy are useful correctness machinery,
but its ambient-dimension f32 leaf state is not a release architecture. At the
observed 414,100-leaf density, its numeric payload alone exceeds 3.5 GiB at 384
dimensions and approximately doubles with every dimension doubling, before a
retiring generation, tree, caches, runtime, or query workspaces.

V35 is a breaking format replacement. It separates source dimension `D`,
routing dimension `M`, and remote code rate. Large immutable code and vector
objects may occupy tens of gigabytes in S3. Resident routing state, active and
retiring generations, caches, runtime, and all admitted query workspaces must
remain below 3 GiB. Search never downloads or retains the corpus.

The release objective is not perfect recall at any cost. It is a Pareto lead:
high sustained write throughput, selective remote reads, low warm latency,
competitive cold latency, and at least 995,000-ppm aggregate Recall@10 on the
sealed qualification set. Any claim is paired against disclosed competitors
at equal or better recall on the same source vectors, queries, truth, host, and
storage tier.

## Evidence that constrains the design

- The authenticated width-12 global-ADC diagnostic scanned all 4,096 cells and
  9,990,000 codes for 32 queries. Its faithful reducer exactly reproduced the
  routed result: 671,875-ppm aggregate recall, 100,000-ppm minimum recall, and
  676,100-ppm oracle attainment. Removing the 320-cell router did not recover
  quality. The current PQ representation/reducer, not only its first-level
  router, is insufficient.
- The f16 row-identity control reached 996,875 ppm, and the fixed-layout oracle
  reached 993,750 ppm aggregate with 900,000 ppm minimum, but 192 bytes per
  96-dimensional row cannot be resident at 100M scale.
- V33's exposed 1M rank-four diagnostic retained all 1,280 truth owners for
  128/128 queries with required group frontiers 5/21/43 at p50/p95/max. This is
  a burned mechanism signal, not held-out release evidence.
- V34 proves exact traversal equivalence for the stored score. It does not show
  that 96-dimensional ellipsoids solve concentration of measure or generalize
  to high-dimensional embeddings.

## Representation

### Shared corpus-only projection

Train one deterministic, query-independent orthonormal projection `P` from the
construction corpus only. Persist `P` as non-null f32 Arrow state with source
dimension `D` and routing dimension `M`. The preregistered development ladder
is `M in {64, 128, 192}`. There is no outcome-triggered 256-dimensional
escalation: if 192 dimensions fail the quality or complement-energy gates, the
representation is rejected rather than consuming the memory safety margin.
Holdout chooses nothing: it evaluates the one development-selected format
exactly once. The persistent format accepts any positive `D` for which
`M<=D` and all Arrow widths and allocation arithmetic validate; 384--3,072
dimensions are qualification bands, not hard-coded API bounds.

The primary basis is deterministic randomized SVD/PCA trained from a fixed,
source-ordinal sample. The non-learned control is
`srht-prefix-orthonormalized-v1`: seeded restricted Hadamard candidates are
accepted in a seeded permutation and orthonormalized in two fixed-order f64
modified-Gram--Schmidt passes. This is not an unmodified padded SRHT: truncating
zero-padded Hadamard rows does not preserve orthogonality when `D` is not a
power of two. Both arms persist the same dense source-major `D*M` f32 basis and
use the same query kernel, bytes, training visibility, and routing controls.

PCA uses the uncentered second moment, `ell=min(D,M+16)`, a domain-separated
Rademacher sketch, one initial streamed covariance-operator application,
exactly two subspace iterations, and a fixed-order Rayleigh--Ritz solve: four
passes over the registered sample in total. The uncentered objective directly
maximizes captured norm energy for the later complement term. Training consumes
unique sample ordinals in increasing order, caps `B` at 2,048 rows, and uses
memory `O(D*ell + ell*ell + B*D)` independent of sample population. Repeated eigenspaces and rank deficiency are completed
deterministically by projecting source axes in increasing coordinate order;
zero basis rows are forbidden. Sample selection, seeds, pass count, block
limit, eigensolver/pivot order, eigenvalue-cluster tolerance, trainer identity,
and arithmetic policy are authenticated metadata.

Training and serving both use the same linear, uncentered geometry: `z=Pq` and
`y=Px`. V35 performs no implicit normalization or mean subtraction. After the
single f64-to-f32 basis rounding, the training basis is dropped. Every later
consumer uses only decoded f32 coefficients. Authority is that decoded basis widened to f64 and an
increasing-source-dimension ordered sum. SIMD vectorizes across output
coordinates, never across source-coordinate reductions, and must return
bit-identical f64 projected coordinates to scalar authority. Queries with the
wrong length or any nonfinite input are rejected identically.

The basis artifact is one Arrow batch with exactly `D` rows and one non-null
`coefficients: FixedSizeList<item: Float32 not null, M>` field. Row order is
source-coordinate order. The decoded Gram matrix must satisfy
`abs(G[i,j]-delta[i,j]) <= 5e-4`. Direction signs are canonicalized before the
single f32 rounding by making the lowest-coordinate maximum-absolute
coefficient positive; every zero is positive zero. A logical SHA-256 binds a
domain tag, arm, `D`, `M`, seed, sample identity (empty for the control), exact
training descriptor, and source-major little-endian f32 bits. The Arrow object
also has its independent complete-byte SHA-256.

### Compact projected leaf patches

Each leaf stores one or two projected patches. Each patch contains:

- population, group, logical interval, and assignment bounds;
- int8 projected mean coefficients plus f32 scale;
- u8 projected residual-diagonal coefficients plus f32 scale;
- six int8 projected residual directions plus per-direction f32 scales and
  nonnegative f32 weights;
- trace, trace-square, population factor, projected spectral bound, and one
  nonnegative discarded-space energy scalar;
- a leaf ordinal; exact format, projection, training-sample, and
  source-generation bindings live once in the authenticated generation header.

For a finite source query `q` under the registered `none` normalization, let `z=Pq` and
`E_q=max(0,||q||^2-||z||^2)`. Each patch stores `E_l`, the ordered f64 mean of
the same omitted-energy expression over its construction rows, rounded once to
f32. Decoded scoring is exactly

`s(q,l)=D_p+t-a*sqrt(2*h+4*u^T*Sigma*u)+(sqrt(E_q)-sqrt(E_l))^2`,

where `u=z-mu` and the other terms are the decoded V34 lower-tail algebra in
projected space. Node state stores outward `E_min/E_max`; its complement lower
bound is squared distance from `sqrt(E_q)` to
`[sqrt(E_min),sqrt(E_max)]`. This is exact for ordering the stored heuristic,
not a reconstruction of omitted coordinates. SIMD produces a conservative
score interval; disjoint intervals order directly and overlapping intervals
fall back to the increasing-dimension f64 scorer. Canonical receipts contain
the f64 authority score. Because the persisted f32 basis is only approximately
orthonormal, `E_q` is the explicitly clamped stored heuristic rather than a
claim of exact geometric residual energy. A two-patch arm is
eligible only against an equal-byte extra-centroid control. The development
matrix therefore compares one patch, two patches, equal-byte centroids, and
the SRHT control under identical group, row, byte, and page budgets.

The fixed projection for 414,100 leaves is `8*M + 128` bytes per one-patch leaf:

| M | bytes/leaf | one generation | two generations |
|---:|---:|---:|---:|
| 64 | 640 | 265,024,000 B | 530,048,000 B |
| 128 | 1,152 | 477,043,200 B | 954,086,400 B |
| 192 | 1,664 | 689,062,400 B | 1,378,124,800 B |

The projection matrix is at most `3072*192*4 = 2,359,296 B` in the ordinary
ladder. Tree state is capped at 32 MiB per generation. The checked 192-wide
admission budget is 1,378,124,800 B for active and retiring leaves, 67,108,864
B for both trees, 4,718,592 B for two bases, 251,658,240 B shared caches,
67,108,864 B for the admitted delta state,
268,435,456 B runtime/allocator, 536,870,912 B for sixteen query workspaces,
and 268,435,456 B unallocated headroom: `2,842,461,184 B`. The manifest
computes every term with checked integer arithmetic and rejects a total at or
above `3,221,225,472 B`.

Construction keeps one packed upper-triangular f64 co-moment per live worker:
`192*193/2*8 = 148,224 B`. Sealing transitions that allocation to the dense
workspace required by the exact symmetric eigensolver; packed and dense
co-moments are not simultaneously owned. The explicit conservative seal
workspace is `2*M*M*8 + 4*M*8`, or `595,968 B` at M=192 and `9,535,488 B`
for sixteen workers. This construction-only workspace is inside the declared
runtime/allocator reservation and never enters serving RSS. A reader exposing
a second live leaf to one worker still fails before allocation.

The prose arithmetic above is explanatory; the canonical receipt records the
component vector and checked sum. Tests pin the exact sum
`2,842,461,184 B`, leaving `378,764,288 B` below 3 GiB. A two-patch 192-wide
arm must supply its own complete projection and is expected to fail unless leaf
count or bytes are reduced; it receives no implicit memory exception.

The 251,658,240-B shared-cache term is not an undifferentiated object cache.
It contains exactly 25,000,000 B for one-bit active and retiring base-row
liveness planes, 67,108,864 B for immutable chunk/page directory roots and
cached blocks, and 159,549,376 B for code/page object data. Directory roots
contain bounded group summaries and identities for selectively fetched blocks;
they never materialize one heap object or URI string per corpus row or page.
Liveness is an immutable, chunked,
snapshot-bound plane. Replacement and delete publication changes that plane
and the bounded mutation directory; it does not rewrite full base code or
vector pages. Both candidate admission and exact-page reranking consult the
same pinned liveness snapshot, so a stale row may consume routing work but can
never enter the heap or final result.

The 64-MiB delta term contains 32,000,000 B for one million 32-byte mutation
entries, 16,000,000 B for postings/page references, 6,890,624 B for 4,141
192-wide one-patch leaves, 223,680 B for 699 320-byte nodes, and 4,194,304 B
four-run overhead, with 7,800,256 B reserved. Delta row/leaf counts and every
component are checked; unused reservation cannot be borrowed by another term.

The serving mapping keeps int8/u8 planes encoded and decodes only the leaf/node
currently being scored into a bounded query workspace. It never retains eight
f32 decoded planes. Generation validation streams with an 8-MiB buffer and the
active/retiring accounting includes both mapped Arrow buffers. The shared-cache
reservation includes chunk/page directories; the separate delta reservation contains mutation and delta-route
state. Each component is reported separately and the checked total, not the
nominal reservation, is the authority.

### Remote codes and vectors

Routing summaries and trees are resident. Row codes, postings, and exact
vectors remain immutable S3 objects. Remote code formats bind both bits per
source dimension and total bytes per row; no 96-dimensional fixed-byte cap is
carried forward. At four bits per dimension, 100M code payloads are 19.2 GB at
384D, 38.4 GB at 768D, 76.8 GB at 1,536D, and 153.6 GB at 3,072D before object
envelopes. Those sizes are allowed in S3 and forbidden as serving RSS.

The SQ8 control doubles each packed payload figure but remains remote; it gets
the same per-query returned-byte limit as SQ4.

V35's executable remote-code ladder is affine residual scalar quantization at
exactly four or eight bits per source dimension. Each remote group stores one
f16 source-dimensional group center plus one f32 residual scale per dimension.
The center is the increasing-row-ordinal f64 mean rounded once to f16. Encoding
and sigma both use that decoded f16 center. Scale is `8*sigma/(2^bits-1)`, where
`sigma` is the population residual standard
deviation computed in the same order; zero sigma uses scale zero and code zero.
Values outside center plus/minus four sigma saturate. Codes are packed
source-order nibbles with the high nibble first for SQ4 or source-order bytes
for SQ8, u64 row IDs, u64 sequences,
and u32 primary/replica page ordinals in structure-of-arrays order. Query ADC
decodes `center-4*sigma+code*scale` in architecture-specific SIMD, sums squared
differences in increasing-dimension scalar authority order, and keeps at most the best 12,288
rows by `(distance,row_ordinal)`. The complete returned code-object budget is
8 MiB, including headers, centers, IDs, sequences, page references, and chunk
envelopes. Thus row work shrinks as `D` or rate grows; packed SQ4 payload alone
fits at most 43,690/21,845/10,922/5,461 rows at
384/768/1,536/3,072D before envelope charges. The frozen reducer walks candidates
in `(distance,row_ordinal)` order. If either authenticated page reference is
already selected the row is covered; otherwise it selects the primary page. It
stops after eight distinct pages and reranks their vectors in full source
dimension. A replica is an alternate existing page reference for availability,
not a second mandatory vector copy. Sixteen- and 32-page results are diagnostic frontier points,
not fallback release winners.

Every row decoded from an exact page is checked against the same snapshot
mutation directory before it can enter the exact top-k. Page selection does not
authorize stale, tombstoned, or shadowed rows to reappear during reranking.

The query path authenticates a bounded directory root, selectively fetches only
the directory blocks named by the route prefix, fetches only selected complete
code chunks or gap-free coalesced byte ranges, retains the bounded candidate
heap, then fetches selected exact-vector pages/ranges for reranking. Route,
directory, query, generation, source dimension, code rate, and visibility
snapshot digests are cross-bound before any data GET. Every rangeable object has a manifest of
independently SHA-256-authenticated chunks; a response must match the registered
bucket, key, version ID, interval, length, and chunk digest before decoding.
Whole-file digests remain publication authority but do not authenticate an
unchecked range. The receipt reports GET count, requested and returned bytes,
range amplification, retries, decompression bytes, and cache state. A query is
invalid if it opens an unselected group, scans the full remote code population,
downloads the corpus, or persists page bodies outside the bounded cache.

Chunks are at most 1 MiB encoded and 2 MiB decoded. Exact-vector pages contain
at most 256 rows and are at most 4 MiB encoded and 4 MiB decoded at the maximum
qualified dimension. Authentication precedes decoding, so one query reserves
3 MiB for one complete at-most-1-MiB encoded chunk and at most 2 MiB of decoded
blocks; no second decoded chunk may coexist. One query owns at most 32 MiB: 4 MiB
traversal/score state, 3 MiB code buffers, 1 MiB for the 12,288-candidate heap
and page set, 8 MiB sequential page buffers, 1 MiB query/result state, and
15 MiB allocator and SDK headroom. Planned code ranges may be dispatched
concurrently within that 15-MiB in-flight allowance, but are delivered in plan
order into the single owned code buffer. The eight selected page objects are one
bounded request wave; their response chunks remain charged to SDK headroom and
pages are authenticated, decoded, reranked, and released sequentially rather
than accumulated. Code buffers are released before exact-page decode. Every
maximum is a manifest field validated before query admission.

## Search and hierarchy

Reuse V34's deterministic 16-way conservative traversal mathematics over
projected leaf state, but not its f32 node layout. Each V35 node stores an int8
projected center plus f32 scale, indices/counts, and outward-rounded scalar
bounds in at most `M+128` bytes. Its radius includes the decoded-center
quantization error. At `M=192`, 69,905 nodes occupy at most
`69_905*320 = 22,369,600 B` per tree, below the 32-MiB admission cap. Exactness
means equality to exhaustive scoring of the same decoded V35
representation, including `(score, group, leaf)` ties, prefix stopping, and
overflow identity. It does not mean equality to full-dimensional neighbour
distance.

The router returns the longest score-ordered complete-group prefix within only
the quantities known before fetching: group, row, and code-byte budgets. The
first overflowing group is certified and reported; groups are never skipped to
make the receipt pass. Code execution then applies the fixed 12,288-row heap;
page reduction then applies the fixed first-eight-distinct rule. Candidate and
page counts cannot retroactively alter the route prefix. Query projection,
hierarchy work, code scan, candidate merge, page reduction, and exact rerank
have separate timing and work counters.

The authoritative performance gates are: warm router p95 at most 5 ms and p99
at most 8 ms on one pinned qualification core; warm end-to-end p99 at most
15 ms when every selected object is in the declared cache; and both warm and
cold median-of-three p95/p99 at most 105% of the paired baseline with QPS at
least 95%. Cold S3 has no absolute 15-ms gate. At least one of cold returned
bytes or GETs must improve by 10%. Every repetition must pass identity, recall,
memory, error, and access gates, and no individual latency repetition may
exceed 110% of baseline. Host, region, SDK, connection reuse, concurrency, and
query order are paired.

## Streaming construction and write path

Construction never materializes the corpus. A bounded registered sample object
containing exact source ordinals trains `P` and locality quantiles first. One
subsequent full source pass projects bounded row blocks and assigns a 128-bit
locality key. The sample selects the sixteen projected coordinates with greatest
ordered f64 variance, breaking ties by coordinate. Each is quantized to eight
bits against 255 sample-derived f64 quantile boundaries, with equality assigned
to the lower bucket; the key is their most-significant-bit-first Morton
interleave. Bounded S3 scratch runs store
`(key,source_ordinal,projected_row,source_row)` and merge in
`(key,source_ordinal)` order. Consecutive runs of at most 256 rows form leaves.
Consecutive leaves form storage groups targeting 349,526 through 524,288 code
bytes after descriptor/envelope charges. The lower bound is
`ceil(8 MiB / 24)` and applies to every nonterminal group; only the final group
may be smaller. The upper target is `8 MiB / 16`. This makes a complete legal
8-MiB route executable under the hard 24-GET ceiling even when selected groups
cannot be coalesced, under both the 8-MiB encoded-object cap
and a 64-MiB builder-buffer cap including source/projected rows, IDs,
references, moments, and allocator capacity. This
one layout is reused by one-patch, two-patch, and equal-byte-centroid controls
within a projection arm.

Scratch objects contain bounded Arrow record batches. Each complete object is
authenticated before semantic use, then exposed through at-most-256-row
cursors. A preregistered bounded-fan-in external merge reports every pass and
scratch byte; opening one complete decoded run per input is forbidden. The
merge keeps one live leaf accumulator, computes and seals each patch,
encodes code planes, writes exact-vector pages, and emits assignment runs. Local
builder RSS is capped below 3 GiB with declared 64-MiB buffers; remote scratch may be
large but is separately prefixed, byte-accounted, lifecycle-tagged, and deleted
only after terminal publication/receipt. If the sample object must itself be derived, that is a
separate streaming sampling pass and receipt, never an unreported hidden pass.
Sealing merges bounded external runs and emits Arrow routing generations plus
Parquet evidence.

Online writes use immutable append-only segments and conditional manifest
publication. Inserts become searchable after one segment seal without global
retraining. Tombstones and replacements resolve by `(id, sequence)` before
final top-k. Readers pin one base plus ordered deltas; at most four uncompacted
runs and one million delta rows are admitted. Backpressure starts before either
bound. Compaction streams affected base/delta objects and publishes a new
generation atomically.

Every pinned snapshot includes a complete mutation directory for all admitted
deltas, keyed by ID and containing latest sequence plus live/tombstone state.
The directory is consulted before a base or delta candidate enters the bounded
heap, so a moved replacement or tombstone in an unselected group still removes
the stale base row. At one million delta IDs its checked 32-byte entries consume
32,000,000 B inside the 64-MiB delta reservation. That reservation is aggregate
across every old/new pinned snapshot, construction copy, hash-table allocation,
and run; publication applies backpressure before allocating a copy that would
cross it. Compaction eliminates covered entries only after the new base is
published and old readers release their pins.

Qualification records rows/s, source bytes/s, encoded bytes/s, p50/p95
visibility, compaction bytes per input byte, peak RSS, swap, and concurrent-read
latency. The release target is a Pareto improvement over the paired current
writer; no throughput claim is inferred from offline arithmetic.

## Cross-language authority

Arrow IPC is the canonical resident routing format. Parquet is canonical for
bulk vectors, queries, truth, and construction/evaluation samples. Small
manifests and receipts use canonical newline JSON. There are no private Rust
serialization shortcuts and no legacy readers.

Every artifact binds dataset/source identity, `D`, `M`, metric, normalization,
row/leaf/group counts, projection algorithm and checksum, training ordinals,
patch arm, remote code rate, object roles/URIs/digests/lengths, generation,
builder binary, and parent receipts. Concrete schema, field order, nullability,
endianness, finite values, logical ordering, and exact digest algorithms are
validated before semantic use. Arrow/Parquet round trips are tested in Rust and
Python without converting bulk evidence to JSON.

## Fail-fast evaluation ladder

### P0: sub-second authority and algebra

Unit tests pin checked memory arithmetic, projection identities, quantization,
score scalar/SIMD equality, conservative bounds, tie order, prefix stopping,
Arrow/Parquet schemas, remote-range authorization, and failure receipts.

### P1: free synthetic high-dimensional falsifier

Use one million generated vectors at `D={384,768,1536,3072}` with intrinsic
ranks 32/64/128, random rotations, anisotropy, multimodality, heavy tails,
hubness, distribution drift, and a full-rank isotropic negative control. Train
without queries/truth. For each candidate arm on the positive families require:

- exact optimized/exhaustive equality over the stored representation;
- at least 999,000-ppm owner containment before remote codes;
- p95 leaf evaluations at most 25% and router p95 at most 5 ms;
- checked 100M resident projection below 3 GiB;
- no whole-corpus or unselected-object access.

Full-rank isotropic data is reported as a concentration stress test, not forced
to fail. A separate evaluator, after route outputs are immutable, applies an
independent per-query cryptographic permutation to sealed owner/page labels
using `HMAC(master_seed,query_ordinal)`. The router capability cannot read the
master seed or outputs. Conditional on each query's fixed selected-page and
distinct owner-page counts, hits follow that query's exact hypergeometric
distribution. The evaluator convolves those distributions in query-ordinal
order and requires the observed total below the combined upper-tail bound at
`alpha=1e-6`; repeated truth rows on one page count once. Exceeding the bound
invalidates the harness for leakage. Candidate-arm failures do not reject
other preregistered candidates. Equal-byte centroids use the same leaf layout,
recursive maximum-variance split, f32 storage, exact byte envelope, and
minimum-center-distance score. One/two-patch comparisons keep leaf count and
layout fixed; an arm that does not fit is rejected rather than shrinking the
layout.

### P2: public 1M evidence

Run at least one fixed public corpus in each mandatory release band: 384D, 768/960D,
1,536D, and 3,072D. Candidate datasets include MS MARCO/MiniLM, Cohere/Wikipedia
embeddings, GIST1M, and a frozen DBpedia embedding corpus. Dataset availability
and licenses are registered before results. Missing a band blocks the general
384--3,072D release claim; a narrower claim must name its supported dimension
range. Each uses 600 development and 600 sealed holdout queries with exact
full-dimensional GT@10.

Development selects one `(M,projection,patches,rate)` arm using one global fixed
lexicographic rule. First reject an arm that fails any authority/resource/
containment gate on any mandatory dataset. Among survivors: maximize the
minimum per-dataset aggregate Recall@10; maximize total correct GT hits summed
over all datasets; minimize the maximum per-dataset returned S3 bytes; minimize
the maximum per-dataset router p99; then minimize resident bytes. The choice
uses only the eight-page result.
Sixteen/32-page diagnostics and per-dataset winners are never eligible
substitutes. A remaining exact tie chooses the lexicographically smallest
canonical arm ID `(M,projection,patches,rate)`. No outcome-specific manual
choice is allowed.

Only a committed development pass opens holdout. Holdout requires aggregate
Recall@10 at least 995,000 ppm, minimum-query recall at least 800,000 ppm,
owner containment at least 999,000 ppm, warm router and memory gates, and a
strict GET or byte improvement without latency/throughput regression against
the paired baseline. Competitor comparisons report the entire recall/latency/
throughput frontier, not one cherry-picked operating point.

Every result reports four causal checkpoints against exact truth: owner rows
inside routed groups, truth rows retained by the SQ4/SQ8 candidate heap, truth
rows present in the first eight pages, and final exact Recall@10. A failure is
assigned to routing, remote quantization/reduction, page layout/reduction, or
reranking respectively; aggregate recall cannot hide an earlier mechanism
failure.

### P3 and P4: scale only survivors

P3 runs the 768/960D and highest available at-least-1,536D survivor at 10M on
`causality` EC2 Spot. P4 runs one 100M qualification at the highest dimension
whose immutable 100M source is registered, and blocks a broader scale claim if
that dimension is below 1,536. Both use immutable receipts, interruption
restarts under new attempt identity, immediate instance termination, and no
inspection of incomplete scientific outputs. A completed scientific failure is
never rerun with changed parameters under the same registration.

Before P3, registration names the real corpus, license, exact source URI/hash,
dimension, row count, 600-query split, and exact full-dimensional blocked-scan
GT@10 procedure. MS MARCO may supply the approximately-8.8M real-text rung but
must be labeled by its actual count, not rounded to 10M. When no licensed public
100M high-dimensional source exists, P4 uses the P1 counter-based generator at
exactly 100M rows and 1,536D plus 128 sealed queries; GT@10 is one preregistered
f64 blocked full scan. That result qualifies scale/resource mechanics only,
never real-data quality. A real 100M quality claim remains blocked until an
equivalent immutable public or reproducibly generated embedding corpus is
registered.

The absolute 15-ms warm gate uses a preregistered cache-fitting query subset
whose union of selected code chunks and exact pages is at most the
159,549,376-B object-data cache allocation. It runs 1,024 warmups plus at least
10,000 repeated recorded queries. The complete 10,000-distinct-query workload
is still measured, but receives only paired relative latency/QPS gates; it
cannot claim an all-hot absolute bound when its working set exceeds the cache.
Cold measurements use unique uncached
objects, an established TLS/SDK client, and no application page/code cache;
they report first-byte and complete-query latency separately. Each point has
three terminal repetitions. At recall at least 995,000 ppm, V35 applies the
single authoritative performance gate table above; this section introduces no
second definition.

The write gate runs 70% inserts, 20% replacements, and 10% deletes through at
least three complete compactions while fixed read concurrency is active. It
chooses replacement and delete IDs uniformly without replacement from the live
ID set for each batch, preventing locality from making copy cost look smaller.
It requires median sustained rows/s at least 125% of the paired writer in all
three terminal repetitions, visibility p95 at most one second, total S3 write
amplification at most 3.0, zero lost/resurrected IDs, and concurrent-read p99 at
most 110% of the same V35 read workload without writes. Amplification numerator
is every final, scratch, retry, and compaction byte written to S3; denominator
is the logical uncompressed source bytes accepted by the workload.

## Leakage and causal controls

- Projection, residual descriptors, leaves, patches, and layouts see corpus rows only.
- Query and truth roles are capability-separated from construction.
- Development and holdout query families are deduplicated before sealing.
- Exact truth and reranking use original full-dimensional vectors.
- Equal-byte centroids isolate whether low-rank shape state helps.
- SRHT isolates learned-basis benefit.
- One/two patches isolate multimodality from merely spending more bytes.
- Exhaustive stored-score routing isolates hierarchy from representation.
- Full-rank isotropic and drift controls detect concentration failure and
  outcome leakage.

## Explicit non-goals

- No claim that ellipsoids solve the curse of dimensionality.
- No release claim from 96/97-dimensional data.
- No full corpus, full code plane, or exact-vector population in RAM.
- No hard 15-ms cold-S3 promise.
- No compatibility with V34 or earlier experimental formats.
- No learned query router before the query-independent V35 controls pass.
- No paid scale run before P0, P1, and P2 development gates pass.
