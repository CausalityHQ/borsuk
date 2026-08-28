# Standard-S3 V23 Quantized Posting Pages

**Status:** Approved architecture, pending implementation plan. V23 remains a
claim-ineligible diagnostic until every gate in this document passes.

**Predecessor:** V22 Stage L completed at source `14464c8` and is recorded in
`docs/research/cold-read-latency-design.md`. Its authenticated Deep Image 10M
census tested 42 exact-vector layouts over 32 frozen queries. No layout met the
joint limit of four S3 requests and 1 MiB. The V22 evidence commit is
`e32f4e2`.

**Supersedes:** The V20 exact-vector cell-card serving layout and the rejected
V21/V22 production hypotheses for the next pre-release dense-ANN format. BORSUK
is unreleased, so V23 defines one new format and no legacy reader or migration
path.

## Objective

Make strict-cold dense ANN over S3 Standard a one-wave operation while keeping
quality and process memory production-safe. The first qualification dataset is
Deep Image 10M. A V23 production candidate must satisfy all of:

- recall@10 at least `0.975` over the frozen 1,000-query publication set;
- at most four query-scoped S3 GETs and `1,048,576` backing bytes for every
  strict-cold query;
- one parallel S3 wave on the ordinary path, with no dependent object read;
- cold p50/p95/p99 no worse than `60/100/150 ms`, with a target p99 below
  `100 ms` and a hard release requirement below the honestly paired competitor
  p99;
- process RAM at most `3 GiB`, including routing authority, caches, mutation
  overlay, concurrent query buffers, allocator capacity, and runtime overhead;
- bounded write amplification, deterministic builds, content-addressed
  immutable objects, and no query-result cache or local serving tier;
- S3 Standard only: no S3 Express, CDN, local index replica, or persistent disk
  cache.

The four-request/1-MiB limits are architectural inputs, not values to relax
after an unsuccessful run. Latency is measured only after those limits and the
quality gate pass.

## What V22 proves

V22 separates two failures that cannot be repaired by another row ordering.

At an exact prefix of ten, the useful payload is only 3,840 bytes, but the ten
rows occupy nearly ten independent 32-row exact blocks. The best semantic
layout made only 6 of 32 queries eligible; 22 were request-limited. At a prefix
of 256, the rows occupy a median of about 220 exact blocks and require at least
3.59 MiB of selected block bytes before speculative gaps. Thirty of 32 queries
were byte-limited. Existing exact storage costs about 662 bytes per 384-byte
row.

The frozen top ten can span ten coarse routing cells. Therefore a layout that
retains one authoritative coarse-cell owner per row cannot guarantee four
physical reads. Cross-boundary replication or an equivalent global posting
authority is required.

V22 does not measure code-only result quality or the effect of replicated fine
postings. Those are the two load-bearing V23 hypotheses and must be falsified
before a persistent format is built.

## Considered approaches

### 1. Quantized posting pages with boundary replication — selected

Keep routing metadata resident, store compact row codes and IDs in capped
posting pages, replicate boundary rows into nearby postings, fetch at most four
pages concurrently, and rank directly from their codes. This matches the
economics: one MiB holds tens of thousands of compact codes but only about
2,700 raw Deep Image vectors. It removes the dependent exact-vector wave.

Risk: code fidelity and four-page GT coverage are inferred, not proved. D1 and
D2 below exist to reject the approach cheaply if either is insufficient.

### 2. Replicated exact-vector pages — rejected as the primary path

Exact scoring is attractive, but the byte limit holds fewer than 2,700 raw
Deep Image rows and fewer than 700 raw 768-dimensional rows before IDs and
format overhead. V22's observed tail needs more regions than four and more
bytes than one MiB. Replication increases storage without fixing the read-byte
floor.

### 3. Disk graph traversal — rejected as the primary path

A graph can preserve recall with compact memory, but its dependent neighbor
expansions turn S3 latency into multiple serial round trips. It is appropriate
for SSD-backed systems, not a strict-cold S3 Standard p99 target.

## Architecture

V23 has four serving components:

1. A resident page router containing compact page centroids, a bounded ANN
   graph over those centroids, immutable page references, and generation
   authority.
2. Content-addressed posting-page objects. Each page carries IDs and one
   fixed-width code for every primary or replicated row assigned to it.
3. A resident post-generation mutation overlay encoded by the same quantizer.
   It contains new/upserted rows and tombstones newer than the page watermark.
4. The existing exact vector store and point directory for `get`, offline
   verification, rebuilds, and APIs that explicitly request exact vectors. It
   is not read by the ordinary low-latency ANN path.

The router selects at most four pages before any I/O. Their GETs are submitted
as one wave. Decoding, SIMD distance evaluation, deduplication, overlay
reconciliation, and top-k selection happen after that wave. Search returns
approximate ANN distances. Exact vector materialization is a separate explicit
operation and is excluded from the cold ANN SLA.

## Quantizer and page format

The diagnostic evaluates the existing SIMD-capable quantizer families rather
than inventing a new distance kernel:

- SRHT product quantization at `{8,16,32,64}` bytes per row;
- Fast TurboQuant widths whose encoded sizes are relevant to the same range.

One quantizer and width is frozen only after D1 and D2. The query-preparation
and contiguous-code scan use the same production SIMD implementation as the
current global scan path. Scalar code exists only as a correctness oracle.

Each posting object uses a private V23 binary format:

```text
header
  magic, version, metric, dimensions, code family, code width
  generation checksum, page ordinal, primary rows, replicated rows,
  id section bytes, code section bytes, 32 reserved zero bytes
offsets[n + 1]       // compact raw-ID boundaries
ids[offsets[n]]      // authenticated raw record-ID bytes
codes[n * width]     // contiguous SIMD scan plane
```

The format contains no Arrow or Parquet per-row metadata. A page is accepted
only when its complete authenticated encoded length is at most `245,760`
bytes. Four maximum pages therefore consume at most `983,040` bytes, leaving
64 KiB of the network budget reserved for bounded headers and future receipt-
proven overhead. The decoder checks lengths, arithmetic, concrete types,
cardinality, sorted unique primary identities, duplicate legality, and the body
layout after authenticating the whole-object checksum; reserved header bytes
must remain zero before slices are exposed.

Posting paths are content addressed by the complete encoded bytes. The root
binds path, checksum, encoded bytes, row counts, centroid, and page ordinal.
No footer, directory, or codebook object may be fetched during a measured
query; all such authority is prepared into resident memory.

## Deterministic build and boundary closure

The build starts from one authenticated mutation-free snapshot at a recorded
watermark.

1. Fit a deterministic balanced page router from the registered training
   sample. Training seed, sample ordinals, reductions, ties, and SIMD/scalar
   equivalence are bound into the build receipt.
2. Assign every live row to one primary page. Primary pages are balanced to a
   registered row target and must fit the hard encoded cap before replicas.
3. For every row, inspect a bounded set of neighboring page centroids. Rank
   secondary assignments by the registered distance-ratio closure score, then
   by page ordinal and source ordinal.
4. Each page retains candidates in a bounded heap capped by its primary-row
   count, so all heaps together retain at most one corpus row count and require
   no corpus-wide candidate sort. Admit retained secondary copies in canonical
   strength order until the exact encoded-byte capacity is exhausted. Primary
   rows are never evicted by replicas.
5. Encode and upload one page at a time. Retain only its compact root reference
   after upload. Publish the new root atomically after every referenced page is
   durable.

The diagnostic sweeps primary page targets `{512,1024,2048}`, maximum
assignments per row `{1,2,3}`, and query page counts `{1,2,3,4}`. It may add
`4096` rows only if the encoded cap permits it at the tested width. The root
reports primary and replicated row histograms, rejected replica counts, page
encoded-size distribution, storage amplification, and boundary-score
distribution.

The serving design permits a maximum storage amplification of `2.0x`, while
the preferred production gate is `1.5x`. A factor above `2.0x` fails rather
than silently consuming more storage.

## Query path

For one dense ANN query:

1. Validate dimensionality and metric, normalize once where required, and
   prepare the frozen quantizer lookup tables.
2. Search the resident page-centroid graph and select exactly the best one to
   four page ordinals under the registered router search budget. The physical
   page set is fixed before I/O.
3. Atomically acquire one transient permit for the sum of all page encoded
   bytes, decoded views, result heap capacity, and overlay scan scratch.
4. Issue all page GETs concurrently through the shared fixed-width I/O pool.
   Fetch and checksum failures preserve their typed storage errors. The query
   never launches a dependent replacement page.
5. Decode zero-copy ID/code slices, run the production SIMD code scan, and
   merge page candidates by `(distance, raw ID)`. Replicas are deduplicated by
   raw ID.
6. Apply tombstones and newer overlay versions, scan overlay codes with the
   same query tables, merge top-k, and return approximate distances.

The ordinary path is one wave. A conditional `3+1` two-wave variant may be
tested only as a separately registered fallback after the one-wave candidate
fails quality and only when it still respects four total GETs, one MiB, and the
release p99. It cannot be silently substituted into one-wave evidence.

## Writes, mutations, and compaction

Foreground writes keep the existing WAL-first durability contract. They do
not rewrite posting pages synchronously.

- Puts encode their posting code once using the current generation quantizer
  and enter the bounded resident mutation overlay.
- Deletes enter the overlay tombstone authority.
- Searches reconcile immutable pages against overlay state before returning.
- A materializer folds a sealed overlay prefix into a new page generation and
  publishes one root CAS. Failed builds leave only unreachable content-
  addressed pages for GC.
- The page root watermark defines the ordering boundary: immutable page rows
  are the complete greatest state at or before the watermark; overlay entries
  are strictly newer. Pages therefore need no per-row mutation stamp.

An overlay hard limit retains the current write-backpressure semantics. V23
does not claim write improvement unless the existing lifecycle benchmarks stay
green and publication measurements show no regression in acknowledged-write
latency or throughput.

## RAM and concurrency accounting

Every allocation belongs to one of two existing bounded pools.

Resident authority includes the page router, page references, quantizer,
mutation overlay, and bounded retained cache. Transient authority includes
physical page buffers, decoded views, prepared query tables, dedup state, and
top-k heaps.

The builder and opener compute capacities from actual vector capacities and
encoded lengths, not constants per row. Open fails before serving if the
resident authority exceeds its partition. Query admission reserves the whole
wave before any GET starts, preventing partial-wave hold-and-wait. At most one
permit transfers from physical buffers into decoded/result ownership, and all
failure/cancellation paths release it through RAII.

The diagnostic builder projection conservatively charges six corpus-sized
index vectors in addition to decoded row payloads and one retained replica
candidate per live row. This covers overlapping cell indexes, semantic-split
leaves, owner/replica vectors, and materialized ordinal vectors without relying
on allocator reuse.

The 10M diagnostic records the core builder's conservative working-set
projection and the fresh worker process's independently observed peak RSS, and
projects and measures 3-GiB serving-process usage. The
100M projection includes the complete router, codebook, root, overlays, four
simultaneous maximum pages per admitted query, active-query count, and runtime
overhead. No corpus-wide code plane is assumed resident.

The D2 projection scales the observed page count to exactly 100,000,000 rows
with ceiling division. Its compact root charges a 96-byte header plus 96 bytes
of fixed authority and the full `f32` centroid per projected page. The decoded
catalog charges 32 fixed bytes plus the centroid per page, the bounded
17-level/32-neighbour production router reserves 4,096 bytes per page, and a
separate 512 MiB reserve covers the codebook, overlays, allocator/runtime state,
and other fixed serving authority. Two simultaneous 983,040-byte waves are
added explicitly. The validator recomputes this formula from authenticated row,
page, and dimension authority; a report cannot self-assert a smaller number.

## Ordered diagnostic gates

All diagnostic artifacts are canonical, receipt-bound,
`claim_eligible:false`, and run from Spot instances under the publication
termination contract. A failed stage prevents later paid stages.

### D1: code-fidelity replay

Reuse the authenticated V22 scratch/corpus scan and the same 32 frozen queries.
For every tested quantizer width, score codes over:

- the exact top-2,048 oracle pool, isolating representation loss; and
- the complete current routed pool at registered probe depths, measuring the
  representation plus existing routing interaction.

Emit per-query top-k IDs, approximate distances, exact GT hits, code width,
quantizer/codebook checksum, candidate rows, SIMD CPU time, and aggregate
recall. Scalar and SIMD output must be byte-identical in IDs and within the
registered numeric tolerance in distance.

D1 also binds a canonical BLAKE3 over every ordered query ordinal, vector
length, and raw finite `f32` bit pattern. D2 must reproduce that digest and the
D1 ground-truth IDs before evaluating any page arm.

D1 passes only when at least one width no greater than 64 bytes reaches:

- oracle-pool recall@10 at least `0.990`;
- routed-pool recall@10 at least `0.975` at a corpus-bounded candidate count;
- p99 query preparation plus code scan at most `15 ms` on the registered
  serving CPU; and
- a four-page byte projection no greater than `983,040` bytes.

If no width passes, reject V23 before page construction. Do not compensate by
raising the network or recall limits.

### D2: replicated-page simulation

Run only D1-passing widths. Build candidate balanced page assignments from the
full corpus, add bounded closure replicas, and simulate the exact production
router and page-byte cap for each registered one-to-four-page query arm. Decode
and rank directly from the authenticated immutable page bytes; do not use query
labels, GT IDs, or query-specific layout decisions during the build.

For every query emit selected page ordinals, encoded bytes, candidate rows,
GT coverage before ranking, code-only recall, and CPU time. For every arm emit
the complete page directory and exact encoded sizes, projected compact-root
bytes, storage amplification, projected builder working bytes, and projected
100M RAM. The fresh D2 worker receipt separately binds its
observed process peak RSS; the pure scientific report never labels an
allocation model as a measured peak.

D2 passes only if every frozen query uses at most four pages and 983,040 page
bytes, aggregate recall@10 is at least `0.975`, no query has recall below
`0.8`, storage amplification is at most `2.0x`, projected process RAM is at
most 3 GiB, and p99 CPU is at most 15 ms. Freeze at most three nondominated
passing arms over the deterministic scientific axes recall, bytes, page count,
storage, and RAM. If no arm passes, retain a failing frontier as terminal
negative evidence and stop before D3. CPU remains a hard gate and reported
measurement. Timing jitter can flip pass/fail membership at that boundary, but
it never participates in dominance or ordering.

### D3: real S3 wave replay

Join each relative `pages/{blake3}` reference to the attempt prefix, write the
representative immutable page objects for each frozen D2 arm, and issue
the exact measured one-wave shapes from the registered runtime host. Run at
least 1,000 cold waves per arm with disk cache zero, unique handles, and
positive query-scoped backing I/O. Record service/queue/total latency, bytes,
requests, CPU, RAM, and errors.

D3 passes only when all request/byte/RAM invariants remain exact and cold
p50/p95/p99 are at most `60/100/150 ms`. This is still not a product claim.

## Production qualification

Only a D1/D2/D3 winner authorizes the persistent V23 implementation and fresh
Deep Image 10M build. The production campaign then requires:

- exact source/archive/binary/manifest/protocol/index authority;
- five strict-cold repetitions and five warm repetitions over 1,000 queries;
- recall, p50/p95/p99/max, request and byte distributions, stage telemetry,
  process RSS, and cost per million queries;
- concurrent throughput at registered worker counts without relaxing latency,
  recall, RAM, request, or byte gates;
- lifecycle insert/update/delete/compact non-regression;
- crash, corruption, cancellation, partial-upload, and root-CAS tests;
- a fresh 100M projection and then a real 100M run only after the 10M gate;
- paired Turbopuffer and Amazon S3 Vectors cold measurements under disclosed
  equivalent conditions before any superiority statement.

Every Spot instance is terminated immediately after its terminal receipt. An
interrupted measurement cell is discarded and retried under a new immutable
attempt number.

## Error handling and fail-closed rules

- Unsupported format versions, code widths, metrics, dimensions, or checksum
  authority fail at open.
- A missing, short, oversized, corrupt, or duplicated page fails the query; it
  never returns partial results as complete.
- Router/page-generation mismatch fails before I/O.
- Failure to reserve the full wave returns bounded-resource failure before any
  GET starts.
- Mutation overlay overflow applies write backpressure; it never drops a
  tombstone or serves past its authority.
- D1/D2/D3 negative results are valid completed diagnostics, not infrastructure
  failures and not permission to weaken a gate.

## Testing strategy

Implementation follows test-driven slices:

1. Pure scalar/SIMD quantizer replay and strict result validation.
2. Binary page encoder/decoder with mutation matrices for all lengths,
   offsets, counts, checksums, and concrete types.
3. Deterministic balanced primary assignment and capped closure replication.
4. Router selection and exact four-page/byte invariants.
5. One-wave admission, typed errors, cancellation, and concurrent memory
   bounds.
6. Overlay reconciliation, duplicate replicas, tombstones, and generation
   publication.
7. Tiny end-to-end local object-store tests, then claim-ineligible AWS D1/D2/D3.

Focused tests run while iterating. Strict Clippy, rustfmt, Python static gates,
affected integration tests, and one repository-wide assurance run only after a
stable diff. No full local suite starts under swap pressure.

## Explicit non-goals for the MVP slice

- No legacy V20/V21/V22 reader or migration tool.
- No S3 Express, local serving replica, CDN, or persistent disk cache.
- No graph walk over S3.
- No exact-distance guarantee in the low-latency ANN response.
- No redesign of sparse, text, late-interaction, or point-lookup formats in
  this slice; their existing paths must remain correct and green.
- No publication claim from 32-query diagnostics.

The MVP succeeds when one fresh V23 dense-ANN format passes the full 10M cold,
quality, memory, throughput, lifecycle, and paired-competitor gates. Other
feature families remain part of the broader release objective but cannot delay
the cold dense-ANN architecture decision.
