# V23 Residual RaBitQ Row-Cover Falsifier Design

**Date:** 2026-08-31

**Status:** Approved design for a bounded architectural falsifier

## Decision

Replace the rejected leaf-to-page incidence selector with a row-granularity
residual RaBitQ scorer. The existing query-independent 65,536-leaf centroid
tree prunes the row set. Within the probed leaves, the scorer estimates a
distance for every resident row code, retains a bounded global row ranking,
and applies the already-validated deterministic at-most-eight-page cover over the
primary and replica page assignments.

This is a new prerelease format. It has no compatibility reader, alias,
migration path, or fallback to the rejected SRHT-PQ or incidence artifacts.
Bulk artifacts use Arrow IPC or Parquet; small receipts and results use strict
typed canonical JSON.

## Evidence and failure mechanism

The current 384-row page layout is not the primary quality failure. Its exact
eight-page oracle recovers 318 of 320 development neighbors: 993,750 ppm
aggregate recall, 900,000 ppm minimum-query recall, and every oracle-reachable
row. Exact f16 row ranking reached the same ceiling.

The compact selectors lose the row identities needed to construct that page
cover:

- width-12 exact-global ADC reached 671,875 / 100,000 / 676,100 ppm;
- K32 fixed page prototypes failed;
- the leaf-incidence screen reached only 581,250 / 100,000 / 584,905 ppm;
- exhaustive scoring of all 65,536 leaves matched the best tree-beam result.

The last equality rules out tree traversal as the causal incidence failure.
The incidence representation discards query-dependent residual distance
within each leaf. More leaf probes, a larger posting cap, or a longer timeout
cannot restore that information.

## Alternatives considered

### Residual RaBitQ row scoring — selected

For each row, store a 96-bit sign code for a randomly rotated normalized
leaf residual, the residual norm, the quantized-vector alignment denominator,
and the two page assignments. RaBitQ supplies an unbiased inner-product
estimator and an explicit error bound rather than the biased blockwise error
observed in the existing PQ selector. The query path scores rows, not pages or
leaf counts.

This is the smallest representation that changes the failed information
boundary while remaining below 3 GiB at 100M rows.

### Multi-bit residual quantization

Two or more bits per dimension may improve distance accuracy, but a 24-byte
code plus two 32-bit page assignments and estimator scalars exceeds the 3-GiB
100M-row envelope. It is not the first falsifier. A later format may use it
only if a measured one-bit error boundary proves the extra bits necessary and
a different exact memory budget is approved before evaluation.

### Disk graph or SPANN-style closure routing

Graph and closure methods are credible production families, but they combine
row ranking, disk layout, traversal, and extra I/O in one experiment. Under an
exact eight-page request limit, graph-node reads also compete with result-page
reads unless the graph is resident, which violates the current RAM envelope at
100M rows. This path is too causally ambiguous for the next falsifier.

### Richer page summaries

Centroids, primary-plus-replica centroids, and K32 clustered page prototypes
have already failed. Adding more fixed page summaries repeats the same smooth
page-level approximation and is rejected without another run.

## Representation

### Query-independent geometry

Reuse only the authenticated query-independent 65,536-leaf centroid tree and
the primary/replica page assignments. A fixed manifest seed generates one
96-by-96 orthogonal rotation. Neither development queries, ground truth,
selected pages, nor holdout data may influence the tree, rotation, codes, row
order, or page assignments.

For row vector `x` assigned to centroid `c`:

1. compute residual `r = x - c`;
2. store `norm = ||r||` as finite nonnegative `f32`;
3. rotate and normalize `r` using the registered orthogonal matrix;
4. store the 96 signs as 12 bytes;
5. store the finite positive RaBitQ alignment denominator as `f32`;
6. store primary and replica page ordinals as `u32`, using `u32::MAX` only
   when the authenticated row has no replica.

Rows are ordered by leaf ordinal and canonical record identity. A 65,537-entry
`u64` offset vector identifies each leaf range, so leaf identity is implicit
and no per-row leaf field is resident.

An exact zero residual uses the all-zero sign code, zero norm, and canonical
alignment `1.0`; query scoring bypasses the estimator and returns the exact
centroid distance. Every nonzero residual requires a positive norm and
alignment.

### Cross-language artifacts

The falsifier emits one Arrow IPC file with zero-copy fixed-width columns:

- `sign_code: fixed_size_binary[12]`, non-nullable;
- `residual_norm: float32`, non-nullable;
- `alignment: float32`, non-nullable;
- `primary_page: uint32`, non-nullable;
- `replica_page: uint32`, non-nullable.

The leaf offsets, f16 centroids, and f32 rotation matrix are separate Arrow IPC
arrays bound by the same manifest. Query and ground-truth bulk data remain
Parquet. Construction receipts, execution receipts, and scientific results are
strict canonical newline JSON. Every role binds URI, SHA-256, BLAKE3 where
registered, exact byte length, schema fingerprint, source commit, source
archive, dataset identity, seed, and predecessor receipt.

The historical-page adapter and Rust constructor communicate through one
standard Arrow IPC stream with non-nullable `canonical_record_id: binary`,
`vector: fixed_size_list<float32>[96]`, `page_ordinal: uint32`, and
`is_primary: bool` fields. Each authenticated BVP2 occurrence appears once;
the Rust external ID merge derives exactly one primary plus zero or one
replica before encoding a unique row. The stream is never persisted as a
corpus copy and is not a production index format.

Manifests authenticate existing inputs and declare an ordered output-role set
plus one immutable output URI prefix. They never pretend to know output
digests or lengths before execution. A terminal receipt authenticates every
produced output with its exact URI, SHA-256, length, and predecessor manifest
digest. The development manifest is generated only after the construction
receipt and Arrow outputs exist, so all nine development inputs are exact.

The serving implementation maps Arrow buffers directly. It does not invoke a
dynamic loader, copy a runtime, deserialize an old schema, or use a hidden
storage client.

## Exact 100M-row memory projection

The fixed serving projection is:

| Resident object | Bytes |
|---|---:|
| 100M sign codes, 12 bytes/row | 1,200,000,000 |
| 100M residual norms, 4 bytes/row | 400,000,000 |
| 100M alignment values, 4 bytes/row | 400,000,000 |
| 100M primary page ordinals, 4 bytes/row | 400,000,000 |
| 100M replica page ordinals, 4 bytes/row | 400,000,000 |
| 65,537 leaf offsets, 8 bytes each | 524,296 |
| 65,536 f16 centroids, 96 dimensions | 12,582,912 |
| authenticated centroid tree | 40,369,836 |
| f32 96-by-96 rotation | 36,864 |
| registered runtime/headroom reserve | 64 MiB |
| **Total** | **2,920,622,772** |

The total is 300,602,700 bytes below the 3-GiB ceiling of 3,221,225,472
bytes. Arrow metadata and alignment must fit inside the 64-MiB reserve; a
measured projection above the total fails closed.

## Query algorithm

1. Authenticate the request and resident artifact identities.
2. Transform the query once with the registered rotation.
3. Use the centroid tree to return the fixed leaf-probe ladder `32, 64, 128`.
4. Apply bounded development limits derived only from the authenticated
   indexed row count and fixed page geometry. The retained-row limit is the
   production constant 4,096 because heap capacity and the fixed 384-row page
   geometry do not scale with corpus cardinality. The scored-row limit remains
   `ceil(262,144 * indexed_rows / 100,000,000)`, because scanned work does scale
   with corpus cardinality. At 9,990,000 rows these are exactly 4,096 and
   26,189; at 100M they are exactly 4,096 and 262,144. If a
   ranked leaf prefix would exceed the scored-row limit, truncate the prefix
   before the first overflowing leaf and record both the requested and actual
   leaf counts. Failing the whole cell instead of applying this deterministic
   prefix is forbidden.
5. For each probed leaf, form the rotated query residual and quantize its 96
   components into codes `q_i in 0..=15` with registered `minimum` and `step`.
   Build twelve query-specific 256-entry tables, where table `j` maps an eight-
   bit row sign mask to the sum of the corresponding eight `q_i` values. For a
   row sign code, compute `sum_sign = 2*popcount(sign)-96` and
   `sum_sign_code = 2*sum(q_i where sign_i=1)-sum(q_i)`, then reconstruct the
   dot as `invsqrt96 * (minimum*sum_sign + step*sum_sign_code)`. This is twelve
   byte-table lookups per row; expanding a sign code to `[f32; 96]`, performing
   96 floating operations, or recomputing a duplicate scalar score in the
   serving loop is forbidden. A direct 96-component scalar oracle must agree
   within the fixed primitive forward-error bound, and both paths must select
   identical pages exactly. Error against the unquantized scalar f64 distance is
   recorded as scientific evidence and is governed by the recall gates rather
   than an invented dataset-independent cutoff. Ties are deterministic
   `(distance, row_ordinal)`.
6. Retain only the fixed production best-row limit in a bounded heap. Full
   ranked-row allocation or sort is forbidden.
7. Apply an exact deterministic recall-at-ten page cover to only the first ten
   ranked rows and their two page assignments. Choose at most eight unique
   pages to maximize the number of those ten rows covered; ties use the
   lexicographically smallest strictly ordered page vector. Rows below rank ten
   are forbidden from contributing votes because they are outside the quality
   metric and can otherwise displace a page containing a top-ten row. If fewer
   than ten rows were scored, cover every available ranked row under the same
   rule and let the missing rows self-penalize recall. A saturated cover is
   recorded at its natural width and is never padded with an unearned page.
8. Emit the ordered pages plus complete causal and resource evidence.

At 100M rows, 128 balanced leaves contain about 195,313 rows. The hard scan
cap is 262,144 rows. A maximal query reads at most 7,340,032 row bytes, performs
one 96-component four-bit query quantization per probed leaf and at most
3,145,728 byte-table lookups across 262,144 rows, and considers at most 8,192
page assignments in the cover. The preflight measures this complete kernel;
no operation-count estimate substitutes for the 15-ms p99 gate. Tree work
remains the already-measured bounded centroid subset rather than a
65,536-centroid scan.

## Causal falsifier

One query-independent corpus stream constructs all codes. The same immutable
code artifact then evaluates paired controls on only burned development
ordinals 0--31:

1. **exact-f16 exhaustive control**: known ceiling; must reproduce 318 oracle
   hits;
2. **exact-f16 tree controls**: score the identical `32/64/128` ranked-leaf
   prefixes and identical scale-normalized row heaps used by RaBitQ;
3. **RaBitQ exhaustive diagnostic**: records the global-code ceiling but is
   not allowed to reject a passing serving cell because a fixed global heap is
   a much harder selection problem than a tree cell;
4. **RaBitQ tree candidate**: the only serving candidate.

Construction also emits a development-only Arrow fixed-size-list f16[96] row
plane in the identical leaf/record order. It supplies the two exact controls
and is explicitly excluded from the serving projection and production index.
The execution process authenticates it but never exposes it to the RaBitQ
candidate scorer.

The classification is outcome-blind and cell-paired:

- exact exhaustive differs from 318: `authority-stop`;
- for a probe cell, exact tree fails: `tree-pruning-rejected` for that cell;
- exact tree passes but the paired RaBitQ tree cell fails:
  `rabitq-estimator-rejected` for that cell;
- any tree RaBitQ cell reaches the development ceiling:
  `development-candidate-accepted`.

The exhaustive RaBitQ diagnostic is always serialized with its metrics, but it
has no rejection precedence over a passing tree candidate. This avoids a false
negative caused by retaining the same number of rows from all 9.99M rows rather
than from the roughly 4.9K/9.8K/19.5K rows in a `32/64/128` development cell.

The fixed recall-at-ten reducer is a methodology correction, not a tunable
parameter: ten is the registered recall depth and eight is the immutable page
budget. The prior authenticated row-vote F0 evidence independently proves that
the exact top ten contain 318/318 oracle-reachable hits, while the first
reciprocal-rank development screen lost 23 of them after allowing ranks
11--4,096 to vote. No reducer ladder or result-dependent cutoff is permitted.
After this single amended development screen, the smallest passing probe count
wins; ties use the smallest scanned-row count and then the lowest leaf ordinal
sequence.

## Gates

### Development selection

A candidate must recover all 318 oracle-reachable hits on the burned 32-query
cohort: 993,750 ppm aggregate recall, 900,000 ppm minimum-query recall, and
1,000,000 ppm oracle attainment. It must also satisfy:

- between one and eight selected pages, with the achieved count recorded by
  the canonical page list and no padding or duplicate page;
- at most the scale-normalized scored-row limit (26,189 at 9.99M and 262,144
  at 100M);
- at most the fixed 4,096 retained-row limit and 8,192 page assignments;
- projected serving bytes at most 2,920,622,772;
- scalar/optimized selected pages exactly equal;
- construction receipt and D2 truth bind the same index identity, page
  namespace, and exact page count 28,282; every stored page ordinal is below
  that count;
- per-query requested/actual leaf counts, scored/retained rows, estimator
  error, kernel elapsed nanoseconds, and selected pages are present and
  independently recomputed by result validation;
- no page-body or holdout read;
- no nonfinite input, estimator, or score;
- `claim_eligible=false`.

### Sealed holdout and release

Development success authorizes only a separately preregistered sealed holdout
evaluation. The holdout must reach at least 991,000 ppm aggregate recall,
900,000 ppm minimum-query recall, 995,000 ppm oracle attainment, and 15 ms
resident CPU p99 over at least 10,000 raw timing samples. These gates exceed
the existing 99.04% BORSUK recall result rather than merely clearing the old
97.5% floor.

Literal 100% recall is impossible on the current frozen development layout:
its exact eight-page oracle is 318/320. Reaching 100% while retaining exactly
eight page reads requires a later query-independent page-layout rebuild. This
falsifier first demands perfect attainment of the current layout's reachable
ceiling; it must not claim perfect end-to-end recall.

No D3, competitor claim, production default, or paid serving campaign is
authorized until the sealed holdout and resident timing gates pass.

## Construction and execution boundaries

The construction phase may perform one authenticated streaming pass over the
frozen page roster and page bodies. A phase-private Python adapter validates
the immutable historical page envelope and emits the standard Arrow occurrence
stream; it cannot name queries or outputs. Rust consumes that stream once,
deduplicates replicas by canonical record identity with bounded external
sorting, and writes only canonical Arrow/JSON artifacts. The construction plan
binds the tree receipt, tree, page roster, page-generation namespace, source
archive, rotation seed, expected page/occurrence/unique-row counts, ordered
output roles, and output prefix. The receipt binds all actual output bytes.
Construction stops on RSS, PSI, swap, timeout, or progress failure. Spot is
required for new AWS work. The instance terminates immediately after a
terminal receipt.

The execution phase consumes only the constructed Arrow artifacts, tree,
frozen D2 report, and query Parquet. It has no page-body client. Development
and holdout are separate capabilities and separate processes; development
cannot name, list, HEAD, or read the holdout object.

## Verification strategy

Implementation follows strict RED/GREEN slices:

1. typed authority, Arrow schemas, receipts, and canonical result;
2. deterministic rotation and RaBitQ scalar estimator;
3. twelve-byte query-LUT scorer differential tests, including ties, zeros,
   subnormals, nonfinite values, reversed blocks, every byte mask, and scalar
   oracle agreement;
4. scale-normalized scored limits, the fixed production retained limit,
   deterministic lowest-ranked-leaf
   truncation, bounded row heap, and exact page-cover reuse;
5. streaming constructor with duplicate, order, digest, and interruption
   mutations;
6. paired exact/RaBitQ tree evaluator, diagnostic exhaustive evidence,
   classification-precedence mutations, shared serving-call-path proof,
   per-query timing/error evidence, and D2 page/index authority mutations;
7. phase-separated controller, cleanup, terminal, and no-holdout/no-page
   capability tests;
8. focused gates, strict workspace Clippy, full locked workspace/all-targets,
   Python discovery, Ruff, syntax checks, and document validation.

Scientific execution is a separate terminal step after source, binary,
manifest, inputs, cost, resource stops, and cleanup are frozen. A failed
architecture is recorded and rejected; its gates are never weakened and its
run is never repeated with tuned parameters.

## Research basis

RaBitQ motivates the unbiased randomized binary estimator and error bound:
<https://arxiv.org/abs/2405.12497>. Its multi-bit extension is described at
<https://arxiv.org/abs/2409.09913>. SPANN and DiskANN remain comparison
architectures, not imported implementations: this falsifier deliberately
isolates compact row scoring before coupling the result to a new disk graph or
posting-list closure layout.
