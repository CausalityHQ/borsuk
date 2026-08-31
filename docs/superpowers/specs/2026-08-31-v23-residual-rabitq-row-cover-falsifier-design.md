# V23 Residual RaBitQ Row-Cover Falsifier Design

**Date:** 2026-08-31

**Status:** Approved design for a bounded architectural falsifier

## Decision

Replace the rejected leaf-to-page incidence selector with a row-granularity
residual RaBitQ scorer. The existing query-independent 65,536-leaf centroid
tree prunes the row set. Within the probed leaves, the scorer estimates a
distance for every resident row code, retains a bounded global row ranking,
and applies the already-validated deterministic eight-page cover over the
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
4. Reject a cell before scoring if its leaf ranges contain more than 262,144
   rows at the 100M projection.
5. For each probed leaf, form the rotated query residual and quantize its 96
   components with the fixed four-bit query quantizer described by RaBitQ.
   Estimate every candidate residual distance with the registered SIMD
   estimator. The implementation must agree with an unquantized scalar f64
   reference within the preregistered estimator bound and use deterministic
   `(distance, row_ordinal)` ties.
6. Retain only the best 4,096 rows in a bounded heap. Full ranked-row
   allocation or sort is forbidden.
7. Apply the existing deterministic reciprocal-rank page cover to the two page
   assignments, selecting exactly eight unique pages. Ties are
   `(gain, reciprocal_rank_sum, page_ordinal)` with the registered directions.
8. Emit the ordered pages plus complete causal and resource evidence.

At 100M rows, 128 balanced leaves contain about 195,313 rows. The hard scan
cap is 262,144 rows. A maximal query reads at most 7,340,032 row bytes, performs
one 96-component four-bit query quantization per probed leaf and at most
262,144 fixed 96-component bitwise/SIMD estimations, and considers at most
8,192 page assignments in the cover. The preflight measures this complete
kernel; no operation-count estimate substitutes for the 15-ms p99 gate. Tree
work remains the already-measured bounded centroid subset rather than a
65,536-centroid scan.

## Causal falsifier

One query-independent corpus stream constructs all codes. The same immutable
code artifact then evaluates four strictly ordered controls on only burned
development ordinals 0--31:

1. **exact-f16 exhaustive control**: known ceiling; must reproduce 318 oracle
   hits;
2. **exact-f16 tree control**: isolates loss from the `32/64/128` leaf probes;
3. **RaBitQ exhaustive control**: isolates quantization error from tree
   pruning;
4. **RaBitQ tree candidate**: the only serving candidate.

Construction also emits a development-only Arrow fixed-size-list f16[96] row
plane in the identical leaf/record order. It supplies the two exact controls
and is explicitly excluded from the serving projection and production index.
The execution process authenticates it but never exposes it to the RaBitQ
candidate scorer.

The classification is outcome-blind:

- exact exhaustive differs from 318: `authority-stop`;
- exact tree fails while exact exhaustive passes: `tree-pruning-rejected`;
- exhaustive RaBitQ fails while exact exhaustive passes:
  `rabitq-representation-rejected`;
- exhaustive RaBitQ passes but every tree cell fails:
  `tree-rabitq-composition-rejected`;
- a tree RaBitQ cell reaches the development ceiling:
  `development-candidate-accepted`.

No parameter may be added after opening development results. The smallest
passing probe count wins; ties use the smallest scanned-row count and then the
lowest leaf ordinal sequence.

## Gates

### Development selection

A candidate must recover all 318 oracle-reachable hits on the burned 32-query
cohort: 993,750 ppm aggregate recall, 900,000 ppm minimum-query recall, and
1,000,000 ppm oracle attainment. It must also satisfy:

- exactly eight selected pages;
- at most 262,144 scored rows;
- at most 4,096 retained rows and 8,192 page assignments;
- projected serving bytes at most 2,920,622,772;
- scalar/optimized selected pages exactly equal;
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
frozen unique primary rows. It must deduplicate replicas by canonical record
identity, use bounded external sorting, checkpoint only canonical Arrow/JSON
artifacts, and stop on RSS, PSI, swap, timeout, or progress failure. Spot is
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
3. optimized scorer differential tests, including ties, zeros, subnormals,
   nonfinite values, reversed blocks, and scan-cap rejection;
4. bounded top-4,096 row heap and exact page-cover reuse;
5. streaming constructor with duplicate, order, digest, and interruption
   mutations;
6. causal four-control evaluator and classification mutations;
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
