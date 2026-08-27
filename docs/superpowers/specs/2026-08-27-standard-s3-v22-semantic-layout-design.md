# Standard-S3 V22 Resident-Code Semantic Layout

**Status:** Proposed claim-ineligible diagnostic ladder. No V22 persistent
format, production build, or publication claim is authorized until every
prerequisite stage passes.

**Predecessor:** V21 is rejected by immutable evidence rooted at
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-77f321472e3f25c63957de46/runtime-v21-feasibility/arms/0000/attempts/0001/`.
Its best arm achieved only `0.1119` recall@10 despite per-query maxima of four
requests and `1,042,944` physical bytes, with `208,209,490` projected serving
bytes. V22
retains the quality, request, byte, and RAM gates and replaces the registered
V21 architecture.

**Supersedes:** `docs/research/cold-read-latency-design.md` and the rejected V21
design for the next pre-release dense-ANN format.

## Objective

Serve strict-cold dense ANN from S3 Standard only, with no S3 Express, local
serving tier, CDN, query-result cache, or query-populated persistent cache.
Deep Image 10M must meet all of:

- recall@10 at least `0.975` and pre-exact GT coverage at least `0.990`;
- at most four actual S3 GETs and `1,048,576` physical bytes per query;
- projected serving RSS at most `768 MiB`;
- cold p50/p95/p99 at most `60/80/100 ms`, with a stretch p99 target of
  `75 ms`;
- no superiority claim until an honest paired cold run beats each disclosed
  competitor p99 by at least 20% under equivalent conditions.

The latency target follows from one parallel S3 wave. It never substitutes for
the request, byte, memory, or quality gates.

## What V21 proved

V21 represented each physical 32/64-row region by one coarse code and spread,
then admitted exact payload at roughly 96-KiB bundle granularity. The best arm
selected about 945 rows per query, but 535 of 1,000 queries contained none of
their ten ground-truth IDs. Its request, byte, and projected memory use already
fit, but the sample combines routing, representation, physical layout, and
truncate-on-first-rejection admission. V22's ordered stages exist to attribute
those losses instead of assuming their source.

One exact Deep Image row contains 384 vector bytes. The 1-MiB cap can carry
roughly 2,300--2,500 complete rows after IDs and format overhead. V20 reached
high recall through the same coarse routing generation by scanning row-level
codes, although it fetched those codes inefficiently from S3. V22 tests whether
keeping compact row-level residual codes resident and semantically repacking
exact rows can select the useful 500--2,000 candidates in at most four ranges.

The V21 fixed 40,000,000-byte selector cap is rejected with V21. The only
demonstrated positive control is V20's 64-byte row code. V22 registers eligible
residual-code widths `{8, 12, 16, 24, 32}` bytes per corpus row and one explicit
memory-ineligible 64-byte positive control while retaining the real `768 MiB`
serving-RSS cap. An eligible width passes only when capacity is computed from a
preparation baseline that excludes replaced V20 code/root authority and
includes every V22 resident allocation; selector memory is not free or hidden.
The 64-byte control can validate the pipeline but can never be eligible.

## Likely V22 production layout

This section is a hypothesis until the diagnostic ladder passes.

1. Keep the authenticated coarse IVF router and immutable-generation model.
2. Encode every row's residual from its primary routing centroid into a compact
   resident code plane. Codes are ordered by authenticated row ordinal and
   indexed by resident cell spans.
3. Partition rows within a cell into exactly `ceil(cell_rows/M)` deterministic
   semantic microclusters of at most `M in {32,64}` rows using recursive
   balanced bi-pivot splits. Start from lexicographic authenticated raw
   record-ID byte order. At each node choose the row farthest from the
   minimum-ID row, then the row farthest from that pivot, with raw record-ID
   bytes as the distance tie-break. Compute each row's distance-difference
   score once per level, stable-sort by `(score, raw record-ID bytes)`, and
   split at the balanced median.
   Recurse until the leaf fits. Start with the leaf containing the minimum
   record ID, then order leaf centroids by a deterministic nearest-neighbor
   chain with minimum leaf record ID as the distance tie-break. Split work is
   bounded by `O(n log(n/M) * dimensions + n log^2(n/M))`; leaf ordering is
   `O((n/M)^2 * dimensions)` per cell, never corpus-wide quadratic clustering.
4. For the cross-cell authority, start with the smallest cell index and order
   remaining cell centroids by the same nearest-neighbor rule, using cell index
   as the distance tie-break. This adds bounded `O(C^2 * dimensions)` diagnostic
   work for `C` authenticated cells. Order those cells and microclusters into
   content-addressed group objects so a ranked row prefix expands to a small
   number of contiguous exact-row ranges. The within-cell authority instead
   retains the authenticated V20 cell order.
5. At query time, route in resident memory, scan only the routed resident code
   spans, rank rows, expand admitted rows to microcluster ranges, coalesce under
   the exact request/byte/amplification limits, issue one concurrent Standard-S3
   wave, and exact-rerank decoded rows.

Region representatives may remain only as an optional cell/microcluster skip
filter. They cannot replace the row-level ranker. No query-time selector,
footer, or code-plane GET is permitted.

Multi-assignment is not part of this diagnostic. If primary routing fails
Stage L's `0.995` GT-cell-coverage gate, V22 stops and a separate design must
preregister its resident-code, posting,
storage-replication, duplicate-fetch-byte, and bucket-skew products before it
can run.

## Phase 0 diagnostic ladder

Reuse the exact authenticated mutation-free V20 Deep Image generation and the
publication query/ground-truth authority. All stages are read-only and emit
canonical, receipt-bound, `claim_eligible:false` evidence. A failed stage stops
later stages rather than relaxing a gate.

### L: routing-and-layout census

Make one authenticated corpus pass before training a new quantizer. For every
publication query, compute the true exact prefixes
`{10, 256, 512, 1024, 1536, 2048}`. The exact top ten must match the frozen
ground truth. Decode and bind the generation's authenticated routing-cell
count; Deep Image 10M is expected to assert exactly 4,096, while other profiles
are not forced through that literal. For every row in every prefix, record the
one-based routing rank of its primary cell over that complete cell order. This
single rank subsumes a probe sweep and yields coverage and routed-row curves
for any prefix.

Project four layout families, producing seven concrete layout authorities,
without writing a format:

- the authenticated V20 physical ranges;
- the authenticated V20 one-dimensional two-pivot row order repacked into
  32/64-row units, isolating granularity from semantic ordering;
- deterministic 32/64-row metric microclusters while retaining current
  cross-cell placement;
- the same microclusters plus deterministic neighboring-cell group order.

The authorities are V20 physical; V20 two-pivot repacked at 32 and 64 rows;
semantic-within-cell at 32 and 64 rows; and semantic-cross-cell at 32 and 64
rows. With six exact prefixes this is exactly 42 `(layout, prefix)` pairs.
The two-pivot control uses the current V20 per-cell one-dimensional projection
and cuts that total order into full `M`-row units plus one final partial unit;
it does not use the recursive semantic partitioner. Its anchor and tie order use
the authenticated raw record-ID bytes, and its distance/projection reductions
use the same `f32` kernels as the V20 builder.

For every `(layout, prefix)` pair, expand exact rows to complete physical units
and run the exact coalescer. Emit per-query ranges, primary useful/selected/
physical bytes, amplification, primary-cell routing ranks, cell boundaries,
selected rows-per-range histogram, contiguous-run-length histogram, explicit
speculative bytes, authenticated projected-object path/length/checksum authority,
and limiting bound.
Report within-cell and cross-cell deltas separately. Census planning first
measures the complete prefix under the registered amplification allowance and
then classifies it as `eligible`, `bytes`, `requests`, or `amplification`;
budget-negative arms therefore emit complete evidence instead of failing the
42-arm run. Each projected unit binds its exact decoded row bytes independently
of its authenticated encoded length. Compression may make packing purity exceed
one, but cannot hide decoded-row under-sizing.

A layout/prefix pair advances only if routing reaches at least `0.995` GT cell
coverage with at most 512,000 routed rows for every query and every query's
exact prefix fits four primary ranges, 1 MiB primary physical bytes, and 2x
primary amplification. The 2,048-row arm has only about 1.33x physical headroom
after 384-byte vectors, before ID/format overhead; evidence reports the exact
prefix packing purity for every prefix instead of treating all arms as having
the same 2x allowance. If no pair advances, stop V22 before code training.

### G2: resident residual-code sweep

Run only the routing budgets, prefix sizes, and layouts that Stage L proves
physically viable. Train diagnostic-only residual codebooks at eligible widths
`{8, 12, 16, 24, 32}` bytes per row plus one memory-ineligible 64-byte positive
control. Each fitted codebook is a current-source diagnostic artifact with its
own digest; it is not falsely attributed to the historical base index.

One scan per `(width, routed-cell count)` produces a complete stable ranking.
Candidate prefixes and 32/64-row layout expansion are cheap views of that same
ranking; they do not rescan per arm. For every view and query:

1. record GT and true-prefix approximate ranks;
2. expand ranked rows to semantic microclusters under explicit admission
   policies `{truncate, skip}`;
3. coalesce under four primary requests, 1 MiB primary physical bytes, and 2x
   primary amplification; rejected rows cannot mutate already accepted ranges;
4. exact-score the logically fetched rows;
5. record representation coverage, layout coverage, final recall, selected
   rows, range/run histograms, primary bytes, CPU time, and limiting bound.

An arm is 10M-eligible only if all queries fit the physical gates, aggregate GT
coverage is at least `0.990`, recall@10 is at least `0.975`, measured p99
routing+scan+decode+dedup+exact CPU is at most `15 ms`, and projected serving
RSS is at most `768 MiB`. RSS is computed as measured V20 preparation bytes
minus explicitly enumerated replaced resident allocations plus real V22
capacities and query transient capacities; an allocation is never subtracted
merely because the design intends to replace it.

The 100M projection has a separate exact `3 GiB` (`3,221,225,472`-byte) process
gate. At 100M, a 32-byte plane occupies `3,200,000,000` bytes (`2.98 GiB`) and
leaves only `21,225,472` bytes for routing authority, directories, allocator
overhead, and query transients. It is eliminated only when the exact measured
non-code capacity exceeds that residual; no decimal/GiB shortcut decides the
gate. A 24-byte plane may also fail after required authority. Report 10M and
100M eligibility independently. The 64-byte control is never eligible.

Freeze a bounded Pareto set of at most three eligible arms over resident bytes,
primary ranges, primary bytes, and CPU. Do not select the lowest-memory arm
before measuring its actual S3 wave.

### D2: S3 Standard wave replay

Replay the measured range-width/size shapes for every frozen Pareto arm as real
S3 Standard GET waves. Run on the registered serving instance class and cgroup,
not the build-class diagnostic host. Every arm tests hedge `off`. Delays
`{20 ms, 35 ms}` are registered only when the primary plan uses at most three
GETs and `primary bytes + largest hedgeable range bytes <= 1,048,576`, thereby
reserving one request and the full possible duplicate payload. Four-primary-GET
arms have no hedged variant; this is explicit evidence, not a silently skipped
measurement or a failure of the unhedged arm. Primary amplification excludes
the duplicate; hedge request and bytes are separate evidence and still count
toward the total four-GET and 1-MiB network caps.

The prior same-host four-wide 64-KiB S3 Standard wave measured `57.950 ms` p99;
it is a registered baseline, not a substitute for D2. Report both full
end-to-end query latency and the diagnostic additive decomposition. Pass only
when the real end-to-end p50/p95/p99 meets `60/80/100 ms`. This remains
claim-ineligible evidence, not a product result.

## Evidence and authority

The diagnostic must:

- bind current source/archive/binary and the complete historical base-index
  authority independently;
- authenticate every decoded row, codebook, root, query, and truth identity;
- stream or bound the corpus working set; the diagnostic host's large memory
  allowance is disclosed and never used as the serving projection;
- emit separate canonical L, G2, and D2 sample/aggregate artifacts with exact
  schema, ordering, cardinality, finite numeric, and recomputation checks;
- let Python independently parse and recompute evidence before atomic result
  publication;
- bind raw artifact digests, result digest, Spot instance, cgroup memory peak,
  zero swap/OOM, purchase option, and terminal state in the receipt;
- terminate compute immediately after its terminal marker.

Incomplete artifacts are never inspected or promoted. No stage mutates the
base manifest, publishes a V22 object, or validates as an ordinary recall run.

## Production qualification after eligibility

Only after Stage L, G2, and D2 pass:

- introduce a new format marker with no V20/V21 compatibility layer;
- build semantic groups and the resident code plane sequentially under bounded
  memory, publish complete content-addressed objects before the manifest, and
  leave failures unreachable;
- derive 100M routing/catalog size from measured cell skew and quantize or
  hierarchically route centroids so the complete resident authority fits;
- run five serial strict-cold Deep Image 10M repetitions from the frozen source;
- publish a separate fresh-open + serving-preparation + first-query latency and
  RSS distribution; never substitute it for the query-only strict-cold number;
- require build/ingest/materialization/mutation non-regression and 1/4/16/32
  worker throughput evidence;
- prove every cold sample uses positive query-scoped backing GETs/bytes, zero
  disk-cache service, and a fresh serving handle;
- then run paired S3 Vectors and Turbopuffer cold comparisons with the same
  corpus, queries, k, quality floor, client region, and disclosed first-pass
  definition.

## Required verification

1. Routing-cell count, ceiling coverage, and row counts are exact on hand
   fixtures and the authenticated generation; producer and validator use the
   same bound.
2. Residual codes are deterministic across order, concurrency, and spill;
   prepared scoring matches existing metric kernels.
3. Semantic microclusters and neighboring-cell order are deterministic and
   bounded; no quadratic corpus-wide clustering is permitted.
4. Perfect-selector oracle ranges and bytes are independently hand-derived.
5. Ranked-prefix planning never exceeds four GETs, 1 MiB, or 2x amplification
   and never lets unselected rows poison physical ranges.
6. Exact serialized/sized payload bytes agree for empty, singleton, skewed,
   maximum-width, and compressed data.
7. Malformed codes, rows, offsets, lengths, hashes, order, duplicates, and
   aggregate counts fail before unsafe allocation or scoring.
8. One query owns one bounded wave; success, error, cancellation, and hedge
   paths release all tasks and permits.
9. Capacity accounting covers real vector/string/container allocations for
   routing, resident codes, directory, encoded responses, decoded rows,
   deduplication, exact scoring, and concurrent workers.
10. Diagnostic reads leave manifest bytes and the object roster unchanged.
11. Python validators mutation-test every evidence field and recompute every
    aggregate before publishing completion.
12. Focused tests, formatting, strict workspace Clippy, repository assurance,
    and read-only cross-provider review are green before paid execution.

## Non-goals

- No S3 Express, local serving tier, CDN, or persistent query cache.
- No V20/V21 compatibility, migration, or dual reader.
- No quality/resource-gate relaxation.
- No publication claim from diagnostic evidence.
- No V22 format build before the routing, layout, ranker, and S3-wave stages
  pass in order.
