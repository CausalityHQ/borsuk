# V32 Virtual Geometric Page Repacking Design

## Decision

Before rebuilding any corpus or reading any page body, V32 will replay one
query-independent physical-layout change over the authenticated one-million-row
resident artifacts: partition every routing microleaf independently into
deterministic balanced geometric pages of at most 480 rows. The replay preserves
the hierarchy, routing centroids, code-parent association, 24/48-byte PQ codes,
global 768-microleaf prefix, 262,144-code ceiling, 12,288 retained candidates,
candidate order, and page reducer. The frozen 16-page arm is retained only as a
diagnostic control; advancement requires first-distinct selection of the first
eight virtual pages from the same ordered candidate replay. Only
logical-row-to-page membership changes.

The diagnostic is claim-ineligible and reads zero page bodies. It must
authenticate and reproduce the complete frozen current-layout terminal before
reporting either treatment. No layout is materialized unless the eight-page
treatment reaches perfect containment.

## Governing evidence

Source `af05a46b75212c894fc5208aa768910552ed083d` produced the immutable
one-million-row Deep Image terminal at
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/af05a46b75212c894fc5208aa768910552ed083d/attempts/v32-deep-1m-global-containment-l768-20260905T020228Z-a0001/TERMINAL.json`.
The 262,537-byte object has SHA-256
`88226dcc0bc3a6b7034349d95698c0946d500a40b7ba1133bdd418fc5eefb74e`.

Across frozen query ordinals 64 through 95, the global diagnostic placed every
truth microleaf within the first 625 of 768 ranked microleaves and scanned at
most 230,856 codes. All twelve missed truth rows were present in the retained
candidate population and failed only at page reduction. First-distinct selected
pages recovered 308/320 neighbors (962,500 ppm aggregate), with a 7/10 minimum
and 23/32 perfect queries. Reciprocal-rank page scoring regressed to 298/320.
The selected 16 pages occupied at most 3,117,216 encoded bytes and no page body
was read.

This exonerates the tested global routing frontier and candidate-retention
budget for this cohort. It does not qualify release: the development cohort is
burned, and 308/320 is below the perfect-recall gate.

## Causal defect

Construction geometrically partitions each code-parent population into routing
microleaves of at most 1,024 rows. The current assembler then appends their rows
to one global 480-row page buffer. It does not flush on a routing-microleaf
boundary, and derives leaf page ranges from global logical offsets. Physical
page membership is therefore unrelated to the geometric split and may combine
the tail of one microleaf with the head of another.

The proposed replay tests whether weakly ranked truth rows become retrievable
when they share a page with stronger nearby rows from the same microleaf. It is
not another page-score tuning exercise and does not consume query or truth data
while constructing page membership.

## Virtual layout algorithm

For every routing microleaf, in ordinal order:

1. Decode each resident logical row's exact 24- or 48-byte PQ code.
2. Reconstruct its residual from the corresponding frozen codebook, add the
   stored code-parent centroid, and normalize the resulting finite nonzero
   96-dimensional vector.
3. Pair the reconstructed vector with the authenticated source ordinal from
   `logical-sources.arrow`. Source ordinal is the only tie breaker.
4. Partition the microleaf into exactly `ceil(n / 480)` groups with the existing
   balanced-cosine splitter: source-order seed, farthest second seed, four fixed
   refinement iterations, stable source-ordinal ties, and recursive balanced
   bisection.
5. Assign one virtual page ordinal to each group. A row appears in exactly one
   group. Groups never cross microleaves, contain 1 through 480 rows, and are
   independent of queries, truth, previous results, and page credentials.

The diagnostic may retain one `u32` virtual-page ordinal per row at one-million
scale (4,000,000 bytes). This mapping is diagnostic scratch, not a serving
format. A production format cannot claim zero indirection if pages cross routing
or code-parent boundaries: it must either preserve code order and account for a
packed mapping, or replace the routing layout. At one billion rows a `u32`
mapping alone is 4 GB and therefore cannot fit a strict 3 GiB total budget.

## Replay and result contract

The replay authenticates the governing terminal URI, 262,537-byte length and
SHA-256, then requires exact per-query equality with it: 308 first-distinct
hits, 298 reciprocal-rank hits, 7/10 minimum, 23 first-distinct perfect queries,
the same twelve page-reducer misses, identical selected-page identities,
candidate/leaf ordering hashes and work counts, at most 230,856 scanned codes,
and zero page reads. Aggregate equality alone is insufficient. A mismatch is an
authority failure and suppresses all treatment output.

The qualifier authenticates resident artifacts once, validates both PQ
codebooks once, constructs the virtual ownership map once, and processes all 32
queries in one process. An authenticated Arrow/Parquet diagnostic-request table
contains `query_ordinal` and a nonnullable fixed-size list of ten logical truth
IDs. For each query the router creates one immutable candidate replay containing
ordered logical IDs and score bits, ordered routing leaves, stop reason, and work
counts. Current-16, virtual-16, and virtual-8 reducers consume that same replay;
truth is joined only afterward.

For each query the result retains all three page lists, virtual target
membership, truth-bearing microleaf/page counts, recovered and newly lost rows,
work, and the candidate/leaf sequence hashes. The ownership-map hash is recorded
once. Canonical JSON holds the compact receipt while Arrow/Parquet holds detailed
tabular evidence; both bind exact input URI/SHA-256/length identities and remain
`claim_eligible=false`.

The advance gate is all of:

- 320/320 aggregate containment, 10/10 minimum, and 32/32 perfect queries;
- exactly eight distinct treatment pages per query; virtual-16 is evidence only;
- unchanged candidate scores/order, selected microleaves, scanned-code count,
  candidate depth, and query-table construction;
- zero page-body reads;
- derived selected bytes at most `8 * 196,608 = 1,572,864` per query;
- deterministic output under input-order reversal and repeated execution.

No 319/320 result authorizes materialization or parameter tuning.

## Eight-page feasibility

The current layout is already impossible at eight pages for queries whose ten
truth rows occupy nine or ten physical pages. The replay separately records two
query-independent bounds:

- more than eight truth-bearing routing microleaves proves that any
  microleaf-exclusive layout is incapable of perfect eight-page containment;
- at most eight truth-bearing microleaves but more than eight virtual truth pages
  rejects this particular within-microleaf partition.

When both oracle bounds are at most eight, the actual first-distinct eight-page
selection must still recover all ten rows. Oracle feasibility alone is not an
implementable serving result.

## Resource and format bounds

The diagnostic downloads only the authenticated manifest, resident Arrow/Parquet
artifacts, logical-source Arrow mapping, query Parquet, truth Parquet, and truth
receipt. It downloads no page body and no corpus shard. Vectors and pages remain
Arrow IPC or Parquet; JSON contains only authority, configuration, reductions,
and receipts.

At 100 million rows the existing mixed-width code planes project to 2.52 billion
bytes and the bitmap/rank structure to 12.5 million bytes. The production layout
adds no per-row metadata; only additional page-range/location rows are resident.
With 16 pages, exact reranking covers at most 7,680 vectors and S3 transfer is
bounded by 3,145,728 encoded bytes. This implies 16,000 GET/s and approximately
3.15 GB/s of payload at 1,000 QPS, so 16-page success is a quality checkpoint,
not a serving success. The eight-page gate bounds exact reranking to 3,840
vectors, 8,000 GET/s and 1.573 GB/s (about 12.6 Gbit/s) at 1,000 QPS before
protocol overhead. A 10-Gbit link is insufficient at that ceiling.

The existing mixed-width resident codes project to 25.2 GB at one billion rows,
before runtime overhead. Therefore the 3 GiB requirement must be defined and
tested as either a per-shard bound with an explicit shard count or, if total, a
new page-summary router that keeps row codes only in object storage. This replay
does not qualify billion-row resident memory.

## Progression

Before reconstruction, any query with more than eight truth-bearing microleaves
rejects every unique-owner microleaf-exclusive exact-eight design. After the map
is built, more than eight truth-bearing virtual pages rejects this layout. A
perfect virtual-16 result does not override either rejection.

If actual first-distinct-eight fails, preserve the result and reject
within-microleaf geometric repacking. The next single no-page falsifier is a
deterministic, capacity-balanced global geometric packing that may cross old
microleaf/code-parent boundaries, while preserving source-ID ties and the same
candidate replay. If its eight-page occupancy still fails, unique-owner
480-row pages are rejected and the following separately preregistered option is
bounded query-blind boundary replication with explicit capacity, deduplication,
and set-cover feasibility. Do not tune router beams, PQ widths, and page scoring
again on the burned cohort.

Only a perfect eight-page replay may proceed to a frozen, disjoint query cohort.
Only a perfect disjoint result may authorize one bounded Causality Spot builder
that streams the registered corpus, emits a new incompatible pre-release layout
format, and physically reorders code planes and page rows together. Serving
parity must use the same global-prefix route before CPU or S3 latency evidence is
accepted. Resident compute must then reach p99 at most 12 ms, end-to-end p99 at
most 15 ms at the declared load, and the full process must remain below 3 GiB.

D3, 100-million-row construction, caching as authority, compatibility readers,
and competitor claims remain fenced.
