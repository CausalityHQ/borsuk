# V29 Boundary Page-Graph S3 Index Design

## Decision

V29 replaces V28's single-best-row page reducer with a query-independent
boundary page graph. Exact vectors remain exclusively in immutable Arrow pages
in S3. Serving retains the V28 hierarchy and packed leaf-residual PQ plane,
uses approximate row evidence to select eight seed pages, promotes two graph
frontier pages, fetches exactly ten pages in one concurrent wave, and exact
reranks their decoded vectors.

This is a pre-release format replacement. There is no V28 compatibility reader,
alias, migration path, or dual writer.

## Evidence and causal boundary

The Deep Image 100K gate established the following facts:

- leaf-residual PQ8 at 24 bytes recovered 318/320 truth neighbors;
- query-independent per-leaf refinement recovered 319/320 at a projected
  2,625,266,208 resident bytes for 100M rows;
- the remaining truth row was absent from the primary 64-leaf route;
- one secondary leaf assignment admitted it, but its PQ row and page rank was
  82, outside the ten-page frontier;
- exact scoring over that same bounded secondary candidate set recovered
  320/320 with ten pages;
- 12-page widening, radius routing, 6-bit PQ, 30-byte PQ, low-rank correction,
  per-leaf shrinkage, cosine-score variants, and top-two page aggregation did
  not improve the 319/320 boundary.

The hierarchy can therefore contain perfect recall, but a hard page decision
from noisy row scores loses boundary evidence. V29 preserves the hierarchy and
exact S3 rerank while using a small page graph to promote boundary pages. It is
not a page-prototype index: edges encode corpus row assignments across leaf
boundaries rather than compressing a page into one or more vectors.

## Query-independent graph construction

Construction has corpus and output capability only. It cannot read queries,
truth, prior results, or evaluation artifacts.

For every normalized corpus row, construction computes the existing primary
leaf and the best distinct alternate leaf through the bounded hierarchy beam.
The row is physically written once, under its primary leaf, and receives no
serving-time secondary posting. After page packing, let `P(row)` be its physical
page and let `pages(L)` be the ordered pages whose primary leaf is `L`. For each
`Q` in `pages(alternate_leaf(row))`, construction adds one undirected integer
vote between `P(row)` and `Q`. Self edges are discarded.

For every page, retain the best 16 neighbors by `(descending vote,
neighbor_page_ordinal)`. An edge is stored as `(neighbor_page_ordinal:u32,
vote:u32)`. The graph is deterministic across input order, batch size, and
thread count. It rejects duplicate source owners, missing pages, invalid leaf
ordinals, overflow, asymmetric encoded edges, noncanonical ordering, and any
query-derived input.

The cross-language artifact is `page-graph.parquet` with nonnullable columns
`page_ordinal:u32`, `neighbor_page_ordinal:u32`, `vote:u32`, and
`neighbor_rank:u8`. Rows order by `(page_ordinal,neighbor_rank)`. The manifest
binds its exact schema, SHA-256, length, source commit/archive, hierarchy,
layout, code plane, page roster, degree 16, and construction receipt.

## One-wave query algorithm

For one normalized query:

1. V28 hierarchy and leaf-residual PQ retain the fixed bounded row frontier.
2. Reduce ranked rows to a canonical page-evidence sequence by first
   occurrence, retaining at most 128 unique pages.
3. Select the first eight unique pages as seeds.
4. For each evidence page at zero-based rank `r`, each graph neighbor receives
   `vote * floor(2^24 / (r + 1))`, accumulated in `u64` with checked arithmetic.
5. Exclude seed pages and select two frontier pages by
   `(descending accumulated score,page_ordinal)`.
6. Fetch the eight seeds and two frontier pages in exactly one concurrent S3
   wave. Authenticate and decode complete Arrow objects, then exact rerank by
   `(squared_l2,source_ordinal)`.

There is no dependent S3 wave, runtime graph mutation, query-result cache
requirement, page-body access during routing, or corpus-sized score buffer.

## Resource bounds

The V28 24-byte conservative projection is 2,864,197,632 bytes. At 512 rows per
page, 100M rows produce at most 195,313 pages. Degree-16 edges at eight bytes
plus offsets and alignment are capped at 32 MiB. V29 therefore remains below
2.90 GB and the 3,221,225,472-byte ceiling.

Query work remains at most 1,000,000 PQ codes, 128 page-evidence entries, 2,048
edge visits, ten page GETs, and 4,587,520 encoded bytes. CPU p99 must be at most
15 ms. Standard-S3 cold end-to-end p99 target is 100 ms with a hard 150-ms
ceiling. Actual request count, bytes, request-wave latency, throughput, CPU,
RSS, PSI, and swap are recorded separately.

## Qualification gates

The immutable 100K development gate requires exactly 320/320 Recall@10,
32/32 perfect queries, ten pages, one S3 wave, and all work/memory bounds. A
plain V28 seed-only control and a graph arm run over the same candidates. Graph
construction cannot read the 32 queries or truth.

Only a passing committed arm may open one sealed 9.99M cohort. That gate
requires at least 995,000 ppm aggregate Recall@10, 997,500 ppm compliance with
the 800,000-ppm per-query floor, at least 800,000 ppm minimum recall, and all
resource bounds. A later 100M gate verifies scale, not quality tuning. D3 and
competitor claims remain fenced until those gates pass.

## Fail-fast order

1. Unit tests lock graph construction, Parquet authority, deterministic votes,
   bounded evidence, one-wave fetch, and exact rerank.
2. A synthetic boundary fixture must fail under seed-only routing and pass with
   the graph.
3. The immutable Deep Image 100K gate runs in seconds and must reach 320/320.
4. Same-region S3 latency is measured only for the passing arm.
5. Sealed 9.99M and 100M work run only after the preceding gates pass.

