# V24 Witness-to-Page Router

**Status:** Approved prerelease architecture. This design replaces the failed
V23 experimental page routers; it does not preserve their formats or APIs.
Qualification remains claim-ineligible until the sealed holdout passes.

## Decision and evidence

V23 established three useful facts on authenticated Deep Image evidence:

- the historical D2 layout can cover the desired neighbors within eight pages;
- exhaustive leaf scoring did not improve the 58.125% leaf-incidence result;
- balanced page centroids, radii, and replicas recovered at most 3.1738% on an
  unbiased pseudoquery cohort, even at 64 pages.

The official balanced-development adapter also contained a dormant identity
defect: raw dataset neighbor IDs were treated as positional ordinals in a
RaBitQ control file reordered by `(leaf, canonical_record_id)`. That path never
opened in the paid run, so it does not invalidate the pseudoquery rejection,
but no successor may reuse positional identity.

The next router therefore uses query-dependent row-level evidence and carries
the original dataset row ordinal through every bulk artifact. It may widen
construction memory and the page ladder, but it must not weaken recall truth,
artifact authentication, or holdout isolation.

## Selected architecture

### Corpus witnesses

Select exactly 1,048,576 normalized corpus rows by the smallest deterministic
`(SplitMix64(source_ordinal xor seed), source_ordinal)` keys. A witness is an
actual f16-normalized corpus vector plus its original `u64` dataset ordinal;
there are no learned aliases or regenerated positional IDs.

Build one deterministic in-memory HNSW graph over the witnesses. The frozen
construction arm uses `M=16` and `ef_construction=64`. Posting assignment uses
one HNSW search at fixed `ef_assignment=128`, exact fused-f32 reranking of that
visited candidate set, and the best two reranked witnesses. It does not claim a
global exact two-nearest scan over 1,048,576 witnesses. The only preregistered
query-time `ef_search` ladder is 128, 256, and 512. Graph construction is
query-independent and sees no benchmark query, neighbor, page-quality, or
prior-result bytes.

### Witness-to-page evidence

Posting construction streams the authenticated historical D2 page corpus once.
Every page record ID must be the canonical decimal dataset ordinal, be smaller
than the registered construction row count, and inherit the exact construction
corpus digest through the page-generation manifest. Each unique primary row is
assigned to the two best exact-reranked witnesses returned by the registered
`ef_assignment=128` search; its primary and optional replica page labels
contribute deterministic integer mass to those witnesses. Duplicate page
occurrences cannot duplicate a row's witness assignment. Witness rows use the
same normalization function as construction and a page row that is itself a
registered witness must match that witness vector exactly.

For every witness, retain the top 64 `(mass, page_ordinal)` postings, ordered by
descending mass then ascending page ordinal. Prefixes 16, 32, and 64 form the
only posting-cap ladder. The posting plane is stored as Arrow IPC with fixed
little-endian integer columns; manifests and receipts remain canonical JSON.

### Query selection

Search the witness graph, rerank returned witnesses by exact fused-f32 cosine
distance, and keep the best 8, 16, or 32 witnesses. Fuse their postings with
the fixed score

`sum((2^32 / (witness_rank + 1)) * posting_mass)`.

Page ties use ascending page ordinal. Select exactly the smallest page budget
in 8, 16, 32, 64 that passes every quality and resource gate. No exhaustive
fallback, benchmark-derived weight, learned page rescorer, or outcome-dependent
parameter change is permitted.

## Formats and identity

Bulk cross-language data uses Parquet or Arrow IPC. JSON is restricted to
small manifests, policies, progress, receipts, and results.

The V24 construction-row schema is exactly:

- `source_ordinal: UInt64 non-null`;
- `vector: FixedSizeList<element: Float32 non-null, 96> non-null`.

The witness schema is exactly:

- `witness_ordinal: UInt32 non-null`;
- `source_ordinal: UInt64 non-null`;
- `vector: FixedSizeList<element: Float16 non-null, 96> non-null`.

The posting schema is exactly:

- `witness_ordinal: UInt32 non-null`;
- `page_ordinal: UInt32 non-null`;
- `mass: UInt32 non-null`.

Its Arrow schema metadata additionally binds exact `witness_count`,
`unique_source_rows`, and `physical_source_rows` decimal values. These are
authenticated artifact authority, not inferred from truncated top-64 mass.

All tables require exact names, order, physical types, nullability, row counts,
sortedness, uniqueness, finite nonzero vectors, URI, generation, encoded length,
and SHA-256. V24 rejects V23 artifacts rather than adapting them.

## Authority and leakage boundary

Training, posting construction, development, and holdout run on fresh phase-
specific workers. A credentialed parent stages exact registered files; the
scientific child runs a direct binary offline with no AWS credentials or page
client. The child authenticates its complete input inventory before semantic
use. No dynamic-loader discovery, private-root emulation, `ldd`, or mount magic
is part of scientific authority.

Training sees only corpus shards. Posting sees only witnesses/graph and the D2
roster/pages. Development sees only the sealed router and burned queries 0--31.
Holdout binding independently maps all neighbor IDs for queries 32--159 through
canonical decimal page record IDs, and holdout evaluation runs once after a
cell is sealed. Repeated-query or prior-result data never enters construction.

## Resource arithmetic and gates

At 100 million rows the serving projection is bounded as follows:

- witness vectors: `1,048,576 * 96 * 2 = 201,326,592` bytes;
- level-zero graph edges: `1,048,576 * 16 * 4 = 67,108,864` bytes;
- upper graph levels and offsets: at most 33,554,432 bytes;
- cap-64 postings at 12 bytes each: 805,306,368 bytes;
- page metadata, score workspace, allocator reserve, and executable: at most
  536,870,912 bytes.

The registered maximum is therefore 1,644,167,168 bytes, below the 3 GiB
steady-serving gate. A separate load preflight measures the complete graph and
posting decode transient and must also remain below 3 GiB peak RSS; steady-state
arithmetic cannot substitute for that measurement. Construction may use 32 GiB RSS and 500 GiB scratch because it is
offline and query-independent. A scientific process stops on swap growth,
memory PSI full avg10 above 0.50, RSS above its phase cap, missing progress for
20 minutes, or two hours wall time.

Every development and holdout cell must satisfy:

- aggregate recall at least 975,000 ppm;
- minimum-query recall at least 800,000 ppm;
- oracle attainment at least 995,000 ppm;
- warm native selector p99 at most 15,000,000 ns;
- serving projection below 3 GiB;
- exact selected page count equal to its registered budget.

The first passing cell in lexicographic `(page_budget, ef_search,
selected_witnesses, posting_cap)` order is sealed. A larger budget cannot
replace a smaller passing one. Page-body fetch/decode/exact-rerank latency is
measured in a separate production integration gate and cannot be represented
as selector latency.

## Causal controls and kill rules

Reduced fixtures separately compare the exhaustive control with scalar sorting
and exercise real graph traversal at `ef < row_count`, including deterministic
candidate reranking and disconnected-graph rejection. In evaluation, an exact
witness scan is diagnostic when serving fails: if exact passes and graph search
fails, graph retrieval is causal; if both tested selectors fail, the tested
witness/posting reducers are rejected. An observed serving pass takes
precedence because nearest-witness order is not guaranteed to dominate page
fusion and the diagnostic cannot veto a passing serving result.
If routing passes but page-body exact rerank fails, page integration is causal.

One unbiased query-independent pseudoquery split runs before burned
development. It can reject an arm but cannot select among arms. Development
selects one preregistered cell; only that cell reaches the sealed holdout.
Incomplete metrics are never inspected.

No result authorizes D3, product integration, or a release claim unless the
sealed holdout and the separate end-to-end page-body latency gate both pass.

## Implementation boundary

Create a private `v24_witness` Rust subsystem, one strict local-file example,
and one phase launcher/stager. Reuse only generic canonical JSON, Arrow/Parquet,
resource-monitoring, and page-codec primitives. Do not add V23 readers, version
dispatch, aliases, production storage APIs, or compatibility layers.

Implementation follows strict TDD: identity/schema authority; deterministic
witness sampling; graph determinism and scalar/SIMD equality; one-pass posting
construction; bounded codecs; query fusion; causal evaluation; local CLI;
offline phase orchestration; reduced cross-process determinism; then focused,
affected, and one final repository assurance progression.
