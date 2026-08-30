# V23 Clustered Page-Prototype Falsifier

**Status:** Proposed no-new-compute architectural falsifier. It authorizes
neither a new V23 format nor D3. BORSUK is unreleased, so a passing result may
replace the failed BVS3 selector without a legacy reader; a failing result ends
the page-prototype line of work. The full evidence run reads immutable S3
objects and therefore requires a separately coordinated execution-location and
request/transfer-cost check; this document does not authorize those reads.
The page stream is additionally fenced until the exact five-artifact
Revision-4/BVS2 authority bundle reaches the first registered page GET through
the real direct CLI while page access is replaced by a fail-closed sentinel.

**Evidence basis:** The authenticated BVS3 D2 cell at source `c339a546` proved
that the 384-row/eight-page geometry is viable but that resident per-row PQ is
not. The page oracle reached 993,750 ppm aggregate and 900,000 ppm
minimum-query recall, storage amplification was 1,863,874 ppm, and both
selector widths fit the 3-GiB projection. The 8/12-byte selectors nevertheless
reached only 556,250/671,875 ppm recall and about 50 ms CPU p99. D3 was not
launched.

An exact global replay of the historical Revision-4 selector then removed its
320-cell routing filter and scored all 450,087 stored 16-per-page f16
farthest-point representatives. It still reached only 265,625 ppm aggregate,
zero minimum-query recall, and 267,295 ppm oracle attainment. Extreme medoids
are therefore rejected. A separately authenticated true mean over every page
reached 696,875 ppm aggregate, so clustered means remain a distinct and
testable hypothesis.

## Objective

Determine, without new compute infrastructure or outcome-informed training,
whether 32 deterministic spherical means per immutable page can meet the
representation-quality ceiling required by the Standard-S3 design:

- aggregate eight-page recall at least 975,000 ppm;
- minimum-query recall at least 800,000 ppm;
- attainment of the authenticated 318-hit page oracle at least 995,000 ppm;
- exactly the same 32 frozen Deep Image queries and exactly eight pages;
- projected 100M-row resident memory at most 3 GiB;
- no D3, product latency claim, or competitor comparison.

K=32 is tested first because it is the largest page-prototype plane that fits
the conservative 100M memory budget. Failure rejects every smaller K without
running them. Success only authorizes a later outcome-blind `{4,8,16,24}`
ladder and a separate approximate-routing design.

## Considered approaches

### 1. Streaming spherical K=32 page means — selected

Authenticate each existing page, decode its f16 primary and replica rows,
train 32 query-independent spherical clusters, and immediately score their
means against the frozen queries. The process retains one page and its means,
not the page corpus or a global prototype plane. This tests the best remaining
page-summary hypothesis while preserving every competing page in the ranking.

Risk: spherical k-means optimizes population distortion rather than nearest
neighbor recall. That is precisely why the exact ceiling test precedes any
production implementation.

### 2. Shared global cells with page postings — rejected for this falsifier

A query could rank global semantic cells and accumulate page votes from compact
postings. It is cheaper in RAM, but BVS3 already showed that coarse-cell
admission loses too much query-specific row identity. Testing it before the
unfiltered clustered-mean ceiling would conflate representation and routing
again.

### 3. Learned page classifier or residual sketch — rejected

A learned query-to-page model could be compact, but training it on the 32
frozen queries would leak outcomes and training it honestly needs a separate
query distribution, loss, calibration split, and generalization study. It is
not the cheapest falsifier and creates a larger correctness surface.

### 4. HNSW over every prototype — rejected

At projected 100M scale, K=32 produces about 9.05 million prototypes. A graph
node per prototype violates the memory budget even before page references and
runtime reserve. A passing exact ceiling would instead justify a compact
two-level IVF in a separate design.

## Immutable authority

The tool accepts exact explicit inputs, never discovers a latest object:

- Revision-4 source commit
  `c59128ee68eb28beaa7f5eef7e0570dc7c787b88`;
- D2 attempt prefix
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-f7a6e06a6a40c1165b6cb889/runtime-v23-d2/arms/0000/attempts/0001/`;
- terminal-marker SHA-256
  `db12dd670ae5121fa4d90147fba7816d6a20878764a28d089be45be1138579ef`;
- `RESULT_COMPLETE.json` SHA-256
  `41ec2b4eb9e0506f4732c2e0ff34d92e1493b24953669c486fc5714a38002a00`;
- D2 report SHA-256
  `665dc206d04073b8cbc0b8bab9e5645760440d2336ddf4bfebea81d176b4779d`;
- page-manifest SHA-256
  `dfa5759c06663655b4a963a7687b40c8bd8020bebf805d7c825a88c6d0df53e1`;
- page namespace equal to the attempt prefix plus `pages/`;
- Deep Image test object
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/datasets/deep-image-96/attempts/0001/materialized/test.parquet`,
  with exact object SHA-256
  `296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4`;
- report query ordinals, ground-truth page assignments, and oracle hits.

The page manifest must contain 28,282 consecutive page ordinals and bind every
page's generation checksum, metric, dimension, family, code width, path,
BLAKE3 checksum, encoded length, primary rows, and replicated rows. Any
authority difference stops before a scientific result is produced.

The historical BVP2 decoder lives only in this evidence script and is pinned
to the listed immutable source and artifact digests. Production library code
must not import it, dispatch to it, or retain a BVP2 compatibility path.

## Revision-4/BVS2 authority boundary

A read-only audit of the five immutable prerequisites found that the current
falsifier is not literally a BVS3 reader consuming BVS2. It is a bespoke
hybrid: it names the historical `borsuk-v23-d2-v8` report and BVP2 pages, but
its synthetic fixtures and several validation assumptions reproduce neither
the frozen Revision-4 writer nor the current BVS3/v9 contracts. The correction
is one evidence-only adapter, not a production compatibility layer.

The only known exact-bundle rejection is the selector's sparse final page.
The frozen artifact has 450,087 anchors for 28,282 pages at 16 anchors per
page, so the valid historical relation is
`page_count <= anchor_count <= page_count * anchors_per_page`, not equality.
Its encoded length is exactly
`96 + coarse_cells * dimensions * 4 + (coarse_cells + 1) * 4 +
anchor_count * (12 + dimensions * 2)` = 93,407,096 bytes. The adapter must
also close silent authority gaps that the current synthetic fixtures conceal:

- `RESULT_COMPLETE.json` must have the frozen exact key set and concrete types
  and bind report, roster, attempt, instance, source archive, index, D1, query,
  runtime attestation, and the reconstructed D2 summary;
- the terminal must cross-bind those same identities and uses the historical
  shell writer's compact insertion-order JSON plus one newline, while result,
  report, and roster use sorted canonical JSON plus one newline;
- arm constants and arithmetic must be recomputed: routing
  `min(320, coarse_cells)`, ranked-anchor cap 8192, target rows 384, at most two
  assignments per row, eight query pages, primary/assignment totals, storage
  amplification, selector length, and projected root/RAM/build bytes;
- every page must retain the historical 65,535 primary-row and replica-row
  caps in addition to the existing digest, ordering, layout, and roster/report
  equality checks;
- query evidence must recompute every cross-object and within-report relation
  available from authenticated primitives: selected/oracle membership,
  ground-truth assignments, hits, recall, bytes, candidates, selector
  telemetry, CPU, all arm aggregates, pass, and the frozen coverage-oracle tie
  break. Ranked IDs and distances retain exact schema/type/order validation and
  feed those relational checks, but the adapter must not claim to regenerate
  historical selector distances because selector bytes are not among the five
  prerequisites;
- the query object must bind the registered URI/SHA and dataset materialization
  and have the full physical nonnullable `emb: FixedSizeList<Float32,96>`
  schema, exactly 10,000 rows, finite selected vectors, and nonzero norms.

The evidence-adapter API is deliberately narrow:

```python
@dataclasses.dataclass(frozen=True, slots=True)
class Revision4Bvs2Paths:
    terminal: Path
    result: Path
    report: Path
    roster: Path
    query: Path

@dataclasses.dataclass(frozen=True, slots=True)
class Revision4Bvs2Authority:
    registered: RegisteredAuthority
    shape: ScientificShape

```

The concrete method signature is
`Revision4Bvs2Authority.load(paths: Revision4Bvs2Paths) -> Authority`.

`load` authenticates role-specific bytes, validates the dependency graph
terminal -> result/report/roster -> query, recomputes the frozen cross-object
and within-report relations available from those authenticated primitives, and
returns only the normalized existing `Authority`. Raw BVS2
dictionaries never cross that boundary. `load_authority` remains a thin
call-compatible wrapper; the streaming and clustering code receives no legacy
branches.

## Streaming data flow

1. Pass the five exact local prerequisites through `Revision4Bvs2Authority`;
   authenticate every role-specific byte stream and cross-object relation
   before constructing or invoking a page client.
2. Authenticate the complete test object's physical schema and materialization,
   then load only the 32 registered query rows and
   unit-normalize them exactly once.
3. Iterate page references in ordinal order. Use at most four concurrent S3
   GETs and a bounded reorder queue, but feed training strictly in page order.
4. For each body, check object path, length, BLAKE3, BVP2 header, generation,
   page ordinal, metric, f16-flat/dimension authority, row counts, offsets,
   sorted unique IDs, finite codes, and absence of trailing bytes.
5. Decode only that page's primary and replica vectors. Renormalize decoded f16
   rows to unit length so spherical training and query scoring use one metric.
6. Train K=32 means with the frozen algorithm below. Score the 32 queries
   against those means and retain only the minimum distance for this page.
7. Discard the body, rows, assignments, and means before consuming the next
   page. The final resident scientific state is a `32 x 28,282` f32 page-score
   matrix plus authority and result metadata.
8. For each query, choose exactly eight pages ordered by `(distance,
   page_ordinal)`. Recompute hits from the authenticated ground-truth page
   assignments and compare with the authenticated page oracle.

No page corpus, row plane, or prototype plane is written locally. Sampling is
forbidden because it removes false-positive competing pages.

## Frozen clustering algorithm

Training is query-independent and identical for every page:

- input rows are the page's primary rows followed by replica rows, each already
  validated and then unit normalized;
- K is `min(32, row_count)`;
- initialization is deterministic k-means++ with a SplitMix64 stream seeded by
  the first eight little-endian bytes of the page body's BLAKE3 checksum; the
  first center is `(next_u64 * row_count) >> 64`, subsequent probability mass
  is squared cosine distance, a cumulative boundary chooses the lowest input
  position, and zero total mass chooses the lowest unused input position;
- run exactly eight Lloyd iterations in f64 accumulation and input order;
- assignment uses maximum cosine similarity with the lowest-center tie;
- normalize every nonempty mean before the next iteration;
- repair an empty cluster with the lowest-position row having maximum distance
  to its assigned center among donor clusters containing at least two rows, so
  repair never creates another empty cluster;
- encode final means to little-endian finite f16 and decode them back before
  scientific query scoring;
- page score is the minimum squared-Euclidean distance from the normalized
  query to any decoded mean; prototype multiplicity never adds page weight.

Fixed iterations avoid platform-dependent convergence exits. One BLAS thread
and canonical input order make the result reproducible. The output binds the
algorithm name, K, iteration count, PRNG, numeric types, tie rules, and every
input digest.

## Memory and compute bounds

At the observed page density, 100M rows project to 282,820 pages. A production
K=32 plane would contain 9,050,240 prototypes. Each prototype needs 192 f16
bytes plus one u32 page ordinal, for exactly 1,773,847,040 bytes (1.652 GiB).
The conservative feasibility sum is:

- prototype and page plane: 1,773,847,040 bytes;
- 282,820 page references at 320 bytes: 90,502,400 bytes;
- 65,536 f16 coarse centroids: 12,582,912 bytes;
- coarse offsets: 262,148 bytes;
- conservative 4-KiB-per-coarse-node graph: 268,435,456 bytes;
- fixed runtime reserve: 536,870,912 bytes;
- two maximum page waves: 3,932,160 bytes.

The sum is 2,686,433,028 bytes; the implementation must recompute this with
checked integer arithmetic and persist the exact value. It is below 3 GiB but
leaves no room for duplicate decoded prototype ownership. Any future selector
must mmap or zero-copy the prototype plane.

The falsifier itself has stricter bounds: at most four page bodies, one decoded
page, one page's K=32 work buffers, the query matrix, the page-score matrix,
and JSON authority. RSS must remain below 768 MiB. It writes no prototype
scratch. Network volume equals the authenticated page corpus, approximately
3.7 GiB, but local disk growth remains negligible. The command must refuse to
start the full stream unless the operator supplies `--execute-complete-stream`;
unit and fixture tests never require that flag.

## Pressure and failure behavior

- Do not start while another pytest, Cargo, rustc, Clippy, or benchmark process
  owns the local lane.
- Before the full stream, establish whether the execution host is in the page
  bucket's AWS region and report the exact expected 28,282 GETs plus transfer
  path. This is an authorization check, not scientific input.
- Monitor cgroup memory pressure and swap while the original process runs.
- Stop the original process group without restart if memory PSI full avg10
  reaches 0.50, RSS reaches 768 MiB, swap grows by 128 MiB, page-order progress
  stops for five minutes while network health is good, or any authority check
  fails.
- A stop may report only the last authenticated page ordinal and digest. It may
  not publish partial quality metrics.
- Delete only the explicit scratch directory after process-group clearance.
- A completed result is one canonical JSON document printed once and hashed
  with SHA-256. It is evidence, not a production result.

## Result contract and gates

The result records all input digests, page/row counts, total bytes read,
algorithm constants, query ordinals, per-query selected pages/hits/oracle
hits, aggregate recall, minimum-query recall, oracle attainment, elapsed/CPU
time, peak RSS, PSI peak, and swap delta. Concrete types and exact key set are
validated independently.

The K=32 hypothesis passes only if all are true:

- all 28,282 pages and every declared row authenticated;
- exactly 32 registered queries and eight selected pages per query;
- aggregate recall at least 975,000 ppm;
- minimum-query recall at least 800,000 ppm;
- oracle attainment at least 995,000 ppm;
- projected 100M serving memory at most 3 GiB;
- zero authority errors, non-finite values, OOM events, and swap growth.

Failure ends page-prototype work and directs the next architecture toward a
compact row-to-page classifier or a different page layout. Success authorizes
only the smaller-K ceiling ladder and a new BVS4 IVF design. It does not
authorize D3 or another paid measurement.

## Testing boundary

The checked-in implementation must be test-driven. Synthetic fixtures cover
page authentication, deterministic clustering, empty clusters, f16 roundtrip,
tie ordering, exact eight-page selection, recall/oracle recomputation,
projected-memory arithmetic, canonical output, bounded fetch reordering, and
every mutation of the authority and result schemas. A streaming test must
prove peak retained page bodies never exceed the configured bound and that no
page or prototype corpus is written to disk.

Before another complete stream, one environment-gated integration test must
load the exact terminal, result, report, roster, and query files, verify their
five registered SHA-256 values, invoke the real direct CLI, replace the page
client's first `get_object` with `PAGE_S3_ACCESS_FORBIDDEN`, and prove the
attempted key is the registered ordinal-zero page. The test is named
`HistoricalBundleIntegrationTests.test_revision4_bvs2_exact_bundle_reaches_first_registered_page_get`
and is skipped unless `BORSUK_REV4_BVS2_FIXTURE_DIR` names the explicit local
five-file directory. Reaching the sentinel is the only passing outcome; an
authority error, actual S3 request, or scientific result is a failure.

The mutation matrix has six independently reviewable families: result receipt
schema/bindings; terminal bytes/schema/bindings; arm constants and formulas;
page row caps and roster equality; query evidence and oracle recomputation; and
full Parquet schema/materialization binding. Each semantic mutation rehashes
only its changed role so it reaches the intended validator. Exact registered
SHA rejection remains a separate earlier gate.
