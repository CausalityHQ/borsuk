# V125 candidate source-tier serving contract and next gate

Status: architecture candidate, **not frozen production defaults**. This
contract follows the sealed V121/V123/V124 development evidence. The V121
deep-image-96-angular 9.99M test-first-1000 SQ8 returned gate failed at
98.034%; V123 corrected FP16 source cosine on the same used cohort returned
99.959% within the 512 nominees. V124 showed why removing expansion is not
generic: the used ReLAION-1M validation-1000 512-nominee roster contained
only 96.849% of GT100 positions, whereas V116's separate expanded SQ8
reader returned 99.208%. These studies have different layout fitters and do
not constitute one matched cross-corpus production method.

V126 subsequently replayed the fixed V116 expansion and capped-control
routes at top-512 width and scored each union against original source
coordinates on the already used ReLAION-1M validation-1000 split. Its
authenticated closeout is in
`docs/research/v126-relaion-expansion-source-closeout.md`: exact source and
FP16 both returned 99.563% versus 99.432% for the same-run capped control,
above their V116 SQ8 baselines of 99.208% and 98.618%. This advances the
expansion-plus-source candidate to a matched 100k serving screen; it does
not remove the need for fresh cross-corpus qualification.

## First implementation slice and remaining boundary

`crates/borsuk/src/native_source_tier.rs` adds a versioned, generation-pinned
float32 source-plane writer and a local reader that checks the complete
artifact SHA-256, geometry, source-object identity, and generation at open.
The reader derives SHA-256 digests for explicit 4-KiB-to-1-MiB verification
blocks during that scan and checks each block fetched by a later query.
It computes deterministic float64 cosine rankings within an externally
supplied candidate union. Four focused library tests cover ranking, ties,
invalid rows/IDs, nonunit vectors, tampering before open, and generation and
source-identity mismatch. The final V125 code-check Spot attempt
`i-0b9f2095ac8843161` ran these four tests successfully from source archive
SHA-256 `f59eb815a1e36b793530c66d222391447ca703e1837d188ec20835546a985574`;
all three initial code-check instances were terminated. Its package-wide Clippy step
failed on 137 diagnostics in older modules and none in the new module, so
this is a targeted test gate, not a clean full-assurance result.

An additional red/green Spot gate confirmed a valid-vector mutation after
open was formerly scored without error, then confirmed that the repaired
reader rejects it: the final source archive SHA-256 was
`e6148fd2139ba435329aa388d40432ecc4e90d9fc444f15d33aa21dfcd06dde8`,
terminal SHA-256 was
`f48a6378389961cec0b8ebc98e9c65d2c1cb0ab3b1828942207125a9107990e0`,
and all six focused Rust tests passed. Both new Spot instances were
terminated. The superseded v1 source plane did not carry source IDs.
Lean's current `sourceDigestBytes` model checks the v2 table payload for
two complete 100M generations at 64-KiB blocks: 38,281,280 bytes for D96
and 300,781,312 bytes for D768. This is a modeled digest payload, not
charged RAM or latency; larger blocks trade lower resident digest memory
for more local bytes read and hashed per candidate.

Source-plane format v2 stores each source ID beside its float32 vector and
rejects duplicate IDs during construction. A source-ordinal candidate may
carry an expected ID, but ranking requires it to equal the authenticated
row ID; `BORSST01` artifacts are rejected by their format marker. The
physical-to-source-ordinal layout remains a separate authenticated
generation object. A red/green Causality Spot gate first confirmed that a
wrong candidate ID was accepted by v1 (six tests passed, one failed), then
confirmed all eight focused tests pass on v2 source archive SHA-256
`211791f74f5655267a60e55eed905e7ec9bef74bf1ea382ab90ca3b49bfcfbac`.
The final terminal SHA-256 was
`a270d52072e23af74eae9cb138f72d9b49c315def32a79809e7ed50fb9a4416f`;
both gate Spots were observed terminated. The builder's global uniqueness
check currently uses an O(N) hash set, whose charged build memory must be
measured before 100M qualification. The writer records a caller-supplied source-object digest;
the build path must authenticate that original object independently.

`native_source_hydration.rs` now streams one ETag-pinned `object_store` GET
into a temporary file, verifies declared length, response ETag, SHA-256,
format, generation, source identity and block digests, then atomically
publishes and directory-syncs the local cache. A valid cache is rechecked
and reused after restart; a corrupt cache is replaced from the pinned object.
The async path dispatches local writes, full-file validation and directory
sync away from the runtime thread. It returns successful startup GET, byte
and local-validation counts. The final Spot source archive SHA-256 was
`beaa726843010797eaf8e643b867efbca96f1e3719f014bfe60cdc533fccd4e7`;
terminal SHA-256 was
`b127ae337c615420ab99509d0803bb05d35d6ffd3b4acc17d55f34b13f8e28b3`.
All ten focused library tests passed and both hydration test Spots were
observed terminated. These tests use an in-memory object store; live S3
startup transfer, failed-attempt accounting, FP16 interval refinement and
per-query local-read/S3 cost remain unqualified. Gate 1 below is still open.

## Query and generation contract

An immutable generation has authenticated objects for (1) a query-blind
router, (2) an expansion page plane, (3) a physical-to-source-ordinal map,
(4) FP16 normalized source coordinates and per-row directional error bounds,
and (5) original float32 source coordinates and source IDs for exact fallback. The FP16 and
float32 planes refer to the same source-ID roster and generation. Every
object's format/version, dimension, row count, byte length, SHA-256/block
digests and source identity are in the generation manifest. Opening a
generation rejects mismatches and old experimental formats. A compare-and-
swap publication pointer makes the complete generation visible atomically;
readers pin it for a query. A restarted node can hydrate and authenticate a
local SSD mirror from S3 without relying on the previous node's cache.

A query (a) uses the resident router to nominate a target-dependent roster,
(b) may fetch an authenticated, capped expansion wave whose additional IDs
are included in the final candidate set, (c) scores all candidate FP16
vectors using the deployed cosine kernel, (d) computes conservative score
intervals and exact-scores every candidate whose upper bound can cross the
100th-largest lower bound, and (e) returns stable `(descending score,
ascending source ID)` top-100. The exact source tier must be accessible for
all possible candidates, including router nominees outside expanded S3
ranges. If a bound or exact vector is unavailable, the query fails visibly or
uses a verified exact fallback over the whole candidate set. It must not
silently drop uncertain rows. The result is exact **within the union** when
the score-interval premise and implementation refinement hold; aggregate
recall also needs that union to contain the true neighbors.

The expansion policy cannot be disabled merely because V123's deep-image
cohort gained zero FP16 hits from it. The V124 ReLAION nominee-only capture
ceiling proves that the frozen ReLAION roster could not reach 99%. A new
router may alter this balance, so the route and expansion must be measured
together. Keep all S3 GETs, bytes, retries and local source-tier random reads
in the query cost record. A local mirror is a serving accelerator for an S3
generation; its startup transfer, SSD footprint, failure recovery and
generation overlap are part of the product cost.

## Precision certificate and failure boundary

For a unit original source vector `x`, a nonzero decoded FP16 vector `z`,
and a unit query `q`, the ideal cosine-score difference is at most
`norm(unit(z)-unit(x))` by Cauchy-Schwarz. The generation builder can store a
conservative per-row bound. The deployed kernel adds a separately certified
floating-point error term for decode, norm, dot product and score comparison.
V124 used float64 NumPy plus `1e-12` and checked its intervals on 2,000 used
queries; that number is **not** a proven production kernel bound. The
checked Lean theorem in `formal/AdaptiveRerank.lean` only transfers a sound
interval premise into safe pruning. The exact top-k sort, ties, source map,
storage authentication and kernel arithmetic require implementation tests
and refinement evidence. The interval method may refine every candidate in
the worst case; a partial-refinement quota may not compromise correctness.

## Resource policy without a vector-count quality knee

Use explicit workload parameters `(N,D,R,C,G,L)`: rows, dimensions, target
recall, active query concurrency, simultaneously pinned generations and
latency target. Per-generation FP16 coordinate payload is `2ND` bytes, with
an additional bound/ID map and router; exact float32 coordinates are `4ND`
bytes before the 8-byte ID per row and 64-byte header. At 100M rows and D96, the two coordinate payloads
are respectively 19.2 GB and 38.4 GB **per generation**; at D768 they are
153.6 GB and 307.2 GB. These are decimal payload projections, not charged
RAM or required memory. For two complete FP16 generations with 8 metadata
bytes/row, Lean checks 40.0 GB at D96 and 308.8 GB at D768. The actual
admission formula must add `G` router/map/cache copies, `C` query scratch and
in-flight response buffers, allocator overhead and measured page cache.

`R` controls a measured candidate/expansion policy and therefore the needed
cache working set. `L` decides whether the target can be met with SSD-backed
FP16/exact planes or requires more resident RAM. If the requested quality
and latency combination exceeds provisioned resources, report the resource
requirement or reject admission; do not lower quality at a fixed `N` or hide
a 3-GiB cap. The production default remains unfrozen until matched quality,
latency, concurrency, generation rollover and cost gates pass.

## Ordered validation gates

1. Build one query-blind source generation and a source-tier reader with
   authenticated SSD hydration, FP16 scoring, exact fallback and per-query
   byte/GET/local-read accounting. Test corruption, restart, generation pin,
   tie behavior and fallback exhaustion on small synthetic fixtures.
2. On the cheapest decisive 100k paired screen, compare returned Recall@100,
   p05 and sub-90 against the strongest current BORSUK control. Require exact
   source-tier parity within the fixed candidate union and measure charged
   RAM, SSD, cold/warm latency and expansion GETs. The V122 100k deep-image
   result is historical because it fetched the full 10.8-MB SQ8 object;
   preserve it as context, not an architecture-matched frozen baseline.
3. Replace the flat summary/PQ nomination scan with a query-blind
   hierarchical or otherwise sublinear method. Measure visited summaries,
   coded rows, nominee capture, expansion benefit and end-to-end latency on
   matched development cohorts before using fresh GT. Any new router must
   be paired with the same source scorer; V123/V124 fixed-roster quality does
   not transfer automatically.
4. Freeze the architecture and test a matched ReLAION-1M build plus new
   deep-image ordinals outside 0..999, then 10M and 100M. Count every wave,
   run from the exact frozen revision, include generation overlap and
   concurrent searches, and compare honestly with first-party or paired
   reproduced S3 Vectors/Turbopuffer baselines. No 100M quality, latency or
   price claim is established by V124.
