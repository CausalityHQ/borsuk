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

## Query and generation contract

An immutable generation has authenticated objects for (1) a query-blind
router, (2) an expansion page plane, (3) a physical-to-source-ID map,
(4) FP16 normalized source coordinates and per-row directional error bounds,
and (5) original float32 source coordinates for exact fallback. The FP16 and
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
an additional bound/ID map and router; exact float32 backing is `4ND`
bytes before metadata. At 100M rows and D96, the two coordinate payloads
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
