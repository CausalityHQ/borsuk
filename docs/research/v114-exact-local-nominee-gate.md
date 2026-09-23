# V114 exact local nominee tier: design and preregistered gates

Status: source-frozen 100k correctness and paired 1M development gates passed;
live S3 and cross-corpus gates pending. See
[the terminal-bound 100k closeout](v114-exact-local-100k-closeout.md) and
[paired 1M closeout](v114-1m-paired-closeout.md).
V113's terminal ReLAION-100k
development screen rejected a 16-byte resident residual plane: mean/p05
SQ8-primary overlap 74.037/62 against the frozen 95/90 gate. It did not
measure returned recall or serving latency. The candidate here changes
**placement**, retaining the exact SQ8 nominee scores that produced V112's
99.234% offline returned Recall@100 on ReLAION-1M development. V114 now has
its own paired, validated 99.234% offline result; V112 remains historical
context, not the paired control.

## Intended outcome and architecture

The intended method is generic across embedding families: source-only PQ
nomination chooses 512 row ordinals, the same authenticated SQ8 bytes used
for final scoring provide exact scores for those nominees, and the best
100 receive 513 votes each before a 32-GET/16,777,216-byte physical
interval plan. The remaining 412 receive one vote each. The selected page
ranges are fetched in one S3 data wave. A query never fetches SQ8 nominee
bytes from S3 before planning. There is no query/GT-trained correction,
dataset branch, or arbitrary vector-count knee.

An immutable generation stores one SQ8 row object in V63 physical order:
8-byte ID, 4-byte norm and `D` code bytes per row. The local score tier is
an authenticated mirror of the corresponding S3 object, not a second
encoding. The implemented mirror manifest binds the SHA-256 of all bytes,
row count, dimension, low/step vectors, generation ID and maximum nominee
count for bounded per-query scratch. The frozen V77 manifest separately
binds PQ64 codebooks, codes, summaries, queries and truth; the 1M gate
checks its whole-file SHA-256. A unified production generation authority
must bind the router/layout and mirror together before release.
For disk placement, a source-only sidecar contains the
SHA-256 of each consecutive 4,096-byte block of the exact SQ8 object,
including the short final block. Its own full SHA-256 is in the generation
manifest. Hydration verifies the full object and sidecar hashes before
publishing the generation; each block read for a nominee is checked against
its authenticated digest before its bytes are scored. A pinned old generation remains readable until
its queries drain. Append and compaction publish a fresh authenticated
generation; a partial or mismatched local mirror is never visible.

Two placements implement the same scorer interface: RAM-resident bytes or
a local block-device file. The first disk implementation may use buffered
reads; direct I/O is an independent measured optimization because it
requires aligned buffers and may change latency or page-cache charging.
All candidates must use the same scorer operation order and tie rule as
the paired reference, or demonstrate per-query primary-set equality to
that reference. Placement alone must never alter row scores or routing.
The reader records local read p50/p95/p99, bytes and read operations,
page-cache/cgroup memory, disk occupancy, cold hydration time and
concurrent throughput. The exact SQ8 payload is `N × (D + 12) × G` bytes
for `G` full pinned generations: 156 billion bytes for 100M rows at
`D=768, G=2`, before metadata and allocation overhead. A disk placement
uses the same payload on local storage but its charged RAM is measured,
not assumed small. A 4-KiB SHA-256 sidecar adds approximately 610 million
digest bytes per 78-billion-byte generation before allocator overhead;
candidate rows may cross block boundaries, so local reads per query are
measured rather than assumed to equal 512.

`M(N,D,R,C,G)` is the measured least-cost frontier among qualified
placements and representations. Requested recall and concurrency may
require larger memory or more nodes at 100M. The API must expose the
qualified capacity and reject an unqualified request; it must not silently
switch quality at a count threshold. Exact local scoring solves the V113
nominee error but does not solve 100M PQ nomination work, region growth,
cost or tail latency. These remain explicit scale gates.

## Cheapest correctness gate: 100k

Build the local mirror only from the frozen ReLAION-100k source and SQ8
object, with query and ground-truth paths unavailable during build. For
each of the frozen development 1,000 queries, compare the exact local
scorer's nominee scores, `(score, ID)` top-100 list, page votes and
physical interval plan against an independent scorer/planner on the same
frozen PQ64 top-512 roster and SQ8 bytes. Require exact top-100 and page
set equality for all 1,000, zero authentication failures and zero GET/byte
cap violations. Numeric score-bit equality is required only where the
scorers use the same specified floating-point operation order; otherwise
set equality is the authority and all differing scores are recorded.
This gate uses no GT and does not claim returned recall. A mismatch is a
harness or implementation failure, not evidence for a new parameter sweep.

## Paired 1M development and live gate

After the 100k correctness gate, freeze one source revision and replay
ReLAION-1M **development** with the V63/V77 page layout and SQ8 object.
Run three paired arms in one campaign: V109-style PQ64 page admission,
V112-style exact local-SQ8 oracle using the *new* physical planner, and
the production local-mirror reader. The exact reader must match the
paired oracle's primary sets, page sets, final hits and physical budgets
on every query. It must return at least 99.0% Recall@100, have p05 at
least the larger of paired V109 p05 and paired oracle p05 minus one, no
more sub-90 queries than paired V109, and zero cap violations. If the
paired oracle misses the absolute floor, diagnose nomination/layout or
planner instead of retuning the local score tier. The historical V112
99.234% is a parity check, not a required exact count after planner or
format changes.

Serve the same frozen 1M cohort against live S3 with the local mirror on
an EC2 Spot type that actually has local NVMe. Benchmark a same-run
RAM-backed exact arm and a disk-backed exact arm at the same concurrency,
with cold and warmed caches reported separately. To promote disk placement,
require that its added local-read p95 is at most 15 ms and p99 at most
30 ms relative to the RAM arm, and that end-to-end p95 is at most 15 ms
above that paired arm while preserving all hits and physical caps. These
are provisional research promotion gates, not a product SLO or a claimed
measurement. Record QPS at the same concurrency, cgroup peak/steady RAM,
local SSD occupancy and IOPS, S3 GET/bytes, hydration and two-generation
rollover. Do not infer a memory result merely from requesting direct I/O.
The exact instance class, Spot capacity and price are registered before
launch, with interrupted cells discarded and restarted under new IDs.

## Cross-corpus and scale gates

Freeze acceptance before untouched ReLAION-1M validation and
deep-image-96-angular. Train only corpus-derived PQ/layout artifacts for
the latter and apply the same score, voting and physical-cap rules.
Normalize its nonzero vectors for the declared angular metric before
building, as in the V113 preregistration. Both must clear the same
absolute 99.0% Recall@100, p05 at least 90 and zero-cap rule, plus paired
oracle equivalence for exact placement. Then measure 10M region/nomination
growth and local-score I/O before a 100M campaign. The 100M gate uses
the exact frozen revision, measures `M(N,D,R,C,G)` and cost with `G ≥ 2`,
and compares against matched S3 Vectors/Turbopuffer conditions. No
100M performance or resource number is claimed yet.

Lean can certify that a fixed deterministic downstream route returns
the same hits when authenticated score bounds and the primary margin
preserve the primary list; exact mirrored bytes make score error zero
under a matched scorer. Lean also checks the SQ8 payload arithmetic.
Implementation refinement, floating-point order, local I/O latency,
unseen recall and S3 tail behavior still require the gates above.
