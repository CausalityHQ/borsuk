# V26 Dual PQ-Key Router Design

## Decision

Replace the rejected 16-bit random-hyperplane SimHash router with two deterministic
16-bit keys derived from the existing PQ16 code. Plane A uses code bytes `(0, 8)`;
plane B uses `(4, 12)`. These four separated PQ subspaces make routing distance
aligned without retraining, query leakage, or another construction-corpus pass.

The experiment is an offline, claim-ineligible 32-query falsifier. A serving design
is accepted only when the narrowest preregistered arm passes the unchanged gates:
aggregate recall at least 975,000 ppm, minimum-query recall at least 800,000 ppm,
oracle attainment at least 995,000 ppm, and p99-equivalent maximum latency at most
15,000,000 ns while selecting exactly ten pages.

## Evidence and failure being addressed

The authenticated 9,990,000-row SimHash preflight at source
`696e4a94becb5dc5bd8f6149d2ca30d97837bfc4` reached only 884,375 ppm aggregate
recall and 400,000 ppm minimum-query recall at its widest arm; latency was
21,494,335 ns. The same PQ16 representation reaches 999,218 ppm at global width
1,024 and perfect recall at width 2,048. Therefore the encoded representation is
sufficient, while random-hyperplane bucket proximity is not.

## Representation

The resident index contains:

- the existing source-order PQ16 codes: `100,000,000 * 16 = 1,600,000,000` bytes;
- two source-ordinal planes: `2 * 100,000,000 * 4 = 800,000,000` bytes;
- two offset planes: `2 * 65,537 * 8 = 1,048,592` bytes;
- the PQ codebook: `16 * 256 * 6 * 4 = 98,304` bytes;
- the existing bounded cold-vector cache: `512 MiB = 536,870,912` bytes;
- eight bytes of fixed accounting.

The exact projection is 2,938,017,816 bytes, 283,207,656 bytes below 3 GiB.
The persisted format remains language-neutral Arrow IPC: the existing codebook and
source-order codes plus `pq16-dual-key-offsets.arrow` and
`pq16-dual-key-ordinals.arrow`. Schemas are concrete, non-nullable, versioned, and
bound by SHA-256 in the strict serving manifest. No compatibility path is retained.

Construction is deterministic counting placement, not comparison sorting. For each
plane it counts all 65,536 keys, prefix-sums offsets, and writes source ordinals in
ascending source order inside each key. It derives entirely from authenticated PQ16
codes and therefore requires no vector corpus, query, truth, or page-body access.

## Query algorithm

For each plane, compute the 65,536 partial PQ distances from the two corresponding
lookup tables and order keys by `(distance, key)`. The fixed arm ladder is 128, 512,
and 1,536 keys per plane. Visit both planes, deduplicate source ordinals, score every
visited row with all sixteen PQ lookup tables, and retain a bounded deterministic
top-2,048 heap ordered by `(distance, source_ordinal)`. Fetch and exactly rerank only
those 2,048 rows from authenticated Arrow cold vectors, then apply the existing
deterministic ten-page cover.

The implementation records keys and unique rows scanned per plane, duplicate count,
exact rows read, cold batches/workers, latency, selected pages, recall, oracle
attainment, and all authority identities. No caller-tunable routing surface exists.

## Falsification and release rule

Unit tests compare counting construction and ranking against scalar full-sort
references, including empty/skewed buckets, duplicates across planes, ties, and
reversed block order. Arrow round trips must preserve exact arrays and reject schema,
inventory, digest, and ordering drift. The local runner authenticates the preserved
V2 serving bundle, full 512-row truth artifact, and query artifact before evaluating
the fixed first 32 queries. It performs zero page-body reads.

The existing sub-minute V26 gate is used after each stable edit. A single causality
Spot run then builds only the two ordinal planes from the preserved PQ16 files and
executes the three fixed arms. Full workspace tests and Clippy run once only after a
scientifically passing architecture. A failed arm ladder rejects this router without
loosening quality or latency gates.

## Evidence-driven second falsifier

The first authenticated run at source
`ba509c88e4ce83a0d67f74756694093cee02f6de` rejected the initial reducer. Its
1,536-key arm reached exactly 975,000 ppm aggregate and 800,000 ppm minimum
recall, but only 975,000 ppm oracle attainment and 47,115,175 ns maximum
latency. It visited 510,939 to 825,730 unique rows per query and fetched 2,048
exact vectors scattered across all 153 Arrow batches. Memory remained healthy
at 500,703,232 bytes peak RSS with zero PSI and swap growth.

The second falsifier keeps the same Arrow representation and immutable inputs.
It replaces comparison-sorting all 65,536 keys with deterministic linear
partitioning by `(distance,key)` and replaces candidate sorting/deduplication
with a source-ordinal bitset plus append-only unique vector. These scratch
structures are charged to the existing 512 MiB runtime reserve; persistent and
projected resident bytes remain 2,938,017,816.

The fixed arm ladder becomes `(1,536 keys/plane, 512 exact rows)`, `(4,096,
512)`, and `(8,192,512)`. Depth 512 is externally justified by the prior
authenticated global PQ16 result, where it was the smallest exact-rerank depth
passing all quality gates. The wider key ladder tests candidate coverage only;
it is not caller tunable. The canonical sample records both key limit and exact
row limit. The unchanged acceptance gates remain 975,000 ppm aggregate,
800,000 ppm minimum-query, 995,000 ppm oracle attainment, and 15,000,000 ns
maximum latency with exactly ten pages and zero page-body reads.

Tests must prove identical candidate ranking against the old full-sort/scalar
reference for partial limits, ties, duplicates, and reversed bucket visitation.
They must also prove the bitset bound and exact fixed arm matrix. Only after the
sub-minute gate passes may one new Spot falsifier reuse the preserved serving,
query, and truth artifacts. Failure rejects this two-plane family; it does not
relax recall, latency, or D3 fences.
