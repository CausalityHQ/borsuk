# V128 generation-bound source-ID map candidate

Status: production format candidate for the matched 100k serving gate;
performance and 100M capacity are not yet qualified.

## Purpose and format

The V126 fixed expansion returns original source IDs. The authenticated
source plane uses source ordinals. Resolve both router-scored and
expansion-returned IDs through one map, then score the deduplicated union
with `NativeSourceTier::rank_exact_with_stats`. Candidate membership never
shrinks at a row-count or memory threshold. Unknown IDs and mismatched
source rows fail the query visibly.

`BORSMAP1` version 1 has a 96-byte header: marker, version/reserved fields,
row count, generation, original source-object SHA-256, and exact source-plane
artifact SHA-256. It is followed by exactly `N` little-endian `(source_id,
source_ordinal)` pairs, 16 bytes each, strictly sorted by ID. At build,
reject duplicate IDs, missing/extra rows, and ordinal overflow. At open,
check byte length, full SHA-256, header binding, sorted unique IDs and a
complete permutation of ordinals. The immutable in-memory table retains
the authenticated pairs; lookups need no per-query map disk read. The
source plane verifies each resolved row's embedded ID during scoring, so a
map/source semantic mismatch fails closed. Source and map are pinned to the
same generation and source identities.

The exact resident pair payload is `16N` bytes per generation, plus vector
capacity and allocator overhead. It is 16,000,000 bytes at 1M rows and
1,600,000,000 bytes at 100M rows, or 3,200,000,000 payload bytes for two
complete 100M generations. Build and open also need an `N`-bit ordinal
permutation check (12,500,000 bytes at 100M). These are arithmetic
projections, not charged RAM measurements. Admission uses `(N,D,R,C,G,L)`
and includes this map for each pinned generation. A higher recall target
may require more expansion and source-plane cache, but this map has no
quality knee or dataset-specific branch.
`formal/AdaptiveRerank.lean` checks the complete format-payload examples
including the 96-byte header and monotonicity in `N`; it does not establish
charged RAM, recall or latency.

The first gate is a synthetic generation test: nonmonotone IDs, overlap
between router and expansion, exact scoring of the whole union, and
rejection of tampering, missing IDs, duplicate IDs, ordinal errors and
generation/source mismatch. Then a matched 100k live-serving cell must
measure actual resident bytes, cold/warm map open time, lookup cost and
the complete query latency/cost beside the strongest BORSUK control. Only
after that screen should this format be included in fresh 1M and later
10M/100M gates. The simple O(N log N) sort and O(N) resident map are
explicit capacity choices; revise them if measured build/startup/RAM cost
loses at scale, without changing candidate quality.

The focused red check archive SHA-256 was
`f75cabb028302bc4697e647d888d68c471936cbf418c79d3af98e63d49cd00ca`;
terminal SHA-256 was
`50149c3c6366d5308c72865e3d3ab3352bf0bd32ccf3d7ebf7d5d1b07de29f74`.
It failed because the map writer and reader were absent, as expected. The
green check archive SHA-256 was
`f007065977f77b217ad0e4d622db6399514f3bdc85deadbd91a3c007f4eb18ae`;
terminal SHA-256 was
`c52fa1a9edbbc10fd3aba7a0a67a37bd510f29083b7d98b30864a8ee3bd30908`.
Both new map tests passed, and both Spot instances were observed terminated.
