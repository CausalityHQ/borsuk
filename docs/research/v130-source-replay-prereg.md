# V130 production-source replay preregistration

Decision: determine whether the same 512-router-plus-top-512-returned union,
resolved through the production source-ID map and exact-source scorer,
improves quality on the sealed V122 deep-image-96 100k development cohort
without excessive local scorer cost. This is a development replay on already
used test ordinals 9000–9999. It does not qualify an untouched split, live S3
latency, 10M selectivity, or an end-to-end product comparison.

Frozen input: V122 completed terminal SHA-256
`5475dfdb8608b80c2dbdc3550721d9f5d106fb79c613cbc83cb40a7a29a4a89c`.
The worker checks every downloaded input's terminal-listed SHA-256 and byte
length, then checks all 1,000 evidence rows against the sealed truth and
summary. The source input has 100,000 D96 F32 rows. Candidate and capped
control use the exact V122 physical byte ranges, 32 GET and 16,777,216-byte
caps, and top-512 returned expansion. Both arms use the same production
writer, map, scorer, block size 65,536 bytes, and exact cosine rule. Query
arm order alternates by ordinal. No setting varies by dataset or vector count.

The first validation gate requires production `rank_returned_ranges` top-100
IDs to match V122 per query and arm. A mismatch stops the cell before any
source recall claim. The frozen SQ8 baselines are candidate 99,106 and capped
control 98,203 GT100 hits out of 100,000. For source ranking, promote only
if candidate hits are at least 99,500 and at least control hits, candidate
p05 is at least 98, and no candidate query has fewer than 90 GT hits. A
separate local-cost screen requires candidate source-rank p95 at most 30 ms
and maximum authenticated local read bytes at most 32 MiB. These cost values
are provisional headroom gates for a later live S3 measurement, not a warm
service latency claim. Failure requires diagnosis of scorer, roster, or I/O
cost and a materially revised implementation before a new cell.

The one Causality `c7i.8xlarge` Spot attempt has a wall deadline. An
interruption discards the incomplete measurement cell; a fresh attempt uses
a new immutable prefix. Raw per-query evidence, source/map artifact hashes,
summary, resources, and terminal receipt are uploaded to S3 before the
instance shuts down. The launcher terminates the instance after a terminal
marker and never starts an overlapping copy.
