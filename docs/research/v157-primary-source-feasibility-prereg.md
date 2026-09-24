# V157 source-primary feasibility gate

## Decision

Can the V114/V154 exact-SQ8 primary step be reproduced through a single
object-storage SQ8 object inside the same **32 GET / 16,777,216-byte total
request cap**, without an uncharged local SQ8 mirror or a new score bound?
If fetching the physical pages containing all 512 nominees already exceeds
the byte cap, exact scoring of all nominees cannot be the first step under
that cap. This is a necessary I/O condition, not a latency or recall test.
The alternative of taking the first 100 PQ64 nominees changes the primary
set. We will measure its row/page overlap with the exact-SQ8 primary to
quantify that change, without declaring its quality from overlap alone.

The source-only PQ64 nominee list is score ordered. V114 used exact SQ8
scores over those 512 nominees to select 100 primary physical rows; V154
and V155 consumed those exact-primary rosters. The V156 design's description
of the router directly emitting primary pages must be revised if this gate
shows a different roster.

## Frozen used inputs

Both inputs are complete terminal-bound historical captures. The evaluator
reads only `query_ordinal`, `nominees` and `primary`, never GT, returned IDs,
or hit counts.

| Cell | Dataset and split | Input | Bytes | SHA-256 |
| --- | --- | --- | ---: | --- |
| 100k first | deep-image-96-angular random100k train subset; publication-test queries 9000–9999 (used) | V122 `evidence.jsonl` at `research/v122-deep-image-100k/afe07cb5a9ba8518263375595f589639fdf3f4f1/runs/v122-20260924T011355Z/a0001/artifacts/` | 6,183,526 | `deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6` |
| 1M second | ReLAION-1M validation-1000 (used) | V116 `rust-replay.jsonl` at `research/v116-validation-paired/5e9b35ad40ea023eab4407aa611d759e1893bb34/runs/v116-validation-20260923T235426Z/a0001/artifacts/` | 13,455,525 | `3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960` |

The V122 and V116 terminal markers report `complete` and authenticate these
artifact lengths and hashes. Their recorded source commits are
`afe07cb5a9ba8518263375595f589639fdf3f4f1` and
`5e9b35ad40ea023eab4407aa611d759e1893bb34` respectively. Old
quality figures are not compared as if the V156 architecture had produced
them.

## Arithmetic and reporting

For each of 1,000 queries in each cell, require 512 distinct physical
nominees, 100 distinct exact primaries contained in the nominee set, and
contiguous query ordinals. The alternative primary is `nominees[:100]`.
Record row intersection, distinct primary-page intersection and each
primary-page count. These overlap measures are descriptive only.

For exact-SQ8 nomination through S3, map all 512 nominees to 256-row pages.
Find the minimum byte cover with at most 32 contiguous GET ranges by joining
the smallest gaps between page runs. Charge every bridged page and the exact
short final page: row width is 108 bytes at D96 and 780 bytes at D768.
Record distinct nominee pages, unbridged run count, minimum bytes and whether
that *nomination phase alone* fits 16,777,216 bytes. Report counts and
p05/p50/p95/p99 for paired overlap and cover bytes, with byte units explicit.
An independent checker recomputes the cover from the raw query records and
compares every output row and the summary.

If any query's minimum nomination cover exceeds the request cap, an
all-512 exact-SQ8 S3-primary implementation cannot preserve that query's
V114 primary under the fixed cap without a different authenticated score
certificate, larger cap or local exact data. If all fit, this only clears
the necessary byte gate; combined page fetches, latency and actual wire GETs
remain unproved. The PQ-direct alternative goes to a separate paired
returned-quality gate and is never promoted from overlap statistics.

Run this deterministic arithmetic once on Causality Spot from an immutable
source archive. Record instance ID and terminal status, sync raw rows and
summary to S3, and terminate compute immediately after the terminal marker.
On interruption, discard the cell and start a new attempt; do not reuse a
partial output. No dataset-name or vector-count switch enters the method.
