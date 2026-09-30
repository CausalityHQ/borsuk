# Source unit authority prerequisite

The source-plane writer now streams SHA256 for each 32-row encoded unit,
including a partial tail. Records and the digest table are flushed and synced
before manifest publication. The extra 16 KiB writer buffer is charged before
allocation. Source format v3 and generation format v5 reject older layouts.

The generation publication/GC roster retains records and adds the digest table.
Resident reference reopening authenticates both the table and its correspondence
to records, charging two table copies during validation. Remote startup still
hydrates records at this checkpoint; this is not the completed paging arm.

Local focused verification on 2026-09-30:

- Source writer red: missing page_digests.bin, exit 101, as expected.
- Source/application IDs/generation/delayed-delete GC: 13 passes, one AWS-only
  smoke test ignored, exit 0. Partial tail and a rebound-but-inconsistent digest
  table are covered, as are publication/reopening/conditional fences and GC.
- SQ8 authority isolation in prerequisite 87cd403d: four focused passes.

No full-workspace assurance, cold latency, throughput, scale or cost result is
claimed for these changed sources. Closed historical binaries remain immutable.
Next: authenticated source range transport and startup-only roster separation,
with source caps selected from the independently authenticated closed-trace replay.

## Bounded transport prerequisite

The existing one-attempt conditional range transport now exposes bounded verified
ranges for source records and remains the SQ8 ranker's shared implementation.
Sorted/disjoint geometry, aggregate bytes, GET count and concurrency are admitted
before I/O. All range futures finish inside the caller; failure preserves total
submitted GETs, verified bytes and failed GETs. There is no retry or detached task.

Verification: missing transport API produced the expected compile failure; the
new source-tail/admission/tamper accounting test then passed. All seven affected
transport fixtures passed, including real HTTP range faults without hidden retries
and publication/reload/search. The generation-builder unit check also passed.

## Shared discovery/nomination seam

Graph discovery and record-backed nomination now have separate internal entry
points. Both retain the same shared rank_walked_source implementation and SQ8
physical admission; the local record provider remains the reference path.
Missing supplied records fail explicitly rather than panicking. This seam allows
remote source ranges to be supplied without duplicating the nomination algorithm.

Affected generation/application-ID integration checks: five passes, one AWS-only
smoke test ignored. This does not yet verify local versus paged output parity:
there is no paged generation runtime at this checkpoint.

## Metadata-only source opening and shared cover

TwoBitPlane::open_metadata now authenticates the source manifest, mean/codec and
unit-digest table without opening or retaining records.bin. It returns authority
bound to the caller's positive generation. Metadata and resident-reference opens
share strict validation; missing resident records return None. The source test
removes records.bin, verifies metadata opening under a cap that rejects resident
loading, prepares a query, and authenticates separately supplied record bytes.
Four source tests passed (the first fixture run's query scratch was corrected
from 4096 to 65536; codec admission was not weakened).

The existing lossless physical cover accepts explicit page geometry for source
units; SQ8 keeps 256 rows. It rejects invalid geometry/out-of-bounds blocks before
range construction. All ten affected admission tests passed, including source
partial-tail coverage and the existing exact-charge/pattern checks.

Remote generation startup and source query accounting are still pending. These
prerequisites do not constitute a working paged generation or a performance result.
