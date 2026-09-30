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

## Runtime integration and completed review (2026-09-30)

The later runtime patch replaces the pending state above: generation v6 stages
nine metadata objects, then admits the source object's length and strong ETag
with one separate HEAD. It retains the persistent ten-object publication/GC
roster, fetches authenticated source ranges before shared nomination, and charges
source payloads and authority alongside query slots and retired generation pins.
Source and SQ8 failures retain separate counters. Local reference planning stays
available; paged planning requires an explicit reader.

Dual critique 941d951024d24994 completed (Opus fc9be341f4bb418d and Astra
39085845ba48461d), reviewing frozen diff SHA256
e0d92bf7268d2989249b35c89202e32b3b79080a63dcb74a5d1868a87609833e.
Both found the shared nomination, identity and admission structure sound. The
concrete follow-ups are redirect/error-body transport bounds, explicit source
accounting in development consumers, and fragmented-range parity evidence.

The redirect regression reproduced 11 wire requests from one admitted GET.
The native connector now disables both redirect following and reqwest retries,
keeps SDK retries disabled, and drops unsuccessful bodies before SDK collection.
It uses the already locked reqwest 0.13.4 dependency, with fixed HTTP/1, verified
system TLS, 30-second request and 5-second connection deadlines. Eight affected
transport fixtures passed, including an unfinished 1 GiB advertised error body.
The new source-accounting checker passes its runnable cap/type/failure tests;
it is for future paged arms and does not alter historical qualification.

The development HTTP example serves an immutable generation, not a maintenance
coordinator. Qualification must keep its namespace immutable and run no GC.
Product use with cooperating maintenance requires the existing lifetime-pinned
TwoBitIndex API; cross-host pin coordination remains a separate product gap.

Five source-walk checks passed, including 513-row partial-tail/failure accounting
and a 262,145-row D2 synthetic fragmented cover. The latter requires multiple
GETs and bytes below the full source object, compares local/paged traces, plans
and ordered hits, and sweeps sorted distinct caps 1, 2, n-1 and n. Physical GETs
stay within each cap and source bytes do not increase as the cap increases.
The fixture's conservative build/open budgets were increased to 256 MB after
admission failures; production limits and admission formulas were unchanged.
Eight final transport checks also passed, with the redirect and unfinished-body
fixtures each asserting one submitted/failed GET, zero verified bytes and one
wire request. HTTP startup now reuses this connector as well as query reads.

Final integration passed: application IDs three (one AWS smoke ignored), delayed
delete/GC four, generation two, HTTP example two. Together with the five source
and eight transport checks, this slice has 24 focused Rust passes and one ignored
AWS smoke test. The Python accounting self-check passed separately.
No ARM qualification, full-suite assurance, latency, throughput, quality, scale
or cost measurement is claimed for this runtime revision.
