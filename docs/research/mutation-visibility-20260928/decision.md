# Pre-top-k mutation visibility

## Code decision

Reuse the authenticated page reader and exact SQ8 ranker with a borrowed,
sorted unique signed-i64 exclusion roster. Suppress deleted/replaced logical
IDs before top-k, while still validating all fetched records. The range-reader
helper and `TwoBitGeneration::search_excluding` return up to k visible rows,
including zero, without extra GETs. Existing strict helpers retain their
underfill errors and original request/byte/failure charges.

No mask HashSet, base-ID map, base scan or hydrated vector plane is added.
Lookup is binary search over the caller's immutable roster. Sortedness and
uniqueness reject before network I/O. The generation also checks roster length
against `already_pinned_bytes`; callers must charge/authenticate/bind the complete
snapshot there, including overlapping snapshots. This declaration is payload
admission, not an absolute RSS guarantee or snapshot authentication service.

## Gate

Exact std-only production-module red/green and focused remote Cargo tests verify
replacement of a masked best result by the next visible candidate, signed-ID
extremes, all-masked underfill, invalid roster zero GETs, unchanged physical
charges, corruption/duplicate rejection for masked rows and empty-roster parity.
Range-reader Cargo exit0 (four tests); generation/ID integration exit0
(two tests). Final empty-range rejection was added afterward with exact
std-only production-core red101/green0 (seven tests). The only other post-Cargo
change was documentation grammar. Original statuses and source/log hashes are
archived in [verification.json](verification.json); compressed logs retain the
raw SHA256. Final remote/local source parity, rustfmt edition2024 and
diff-whitespace checks pass. Missing-API red exit1 is also preserved.

## Remaining product work

Durable CAS snapshot publication, insert/update overlay scoring, recovery,
compaction and garbage collection remain OPEN. The next implementation gate is
an authenticated generation-bound mutation snapshot and stale-writer rejection,
followed by mutate/reopen/search parity. This primitive alone is not a mutation
API, quality result or vendor win. No paid campaign or architecture sweep.
Both matched vendor comparisons, winning ANN nomination and 100M/lifecycle
qualification remain OPEN in the [acceptance matrix](../../release/competitive-acceptance.md).
