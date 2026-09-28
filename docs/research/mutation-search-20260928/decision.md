# Root-bound mutation search

## Decision

Expose `TwoBitGeneration::search_with_mutations` over an opaque recovered
snapshot. Verify exact base root/dimensions and retained payload charge before
reads; keep the existing semaphore held across the full query. Reuse the base
page plan, conditional ETag/page-hash reads and pre-top-k mask. Score pending
normalized FP32 puts once and select the best combined score/ID candidates with
a bounded std BinaryHeap. Hits have logical ID/score, not a fabricated physical
ordinal for pending rows. Base remains the same SQ8 approximation.

Model additional heap/output payload across all active queries, on top of
existing complete loader admission. No compensation GET, base-ID/vector plane
hydration or pending graph is introduced. Return actual underfill, base-query
GET/byte/failure charges, mutation revision/hash and rows/puts evaluated. Earlier
snapshot recovery is lifecycle I/O and is not included in base-query GET counts.

## Original gate

Missing-method red exit101; first range/HTTP green exit0 (four tests),
expanded final range/HTTP gate exit0 (four tests), then application-ID and
generation regressions exit0 (two tests). Final source SHA matches Spark;
focused rustfmt edition2024 and whitespace checks pass.
The original real local HTTP fixture now creates and publishes a generation,
persists mutations, reloads metadata/snapshot, searches changes, updates/deletes,
recovers again and verifies the immutable old snapshot. Wrong root and missing
payload charge cause zero GETs. Unbounded requested k allocates only available
candidates; k1 exercises worst-first heap replacement. Each successful query
still performs exactly one verified 28-byte GET. Existing corruption/ETag/error
accounting remains exercised. This tiny D2 fixture is correctness evidence,
not a quality or performance measurement.

[Verification receipts](verification.json) retain exact source/raw-log hashes
and terminal exits. No local compiler, paid campaign, parameter sweep or new
consultation was started. Both-vendor quality/latency/QPS/$ and 100M qualification
remain OPEN in the [acceptance matrix](../../release/competitive-acceptance.md).

## Next single product decision

Preserve canonical normalized source and logical IDs durably for compaction.
The current root pins only its source hash, SQ8 object and query metadata; it
cannot recover the original source from an owned immutable object. Rebuilding
from decoded SQ8 would introduce repeated quantization drift. Add authenticated
canonical source binding with bounded streaming reads, then coordinate base/delta
handoff and GC so concurrent writes and pinned readers remain safe. No new
architecture benchmark until the relevant correctness prerequisites are ready.
