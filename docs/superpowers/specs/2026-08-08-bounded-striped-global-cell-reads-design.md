# Bounded Striped Global-Cell Reads Design

## Goal

Reduce uncached global-PQ S3 tail latency without changing routing, the exact
candidate set, recall, or the standard Arrow/Parquet durable formats. The read
path must also retain a real query-wide memory and transfer bound at large
probe counts.

## Evidence and rejected alternatives

The terminal v66 128K-by-768D, eight-writer cell preserved recall@10 1.0 and
reduced post-drain read p95 from 237.196 ms to 203.884 ms. Exact delta rerank
fell to about 0.2 ms for 17 of 20 queries, but the selected whole-cell Arrow
range was 1.4--4.1 MiB and delta approximate staging reached 199.911 ms. One
large S3 GET, rather than ADC or exact scoring CPU, is now the tail.

Three alternatives were considered:

1. **More, smaller hierarchical cells.** A completed local qualification built
   1,024 hierarchical cells over 128,000 checksum-pinned Cohere 768D vectors.
   On 100 real queries, 28, 32, 40, 48, and 64 probes with 128 exact candidates
   reached recall@10 0.905, 0.918, 0.925, 0.932, and 0.941 respectively. This is
   below the 0.95 production-quality boundary and is rejected.
2. **Separate compact codes and exact vectors into dependent objects.** v65
   already measured this request shape: code staging followed by exact-row
   staging produced 237.196 ms post-drain p95. Repackaging the same dependency
   does not remove its latency.
3. **Parallel range stripes inside the existing Arrow object.** This preserves
   the qualified routing and candidate set while replacing one multi-MiB
   transfer with a bounded parallel wave. This is the selected approach.

## Architecture

The global-PQ descriptor and Arrow bundle layout do not change. A code group
still identifies one contiguous code-to-exact Arrow envelope. Before issuing
I/O, the query planner assigns one of two read shapes to every selected group:

- **code-only:** fetch the existing compact code range;
- **full-envelope:** fetch the complete bounded Arrow cell range, split into
  adjacent stripes no larger than 1 MiB and concatenate them in byte order.

A group is eligible for full-envelope prefetch only when its complete range is
at most 4 MiB. The planner debits the complete range against the existing 8 MiB
query-local reuse budget and its stripe count against a 16-stripe query-stage
cap before any request starts. Once either budget is exhausted, all remaining
groups are code-only. Code-only bytes do not consume the exact-reuse budget and
are never retained as if they could satisfy exact rows.

The stripe count is therefore at most four per prefetched group and at most 16
per base or delta query stage. Smaller ranges remain one request. Existing
global decode admission and query admission continue to bound aggregate
concurrency.

## Cache semantics

The planned read shape is authoritative. A full-envelope group is read through
the striped range API even when its code slice is already present in the
in-memory code cache. The disk range cache can satisfy the envelope with zero
backing requests; otherwise the bounded remote stripe wave runs. This prevents
cache state from silently changing a one-stage query into code-cache plus
dependent sparse rerank.

Code-only groups retain the existing in-memory code-cache shortcut. Successful
striped reads are cached under a deterministic key derived from the object and
ordered stripe ranges. A second identical striped read must perform zero
backing requests and reconstruct identical bytes.

## Failure handling and integrity

All stripes must complete successfully before the range is returned. Missing,
overlapping, reordered, or truncated bytes fail the query. The existing BLAKE3
checksum of every selected code slice and exact payload remains authoritative
after concatenation. A partial remote wave is never published as a cache hit.

## Verification

Implementation follows test-first development:

1. A storage regression requires a 3 MiB range to return exact bytes through
   three backing GETs, a sub-1 MiB range through one GET, and the repeated 3 MiB
   read through zero backing GETs.
2. A global planner regression requires cumulative full-envelope ranges to
   remain within 8 MiB and 16 stripes, leaves later groups code-only, and proves
   code-only bytes do not consume the exact-reuse budget.
3. A cache-state regression proves that a planned full-envelope read is not
   bypassed merely because its code slice is memory-resident.
4. Existing storage, global-PQ, group-commit, strict Clippy, full Rust, pinned
   Python, policy, and structural-smoke gates must pass.
5. A fresh immutable AWS attempt must pass fail-closed validation before any
   terminal CSV is inspected. Promotion requires the complete frozen matrix,
   not merely another 2K/r01/w8 improvement.

This design does not claim production readiness, 100M qualification, or
competitor parity. It removes one demonstrated tail mechanism while preserving
the evidence boundaries required for those later claims.
