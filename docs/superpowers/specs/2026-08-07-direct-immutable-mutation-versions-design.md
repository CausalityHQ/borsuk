# Direct Immutable Mutation Versions and Bounded Delta Ingest

**Status:** Architecture selected by the user on 2026-08-07. Written-spec
review is pending before implementation.

## Objective

Remove collection-wide coordination from ordinary upsert and delete
acknowledgements while retaining S3 as BORSUK's only storage service. The
production path must scale across independent library processes, converge on
one deterministic value for every ID, preserve crash-safe visibility, and meet
the frozen write/read p95 and recall gates without depending on a warm cache.

This is a pre-release format and semantic break. Experimental indexes using
the global `id-directory/last-write-wins/NEXT` counter are rejected rather
than migrated or read through a compatibility path.

## Terminal evidence

The terminal v62 AWS campaign used 768-dimensional vectors and independent OS
processes against one S3 collection. One writer passed with 702 records/s,
110.287 ms write p95, 124.681 ms active-tail read p95, 55.260 ms post-drain
read p95, complete visibility, and recall 1.0. Eight writers preserved all
128,000 records and recall 1.0, but produced only 1,302 records/s aggregate,
1,376.614 ms write p95, 493.610 ms active-tail read p95, and 287.722 ms
post-drain read p95.

Terminal raw samples show the write failure is causal: one group expanded from
the normal counter GET, counter CAS PUT, and extent PUT to 49 losing GET/CAS
attempts followed by one extent PUT. Its 99 requests took 4.095 seconds. The
read failure has a separate cause: background materialization fires only when
one stripe reaches sequence 1,024, while each v62 writer produced 250 groups.
The eight-writer arm therefore searched the complete growing WAL tail.

## Selected architecture

### Direct immutable acknowledgement

Ordinary upsert and delete paths allocate versions in process memory and write
one checksum-verified immutable WAL extent. Successful extent creation remains
the durability and acknowledgement boundary. Normal acknowledgement performs
no collection counter GET, conditional counter PUT, manifest publication,
materialization, or external-service request.

S3 remains the only durable service. DynamoDB, a required RPC ingest service,
and cache-dependent correctness or performance are excluded.

Strict duplicate-rejecting `add` remains a distinct operation and may use its
existing bounded explicit-ID coordination. Its cost and semantics must not be
presented as ordinary upsert throughput. No strict compare-and-set mutation is
claimed unless it has a separate protocol that proves atomic validation and
publication.

### Mutation version

Replace record `generation: u64` with a checked, lexicographically ordered
128-bit `MutationVersion`:

```text
bits 127..80  physical Unix milliseconds (48 bits)
bits  79..64  logical counter             (16 bits)
bits  63..0   writer token                (64 bits)
```

The library generates a cryptographically random 128-bit writer identity for
each independent mutation clock and derives the version's 64-bit token with
BLAKE3 domain separation. Neither value is caller-controlled. Every extent
persists the full writer identity alongside its token; observing two different
identities with one token is hard corruption rather than an ambiguous tie. A
process-local atomic hybrid logical clock generates versions without object
store I/O:

1. choose the greater of the current physical millisecond, the prior local
   physical millisecond, and any physical millisecond observed by this handle;
2. increment the logical component when physical time does not advance;
3. if the 16-bit logical component overflows, advance the synthetic physical
   component by one millisecond and reset the logical component;
4. append the stable writer token and compare the complete 128-bit value as
   unsigned big-endian bytes.

One `GroupCommitWriter` shares one atomic mutation clock across all of its
worker stripes. It assigns versions before lane fan-out, preserving caller
order without a storage round trip. Ordinary index handles own independent
clocks. Reads and refreshes advance a handle's observed HLC floor but never
rewrite an already allocated version.

The format stores the 16-byte value directly in WAL extents, segments,
tombstone runs, ID deltas, exact metadata, and every checksum-covered
descriptor that currently persists a record generation. Manifest/catalog
generation counters are unrelated and remain their existing integer type.

### Conflict semantics

All readers, materializers, compactions, reopen paths, and modalities select
the greatest `MutationVersion` for one record ID. Equal versions with unequal
payloads are corruption and fail closed. Consequently every replica and
reader converges on exactly one value or tombstone.

The production contract is:

- writes allocated by one mutation clock preserve program order;
- a write allocated after that handle observes a remote version dominates the
  observed version;
- unobserved writes from independent hosts are ordered deterministically by
  HLC and writer token;
- concurrent or clock-skewed cross-host writes do **not** promise that wall
  clock invocation or acknowledgement order determines the winner.

This deliberately replaces collection-wide linearizable last-write-wins. A
later acknowledged write can lose when it was allocated from an unobserved
clock sufficiently behind the earlier writer. The API and documentation call
this deterministic convergent last-write-wins, not linearizable ordering.

### Bounded active tail

Removing the sequencer increases ingest capacity and therefore makes bounded
materialization mandatory. Replace the per-stripe sequence-modulus trigger
with collection-wide work accounting and one fenced materializer:

- writer stripes publish bounded progress metadata containing durable and
  materialized record/byte frontiers; vector payload never enters a mutable
  head;
- a process requests maintenance when its local unmaterialized contribution,
  the observed collection total, or the oldest durable extent crosses the
  soft bound;
- contenders acquire a short S3 materializer lease; only the winner builds and
  publishes a delta, while losers continue direct acknowledgements below the
  hard bound;
- the winner refreshes the active-stripe directory, captures a stable frontier,
  and builds immutable indexed L0 delta segments incrementally;
- every dense/PQ, exact, sparse, text, and late-interaction sidecar required by
  that delta is complete and checksum-verified before its manifest becomes
  visible;
- manifest-version-fenced checkpointing and retirement preserve old-reader and
  crash safety;
- writers return typed retryable backpressure before accepting work that would
  exceed the hard collection tail bound.

The first defaults are qualification inputs, not frozen product claims: a
soft bound of 8,192 records or 32 MiB, a hard bound of 32,768 records or
128 MiB, and a maximum unmaterialized age of 250 ms. Local structural tests
must prove the bounds; AWS evidence may lower them. Raising a bound to make a
test pass is not a performance fix.

Queries search only the bounded raw tail plus immutable indexed deltas and the
base hierarchy. Uncached queries must pass the latency gate from object-store
reads and SIMD/PQ execution. Decoded caches, prefetch, and single-flight reuse
remain optional accelerators and are reported separately.

## Failure and lifecycle behavior

- A clock allocation followed by a failed extent PUT is an unused version gap.
- An accepted-but-response-lost extent PUT is reconciled by exact key and
  checksum; unequal bytes at the same key are a fencing failure.
- A process crash after extent creation leaves an authoritative immutable
  write discoverable by the existing epoch recovery protocol.
- Clock rollback cannot make one live clock regress because its HLC is
  monotonic. Cross-process rollback is covered by the documented convergent,
  non-linearizable conflict contract.
- A writer-token collision detected in one snapshot or extent set is hard
  corruption. The persisted writer identity and extent identity allow the
  reader to distinguish a true duplicate from unequal payload reuse.
- Materializer lease loss prevents publication but does not revoke durable
  writer extents. A successor rebuilds from the stable captured frontier.
- Publication succeeds before any covered extent is retired. Failed or losing
  builds remain immutable GC candidates behind the existing grace and
  reachability rules.
- Readers fail closed on corrupt versions, missing sidecars, an exceeded hard
  tail, or modality-incomplete commits. They never skip a branch to meet a
  latency target.

## TDD and qualification gates

Implementation proceeds in independently delivered slices and must prove:

1. The mutation clock is monotonic under equal milliseconds, physical-clock
   rollback, logical overflow, concurrent allocation, and observed remote HLC
   advancement. Big-endian encoding round-trips and byte order equals semantic
   order.
2. Two unequal payloads with one identical version fail closed. Same-writer and
   observed-version ordering pass; an injected clock-skew case permanently
   documents that later acknowledgement is not a linearizability guarantee.
3. WAL, segment, tombstone, ID-delta, compaction, reopen, sparse, text, named
   dense, typed dense, and late-interaction paths preserve the full version.
   Old experimental formats are rejected clearly with no dual reader.
4. A normal group uses exactly one immutable extent PUT per touched writer
   stripe and zero global generation-counter requests. Ordinary upsert/delete
   use no `id-directory/last-write-wins/NEXT` object.
5. Independent writers with disjoint and conflicting IDs converge after
   reopen, drain, compaction, crash takeover, and reversed stripe assignment.
   Every acknowledged non-conflicting record remains visible exactly once.
6. Aggregate tail pressure elects one materializer, publishes only complete
   indexed L0 deltas, bounds raw records/bytes/age, and applies backpressure
   before the hard limit. Concurrent materializers cannot duplicate visible
   values or turn a publication conflict into an acknowledgement failure.
7. Local 768D structural qualification reconciles raw artifacts, request roles,
   visibility, deterministic conflict results, cold reads, and resource bounds.
8. One frozen AWS revision runs five paired repetitions for 2K and 16K logical
   cells with 1, 8, and 32 independent writers. Required gates are write p95
   below 200 ms, active-tail and post-drain read p95 below 200 ms, complete
   inserted-ID visibility, recall 1.0 for the routing matrix, bounded tail
   work, and honest acknowledgement and drain-inclusive throughput.
9. Realistic uncached 1M/768D and 1M/1536D datasets must then prove recall@10
   at least 0.95 and read p95 below 200 ms before any 100M scale or competitor
   claim. Dense, sparse, text, named dense, typed vectors, and late interaction
   remain unqualified until their own correctness, recall, latency, and
   resource gates pass.

Frozen campaign artifacts remain immutable and architecture-specific. A new
format revision starts a new evidence lineage; v62 remains causal evidence for
removing the global counter, not a like-for-like performance baseline.
