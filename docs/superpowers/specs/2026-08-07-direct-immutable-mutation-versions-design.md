# Direct Immutable Mutation Versions and Bounded Delta Ingest

**Status:** Approved by the user on 2026-08-07.

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

Ordinary upsert and delete paths allocate versions in process memory, create
one checksum-verified immutable WAL extent, then conditionally publish that
extent through the owning stripe's versioned JSON head. The per-stripe head CAS
is the durability, discoverability, and stale-owner acknowledgement boundary.
Normal acknowledgement therefore performs two sequential S3 PUTs but no GET,
HEAD, LIST, collection counter, manifest publication, materialization, or
external-service request. Independent stripes never contend on one head.

S3 remains the only durable service. DynamoDB, a required RPC ingest service,
and cache-dependent correctness or performance are excluded.

Strict duplicate-rejecting `add` remains a distinct operation and may use its
existing bounded explicit-ID coordination. Its cost and semantics must not be
presented as ordinary upsert throughput. No strict compare-and-set mutation is
claimed unless it has a separate protocol that proves atomic validation and
publication.

### Mutation version

Replace caller-visible record `generation: u64` with an internal, checked,
lexicographically ordered 192-bit `MutationVersion`:

```text
bits 191..144  physical Unix milliseconds (48 bits)
bits 143..128  logical counter             (16 bits)
bits 127..0    writer identity             (128 bits)
```

The library generates a cryptographically random 128-bit writer identity for
each independent mutation clock. It is never caller-controlled. Retaining the
complete identity avoids a 64-bit birthday-collision contract. A process-local
atomic hybrid logical clock generates versions without object-store I/O. Its
atomic state is the complete 64-bit
`(physical_milliseconds, logical_counter)` prefix, not merely the physical
millisecond:

1. atomically raise the local HLC floor to the complete HLC prefix of every
   version observed by this handle;
2. if the current physical millisecond exceeds the floor's physical component,
   allocate from `(now, 0)`; otherwise allocate the next logical value after
   the complete observed floor;
3. reserve a group as one contiguous HLC-prefix range with compare-and-swap;
4. if the 16-bit logical component overflows, advance the synthetic physical
   component by one millisecond and reset the logical component;
5. append the stable writer identity and compare the complete 192-bit value as
   unsigned big-endian bytes.

Advancing past the complete observed prefix is essential: copying only an
observed physical millisecond and resetting its logical counter could allocate
a version below the value just observed.

Times before the Unix epoch and physical values beyond `2^48 - 1` are rejected.
Every checksum-covered extent, segment, tombstone/ID delta, sidecar root, and
aggregate descriptor persists its maximum mutation version so `refresh()` can
advance the clock without decoding vector payloads.

`BorsukIndex` owns an `Arc<MutationClock>`. Rust clones of one logical handle
share it; a separate `open()` creates a new clock. `GroupCommitWriter::new`
transfers the consumed handle's clock to its front end and assigns versions
before ID deduplication and lane fan-out. Worker stripes never allocate or
rewrite versions. Repeated IDs in one group retain the greatest assigned
version and must pass the equal-version digest check. Reads and refreshes
advance the handle's observed HLC floor but never rewrite an allocated version.

There is no BORSUK-specific mutation-version wire format. In-memory comparison
uses the 24-byte big-endian value, while durable Parquet and Arrow schemas store
the logical fields directly as `mutation_hlc: UInt64`,
`mutation_writer: FixedSizeBinary(16)`, and
`mutation_digest: FixedSizeBinary(32)`. Parquet and Arrow implementations may
apply their standard dictionary and compression encodings; a stock reader must
still recover the typed logical columns without a BORSUK framing decoder.
Materialization and compaction preserve these logical values exactly. Manifest
and catalog generation counters are unrelated and remain their existing
integer type.

### Standard durable formats

Every production object is independently readable as one of these portable
formats:

- Arrow IPC stores foreground mutation extents and fixed-width candidate,
  quantizer-code, exact-vector, graph-adjacency, and late-interaction sidecars
  where low encode cost or zero-copy typed arrays are operationally useful;
- Parquet stores materialized segments, tombstones, ID deltas, and
  scan-oriented dense, sparse, lexical, and metadata tables;
- versioned UTF-8 JSON stores small mutable or conditional control records,
  including heads, directories, manifests, leases, fences, and checkpoints.

Schema versions and BORSUK semantics may use ordinary Parquet/Arrow schema
metadata or JSON fields. This does not permit a custom outer frame, magic
prefix, packed row file, opaque graph blob, or hand-written binary control
record. PQ codes use Arrow fixed-size lists of `UInt8`; graph adjacency uses
Arrow list arrays with typed integer neighbours. Artifact maximum versions are
typed columns/statistics plus documented schema metadata, so refresh can read
standard file metadata without decoding vector payloads. Arrow mutation
extents deliberately avoid Parquet's per-file encoding/footer overhead on the
foreground acknowledgement path. Old experimental custom layouts are rejected
rather than retained behind dual readers.

### Canonical mutation envelope

Versioning applies to a logical entity mutation, not only its dense vector:

```text
Mutation {
    id,
    version,
    operation: Put(CanonicalRecord) | Delete,
    canonical_digest,
}
```

`CanonicalRecord` contains primary and named dense vectors, sparse vectors,
text terms, late-interaction matrices, metadata, and storage type declarations.
The digest is computed once over that logical representation and survives
physical transformation into WAL, PQ, exact, lexical, sparse, and
late-interaction artifacts. Derived token rows never allocate their own
versions. Equal `(id, version)` values with unequal operation or digest are
corruption even when their physical encodings differ.

Foreground delete appends a new versioned tombstone even when the caller's
pinned snapshot already appears deleted, because an unseen independent upsert
may exist. Delete reporting therefore counts accepted durable mutations;
exact changed-ID and live-tombstone counts are observed/materialized
statistics, not linearizable foreground facts.

### Conflict semantics

All readers, materializers, compactions, reopen paths, and modalities select
the greatest `MutationVersion` for one record ID. Equal versions with unequal
operation/digest are corruption and fail closed. Consequently every replica
and reader converges on exactly one value or tombstone.

The production contract is:

- writes allocated by one mutation clock preserve program order;
- a write allocated after that handle observes a remote version dominates the
  observed version;
- unobserved writes from independent hosts are ordered deterministically by
  HLC and writer identity;
- concurrent or clock-skewed cross-host writes do **not** promise that wall
  clock invocation or acknowledgement order determines the winner.

This deliberately replaces collection-wide linearizable last-write-wins. A
later acknowledged write can lose when it was allocated from an unobserved
clock sufficiently behind the earlier writer. The API and documentation call
this deterministic convergent last-write-wins, not linearizable ordering.

One entity envelope is atomic across all of its modalities. A public batch
that spans multiple writer stripes retains the existing partial-durable-success
contract and returns exact per-stripe extent identities; it is not advertised
as an all-or-none multi-entity transaction.

### Bounded active tail

Removing the sequencer increases ingest capacity and therefore makes bounded
materialization mandatory. Replace the per-stripe sequence-modulus trigger
with collection-wide work accounting and one fenced materializer:

- every one of the fixed 64 writer stripes enforces its own durable-minus-
  materialized record, byte, and extent quotas before creating an extent. The
  initial hard quota is 512 records, 2 MiB, or eight extents per stripe,
  proving a collection-wide maximum of 32,768 raw records, 128 MiB, and 512
  extent GETs even when all stripes are live and the materializer is
  unavailable;
- writer stripes publish bounded progress metadata containing cumulative
  durable and materialized record/byte frontiers; vector payload never enters
  a mutable head. Lease takeover reconstructs exact counters from immutable
  extents before admitting another write;
- a process requests maintenance when its local unmaterialized contribution,
  the observed collection total, or the oldest durable extent crosses the
  soft bound;
- contenders acquire a monotonically fenced S3 materializer epoch. Only the
  current holder builds a candidate, while losers continue direct
  acknowledgements below their stripe hard bounds;
- the winner refreshes the active-stripe directory, captures a stable frontier,
  and builds immutable indexed L0 delta segments incrementally;
- every dense/PQ, exact, sparse, text, and late-interaction sidecar required by
  that delta is complete and checksum-verified before its manifest becomes
  visible;
- manifest-version-fenced checkpointing and retirement preserve old-reader and
  crash safety;
- a root publication names the materializer fencing epoch, exact predecessor,
  and captured `(stripe, lease_epoch, sequence)` prefixes. The lease is only an
  efficiency hint: collection-root CAS plus those fences prevent a paused old
  holder from publishing or retiring through a successor;
- writers return typed retryable backpressure before an extent would exceed
  that stripe's fixed quota. No stale collection-wide observation is used to
  claim a strict hard bound.

The first collection-level maintenance defaults are qualification inputs, not
frozen product claims: a soft bound of 8,192 records or 32 MiB and a maximum
unmaterialized age of 250 ms. The per-stripe quotas above are the hard safety
bound and do not depend on the freshness of a directory or aggregate counter.
The age is a maintenance trigger/SLO, not a structural guarantee after every
writer exits. L0 compaction starts at eight segments or 256 MiB; observed debt
at 32 segments or 1 GiB applies write backpressure. Local tests additionally
bound raw and L0 GET fanout. AWS evidence may lower these inputs. Raising a
bound to make a test pass is not a performance fix.

Queries search only the bounded raw tail plus immutable indexed deltas and the
base hierarchy. Uncached queries must pass the latency gate from object-store
reads and SIMD/PQ execution. Decoded caches, prefetch, and single-flight reuse
remain optional accelerators and are reported separately.

Delta construction uses the persisted production router, quantizer, and
sidecar policy; it must not silently fall back to scalar bounds. Publication
occurs only after every modality artifact is checksum-verified, then one
collection-root commit makes all modality manifests visible together.

## Failure and lifecycle behavior

- A clock allocation followed by a failed extent PUT is an unused version gap.
- Versions and canonical bytes are allocated once for the stable
  `(stripe, lease_epoch, extent_sequence)` key. Extent creation alone is staged,
  not acknowledged. A conditional JSON-head PUT names the exact extent,
  checksum, sequence, cumulative tail counters, and maximum mutation version.
  Only a successful or exactly reconciled head publication makes it
  authoritative. Receipts expose the complete extent and head identities.
- If extent creation succeeds but head publication loses its CAS, the extent is
  an unreachable GC candidate and the write is not acknowledged. A stale owner
  can never publish after takeover because it retains the predecessor head
  version; this remains true even when it paused before or during extent PUT.
- Accepted-but-response-lost extent or head PUTs block later stripe work until
  the exact object/head is read and checksum/content reconciled. Unequal bytes
  or a different successor are fencing failures. Exceptional reconciliation
  requests are reported separately from the two-request steady state.
- A process crash after head publication leaves an authoritative immutable
  write discoverable from the stripe head. A crash before publication leaves a
  non-authoritative immutable GC candidate.
- Clock rollback cannot make one live clock regress because its HLC is
  monotonic. Cross-process rollback is covered by the documented convergent,
  non-linearizable conflict contract.
- Materializer lease loss prevents publication but does not revoke durable
  writer extents. The stale holder's fencing epoch cannot win collection-root
  CAS after takeover; a successor rebuilds from the stable captured frontier.
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
   advancement, including an observed version at the same physical millisecond
   with a much larger logical counter. Big-endian encoding round-trips and byte
   order equals semantic order.
2. Two unequal operations/digests with one identical version fail closed.
   Same-writer and observed-version ordering pass; an injected clock-skew case
   permanently documents that later acknowledgement is not a linearizability
   guarantee. Callers cannot supply internal mutation versions.
3. WAL, segment, tombstone, ID-delta, compaction, reopen, sparse, text, named
   dense, typed dense, and late-interaction paths preserve the full version,
   canonical digest, typed standard-format columns, and maximum-version
   metadata. Stock Parquet, Arrow IPC, and JSON readers validate every durable
   object role. Old experimental formats are rejected clearly with no dual
   reader.
4. A normal group uses exactly one immutable extent PUT and one conditional
   per-stripe JSON-head PUT per touched writer stripe, with zero steady-state
   GET/HEAD/LIST or global generation-counter requests. Ordinary upsert/delete
   use no `id-directory/last-write-wins/NEXT` object. A paused stale owner loses
   head publication after takeover and cannot acknowledge an undiscoverable
   extent.
5. Independent writers with disjoint and conflicting IDs converge after
   reopen, drain, compaction, crash takeover, and reversed stripe assignment.
   Every acknowledged non-conflicting record remains visible exactly once.
   Ambiguous PUT reconciliation reuses identical bytes and extent identity.
6. Aggregate tail pressure elects one materializer, publishes only complete
   indexed L0 deltas, bounds raw records/bytes/extents and L0 fanout, and
   applies backpressure before each stripe's hard limit. Age is tested as an
   active-maintenance SLO. A paused stale materializer cannot publish or retire
   after a fenced successor. Concurrent materializers cannot duplicate visible
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
