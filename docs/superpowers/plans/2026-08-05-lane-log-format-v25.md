# Lane-owned ingest log format-v25 implementation plan

**Goal:** Replace the strict scalar mutation protocol's 7--8 PUT and 3--9 GET
acknowledgement chain with a two-dependent-write, zero-GET steady-state lane log,
while retaining durable visibility, exact duplicate/upsert semantics, bounded
fresh-read work, crash recovery, and scalable background materialization.

**Decision evidence:** The terminal Cohere 1M/768D v6 cell measured strict
scalar p95 at 1,028.593 ms and exposed synchronous 21.7-second materialization.
The independently validated `object-store-floor-v2` campaign measured the target
3,072-byte immutable PUT followed by a changing 4,096-byte conditional HEAD at
64.781 ms p95 over 1,000 samples. The object store is not the blocker; protocol
amplification is.

**Compatibility:** This is a pre-release format cutover. Increment the storage
format once when the new path becomes authoritative, reject older experimental
indexes, and delete superseded claims, pending commits, transaction states,
shared generation counters, and linked frontiers rather than retaining dual
read/write paths.

## Invariants

1. An ID maps permanently to `hash(id) % lane_count`; all mutations for it are
   serialized by one fenced lane owner.
2. A mutation group is acknowledged only after its immutable block PUT and the
   conditional lane-HEAD PUT both succeed. These are the only foreground
   object-store writes in steady state; no foreground GET or LIST is permitted.
3. A block contains records, tombstones, exact ID-directory changes, and
   mutation metadata in one checksummed container. A block without HEAD
   reachability is invisible and GC-eligible.
4. `generation = (lease_epoch, lane_sequence)` provides deterministic LWW
   ordering without a shared counter. Generated IDs include the same tuple.
5. The lane owner holds an exact partitioned live-ID authority under the RAM
   budget. Bloom filters may reject work but never establish absence.
6. Refresh reads the fixed lane HEAD set in parallel and never lists pending
   transactions or pointer-chases a linked frontier.
7. Acknowledgement never materializes, flushes, compacts, or deletes objects.
   A separately fenced maintenance worker advances `materialized_sequence`.
8. The unmaterialized tail has a hard byte/record bound derived from measured
   read and materialization rates. Writers apply bounded backpressure when the
   maintainer falls behind; they never permit unbounded tail growth.
9. Cross-lane batches use per-lane atomicity and return every committed
   `(lane, lease_epoch, sequence)` receipt. The public contract must disclose
   that a crash can partially commit a multi-lane batch; deterministic retries
   are idempotent.

## Stage 1: one block and one flat HEAD

- Add RED request-trace tests requiring one immutable block PUT plus one
  conditional HEAD PUT, zero GET/LIST, and no transaction-state/descriptor/run
  objects on a warm append.
- Add crash-point tests: before block, after block/before HEAD, and after HEAD.
  Only the last state is visible; the orphan block is reclaimable.
- Implement a fenced, checksummed block container and bounded flat HEAD.
- Fail immediately on HEAD CAS conflict; a valid lane owner never rebases an
  acknowledgement through an unseen competing writer.

## Stage 2: leases, fencing, and exact ID authority

- Add RED tests for lease loss fencing a zombie writer and strict duplicate
  rejection issuing zero object-store requests.
- Build the lane's exact ID authority at lease acquisition from the persistent
  base directory plus committed tail, charging it to the RAM budget.
- Remove claim pages, synchronized checkpoints, transaction-state ownership,
  generation shards, and shared generation counters.

**Implementation status:** The lane HEAD now carries the owner, monotonic lease
epoch, and expiry. Acquisition rebuilds and budget-checks exact ID state from a
caller-supplied materialized base plus every checksummed HEAD-reachable block
before its lease CAS. Blocks carry live/deleted/purged ID deltas; strict duplicates and expired
leases fail before object-store I/O; takeover, renewal, ambiguous-CAS poisoning,
and delete/upsert/purge transitions have deterministic tests. The old
claim/counter path remains authoritative until the Stage 3 public write-path
cutover, so this stage is not a performance claim by itself.

## Public writer cutover progress

The process-local writer routes every record by stable ID hash over the
persisted ownership-lane count, independently of the configured worker-pool
width. Each worker owns one or more fenced lane handles; changing execution
parallelism therefore cannot move an ID to a different durable log. A call
spanning ownership lanes is partitioned before enqueue, waits for every touched
lane, and returns explicit per-lane receipts plus aggregate request accounting.
Workers now acknowledge through the format-v25 primitive: one immutable block
PUT followed by one conditional HEAD PUT with zero steady-state reads.
Lane HEADs also persist a generation clock seeded once from the legacy shard
allocator at writer acquisition. This makes records acknowledged after an
existing put/delete dominate that older generation without adding reads to the
acknowledgement path. Same-ID requests coalesced into one group use submission
order instead of failing the whole group.

Lane blocks can now encode and losslessly decode real `VectorRecord` batches
through the existing inline-vector WAL table schema. A fixed-fanout reader
visits the configured HEAD paths without LIST and fetches each reachable block
with one GET (an earlier size-HEAD plus GET path is covered by a zero-HEAD
regression test). HEAD reads overlap on the process-wide bounded blocking-I/O
pool, with an overlap/cap regression test, while result order remains stable.
The reader is installed in `BorsukIndex`; open and refresh fetch the fixed HEAD
set in parallel, decode the bounded reachable blocks, and merge their newest
per-ID records into fresh exact reads without LIST. A format activation marker
is still required before the version bump so reopen can distinguish a lane that
was never initialized from loss of an authoritative HEAD.
The shared decoded-tail cache is keyed by the checksums of the observed HEAD
bytes rather than a handle-local revision counter, preventing cloned handles at
the same local revision from aliasing different snapshots.

The v26 cutover collapses the two dependent acknowledgement PUTs into one
conditional HEAD PUT by carrying new blocks inline. The storage-version bump
is also the activation contract: creation installs every empty lane HEAD before
publishing `CURRENT`, so a valid v26 index can never treat a missing HEAD as an
unused lane. The owner spills at 8 MiB of inline payload after returning
the durable receipt: it uploads checksum-addressed block objects first, then
CAS-replaces only the inline representations with external descriptors. A
failed spill leaves the authoritative inline bytes untouched and is retried
before the lane accepts more work. Receipt telemetry carries exact HEAD bytes;
the benchmark validator recomputes their total and maximum from raw samples.

## Stage 3: fixed-cost refresh and fresh reads

- Add RED tests requiring exactly `lane_count` parallel HEAD GETs and zero LISTs
  regardless of committed transaction count.
- Resolve HEAD block descriptors directly; remove pending write-epoch scans,
  root reservations, per-transaction descriptors, and linked lane frontiers.
- Search the hard-bounded raw tail and materialized delta under one global
  candidate/segment budget so base-plus-delta does not double the read budget.

## Stage 4: off-acknowledgement materialization

- Add deterministic tests proving slow/failing maintenance cannot extend or
  fail an already-durable acknowledgement.
- Continuously build lane-local L1 segments against frozen global quantizers.
- Advance `materialized_sequence` only after the replacement manifest is
  durable; then retire covered blocks under normal GC age rules.
- Apply backpressure at the registered tail bound and report lag/throughput.

**Implementation status:** explicit `GroupCommitWriter::drain` now barriers all
workers, publishes the captured lane tail into immutable segments, advances
each lane's materialized prefix only after that manifest publication, retains
post-snapshot suffix blocks, and retains the immutable global search base while
the new segments remain a materialized delta. The bounded
tail survives repeated drain cycles in integration coverage. A serialized
background materializer now triggers at each 64-block lane interval; a
2,400-record integration stream crosses multiple former pressure boundaries
without caller-driven drains. A failed pass is retained and synchronously
retried before accepting more work. Lag/resource telemetry and an AWS rate
proof remain required before the sustained-ingest gate.

Materialization publishes a generation overlay atomically with new segments so
repeated drains of upserts do not multiply visible IDs. Concurrent drains are
serialized across writer clones; a regression test first reproduced 44 visible
rows from 32 IDs before this fence was added. Text and named-vector indexes are
currently rejected at group-writer construction because their child-index
materialization has not yet moved to lane log; silent partial modality writes
are forbidden. Supporting those modalities remains an exact pre-AWS gate.

The default worker pool now matches the eight persisted ownership lanes so a
multi-lane batch issues its dependent commits concurrently rather than routing
sixteen S3 round trips through one worker. Long-lived owners renew at half-TTL;
the rare renewal PUT is included in receipt accounting. Refresh still performs
one fixed GET per lane HEAD, but an unchanged HEAD set now reuses the pinned
snapshot without fetching or decoding any immutable block. When a HEAD advances,
reachable blocks are resolved concurrently across all lanes and previously
decoded checksum-addressed blocks are reused from the byte-bounded WAL cache, so
only new blocks issue GETs. The pinned snapshot also owns immutable per-block
record arcs keyed by path and generation. An advancing HEAD therefore reuses old
decoded blocks even with a zero-byte cache instead of copying the entire bounded
tail; retired descriptors release their snapshot references naturally. Real
request-counter and pointer-identity regressions cover these cases and the public
refresh path. The remaining public refresh LIST belongs to the legacy cell-WAL
compatibility path and must disappear with the format-v25 activation cutover;
this slice does not claim the final zero-LIST read gate.

## Exact gates before AWS

- Request trace: warm scalar acknowledgement `PUT <= 2`, `GET = HEAD = LIST =
  DELETE = 0`.
- Crash/fencing/idempotency, same-ID multiwriter, upsert/delete/generated-ID,
  named/text/sparse payload, reopen, GC, and concurrent maintenance suites pass.
- Full Rust tests, strict all-target/all-feature Clippy, Python validators,
  formatting, format-policy checks, and source/archive identity checks pass.

## AWS architecture qualification

- Fresh format-v25 indexes and result prefixes; Cohere Medium 1M/768D first,
  DBpedia OpenAI 1536D after its checksum-pinned preparation completes.
- Three paired repetitions for architecture selection; five only after the
  exact revision/defaults are frozen for publication.
- Separate workload points: scalar durable p95 below 200 ms; batch
  32/128/1,024 durable p95 below 200 ms; sustained 768D ingest at least 10,000
  vectors/s for ten minutes; tail bound never exceeded.
- With a non-empty tail and during concurrent materialization: recall@10 at
  least 0.95 and read p95 below 200 ms.
- Preserve raw artifacts and resource telemetry. Never inspect an incomplete
  measurement CSV.
