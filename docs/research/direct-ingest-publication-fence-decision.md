# Direct ingest publication-fence decision

**Status:** Architecture correction accepted on 2026-08-07 before live cutover.

## Decision

Retain the collision-free 192-bit `HLC64 + writer128` mutation version. Publish
each immutable Arrow IPC extent through one conditional PUT to its owning
stripe's versioned JSON head. A normal acknowledgement is therefore two
sequential, stripe-local S3 PUTs—not one PUT and not a collection-wide CAS.

The head publication is required for correctness as well as discovery. It
names the exact extent/checksum, advances the durable sequence and cumulative
tail counters, and carries the maximum mutation version. A writer acknowledges
only after its expected-head conditional update succeeds or an ambiguous
response is reconciled to that exact successor.

## Rejected shortcut

A proposed UInt64 layout used a recovered HLC prefix plus the six-bit persisted
stripe ID as a collision-free tie-breaker. It was rejected because stripe
takeover can overlap a paused old owner:

1. A allocates the next prefix and pauses before its extent PUT.
2. A's lease expires; B observes no such extent and takes over.
3. B allocates the same prefix/stripe version and publishes.
4. A resumes and creates its old-epoch extent.

This produces either equal-version unequal-digest corruption or an
acknowledged extent beyond the old epoch's sealed discovery frontier. Reserving
prefix intervals could prevent the version collision, but does not alone make a
late acknowledged extent discoverable. Authoritative-time exclusion or a
per-write publication fence is still required.

The current implementation also checks lease expiry after PUT using the
`now_ms` sampled before PUT. A slow or paused request can therefore cross
takeover and still pass that stale timestamp test. The head CAS removes this
timing assumption: takeover changes the expected object version, so the old
owner cannot publish or acknowledge regardless of clocks or pauses.

## Performance rationale

The second PUT is per stripe and has no healthy-path contention. The frozen v62
failure came from many writers retrying one global `NEXT` object—up to 99 S3
requests and 4.095 seconds for one group—not from two uncontended requests.
Group batching and 64 independent stripes preserve high aggregate throughput.
The exact two-PUT path must still prove write p95 below 200 ms on AWS; this
decision does not assume that result.

Keeping the full writer identity costs 16 bytes more than the rejected UInt64
layout, about 0.52% of a 768D f32 vector before compression. The terminal Arrow
codec qualification already included that writer column and remained far below
the local codec latency budget. That modest cost is preferable to reserved
range replenishment, authoritative clock dependencies, or a latent collision.

## Required tests

- Pause after extent creation, take over, resume, and prove the stale head CAS
  cannot acknowledge or expose the orphan.
- Lose the extent-PUT response and reconcile identical bytes at the exact key;
  unequal bytes fail closed.
- Lose the head-PUT response and acknowledge only when the stored JSON is the
  exact intended successor; another successor fails closed.
- Crash before and after head publication and prove the visibility boundary.
- Require two PUTs and zero GET/HEAD/LIST in the steady state, with exceptional
  reconciliation requests reported separately.
- Prove 1/8/32 independent writers never touch the removed global counter and
  meet write/read p95, visibility, recall, tail, and resource gates.
