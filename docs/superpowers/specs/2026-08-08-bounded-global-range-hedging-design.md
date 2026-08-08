# Bounded Global Range Hedging Design

**Status:** Approved for autonomous implementation from terminal v67/v69 evidence

## Problem

The terminal v67 `c2000/r01/l1/w8` arm preserved 128,000/128,000 point
visibility, inserted-ID recall@10 1.0, 83.387 ms write p95, and 7,057.627
acknowledged records/s, but failed the post-drain read gate at 210.608 ms p95.
The failing second query was not a cold-open sample: 206.748 ms was spent in
the materialized-delta approximate stage while exact rerank consumed only
0.162 ms. The same 3.70 MB logical query took 117.956 ms in v66. This isolates
remote range-wave variance rather than candidate scoring, recall, or persistent
format as the immediate tail.

The terminal v69 confirmation rejected wider physical stripes as the general
fix. At identical 2.634 MB/query and recall 1.0, 4 MiB stripes reduced GETs/query
from 2.788 to 2.028 but improved pooled p95 only 5.18% and won only two of five
paired repetitions. The 1 MiB default therefore remains unchanged.

AWS's first-party S3 performance guidance recommends concurrent byte-range
fetches and aggressive retries of slow operations for latency-sensitive
applications. A retry can take a different distributed path and finish quickly.
BORSUK currently waits indefinitely for every member of a striped range wave;
one straggler therefore sets the query tail.

Primary source: [AWS, “Performance guidelines for Amazon S3”](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html).

## Approaches Considered

### 1. Bounded slow-read hedge for immutable global-PQ stripes (selected)

Start one duplicate range GET only when the original immutable global-PQ
stripe has not completed after a fixed delay. Return the first successful
response, cancel the loser, and preserve the existing ordered reconstruction.
Expose the delay through `OpenOptions`; `None` disables hedging for paired
qualification. The production candidate uses 75 ms, chosen above the terminal
raw S3 3,072-byte PUT p95 of 27.436 ms and below the 200 ms query objective.

This directly attacks the observed straggler while preserving logical bytes,
candidate selection, exact rerank, recall, and standard Arrow artifacts. It
can increase physical GET count and billed transfer for slow operations, so
both must remain visible in reports and promotion gates.

### 2. Fuse base and delta into one global scheduler

Load descriptors concurrently, route each trained layer, schedule both layers
under one query-wide concurrency budget, and exact-rerank once. This removes
recursive search and cold serialized setup and is the preferred next
architecture experiment. It does not eliminate a straggling S3 request inside
an already-parallel delta wave, so it follows rather than replaces this test.

### 3. Rebuild the global base during every drain

This removes the second ANN layer but introduces a corpus-wide read and
re-encode at the write maintenance boundary. Its cost grows with the corpus
and conflicts with high-throughput ingest and 100M-vector scalability. The
existing half-base promotion threshold remains explicit maintenance policy;
ordinary drain must not adopt this approach.

## Runtime Contract

- Add `OpenOptions::global_pq_slow_read_hedge_after: Option<Duration>`.
- Default to `Some(Duration::from_millis(75))` for production S3-oriented
  global-PQ reads; `None` disables the hedge.
- Apply hedging only to immutable global-PQ striped range reads. Do not change
  WAL reads, coordination objects, manifests, segment writes, or exact-rerank
  candidate selection.
- Start at most one hedge per physical stripe. Preserve the existing
  `DEFAULT_GLOBAL_PQ_PREFETCH_STRIPES` concurrency bound for primary requests;
  hedges may temporarily double only the number of slow in-flight stripes.
- Return the first successful result. If one request fails, wait for the other;
  fail only when both fail. Preserve checksum/range-length validation above
  this layer.
- Count every issued backing GET through the existing object-store wrapper.
  `bytes_read` remains logical query bytes; backing request/byte telemetry
  records physical amplification.
- The disk-cache key and stored bytes remain the logical ordered range bundle;
  a cancelled loser must never write cache state.

## TDD and Local Verification

The regression test uses real asynchronous futures with deterministic delays:

1. A primary that completes before 75 ms must return its bytes and issue no
   hedge.
2. A primary delayed beyond the threshold and a fast hedge must return the
   hedge result before the primary delay and record two attempts.
3. One failed request plus one successful request must succeed; two failures
   must fail.
4. A multi-stripe read must reconstruct exactly the original ordered bytes and
   stay within one hedge per slow stripe.

The first hedge test must be observed RED against the current single-attempt
implementation before production code changes. Focused storage and global-PQ
tests, strict Clippy, the complete Rust gate, and pinned Python policy tests are
required before push.

## Paired AWS Qualification

Qualification reuses the immutable terminal v67 index and canonical samples;
it writes no index object. It compares:

- control: 1 MiB stripes, hedge disabled;
- candidate: 1 MiB stripes, 75 ms hedge;
- five paired repetitions with alternating order;
- 500 deterministic writer-0 queries per arm, spread uniformly over that
  writer's 1,000 operations;
- one process and no disk cache per arm, so every code/rerank payload remains
  an uncached backing read while immutable descriptors may remain resident as
  they would in a service process.

Every arm must preserve identical query IDs, inserted-ID recall@10 1.0,
identical logical bytes, zero PUT/DELETE operations, and complete raw resource
and storage telemetry. The candidate is promotable only when:

- pooled and worst-repetition p95 are below 200 ms;
- candidate p95 is no worse in at least four of five paired repetitions;
- pooled p95 improves by at least 10%;
- pooled p50 regresses by no more than 5%;
- physical GETs/query increase by no more than 20%; and
- physical backing bytes/query increase by no more than 20%.

Terminal markers and infrastructure health are the only eligible observations
until every repetition and the root completion marker exist. The repository
validator must run before any measurement CSV is opened.

## Decision

Promote the 75 ms default only if every gate passes. Otherwise keep hedging
disabled by default, preserve the terminal artifacts, and proceed to the fused
base-plus-delta scheduler with the failed evidence recorded. Passing this
qualification proves only the 128k/768D S3 shape; 2K/16K logical-cell,
1/8/32-writer, concurrency, larger dataset, and 100M-vector claims remain
separate required gates.
