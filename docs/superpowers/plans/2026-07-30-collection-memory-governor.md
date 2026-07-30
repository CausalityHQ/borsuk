# Collection-wide memory governor implementation plan

> Execute each task test-first and keep every commit independently buildable.

**Goal:** Enforce one RAM envelope across primary and named modalities,
including resident manifests, retained caches, and concurrent decode work.

**Design:** `docs/superpowers/specs/2026-07-30-collection-memory-governor-design.md`

**Primary files:** `crates/borsuk/src/segment_cache.rs`,
`crates/borsuk/src/index.rs`, `crates/borsuk/src/record.rs`,
`crates/borsuk/tests/local_index.rs`, bindings and production documentation.

## Task 1: Add byte-accounting primitives

Write failing unit tests in `segment_cache.rs` proving:

- nonblocking retained reservations never exceed capacity;
- dropping an owned retained reservation releases its exact bytes;
- peak retained bytes are recorded;
- owned transient permits can outlive the call that acquired them;
- mixed transient callers never exceed capacity; and
- one oversized transient object runs alone.

Implement:

- `RetainedBytePool` with `try_reserve`, `used_bytes`, `peak_bytes`, and
  `capacity_bytes`;
- an owned permit that stores `Arc<RetainedBytePool>`;
- `ByteAdmissionGate::acquire_owned(&Arc<Self>, bytes)` and peak/capacity
  counters while preserving the borrowed-permit API.

Run:

```bash
cargo test -p borsuk segment_cache::tests --locked
```

Commit: `feat: add collection byte accounting primitives`

## Task 2: Put all retained cache entries under the shared pool

First add failing tests proving:

- `DecodedObjectCache::new(0)` retains no entries;
- two different typed decoded caches sharing a small retained pool cannot make
  their combined resident bytes exceed it;
- decoded-segment entries release retained reservations on eviction;
- ordinary and late-interaction sidecar index caches respect the same pool.

Extend `DecodedObjectCache`, `DecodedSegmentCache`, `SidecarIndexCache`, and
`LateInteractionSidecarIndexCache` with optional shared-pool constructors.
Store the owned reservation in each cache entry. On reservation failure, evict
the cache's oldest eligible entry and retry; skip retention when no local
eviction can free enough shared bytes. Do not block a query on cache admission.

Expose test-only resident byte counters and assert that cache-local bytes match
owned shared reservations.

Run:

```bash
cargo test -p borsuk segment_cache::tests --locked
cargo test -p borsuk index::tests --locked
```

Commit: `feat: govern retained cache bytes across cache types`

## Task 3: Preflight the complete collection manifest set

Add integration regressions in `crates/borsuk/tests/local_index.rs`:

1. create a primary plus two named dense modalities;
2. make each manifest estimate fit the runtime budget individually;
3. make their checked aggregate exceed it; and
4. assert `open_with_options` returns `RamBudgetExceeded` with the aggregate
   resident byte count.

Add corruption tests for mismatched child persisted RAM budgets and checked-sum
overflow.

Refactor `open_with_storage` to load and validate all manifests pinned by the
collection snapshot before constructing any index handle. Introduce a
`LoadedModality` descriptor and compute:

- effective collection budget;
- checked aggregate resident bytes;
- retained and transient pool capacities.

Pass the already-loaded child manifests into `open_named_indexes`; do not load
them a second time.

Run:

```bash
cargo test -p borsuk --test local_index collection_ram --locked
```

Commit: `feat: enforce aggregate collection resident budget`

## Task 4: Share one collection read runtime

Add pointer-identity tests for a root and two children covering decoded segment,
lexical, graph, tombstone, BM25, late-interaction, sidecar, WAL, search-count,
decode-count, and byte-admission state.

Create `CollectionReadRuntime` after collection preflight. It owns the shared
governor, all content-addressed caches and single-flight maps, and all admission
gates. Replace the corresponding `BorsukIndex` fields with
`Arc<CollectionReadRuntime>`. Keep modality-version caches on `BorsukIndex`.

Construct the runtime once in open. On create, install one runtime into the root
and all newly created children before returning. Remove the post-open special
case that shares only `wal_tail_runtime`.

Make the retained pool capacity the lower half of bytes left after aggregate
resident manifests and the transient pool the upper half. Per-cache options
remain local ceilings.

Run:

```bash
cargo test -p borsuk index::tests --locked
cargo test -p borsuk --test local_index named --locked
```

Commit: `refactor: share read runtime across collection modalities`

## Task 5: Account active decode objects

Add concurrency tests that overlap dense segment, projected-vector,
global-graph, lexical, WAL, and late-interaction decoding. Assert the shared
transient peak never exceeds capacity and results equal an unbounded reference.

Use owned transient permits around decoded allocations:

- derive a conservative dense-cell estimate from persisted object count,
  dimensions, codec widths, and stored object bytes;
- use exact requested row count and element width for Arrow vector batches;
- use persisted lexical decoded-byte estimates;
- use graph object decoded estimates;
- use late-interaction batch metadata;
- use the existing decoded WAL-run estimate.

Keep each permit in the query-local wrapper until the decoded object is dropped.
Cached objects transfer to retained-pool ownership before their transient permit
is released. Avoid nested acquisition by reserving at leaf decode boundaries.

Share the count-based gates. Prove a hybrid query completes with
`max_concurrent_searches=Some(1)` because there is no outer recursive
acquisition.

Run:

```bash
cargo test -p borsuk index::tests --locked
cargo test -p borsuk --test local_index hybrid --locked
```

Commit: `feat: govern multimodal decode working memory`

## Task 6: Keep the aggregate invariant across mutations and refresh

Add tests where a root or named modality prospective manifest would push the
collection sum over budget. Assert publication fails before the new collection
frontier becomes visible and the previous snapshot remains readable.

Maintain modality resident estimates in the collection runtime. Add a
rollback-safe prospective reservation guard used by every manifest publication
path. Commit the new estimate only after publication succeeds; refresh replaces
the complete estimate map from the newly pinned collection snapshot.

Replace modality-local `enforce_ram_budget` calls on opened handles with the
collection reservation path. Retain the standalone helper during bootstrap
before a runtime exists.

Run:

```bash
cargo test -p borsuk index::tests --locked
cargo test -p borsuk --test local_index ram_budget --locked
```

Commit: `feat: preserve collection RAM budget across publication`

## Task 7: Expose and validate collection memory telemetry

Add fields to Rust reports/stats and both bindings:

- collection resident manifest bytes;
- retained bytes/capacity/peak;
- transient bytes/capacity/peak.

Root stats and public hybrid reports use the collection resident sum. Replace
the current hybrid `max()` aggregation. Update Python repr/conversion,
TypeScript conversion/types, CLI JSON expectations, API docs, architecture,
production readiness, and the hardening audit.

Run:

```bash
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build -p borsuk-cli --locked
cargo test -p borsuk-python --locked
cargo test -p borsuk-node --locked
```

Commit: `feat: report collection memory envelope`

## Task 8: Production verification and immutable evidence

Run the exact local release gates:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo build --workspace --all-targets --locked
python3 -m unittest discover -s benchmarks/methodology/tests -p 'test_*.py'
python3 scripts/validate_research_docs.py
python3 scripts/validate_reported_comparisons.py
```

Push the verified commit. On AWS, run a frozen multimodal matrix with dense,
sparse/text, and late-interaction legs under concurrent hybrid load. Record
governor peaks, process RSS, recall/result equality, p50/p95/p99, QPS, and
object-store requests. Validate the immutable artifact prefix before inspecting
or reporting measurement values.

Only then close the P1 memory-accounting item in
`docs/research/production-hardening-audit-2026-07-28.md`.

Commit: `docs: publish collection memory evidence`
