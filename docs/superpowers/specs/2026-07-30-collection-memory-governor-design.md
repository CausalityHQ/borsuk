# Collection-wide memory governor design

**Date:** 2026-07-30

## Goal

Make the configured RAM budget a collection-level contract for a root index and
all of its named modalities. Opening or querying a multimodal collection must
not multiply retained-cache or decode-working-set budgets by the number of
modalities.

## Current failure

`open_with_storage` opens the primary manifest and then calls
`open_with_loaded_manifest` once per named dense or late-interaction modality.
Each call clones `OpenOptions` and constructs independent decoded caches,
sidecar-index caches, single-flight maps, search gates, and byte-admission
gates. Only the WAL runtime is replaced with the root runtime after all children
are open.

Consequences:

- the RAM-budget check is applied to each manifest independently rather than to
  their collection-wide resident sum;
- the default retained-cache ceilings can exceed the default 512 MiB budget
  even for one modality;
- every named modality repeats those ceilings;
- hybrid legs can decode concurrently under unrelated admission gates; and
- `SearchReport.resident_bytes_estimate` reports the maximum leg estimate
  instead of the collection sum.

## Contract

For an effective finite collection RAM budget `B`:

1. all loaded modality manifest estimates must sum to at most `B`;
2. the remaining bytes are split between retained immutable caches and active
   decode work;
3. root and child handles share both pools, all type-safe retained caches,
   single-flight maps, and concurrency gates;
4. the sum of resident manifest estimates plus the two pool capacities never
   exceeds `B`;
5. one irreducible decoded object larger than the transient pool may run alone,
   matching the existing weighted-admission behavior; and
6. a cache insertion that cannot obtain collection retained bytes is skipped
   after evicting eligible entries from that cache. Query correctness never
   depends on cache admission.

With no persisted or runtime RAM budget, existing explicit cache ceilings
remain in effect and transient byte admission remains optional. This is an
intentional research-only escape hatch.

## Preflight

`open_with_storage` loads and validates every manifest pinned by the collection
snapshot before constructing a `BorsukIndex`. It computes the checked sum of
`Manifest::resident_bytes_estimate()` and rejects the open with
`RamBudgetExceeded` when the sum exceeds the effective collection budget.

Create performs the same check after all modality manifests exist and before
returning the collection handle. Refresh and mutation publication validate the
prospective root-plus-child manifest set, not just the changed modality.

The effective collection budget is the minimum of the primary persisted budget
and the runtime override. Child persisted budgets are schema-derived from the
primary and must match it.

## Runtime layout

Add one `CollectionReadRuntime`, created after preflight and shared by the root
and all children. It owns:

- a `CollectionMemoryGovernor`;
- decoded segment, global-cell graph, tombstone, BM25 page, lexical run,
  lexical term-page, late-interaction batch, and WAL-run caches;
- ordinary and late-interaction Arrow sidecar-index caches;
- corresponding single-flight maps;
- collection-wide search-count, cell-decode-count, global-PQ rerank, and
  weighted decode gates.

Modality-version caches remain per modality because their keys are not globally
scoped: resident routing summaries, coarse and persisted quantizers, resident
global PQ descriptors, and resident lexical roots.

The governor has two non-overlapping pools:

- **retained pool:** 50% of bytes remaining after resident manifests;
- **transient pool:** the other 50%.

An odd byte goes to the transient pool. Explicit per-cache options remain
category ceilings, but they no longer imply additive reservations: every entry
also owns bytes from the shared retained pool. This avoids both multiplication
and a permanently fragmented proportional partition.

The existing cache implementations receive an optional retained-pool handle.
Their entries own an `Arc`-backed byte reservation. On pressure they evict their
own least-recently-used unpinned entries and retry; if another cache owns the
remaining pool, the new item is simply not retained. A later cross-cache global
LRU can improve hit rate without changing the safety contract.

## Query reservations

The existing lexical and WAL byte gates are replaced by the shared transient
pool. Dense segment, projected-vector, graph, and late-interaction decode paths
reserve their persisted or conservatively computed decoded-byte estimate before
allocating and keep the owned permit until the decoded query object is dropped.
An oversized object consumes the entire transient pool and therefore runs
alone.

The count-based search and cell-decode gates are also shared. Hybrid search does
not acquire an outer search permit; each parallel leg acquires exactly one
permit after named-vector dispatch, so sharing cannot recursively acquire the
same permit.

## Reporting

Root `IndexStats.resident_bytes_estimate` and hybrid
`SearchReport.resident_bytes_estimate` report the checked collection-wide
manifest sum. Add explicit runtime counters for retained bytes, transient bytes,
and their capacities so RSS evidence can be reconciled with the configured
envelope. Child-internal reports may retain modality-local details, but public
collection entry points overwrite the resident estimate with the collection
sum.

## Tests

The acceptance matrix includes:

- two named modalities whose individual manifests fit but aggregate manifests
  exceed the budget: open fails;
- root and child handles use pointer-identical caches and gates;
- mixed cache types cannot exceed the retained pool;
- mixed dense, sparse/text, WAL, and late-interaction decode reservations cannot
  exceed the transient pool;
- a hybrid query with a one-permit search gate completes without deadlock;
- concurrent multimodal load stays inside governor counters and preserves exact
  result equality against an unbounded reference;
- zero-byte cache ceilings retain nothing; and
- the full Rust workspace/all-targets, bindings, CLI binary build, and
  methodology validators remain green.

RSS has allocator, thread-stack, object-store, and runtime overhead not owned by
the index. The AWS evidence therefore reports both governor peaks and process
RSS; it does not claim that RSS equals `ram_budget_bytes`.

## Rollout and benchmark gate

This is a pre-release breaking change. Compatibility with unpublished manifests
or cache behavior is not a constraint.

No memory-efficiency or throughput claim is promoted from local tests. After
the correctness gates pass, run the multimodal hybrid load on the production
AWS shape and publish the immutable configuration, governor peaks, RSS,
latency/QPS, recall, and request counts.
