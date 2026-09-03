# V26 PQ4 100M Fan-Out Design

## Decision

Qualify PQ4 at 100,000,000 rows as ten independently authenticated
10,000,000-row shards. Generate and build the shards concurrently on AWS EC2
Spot, materialize the finished Arrow snapshots onto one sufficiently large
same-region serving host, search all shards concurrently, and merge exact local
top-k results deterministically. This is a scale qualification, not a relabeling
of the 9,990,000-row Deep-Image quality result.

The frozen 9.99M Deep-Image result remains the standard-dataset quality and
latency authority: 997,708-ppm aggregate Recall@10, 1,000,000-ppm floor
compliance at the 800,000-ppm per-query floor, and 11,465,765-ns p99. The 100M
campaign uses a new query-independent `synthetic-clustered-100m-96` corpus and
must be labeled synthetic everywhere. It cannot support a Deep-Image or paired
competitor claim.

## Why this boundary

PQ4 is deliberately fixed at 96 dimensions: 32 three-dimensional
subquantizers and a 16-byte code per row. The repository's already staged 100M
datasets are 768-dimensional and cannot be passed through this format without
creating a different codec. The existing deterministic synthetic generator is
dimension-parametric and can generate a fresh 100M-by-96 corpus without query
feedback. Duplicating the 9.99M Deep corpus ten times is forbidden.

A monolithic 100M scan is also forbidden as a latency claim. It would serialize
the work and obscure the scaling boundary. Ten shards preserve the qualified
10M scan size, allow parallel training, and make the only new serving costs
fan-out, contention, and bounded merge.

## Corpus authority and partitioning

The corpus recipe is fixed before execution:

- dataset ID `synthetic-clustered-100m-96`;
- generator `synthetic-clustered-v1`;
- total rows `100000000`, dimensions `96`, group size `100`, queries `100`;
- one literal seed recorded in the campaign authority;
- ten half-open global ordinal intervals `[0,10000000)`, ...,
  `[90000000,100000000)`;
- Parquet physical schema
  `emb:fixed-size-list<element:f32;96>:non-null`;
- global opaque IDs are the eight-byte little-endian source ordinal.

Range generation must be byte-deterministic and use the global total when
constructing centroids and members. A range cannot behave as an independent
10M corpus. Each worker emits bounded Parquet files, a sorted compact newline
manifest, SHA-256 and byte length for every file, and its global ordinal bounds.
Query and truth Parquet are generated once from the same recipe; build workers
have no access to query or truth.

The final corpus authority binds all ten partition manifests, their ordered
non-overlapping intervals, their exact union of 100,000,000 ordinals, the
generator binary/source identities, and the query/truth identities. Any gap,
overlap, reorder, digest drift, schema drift, or cross-partition ID collision
fails before a build starts.

## Parallel construction

The controller launches ten Spot workers, distributed across available
`eu-central-1` availability zones. Every worker receives only its immutable
partition authority and frozen binaries. It generates its range, stages the
cross-language Parquet into the public `id`/`vector` PQ4 schema, builds one
snapshot with `Pq4Builder`, validates all five Arrow/JSON roles, uploads the
snapshot and canonical receipt, deletes scratch, and terminates.

Workers do not share mutable state and may complete in any order. A Spot
interruption produces a canonical failed/interrupted receipt and discards that
attempt. A replacement uses a fresh attempt prefix; no partial output is ever
promoted. The coordinator promotes only ten terminally successful receipts
whose shard ordinals and source intervals form the exact registered set.

## Local fan-out API

`Pq4ShardedIndex` is an immutable local-only serving object:

```rust
pub struct Pq4ShardedOpenOptions {
    pub memory_budget_bytes: u64,
    pub fanout_threads: usize,
    pub shard_query_threads: usize,
    pub admission_timeout_ms: u64,
}

impl Pq4ShardedIndex {
    pub fn open(
        shards: &[(u32, std::path::PathBuf)],
        options: Pq4ShardedOpenOptions,
    ) -> borsuk::Result<Self>;

    pub fn search(
        &self,
        query: &[f32; 96],
        k: usize,
    ) -> borsuk::Result<Vec<Pq4Match>>;
}
```

`open` requires a nonempty set of at most 256 unique contiguous ordinals
starting at zero, authenticates every shard, and admits the sum of all code
planes plus one concurrent scratch allocation per shard under the single
process budget. The 100M campaign authority separately requires exactly ten.
`search` executes one local search per shard concurrently, collects exact local
top-k, and calls the existing bounded merge ordered by
`(squared_distance, shard_ordinal, source_ordinal)`. A failed shard fails the
whole query. There is no partial result, network client, dynamic loader, page
API, compatibility path, or arrival-order dependence.

The library accepts paths and typed values only. Parquet is the corpus and
evidence interchange; Arrow IPC is the snapshot format. Object-store download
and process supervision stay outside the library.

## Memory and work bounds

For 100M rows, ten code planes contain exactly 1,600,000,000 bytes. Ten
simultaneous score buffers contain 200,000,000 bytes. Histograms, 3,072-row
candidate sets, codebooks, manifest state, Arrow metadata, IDs, and fixed
runtime reserve must keep the exact checked projection at or below
2,336,975,744 bytes and observed peak RSS below 3 GiB. Opening fails before
allocation when the registered budget cannot admit all ten shard searches.

Each query scans exactly 100M PQ codes in ten parallel 10M jobs and exact-reranks
at most 30,720 vectors before the final ten-way merge. The merge holds at most
`10 * k` matches. The sealed receipt records per-shard elapsed time, coordinator
overhead, total latency, rows scanned, candidates reranked, and observed RSS.

## Release gates and claim boundary

The fixed 100M gates are:

- exactly 100,000,000 unique source ordinals and ten authenticated shards;
- aggregate Recall@10 at least 995,000 ppm on the 100 analytic queries;
- every query Recall@10 at least 800,000 ppm;
- complete `Pq4ShardedIndex::search` p99 at most 15,000,000 ns;
- maximum latency recorded but not used as a tuning gate;
- projected bytes at most 2,336,975,744 and observed RSS below 3 GiB;
- zero swap growth and memory PSI full avg10 at most 1%;
- exactly ten shard searches and 100,000,000 rows scanned per query;
- no page-body, object-store, or network call during timed search.

Passing proves synthetic 100M scale for the local fan-out implementation. It
does not prove 100M Deep-Image quality or distributed RPC latency. Standard
quality remains rooted in the sealed 9.99M Deep-Image result. A paired
competitor claim requires a separate preregistered run over identical data,
truth, hardware class, and query cohort.

## Fail-fast verification ladder

Implementation never pays the full-suite cost after each change:

1. one exact authority, partition, fan-out, or monitoring test;
2. the complete `borsuk-pq4` crate only after a component is stable;
3. `python3 scripts/check_v26_fast.py --affected` once per coherent slice;
4. strict workspace Clippy once for the release candidate;
5. one locked workspace/all-targets test after all affected gates are green;
6. a 100K-row three-shard separate-process preflight before any 100M launch;
7. one sealed 100M campaign.

Every filtered Cargo command must execute at least one test. The controller
stops at the first failed phase and cannot silently advance.

## Active monitoring and automatic stops

The controller remains active until every worker and the final serving run are
terminal. It polls AWS and immutable receipt prefixes every 30 seconds and
prints a bounded status line containing phase, attempt, instance state/checks,
completed rows or queries, elapsed time, RSS, memory PSI full avg10, swap delta,
and estimated spend. It emits an operator update on phase changes, any failure,
and at least every 15 minutes during otherwise healthy long work.

The current process is preserved; the monitor never starts a duplicate because
output is quiet. It stops and records failure on an impaired EC2 check, RSS cap,
three consecutive PSI samples above 1%, any swap growth, no build progress for
five minutes, no query progress for one minute, a 7,200-second worker wall cap,
or a 3,600-second serving wall cap. Spot interruption is the only automatic
fresh-attempt case. Scientific failures require diagnosis and a new committed
fix before another paid attempt.

All S3 prefixes are fresh and role-disjoint. Terminal artifacts and bounded
logs are uploaded before termination. Instances terminate immediately after
terminal classification, and the controller verifies that no campaign
instance remains running.

## Delivery sequence

First add deterministic range/authority contracts and the sharded local API.
Then add the monitored controller and prove it against fake AWS plus a reduced
separate-process corpus. Run one final repository assurance gate, freeze source
and binary identities, and only then launch the Spot campaign. Record the final
result and explicit claim boundary in the research ledger before any package or
competitor release claim.
