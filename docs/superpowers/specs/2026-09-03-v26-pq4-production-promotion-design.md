# V26 PQ4 Production Promotion Design

## Decision

Promote the holdout-qualified V26 PQ4 fast scan into the public `borsuk` crate
as a new `Pq4Index` API. This is a clean pre-release format and execution path,
not another mode in the legacy `BorsukIndex` manifest. There is no version
dispatch, migration reader, alias, scientific-result loader, dynamic-loader
workaround, or transparent network access in the query path.

The production snapshot is an explicit local directory of immutable Arrow IPC
and Parquet files. Object storage may distribute that directory, but a separate
deployment step materializes and authenticates it before `Pq4Index::open`.
Search performs no S3 calls and therefore has a measurable local latency
contract.

## Qualified evidence and remaining release boundary

The frozen 480-query holdout selected ten pages at 996,458 ppm aggregate
recall, 800,000 ppm minimum-query recall, and 996,458 ppm oracle attainment.
The candidate scan and candidate-vector exact rerank measured 13,863,178 ns
p99 with a 2,336,975,744-byte 100-million-row memory projection. This proves
the router, but it does not yet prove public API behavior or the latency of
opening and scanning the selected page bodies. Production promotion therefore
keeps the quality gates and adds one end-to-end p99 gate before release.

## Public API

The public surface is intentionally narrow:

```rust
pub struct Pq4BuildConfig {
    pub dimensions: usize,
    pub page_rows: usize,
    pub build_threads: usize,
}

pub struct Pq4OpenOptions {
    pub query_threads: usize,
    pub candidate_depth: usize,
    pub page_budget: usize,
    pub cache_bytes: usize,
}

pub struct Pq4Builder;

impl Pq4Builder {
    pub fn build_parquet(
        input: &std::path::Path,
        output: &std::path::Path,
        config: Pq4BuildConfig,
    ) -> borsuk::Result<Pq4BuildReport>;
}

pub struct Pq4Index;

impl Pq4Index {
    pub fn open(
        directory: &std::path::Path,
        options: Pq4OpenOptions,
    ) -> borsuk::Result<Self>;

    pub fn search(
        &self,
        query: &[f32],
        k: usize,
    ) -> borsuk::Result<Vec<Pq4Match>>;
}
```

The input Parquet schema is concrete and non-nullable: `id: binary` and
`vector: fixed_size_list<float32, dimensions>`. The output match contains the
opaque ID, squared distance, and source ordinal. Metadata filtering and mutable
updates are outside this promotion; a rebuild produces a new immutable
snapshot.

## Snapshot format

`manifest.json` is canonical newline JSON and binds every file by role,
SHA-256, encoded length, row count, schema, generation, and source identity.
Opening rejects missing, extra, renamed, or mutated files before semantic use.

- `codebook.arrow`: one non-nullable fixed-size centroid tensor for exactly 32
  three-dimensional subquantizers and 16 centroids per subquantizer.
- `codes.arrow`: source-order 32-row transposed blocks, 512 bytes per block,
  low nibble for the even row and high nibble for the odd row.
- `vectors.arrow`: source-order non-nullable `f32[dimensions]` vectors for
  bounded exact rerank through memory mapping.
- `row-map.arrow`: source ordinal to non-nullable primary and replica page
  ordinals. This is memory-mapped and touched only for bounded candidates.
- `pages.parquet`: page-ordered records with page ordinal, source ordinal, ID,
  and exact vector. Row groups are page boundaries, so ten selected pages are
  ten bounded local reads rather than corpus-wide decoding.

Arrow IPC carries dense fixed-layout arrays shared by Rust, Python, and other
Arrow implementations. Parquet carries durable page records and later
cross-language analytics. No Rust-only binary structure is an authority.

## Build path

The builder is query-independent and reads the input twice. Pass one validates
the complete schema and rows while retaining the deterministic stratified
8,192-row training sample. Thirty-two subquantizers train concurrently with
Rayon for four Lloyd iterations. Pass two streams bounded record batches,
normalizes vectors, encodes blocks in parallel, and writes the source-order
vector and row-map files plus page-ordered Parquet.

Page assignment is deterministic and query-independent. Each row has one
primary and one replica page. The builder validates balanced row caps and exact
roster equality before atomically renaming a completed temporary snapshot.
Incomplete output directories are never opened.

## Query path

`Pq4Index::open` authenticates every file, validates the complete Arrow/Parquet
schemas, memory-maps codes, vectors, and row mappings, and freezes a Rayon
query pool. The only unsafe architecture code remains behind a safe function in
`borsuk-fma`; the public crate continues to forbid unsafe code.

Each query:

1. validates and normalizes the 96-dimensional vector;
2. prepares 32 sixteen-entry `u8` distance tables;
3. scans all code blocks in fixed chunks across the frozen query pool;
4. merges fixed 8,192-bin histograms and retains the best 2,048 rows by
   `(score, source_ordinal)` without a corpus-sized pair allocation;
5. gathers those exact vectors and page assignments from local memory maps;
6. reranks by `(squared_l2, source_ordinal)` and applies the qualified
   deterministic ten-page reducer;
7. reads the ten Parquet row groups, computes exact distances, deduplicates
   replicated source ordinals, and returns the best `k` matches.

Concurrent calls use a bounded scratch pool. Every admitted search owns its
score buffer, histograms, candidates, and page buffers; if the configured RAM
budget cannot admit another search, it waits or fails explicitly rather than
oversubscribing memory. There is no shared mutable score buffer.

## Memory and latency contracts

The 100-million-row single-query projection remains exactly 2,336,975,744
bytes: 1.6 GB codes, 200 MB score buffer, 512 MiB bounded vector/page cache,
6,144-byte codebook, histogram, candidates, and fixed scratch. Memory-mapped
vector and row-map files are charged through the cache and measured resident
working set, not their virtual length. Each additional concurrently admitted
query charges one 200 MB score buffer plus bounded scratch; admission must keep
the configured process projection below 3 GiB.

The existing 15 ms p99 qualifies routing plus candidate exact rerank. The
release gate measures the whole public `search` call with ten page row groups.
The target stays 15 ms p99; if local page decoding makes that impossible, the
implementation must reduce page-row decode cost or adopt an Arrow page file
before changing the bound. Quality gates remain 975,000 aggregate, 800,000
minimum-query, and 995,000 oracle-attainment ppm.

## Fail-fast verification

Development uses a layered gate and never repeats the full workspace suite per
fix:

1. one exact test node for the changed contract, normally under one second;
2. `cargo test -p borsuk --lib v26_release_contract_ -- --nocapture`, covering
   representation, reducer, memory, and public search boundaries in tens of
   seconds;
3. `python3 scripts/check_v26_fast.py --affected` once per stable slice;
4. strict workspace Clippy once after affected tests are green;
5. one locked workspace/all-targets test only for the final candidate.

The release-contract fixtures are reduced but structurally faithful. They must
exercise multiple blocks, a partial final block, duplicated primary/replica
pages, deterministic ties, concurrent searches, and ten physical page row
groups. They detect wrong algorithms quickly; only the final large gate detects
repository-wide integration drift.

## End-to-end validation and release

After local assurance, build one immutable 9.99-million-row production snapshot
on `causality` Spot in any available `eu-central-1` zone. Run development
queries first, then one sealed 480-query holdout only after code and parameters
freeze. Record build throughput, search p50/p95/p99/max, exact recall, page
count, I/O bytes, peak RSS, PSI, and swap. Use the authenticated Arrow/Parquet
snapshot and terminate the instance immediately.

The public PQ4 path is release-eligible only if it preserves the holdout quality
gates, uses exactly ten pages, performs no network calls in `search`, stays
below 3 GiB projected and observed RSS, and meets 15 ms end-to-end p99. Failure
returns to the narrow gate; it does not trigger a repeated full repository or
paid run. Competitor and D3 claims remain fenced until this boundary passes.
