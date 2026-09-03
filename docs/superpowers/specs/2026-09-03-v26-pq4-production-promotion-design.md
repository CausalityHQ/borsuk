# V26 PQ4 Direct-Row Production Promotion Design

## Decision

Promote the V26 PQ4 fast scan into the public `borsuk` crate as an immutable
local `Pq4Index` shard that returns exact-reranked rows directly. Delete the
page-selection and page-decoding stage from this new API. This is a clean
pre-release format: there are no compatibility readers, aliases, version
dispatch, dynamic loaders, `ldd` workarounds, storage clients, or hidden
network calls.

Arrow IPC is the local snapshot format for dense, cross-language arrays.
Parquet is the corpus-input and evaluation-evidence interchange. Object
storage may distribute a snapshot, but deployment must materialize and
authenticate it before `Pq4Index::open`.

## Evidence and release boundary

At depth 2,048, the 480-query diagnostic achieved 996,458 ppm page-containment
recall and 13,231,400 ns p99, but direct exact-row return achieved only 993,541
ppm aggregate and one 700,000-ppm query. At fixed depth 3,072, direct-row recall
rose to 997,291 ppm aggregate while p99 remained 14,863,808 ns; 479 of 480
queries achieved at least 800,000 ppm. The 3,072 result is rooted at source
`e538eb51ffb67cb9a7d5fd07f279f2d92cf16cc9` with canonical result SHA-256
`324eed01edb363223056c49505bdf365f8071c972683b935f3c0dac33b07624c`.

The old page path is rejected. Ten pages at the qualified 2,816-row page cap
would decode about 10.8 MB of vectors per query after the already measured
scan and rerank, which cannot fit the remaining latency budget.

Queries 0..511 are now burned development evidence because depth 3,072 was
selected after observing them. Release requires exact truth for unused query
ordinals and one sealed cohort. The release gates are:

- aggregate direct-row Recall@10 at least 995,000 ppm;
- at least 997,500 ppm of queries individually achieve Recall@10 of 800,000
  ppm or higher;
- complete local `search` p99 at most 15,000,000 ns;
- observed and projected process RSS below 3 GiB, zero swap growth, and memory
  PSI full avg10 at most 1%;
- no network or page-body reads.

Absolute minimum recall and maximum latency remain evidence. They are not
single-observation release gates, which avoids tuning an architecture to one
observed query while retaining explicit tail visibility.

## Public API

```rust
pub struct Pq4BuildConfig {
    pub dimensions: usize,
    pub build_threads: usize,
}

pub struct Pq4OpenOptions {
    pub query_threads: usize,
    pub candidate_depth: usize,
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

The input Parquet schema is exactly two non-nullable fields: `id: binary` and
`vector: fixed_size_list<element: float32 not null>[dimensions]`. A match
contains the opaque ID, squared distance, and source ordinal. Mutable updates,
metadata filtering, and remote search are outside this promotion.

## Snapshot format

`manifest.json` is sorted compact newline JSON and binds every file by role,
SHA-256, encoded length, row count, complete schema, generation, and source
identity. Opening rejects missing, extra, renamed, or mutated files before
semantic use.

- `codebook.arrow`: exactly 32 three-dimensional subquantizers and 16
  centroids per subquantizer.
- `codes.arrow`: source-order 32-row transposed blocks, 512 bytes per block,
  with the even row in the low nibble and odd row in the high nibble.
- `vectors.arrow`: source-order non-nullable `f32[dimensions]` exact vectors.
- `ids.arrow`: source-order non-nullable opaque binary IDs.

Codes are loaded into owned memory at open. Exact vectors and IDs use bounded
positional reads through safe standard file APIs. This avoids a custom loader
or unsafe mmap surface in the public crate.

## Parallel build

The builder reads the input twice. Pass one validates every row and retains the
deterministic stratified 8,192-row sample. Thirty-two subquantizers train in
parallel with Rayon for four Lloyd iterations. Pass two streams bounded record
batches, normalizes vectors, encodes blocks in parallel, and writes codes,
vectors, and IDs in source order. The writer validates row-count and ordinal
equality, fsyncs the files, writes the manifest last, and atomically renames the
temporary directory. A failed build leaves no openable snapshot.

## Local query path

`Pq4Index::open` authenticates every byte and complete Arrow schema, owns the
compact code plane, opens vectors and IDs, and freezes a Rayon query pool. The
public crate retains `#![forbid(unsafe_code)]`; AArch64 NEON and x86_64 SIMD are
exposed only through safe `borsuk-fma` functions.

Each query:

1. validates and normalizes the 96-dimensional vector;
2. prepares 32 sixteen-entry quantized distance tables;
3. scans all transposed code blocks in fixed parallel chunks;
4. merges fixed 8,192-bin histograms and keeps the best 3,072 rows by
   `(score, source_ordinal)` without allocating a corpus-sized pair array;
5. reads only those exact vectors and reranks by
   `(squared_l2, source_ordinal)`;
6. reads IDs for the final `k` rows and returns them.

Concurrent searches use bounded scratch admission. Each admitted search owns
its score buffer, histograms, and candidate vectors. Admission waits or fails
explicitly rather than oversubscribing memory.

## Scaling and resource contract

The conservative 100-million-row single-query projection is exactly
2,336,975,744 bytes: 1.6 GB owned codes, a 200 MB score buffer, a 512 MiB
bounded vector/ID cache, the 6,144-byte codebook, histograms, candidates, and
fixed scratch. The exact calculation is locked by a release-contract test.

A single 100-million-row linear scan is not claimed to meet 15 ms. Production
scales horizontally with independently authenticated roughly 10-million-row
shards. Shards train and encode concurrently, search concurrently, return exact
local top-k rows, and a coordinator merges the bounded results by
`(distance, shard_ordinal, source_ordinal)`. This preserves deterministic global
top-k if every shard returns its exact local top-k. The public local-shard gate
comes first; a later distributed gate must measure coordinator and network p99
before any 100-million-row latency claim.

## Fail-fast assurance

Development uses the narrowest gate that can falsify the current change:

1. one exact contract node, normally below one second;
2. `cargo test -p borsuk --lib v26_release_contract_ -- --nocapture` at a
   stable component boundary;
3. `python3 scripts/check_v26_fast.py --affected` once per stable slice;
4. strict workspace Clippy once after affected gates are green;
5. one locked workspace/all-targets test only for the final candidate.

Reduced fixtures cover partial blocks, score ties, 3,072-row bounded ranking,
positional vector/ID reads, concurrency admission, shard merging, and
independent recomputation of aggregate and tail-compliance gates. Full-scale
science never substitutes for these fast contracts.

## Final validation

After local assurance, build one authenticated 9.99-million-row production
shard on `causality` Spot. Run burned development queries first. Generate exact
truth for unused query ordinals without opening their results, freeze source,
binary, snapshot, depth, and gates, and then run one sealed 480-query holdout.
Record build throughput, p50/p95/p99/max, aggregate recall, tail compliance,
minimum recall, I/O bytes, peak RSS, PSI, and swap; terminate immediately.

The shard API is release-eligible only if every gate above passes. Distributed
100-million-row and competitor claims remain fenced until a separate parallel
fan-out result measures complete end-to-end latency. D3 remains fenced.
