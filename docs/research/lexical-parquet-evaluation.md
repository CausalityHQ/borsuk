# Lexical Parquet Evaluation

This document defines the evaluation contract for BORSUK BM25 and named-sparse
retrieval. It is a protocol, not a result table: publication numbers are added
only after the current code recreates every index from source.

## Engine under test

Both modalities use one physical hierarchy:

```text
resident field root
  bounded term-range Parquet page
    selected posting Parquet row group
    selected row-metadata Parquet row group
```

The field root is loaded during `open`, outside measured request latency. Query
traffic never reads a complete postings or row-metadata file merely to find a
block. It reads the footer plus projected column chunks for selected row groups.
The local cache stores those immutable byte ranges. Concurrent callers may
single-flight and share an active decoded block, but decoded postings are not
retained as a corpus-sized memory cache.

The persistent lexical layout has no private binary codec. Roots, term pages,
segment build shards, postings, and row metadata are standard Arrow-schema
Parquet tables.

## Correctness contract

Default sparse and BM25 search is exact for the indexed representation.

- Named sparse scoring is the exact query/document dot product.
- BM25 uses global live-corpus document count, average document length, and
  global document frequency.
- Record id plus generation applies the same MVCC visibility as dense search.
- While an update/delete overlay exists, BM25 derives corpus statistics from
  live generations; stale physical rows may not contribute to `N`, `avgdl`, or
  document frequency.
- Compaction and append-after-reopen must preserve every live term.
- Sparse upper bounds are sign-safe for positive and negative weights.
- BM25 upper bounds use maximum term frequency and minimum document length.
- A run is skipped only when its bound is strictly below the kth score; equality
  remains searchable to preserve deterministic id tie breaks.

Every configuration point must be checked against a brute-force reference
before latency or throughput is publishable.

## Layout properties and defaults

The current defaults are properties of data and resource budgets, not benchmark
dataset names:

| Property | Current policy | Purpose |
|---|---:|---|
| decoded posting + metadata block target | 1 MiB | bound one run's transient working set |
| term-page target | 1 MiB estimated decoded | bound term routing reads and decode |
| term-page hard entry cap | 4,096 | protect against unusually short entries |
| query prefetch width | 16 | overlap object latency without unbounded fan-out |
| admitted searches | 4 by default | bound process-wide concurrent request work |
| lexical RAM share | half the effective RAM budget | leave room for dense/routing/application work |

The weighted byte gate is global across requests and lexical modalities. A
missing or oversized estimate is admitted alone, never treated as zero. The
evaluation must report actual decoded block distributions; a mean alone cannot
justify these defaults.

## Traffic states

“Cached” and “uncached” describe observed bytes, not a query label:

- **uncached**: the index handle is already open and serving metadata is loaded,
  but selected immutable data ranges are absent from the local disk cache;
- **cached**: selected immutable ranges are served from the disk cache with no
  backing-store request;
- **mixed**: each request reports its observed cached-byte fraction because
  query overlap and read-through filling change coverage during the run.

Cold library initialization is measured separately as `open`/prepare latency.
It must not be folded into uncached query latency.

Mixed-cache points cover 0%, 10%, 25%, 50%, 75%, 90%, and 100% intended
coverage, with observed disk-cache and backing-store bytes published beside
latency. Query sets must include popular-term overlap, disjoint terms, and
queries that cross cached and uncached term pages.

## Dataset matrix

Real corpora and synthetic controls are both required:

- text-only BM25;
- sparse-only learned/synthetic vectors;
- dense + text;
- dense + sparse;
- sparse + text;
- dense + sparse + text.

Synthetic controls vary document count, vocabulary size, Zipf exponent, average
document length, sparse non-zeros per row, posting skew, positive/negative
weights, id width, update/delete rate, and query term count. Held-out shapes are
required so the automatic policy cannot overfit named public benchmarks.

Real datasets must be identified by source, preprocessing, tokenizer, split,
row count, dimensionality/vocabulary, non-zero distribution, and query count.

## Curves and distributions

For each modality/configuration publish:

- exact recall and tie-aware recall;
- p50, p90, p95, p99, maximum, mean, and standard deviation of latency;
- throughput under 1, 2, 4, 8, and 16 callers;
- posting runs planned, read, and block-max skipped;
- term pages and Parquet row groups read;
- logical bytes, disk-cache bytes, backing bytes, GETs, and cache fraction;
- process CPU, RSS, virtual memory, disk read/write bytes, cache footprint, and
  build scratch footprint as time series;
- index build duration, peak resources, durable footprint, and object count.

Research-ceiling runs may remove admission caps but must be labelled uncapped.
Production-default runs retain the global search and byte gates.

## Required comparisons

The comparison matrix uses the same corpus, tokenizer/sparse values, queries,
top-k, concurrency, cache state, machine type, region, and object-store
placement. Compare against locally runnable Lucene/Tantivy-style BM25 and sparse
inverted indexes where semantics match, plus managed systems only through their
documented/public APIs. Managed cost rows separate storage, requests, and the
client compute required by every option.

No latency comparison is publishable if recall or lexical semantics differ.
Where another system cannot expose exact BM25/sparse results, publish the
recall–latency curve instead of presenting one unmatched point.

## Acceptance gates before AWS publication runs

1. All exactness, MVCC, reopen, compaction, corruption, and concurrency tests
   pass.
2. A generated high-skew corpus demonstrates bounded query RSS under the
   production cap.
3. A large-vocabulary build demonstrates bounded root-publication RAM and
   reports scratch/disk use.
4. Update-heavy BM25 uses a bounded persisted statistics-delta path; the current
   exact live-row fallback is not a publishable low-latency production point.
5. Query reports reconcile logical, cache, backing, and request counters.
6. Every AWS index is recreated under a fresh prefix from the current source.
7. Raw per-query samples and resource timelines are retained; charts are
   generated from those raw files, never copied from old documentation.
