# Dense, sparse, and text retrieval

This study evaluates one index through every non-empty combination of three
retrieval signals:

| Mode | Dense embedding | Sparse vector | BM25 text |
|---|---:|---:|---:|
| `dense` | yes | no | no |
| `sparse` | no | yes | no |
| `text` | no | no | yes |
| `dense+sparse` | yes | yes | no |
| `dense+text` | yes | no | yes |
| `sparse+text` | no | yes | yes |
| `dense+sparse+text` | yes | yes | yes |

These are retrieval-signal combinations, not the historical segment leaf mode
also named `hybrid`. Each requested signal produces a ranked candidate list;
BORSUK fuses those lists with reciprocal-rank fusion by default. Weighted
fusion remains an explicit ablation and is never silently selected per dataset.

## Shared relevance contract

Every mode is scored against the same query IDs and qrels. This is essential:
separate dense, lexical, or synthetic ground truths would make cross-mode
effectiveness incomparable.

The primary real-data metric is nDCG@10 because BEIR qrels may be graded.
Recall@10, Precision@10, and MRR@10 are retained beside it. Precision@10 is
retained to expose distractors even when recall is high. Each raw query row
contains the effectiveness metrics, latency, logical bytes, physical disk-cache
and backing bytes/reads, request counts, candidate depth, selected-cell budget,
fusion method, and observed cache tier.

## Real datasets

Here, “dataset” means an evaluation fixture, not a production selector.
BORSUK never branches on `scifact`, `fiqa`, `glove`, or any other corpus name.
Real and synthetic corpora span properties that the automatic policy must
handle, and held-out corpora test whether that policy generalizes to user data
it has never seen.

The publication preparation pipeline uses public BEIR releases:

| Dataset | Documents | Test queries | Role |
|---|---:|---:|---|
| SciFact | 5,183 | 300 | scientific claims and evidence |
| NFCorpus | 3,633 | 323 | biomedical consumer questions |
| FiQA-2018 | 57,638 | 648 | financial question answering |
| SCIDOCS | 25,657 | 1,000 | scientific document relatedness |
| Quora | 522,931 | 10,000 | duplicate-question retrieval |

The source archive checksum, split, corpus/query counts, qrel count, encoder
revision, query prefix, package versions, dense width, sparse vocabulary size,
and every emitted-file checksum are recorded in each prepared manifest.
Publication preparation rejects the deterministic hash embedder reserved for
unit tests.

Dense vectors use `BAAI/bge-small-en-v1.5` at pinned revision
`5c38ec7c405ec4b44b94cc5a9bb96e735b38267a`, including its retrieval query
prefix. Sparse vectors use corpus-fitted TF-IDF with a recorded vocabulary.
The text leg receives the original title/body text and uses BORSUK BM25. See
the [BEIR repository](https://github.com/beir-cellar/beir), the
[BEIR paper](https://arxiv.org/abs/2104.08663), and the
[BGE model card](https://huggingface.co/BAAI/bge-small-en-v1.5).

## Synthetic controls

Real corpora do not reveal why fusion wins or loses. The synthetic suite uses
query-specific oracle documents while changing signal agreement:

- `aligned`: dense, sparse, and text all identify the same oracle document;
- `complementary`: each signal owns one different relevant document, so the
  information available to one, two, and three correct legs is `1/3`, `2/3`,
  and `1`;
- `dense-sparse-complementary`: dense and sparse each retrieve a different
  relevant document while text is a distractor;
- `dense-text-complementary`: dense and BM25 text each retrieve a different
  relevant document while sparse is a distractor;
- `sparse-text-complementary`: sparse and BM25 text each retrieve a different
  relevant document while dense is a distractor;
- `dense-conflict`: dense points toward a distractor while sparse/text agree;
- `sparse-conflict`: sparse points toward a distractor;
- `text-conflict`: text points toward a distractor.

Each pairwise control therefore gives either correct single leg a Recall@10
ceiling of `1/2`, while the intended pair has enough information to reach `1`.
Actual fused recall is measured rather than assumed because distractor overlap
and the fusion rank constant can keep the system below that ceiling. The
generator seed and complete manifest make every corpus reproducible. These
controls test whether a fusion method genuinely combines independent evidence
or only benefits from duplicate rankings.

Every fused-mode matrix sweeps RRF rank constants `1`, `5`, `10`, `30`, and
`60`. Single-signal modes run once because their ranking is independent of the
fusion constant. A rank constant is eligible for the default only if it reaches
the best measured recall/nDCG envelope across the real and synthetic suite;
latency breaks quality ties, but cannot compensate for a material quality loss.
Exact/exhaustive search or reranking remains the correctness fallback when an
approximate dense profile misses that gate.

The current recall-qualified default is `k=1`. On the three-way complementary
AWS control, `k=1` reached Recall@10 `1.000` and nDCG@10 `0.987`; the historical
`k=60` collapsed to Recall@10 `0.006`. On SciFact, `k=1` or `k=10` also
dominated `k=60` for triple-fusion recall. The larger constants remain in the
ablation matrix, not in the production default.

## Cache and timing contract

`startup` opens the index and prepares serving metadata before any query timer
starts. A content query is classified from measured physical bytes:

- `backing_only`: all measured payload bytes came from object storage;
- `disk_only`: all came from the local read-through disk cache and backing
  reads/bytes are zero;
- `mixed`: both tiers served bytes;
- `resident_only`: the query performed no physical payload read.

The experiment also records the requested hot-query fraction and the observed
disk/backing byte fractions. It does not call an entire process “cached” merely
because some metadata was fetched during open. The first request can
legitimately be `mixed`: serving metadata may already be local while modality
sidecars are still fetched from S3.

Every selected point uses repeated raw queries. Charts show the arithmetic mean
with sample-standard-deviation whiskers and retain p50, p95, p99, and maximum
latency. A single p95 without raw repetitions is not publication evidence.

## Resource and production contract

Build and query phases have separate process-resource timelines:

- CPU percentage;
- RSS and virtual memory;
- process disk read/write bytes;
- local cache footprint; and
- scratch-space footprint.

The production profile keeps the normal four-query admission cap, the shared
24-cell decode cap, and a 512 MiB resident budget. Uncapped query/user scaling
is a separate research-ceiling experiment. Dataset, mode, query count, and cell
width must never multiply into unbounded decode memory.

The production auto policy uses properties available for every user index:

- dense dimensionality, padded code width, row count, metric, and stored bytes;
- sparse row count, non-zero/posting count, vocabulary size, and measured
  decoded Parquet block bytes;
- text document count, total document length, posting/vocabulary shape, and
  measured decoded BM25 Parquet block bytes;
- effective RAM budget, admitted query count, active modality count, storage
  tier, and observed cache coverage.

At ingest, each segment records the maximum decoded byte size of its bounded
lexical Parquet blocks. The default reserves half of the effective RAM envelope for
routing, dense search, result assembly, allocator slack, and the host
application. The other half is a shared FIFO weighted byte semaphore for all
sparse/text decodes. A per-query wave receives
`lexical byte capacity ÷ admitted searches ÷ maximum active lexical legs`, with
additional transient decode headroom. This is a real global cap, not merely a
per-query estimate, so users and modalities cannot multiply past it. A missing
measurement is treated as unknown and admitted alone, never as zero bytes.
BM25 and named sparse search both use exact, byte-bounded waves. Safe block-max
bounds can omit later storage reads once they cannot enter the top-k. Explicit limits
remain overrides, but dataset-name presets are forbidden.

The cache-mixture benchmark records both the intended primed fraction and each
query's observed cached-byte fraction. Immutable cells can overlap between
queries and a read-through cache grows during a run, so benchmark/query names
are not accepted as evidence that a request was fully cached. Concurrency
points record admission waiting inside per-request latency.

Policy fitting and publication evaluation are separated: thresholds may be
selected on the designated development matrix, then must pass unchanged on
held-out corpora, dimensions, scales, sparsity/term-distribution regimes, cache
mixtures, and user concurrency. We publish the property ranges and selected
formula, not a table saying which benchmark gets which settings.

Results enter publication tables only after a fresh source recreation under a
new S3 prefix. Any library, benchmark, artifact-schema, cache-policy, codec, or
resource-sampler change invalidates the complete matrix.
