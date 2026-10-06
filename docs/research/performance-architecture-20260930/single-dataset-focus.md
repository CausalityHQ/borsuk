# Single-dataset benchmark focus

Operator direction on 2026-10-06 replaces the prospective ReLAION/CoHere benchmark pair. Product requirements remain unchanged.

## Selected workload

Cohere Wikipedia, Cohere embed-multilingual-v3, 1024 dimensions, k=10. Turbopuffer's current published vector methodology uses this dataset: https://turbopuffer.com/v3 (checked 2026-10-06). Its published comparison uses 10 million documents, GCP us-central1, 8 queries per second for 10 minutes, separate hot and cache-disabled runs. Published ratios are not a matched BORSUK baseline.

Use one immutable corpus selection and disjoint query split for BORSUK and S3 Vectors. Published Turbopuffer results remain a disclosed external target. Freeze exact dataset revision, selected IDs, vector bytes, normalization, distance metric, query bytes, ground truth and ties before execution. Confirm the source embedding field before downloading a large body. D768 historical embeddings cannot stand in for these D1024 vectors.

## Ordered execution

1. Preserve the completed fitter qualification and the existing source-only Rust worker. Finish its correctness work without launching the superseded real two-dataset panel.
2. Adapt the native implementation to this workload, using a fixed small subset for the first correctness/recall falsifier. Keep the rest of the same corpus for later scale gates; add no other dataset.
3. Complete native correctness on the fixed small cohort, then freeze the 10M corpus and query workload. Smaller cohorts do not support competitor comparison claims.
4. Run BORSUK and S3 Vectors on the identical 10M corpus and queries. Require equal or better recall before claiming a latency/QPS-per-total-dollar win. Use the same comparison accounting and disclose cache-control differences.
5. Optimize only bottlenecks demonstrated on this dataset. The product comparison target is the published 10M workload below; smaller cohorts are correctness preflights only. Defer scale campaigns beyond10M.

No algorithm win, dataset readiness, service access, matched baseline or measured performance is claimed by this decision. Historical receipts and protocols remain immutable. No new ReLAION or paired multi-dataset campaign is authorized by the current benchmark direction.

## First cohort selection, declared before vector acquisition

For the correctness preflight, enumerate English `en/*.parquet` files in lexicographic path order at revision `ade45fb52bd549f5e8c065636fe4160a43c2af36`, preserving row order within each file. Corpus source ordinals are `[0,100000)`; query source ordinals are `[100000,101000)`. Read only enough whole authenticated shards to supply these rows. Missing rows, duplicate document IDs, null/nonfinite embeddings, zero norms, or a dimension other than 1024 invalidate preparation; do not silently replace rows or switch datasets. These are our cohort choices, not disclosed Turbopuffer document/query IDs.

Retain original embedding values as little-endian f32 and stable source locators/document IDs. Use cosine distance, k=10, no filters. Ground truth is exhaustive cosine distance over the corpus computed in f64 from those same f32 values, with stable corpus ordinal as the tie breaker. Seal source shard bytes/SHA256, corpus and query ID/byte hashes, ground-truth bytes, metric and tie rule before service ingestion or ANN evaluation. All services receive identical vector values and IDs. This selection declaration is not a completed data seal or a claim of corpus readiness.

First require native correctness and measured recall on all 1000 preflight queries; retain per-query results. Then freeze the separate 10M comparison protocol. Do not choose another split based on recall, inspect an incomplete performance campaign, or claim cold-cache equivalence where a service does not expose it. Scale later by extending the corpus from this same source while permanently excluding these query document IDs; ground truth must be recomputed for each corpus size.

## Source identity checked before acquisition

Turbopuffer's dataset link resolves to [CohereLabs/wikipedia-2023-11-embed-multilingual-v3](https://huggingface.co/datasets/CohereLabs/wikipedia-2023-11-embed-multilingual-v3). The embedding column is `emb`; the viewer reports 1024 entries. A bounded read of the publisher's dataset API on 2026-10-06 resolved revision `ade45fb52bd549f5e8c065636fe4160a43c2af36`. Use immutable revision URLs for subsequent schema and shard validation. No parquet body was downloaded in this check.

The public Turbopuffer methodology does not disclose selected document IDs or query bytes. Reproduce a matched BORSUK/S3 Vectors comparison with our frozen cohort; label comparisons with their published chart as dataset-family targets, not exact reproductions. The dataset revision alone is not a corpus, query, or ground-truth seal.

## Common region and service admission

Use AWS Frankfurt (`eu-central-1`), with Turbopuffer region `aws-eu-central-1`, and place the benchmark client there. Both [Turbopuffer's region list](https://turbopuffer.com/docs/regions) and [S3 Vectors' region list](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-regions-quotas.html) advertise this region (checked 2026-10-06). This differs from the published GCP experiment and must be disclosed.

A read-only SDK check using AWS profile `causality` in this region returned HTTP 200 for `ListVectorBuckets(maxResults=1)` with no buckets in that page. This establishes that operation's access only; index creation, ingestion and querying remain unverified. No resources were created. Turbopuffer credentials are not required for the current BORSUK/S3 Vectors comparison scope. Do not substitute its published charts for a matched service run.

## Native cohort preparation gate

Use one source-only Rust binary with existing Arrow/Parquet, SHA256 and SQ8/generation APIs; add no dependencies or ANN algorithm. Its local inputs are the two whole authenticated English Parquet shards above, supplied by a bounded remote downloader. Full shard bytes/SHA and immutable revision must be sealed before extraction; an ETag/footer is not a whole-body identity. Decode `emb` as finite nonzero D1024 f32 vectors, preserving source order and original values. Admit Arrow row-group decode from pinned metadata before reading bodies; reject oversized batches instead of silently lifting the cap. Retain canonical document identity/source locators and refuse missing/duplicate identities.

The fixed outputs are corpus LEf32 (409600000 bytes), queries LEf32 (4096000 bytes), exact top10 LEu64 truth (80000 bytes), corpus/query IDs and a marker-last identity receipt. Source ordinals `[0,100000)` and `[100000,101000)` are immutable. Compute exhaustive cosine in f64 over original f32 values, with ascending corpus ordinal ties. Ground-truth construction is independent of ANN and uses no ANN result; retain an independent tiny scalar oracle and ties/signed-zero tests. Do not alter corpus/query membership based on quality.

Prepare BORSUK through existing Rust normalization, SQ8 and generation builders only after original cohort identities are frozen; preserve original bytes for both external services. A completed preparation receipt proves data readiness only, never ANN recall/latency superiority. Require secure regular input files, full SHA/exact EOF, bounded allocation/Arrow decode, no overwrite, file+directory synchronization and marker-last failure handling. Production scope is exactly100k/D1024/1000/k10; small geometry is private test-only. Prospective execution envelope:4CPU/8GiB/noSwap,8GiB scratch,2400s; this declaration is not a launch or measurement.

## Current operator comparison scope

The latest operator direction replaces the prospective scale ladder and comparison plan above. Use Turbopuffer's published vector methodology: Cohere Wikipedia embed-multilingual-v3,D1024,10 million documents,top10,8queries/second for10minutes; report hot and cache-disabled cold independently. Run our own S3 Vectors benchmark and BORSUK benchmark on the SAME sealed vectors,IDs,queries,cosine metric,region,client,load schedule and end-to-end measurement. Only this matched10M benchmark supports comparison/evaluation claims; historical experiments and the100k cohort are correctness/research evidence only. Defer100B and other scale campaigns beyond10M. Preserve live qualification and immutable receipts.

The official test ran in GCP us-central1 on c4a-highmem-32; our S3 Vectors reproduction necessarily uses AWS. Record this mismatch for published Turbopuffer numbers and do not claim measured Turbopuffer parity from them. Exact Turbopuffer document/query IDs are unpublished. Verify our selected population/model/values, then disclose the cohort selection. BORSUK and S3 Vectors must use identical inputs. If S3 Vectors does not expose cache disablement or cache hit telemetry, record that limitation and compare the controllable conditions; do not label an unverified cache state as the published cold/hot condition.

Before a paid comparison, freeze query selection and deterministic8QPS schedule (4800 requests per10-minute cell),query bytes/GT,client placement/concurrency,ingestion completion/warmup,metric/tie rule,full resource+cost accounting and cache controls per system. Cache-state or population mismatch invalidates an equivalence claim, not the library architecture. First complete the current native correctness qualification and tiny real-data preflight. No10M job has been launched or measured by this amendment.

## Prospective 10M population and load selection

The pinned publisher README reports 41,488,110 English passages, so its reported population can supply this arm without adding another language or dataset. This is publisher metadata, not authenticated vector-body evidence. Keep the existing disjoint 1000 query ordinals `[100000,101000)` permanently excluded. For the comparison corpus, select source ordinals `[0,100000)` followed by `[101000,10001000)` in the same lexicographic English shard/row order: exactly 10,000,000 rows. Preserve the original f32 values, publisher IDs and source locators. Reject missing/invalid/duplicate rows rather than replacing them. These IDs are our prospective selection; Turbopuffer's exact selection remains unpublished.

For each 600-second load cell, schedule request `j` at `j/8` seconds for `j=0..4799`, using query ordinal `j mod 1000`. Use the identical scheduled query bytes/order in both systems. Record scheduled, dispatched and completed timestamps separately; do not reduce the offered rate when one system falls behind. Freeze an explicit maximum in-flight limit and an overload/deadline disposition before launch. Report per-query recall over all 1000 queries and end-to-end latency over the actual scheduled load; do not treat repeated requests as additional independent quality samples.

At D1024, the original corpus alone is 40,960,000,000 bytes (arithmetic), excluding IDs, derived indexes, download/decode buffers and ground truth. This does not establish a feasible build or RAM envelope. Before acquisition or comparison, authenticate the selected shard roster and whole bytes, validate every selected row, recompute exact cosine top10 ground truth for the 10M corpus, and preregister bounded build/query/scratch/resource/cost limits. No 10M data or query job is launched by this declaration.
