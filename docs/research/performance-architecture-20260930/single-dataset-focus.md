# Single-dataset benchmark focus

Operator direction on 2026-10-06 replaces the prospective ReLAION/CoHere benchmark pair. Product requirements remain unchanged.

## Selected workload

Cohere Wikipedia, Cohere embed-multilingual-v3, 1024 dimensions, k=10. Turbopuffer's current published vector methodology uses this dataset: https://turbopuffer.com/v3 (checked 2026-10-06). Its published comparison uses 10 million documents, GCP us-central1, 8 queries per second for 10 minutes, separate hot and cache-disabled runs. Published ratios are not a matched BORSUK baseline.

Use one immutable corpus selection and disjoint query split for BORSUK, Turbopuffer and S3 Vectors. Freeze exact dataset revision, selected IDs, vector bytes, normalization, distance metric, query bytes, ground truth and ties before execution. Confirm the source embedding field before downloading a large body. D768 historical embeddings cannot stand in for these D1024 vectors.

## Ordered execution

1. Preserve the completed fitter qualification and the existing source-only Rust worker. Finish its correctness work without launching the superseded real two-dataset panel.
2. Adapt the native implementation to this workload, using a fixed small subset for the first correctness/recall falsifier. Keep the rest of the same corpus for later scale gates; add no other dataset.
3. Measure a matched Turbopuffer baseline on the exact chosen subset and query split; measure BORSUK with the same recall calculation, region, load, cache state and end-to-end timing. Published targets remain labelled unmatched until reproduced.
4. Run S3 Vectors on the identical corpus and queries. Require equal or better recall before claiming a latency/QPS-per-total-dollar win. Use the same comparison accounting and disclose cache-control differences.
5. Optimize one native bottleneck demonstrated by that workload, then scale this dataset to 1M and 10M. Preserve the 100M feasibility and lifecycle requirements without introducing more datasets.

No algorithm win, dataset readiness, service access, matched baseline or measured performance is claimed by this decision. Historical receipts and protocols remain immutable. No new ReLAION or paired multi-dataset campaign is authorized by the current benchmark direction.

## First cohort selection, declared before vector acquisition

For the first baseline, enumerate English `en/*.parquet` files in lexicographic path order at revision `ade45fb52bd549f5e8c065636fe4160a43c2af36`, preserving row order within each file. Corpus source ordinals are `[0,100000)`; query source ordinals are `[100000,101000)`. Read only enough whole authenticated shards to supply these rows. Missing rows, duplicate document IDs, null/nonfinite embeddings, zero norms, or a dimension other than 1024 invalidate preparation; do not silently replace rows or switch datasets. These are our cohort choices, not disclosed Turbopuffer document/query IDs.

Retain original embedding values as little-endian f32 and stable source locators/document IDs. Use cosine distance, k=10, no filters. Ground truth is exhaustive cosine distance over the corpus computed in f64 from those same f32 values, with stable corpus ordinal as the tie breaker. Seal source shard bytes/SHA256, corpus and query ID/byte hashes, ground-truth bytes, metric and tie rule before service ingestion or ANN evaluation. All services receive identical vector values and IDs. This selection declaration is not a completed data seal or a claim of corpus readiness.

First require native correctness and measured recall on all 1000 queries; retain per-query results. Then freeze a separate latency/load protocol against the same sealed cohort. Do not choose another split based on recall, inspect an incomplete performance campaign, or claim cold-cache equivalence where a service does not expose it. Scale later by extending the corpus from this same source while permanently excluding these query document IDs; ground truth must be recomputed for each corpus size.

## Source identity checked before acquisition

Turbopuffer's dataset link resolves to [CohereLabs/wikipedia-2023-11-embed-multilingual-v3](https://huggingface.co/datasets/CohereLabs/wikipedia-2023-11-embed-multilingual-v3). The embedding column is `emb`; the viewer reports 1024 entries. A bounded read of the publisher's dataset API on 2026-10-06 resolved revision `ade45fb52bd549f5e8c065636fe4160a43c2af36`. Use immutable revision URLs for subsequent schema and shard validation. No parquet body was downloaded in this check.

The public Turbopuffer methodology does not disclose selected document IDs or query bytes. Reproduce a matched comparison by uploading our frozen cohort to each service; label comparisons with their published chart as dataset-family targets, not exact reproductions. The dataset revision alone is not a corpus, query, or ground-truth seal.

## Common region and service admission

Use AWS Frankfurt (`eu-central-1`), with Turbopuffer region `aws-eu-central-1`, and place the benchmark client there. Both [Turbopuffer's region list](https://turbopuffer.com/docs/regions) and [S3 Vectors' region list](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-regions-quotas.html) advertise this region (checked 2026-10-06). This differs from the published GCP experiment and must be disclosed.

A read-only SDK check using AWS profile `causality` in this region returned HTTP 200 for `ListVectorBuckets(maxResults=1)` with no buckets in that page. This establishes that operation's access only; index creation, ingestion and querying remain unverified. No resources were created. Turbopuffer credentials are not yet provisioned. Continue native correctness work while that access is pending; do not substitute published charts for a matched service run.

## Native cohort preparation gate

Use one source-only Rust binary with existing Arrow/Parquet, SHA256 and SQ8/generation APIs; add no dependencies or ANN algorithm. Its local inputs are the two whole authenticated English Parquet shards above, supplied by a bounded remote downloader. Full shard bytes/SHA and immutable revision must be sealed before extraction; an ETag/footer is not a whole-body identity. Decode `emb` as finite nonzero D1024 f32 vectors, preserving source order and original values. Admit Arrow row-group decode from pinned metadata before reading bodies; reject oversized batches instead of silently lifting the cap. Retain canonical document identity/source locators and refuse missing/duplicate identities.

The fixed outputs are corpus LEf32 (409600000 bytes), queries LEf32 (4096000 bytes), exact top10 LEu64 truth (80000 bytes), corpus/query IDs and a marker-last identity receipt. Source ordinals `[0,100000)` and `[100000,101000)` are immutable. Compute exhaustive cosine in f64 over original f32 values, with ascending corpus ordinal ties. Ground-truth construction is independent of ANN and uses no ANN result; retain an independent tiny scalar oracle and ties/signed-zero tests. Do not alter corpus/query membership based on quality.

Prepare BORSUK through existing Rust normalization, SQ8 and generation builders only after original cohort identities are frozen; preserve original bytes for both external services. A completed preparation receipt proves data readiness only, never ANN recall/latency superiority. Require secure regular input files, full SHA/exact EOF, bounded allocation/Arrow decode, no overwrite, file+directory synchronization and marker-last failure handling. Production scope is exactly100k/D1024/1000/k10; small geometry is private test-only. Prospective execution envelope:4CPU/8GiB/noSwap,8GiB scratch,2400s; this declaration is not a launch or measurement.
