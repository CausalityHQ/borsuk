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

## Source identity checked before acquisition

Turbopuffer's dataset link resolves to [CohereLabs/wikipedia-2023-11-embed-multilingual-v3](https://huggingface.co/datasets/CohereLabs/wikipedia-2023-11-embed-multilingual-v3). The embedding column is `emb`; the viewer reports 1024 entries. A bounded read of the publisher's dataset API on 2026-10-06 resolved revision `ade45fb52bd549f5e8c065636fe4160a43c2af36`. Use immutable revision URLs for subsequent schema and shard validation. No parquet body was downloaded in this check.

The public Turbopuffer methodology does not disclose selected document IDs or query bytes. Reproduce a matched comparison by uploading our frozen cohort to each service; label comparisons with their published chart as dataset-family targets, not exact reproductions. The dataset revision alone is not a corpus, query, or ground-truth seal.

## Common region and service admission

Use AWS Frankfurt (`eu-central-1`), with Turbopuffer region `aws-eu-central-1`, and place the benchmark client there. Both [Turbopuffer's region list](https://turbopuffer.com/docs/regions) and [S3 Vectors' region list](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-regions-quotas.html) advertise this region (checked 2026-10-06). This differs from the published GCP experiment and must be disclosed.

A read-only SDK check using AWS profile `causality` in this region returned HTTP 200 for `ListVectorBuckets(maxResults=1)` with no buckets in that page. This establishes that operation's access only; index creation, ingestion and querying remain unverified. No resources were created. Turbopuffer credentials are not yet provisioned. Continue native correctness work while that access is pending; do not substitute published charts for a matched service run.
