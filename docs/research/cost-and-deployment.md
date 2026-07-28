# Cost and Deployment Boundary

This page keeps three different costs separate: the durable index, the search
service, and the application/client that calls it. It does not manufacture a
per-vector BORSUK fee and it does not charge BORSUK for a client host while
pretending that a managed database has no client.

## Fair accounting boundary

Every compared system needs application or client compute. With Amazon S3
Vectors, turbopuffer, Pinecone, or Chroma, that client sends a request to a
remote search service. With BORSUK, the library runs the search inside the
client process. Therefore:

- common application/client compute is excluded from the headline product-cost
  row for every system;
- BORSUK's incremental bill is its user-controlled object storage, requests,
  and any optional cache disk; there is no separate database cluster or BORSUK
  query API fee;
- managed-service list prices include their remote search compute, while their
  client CPU/RAM is still outside that service bill; and
- resource comparisons report the measured client/engine CPU, RAM, disk, and
  network separately. They are not silently converted into an arbitrary EC2
  monthly charge.

The Frankfurt `c7g.8xlarge` used by the direct experiments cost `$1.3192/hour`
on demand at the 2026-07-20 price snapshot. That is reproduction context and an
upper-bound benchmark host, not a BORSUK-only surcharge or a recommended
always-on deployment.

## Measured BORSUK storage and request cost

The source artifact is
[`aws-cost-model-2026-07-20.csv`](../web/assets/benchmarks/aws-cost-model-2026-07-20.csv).
Index bytes and object counts are direct listings of the exact selected S3
prefixes. GETs/query come from the selected production profiles. The formulas
use the current AWS Price List values for S3 Standard in `eu-central-1`:

```text
monthly storage = index_bytes / 1,000,000,000 * $0.0245 per GB-month
GET cost / 1M queries = measured_GETs_per_query * $0.43 per 1M GETs
```

| Dataset | Selected index | Storage/month | Uncached GETs/query | GET cost/1M queries | Disk-cached backing GETs |
|---|---:|---:|---:|---:|---:|
| Fashion-MNIST | 0.340 GB | $0.008 | 34.14 | $14.68 | 0 |
| GloVe | 1.331 GB | $0.033 | 185.72 | $79.86 | 0 |
| SIFT | 1.030 GB | $0.025 | 84.28 | $36.24 | 0 |
| NYTimes | 0.744 GB | $0.018 | 80.70 | $34.70 | 0 |
| GIST | 8.499 GB | $0.208 | 168.06 | $72.27 | 0 |
| Deep-Image | 10.943 GB | $0.268 | 166.44 | $71.57 | 0 |

The six selected indexes total 22.887 GB and about `$0.56/month` in S3
Standard storage before replicas, version retention, writes, or tax. The query
column is not a fixed fee: it is the measured GET count for the stated profile.
A different recall point, layout, cache hit rate, or workload changes it. S3
Standard has no data-retrieval charge, and the benchmark client and bucket are
in the same AWS Region; cross-region or internet transfer is outside this
model.

All six selected prefixes are fresh descriptor-v8 `pq-scan-only` source
recreations and contain no graph object. The table uses the actual listed
prefix bytes and measured selected-profile GETs; it does not subtract an
estimated graph fraction or carry a footprint across a format boundary.

## Managed-service list-price context

Prices were checked on 2026-07-20. Billing units are intentionally not mashed
into a synthetic winner because stored logical bytes, indexed bytes, queried
bytes, read units, result bytes, plan minimums, and consistency/cache behavior
are not equivalent.

| Product | Current public price context | What the bill includes |
|---|---|---|
| [Amazon S3 Vectors](https://aws.amazon.com/s3/pricing/) | `$0.06/GB-month` storage, `$0.20/GB` writes, `$2.50/million` Query API calls, processed-data tiers, and returned-data charges; AWS's 10M-vector/1M-query example totals `$11.38/month` | Managed vector storage and remote search compute; application/client compute remains external |
| [turbopuffer](https://turbopuffer.com/pricing) | Launch `$16/month` minimum, Scale `$256/month` minimum, Enterprise `>= $4,096/month` plus a 35% usage premium | Managed remote service; usage dimensions and cache/pinning choices apply |
| [Pinecone](https://www.pinecone.io/pricing/) | Starter free allowance, Builder `$20/month`, Standard `$50/month` minimum, Enterprise `$500/month` minimum; serverless bills storage/read/write units | Managed remote service; client compute remains external |
| [Chroma Cloud](https://www.trychroma.com/pricing) | Starter `$0 + usage` with `$5` credits; `$2.50/GiB` written, `$0.33/GiB-month`, `$0.0075/TiB` queried, `$0.09/GiB` returned | Managed remote service; client compute remains external |

Amazon announced an S3 Vectors query-price reduction effective 2026-06-16;
the table uses the [updated S3 price page](https://aws.amazon.com/s3/pricing/),
not the earlier launch price. Pinecone documents that a query consumes one read
unit per GB of targeted namespace with a 0.25-RU minimum in its
[cost guide](https://docs.pinecone.io/guides/manage-cost/understanding-cost).

## Deployment and license boundary

BORSUK needs no database cluster. Durable state can be only a local directory
or blob/object-storage prefix. Queries still need an application process that
loads the library, and production commonly adds bounded local disk for the
read-through cache. This is an embedded-compute design, not compute-free search.

BORSUK is source-available under BUSL 1.1. Its Additional Use Grant permits
production use without a separate software fee only when the legal entity and
its affiliates had no more than US $100,000 in gross annual revenue in the
most recent fiscal year. Individuals may use it for personal noncommercial use,
research, development, evaluation, and testing. Production use above that
threshold needs a separate commercial license. The Change Date is 2030-07-02
and the Change License is MIT, subject to the BUSL per-version earlier
fourth-anniversary rule in [`LICENSE`](../../LICENSE).

This license boundary is independent of infrastructure cost: “no database
cluster” and “no per-vector service fee” do not mean universally free software,
zero compute, or zero operational cost.
