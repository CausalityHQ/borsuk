# Published comparison targets — checked 2026-10-07

Operator scope: benchmark BORSUK using the same dataset family and published conditions, and compare with published vendor results. Do not launch separate vendor-service runs.

| System / scope | Documents / dimensions / k | p50 | p90 | p95 | p99 | Load and cache |
| --- | --- | --- | --- | --- | --- | --- |
| BORSUK completed real-S3 admission | 100k / 1024 / 10 | 181.36 ms | 207.87 ms | 219.46 ms | 245.02 ms | 1000 serial requests; no resident payload cache |
| Turbopuffer published warm namespace | 10M / 1024 / 10 | 14 ms | 17 ms | Not published | 27 ms | 8 QPS; warm namespace |
| Turbopuffer published cold namespace | 10M / 1024 / 10 | 874 ms | 1214 ms | Not published | 1686 ms | 8 QPS; cold namespace |
| S3 Vectors general service statement | Dataset/count/k undisclosed | Not specified | Not specified | Not specified | Not specified | Frequent queries around 100 ms or less; infrequent queries under one second |

Turbopuffer absolute figures come from its [main page](https://turbopuffer.com/), not its v3 ratio chart. The [v3 methodology](https://turbopuffer.com/v3) names Cohere Wikipedia embed-multilingual-v3, 10M documents, GCP us-central1, a c4a-highmem-32 client, 8 QPS for 10 minutes, hot 100% cache hit and cache-disabled cold runs. That page does not establish that every main-page figure used the identical selection/protocol; exact corpus IDs/query split and measured recall remain undisclosed. Do not silently combine these sources into a fully specified paired benchmark.

The [AWS GA announcement dated 2025-12-02](https://aws.amazon.com/blogs/aws/amazon-s3-vectors-now-generally-available-with-increased-scale-and-performance/) provides general latency statements, not Cohere-specific percentiles. No same-Cohere first-party S3 Vectors percentile result was established in this source search. This is missing comparison evidence, not permission to substitute an unrelated dataset or assert a win.

BORSUK recall@10 is 97.23%, underfill 0/1000; reciprocal serial throughput is 5.386 queries/s, not concurrent service capacity. Source/receipt details: [native admission reduction](native-s3-sealed-reduction/gates-a0001/decision.md). Current BORSUK scale, region, cache, load and recall evidence differ from vendor references. No matched quality, latency, throughput or total-cost win is established.

Next performance gate must retain the same frozen corpus/query/truth bytes and disclose startup, resident cache, concurrency, physical requests, measured RAM and complete lifecycle cost. Scale this Cohere corpus after the small causal arm survives. Library APIs remain dataset-independent.
