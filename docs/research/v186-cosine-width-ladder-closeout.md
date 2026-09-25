# V186 closed cosine PQ candidate-width ladder

## Decision

On the fresh preregistered ReLAION-1M source holdout, the smallest radius,
**32 units**, passes the source-rank gate at the smaller 446-unit allowance:
12,785/12,800 exact truth rows, p05 99, versus required 12,745 and 98.
Every truth row reachable by its width-32 candidate set is already in the
top 446. Radius 64 reaches 12,790 and radius 128 reaches 12,794 at the same
allowance, but scores a larger candidate universe. Advance width-32 cosine
ranking to a fresh, GT-blind mandatory-cover physical interval-planner gate.

This is **not** a stable production-quality result: the independent V185
holdout on ReLAION-1M source ranks 1537–1664 scored only 12,738/12,800,
p05 98, at width 32/top 446 and failed the same gate. Its single 28-miss
candidate outlier did not recur in V186. The observed panel variability is
the next tail-reliability problem; a physical-plan pass must be replicated
on fresh source panels before used-query returned recall, S3 latency, or a
production default. Do not replace the failed V185 evidence with the easy
V186 panel or add a query-specific exception.

## Closed authority

Frozen source commit `3eb74be9761506b872c75dc34c015d96fb8e5619`,
one Causality Spot `c7i.12xlarge` worker `i-0b0e6f5d55a500d09`, terminated
after the complete terminal. Attempt:
`s3://borsuk-bench-453182569524-euc1/research/v186-cosine-width-ladder/3eb74be9761506b872c75dc34c015d96fb8e5619/runs/a0001/`.
Terminal SHA256 `e58b9609913eb7e7c1813140a9a4a6518ead11b5f7dde9fd9ebfdeb753c0d21c`;
summary SHA256 `0e2be7e008a83d8d6ec33d6783c02bd3a35931c187609dd3848047635b84602a`;
features SHA256 `20de8883ae073ced9d048856d26854bbf6140170b74f9c2cd498286f46a9c6a2`.
The launcher streamed and rehashed every terminal artifact, then verified
instance termination. Independent closed read-back rehashed the summary,
features, seal, labels and resource logs; checked all 256 query identities,
nested candidate sets, mandatory cover, containment bounds and aggregate
counts. All three complete GT-blind rankings were sealed to S3 before source
truth opened. The byte-count erratum is recorded separately in
`v186-cosine-width-ladder-prereg-erratum.md`; it changes no gate or result.

## Measured source-only evidence

Dataset: **ReLAION-1M D768 source pseudoquery SHA ranks 1665–1920**. Fit
is ranks 1665–1792 and holdout 1793–1920, 128 queries each. Exact float64
cosine GT100 excludes each query row. Counts are true neighbors contained
in candidate units, **not** returned Recall@100.

| Split | Radius | Candidate / 12,800 | Top 446 / 12,800 | Top 672 / 12,800 | p05 at 446 | Mean candidate units/query | Max candidate units/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| Fit | 32 | 12,790 | 12,789 | 12,790 | 99 | 1,959.01 | 6,860 |
| Fit | 64 | 12,791 | 12,789 | 12,791 | 99 | 3,233.53 | 10,817 |
| Fit | 128 | 12,792 | 12,790 | 12,791 | 99 | 5,371.18 | 15,870 |
| Holdout | 32 | 12,785 | 12,785 | 12,785 | 99 | 1,824.80 | 5,299 |
| Holdout | 64 | 12,790 | 12,790 | 12,790 | 99 | 3,012.83 | 8,632 |
| Holdout | 128 | 12,794 | 12,794 | 12,794 | 100 | 5,031.94 | 14,037 |

Radius 32 scores about 5.84% of the 31,250 physical units on average in
holdout. Radius 128 scores about 16.10% on average and up to 44.92%; those
are **offline PQ candidate-scoring fractions**, not object reads or serving
RAM. The 446 selected complete units would be 11,132,160 raw SQ8 bytes
before mandatory cover and interval bridges. A physical plan may require
more units and GETs.

The strongest existing BORSUK comparator remains **V155 used ReLAION-1M
D768 validation-1000** actual returned exact-source Recall@100
99,567/100,000, p05 98, at 11,134,007,040 planned bytes and 22,126
GETs. V186 is an unpaired source panel and cannot be described as beating
V155. No paired S3 Vectors or Turbopuffer measurement exists at this
revision.

Spot preparation took 65.32 seconds and peaked at 9,314,156 KiB RSS;
source-truth evaluation took 31.21 seconds and peaked at 9,580,044 KiB RSS.
These are offline experiment-process measurements, not S3 query latency or
charged serving memory. No local full suite ran during devbox swap pressure.
