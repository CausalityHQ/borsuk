# V188 closed leave-one-out nominee source screen

## Decision

On a fresh ReLAION-1M source holdout, removing the query's own row from
the 512 V115 nominees and replacing it with PQ rank 513 **did not change
exact-source containment on any of 128 queries** at top 446 or 672 scored
units. The leave-one-out top-446 count was 12,777/12,800, p05 99, above
the preregistered 12,745/p05 98 screen; its candidate ceiling was
12,779/12,800. Advance the leave-one-out roster rule to a fresh physical
planner gate. The result rules out own-row nominee seeding as the cause of
the aggregate rank-quality signal on this cohort; it does not prove that
source pseudoqueries are representative of external validation queries or
erase V185's separate failed tail.

The self row was PQ nominee rank 1 for all 128 fit queries and rank 1 or 2
for all 128 holdout queries. Removing it changed the width-32 candidate
set on 15 fit and 17 holdout queries, and the top-446 unit set on nine
queries in each split, but no paired truth count at either allowance.
This is a direct paired finding, not a projection. It licenses no
production default, returned-recall, S3-latency or charged-RAM claim.

V187 remains the physical bottleneck: its width-32 cosine rank top 446
contained 12,778/12,800 source truth rows on a different holdout, but its
GT-blind interval plan fetched only 12,743 and used 3,868 GETs/128 queries
against V155's scaled 2,832.128. Repeating the same greedy optional-unit
admission would not answer a new design question. The next change must
make optional-unit selection GET aware, then test its physical plan on a
fresh leave-one-out source cohort before paired used validation.

## Closed authority

Frozen source commit `ba10488847a87925c1ae962229bfdcad6826c926`, one
Causality Spot `c7i.12xlarge` worker `i-0910c818e359f3e74`, terminated
after the complete terminal. Attempt:
`s3://borsuk-bench-453182569524-euc1/research/v188-leave-one-out-source/ba10488847a87925c1ae962229bfdcad6826c926/runs/a0001/`.
Terminal SHA256 `967be3d2285b437f67075ac911549705af4efc280f651ac6ecdcb48e4ed84122`;
summary SHA256 `3f5f4767dca00cd53742b4c2397a9ddffb77b3ee9e4528c821b94afd8c525f46`;
features SHA256 `bcf4d0e4ee18d4c27061fdfae3a1e28b975800a0114f9cb3ae6d9ff5268c35d8`.
The launcher rehashed complete artifacts and the pre-truth S3 seals,
verified seal time before source-label publication, and confirmed instance
termination. Independent closed read-back rehashed feature, seal, label,
summary and resource files; checked all 256 query identities, unique unit
rankings, candidate containment and every paired aggregate. No incomplete
measurement file was inspected.

## Measured source-only evidence

Dataset: **ReLAION-1M D768 source pseudoquery SHA ranks 2177–2432**. Fit
is ranks 2177–2304 and holdout 2305–2432, 128 queries each. Exact float64
cosine GT100 excludes the query row. Counts are truth rows in scored
candidate units, **not returned Recall@100**.

| Split | Nominee arm | Candidate / 12,800 | Top 446 / 12,800 | Top 672 / 12,800 | p05 at 446 | Mean candidate units/query |
|---|---|---:|---:|---:|---:|---:|
| Fit | Historical self included | 12,788 | 12,787 | 12,788 | 99 | 1,892.84 |
| Fit | Leave one out | 12,788 | 12,787 | 12,788 | 99 | 1,897.12 |
| Holdout | Historical self included | 12,779 | 12,777 | 12,778 | 99 | 1,876.29 |
| Holdout | Leave one out | 12,779 | 12,777 | 12,778 | 99 | 1,879.98 |

Paired leave-one-out wins/ties/losses at both top 446 and 672 were
0/128/0 on fit and 0/128/0 on holdout. The strongest BORSUK returned-
quality comparator remains **V155 used ReLAION-1M D768 validation-1000**
actual returned exact-source Recall@100 99,567/100,000, p05 98, at
11,134,007,040 planned bytes and 22,126 GETs. V188 is unpaired with it;
no paired S3 Vectors or Turbopuffer result exists at this revision.

Spot preparation took 80.74 seconds and peaked at 9,318,000 KiB RSS;
source-truth evaluation took 30.12 seconds and peaked at 9,478,664 KiB
RSS. These are offline experiment-process resources, not serving latency
or charged serving memory. No local full suite ran during devbox swap
pressure.
