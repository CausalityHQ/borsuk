# V180 closed source PQ rank signal

## Decision and root cause

The preregistered fixed width-8, top-672 PQ rank policy **fails** its source
holdout gate. Holdout contained 6,361/6,400 exact source truth neighbors with
p05 96, below the 6,373 and 98 thresholds. The width-8 candidate universe
itself contained only 6,367/6,400. Thus 33 of the 39 missed positions were
outside the candidate universe; only six were in the universe but below the
PQ top-672 cutoff. Two holdout queries incurred those six ranking losses.
The primary responsible layer is candidate generation, and the 64-query fit
panel underestimated this source holdout tail. Do not treat the summary's
`kill-rank-only-pq-utility` code as proof that PQ ordering alone is the dominant
failure. It kills the **whole fixed width-8 rank-only policy** at this gate.

The fit-trained isotonic rank curve predicted 6,396.01 top-672 holdout hits,
35.01 above observed; mean absolute per-query prediction error was 0.806
hits. It is not a calibrated recall guarantee. The next experiment should
expand the candidate source generically, assess candidate coverage separately
from ranking, and calibrate on a larger fresh source fit/holdout panel. A
physical GET/byte planner and paired used-query comparison remain necessary.
No dataset/query ID exception or fixed 100M vector-count knee is licensed.

## Closed authority

Source commit `8a97841fa34dd51aa3d2d047bb0c1e45788bb4ee`; one Causality
Spot `c7i.12xlarge` worker `i-01bff2937c95fdfb5`, terminated after a
complete terminal. Attempt:
`s3://borsuk-bench-453182569524-euc1/research/v180-pq-rank-signal/8a97841fa34dd51aa3d2d047bb0c1e45788bb4ee/runs/a0001/`.
Terminal SHA256 `5e46b5361ef65ab0745adb29dbc39629614574fe23994298a55f300353ef3dd3`;
summary SHA256 `51f75144de8f0a6688063d5fb157a3313317b4bc32b70217ce96290213c10adf`.
The launcher streamed and rehashed all ten terminal artifacts against S3
after the terminal. Independent closed readback rehashed the summary, both
label files and three resource logs. Prepare and fit seals were durably
uploaded before the later truth phase. No incomplete measurement file was
inspected.

## Measured source-only evidence

Dataset: **ReLAION-1M D768 source pseudoquery SHA ranks 769–896**. Fit is
ranks 769–832 and holdout 833–896, 64 queries each. Exact float64 cosine
source truth excludes the query row. All counts below are source truth rows
contained in physical units, not returned Recall@100 or feasible S3 reads.

| Split | Width-8 candidate / 6,400 | PQ top-672 / 6,400 | Top-672 p05 / 100 | PQ loss within candidate |
|---|---:|---:|---:|---:|
| Fit | 6,395 | 6,395 | 99 | 0 |
| Holdout | 6,367 | 6,361 | 96 | 6 |

The strongest existing paired BORSUK comparator is **V155 used ReLAION-1M
D768 validation-1000** exact-source returned Recall@100 99,567/100,000,
p05 98, with 11,134,007,040 planned bytes and 22,126 planned GETs. V180
uses different source-derived queries and does not plan physical reads, so
its counts cannot be subtracted from V155 as a product performance delta.
There is no paired S3 Vectors or Turbopuffer cell at this revision.

Prepare took 23.69 seconds and peaked at 9,342,304 KiB RSS; fit truth took
17.65 seconds and 9,438,564 KiB RSS; holdout truth took 15.72 seconds and
9,348,148 KiB RSS, all on Spot. These are **offline experiment process**
measurements, not serving latency or charged serving RAM. The devbox ran only
narrow Python tests and syntax checks, with no local full suite.
