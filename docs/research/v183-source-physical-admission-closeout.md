# V183 closed source physical admission

## Decision and root cause

The preregistered source-only GT-blind ranked cover has **quality at high
bytes but not at V155-like bytes**. On the 128-query holdout, `elastic_32m`
captured a conservative 12,758/12,800 known candidate truth positions with
p05 99, passing the 12,745 and 98 source gate. Its intervals used
3,396,556,800 planned bytes and 2,538 GETs. V155's used-validation mean
scaled to 128 queries is 1,425,152,901.12 planned bytes and 2,832.128 GETs:
the V183 high-quality arm costs about **2.38× bytes** but fewer GETs.

At the V155-mean per-query arm (22 GETs, 446 units before mandatory-floor
elasticity), the source lower and upper bounds were 12,715 and 12,735 hits,
both below 12,745; p05 lower/upper were 96/97. The 16-MiB arm was between
lower and upper source thresholds, so the candidate-outside bound alone
cannot decide it. The observed failure belongs to **physical byte allocation
and layout under this greedy rank policy**. It does not rule out a different
global allocator, score model or physical layout. Mandatory primary floors
forced six of 128 holdout queries above the V155-mean base units; the arm's
aggregate bytes exceeded the scaled V155 mean by about 54.0 MB, while GETs
stayed below it.

The next cheap decisive gate is a truth-aware *resource lower bound* on this
closed source panel: find whether any mandatory-cover plan can meet the
source truth threshold under V155-scaled aggregate byte and GET budgets.
If impossible, revise row layout or primary policy. If possible, build a
GT-blind global price allocator and test it on a fresh sealed panel before
the paired used ReLAION-1M validation-1000 gate. No production cap or
100M vector-count knee is frozen. Lean's conditional gap and work bounds
can check geometry accounting, but they cannot prove source recall, latency
or serving memory without validated data assumptions and measurements.

## Closed authority

Frozen source commit `d9404a9d081c7fbfa492ae0a0e9c4c87ffdec7b6`, one
Causality Spot `c7i.4xlarge` worker `i-03f784c92c13b86d7`, terminated after
complete terminal. Attempt:
`s3://borsuk-bench-453182569524-euc1/research/v183-source-physical-admission/d9404a9d081c7fbfa492ae0a0e9c4c87ffdec7b6/runs/a0001/`.
Terminal SHA256 `373eebeb91dcb1b8d032d8c936ef033332e29113652bc997179cda76be29c976`;
summary SHA256 `338204804fe77ad36de476e9d943fd628e75bf22b9ec3620f9ffc992518c1078`.
The launcher rehashed all seven terminal artifacts and terminated the worker.
Independent closed readback rehashed summary and resource logs. V182
feature/fit seals and terminal were hash checked before planning; the complete
V183 GT-blind plans and plan seal were durably uploaded before the worker
downloaded V182's closed source labels. No incomplete measurement file was
inspected.

## Source-only measured evidence

Dataset: **ReLAION-1M D768 source pseudoquery SHA ranks 1153–1408**,
reusing V182's closed panel; fit 1153–1280, holdout 1281–1408, 128 each.
The metric counts exact source truth **known inside** V182's width-32
candidate set and fetched by whole-unit intervals. The lower bound ignores
possible truth outside that candidate set which a bridge might fetch; the
upper bound adds all outside-candidate truth positions. This is neither
returned Recall@100 nor an independent holdout, S3 latency or serving RAM.

| Holdout arm | Source hits lower–upper / 12,800 | p05 lower–upper | Planned bytes | GETs | Mandatory floors above base |
|---|---:|---:|---:|---:|---:|
| V155 mean: 22 GET, 446 units | 12,715–12,735 | 96–97 | 1,479,154,560 | 2,565 | 6 |
| Elastic 16 MiB: 32 GET, 672 units | 12,737–12,757 | 97–98 | 2,011,501,440 | 3,230 | 0 |
| Elastic 32 MiB: 32 GET, 1,344 units | 12,758–12,778 | 99–99 | 3,396,556,800 | 2,538 | 0 |

The strongest existing paired BORSUK comparator is **V155 used ReLAION-1M
D768 validation-1000** actual returned exact-source Recall@100
99,567/100,000, p05 98, at 11,134,007,040 planned bytes and 22,126 GETs.
V183's source-derived plans are unpaired and cannot be called a product
improvement. No S3 Vectors or Turbopuffer paired cell exists here.

The Spot plan process took 2.47 seconds and peaked at 40,476 KiB RSS;
scoring took 0.17 seconds and peaked at 45,240 KiB RSS. These are offline
experiment-process measurements, not serving latency or charged serving RAM.
The devbox ran only narrow local geometry tests under zero current cgroup
memory pressure, with no local full suite.
