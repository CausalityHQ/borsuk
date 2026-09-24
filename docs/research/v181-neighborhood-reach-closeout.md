# V181 closed generic neighborhood reach

## Decision

The preregistered **source-only** V181 holdout selects width 8 as the
smallest tested neighborhood passing 12,745/12,800 hits and p05 at least 98.
Width 8 held 12,770/12,800, p05 98; width 16 held 12,777, p05 99; width 32
held 12,781, p05 99. This advances a GT-blind ranking and physical planner
test. It does **not** establish a production width: V180's separate 64-query
holdout under the same width 8 held only 6,367/6,400 with p05 96, so the
source panel tail is unstable. The next screen should use a disjoint panel
and test whether wider candidate reach survives PQ ranking and a finite
unit budget. Caller recall and measured resources, not a corpus-size knee,
must determine any eventual memory/read policy.

## Closed authority and measurement

Frozen source commit `27570a87e95192b9199fcb315cf212a7936972ff`, one
Causality Spot `c7i.12xlarge` worker `i-00f0eb56a9f9fe84a`, terminated after
complete terminal. Attempt:
`s3://borsuk-bench-453182569524-euc1/research/v181-neighborhood-reach/27570a87e95192b9199fcb315cf212a7936972ff/runs/a0001/`.
Terminal SHA256 `60e8252fbc6938434ae3ec45ee7020c6ef7d2852f6b4ed552f5d32114fd0758f`;
summary SHA256 `b0c3d66f3d56fcca2c97bd9db6bdf8c3e949fe8bfe10da2d5199fc4457fc96ae`.
The launcher streamed and rehashed every terminal artifact. Independent
closed readback rehashed the summary, rosters, source labels and resource
logs. Rosters were sealed in S3 before exact source truth was computed.

Dataset: **ReLAION-1M D768 source pseudoquery SHA ranks 897–1152**. Fit is
ranks 897–1024; holdout is ranks 1025–1152, 128 queries each. Metric is
exact float64 cosine source truth contained in width-expanded physical unit
sets after excluding the query row. These are candidate ceilings, not
returned Recall@100 or S3 plans.

| Width in 32-row units | Fit hits / 12,800 | Fit p05 | Holdout hits / 12,800 | Holdout p05 | Holdout median candidate units | Holdout p95 candidate units |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 12,764 | 98 | 12,770 | 98 | 622.5 | 1,662.5 |
| 16 | 12,769 | 99 | 12,777 | 99 | 974 | 2,623.3 |
| 32 | 12,773 | 99 | 12,781 | 99 | 1,586.5 | 4,403.3 |

V155 used **ReLAION-1M D768 validation-1000** remains the strongest paired
BORSUK comparator at 99,567/100,000 actual returned exact-source
Recall@100, p05 98, 11,134,007,040 planned bytes and 22,126 GETs. V181 is
unpaired source-derived containment, so it cannot be read as a product gain.
There is no paired S3 Vectors or Turbopuffer cell at this revision.

Prepare took 36.30 seconds and peaked at 9,361,200 KiB RSS; evaluation took
30.50 seconds and peaked at 9,438,376 KiB RSS on Spot. These are offline
experiment-process resources, not serving latency or serving RAM. The
devbox ran narrow tests only and had zero current cgroup memory pressure.
