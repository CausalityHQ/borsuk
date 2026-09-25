# V187 closed cosine PQ physical interval-plan screen

## Decision and root cause

The fresh ReLAION-1M source holdout passes a **source-only elastic quality
screen**, but the V155-resource arm fails both the quality and GET gates.
At width-32 cosine rank, exact truth contained in the top 446 candidate
units was 12,778/12,800, p05 99. After GT-blind mandatory-cover interval
admission under the floor-elastic 446-unit base, the actual fetched units
contained **12,743/12,800**, p05 98: two hits below the 12,745 gate.
The plan used **1,411,587,840 bytes** and **3,868 GETs** over 128 queries.
Bytes fit V155's 128-query scaled 1,425,152,901.12-byte mean, but GETs
exceed its scaled 2,832.128 by 1,035.872. The planner lost 35 truth
positions relative to rank top 446 while spending nearly the full byte
allowance. This is an optional-unit physical-admission and GET-efficiency
problem, not evidence that PQ cosine ranking itself lacks source signal.

The 32-GET/672-unit elastic arm fetched **12,752/12,800**, p05 98, under
each query's 16-MiB cap, but spent 2,080,865,280 bytes and 3,604 GETs
over 128 queries. It passes only the preregistered elastic source-quality
screen; it is about 1.46 times V155-scaled bytes and 1.27 times its GETs.
Do not freeze it as a default or call it a product win. The independent V185
width-32/top-446 source rank failure remains negative tail evidence.

All **256/256** source pseudoqueries had their own row among V115 nominees.
The row was excluded from PQ rank minima and exact truth, but its nominee
unit still seeded the candidate neighborhood, as in V185/V186. This makes
all source-only quality screens potentially optimistic for external queries.
The next decisive gate must remove the query's own nominee before candidate
construction on a fresh disjoint source panel, then measure the effect on
rank and physical coverage. A passing source gate still needs paired used
validation-1000 returned Recall@100, live S3 latency and charged serving
RAM. Separately, improve GT-blind optional-unit/GET utility and global
resource allocation rather than tuning individual query IDs or adding a
100M-vector memory knee.

## Closed authority

Frozen source commit `36f1dcebf7377f9bab2f0f547fcac2cc5e5f7128`, one
Causality Spot `c7i.12xlarge` worker `i-02a3d98d04257a287`, terminated
after the complete terminal. Attempt:
`s3://borsuk-bench-453182569524-euc1/research/v187-cosine-physical-plan/36f1dcebf7377f9bab2f0f547fcac2cc5e5f7128/runs/a0001/`.
Terminal SHA256 `3d8ba34a4e90a79f4bed43ea7709288fa8ad7d81c76d27e660825b999036d77e`;
summary SHA256 `de21bd2f5589c924f3b475f6d03f0ec251db80d41b437065274ae8e6f71583e5`;
features SHA256 `1bec20fafb92ff080c390c13a94b53e0855241186fb3b0441500abae759a0d51`;
plans SHA256 `12e8db6d029198a341399d0e9c5a7c40c07a788feb17f715df8c32fe300bb39f`.
The launcher rehashed every complete artifact and all four pre-truth S3
seals, verified seal times preceded source-label publication, and confirmed
instance termination. Independent closed read-back rehashed the terminal
artifacts; matched all 256 query identities, complete truth-by-unit mass,
mandatory cover, interval order, unit/byte/GET charges and per-query hits;
and reproduced every aggregate below. No incomplete measurement file was
inspected.

## Measured source-only evidence

Dataset: **ReLAION-1M D768 source pseudoquery SHA ranks 1921–2176**.
Fit is ranks 1921–2048 and holdout 2049–2176, 128 queries each. Exact
float64 cosine GT100 excludes the source query row. Counts below are truth
rows in fetched SQ8 units, **not returned Recall@100**.

| Split | Arm | Physical hits / 12,800 | p05 | Planned bytes | GETs | Infeasible queries | Floors above 446 |
|---|---|---:|---:|---:|---:|---:|---:|
| Fit | 32 GET, floor-elastic 446 base | 12,776 | 99 | 1,375,695,360 | 3,667 | 0 | 1 |
| Fit | Strict 32 GET/446 | 12,682 | 99 | 1,364,288,640 | 3,635 | 1 | 1 |
| Fit | 32 GET/672 elastic | 12,779 | 99 | 2,005,311,360 | 3,466 | 0 | 0 |
| Holdout | 32 GET, floor-elastic 446 base | 12,743 | 98 | 1,411,587,840 | 3,868 | 0 | 0 |
| Holdout | Strict 32 GET/446 | 12,743 | 98 | 1,411,587,840 | 3,868 | 0 | 0 |
| Holdout | 32 GET/672 elastic | 12,752 | 98 | 2,080,865,280 | 3,604 | 0 | 0 |

The fit strict-arm count includes one infeasible query scored as zero; its
12682 hits must not be read as feasible-query recall. The holdout width-32
candidate ceiling was 12,780/12,800; rank top 446 held 12,778, p05 99.
On the same closed holdout, an exact mandatory-only geometry diagnostic
found that the minimum GET count needed to fit each query's primary units
within 446 units sums to **1,038 GETs**, maximum 30 for a query. This is
far below the current 3,868 planned GETs, so the large GET excess arises
when admitting optional units. It does **not** prove that a quality-passing
2,832-GET plan exists. A postterminal 22-GET floor-elastic replay on this
closed panel was diagnostic only: two holdout queries had mandatory floors
above the 16-MiB cap, so a fixed 22-GET per-query rule is not a fair V155
resource comparison.

The strongest existing BORSUK comparator remains **V155 used ReLAION-1M
D768 validation-1000** actual returned exact-source Recall@100
99,567/100,000, p05 98, at 11,134,007,040 planned bytes and 22,126
GETs. It is unpaired with V187. No paired S3 Vectors or Turbopuffer result
exists at this revision. Spot preparation took 46.36 seconds and peaked at
9,316,752 KiB RSS; GT-blind planning took 2.64 seconds and peaked at
95,648 KiB RSS; source-truth evaluation took 32.93 seconds and peaked at
9,498,500 KiB RSS. These are offline process resources, not S3 latency or
charged serving memory. No local full suite ran during devbox swap pressure.
