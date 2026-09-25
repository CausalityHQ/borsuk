# V193 optional utility 100k transfer: closed developmental screen

## Authority and decision

V193 `a0001` completed on Causality Spot instance `i-0165163efae8d71e6`
from source commit `cdbe66a269a2cd580dd46940e1838ef9a915ceb3` and source
archive SHA-256 `5ebe13163b57204f78a4534c7bdc20f63371940356d0c6f16204ca6bbc43d06c`.
The complete terminal at
`s3://borsuk-bench-453182569524-euc1/research/v193-optional-100k-transfer/cdbe66a269a2cd580dd46940e1838ef9a915ceb3/runs/a0001/terminal.json`
has SHA-256 `a176550b370ce1a3d4f5a7b9a265f7a4a91f1165c33a9aa4fe5fbb7bea980676`,
`status=complete`, `exit_code=0`. Its GT-blind plan seal has SHA-256
`f5f11651c50c24e8bbdd399053f39146fb49dc9f180bca64f7a778eadeb88cfb`.
The plans, raw results and summary have SHA-256
`04c412566b38fdb2461069a34e48d92214c983d78ae39051579cec09588964c5`,
`80e1ef702a46ec30b5ac6a2d5fe42ed8643a59719b28284b67eaa06fa64bca19`,
and `cd18612b3675409ba0edf29dce0f47b4e93864aef5230c6092813301d1d5a28b`.
The controller checked the terminal and artifact digests and terminated the
Spot instance. A separate checked-in replay,
`scripts/check_v193_optional_100k_transfer.py`, rechecks all 1,000 queries
and 3,000 arm/query rows from authenticated artifacts. It passed. There is
no active V193 compute.

The preregistered decision is **advance to a fresh 1M source gate**. It is a
screening decision on the already used ReLAION-100k D768 development-1000
real-query split, not a fresh-query quality or significance claim. No 100k
label was used to fit the model or set a price. The V189 ReLAION-1M D768
fit64 model and V192 fit prices were frozen before V193 planning. GT-blind
plans and seal were copied to S3 before the 100k GT100 and V170 returned
rows were opened.

## Measured paired results

All hit counts below are **actual SQ8 returned top-100 intersections with
GT100**, out of 100,000 possible hits across 1,000 queries. Coverage is
GT100 IDs physically present in fetched SQ8 ranges; planned bytes and GETs
are planner outputs, not live object-store serving measurements. The p05 is
the nearest-rank fifth percentile of per-query returned GT100 hits.

| Arm on ReLAION-100k D768 development-1000 reused | Returned hits / 100,000 | p05 hits / 100 | Physical GT100 coverage / 100,000 | Planned bytes | Planned GETs |
| --- | ---: | ---: | ---: | ---: | ---: |
| V193 optional-risk | 99,379 | 98 | 99,765 | 7,116,744,960 | 8,418 |
| V193 full-rank, separately fit price | 99,419 | 98 | 99,829 | 13,089,772,800 | 13,542 |
| V193 constant-risk ablation | 99,379 | 98 | 99,773 | 11,893,814,400 | 8,996 |
| V170 direct PQ64, frozen paired baseline | 99,357 | 98 | 99,747 | 14,785,704,960 | 13,888 |

The candidate field contains 99,934 GT100 IDs. All 3,000 V193 plans are
feasible, preserve the mandatory primary units and stay within **each
query's** V170 byte and GET cap; the optional arm also satisfies the
aggregate cap. Versus V170, optional-risk returns 22 more hits, covers 18
more GT100 IDs, plans 7,668,960,000 fewer bytes (51.9%) and 5,470 fewer
GETs (39.4%). Paired optional-versus-V170 query counts are 49 wins, 902
ties and 49 losses, net +22 hits. Against the stronger contemporary
full-rank arm, optional has **40 fewer returned hits** and 64 fewer
covered IDs, while planning 5,973,027,840 fewer bytes and 5,124 fewer
GETs. Paired optional-versus-full-rank counts are 21 wins, 924 ties and
55 losses. Optional and constant-risk have equal aggregate returned hits;
optional plans 4,777,069,440 fewer bytes and 578 fewer GETs. Thus V193
supports a resource/quality frontier, not categorical quality dominance.

The optional arm's minimum returned hit count is 90, with 26/1,000
queries below 98 and a bottom-decile mean of 97.68. Full-rank has 25
queries below 98 and a bottom-decile mean of 97.80. These are descriptive
development-cohort statistics, not uncertainty bounds for new queries.
V193 offline planning took 127.76 s wall/125.28 s user CPU with 202,960
KiB peak RSS on the Spot worker; offline SQ8 evaluation took 17.54 s
wall/17.27 s user CPU with 252,996 KiB peak RSS. Neither time is serving
latency, and neither RSS value is charged production serving RAM.

## What the screen resolves and what remains

The V192 candidate-optional utility transfers across 1M-fit to 100k
real-query planning without a per-dataset refit and meets the exact
predeclared 100k baseline, physical coverage, p05 and resource gates.
It also shows the cost of allocating more: full-rank gains 40 returned
hits. A single reused cohort cannot establish a generic method, and the
V189 fit diagnostic's zero predictor had lower mean absolute error than
the optional predictor; this campaign does not repair that calibration
limitation. The method must be judged by physical return quality and cost
on fresh data, not fit prediction MAE or attractive numbers on this split.

The next gate freezes the V193 optional and full-rank policies and their
prices, then evaluates at least 512 **new ReLAION-1M D768 source queries**
on a predetermined identity/rank split after all V189/V190 fit and
diagnostic identities. Prepare and seal GT-blind plans before opening
GT100. Report candidate ceiling, mandatory floors, physical coverage,
actual returned top-100 hits, p05, number below 98, bottom-decile mean,
and an uncertainty interval for the lower tail. Compare optional and
full-rank as a paired frontier under the same hard limits, alongside the
strongest authenticated BORSUK comparator available on those exact
queries. Do not retune on the holdout. If this passes, measure live S3
GET/read latency, throughput, charged serving RAM and cost, then repeat
on a distinct dataset and larger scales. The older V155 ReLAION-1M D768
validation-1000 returned baseline is 99,567/100,000 hits, p05 98,
11,134,007,040 planned bytes and 22,126 GETs, but it is **unpaired and
used**; it cannot substitute for the new-query gate. No current V193
live S3 latency, 10M/100M, or matched S3 Vectors/Turbopuffer result is
available.
