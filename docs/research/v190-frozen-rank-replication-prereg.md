# V190 preregistration: frozen rank-priced source replication

## Question and frozen authority

V189's predeclared rank-priced secondary arm met its 128-query holdout
source gate by only one truth row and missed the same threshold on fit.
Test whether that result transfers to an untouched 256-query source
panel **without refitting, changing a price, or choosing query IDs**.
This is a replication of a secondary arm, not a new optimization search.

Dataset is ReLAION-1M D768. Select source pseudoqueries by the existing
stable-ID SHA ordering at ranks **2689–2944**, all 256 used as one
validation split. Exact float64 cosine GT100 excludes the query row.
The V189 frozen fit-trained rank model is the immutable `model.json`
artifact at
`s3://borsuk-bench-453182569524-euc1/research/v189-predicted-interval-source/ae0b160e0b48c419a6bac1b6d9e5964f7fd4b63b/runs/a0002/artifacts/out/model.json`,
SHA-256 `cef01b55c5a196db187dc303dd002f33b00e75c6083465cf8ff746ea3d9beb5c`.
Use only its rank curve and its price `(6000, 50000)` in millionth-hit
units. The model, price, candidate width, nominee rule, physical format,
baseline and gate are fixed before any V190 truth read. The model is a
per-generation source calibration, with no dataset-name branch, query-ID
exception or fixed 100M RAM knee.

## Method and controls

Repeat V189's 513-PQ nominee call and direct-512 prefix check. Remove
the source row and take the next 512 nominees. SQ8 squared-L2 ranks the
100 exact-primary nominees; their physical units are mandatory. Expand
nominee units by width 32, score those rows with PQ reconstructed cosine,
exclude the source row, and rank units by minimum PQ score. The rank
model predicts per-unit mass; the exact sparse priced interval planner
uses fixed prices and at most 32 GETs per query. Reject any plan above
672 full units or 16 MiB. The same V187 greedy 446-base, mandatory-floor
elastic plan is the paired control. There is no margin arm or fit phase.

Before opening source truth, seal all 256 query features and both arms'
complete physical plans to S3. After truth, independently replay
identities, candidate ceiling, top-446 truth, mandatory coverage,
bridge units, bytes, GETs and captured GT100 rows. Measure offline
planner CPU separately from source prep and truth evaluation, and report
offline process RSS. These are not serving latency or charged RAM.

## Gate and interpretation

The frozen rank-priced arm advances only if it fetches
**≥25,490/25,600** exact-source GT100 rows with p05 ≥98, zero infeasible
queries, ≤2,850,305,802 planned bytes and ≤5,664 GETs in aggregate,
and every query ≤672 full units and ≤32 GETs. The aggregate resource
limits are V155's 1000-query planned charges scaled to 256 and floored.
Report the greedy paired control even if it exceeds those limits. A
failed replication rejects this frozen rank/price as a candidate; no
posthoc adjustment on V190 truth is allowed. A pass advances only to a
fresh paired **used ReLAION-1M D768 validation-1000** actual returned
Recall@100, live S3 latency, charged-RAM and failure-recovery gate.

The strongest BORSUK returned comparator remains V155 used validation-
1000 actual returned exact-source Recall@100 99,567/100,000, p05 98,
at 11,134,007,040 planned bytes and 22,126 GETs. V190 source truth
containment is unpaired with V155 and cannot establish a product win.
No paired current-revision S3 Vectors or Turbopuffer result exists.

Run one immutable Causality Spot cell. Record source commit, model and
input hashes, instance identity, complete terminal, pretruth seal times
and artifact hashes. An interruption discards the whole cell and starts
a new attempt ID. Terminate compute immediately after its terminal.
