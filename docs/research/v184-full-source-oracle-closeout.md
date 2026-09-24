# V184 closed full-source aggregate resource oracle

## Decision

The V164 whole-unit physical layout and V182 mandatory primary policy are
**not geometrically infeasible** at the V155-scaled aggregate resource cap
on this closed 128-query source panel. An exact truth-aware witness contains
12,745/12,800 source neighbors, p05 98, while reading **5,841 complete
32-row units = 145,791,360 bytes** in **2,425 GETs**. The byte and GET caps
were 57,097 units = 1,425,141,120 bytes and 2,832 GETs. The witness was
reconstructed from the per-query exact minimum-unit frontiers and replayed
independently from the closed S3 truth/plan artifacts. It covers every
mandatory primary unit. The nonnegative GET-price search was decisive at
price zero, so no exact global GET-constrained fallback was needed.

This narrows V183's 3,396,556,800-byte high-quality GT-blind plan failure:
its byte gap is caused by **optional-unit prediction/admission and resource
allocation**, not a mandatory-cover lower bound or an intrinsic byte floor
of this layout on this panel. The oracle knows the exact 100 neighbors and
therefore is an upper bound on achievable source quality, not a serving
method or a product performance claim. A future GT-blind rule must learn to
stop admitting low-value units and allocate resources across queries using
source-only features. It must then pass a fresh sealed source panel and the
paired used-query validation-1000 gate. No query-ID exception, fixed 100M
memory knee or production default is licensed.

## Closed authority

Frozen source commit `06437bd3bdeb3be60b8ecb1571ad2cbbe27bdcf5`, one
Causality Spot `c7i.8xlarge` worker `i-02a14607af04cdb84`, terminated
after complete terminal. Attempt:
`s3://borsuk-bench-453182569524-euc1/research/v184-full-source-oracle/06437bd3bdeb3be60b8ecb1571ad2cbbe27bdcf5/runs/a0001/`.
Terminal SHA256 `745344c2203a68d379fa20bd8bd234037322a2eb6cf4d3839479877aa32df0f5`;
summary SHA256 `75a3247807246f7c72515d7ca773f8dc13d85fcf859b240500ceaacc6204f8ad`;
witness SHA256 `da196c59e47693e3d1e62cb8d1c53047b34727b1e9f81cc1c15ccb1caa62be25`.
The launcher streamed and rehashed all terminal artifacts before terminating
the worker. An independent closed replay rehashed V182 features and V184
truth/witness artifacts, checked all 128 query identities, whole-unit
interval order and bounds, every mandatory primary unit, exact truth hits,
units and GETs; it reproduced 12,745 hits, p05 98, 5,841 units and 2,425
GETs. V184 independently remapped full exact source truth to V164 physical
units and matched V182's candidate-only labels.

## Scope and resources

Dataset: **ReLAION-1M D768 source pseudoquery SHA ranks 1281–1408**,
the already closed V182 holdout. Exact float64 cosine GT100 excludes the
query row. The metric is truth-aware source containment in planned SQ8
units, not returned Recall@100, an independent holdout, S3 latency or
serving RAM. The strongest paired BORSUK comparator remains **V155 used
ReLAION-1M D768 validation-1000** actual returned exact-source Recall@100
99,567/100,000, p05 98, with 11,134,007,040 planned bytes and 22,126
GETs. V184 is unpaired and cannot be described as beating V155. No paired
S3 Vectors or Turbopuffer measurement exists at this revision.

The Spot oracle process took 19.71 seconds and peaked at 9,289,496 KiB
RSS. These are offline experiment-process measurements, not serving
latency or charged serving memory. The devbox ran only narrow tests and a
small independent artifact replay; no local full suite ran.
