# V173 generic recall and resource gate: design before preregistration

## Decision to test

The V171 strict per-query V155 resource profile is infeasible for one of
1,000 used ReLAION-1M validation queries solely because the relaid SQ8
geometry needs nine primary runs where the historical layout spent three
GETs. The next method must cover mandatory primary units first and allow
GETs to move between queries. It must not branch on a dataset name, query
ordinal, vector-count knee or a fixed 100M RAM ceiling.

Inputs to a production planner are requested `k`, caller recall target
`rho`, maximum per-query bytes and GETs, and a measured cost profile for
the object store. Corpus size, row layout, concurrent generations and
target recall determine actual charged memory. The planner exposes its
measured resident bytes and fails explicitly when a caller's cap is
infeasible. The V155 aggregate bytes/GETs are a **qualification target**
for this 1M comparison, not model coefficients or product defaults.

## Candidate and cost rule

Authenticate the old-router to relaid-SQ8 row permutation before scoring
candidate physical units. Every exact-primary unit is mandatory. Score
optional nominee and nearby units with the direct row-level PQ field
selected by V170; retain the same-universe neighbor-rank arm as a control.
Train optional-unit utility on source-only pseudoqueries from hash ranks
disjoint from V166–V168. Fit and holdout partitions, query exclusion,
candidate universe width, calibration bins, tie rules and any smoothing
must be sealed before real-query truth is opened. Useful-row probability
may be summed within a unit by linearity of expectation; that sum is a
ranking estimate, not a certified per-query recall bound. Verify its
held-out reliability and report misses outside the candidate universe.

For each query, generate a nondominated interval frontier of actual SQ8
bytes, GETs and estimated captured utility, subject to complete primary
coverage and 32 GET / 16 MiB physical caps. A global allocator chooses
one frontier point per query using the caller's cost profile and recall
target; it may redistribute resource across queries. It reports queries
whose mandatory cover or predicted target is infeasible. Use a measured
range-GET cost curve rather than treating one GET as a fixed invented
number of bytes. If a production query needs primary SQ8 scores before
planning, charge its extra wave or its local mirror memory explicitly.

## Cheapest decisive sequence

1. Perform a source-only candidate-universe and calibration reliability
   gate. Kill an arm that fails held-out calibration or has excessive
   useful rows outside its universe. This stage opens no real-query GT.
2. Verify mandatory coverage, exact actual-byte/GET accounting, physical
   caps and aggregate comparison by an independent checker. Stop before
   truth if all preregistered target points exceed either V155 aggregate
   cap: 11,134,007,040 bytes or 22,126 GETs over its used 1,000 queries.
3. Seal complete GT-blind plans and source archive identities. Run the
   paired used ReLAION-1M D768 validation-1000 quality gate with identical
   exact-source reranking and compare to V155's 99,567/100,000 GT100 hits
   and p05 98, with the resource caps above. Report per-query failures,
   direct/control paired differences and charged planner resources.
4. Only a passing used-cohort result earns one frozen holdout and then
   live S3 latency, throughput, memory and scale gates. A used-cohort pass
   is not a product default or a 100M result.

The next experiment preregistration must pin the source-only split,
utility estimator, cost measurement, width, recall target, global
allocation algorithm and thresholds *before* any new truth read. The
Fable consultation's suggested fixed `rho = 0.996`, offset coverage
`99.5%` and reliability tolerances are unverified proposals; they are
not production constants or measured quality. Its estimated optional
GET slack (4,442) and byte slack (10,035,192,960) are simple differences
between the V155 aggregate and V171 mandatory-only witness, not a
measured optional plan or proof of achievable recall.

## Formal boundary

Lean can check a sealed plan's complete primary coverage, interval
union, actual bytes, GET count and conditional recall implication from
an explicitly supplied bound on omitted true neighbours and quantization
error. A source-fitted probability model does not prove that bound for
new queries. Recall, p05, latency, charged RAM and S3 cost still require
paired measurements. Any formal metric premise must match the GT metric;
cosine and squared L2 are interchangeable only under the stated vector
normalization assumption.
