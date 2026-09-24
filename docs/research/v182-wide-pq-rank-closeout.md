# V182 closed wide PQ source ranking

## Decision

The preregistered source-only width-32 PQ rank screen **passes** at the
smaller tested allowance, 672 complete SQ8 units: fresh holdout contained
12,772/12,800 exact source truth neighbors, p05 99, versus 12,745 and 98
gates. Width-32 candidates contained 12,780/12,800, so eight truth positions
were inside the candidate universe but below PQ rank 672. At 1,344 units,
PQ ranking retained 12,779/12,800. This advances a GT-blind physical
interval planner gate with geometry-derived mandatory cover and elastic
caller budgets. It does not select a production memory cap, establish
returned Recall@100, or show that 32 GETs or any byte budget can fetch these
units. V180's independent width-8 holdout failure remains negative evidence
about source-panel tails.

The rank-only fit curve predicted 12,748.25 holdout hits at 672 units versus
12,772 observed (mean absolute prediction error 0.541 hit/query). At 1,344
units it predicted 12,769.05 versus 12,779 observed (0.379 hit/query). This
curve is a utility heuristic, not a proven recall lower bound. It needs a
conservative calibration/reliability gate before a user-facing recall policy.

## Closed authority

Frozen source commit `34fd11f56559cc7d453f3bd04c92aba83d62d5fc`, one
Causality Spot `c7i.12xlarge` worker `i-01b97d3daa35ca77f`, terminated
after complete terminal. Attempt:
`s3://borsuk-bench-453182569524-euc1/research/v182-wide-pq-rank/34fd11f56559cc7d453f3bd04c92aba83d62d5fc/runs/a0001/`.
Terminal SHA256 `fa3de27a1d56747dbc4e2afd1af3a976e88ed5b90626d4f6dd8f525f426841cb`;
summary SHA256 `1fb85ae456ff611599898f538c8ddefd0a6f74d81dc5c8ac22d9c65525fdaa95`.
The launcher streamed and rehashed every complete artifact. Independent
closed readback rehashed the summary and three resource logs. GT-blind
features were durably sealed to S3 before fit truth; the fit curve and labels
were sealed before holdout truth. No incomplete measurement file was read.

## Measured source-only evidence

Dataset: **ReLAION-1M D768 source pseudoquery SHA ranks 1153–1408**. Fit
is ranks 1153–1280 and holdout 1281–1408, 128 queries each. The exact
float64 cosine source truth excludes the query row. Counts below mean truth
rows contained in candidate units, not returned Recall@100.

| Split | Width-32 candidate / 12,800 | PQ top-672 / 12,800 | PQ top-1,344 / 12,800 | p05 at 672 / 100 | p05 at 1,344 / 100 |
|---|---:|---:|---:|---:|---:|
| Fit | 12,784 | 12,747 | 12,773 | not scored | not scored |
| Holdout | 12,780 | 12,772 | 12,779 | 99 | 99 |

The strongest existing paired BORSUK comparator remains **V155 used
ReLAION-1M D768 validation-1000** actual returned exact-source
Recall@100 99,567/100,000, p05 98, at 11,134,007,040 planned bytes and
22,126 GETs. V182 is a different source-derived panel with no physical plan;
it is not a paired product improvement. No S3 Vectors or Turbopuffer paired
measurement exists for this revision.

Prepare took 43.18 seconds and peaked at 9,301,836 KiB RSS; fit truth took
21.79 seconds and peaked at 9,454,256 KiB RSS; holdout truth took 19.82
seconds and peaked at 9,330,620 KiB RSS, all on Spot. These are offline
experiment-process measurements, not serving latency or serving memory.
