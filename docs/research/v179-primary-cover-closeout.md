# V179 production primary-cover increment

The V178 source-only oracle falsified the fixed 32-GET/16-MiB primary policy
for one ReLAION-1M pseudoquery. The production `physical_interval` module now
computes the exact whole-page primary cover floor for any caller GET cap,
reports both actual object bytes and the rounded byte budget its DP requires,
rejects an insufficient cap as `InsufficientBudget`, and verifies all primary
pages after planning. The rule depends on physical page positions and caller
limits, not a dataset name, query ID, corpus-size knee or fixed memory ceiling.

The verified source commit was `9fb01434081bae33c7cc8a18d5eb89968ccc894a`.
One Causality Spot `c7i.8xlarge` worker
`i-039e18f03afc83be2` ran
`cargo test --locked -p borsuk --lib physical_interval::tests` on that exact
source archive at
`s3://borsuk-bench-453182569524-euc1/research/v179-primary-cover-compile/9fb01434081bae33c7cc8a18d5eb89968ccc894a/runs/a0001/`.
Its complete terminal SHA256 is
`beb74e315c935954a0e066bf72c7ec8492fa55bb1bf81258b9cab383db27120a`;
test-log SHA256 is
`a782d17c909e0b21c0d7e11c224537646381c1079dc303bc33ae545a8fb531fd`.
The closed log reports **14 passed, 0 failed**, including an exhaustive
six-page primary-cover comparison, the insufficient-cap behavior and the
short-final-page budget distinction. The launcher streamed and rehashed
all closed artifacts and confirmed the worker terminated. Build/test wall
time was 1:39.83 and peak RSS 5,021,648 KiB on Spot. Neither is a serving
latency or serving-memory measurement.

A separate read-only review process unexpectedly started the same narrow
Cargo test locally, temporarily reaching roughly 1.9 GiB `rustc` RSS while
this devbox had swap allocated. It exited; the consultation was canceled to
prevent further local builds. No full suite ran locally. Documentation
comments added after the remote gate do not alter executable behavior.

This slice makes low-cap requests fail closed. It does not itself allocate a
larger cap, select optional units or establish returned recall. The next
production/research step is a geometry-derived minimum for every query,
followed by caller-approved elastic budgets and a globally priced optional
utility frontier. The next paired gate remains **used ReLAION-1M D768
validation-1000** exact-source returned Recall@100 against V155's
99,567/100,000 hits, p05 98, 11,134,007,040 planned bytes and 22,126 GETs.
That comparison is still unmeasured for this architecture.
