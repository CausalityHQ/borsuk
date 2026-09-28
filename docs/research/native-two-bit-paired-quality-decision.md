# Frozen two-dataset offline quality decision

Both datasets: first100k, D768 cosine k100, validation256–999 (744 queries
each). Same library nomination/physical budgets and sequential-f32 SQ8 score
method; each paired flat control uses its same dataset artifact/calibration
and normalized queries. Values below are measured offline against exact GT100.
Historical V282/V296 raw-query arithmetic remains separate evidence.

| Dataset | Returned mean R@100 percent | p05 hits | Paired flat mean /p05 | Mean deficit pp |
| --- | ---: | ---: | ---: | ---: |
| CoHere |99.131720|98|99.315860/98|0.184140|
| ReLAION |99.162634|97|99.568548/99|0.405914|

Both pass >=98mean, p05>=95, deficit<=0.5pp,32GET and16MiB. Observed
maximum32planned GET/16,773,120bytes for both. ReLAION fetched coverage
99.532258mean/p05 98; mean pre-scoring loss0.467742hits and fetched-to-returned
loss0.369624hits. Nomination versus selection cannot be split by current API
output. Local-index loss is zero: every fetched row is scored.

**GO: freeze the two-bit nomination/centroid graph/physical SQ8 candidate.**
No architecture tweak, parameter sweep or paid scale run follows this result.
These are Rust-library physical plans plus its established Python scoring
mirror, not live Rust/S3 serving evidence. Neither vendor superiority nor a
production release is proven.

ReLAION root c4c2a37be1fd2065980f2e23f21b167384786a3f2317e848ca6f8a04cbec0506.
CoHere root b2420db4aab045979d191b68ab9b61b149b01b6d84782a0cbac0be8779224219.
Keep frozen inputs/raw samples/format/source hashes intact. Original Spark
ReLAION full jobs exit0:98.48s/51,060KiB planner and80.98s/222,488KiB scorer,
zero swaps and no S3 data reads. Whole offline job timing/RSS is not product
latency, QPS or memory. No new paid/cloud instance was launched.

Next single product gate: actual public Rust create/publish/open/search/reload
demo with authenticated bounded range reads and wrong/corrupt/conditional
failures, using this frozen format. Close raw-source SQ8 creation, serving
proof and API lifecycle gaps; no more research micro-gates. Remaining work
includes incremental IDs/mutations, in-process compaction/GC, CI/package
smoke, one fresh1M cold HTTP gate, then matched vendor/scale/lifecycle cost
evidence. The resident historical route stays a reference.
