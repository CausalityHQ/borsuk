# Charge returned-row ranking and concurrent planner payload

Baseefead3ad. Traced both generation loaders to sq8_s3_range::rank_verified...
and returned_sq8::rank_returned_ranges / exact_sq8_nominee::score_nominees.
Current response model3*max_query_bytes excludes global/local score vectors,
ordinal roster, three duplicate/membership HashSets and stable-sort workspace.
For narrow dimensions these row-count allocations can dominate response bytes.
ObjectNativeGeneration additionally adds planner_bytes only once outside the
max_active_queries multiplier, although planners execute concurrently.

Fix one shared checked per-query payload estimator, used by BOTH loaders:
3*wire cap + planner/scratch + conservative256B per maximum returned row
(min(object rows,wire cap/(D+12))) +4D coefficient weights. The256B row allowance
covers score Vec growth, local/global scores, ordinal roster, membership sets
and sorting; allocator/runtime/transport extra remains explicitly excluded.
Multiply the entire query allowance, including planner, by admitted concurrency.
TwoBit retains existing1MiB planner and codec scratch charge. No query algorithm,
ID validation, byte/GET cap, persistent format or API compatibility branch changes.

Small red/green regression models1M D1 rows versus response-only charge; tests
also verify row cap, planner contribution and overflow/invalid rejection. Run
returned_sq8 tests, existing ObjectNativeGeneration fixture and current two-bit
integration on original single Spark job(s), no local build/full suite/cloud.
Docs state this is a modeled admission fix, not measured RSS, latency or vendor
win. Source and exact exit receipts retained, coherent code slice pushed.

Related traced result-lifetime issue: scores.truncate(top_k) retains the full
fetched-row Vec capacity after the query slot is released. Return a fresh
copy of the top-k slice, dropping scratch before return. Existing tie/ordinal
test additionally asserts result capacity equals top_k; direct rustc std-only
harness compiles exact production modules for cheap red/green verification.
No scoring/order/ID-validation change and no Cargo rerun merely to duplicate
unchanged loader admission evidence.
