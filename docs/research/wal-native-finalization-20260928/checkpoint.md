# Current production checkpoint (2026-09-28)

Goal remains active: a self-contained Rust object-native ANN beating BOTH S3 Vectors and Turbopuffer on paired quality, cold end-to-end latency and total lifecycle dollars. No vendor win or default freeze is qualified.

## Closed evidence

Callable epoch-fenced compaction/GC is delivered with restart, stale-generation, delayed-delete and live S3 checks. All57 original library failures were repaired in verified slices. DecoderV36:63passed. Native bulk finalization guard red27pass/2expectedfailures -> greenWAL29passed; stronger specific-error/no-append/authority assertions pass. PortablePQ4:17passed; FMA:7passed. HistoricalARM-onlyV26 tests now assert unavailable/no-output on x86, retaining original ARM positive assertions.

Latest frozen-source full workspace/all-targets gate passed on Causality x86 Spot:2682passed/0failed/26ignored across146target summaries, excluding the initial focusedV26 87pass repetition. Sourcecf329c81b6518d7997b3c1dd7832a08bd0b06dfa0358941c64707c496684a685, workeri-0128209956ceb8d2e terminated, all terminal artifact/source identities independently verified. ARM positive runner execution remains unmeasured. See ../v26-platform-assurance-20260928/counts.json and decision.md.

## Latest quality (verified offline, not serving performance)

All corpora are first100k rows, D768 cosine k100; splits are reused/consumed query identities. Baseline is paired exhaustive nativeSQ8 scoring over identical rows. Mean and p05 are recall@100 percent (100 hits =100%).

| Dataset / split | Returned mean / p05 | Paired flat mean / p05 | Status |
|---|---:|---:|---|
| ReLAION development0–63,64queries |99.15625 /97|99.515625 /99|Short development pass only|
| CoHere development0–63,64queries |99.09375 /98|99.234375 /98|Short development pass only|
| ReLAION validation256–999,744queries |98.850806 /97|99.568548 /99|KILL:0.717742pp deficit exceeds0.5pp|
| CoHere validation256–999 |Unmeasured|Unmeasured|Stopped at first failure|

Plans stayed within32logicalranges and16,773,120bytes/query. Physical GETs/retries, current coldHTTPp90/p95(ms), QPS, RSS and total lifecycle$/query are unmeasured. Older resident performance is stale for this architecture; no comparison with either vendor is measured. Test runtimes are correctness durations and never query latency.

## Problems and decisions

1. Shared claim-free guard omitted current native authority. Fix once before authoritative transaction creation and inside transaction; supportedD64SquaredEuclidean fixtures exercise both fresh and stale handles. Unsupported segment-only finish retains documented fallback. Do not restoreV20 default.
2. Production bounded builder accepts SquaredEuclidean D>=64; normalized-cosine quality research does not qualify direct productionCosine support.
3. Native source-fit layout fails paired quality. Keep KILL; no scale/cloud promotion. Closed64-query ReLAION trace attributes observed candidate/fetched GT equality to discovery on that reused development panel only; validation decomposition is unmeasured. Fable graph-cut/adjacency proposal remains unqualified: decoded centroids alone project96.125B/row, not advertised48storedB/row; scalable build/local maintenance are unproved. Separate layout from routing causal changes and identify a genuinely fresh sealed query panel.
4. No operational blocker or operator decision needed. AWS-only causality Spot. Latest WAL correctness worker terminated with authenticated terminal; no active build/research/benchmark. Historical stopped resources remain untouched.

## Ordered gates

1. Assurance gate complete; commit the final platform fixture and authenticated full-gate receipt, then verify fast-forward delivery to origin/main.
2. Predeclare feasible100M decoded RAM/generation-swap, build and maintenance cost, GET/byte and latency envelope; choose one causal development falsifier with actual SQ8 returned quality and loss decomposition. Research proposals and arithmetic do not substitute for measured RSS or quality.
3. Only a qualified paired100k winner proceeds to fresh1McoldHTTP,10M/100M and both matched vendor comparisons. Record total lifecycle costs and failure recovery; preserve immutableV282 evidence.
