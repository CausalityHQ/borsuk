# Closed-loop ordered-batch concurrency falsifier

Base: 3698e785cd72820493a2dfcf1fd85d7d74bdb13c. Existing public
TwoBitGenerationLimits.max_active_queries already supports 1 and 2. Library,
scoring, layout, routing, retries, per-query budgets and generation pins are unchanged.

Opt in with config schema borsuk-cohere-native-baseline-config-v16 and explicit
max_active_queries: 1 or 2. Result schema is borsuk-cohere-native-baseline-result-v17.
Use execution full, baseline serving, fetch_parallelism64, offload2, no split or
byte-window intervention. Production uses Scale1m, fixed40 and exactly 2 GiB
modeled cap. Both arms use the same binary and current_thread Tokio runtime: cooperative
query overlap on one runtime thread, plus the shared Offload2 workers. A negative
result applies only to this closed-loop cooperative schedule, not to all library
concurrency. No new thread-CPU instrumentation is introduced. Each arm
shares one generation, client, process and shared two-worker/two-slot SOURCE pool.
This is bounded closed-loop ordered batches, not rolling scheduling or service-peak QPS.
A 32-request prefix is diagnostic; it is not population-tail/full1000 evidence.

Bounds: query slots1/2; SOURCE/SQ8 downloads per query64 and global bound64/128
(the phases do not overlap within one query); compute slots/workers GLOBAL2.
Existing generation admission multiplies source/SQ8 buffers, scratch and scheduler
state by active queries. Runner multiplies retained results/plans/traces/query rows,
reserves 512 KiB per unpolled search future (checks its concrete size +4096 before
polling) and 1 MiB for retained cohort/terminal JSON. No capacity widening. Startup
reports modeled_memory_bound_bytes (cap, not exact occupancy), caller bytes,
scheduler bytes, authentication bytes, and external_memory_limit_bytes8GiB.
The 8GiB field is a prospective external resource gate, not an observed measurement.

For v17 query rows, query_process_cpu_ns and transport MUST be null. query_wall_ns
remains per-query latency. After all query rows and before all_queries_sealed,
exactly one query_cohort carries attempted_count, successful_count,
max_active_queries, elapsed_ns, process_cpu_ns, process_cpu_start_ns, process_cpu_end_ns,
sum_query_wall_ns, qps, transport,
truth_opened:false. elapsed includes ordered-batch scheduling and query-record
emission; process CPU and transport are captured ONCE around that cohort.
qps = successful_count * 1e9 / elapsed_ns. sum_query_wall_ns is distinct and may
exceed elapsed_ns. The terminal repeats the identical sealed query_cohort;
query_process_cpu_ns is its process CPU delta and query_wall_ns its sum of walls.
The completed reader removes sequential_qps and emits the explicit cohort instead.
It rejects missing/extra cohort fields, wrong counts/bounds/QPS, non-null per-query
process counters, and inconsistent terminal duplicates. Original bytes, full SHA,
EOF, counts, seal-before-truth, provenance and immutable header pins remain enforced.
Historical serial schemas retain their original output and counter meanings.

Join-all drains every started sibling before ordered output/error selection. A
failed query cannot cancel another started query. The first ordinal error is
selected, successful siblings and their charges are retained, later batches stop,
and no seal/truth follows a failed cohort.

Root next gate: freeze the repaired qualified source and NEW ELF hashes (never
label old a046/3e86 runtime as this binary). The initial first32 proposal is
prospectively superseded by Full1000 reuse: current Full prefixes require a
separately authenticated native prefix-cohort receipt, and regeneration is not
cheap. Do not truncate files and forge a receipt. There is no admission-only CLI.
Root owns actual-input model/resource admission, separate smoke and the eventual
Full1000 Q1/Q2 ABBA descriptive repeated-cohort Pareto screen. Keep exact
generation/requests/truth and external8GiB controls; compare exact IDs, scorebits,
plans and charges before throughput interpretation. Require an artifact-side Q2
overlap witness sum_query_wall_ns > elapsed_ns; if absent report overlap not
established, not a negative concurrency result. No significance, variance, cold,
vendor, or arbitrary-threshold claims. Connection establishment is included;
each cell is a fresh process and backend/cache state remains unknown. No prewarm.
Historical fixed40 reference: 9737hits,127811logicalGET,34229466368logicalB.
No concurrent performance claim is made by this source qualification.

Qualification at source freeze: focused a0002 affected-2 exits0 with runner3/3,
reader4/4 (including included runner cases). After that pass, production-only
fixed1M validation and a pure config-refusal test were added; the exact frozen
source is undergoing the full runner/reader/library/release/Clippy/testbuild
gates. This source freeze is UNVERIFIED until their terminal receipt is collected.
Evidence: /mnt/borsuk-workspace/evidence/concurrent-query-qualification-a0002.
Original failed setup/compile attempts remain in qualification-a0001; no real-data
runs were performed. The new controller started only after original terminal1.

Review repairs: cohort elapsed must cover the sum of each ordered batch's maximum
query wall. The reader rejects a coherently resealed 90ms/10ms pair claiming50ms
elapsed/QPS40, including an updated terminal copy. Production small-population
headers refuse; fixture bypass is test-only. Runner-level overlap holds both
payload GETs at zero permits until both actually enter. Caller memory boundaries
use the real concurrent config and both Q2 slots must be regained after cancellation.
