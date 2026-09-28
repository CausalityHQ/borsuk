# Decision: GO for native integration, not qualification

Both fixed development gates passed. This is one nomination-only causal screen:
combine the already-closed nearest/diverse source graph discoveries, then score
and admit once using the unchanged native two-bit scorer and sparse planner.
Diverse alone remains KILL. No direct union fetch, new graph traversal, full SQ8
kernel, validation, cold serving or scale experiment ran in this screen.

## Compact convergence row

Verified first100k, D768, cosine, k100, consumed development queries0–63:

| Dataset | Nearest returned baseline | Union returned | Actual delta | Exhaustive SQ8 | Union p05 hits/100 | Decision |
|---|---:|---:|---:|---:|---:|---|
| ReLAION |6346/6400=99.15625%|6363/6400=99.421875%|+17 hits/+0.265625pp|6369/6400=99.515625%|99|GO|
| CoHere |6342/6400=99.09375%|6347/6400=99.171875%|+5 hits/+0.078125pp|6351/6400=99.234375%|98|GO|

Every control plan, ranked candidate order, fetched/returned/exhaustive count and
GT stage set reproduced the frozen topology-a0003 nearest control. The exhaustive
baseline is cached SQ8 over all100k rows, not V282's bounded flat centroid router.
All gates unchanged: positive discovery gain; fetched>=6379/6392; returned>=6346/
6342 with no control regression; exhaustive-returned<=32/6400; p05>=95; each
query<=32 ranges and16,773,120B. All passed on both datasets.

| Dataset | Nearest discovery/fetched hits | Union discovery/fetched hits | Nomination wall p90/p95 control→union (ms) | Logical ranges control→union total/64 | Planned bytes control→union total/64 |
|---|---:|---:|---|---|---|
| ReLAION |6372/6372|6392/6392|6.629/6.651→7.121/7.173|1615→1663|1,072,206,720→1,072,281,600|
| CoHere |6387/6385|6395/6393|6.513/6.543→6.827/6.925|1922→1934|1,072,955,520→1,072,955,520|

These timings exclude shared query preparation, cached graph discovery, S3 and
SQ8 scoring. Logical ranges are plans, not new physical HTTP attempts. No cold
latency or QPS improvement is inferred from this table. The latest measured
native S3 nearest baseline remains **ReLAION dev0–63 p90/p95 107.846593/108.673862ms,
serial9.269621QPS**, complete library call with resident router, no client SQ8
cache, reused connections, uncontrolled S3 server cache. It excludes incoming
service HTTP handling. No candidate cold number and no CoHere cold number exist.
No matched S3 Vectors or Turbopuffer comparison or lifecycle-cost result exists.

## Causal decomposition

ReLAION gains20 candidate GT hits without losses; all20 survive nomination and
physical coverage. Returned gains20/loses3, net17. Six hits remain below cached
exhaustive. CoHere gains8 discovery GT hits without losses; all8 survive
nomination and physical coverage. Two GT hits still lost at nomination, unchanged;
returned gains8/loses3, net5. Four hits remain below cached exhaustive. SQ8 ranking
changes the top100 as coverage expands; gained discovery is not itself returned
recall. Distinct graph discovery union fixes the diverse-alone CoHere coverage
regression on this consumed split.

Historical **nearest ReLAION method-validation queries256–999 (744)** still
98.850806% versus exhaustive99.568548%, gap0.717742pp and KILL. That is verified
historical evidence, not this candidate's matched validation result. All1000
queries on each corpus have been consumed; fresh sealed qualification is absent.

## Closed evidence and assurance

GREEN a0001 source archive
`18741a7b22c8e49e15fbf3f6e7804c703edd403942e59769546cf179b7354527`,
terminal`fb39f11ff0efa9f99439b1f167f32ae0f3985be67093c6a71e99a82340d2be55`.
AWS native session91859 closed0. Instance`i-01b4445af3dee632b` **terminated**,439s;
compute estimate$.0219 excludes EBS/S3, not invoice or lifecycle cost.
21 artifacts hashed,397 relevant source files matched, actual AWS termination
checked independently. AWS release compilation and focused standalone union unit
GREEN. Original RED session93828 expected unit failure; four artifacts verified,
instance`i-07f38ca9b6db14bb8` terminated91s, compute estimate$.0046 exclEBS/S3.

128 native nomination calls and64 shared query preparations per corpus; zero
new graph queries and zero new exhaustive SQ8 kernels.256 cached score sorts per
corpus. Ordered IDs obtained on AWS using the same frozen scores/SQ8 IDs and tie
order. Independent local verification performs only authenticated closed metadata,
GT-set/count and clock reductions; no queries or scoring run locally.
Native replay maxRSS87,256KiB ReLAION/69,576KiB CoHere, both0swap. Screen cgroup
peak450,494,464B includes file cache/kernel and download/parity work,0swap/OOM;
these are diagnostic process figures, not integrated two-graph serving RSS.

Production Rust library, dependencies, persistent format and defaults unchanged.
Reuse prior full assurance2684pass/0fail/26existingignored/147targets; this slice
adds a diagnostic bin, controller and receipts only, checked by focused unit and
actual paired baseline parity. No duplicate full assurance cloud campaign ran.

## Ordered next gates

1. Integrate this exact mechanism into callable object-native Rust with one shared
   plane/centroid/page authority and two authenticated source-built graphs. Extend
   root/build/publication/reload/compaction coherently with a new format marker;
   reject old experimental roots clearly. Charge both graph discovery budgets and
   resident/pinned memory. No redundant planes, legacy format reader or serving
   selector based on query/truth.
2. Focused identity, corruption, budget, build/publication/restart/compaction checks;
   once core diff stable, one full assurance gate because this changes core code.
   Native development plans/ordered IDs must reproduce these frozen union results
   and the original nearest control before publishing performance evidence.
3. Runnable client-cold native S3 end-to-end matched control/candidate measurement
   on ReLAION first, CoHere second, including complete traversal CPU, physical
   attempts/bytes/errors, p90/p95, serial throughput and admitted/RSS resources.
   The frozen native cold baseline can be reference evidence; a changed binary
   requires an actual matched control, not a claimed paired comparison to stale
   timing. Gate quality remains fixed; candidate validation then fresh qualification.
4. Only surviving native/quality gates advance to1M,10M,100M and equivalent vendor
   comparison with full lifecycle economics, maintenance and generation swaps.

Remaining BOTH-vendor gap: candidate native integration and cold performance,
method-validation/fresh quality, concurrent QPS/lifecycle cost, incremental upkeep,
100M measured fit and equivalent vendor runs. Two-graph worst-case payload39.5125GB
at100M is arithmetic only;48GiB steady/96GiB pinned/192GiB build targets on256GiB
host,1TiB scratch/12h32CPU remain unqualified. S3 Vectors metadata access succeeded
with0 buckets; no index/query permission inferred. No Turbopuffer credentials are
currently visible. These are future vendor-test dependencies, not a reason to
stall native integration. No operator decision is needed now; product goal active.
