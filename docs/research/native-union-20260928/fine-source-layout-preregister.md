# Finer groups, fixed sample: one paired falsifier

## Launch authority

Launch only after `fine-source-check/a0002` has closed with actual instance
termination and independently verified asserted RED, constructor and recipe
GREEN, exact compiled authority for all 393 native files, and 2688 top-level
passes / 0 failures / 26 existing ignores. The full gate must retain the same
145 Cargo targets: 144 test harnesses and one benchmark with 12 smoke checks.
Adopt only the two exact authenticated source transformations. Reuse the
retained generation builder, native demo, and SQ8 helper; this measurement
cell runs no build or full assurance gate.

## One causal change and paired authority

The candidate uses ceil(N/256) source groups instead of the killed v1 arm's
ceil(N/1024). Hold the original global sample reservoir at
min(N, ceil(N/1024) * 64), seed 8201, 12 training iterations, source assignment
HNSW, radius and ordinal ordering, and the 1024-row extent ceiling unchanged.
The degree-32 successor cycle is identical to the killed candidate. Native
query, scorer, discovery work, nomination, and physical caps remain fixed.

`fine-source-config.json` copies the original immutable source, control,
request, truth, scorer, and cache identities, adding only the v2 recipe
marker. The controller compares the recipe against this explicit field.
Historical v1 artifacts and their archived code remain immutable.

Before quality, verify original source identity, order permutation, native
recipe, coefficient f32 bits, every per-ID SQ8 and two-bit payload, source
mean, and actual SQ8 object HEAD. Construction sees no queries or truth.
The current compiled binary queries the authenticated old flat-union control;
all control discoveries, plans, and quality must replay exactly. Historical
reference numbers alone cannot satisfy matched-control authority.

## Fixed scientific gates

Use first-100k D768 cosine k100, consumed development query ordinals 0–63.
Run ReLAION first; run CoHere only if ReLAION passes. Require the complete
128-plan paired roster per corpus, with 128 seed evaluations and 1272 walk
evaluations per graph, 159 pages per graph and at most 318 union pages.
Keep source two-bit nomination and the 32 GET / 16,773,120 byte caps.

The candidate must meet all of:

- Mean recall@100 at least 98%; p05 at least 95%.
- Exhaustive SQ8 deficit at most 0.5 percentage points.
- Aggregate nonregression against the matched flat-union control: 6363 hits
  for ReLAION and 6347 for CoHere, out of 6400. Exhaustive SQ8 has 6369 and
  6351 hits respectively.
- All fixed physical work caps.

A quality KILL ends this exact finer-group arm and skips remaining corpus,
cold, and scale work. Decompose loss into walk-page exposure, centroid
roster, nomination, physical containment, and exhaustive-flat GT ranking.
Do not sweep, increase work, or weaken gates.

## Conditional native cold measurement

Only after quality GO, run existing native client-cold ABBA:
control, candidate, candidate, control; 64 calls per repetition. Require
exact IDs, ranges, and actual counters, zero failed GETs, and candidate
median p90 at most 250 ms / p95 at most 400 ms. Sync every completed
repetition to S3 before starting the next.

The router is resident, client SQ8 cache absent, valid warmups absent,
connections reused, and S3 server cache uncontrolled. Timing covers the
complete native library call and excludes incoming HTTP. Report actual
startup, RSS, tails, and serial observed QPS; do not transfer old HTTP timing
or claim sustained throughput, vendor, or lifecycle superiority. Fresh
sealed cohorts remain unopened.

## Resources and terminal safety

Use one Causality eu-central-1c c7g.2xlarge Spot instance, four threads on
CPU 0–3, 80 GiB encrypted disposable EBS, 900-second worker cap,
600-second measurement cap, 300-second native phase cap, 8 GiB cgroup,
4 GiB process address space, and zero swap. Spot quote must be at most
$0.30/hour; compute cap $0.075 plus $0.10 EBS/S3 allowance.

Assert idempotency token length at most 64 before registration. Preserve
immutable reservation, source, config, and prefix; use the shared lock and
active-tag guard. Observe only the original native session, terminal marker,
and infrastructure health while active. Never inspect incomplete measurement
logs or CSV files. Require nonempty decision, cgroup, ReLAION result, and
resource artifacts before producer completion; independent verification
rejects their absence regardless of reported terminal success. Sync terminal
artifacts and terminate compute immediately. No automatic replacement.
Preserve INVALID evidence before an explicit correction.

A GO advances only development source/build/native-cold qualification.
Fresh 1M HTTP, maintenance, both vendors, 100M phase/pin RAM, lifecycle cost,
and recovery remain mandatory. Do not freeze production defaults.
