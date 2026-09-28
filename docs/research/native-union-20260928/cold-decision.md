# Native v4 cold: both-corpus development GO, qualification open

Public source-only construction, authenticated publication/head read/remote open,
and callable Rust union search ran on real S3 using the exact compiled v4 binaries.
One balanced ABBA cell per corpus; two64-query repetitions per arm, no warm-ups.
Every native component/graph evaluation/nomination/normal-plan hash and ordered
returned ID reproduced the frozen closed development reference before accepting
measurements.512 actual native searches total;256 offline planner calls and256
cached-score sorts; zero new exhaustive SQ8 kernels. No query/GT in construction.

## Compact convergence row

All numbers below **verified**, first100k,D768,cosine,k100, consumed development
queries0–63. Timing/QPS are medians of two complete native repetitions per arm.
Nearest control duplicates its graph in the authenticated v4 second slot and
traverses once; union candidate uses both source graphs. Same binary/source/scorer,
physical planner/caps, image, instance and resource limits. Old v3 cold numbers
are historical references, not this cell's control.

| Dataset | Matched nearest recall@100 / p05 hits | Native union recall@100 / p05 hits | Actual quality delta | Exhaustive SQ8 recall | Native call p90/p95 control→union (ms) | Serial QPS control→union |
|---|---:|---:|---:|---:|---:|---:|
| ReLAION |99.156250% /97 (6346/6400)|99.421875% /99 (6363/6400)|+0.265625pp /17hits|99.515625%|106.728362/107.559806→108.488395/109.708413|9.458278→9.419019|
| CoHere |99.093750% /98 (6342/6400)|99.171875% /98 (6347/6400)|+0.078125pp /5hits|99.234375%|105.259743/107.101865→106.605597/107.813765|9.572112→9.504854|

Candidate p90/p95 is slightly slower, not a speed win. Both remain within the
predeclared250/400ms development envelope. Exhaustive deficits6hits/.09375pp R,
4hits/.0625pp C. BOTH vendors, incoming service HTTP, concurrent/sustained QPS,
total lifecycle dollars, validation, maintenance and100M remain unmeasured.
No production qualification/default freeze. Next decisive test: same-binary
ReLAION744 method-validation with matched nearest control and unchanged exhaustive
SQ8/GT gates; CoHere only after ReLAION survives.

## Complete repetitions and actual physical reads

p50/p90/p95/p99 are complete native library-call latency in milliseconds. Serial
QPS is64 divided by complete recorded loop wall time, including recording overhead.
No client SQ8 cache; resident router; HTTP connections reused within each process;
S3 server cache uncontrolled. Includes routing/nomination/fresh conditional S3
reads/authentication/native scoring; excludes incoming service request parsing and
response serialization. Data counters exclude metadata/credential/source requests.

| Dataset / arm / ABBA ordinal | p50 | p90 | p95 | p99 | Serial QPS | Data GET attempts | Verified data bytes | Process maxRSS KiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ReLAION control0 |103.445014|106.662595|107.349934|191.044350|9.323155|1615|1072206720|82616|
| ReLAION candidate1 |105.110121|108.766344|110.364050|115.645219|9.421803|1663|1072281600|83436|
| ReLAION candidate2 |104.893958|108.210447|109.052776|123.959390|9.416236|1663|1072281600|83056|
| ReLAION control3 |103.165502|106.794128|107.769679|120.228029|9.593401|1615|1072206720|81920|
| CoHere control0 |102.746118|105.237345|105.609116|175.185864|9.466545|1922|1072955520|80284|
| CoHere candidate1 |104.585052|106.397383|108.616924|119.194347|9.494188|1934|1072955520|81152|
| CoHere candidate2 |104.432543|106.813811|107.010605|114.920258|9.515519|1934|1072955520|80176|
| CoHere control3 |102.669987|105.282140|108.594614|115.076120|9.677680|1922|1072955520|79108|

All repetitions had zero failed data GETs; every query<=32GETs/16,773,120B.
Every repeated ordered-ID list equals the cached frozen sort and the other repeat
of its arm. Per-query CPU, startup and exact counters remain in immutable result
and JSONL receipts, not inferred from wall latency. Startup publication spans
6.961139–8.824282s, head read44.282286–49.817220ms and metadata open
564.919699–595.957871ms. Each process starts from a fresh head namespace; shared
immutable canonical/SQ8 objects retain actual HEAD ETag/length authority.

Whole preparation/native/parity controller174.65s, maxRSS480540KiB,0swap;
cgroup peak2114830336B includes page cache/kernel/build/source extraction/verification,
swap0B and OOM0. Native search-process RSS in table includes publication/open and
query lifetime; no isolated steady RSS or generation-swap claim.

## Authority, closure and collector repair

Source archive`76465d7227c5a8f8b8b1c6ade9b897e633fa208d16cd79db990c11377837f5a2`,
base`b8e44d31bc81f1ac5dd7b65dddf1cdf04136b89b`, terminal
`c419d609f2a7f50e90c08fd4e2d6eeb502ffb8f31d0a14768903c51c03ceac1e`.
AWS native session13847 closed; terminal statuscomplete/exit0. Spot instance
`i-054754088b5f77ee2` **terminated277s**, eu-central-1c c7g.2xlarge, AL2023ARM,
4threads/CPU0–3,8GiBcgroup/4GiBprocess/zeroSwap,80GiBencrypted disposableEBS.
Compute estimate**$.0139**, observed Spot quote×wall, excludesEBS/S3, not invoice
or lifecycle dollars. Reused exact verified green-a0003 binaries and2689pass full
assurance; all392 Rust/Cargo bytes matched their compiled source archive.
No Cargo/review rerun, overlapping copy or active compute remains.

Original local collector failed to create a parent directory for nested artifacts
AFTER reading the successful terminal and entered its termination finally block.
Fixed that shared write with mkdir(parents=True), then downloaded and hashed the
**same original closed** S3 roster; no new measurement or replacement launch.
The frozen archive/user-data preserve the original launcher; current launcher has
this collector-only fix. Independent verifier matched395 source files (392 native
plus controller/config/preregistration), hashed all60 artifacts, recomputed every
GT set count/cap/timing/QPS reduction, checked repeated IDs/plans and actualEC2
termination. `cold/a0001/verification.json` is the closed proof. Python syntax and
bash syntax checked; actual AWS execution is the controller's runnable check.

## Remaining problems / decisions

1. Development quality gain survives real serving, but higher routing work adds
   roughly1–2ms tail latency. Keep mechanism unchanged for validation; no tuning.
2. Historical nearest ReLAION256–999(744) result98.850806% vs exhaustive99.568548%
   remains KILL at its recorded old layout/archive. It is not a matched v4 control.
   Run both current v4 arms on744; require mean>=98%,p05>=95 and<=.5pp exhaustive
   deficit, fixed physical caps and stage decomposition; scientific failure ends arm.
3. All1000 existing queries were consumed. Fresh sealed qualification needs a new
   cohort; old method-validation survival cannot replace that gate.
4.100M phase-specific admission/steady and pinned RSS, scalable source build,
   incremental mutation/compaction/GC/recovery quality and economics remain open.
  39.5125GB resident arithmetic is a projection, not admission/RSS proof.
5. BOTH-vendor equivalent access/comparison and total lifecycle cost are missing;
   S3 Vectors metadata access only, Turbopuffer credentials absent. No operator
   decision is needed for the already-authorized next native validation gate.

Full product goal stays active. No architecture or reviewer cycle restarted.
