# Counterbalanced GET32 versus GET64: 1M native real-S3 engineering screen

2026-10-16. Frozen Cohere Wikipedia multilingual-v3,1M/D1024/cosine/k10,
same1000 queries, source snapshot c3b0e81e93fdb06a13cc38a25868d75030a87e04.
Canonical source-equivalent increment12336e5ad08a171a0485d43a083881236ea3e81a.
This is not a matched vendor or physical-cold-service result.

Protocol was frozen before execution. Order A1,B1,B2,A2; A=SQ8 GET32,
B=SQ8 GET64. Common SQ8 byte cap32MiB,SOURCE128GET/64MiB,download64,
one active query/serial scoring,cache/window/split off,one_retry_100ms.
Same frozen native ELF, generation, inputs, query order and EC2 host;4CPU0-3,
8GiB/noSwap. No concurrent compilers/tests/measurements. Separate source editing
used CPU8 in another checkout. Library caches were off; physical S3 cache
state is uncontrolled. Each repetition starts a fresh process.

| Run | Recall@10 | Median ms | p90 ms | p95 ms | p99 ms | Serial QPS |
|---|---:|---:|---:|---:|---:|---:|
|A1 GET32|97.85%|149.027|198.844|213.489|252.802|6.394|
|B1 GET64|97.80%|146.069|171.088|179.685|214.041|6.712|
|B2 GET64|97.80%|150.067|179.345|200.469|329.262|6.361|
|A2 GET32|97.85%|149.026|200.908|216.110|300.609|6.312|
|A pooled2000|97.85%|149.027|200.290|215.253|272.787|6.353|
|B pooled2000|97.80%|147.798|174.841|186.855|269.335|6.532|

The pooled B/A p90 ratio is0.87294,p95 ratio0.86807,serial-QPS ratio1.02817.
Both B repeats have lower p90/p95 than either A repeat; throughput gain is
modest and variable. B2 p99 regresses versus both A repeats: do not hide this
behind the pooled p99. Only560/1000 corresponding queries are faster using
the mean of each arm's two repeats; mean paired delta is-4.314ms.
Two repeats and reused queries do not establish independent generalization.

Per repetition, SOURCE GET107882/27,428,966,400bytes is unchanged.
SQ8: A30033GET/10,625,249,152bytes; B52384GET/5,067,448,960bytes.
Thus total SOURCE+SQ8 requests rise16.2% and bytes fall14.6%.
The52.3% byte saving is SQ8-only. These are logical successful charges,
not wire or billed accounting. More requests can increase request cost.
This is a latency/Pareto point, not a universal efficiency win.

## Verification and disposition

Original pipeline SSM6ab2e381-5ee9-4649-ab68-5158f0425e03 completed0, all4 native
executions and strict readers0. No intermediate measurement rows were inspected.
Post-terminal reduction a0e4dae1-4752-4efa-9bdd-e563b5f9b821 completed0.
All4000 query ranks, scorebits, physical rows, plans and charges match their
frozen arm references; SOURCE/router charges and traces are invariant.
A repeats have9785 hits, B repeats9780 of10000; five-hit quality loss retained.
No host OOM or swap was observed. RSS is recorded per run in the JSON report;
systemd's printed peak can differ, so do not use its tiny summary value.

Retain GET64 as an opt-in tail-latency candidate. No production default changes.
Next: finish library-owned scheduler-memory admission, close original-f32
provenance and actual-input/canary gates for a matched S3 Vectors run, then
qualify competitor performance under disclosed equivalent conditions.
Turbopuffer has not been measured on this exact cohort. No vendor-win claim.

The initial decision document's proposal to port shortlist serving was
unnecessary: source inspection and actual-S3 parity prove it already exists.
Only the explicit GET limit changed for these runs; no planner port occurred.

## Frozen evidence

Protocol SHA c610cce74b40c5f2334d0323ffd2a2841476e0147194221d88ef2007352b35f1.
Native ELF SHA0c46d8265180e5645d5a9413ff6dec76b38711c558a2d23eb8eb74007f93d8d2.
A config94a2586514aa346d8905b2e7f2e515bba3de71cc6df01804b464064347296770.
B configdf08b3f1232ea8f5e28c0a82c22ef8a11c2efc52a0730f3d6a39adba7b466927.
Verifier script SHA e7197f952ab43ace90498901e2c3b42b0e48dfa03cc4630e02cb0c9f6e87cd7e.
Paired summary SHA57fbd44a0ac6c89305fd7d7cf7059491b3028a1d3ba4f576166024578bebac47.
S3 bucket borsuk-bench-453182569524-euc1, prefix
research/semantic-router/20261016/persistent-rust-workspace-a0001/:
get64-abba-a0001/frozen.tar.gz (SHAe8158222fa470a7866c378bd7c97e90ec56ef2a7af84f026070e68bb95fdaed7);
per-run abba-get64-{A1,B1,B2,A2}-a0001/;
get64-abba-closed-a0001/{evidence.tar.gz,paired-summary.json,terminal.json}.
