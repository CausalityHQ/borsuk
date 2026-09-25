# V213 resident vector graph 100k closeout

**Decision: reject FP16-scored graph navigation at the frozen gate.**
The source-built graph reaches paired quality only with too many dense
vector scores. Do not promote this method to 1M or tune its ef/degree
further on the used panel. The next material format candidate is a
persisted source graph with compact PQ64 navigation codes and a bounded
FP16 final rerank, built in a separate process so builder allocations
do not remain in serving RSS. Falsify that combination on 100k before
another 1M comparison.

The sole complete Causality c7i.4xlarge Spot attempt was `a0001`,
instance `i-0598b6d50e9124454`, now terminated. Source commit
`fc61a4e3bb41c082833507e55f019b4192d83a25`, archive SHA-256
`0345d9e07f955dd329942f444440a7c41a93bb0700fab23a1feed31b38baf49e`.
The terminal SHA-256 is
`3567b82ff494f934f7071db6a191fb53898c25f47a6d8c1e2a61611d214bf83e`
at `s3://borsuk-bench-453182569524-euc1/research/v213-resident-graph-100k/fc61a4e3bb41c082833507e55f019b4192d83a25/runs/a0001/terminal.json`.
The terminal reports `complete`, exit 0; all 16 artifact lengths and
SHA-256 hashes passed independent S3 readback. Raw returned IDs were
sealed before downloading truth and the V193 baseline.

Dataset/split: **ReLAION-100k D768, already-used development queries
0–255**, GT100. The paired V193 full-rank SQ8 baseline has
**25,440/25,600 GT hits** on these exact queries. Construction used
source vectors only, cosine HNSW M=32, base degree=64,
ef_construction=128; the four ef settings were preregistered. All
latencies are whole in-process sequential Rust graph search plus
resident FP16 scoring and stable top-100 selection. They exclude
networking, concurrent admission, mutation, generation swap, and S3
object transport; they are **not product request latency**.

| ef | GT100 hits /25,600 | p05 hits/query | p50 / p95 / p99 (ms) | p95 base visits | sequential QPS | Gate |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 256 | 25,371 | 96 | 5.796 / 10.185 / 12.109 | 5,384 | 164.72 | fail quality and p95 |
| 512 | 25,422 | 97 | 9.501 / 16.414 / 18.784 | 8,789 | 101.26 | fail quality and p95 |
| 1024 | 25,455 | 97 | 15.819 / 26.512 / 29.821 | 14,200 | 60.67 | fail p05 and p95 |
| 2048 | 25,471 | 98 | 28.134 / 42.593 / 47.582 | 22,393 | 36.26 | fail p95 |

The frozen gate required hits≥25,440, p05≥98, whole p95≤10 ms,
and zero vector-body GETs. All arms made zero vector-body GETs;
none passed. The ef=2048 quality arm needs about 22,393 base-layer
FP16 row scores at p95, plus upper-layer work. At D768 those dense
scores dominate the tail. Dropping ef lowers work but loses tail
quality. This is a structural work/quality conflict, not a transport
or planner-only effect.

Build was **188.388 s** within the Rust process; peak RSS was
**839,872,512 bytes**. The graph owned **55,198,760 bytes** of
adjacency allocations and the plane **154,400,000 bytes**. After
dropping source vectors, measured process RSS remained
**833,736,704 bytes**, consistent with builder heap retention; this
is a measured serving-memory problem for this process arrangement.
Cold FP16 hydration was **0.224 s**. The four-arm 1,024-search loop
took **15.375 s**. Source preparation peaked at 1,785,480 KiB RSS
in Python, excluded from serving.

The instance launched 2026-09-25 08:56:05 UTC; the terminal landed
09:03:35 UTC, so launch-to-terminal elapsed was 450 s. EC2 Spot
history for eu-central-1c gave **$0.3644/hour** effective at launch;
the compute-only launch-to-terminal estimate is **$0.0456**, excluding
termination tail, EBS, S3 and any taxes. This is an estimate, not a
billing measurement.

V213 neither proves a 100M latency nor compares against Turbopuffer
or S3 Vectors. The next gate must test one generic compact-navigation
format against the same 100k quality/latency criteria, then confirm a
pass on fresh queries before 1M end-to-end serving and matched external
competitor measurements.
