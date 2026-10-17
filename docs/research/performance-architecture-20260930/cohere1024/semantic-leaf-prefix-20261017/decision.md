# Semantic leaf-prefix quality/read screen
Frozen native source: a0463ed097296c19539d05529ddad615be760fdd. Cohere 1M, D1024, cosine, k=10, fixed 1000 requests. Same authenticated index, source-cache off, Inline authentication, fetch64 and unchanged scoring/shortlist/SQ8 cover.

| Leaf prefix | Recall@10 | Logical verified bytes | Logical GETs |
|---|---:|---:|---:|
|48 incumbent|97.83%|39,690,200,320|139,913|
|44 research|97.63%|37,001,703,424|134,563|
|40 research|97.37%|34,229,466,368|127,811|

Retain 48 as default; retain 44 and 40 as research Pareto alternatives. The 44 prefix sacrifices 0.20 percentage points for 6.77% fewer bytes and 3.82% fewer GETs. The 40 prefix sacrifices 0.46 points for 13.76% fewer bytes and 8.65% fewer GETs. These are measured tradeoffs, not quality improvements.

All 1000 incumbent queries reproduce historical ordered results, plans, traces and logical charges. All three current full files passed native completed-scale validation. Runtime admission and separate real-S3 disposable smoke passed for each arm before its full run. No underfill or query failures.

Qualified source passed 21 individually selected exact tests, 208 affected test executions with two ignored, release builds, workspace all-target correctness/suspicious Clippy, and the actual shim-unset workspace test-build. Counts overlap; this is not a full workspace runtime suite. Initial compile INVALID (two missing fixture defaults) is preserved; root's two-line fixture integration repaired it without changing production behavior.

Timings are descriptive: one 48-then-40 screen and a prospective 44 follow-up, not counterbalanced. Observed p95 was 212.81/202.79/195.93 ms for 48/44/40. The 40 p99 worsened and the 44 process peak increased. No formal cold, concurrent throughput, equal-quality or competitor win is established.

Next distinct experiment: preregister a counterbalanced 48/44/44/48 latency comparison using these unchanged qualified binaries and inputs. Reuse correctness evidence; report recall and read costs alongside latency. Do not promote a new default from this screen.
