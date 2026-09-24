# V132 live S3 transport replay: 100k development decision

V132 completed one frozen Causality Spot cell on `c7i.8xlarge` in
`eu-central-1c`, instance `i-010abd437d618a341`, in 414 seconds. Its source
commit is `8f57a2d3faabd0353270d3b55850ae038b464e56` and source archive
SHA-256 is `6ca9957867febc227ac4bf635f83f99bf987eb593202947a094a252985f2b3a9`.
The complete exit-0 terminal is at
`s3://borsuk-bench-453182569524-euc1/research/v132-live-s3/8f57a2d3faabd0353270d3b55850ae038b464e56/runs/v132-20260924T063919Z/a0001/terminal.json`,
SHA-256 `a34f9db41ba64f5f2c77f18b914f0792966a148a7211b053daa8b5bcd706594d`.
The instance was independently observed **terminated** after the terminal.
All 12 terminal-listed artifacts (1,758,088 bytes) were independently read
back from S3 and matched their recorded byte counts and SHA-256 values.
The immutable generation-131 package was independently authenticated before
the run, as recorded in the [preregistration](v132-live-s3-transport-prereg.md).

Dataset: **deep-image-96-angular random 100k train subset**, with 1,000
already-used **publication-test ordinals 9000–9999** and GT100 within that
subset. This remains a development screen. The candidate is V122's frozen
513/1-vote physical plan; the baseline is its same-query capped control.
Both replay the same 512 nominee rows, returned SQ8 expansion, source-ID map,
and exact F32 source ranker. This measured path includes authenticated local
mirror and source reads plus fresh ETag-pinned, page-authenticated S3 range
GETs. It excludes router/planner execution, multi-query concurrency and
throughput. Arms execute in alternating order by query.

| Measured metric, 1,000 serial queries | Candidate | Paired capped control |
| --- | ---: | ---: |
| Exact-source Recall@100, hits / 100,000 | **99.942%, 99,942** | 98.827%, 98,827 |
| Complete replay p50 / p95 / p99, ms/query | 137.843 / **152.858** / 196.792 | 51.135 / **75.625** / 95.128 |
| S3 subphase p50 / p95 / p99, ms/query | 107.655 / 120.793 / 166.601 | 33.211 / 56.582 / 78.348 |
| Conditional range GET attempts | 1,000 | 24,674 |
| S3 response bytes total / mean per query | 9,428,126,976 / 9.428 MB | 1,535,701,248 / 1.536 MB |
| Authenticated local source bytes total | 23,653,893,440 | 23,443,671,808 |
| Authenticated local block reads total | 361,040 | 357,828 |

The candidate won 335 paired source-hit queries, tied 665, and lost none.
Its S3 response bytes/query had minimum 248,832, median 9,842,688, p95
10,772,352, maximum 10,800,000. The control's corresponding values were
248,832, 1,437,696, 2,865,024, and 3,760,128. The control made up to 32
parallel GETs per query; the candidate made one broad GET on every query.
All 2,000 arms respected the 32-GET and 16,777,216-byte/query caps. No
authentication or query error occurred. Startup staged 17 authenticated
inputs (69,718,777 downloaded bytes) in 8.128 seconds, outside the per-query
times; serving startup authentication took another 0.602 seconds.

The dedicated serving cgroup's charged memory peak, including its local page
cache, was **158,474,240 bytes** (151.1 MiB). The process had exited when
`memory.current` was captured at 84,885,504 bytes, mostly remaining cache.
This is a 100k transport-replay memory measurement, not a 1M/100M bound or a
compiler RSS figure.

Independent closeout parsed all 1,000 rows of `replay.jsonl` (SHA-256
`7c384220908df668d036cfccf24028df0600c76d7967cbb1633aed78e539e5ca`),
verified ordinals 0–999 and source ordinals 9000–9999, unique top-100 IDs,
per-query caps, summed hits/GETs/response and local bytes/block reads,
nearest-rank p50/p95/p99, and paired wins/ties/losses against `summary.json`
(SHA-256 `3018a0a0c200ffa8361f4d47270a215b731f399182f0d9ea5b4138ef3271f445`).
Every query in both arms had identical ordered source IDs, hit counts,
candidate counts, authenticated local bytes and block-read counts to V131's
independently verified sealed replay (SHA-256
`9c687edc8ea66a5ef41b1e83d1d51788022b58cf526f2e9dcdb21a38bb5dd7d9`).
Thus the live transport preserved V131's exact-source quality.

**Decision: reject this physical I/O schedule.** The preregistered viability
screen required candidate complete-replay p95 at most twice the same-run
control p95. The measured ratio was 152.858292 / 75.624654 = **2.0213**.
The candidate reads 6.14 times as many S3 response bytes per query on
average; its S3 p95 is 2.13 times the control, while the non-S3 p95 values
are 33.779 and 20.837 ms respectively. The causal mechanism indicated by
the measured path is the broad candidate SQ8 GET. The measurement does not
isolate transfer bandwidth from S3 response size, so that subcause remains
an inference. It does show that the live latency miss is in the transport
schedule rather than a changed source result or an authentication failure.

Next, derive a **generic** selective-page schedule under explicit capture,
GET-count, byte, and memory rules; do not tune a cutoff to these used queries.
Use frozen evidence to falsify the schedule cheaply, then preregister and
run one new live 100k Spot gate only if it predicts a materially smaller
byte footprint without losing the ≥99.5% Recall@100 source target. A passing
transport screen would permit a frozen end-to-end router/planner and
concurrent serving gate. Matched fresh deep-image and ReLAION-1M builds with
one layout policy must follow before 10M/100M or product-comparison claims.
No commercial baseline was measured in V132.
