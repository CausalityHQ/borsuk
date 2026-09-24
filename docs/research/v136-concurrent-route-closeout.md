# V136 shared-generation concurrent local route closeout

**Decision: pass the preregistered local C1/C8/C32 screen.** One authenticated
V131 serving generation was shared by all request threads. All 24,000 timed
complete candidate requests preserved V135's exact top-100 source-ID order
and hit count. This proves neither live S3 throughput nor scaling beyond the
100k corpus and 10.8-MB SQ8 object.

## Attempts and authentication

Attempt a0001 was a transport-only failure before any science; see
`v136-a0001-bootstrap-failure.md`. The corrected a0002 source commit is
`af689bab9e441aa8d7c4cced218860b02bfbae6a`, complete archive SHA-256
`c4a8bef23eb42f48e6b4ca0e916932fc40182f539a953688677e28c79d1e6cc5`
(11,673,626 bytes). Its terminal is
`s3://borsuk-bench-453182569524-euc1/research/v136-concurrent-route/af689bab9e441aa8d7c4cced218860b02bfbae6a/runs/v136-20260924T081400Z/a0002/terminal.json`,
SHA-256 `a2eef92c474acc2c6d7b2c836ceb7473f43aec10383bfb1544758688b8d952c2`.
The one Causality `c7i.8xlarge` Spot worker `i-0859671215ea6a882`
completed with exit 0 in 631 seconds, and the original launcher independently
observed it **terminated**. All 12 terminal-listed artifacts were downloaded
and verified by byte length and SHA-256. Summary SHA-256 is
`e614fa1361311189331ffffae2ab8934bd468eee4b1feb63f00e1400925c5b1d`;
24,000-row replay SHA-256 is
`85b2dedf15ddfa0389cba5556fcdb7b2b796d866cad5bf482711a43164c7d484`.

The dataset is the **deep-image-96-angular random 100k train subset**,
already-used **publication-test ordinals 9000–9999** with GT100 within the
subset. The fixed generation manifest SHA-256 is
`968ef7d795b53e5869400998ca19c6ab20d9b39830295fac3081c419f63f0f20`.
One untimed verified pass warmed the page cache. Each timed cell made eight
copies of every query through production Rust PQ64 routing, exact nominee
score, weighted planner, authenticated local SQ8 read, returned SQ8 score,
ID resolution and exact-source rank, in C1 then C8 then C32 order. There
was no concurrent control arm; V135's serial candidate is historical
reference rather than a same-run competitor.

## Measured result

| Concurrent requests | Completed requests | Throughput, queries/s | Complete-route p50 / p95 / p99, ms/query | Charged peak |
| ---: | ---: | ---: | ---: | ---: |
| C1 | 8,000 | 23.736 | 43.225 / 46.390 / 47.743 | shared below |
| C8 | 8,000 | 180.676 | 45.433 / 48.519 / 49.597 | shared below |
| C32 | 8,000 | **492.501** | 66.412 / 72.634 / 74.547 | shared below |

The instance's serving cgroup peak across the complete evaluation, including
file cache, was **990,969,856 bytes** (945.06 MiB), below the registered
4,294,967,296-byte cap. C8 and C32 throughput were 7.612× and 20.749×
C1; their p95s were 1.046× and 1.566× C1. C1 p95 was below the unchanged
75.624654-ms serial gate. Thus every preregistered throughput, p95 and
charged-memory threshold passed. Startup input preparation took its own
authenticated stage; in-process generation authentication took 0.335 s.

Independent recount checked each cell's 8,000 ordinals, source-query mapping,
GT hits, range count and bytes, exact nearest-rank p50/p95/p99, and QPS from
wall nanoseconds. Each cell returned 799,536 GT hits over 800,000 possible
positions, exactly eight copies of V135's 99,942/100,000 hits. Every top-100
source-ID order and candidate count matched the authenticated V135 row for
that query. Every request had one range within the 16,777,216-byte budget,
zero nominee or primary order differences, and complete authenticated local
reads. Each cell read 75,425,015,808 SQ8 bytes across its 8,000 requests.

## Scope and next gate

The result establishes a viable shared Rust generation and thread-safe local
read path on this one 100k layout. It measures warm-cache service on one
32-vCPU Spot worker; it excludes cold-start throughput, remote network
requests, multi-generation overlap, updates, and larger SQ8 objects. V135's
fixed capped control was faster at C1 (23.108-ms p95 versus the candidate's
48.006-ms serial p95) with lower 98.827% exact-source recall; V136 did not
rebenchmark that control concurrently.

The existing V132 live-S3 replay already fetched this same V122 sealed
candidate/control range schedule, on this generation and query cohort:
candidate/control p95 was 152.858/75.625 ms. V135 verified all dynamically
planned ranges equal those sealed ranges. That earlier result excluded online
router/planner work and belongs to its historical source revision, so it
cannot be spliced into V136 timing as a new measurement. Repeating the
unchanged S3 schedule would not test a material I/O revision. The next
transport gate must first develop a **generic selective range schedule**
that cuts response bytes while retaining the ≥99.5% exact-source target on
reused development data, then preregister a fresh paired live-S3 cell.
Separately replace the flat PQ64 router's constant-fraction row scan and
the constrained weighted-planner path before matched fresh ReLAION/deep-image
1M gates. No 10M/100M or commercial performance claim follows from this
100k local result. Memory policy should scale smoothly with vector count and
requested recall, without corpus-specific branches.
