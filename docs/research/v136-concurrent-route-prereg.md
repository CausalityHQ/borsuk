# V136 shared-generation concurrent local route preregistration

V135 passed the serial 100k exact-hull route screen, but measured one request
at a time. V136 tests whether **one authenticated generation shared by
threads** can sustain C1/C8/C32 complete candidate routes without changing
their results or exceeding charged memory. This is a throughput and resource
screen on the same previously used development cohort, not fresh recall or
live S3 evidence. The production Rust router, nominee score, weighted
planner, authenticated local SQ8 reader, returned SQ8 scorer and exact-source
ranker execute for every timed request. There is no control arm in this
concurrency run; V135's serial candidate is historical reference.

Use V131's authenticated local generation and **deep-image-96-angular random
100k train subset**, with 1,000 already-used publication-test queries
9000–9999. Fully authenticate all inputs and issue one untimed verified
pass through the candidate path to warm the page cache. Then run three
separate cells in C1, C8, C32 order. Each cell executes eight copies of all
1,000 queries (8,000 completed requests); an atomic counter distributes
queries to threads. The same immutable `ExactServingGeneration` and local
file handle are shared within a process. Time from request entry through
exact-source ranking, measure wall-clock completed queries/second, nearest
rank p50/p95/p99 in milliseconds, total SQ8 range reads and bytes, serving
cgroup peak including file cache, and startup preparation/authentication.
Write every request's result and phase timings to a terminal-listed raw
artifact. Every copy must preserve V135's top-100 source-ID order and GT
hit count for its query, the nominee and primary order, one range within
16,777,216 bytes, and complete authenticated reads. Any mismatch fails the
cell before interpreting speed.

The scalability screen passes only if C1 complete-route p95 ≤75.624654
ms/query, C8 throughput ≥4× C1 throughput, C32 throughput ≥8× C1
throughput, C8 p95 ≤2× C1 p95, C32 p95 ≤4× C1 p95, and serving charged
peak ≤4,294,967,296 bytes. These are engineering decision thresholds,
not commercial claims. Report all measured ratios even if a gate fails;
diagnose CPU, cache/bandwidth, lock contention, and per-request allocations
before changing the implementation. The absolute SQ8 object is 10.8 MB,
so the whole-object hull still fits the physical cap at 100k; no 1M
concurrency prediction follows.

Run one frozen Causality `c7i.8xlarge` Spot attempt with a 5,400-second
worker cap. Monitor only terminal markers and infrastructure state during
execution; authenticate all terminal-listed artifacts after completion.
Discard and restart an interrupted cell rather than combining partial
measurements, and terminate compute immediately after terminal. The next
separate gate is live S3 Range transport, followed by a generic router
revision and matched fresh ReLAION/deep-image 1M builds. No dataset branch
or vector-count memory knee is permitted.
