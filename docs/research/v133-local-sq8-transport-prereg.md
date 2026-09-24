# V133 locally hydrated SQ8 transport preregistration

V132's broad candidate range retained 99.942% exact-source Recall@100 but
failed its live S3 transport screen: complete p95 152.858292 ms/query versus
75.624654 ms/query for the same-run capped control. Its 9.428 MB/query mean
S3 response was the measured dominant cause. V133 tests a materially different
placement: hydrate and authenticate the immutable SQ8 object once from S3,
then read its physical page ranges from the pinned local file. The file path
verifies every touched 4 KiB block and each requested page before ranking.
There is **no vector-count switch** in this placement. The object-storage
generation remains authoritative; local storage is a replaceable serving
copy. RAM placement is an explicit alternative, not an automatic threshold.

Reuse V132's sealed generation-131 package and the **deep-image-96-angular
random 100k train subset**, 1,000 already-used **publication-test ordinals
9000–9999**, and GT100 recomputed within the subset. This remains development
evidence. Both arms keep the frozen V122 physical ranges, 512 nominee rows,
top-512 returned SQ8 expansion, map and exact F32 source scorer. Candidate
and capped-control arm order alternates by query. No fitting, query split,
range schedule, or scoring threshold changes in response to V132 or this run.
The same exact-source output sets and per-query hit counts as V131/V132 are
mandatory. The local range reader rejects cross-generation, geometry, whole
object identity, byte-cap, page-span and digest drift. File mutation after
open must be detected by query-time block verification.

Run one frozen Causality Spot cell. Each query reads the exact requested
physical SQ8 byte spans from the authenticated local file, with at most 32
ranges and 16,777,216 requested bytes per arm. Report p50/p95/p99 of complete
replay, local SQ8 read and exact source subphases, paired hit outcomes, local
SQ8 range/read bytes and verified block reads, source tier bytes/blocks,
startup download/authentication time and charged serving cgroup peak
including page cache. This replay still excludes live router/planner,
multi-query concurrency and throughput. A completed run must publish raw
per-query rows, summary, resource measurements, exact source archive and
generation hashes, all under a terminal marker; discard interrupted cells.
Terminate Spot immediately after its terminal.

The **placement screen** passes only if every query preserves V131/V132's
top-100 source sets and hits, all physical caps and authentication checks
hold, candidate exact-source Recall@100 is at least 99.5%, candidate full
replay p95 is at most twice the same-run local capped control p95, and
candidate full p95 is at most half V132's 152.858292-ms live S3 candidate
p95 (76.429146 ms). These are paired relative screens, not production SLOs.
The candidate must also be no slower than V132's own capped-control p95
75.624654 ms/query to justify this additional local copy in the development
path; the tighter of this and the half-V132 criterion applies.

If this placement fails, diagnose file read/hash work and charged cache use
before considering another transport mechanism. If it passes, test the
complete router/planner and concurrent serving path, then perform matched
fresh deep-image and ReLAION-1M builds under one generic layout and memory
policy. V133 alone cannot establish 1M/10M/100M latency, cost, generality,
or competitive commercial performance. Lean payload and conditional
capture theorems can bound resource formulas and invariants, but hardware
latency and empirical recall remain measurement gates.
