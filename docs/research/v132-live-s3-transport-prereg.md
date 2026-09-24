# V132 live S3 transport replay preregistration

V132 first tests whether the V131 exact-source quality survives a real S3
read through the production one-attempt, ETag-pinned, per-page SHA-256 reader.
This is a **transport integration gate**. It replays V122's frozen physical
ranges and nominee ordinals and does not claim live router/planner latency.
No index fitting, range selection, thresholds or query split will change in
response to this run.

The immutable serving-generation package is
`s3://borsuk-bench-453182569524-euc1/research/v132-live-s3/01c4590b80b540bfb5e4f01909362d1c3ab487f9/generation-131-a0001`.
Its generation manifest SHA-256 is
`968ef7d795b53e5869400998ca19c6ab20d9b39830295fac3081c419f63f0f20`;
terminal SHA-256 is
`0ec4b70965d11c83eec1c5f0ee70c2f181fb2954df1429c8f02f57837842f4a6`.
An independent readback authenticated all 16 referenced objects (67,556,800
bytes), the altered router manifest's sole generation field, all 391 SQ8
page digests and all 2,637 local-mirror block digests. The source tier and
map are the sealed V131 generation-131 artifacts; the SQ8/router arrays are
the sealed V122 bytes. The router manifest and page authority now declare
generation 131, with old artifacts retained unchanged.

Dataset: **deep-image-96-angular random 100k train subset**. Queries: the
already-used **publication-test ordinals 9000–9999**, with GT100 recomputed
within that subset. This is a development replay, not untouched quality or a
cross-dataset generality claim. The paired arms are V122's candidate
513/1-vote physical ranges and same-query V109-style capped control ranges.
Both use the same source tier, ID map, 512 physical SQ8 nominee rows, top-512
returned SQ8 expansion and exact F32 source cosine ranker. The candidate and
control execute in alternating order by query. No query data influences the
index or physical ranges.

Each measured query issues fresh conditional S3 range GETs: at most 32 and
16,777,216 planned response bytes per arm. Control ranges may execute
concurrently within a query; queries execute serially. A range must be an
exact SQ8 page span in the authenticated authority. The reader has SDK
retries disabled; its HTTP fixture showed one request on success and on
bad status, Content-Range, ETag, payload, truncation and 412/500 responses.
Any query error fails the entire cell, with the attempted query and phase
recorded. The runner records every GET request it submits and its expected
and received response bytes; no inferred wire cost from logical row counts.

Primary checks are exact per-query top-100 **set** parity with V131 in both
arms, source Recall@100 of 99.942% candidate (99,942/100,000 GT hits) and
98.827% control (98,827/100,000), zero authentication failures, and no cap
violation. Within-set rank order is recorded separately because the V130
NumPy/Rust comparison differed on nine queries per arm. This gate is rejected
if any set or hit count differs. For transport viability, the candidate's
complete replay p95 must be at most twice the same-run control p95. Crossing
that ratio triggers an I/O-schedule diagnosis before an end-to-end router
gate; the ratio is a generic paired screen, not a production latency SLO.

Report p50/p95/p99 complete replay latency and S3 subphase latency in ms,
paired wins/ties/losses, GET attempts and response bytes per arm, local source
authenticated read bytes/blocks, startup download/authentication time, and
dedicated serving cgroup peak charged memory including file cache. The
complete replay includes mirror nominee-ID reads, S3 reads, returned SQ8
scoring, ID resolution and exact source scoring; it excludes offline range
planning and training. No throughput, multi-query concurrency, physical SSD
I/O, untouched recall, 1M generality, 10M selectivity or competitor result
will be inferred from this cell. The 100k candidate reads the entire
10,800,000-byte SQ8 object in one GET.

Run one Causality Spot attempt with a source archive SHA, instance ID,
bounded wall clock, raw per-query evidence, resource measurements and a
terminal marker. Stop/terminate the instance after the marker; monitor an
incomplete run only through marker and infrastructure state. Independently
recount all terminal-listed artifacts on completion. Only if this transport
gate passes should a new frozen end-to-end Rust router/planner and concurrent
serving gate run, followed by matched generic 1M builds on deep-image and
ReLAION and later selective 10M/100M trials.
