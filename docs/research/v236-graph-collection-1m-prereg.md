# V236 ReLAION-1M persisted graph collection gate

Use one `causality` c7i.4xlarge Spot host in `eu-central-1c` and a new
attempt-specific S3 prefix. This is a publication and reload correctness
gate, not an HTTP latency comparison. Preserve source archive, instance
identity, terminal status, raw query IDs, artifact hashes, wall times,
resident bytes, GETs, RSS and Spot cost. On interruption, discard the
entire cell and restart with a new prefix. Terminate immediately after
the terminal marker.

The authenticated V223 ReLAION-1M D768 graph root is
`c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf`.
Validation queries 0–999 have already been used in method development;
cosine k=100, ef/shortlist 4096/4096. The frozen mutation batch upserts
every 100th physical row under its same source ID and authenticated FP16
vector: 10,000 rows, no logical corpus change. The reference is the sealed
V230 first-pass raw ID stream, SHA-256
`b4597049dd6959dbf5516342408a31e863fbe13505987f88811d58a4ccdd5d14`.
Compare all 1,000 complete ID lists; the reference has 99,668/100,000
exact GT100 hits. The original V219 base raw stream is a separate
revision-1 witness.

Gate: fresh prefix has no graph or collection head; publish and read
back the trusted root; revision 1 cold hydration uses exactly five graph
blob GETs; publish one authenticated mutation snapshot and CAS revision
2; revision 2 warm decoded hydration uses zero graph blob GETs; all
1,000 revision-2 ID lists equal V230; a held revision-1 reader remains
bit-for-bit unchanged after the slot swap; peak process RSS stays within
5 GiB while the old and new readers are both pinned. The present loader
opens a second graph in memory for the swap; measure that actual cost
before considering shared-base reuse. The loader's graph blob GETs and response
bytes exclude collection-head, root, and snapshot requests; disclose
those uninstrumented metadata requests separately. Publish and hydration
wall times are observations, with no speed threshold in this gate.
Seal raw IDs and terminal artifacts before downloading reference truth.

If this passes, the next single gate is frozen end-to-end VPC-peer HTTP
serving from revision 2, compared with V233 on the same query panel and
quality, including p50/p90/p95/p99, QPS, RSS, bytes/GETs and cost. If it
fails, fix the failed publication, authentication, or reload layer before
another 1M serving run. Do not introduce a mutation-count compaction knee;
choose an envelope from recall, latency and memory budgets.
