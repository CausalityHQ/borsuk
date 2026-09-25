# V231 indexed mutation delta, ReLAION-100k

**Decision tested:** whether one deterministic reachable HNSW over an
immutable mutation buffer removes the 10,000-row linear-scan query cost
without losing returned recall or making batch publication too slow.
This is a source-only index; no query labels train or tune it. The
ReLAION-100k D768 development queries 0–255 and previously used
method-held-out queries 256–999, k=100 and exact GT100 are held fixed.

One `causality` c7i.4xlarge Spot cell uses the authenticated V218 graph,
FP16 plane, PQ64 codes/books, physical map, frozen query panel and V218
returned IDs. Every tenth physical row (10,000 rows) is upserted under
its original public ID with its original authenticated FP16 vector.
The logical corpus and GT100 stay unchanged. The new loader must replay
all V218 ID lists before either measurement. Search keeps ef/shortlist
2,048/2,048; both arms run sequential and eight-worker loaded measurements
over the same 1,000 queries. The `linear-10k` arm scans all 10,000 delta
rows. The `indexed-10k` arm builds one deterministic reachable HNSW
(M=16, M0=32, ef construction=64), uses 256 candidates, then exact
FP16 reranks them against the unchanged base results. Index build time,
peak RSS, resident bytes and exact FP16 rows scanned are measured.

Seal both raw ID/timing streams independently to S3 and replay their
hashes **before** downloading GT100. Score the two arms against each
other and V218 by split. The frozen pass gate is indexed GT100 hits on
each split at least the linear and V218 values, combined p05 at least
99, loaded eight-worker p95 at most 75% of the linear arm, index build
at most 60 s, peak indexed RSS at most 512 MiB, at most 256,000 exact
indexed delta-row reranks across 1,000 queries, and zero vector-body
GETs. Report p50/p90/p95/p99 from each arm's own same 1,000 loaded
samples, QPS, sequential timing, build time, RSS, delta resident bytes,
bytes/GETs, instance/region, source and terminal hashes, and estimated
Spot cost. The linear arm is the strongest same-revision comparison;
V229's measured 1,000-row overlay and V218 are historical context.

An indexed quality failure rejects this 256-candidate design and calls
for a material routing/format decision before 1M. A build-time failure
means the full immutable-delta rebuild is not a usable write path; test
a tiered or incrementally maintained index at 100k before promotion.
If this cell passes, run one 1M end-to-end network mutation service
repeat against V230, then a real-S3 1M collection publication gate.
No 10M/100M extrapolation or vendor product win follows from this cell.

The source archive, attempt reservation, original terminal and all
artifact hashes are retained. A Spot interruption invalidates both arms
and requires a fresh attempt. The worker syncs interruption/terminal
evidence and shuts down; the controller independently confirms instance
termination. Incomplete measurement files are not inspected.
