# V232 exact decoded mutation delta, ReLAION-100k

**Decision tested:** whether spending resident RAM to decode FP16
mutation coordinates once removes enough query work to keep an exact
scan viable at 10,000 pending rows. V231's 256-candidate HNSW cut
latency but lost 20 GT100 hits and is rejected; this candidate does
not change routing or candidate count. Each FP16 value is exactly
representable in FP32. The query converts that FP32 value to FP64 and
accumulates cosine products in the same coordinate order as the
current scan, retaining deterministic score and ID ordering.

One `causality` c7i.4xlarge Spot cell uses the authenticated V218
ReLAION-100k D768 graph, FP16 plane, PQ64 codes/books, map, frozen
queries and returned IDs. Development queries 0–255 and previously
used method-held-out queries 256–999, k=100 and GT100 are fixed.
Every tenth physical row (10,000 rows) is upserted under the same
public ID and authenticated FP16 vector; the logical corpus and
truth remain unchanged. Both arms replay all V218 ID lists before
measurement and use ef/shortlist 2,048/2,048. The `linear-10k` arm
decodes FP16 per scored coordinate; `decoded-10k` stores FP32 values
once and scans exactly the same 10,000 rows. No query label trains
or selects either arm.

Run each arm sequentially, then with eight loaded workers on the same
host. Seal each arm's per-query ID/timing stream **and its distinct
loaded per-query timing stream** to S3 before downloading GT100.
Recompute loaded p50/p90/p95/p99 from the sealed raw timings. Record
mutation preparation and decoding time, overlay-owned bytes, process
RSS, throughput, sequential timing, vector GETs, source/terminal
hashes, instance/region and estimated Spot compute cost. The same-run
linear arm is the strongest relevant comparison; V231's linear
10,000-row result is immutable historical context.

The frozen pass gate requires all 1,000 complete returned ID lists
to match the linear arm exactly, per-split GT100 hits at least V218
(25,537 and 74,234), combined p05 at least 99, decoded loaded p95
at most 75% of the linear loaded p95, decoded throughput at least
1.5× linear, one-time decoding at most 1 s, decoded peak RSS at most
512 MiB, overlay-owned resident bytes at most 64 MiB, 10,000,000
delta rows scored over the panel and zero vector-body GETs. If parity
fails, reject the decoded representation; if performance fails,
decide a material exact-score vectorization or mutation-tier change
before another 1M cell. A pass promotes exactly one 1M end-to-end
network serving comparison against V230 and then a real-S3 1M
collection publication gate. Do not extrapolate 10M/100M or claim a
matched vendor win from this in-process cell.

Retain the source archive, attempt reservation, original terminal,
all artifact hashes and sealed raw streams. Spot interruption
invalidates the full two-arm cell and requires a new attempt. The
worker syncs interruption and terminal evidence and shuts down;
the controller confirms termination. Incomplete measurement files
are not inspected.
