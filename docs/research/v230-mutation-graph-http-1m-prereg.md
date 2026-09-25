# V230 ReLAION-1M mutation HTTP gate

Run one two-host `causality` c7i.4xlarge Spot cell in `eu-central-1c`.
The server downloads and authenticates the V219/V223 single graph root
and all five immutable blobs. It then upserts every hundredth physical
row (10,000 rows) under the same public ID and same authenticated FP16
vector. The logical corpus and exact GT100 do not change. Search uses
the library's `ResidentGraphOverlay` with ef/shortlist 4,096/4,096.
This gate measures source-derived mutations in a fully resident server;
the separate V228 gate measured real-S3 collection-head CAS. V230 does
not claim a 1M durable mutation publication or cold per-query object GET.

The frozen panel is ReLAION-1M D768 validation ordinals 0–999,
previously used, k=100. Eight persistent HTTP/1.1 client connections
on a VPC peer run first and immediate-repeat passes. Client timing
includes JSON encode, HTTP exchange, and decode; no response cache is
used. For each pass seal raw per-query IDs and latency to S3 before
downloading the exact GT100 witness. Recompute p50/p90/p95/p99 and
throughput from each pass's same raw 1,000 samples. Report query and
response bytes, vector-body GETs, server peak RSS, overlay-owned bytes,
hardware/region, Spot quote and estimated compute to terminal.

Strongest BORSUK baseline: V223 authenticated HTTP first pass, 99,664
GT100 hits, p05=98, p50/p90/p95/p99
14.828/19.302/20.820/22.564 ms, 497.0 queries/s, server peak RSS
2,026,614,784 bytes. V223 repeat was
14.765/19.269/20.713/22.573 ms, 502.2 queries/s. These are
separate runs with the same panel and peer shape, so differences are
descriptive. V221 S3 Vectors uses unequal internal cache and transport
semantics; Turbopuffer has no authenticated matched tenant result.

Pass only if both passes have no GT100 hit loss versus V219 on either
validation subset 0–255 or 256–999, combined p05 at least 98,
p95 at most 41.640 ms, p99 at most 45.128 ms, completed throughput at
least 200 queries/s, peak server RSS at most 3 GiB, exactly 10,000
delta rows, and zero vector-body GETs. The p95/p99 caps are twice the
V223 first-pass values. If latency or throughput fails, stop linear
delta scans at this size and decide an indexed delta or lower
compaction trigger before another 1M mutation cell; if quality fails,
inspect the masked graph shortlist/rerank invariant first. No 10M or
100M extrapolation is a measurement.

Retain source archive SHA, instance identities, terminal SHA and artifact
hashes. Spot interruption invalidates the entire two-host cell; sync
terminal evidence and restart only under a fresh attempt. Terminate
both hosts as soon as their terminal markers are collected. Do not
inspect incomplete measurement CSV/JSONL files.
