# V209 frozen resident ReLAION-1M serving gate

## Decision

Evaluate one generic source-only PQ64 nomination and complete resident
FP16 scoring path on the already-used ReLAION-1M D768 validation-1000
real queries. Compare actual returned GT100 quality on the same queries
with V199's 99,605/100,000 and V155's 99,567/100,000. Time the
entire in-process Rust request path, including nomination, ordinal map
and FP16 ranking, with eight concurrent workers. Record p50/p95/p99,
throughput, cold hydration, peak process RSS and zero vector-body GETs.
This is an internal product decision gate on a reused panel, not a
fresh-data, network-service or external-product result.

The source-only V115 PQ64 sections and manifest, V63 old physical
order/SQ8, V164 relaid order/SQ8, V196 FP16 plane and V116 request
vectors are immutable, SHA-256 pinned inputs from completed campaigns.
Convert the v1 router manifest to explicit v2 geometry/generation 196
without changing its arrays; require the preregistered derived digest.
Build the old-to-new row map with the production verified writer, which
compares every old and relaid SQ8 record and their whole-file digests.
Require every FP16 plane public ID to equal the relaid SQ8 public ID at
the same physical ordinal. Authenticate the plane, map, router and
trusted generation root before any query. Pin the FP16 S3 key and ETag
in that root. Per-request vector-body GETs and bytes must be zero.

Use fixed 1,024 router regions, 512 nominees and returned top-100. Do
not inspect GT when building artifacts or serving. Upload the raw
returned IDs to a distinct S3 seal before fetching V198's closed
per-query GT100 and baseline-hit witness. Score independently by
intersection; no per-query exception or parameter fit is allowed.

The internal advance gate requires at least V199's 99,605 hits,
p05 hits at least 98, request p95 below 92.23 ms and p99 below
137.87 ms, at least 100 completed queries/s across eight workers,
peak serving-process RSS at most 2 GiB, and zero vector GETs. V199's
92.23/137.87 ms are **transport plus SQ8 plus FP16 only**; its route
and planner were excluded. Their use as conservative latency ceilings
does not imply a like-for-like end-to-end comparison. Report the
metrics even if any gate fails and decide the next architecture step
from the actual bottleneck. The 2 GiB RAM limit is this 1M candidate
cell's resource envelope, not a vector-count knee or 100M policy.

Run one `c7i.12xlarge` Causality Spot cell from an immutable source
archive, with a 7,200-second wall cap. Record instance identity,
source/archive hashes, object ETag and terminal artifacts. Discard and
restart an interrupted cell under a new attempt, read back every
terminal artifact digest, and terminate compute immediately. Monitor
incomplete work by terminal marker and infrastructure health only.

Passing advances to an untouched Deep-Image query split and an actual
network service with matched Turbopuffer/S3 Vectors comparisons under
equivalent conditions. A latency failure calls for one material
algorithm/layout change, beginning with a cheap 100k falsifier; do
not resume V20x planner micro-optimizations.
