# V247 direct S3 Vectors comparator for V246 ReLAION-1M

Run one fresh S3 Vectors float32 cosine index under the authenticated
matched-service runner. Use the same ReLAION-1M source Parquet SHA-256
`2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86`,
validation query SHA-256
`869e225181f7d01a972d8faa144eaff4838c7c1f8f7c0c55091487e234f0bd5e`,
and GT100 SHA-256
`bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871`.
Dataset N=1,000,000, D=768, validation ordinals 0–999 already used,
cosine k=100. The GT is squared Euclidean and vector norms are close
to, but not exactly, unit length; score both products against this
identical GT and disclose that metric distinction.

One `causality` c7i.4xlarge Spot client, first in `eu-central-1c`,
with serialized fallback to the other existing eu-central-1 zones
only if Spot capacity is unavailable. Authenticate all inputs, create
a fresh index, ingest the complete source with five upload workers,
settle 60 seconds, then run validation queries in ordinal order at
eight concurrent SDK requests. Measure a fresh-index first pass and
an immediate repeated pass. No client response cache; S3 Vectors'
server cache state is vendor-managed and opaque. Reuse the existing
bounded 2-hour worker, interruption and resource-pressure policy.
Discard an interrupted cell, reserve a fresh prefix and restart the
whole run; never combine partial passes. Delete the temporary index
and vector bucket, retain cleanup receipt, original terminal, sealed
raw samples and their hashes, and terminate compute at terminal.

The current BORSUK V246 baseline is a separate two-host VPC-peer
plaintext HTTP cell on the same panel, k, concurrency and region,
served resident after authenticated S3 hydration. It returned
**99,662/100,000** GT100 hits in each pass (dev 25,506/25,600,
remaining 74,156/74,400, p05 98). First p50/p90/p95/p99 was
**16.039/21.447/23.375/26.563 ms**, 454.3 QPS; immediate repeat
**15.506/20.427/22.035/24.516 ms**, 479.7 QPS. Server RSS was
2,034,659,328 B; query vector GETs were zero. V246 source commit
`f40baabc9a8851aa57ed5b6fe35115fa795c3192`, closeout SHA-256
`e758b1d5a8971f32b6464964e19094f643a41b14b3a9384b4075e96acfff731f`.

Report S3 Vectors exact GT100 and recall@10/100, p05, per-query
p50/p90/p95/p99 from each pass's same sealed raw samples, completed
QPS, response bytes, SDK retries, ingest time/bytes/PUTs, client
RSS, Spot quote/elapsed compute estimate, service request and storage
cost basis, hardware and availability zone. Compare first to first
and repeat to repeat. Direct quality comparisons use the same GT;
latency, throughput and cost comparisons must label BORSUK's
resident server and peer plaintext HTTP against S3 Vectors' regional
HTTPS managed service and opaque cache. Do not present an unmatched
Turbopuffer published number as a measured win.

This gate characterizes the current commercial comparator. If it
finishes, decide from paired quality, latency, throughput and cost
whether the selected BORSUK architecture is strong enough for a 10M
scale cell. If the competitor is better at a material product metric,
make one concrete algorithm or serving decision before scaling.
