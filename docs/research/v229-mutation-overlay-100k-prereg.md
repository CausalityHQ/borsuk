# V229 ReLAION-100k mutation overlay gate

One Causality c7i.4xlarge Spot attempt uses the selected V218 graph,
FP16 plane, PQ codes/books, physical map, query panel and sealed V218
returned IDs. The exact GT100 witness is downloaded only after this
gate's candidate raw IDs are sealed to S3. Source archive, original
terminal, instance identity and every artifact hash are retained under
the attempt prefix. A Spot interruption invalidates the cell and
requires a fresh attempt; the worker uploads a terminal marker and
shuts down.

For ReLAION-100k D768, development queries 0–255 and previously used
method-held-out queries 256–999, k=100, upsert every hundredth physical
row (1,000 rows) under the **same public ID and same authenticated FP16
vector**. The logical corpus and exact GT100 therefore do not change.
Graph navigation still traverses the masked base rows; FP16 scoring
replaces those rows from a bounded delta scan. EF and shortlist remain
2,048/2,048. The unmutated authenticated loader must replay every
V218 returned-ID list exactly before the overlay measurement starts.

The frozen pass gate is no GT100 loss on either split versus V218
(25,537/25,600 and 74,234/74,400), combined p05 at least 99 hits per
query, loaded eight-worker p95 at most twice V218's 6.915250 ms
(13.830500 ms), peak RSS at most 256 MiB, and zero vector-body GETs.
Report p50/p90/p95/p99 from the same 1,000 loaded query samples,
throughput, sequential timing, masked shortlist count, 1,000,000
delta-row scans, resident overlay bytes, wall time, peak RSS and cost.
If quality or p95 fails, stop the brute-force delta design and choose a
material indexed-delta or compaction-policy change before another 1M
mutation run. This in-process gate is not network product latency.
