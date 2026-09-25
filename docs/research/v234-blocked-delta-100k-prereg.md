# V234 exact blocked mutation delta, ReLAION-100k

**Decision tested:** whether a resident eight-row blocked FP32 layout
removes the serial FP64 dependency chain in the 10,000-row mutation
scan while retaining the exact score arithmetic and ID order. The
coordinates come from authenticated FP16 once; each row's FP64
multiply and add remain in ascending coordinate order. The source
graph, routing and candidate count do not change. A synthetic ARM
microbenchmark in `/tmp/dotbench` suggested 1.57 ms versus 5.31 ms
per 10,000-row FP32 scan, but is not a dataset or c7i measurement.

One `causality` c7i.4xlarge Spot cell uses the authenticated V218
ReLAION-100k D768 graph, FP16 plane, PQ64 codes/books, map, frozen
requests and returned IDs. Development queries 0–255 and previously
used method-held-out queries 256–999, cosine k=100 and exact GT100 are
fixed. Every tenth physical row is upserted under the same ID and
authenticated FP16 vector (10,000 rows). The logical corpus and truth
are unchanged. Both arms replay all V218 ID lists and use ef/shortlist
2,048/2,048. `decoded-10k` is the V232 row-major control; `blocked-10k`
packs the same values into `[block][dimension][lane]` with eight lanes.
No query label selects the representation.

Run arms sequentially, then with eight loaded workers on the same host
and default release compiler flags for **both** arms. Seal each arm's
per-query ID/timing and loaded per-query timing streams to S3 before
downloading GT100. Independently recompute loaded p50/p90/p95/p99 from
the sealed raw timings. Record one-time decoding/packing, overlay-owned
bytes, RSS, throughput, sequential timing, vector GETs, source/terminal
hashes, instance/region and estimated Spot compute cost.

Pass only if all 1,000 complete returned ID lists match the same-run
decoded control exactly, per-split GT100 hits are at least V218
(25,537 and 74,234), combined p05 at least 99, blocked loaded p95 at
most 50% of control p95, blocked throughput at least 1.5× control,
one-time packing at most 1 s, blocked peak RSS at most 512 MiB,
overlay-owned bytes at most 64 MiB, 10,000,000 delta rows scored over
the panel and zero vector-body GETs. Any ID mismatch rejects the
layout. A speed failure means this c7i build did not expose enough
independent instruction work; choose a different exact mutation
kernel/tier before another 1M. A pass promotes exactly one 1M VPC-peer
HTTP run against V233/V230 under the same frozen panel, then a real-S3
1M mutation publication gate. No vendor or 10M/100M claim follows.

Preserve the source archive, attempt reservation, original terminal,
all artifact hashes and sealed raw streams. Spot interruption invalidates
the full two-arm cell and requires a new attempt. The worker syncs
terminal/interruption evidence and shuts down; the controller confirms
termination. Incomplete measurement files are not inspected.
