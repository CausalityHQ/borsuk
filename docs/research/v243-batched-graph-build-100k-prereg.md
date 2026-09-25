# V243 deterministic batched graph build, ReLAION-100k

Decision: V242 cached neighbor distances preserved the graph but missed
its frozen 25% build-time gate. Construction remains serial. Test one
material parallel insertion policy at 100k before any 1M promotion.
The production serial builder remains the control. The candidate uses
the same source F32 cosine geometry, parameters, pruning, reachable
backbone, authenticated FP16 plane and graph format.

Nodes retain the deterministic shuffled insertion order. At inserted
row count `i`, the next batch has `clamp(i / 32, 1,
8 * ef_construction)` rows. Every node in a batch searches the same
frozen adjacency, entry and top level. Eight workers hold separate
epoch-visit arrays. After all searches finish, proposed edges are
connected in original node, layer and neighbor order, then entry/top
level advance in that order. Batch size depends on graph work, never
on worker scheduling, corpus identity or query labels. A one-thread
build uses the identical batch schedule.

Use one `causality` c7i.4xlarge Spot cell with V242's authenticated
ReLAION-100k D768 physical-order source, FP16 plane, PQ books/codes
and requests. Cosine k=100, prior-used development queries 0–255
and method-held-out queries 256–999, M=32, M0=64,
ef_construction=128, PQ ef/FP16 shortlist=2048/2048. Build candidate
graphs with one and eight workers from the same source, compare their
authenticated SHA-256, and serve only the eight-worker graph. Seal raw
IDs before opening GT100 or prior results. Any interrupted cell is
discarded and restarted at a new attempt. Terminate compute at the
terminal marker.

The strongest paired BORSUK baseline is V242's serial builder:
138.640 s Rust build time, 531,288,064 B peak RSS, 100,000/100,000
reachable, 6,463,215 directed base edges, 99,771/100,000 GT100 hits,
development 25,537/25,600, held-out 74,234/74,400, p05=99,
and loaded p95 base visits 23,015. These are historical verified
measurements, not candidate projections.

**Pass requires:** one-worker and eight-worker graph SHA-256 equality;
100,000 reachable, minimum in-degree four, maximum degree at most
256; combined returned PQ GT100 hits at least 99,721/100,000,
held-out at least 74,200/74,400, p05 at least 99; p95 base visits
at most 24,166; eight-worker Rust build time at most **70 s** and peak
RSS at most **600,000,000 B**. Record both build times and hashes,
exact FP16 diagnostic hits, loaded p50/p90/p95/p99, QPS, serving RSS
and GETs. A gate failure stops the
batch design; do not tune the batch schedule on this used panel.
The 1M promotion then requires a frozen same-source gate with its own
quality, build-resource and same-revision serving thresholds. No
100M runtime or vendor win is inferred from this 100k test.
