# V242 cached neighbor distances, ReLAION-100k

Decision: HNSW insertion repeatedly prunes full neighbor lists. The
current comparator recomputes D768 squared distance on every comparison
of the same list members. V242 caches one `Candidate` key per member for
each sort, using the existing `Candidate::cmp` total order. This changes
the build work from repeated distance evaluations during sorting to
one distance evaluation per member; it should not change edge order.
Test this simple asymptotic improvement before a parallel builder.

Run one `causality` c7i.4xlarge Spot cell with the same authenticated
ReLAION-100k D768 source, FP16 plane, PQ artifacts, and prior-used
query panel as V241. Development ordinals 0–255, method-held-out
ordinals 256–999, cosine k=100, M=32, M0=64,
ef_construction=128, PQ ef/FP16 shortlist=2048/2048. Seal raw IDs
before truth or baseline results. Discard any interrupted cell and
restart at a new attempt. Stop the instance after the terminal marker.

The strongest paired BORSUK baseline is V241: graph SHA-256
`d8b70919243a7cd6ecb9448ce23f776374738476c1882cbc6a651fb34753af2f`,
177.361 s Rust build time, 530,915,328 B peak builder RSS,
100,000/100,000 reachable, 99,771/100,000 GT100 hits and p05=99.
Those are verified historical measurements, not V242 projections.

Pass requires exactly the same authenticated graph SHA-256, identical
ordered PQ and exact-FP16 returned IDs for all 1,000 queries,
100,000/100,000 reachable with minimum in-degree four, peak builder RSS
at most 600,000,000 B, and Rust build time at most **133 s**. This
speed ceiling demands at least a 25% reduction from V241, far more
than normal run variation. Record loaded p50/p90/p95/p99, QPS, serving
RSS and GETs descriptively, with no serving win claim. If graph or IDs
change, reject the cached-key implementation. If it is exact but misses
the speed or RSS gates, revert this optimization and proceed to a
material parallel construction design with measured profiling evidence.
If it passes, retain the one-sort-key change and promote it to an
unchanged 1M build resource gate before inferring any 10M/100M benefit.
