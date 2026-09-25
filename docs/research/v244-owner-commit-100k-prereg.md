# V244 owner-partitioned graph commit, ReLAION-100k

Decision: V243's frozen-snapshot batch searches passed deterministic
graph and quality gates, but its sequential edge commits left the
eight-worker build at 79.243 s, above the 70 s gate. V244 keeps the
same query-blind batch schedule and proposals. It groups updates by
owner row and applies each owner's updates in the original proposal
order, using disjoint row ranges in parallel. Updates to distinct rows
commute; the V243 authenticated graph SHA-256 is the exact-output gate.
This is a commit-representation change, not a batch-size retune.

Use one `causality` c7i.4xlarge Spot cell with V243's authenticated
ReLAION-100k D768 physical-order source, FP16 plane, PQ books/codes
and request roster. Cosine k=100; prior-used development queries
0–255 and method-held-out 256–999; M=32, M0=64,
ef_construction=128, PQ ef/FP16 shortlist=2048/2048. Use eight
workers. Seal raw IDs before reading truth or prior results. Discard
and restart an interrupted measurement cell under a new attempt ID;
terminate Spot compute at the terminal marker.

The paired V243 graph SHA-256 is
`498ab9f5a3da7c672ccf362d4ae5c96a853b7d4f9f2d6fe9d7708f0250045d03`.
V243's eight-worker build took 79.243 s at 533,233,664 B peak RSS,
returned 99,767/100,000 GT100 hits (development 25,536/25,600,
held-out 74,231/74,400), p05=99 and p95 base visits 23,030.
The selected serial V242 baseline took 138.640 s, peak RSS
531,288,064 B and returned 99,771/100,000 hits. These are verified
historical values, not V244 predictions.

**Pass requires:** exact V243 graph SHA-256 and size, 100,000/100,000
reachable, minimum in-degree four, identical ordered PQ and exact-FP16
IDs for all 1,000 queries, eight-worker Rust build time at most
**55 s**, and peak builder RSS at most **600,000,000 B**. The 55 s
ceiling demands a further 30.6% wall-time reduction from V243 and
60.3% from V242. Record loaded p50/p90/p95/p99, QPS, serving RSS,
GETs and compute cost descriptively. A failure stops this owner-commit
implementation without retuning its schedule on the used panel. A
pass promotes one frozen 1M same-source build, recall and serving
resource gate; no 10M/100M or vendor win follows from 100k alone.
