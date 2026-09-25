# V218 source-graph topology falsifier on ReLAION-100k

V217's authenticated 1M source graph has 37,780 unreachable rows and
37,362 zero-in-degree rows, versus 1,791 and 1,760 respectively in the
same builder's authenticated 100k graph. The old builder selects nearest
neighbors and drops far reciprocal edges on overflow. SQ8-only scoring
cannot visit structurally unreachable rows, so the V218 decision is a
**graph-construction change**. No PQ beam or per-dataset score tuning is
part of this cell.

Construct one deterministic, source-only cosine HNSW variant with the
existing M=32, M0=64 and ef_construction=128. Retain its source-nearest
search edges, then change the directed base-layer edge policy. Add one
edge per node to its successor in the source-only physical order, forming
a sparse cycle that makes every node reachable from every entry. Repair
rows with fewer than four in-edges by restoring reverse edges from their
existing outgoing neighbors, favoring those local graph relationships;
use the physical successor order only if those slots cannot satisfy the
floor. Added edges are retained rather than pruning another row's sole
incoming path. Abort the build if degree exceeds the authenticated graph
format's 256-edge limit or complete reachability cannot be achieved. The
rule and limits are independent of corpus size and query outcomes. This
low-overhead edge repair tests whether lost reciprocity is sufficient to
explain the recall deficit before paying for a higher-cost diversity
rebuild. Record edge
count, degree and in-degree distribution, reachability, build time,
memory and artifact hash before serving.

Reuse the authenticated ReLAION-100k D768 source, V214/V215 physical
order, PQ64 books/codes, FP16 plane, and V114 SQ8 reference. Build a new
graph from source float32 in physical order; serve k=100 with the same
PQ64-reconstructed cosine scorer, FP16 rerank and one fixed ef=2,048,
FP16-shortlist=2,048. The first 256 already-used development queries are
the V215 panel; queries 256..999 are the V216 method-heldout panel.
Record these two splits separately and their combined total. Run exact
FP16 navigation on the same new graph as a diagnostic, with its distinct
timing scope. Seal every returned ID to S3 before downloading the GT100
and V193/V215/V216 paired witnesses.

The structural gate is **100,000/100,000 reachable**, zero rows below
in-degree four, no invalid/self/duplicate edge and no degree above 256.
The PQ64 returned-quality gate is at least **99,521/100,000 GT100 hits**
(V215 25,473 + V216 74,048), per-query p05 at least 98, and no loss on
either split versus its corresponding V215/V216 result. The serving gate
is whole in-process eight-worker p95 below 10 ms, peak RSS below 256 MiB,
and zero vector-body GETs. Report p50/p90/p95/p99 from the same query
samples, completed QPS, base visits
and graph/plane/PQ/workspace bytes. No arm is selected after truth opens.
The 100k test can falsify this topology change but cannot prove 1M
recall or latency; fixed-ef insertion and graph density change with N.

One 100k pass authorizes one frozen ReLAION-1M validation attempt with
the same builder and score format, comparing directly to V199's
99,605/100,000 hits and p05 98. A 1M pass then authorizes a networked
same-panel cold/no-cache product comparison. That gate must record
per-query p50/p90/p95/p99, recall@k, bytes/GETs, concurrency, RSS,
throughput, hardware, region, source and cost. S3 Vectors is comparable
only on a matched split and workload. Turbopuffer's published cold p90
is contextual unless direct matched access exists. No competitor claim
follows from this 100k cell.
