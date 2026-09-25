# V245 owner-partitioned source graph, ReLAION-1M

Decision: V244 passed the frozen 100k deterministic construction,
quality, time and RSS gates. Promote its unchanged source-only
owner-partitioned batch policy to one ReLAION-1M gate. The graph
format, authenticated FP16 plane, PQ64 cosine navigation and final
FP16 rerank remain the selected resident architecture. This campaign
tests 1M build scalability and returned quality; it is not a matched
network product comparison with S3 Vectors or Turbopuffer.

Use one `causality` c7i.4xlarge Spot instance. Pin the V219
authenticated ReLAION-1M D768 source, physical order, plane, PQ
books/codes and validation queries 0–999, all prior-used. Report
0–255 and 256–999 separately as descriptive splits. Cosine k=100,
M=32, M0=64, ef_construction=128, eight build workers. Freeze the
same four V219 ef/FP16-shortlist arms (2048, 4096, 8192, 16384),
with 4096/4096 as the quality and resource gate. Seal returned IDs
before reading the exact GT100/V198 witness. An interrupted cell is
discarded and restarted at a new attempt. Record instance ID, source
archive hash, artifact hashes and compute quote; terminate at its
terminal marker.

The paired V219 serial BORSUK baseline at ef/shortlist4096 returned
**99,664/100,000 GT100 hits**, development 25,508/25,600,
remaining 74,156/74,400, p05=98; loaded eight-worker in-process
p50/p90/p95/p99 was 13.365/17.697/18.911/20.737 ms, 555.6 QPS,
peak serving RSS 2,023,636,992 B and zero vector-body GETs. Its
graph build took 2,665.254 s at 8,394,833,920 B peak RSS. These are
verified historical measurements from a different revision, not
V245 projections. V244's 100k owner-commit build was 20.725 s at
537,739,264 B with 99,767/100,000 GT100 hits; 100k does not prove
1M behavior.

**Pass requires** 1,000,000/1,000,000 graph rows reachable,
minimum in-degree four, maximum degree at most 256; at 4096/4096,
combined GT100 hits at least **99,614/100,000**, development at least
**25,498/25,600**, remaining at least **74,116/74,400**, p05 at
least 98; graph build at most **900 s** and peak builder RSS at most
**6,000,000,000 B**. Loaded in-process eight-worker p95 at most
25 ms, p99 at most 30 ms, completed throughput at least 400 QPS,
peak serving RSS at most 3 GiB and zero vector-body GETs. Report
p50/p90/p95/p99 from each arm's own raw samples and all split counts.
If quality fails, diagnose graph search topology against V219 before
changing ef on this used panel. If time or memory fails, stop this
builder design and make one material algorithm/resource decision at
100k before another 1M run.

A pass selects this exact build/graph architecture for a same-revision
persisted end-to-end HTTP and direct S3 Vectors comparison under a
frozen workload. Product claims require paired disclosed cache state,
recall, p50/p90/p95/p99, throughput, bytes/GETs, memory, hardware,
region and cost; Turbopuffer remains published context until direct
authenticated access is available. Lean can prove conditional
operation, row-workspace and memory-model bounds, not empirical recall
or wall-clock latency.
