# V227 FP16 graph source, ReLAION-1M

**Decision:** Use the authenticated FP16 plane as the canonical graph
compaction source at the tested 100k and 1M scales. The 1M graph rebuilt
from FP16 is byte-identical to V219's selected F32-source graph. No F32
source object is needed to reproduce that exact graph for these frozen
inputs and builder; changed corpus or builder still requires its own
quality gate.

The sole Causality c7i.4xlarge Spot attempt `a0001` ran on
`i-059c2523b6897ac09` and is terminated. Source commit
`b4553447419d82388f93109abcf2a84bc0515648`, source archive
SHA-256 `7402b86d0bfaf5aa2f5d27cbb11a1995c251e53ba1029ea291d4f35479d47294`,
terminal SHA-256 `15e15a6ae262f837f576d379469ac6628f055cd7358ace1a7679bf15bf97eb9c`.
The terminal reports complete, exit zero; all seven artifacts passed
length and SHA-256 replay. Immutable evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v227-fp16-graph-source-1m/b4553447419d82388f93109abcf2a84bc0515648/runs/a0001/`.

Dataset: ReLAION-1M D768, selected V219 graph for validation queries
0–999 (already used), k=100. V227 changed only the build source from
the F32 preparation to the authenticated FP16 plane; it held M32,
M0=64, ef_construction=128 and graph code fixed. The rebuilt graph is
266,910,966 bytes and its SHA-256 is
`a2805a97c1955adf1cdc0b0da43b4ff205eb1d4646916c09d3c4d2b9f7c1ee0b`,
exactly V219's graph digest. It has 1,000,000 reachable rows, minimum
in-degree four, no rows below four in-edges and 64,927,667 directed
base-layer edges.

The byte-identical graph with the same FP16 plane and PQ inputs means
V219's sealed query result remains the valid selected baseline:
**99,664/100,000 GT100 hits**, p05 98, in-process loaded eight-worker
p50/p90/p95/p99 **13.365/17.697/18.911/20.737 ms**, 555.6 completed
queries/s, and 2,023,636,992 bytes peak serving RSS. These are
verified historical V219 measurements, not new V227 query timings or
network service latency. V227 built the graph in 2,825.970 seconds by
Rust timer, 47m07.53s wall, with 8,394,776,576 bytes peak RSS. The
separate V219 F32-source build took 2,665.254 seconds and peaked at
8,394,833,920 bytes; the runtime difference is across runs and not
assigned a causal performance effect.

V226 established the same byte identity and all 1,000 returned-ID-list
ties at ReLAION-100k. The next product gate is V228's one small real-S3
collection revision/CAS test. Then measure mutation overlay quality,
latency and resource effects on the 100k panel before changing the
selected 1M serving path. The FP16 source decision does not establish
generic compaction correctness or 10M/100M build cost.
