# V258 CoHere first 1M: exact FP16 graph navigation falsifier

V257's closed full-panel candidate ceiling is 99,411/100,000 exact
GT100 hits, below the 99,500 gate. Rescoring its fixed graph-plus-coarse
candidates cannot pass. V251 already tested a different navigation
representation on the same CoHere first100k source graph: exact FP16
distance at ef2048 returned 99,845/100,000 GT100 hits, p05 99 in
both prior-used splits, loaded p95 38.803 ms, and p95 21,236 base
scores. Its then-frozen 10,000-score promotion cap failed and remains
a recorded failure. V258 tests whether that **same** exact navigation
method transfers to 1M with useful quality and latency. This is a
material change to graph candidate generation, not a V257 ef/probe
sweep. It does not train or tune on query truth.

Dataset: CoHere-large-10M canonical first1,000,000 source rows, D768
cosine, k100; authenticated prior-used test ordinals0–999, development
0–255 and validation256–999. Reuse the fully authenticated, closed
V257 `a0001` source, FP16 plane, graph, map, books/codes and requests,
with exact per-artifact size/SHA-256 replay against terminal SHA-256
`e1cef23d26f9d3fb96276541084de06ec6ba09bd6b75206267518fc958530f06`.
No graph rebuild, coarse route, PQ navigation, extra entrypoint,
fallback, or parameter sweep. Traverse that same source-only diverse
graph with exact FP16 cosine scores, ef2048, and return exact-scored
top100. Seal returned IDs before computing exact F32 truth. Check the
truth SHA-256 against V255/V257
`62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39`.

Pass requires mean exact R@100≥0.995 and p05 hits≥98 in **each** split,
combined hits≥99,500/100,000; loaded eight-worker in-process p95≤120
ms, p99≤150 ms, throughput≥70 QPS, peak serving RSS≤3 GiB and zero
query vector-body GETs. Report same-sample loaded p50/p90/p95/p99,
sequential timings, p50/p90/p95/p99 base FP16 scores, resident bytes,
Spot elapsed cost and inherited build costs separately. The new query
work is measured as a fraction of N; there is no fixed vector-count
memory knee or retroactive claim that V251 passed its old work gate.
This falsifier alone is not client HTTP product latency or a vendor win.

If quality and resources pass, promote this exact source/graph/serving
revision to one persisted HTTP no-cache/cold gate, including recall,
same-sample client p50/p90/p95/p99, throughput, bytes/GETs, cache state,
RSS, hardware/region and cost. If it fails quality, stop on this exact
navigation design and choose a different index/candidate-generation
format at a cheap 100k gate. If resources fail, decide from measured
work and latency whether a new format is needed; do not run ef tuning.
10M remains closed until both quality and HTTP gates pass.

One `causality` c7i.4xlarge Spot attempt, immutable source archive and
S3 prefix. Discard interrupted cells, sync terminal artifacts, read
back all sizes/hashes after terminal, and terminate compute immediately.
