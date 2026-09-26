# V261 CoHere first1M: frozen two-scorer graph transfer

V260's no-coarse two-scorer graph passed its frozen CoHere-first100k
quality and in-process resource gates. V255's PQ graph and V258's
exact FP16 graph on the **same closed first1M panel** have a
final-list union candidate ceiling of99,733/100,000 exact GT100 IDs,
development25,516/25,600 p05 98 and validation74,217/74,400 p05 99.
That is only a candidate ceiling: it says nothing about final FP16
rerank quality, combined latency or product HTTP. V261 measures those
facts in one frozen first1M falsifier.

Dataset: CoHere-large-10M canonical first1,000,000 source rows,
D768 cosine, k100; authenticated prior-used test ordinals0–999,
development0–255 and validation256–999. Reuse V257 `a0001`'s fully
authenticated closed source, FP16 plane, graph, map, books/codes and
requests. Its terminal SHA-256 is
`e1cef23d26f9d3fb96276541084de06ec6ba09bd6b75206267518fc958530f06`;
replay each reused artifact size/hash before serving. No graph or
coarse rebuild. Run the **same V260 method**: PQ-cosine graph ef4096,
FP16 shortlist4096→k100; exact FP16 graph ef2048→k100; union the two
final k100 lists and FP16-rerank distinct IDs to k100. Sequential
navigation, eight loaded workers, no coarse index, ef/probe sweep,
query-trained parameters or fallback. Seal IDs before exact F32
truth; require truth SHA-256
`62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39`.

Pass requires each split mean exact R@100≥0.995 and p05 hits≥98,
combined≥99,500/100,000; loaded in-process p95≤135 ms, p99≤155 ms,
throughput≥65 QPS, peak serving RSS≤3 GiB and zero query vector-body
GETs. Report same-sample loaded p50/p90/p95/p99, sequential timings,
separate PQ/FP16 graph score counts, distinct final union size,
hydration and inherited build costs separately, Spot elapsed cost,
and all artifact hashes. No fixed vector-count memory knee. The
result is not client-observed product latency or a vendor win.

If all gates pass, freeze this exact source/graph/serving revision for
one persisted HTTP no-cache/cold-hydration gate with client-observed
same-sample p50/p90/p95/p99, exact recall parity, throughput,
bytes/GETs, RSS, hardware/region, cache state and cost. If quality or
resource gates fail, stop this dual-scorer series and choose a
different index or representation format, starting at a cheap 100k
falsifier. Keep 10M and competitor claims closed until product HTTP
qualifies.

One `causality` c7i.4xlarge Spot attempt, immutable source archive and
S3 prefix, interruption discard/restart, terminal hash readback,
artifact size/hash replay and immediate instance termination.
