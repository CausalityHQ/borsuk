# KILL compressed-score page discovery

ReLAION first100k, D768 cosine k100, development0–63 (64 already consumed queries), same root/source/SQ8/query/GT as native pipeline. Code-scored159 pages/at most40704 rows,32 logical GETs/16773120 planned bytes. No actual cloud transport or product latency measurement.

| Metric | Candidate | Unchanged native-layout route | Paired flat SQ8 |
|---|---:|---:|---:|
| Mean discovered/fetched GT100 coverage (%) |95.968750|fetched99.562500|—|
| Mean returned R@100 (%) |95.656250|99.156250|99.515625|
| Returned p05 (hits/100) |87|97|99|

The hard quality floor, noninferiority and predeclared causal-gain screen fail. First observed missing-GT layer is discovery: selected/fetched ranges add no average recovery beyond discovered pages. Direct compressed-score guidance over page-unioned unit edges does not recover enough pages at this work cap. Do not tune adjacency ordering, increase page/work caps or promote this arm. No CoHere, validation,1M or paid run followed.

Red crafted test exit101 at unimplemented method; seven affected graph tests then pass green exit0, optimized build exit0, original development gate exit0/KILL. Exact candidate patch against855b885a, binary/source hashes and raw plans/samples preserved. Removed experimental APIs/demo flag after KILL and quarantine its remote binary; existing serving route unchanged. Previous full native-fitter KILL also remains.

Decision: stop the local traversal-variant series. Next material implementation must address source-only physical layout and the native flat fitter's unqualified100M build cost, using balanced hierarchical fitting with bounded stream/sample memory. Reconcile earlier contiguous hierarchy/spill failures before fixing that specification, and use a cheap development physical-page oracle before full fitting/quality panels. Do not turn a GT oracle into a production router. Both matched vendor wins and all scale/lifecycle gates remain required.
