# KILL page-diverse expansion; no scale promotion

Frozen ReLAION first100k, D768 cosine k100, development0–63 (64 previously used queries). Same root/SQ8/request/GT,159 candidate pages,1272 discovery-score cap,32 GET and16773120-byte cap. Established sequential-f32 SQ8 mirror, not HTTP/product latency.

| Metric | Candidate | Unchanged native-layout route | Paired flat SQ8 |
|---|---:|---:|---:|
| Mean fetched GT100 coverage (%) |99.484375|99.562500|—|
| Mean returned R@100 (%) |99.062500|99.156250|99.515625|
| Returned p05 (hits/100) |97|97|99|

Candidate meets short quality floor but loses fetched and returned quality and fails the predeclared>=.1pp fetched causal-gain screen. First observed failure: the expansion schedule does not improve page discovery/coverage under unchanged work. This does not refute all bounded navigation, but closes this concrete round-robin page scheduling arm. No CoHere run, validation read, budget relaxation, seed sweep, paid instance or1M promotion.

The crafted graph mechanics test failed red exit101 as intended, then all seven affected graph tests passed green exit0. Original optimized build exit0. Initial scorer setup failed exit1 before queries because the established scoring helper imports PyArrow transitively; added cached PyArrow24 to the offline UV environment, final gate exit0/KILL. Both receipts preserved. Candidate patch applies to parent d772b4fc and source/binary SHA receipts bind exact execution. Removed its experimental APIs/demo flag after KILL; current serving code remains the original route. Previous full native-fitter KILL remains.

Next material decision must change the discovery signal or physical layout, rather than another frontier/work-cap adjustment. Any next candidate needs one cheap development falsifier before frozen qualification and cold HTTP against BOTH S3 Vectors and Turbopuffer. No measured vendor advantage exists yet.
