# V139 approximate centroid-page admission preregistration

**Decision question:** can one source-only, query-dependent unit score expose
unvoted physical pages without reading nearly the whole 100k SQ8 object, and
remain inside the 1M byte/GET cap? V138 showed that a sound center/radius
bound admitted all D96 pages on 917/1,000 used queries. This experiment
removes the radius from the admission decision. Its score is approximate and
has no formal recall guarantee; the Lean conditional containment theorem does
not apply to this selection.

## Frozen method and cohorts

Use the same V122 **deep-image-96-angular random100k train subset**, already
used **publication-test queries 9000–9999**, then the V116 **ReLAION-1M**,
already used **validation-1000**, only if D96 passes. These historical layouts
are different; passing both screens would justify a matched production-layout
implementation, not a cross-corpus quality claim. The authenticated input
URIs and SHA-256 values are those in
`v138-unit-bound-feasibility-prereg.md`, except the GT-bearing V122 evidence
and V116 replay are not downloaded by the worker. Instead, the frozen
primary-only V122 file is SHA-256
`04cdbea9079837806059799d9a90c2f579526de100b7743f83a3794a15c0d6e3`
and the frozen primary-only V116 file is SHA-256
`71bfdf71f293ca1d23f58694866b3ba52ee8ee95ed9b02e676a2a8bf031f3162`.
Neither file contains GT or returned IDs. Request vectors and SQ8 bytes are
authenticated. The evaluator reads no GT.

Reconstruct every SQ8 physical row from the frozen `low`, `step`, and code
bytes. Partition the physical object into consecutive 32-row units and store
each unit's arithmetic mean as an f16 centroid. A query scores every unit by
Euclidean distance to its f16 centroid. The threshold is the largest
Euclidean distance between the query and its 100 sealed primary SQ8 rows,
plus `sqrt(sum((step_i/2)^2)) + 0.001`. Admit every unit whose centroid
distance is at most that threshold. **Always union the physical pages
containing the 100 primary rows**, so the established nomination is retained.
Each page is 256 rows. Cover all admitted pages with the minimum possible
bytes in at most 32 disjoint contiguous ranges by bridging the shortest
page gaps; charge the short final page exactly. This range cover is a lower
bound on byte transfer for this admission rule, not a deployed S3 scheduler.
There is no dataset branch in the score, threshold, unit size, page size, or
range-cover algorithm.

Record 1,000 per-query raw rows with admission counts, primary-page count,
physical ranges, GETs, bytes, and cap result. Report p50/p95 units, pages,
bytes, mean bytes, cap fraction, f16 centroid payload, evaluator RSS, and
cgroup peak. These are source-only selectivity/resource measurements, not
returned Recall@100 or live S3 latency. A flat scan of all unit centroids is
only a feasibility implementation; 10M/100M requires a hierarchical query
router with measured latency and memory scaling.

## Frozen stop and follow-up gates

Reject at D96 if mean minimum bytes exceed **4,714,063 B/query**, half the
historical V132 candidate's measured 9,428,126.976 B/query S3 response on
the same D96 corpus/split, or if more than 5% of queries exceed 16,777,216 B.
This is the same selectivity threshold as V138. If D96 rejects, skip all
ReLAION input. If D96 passes, reject at D768 if more than 5% of queries need
over 16,777,216 B even after the minimum 32-GET gap bridging. An in-cap
result is necessary but not sufficient: the exact next gate is a separate
offline physical GT coverage and returned-quality replay against each sealed
strongest baseline, with p05 and sub-90 tails, followed by a matched live S3
comparison only for a quality winner. Do not tune this score or threshold on
the used GT split and then call the result fresh validation.

Run one Causality Spot attempt from a committed source archive with a
5,400-second worker deadline. Verify every downloaded input and archive,
observe infrastructure and terminal only until completion, authenticate all
terminal-listed artifacts, discard interrupted measurement cells, and
terminate the instance at terminal. Do not overlap benchmark attempts.
