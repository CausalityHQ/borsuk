# KILL unit-prototype nomination; norm bias is insufficient

2026-09-28 UTC, original terminal exit0/advance=false. CoHere first100k D768
cosine GT100,64 previously used development queries0–63, unchanged source
layout,32-row prototype membership and21-extent byte cap. Only normalized
prototype directions before exact squared-distance ranking. For unit query and
unit prototypes this isolates cosine direction from differing mean lengths.

| Same physical layout /21-extent cap | Mean fetched GT100 (%) | p05 (%) |
|---|---:|---:|
| GT-aware feasible containment witness |99.78125 |99 |
| Raw nearest32-row means |92.4375 |79 |
| Unit nearest32-row means |93.0625 |80 |

Frozen fetched98.9%/96 gate fails. Normalization recovers only0.625pp mean/1pp
p05: it cannot explain most of the containment-vs-nomination deficit. This
kills exact nearest unit-sub-summary extent nomination, not all layouts/ANN.
No graph approximation is present to improve. No SQ8, ReLAION, new format,
GET increase, graph build, scale/cloud or vendor measurement follows.
Max logical13 coalesced ranges/15449460B; physical S3 I/O is unmeasured.
3172 prototypes/9744384B centers are diagnostic payload, not serving RSS.
Raw prototype norm min/p05/median/p95/max:
0.783881645/0.814948671/0.863710716/0.911113802/1.000000009.

Synthetic checks cover positive-scale invariance, direction-vs-length ranking,
zero direction rejection and extent-parent aggregation/ties. Probe/scorer
identities match repository helpers; source/order/request/GT identities sealed.
Original terminal/per-query counts replayed. The fixture changes no production
library defaults. Earlier squared-distance negatives remain immutable.

Decision: stop centroid-minimum nomination variants. The source layout can
contain neighbors but summary direction ranking under this fixed byte budget
cannot discover them. A materially distinct boundary-coverage or compressed
row nomination design is needed before any graph/format/serving build. Review
this new terminal evidence and reconcile previous failed arms before choosing
one cheapest source-only/development falsifier; no parameter sweep or cloud.
Both-vendor release and100M/lifecycle gates remain unresolved.
