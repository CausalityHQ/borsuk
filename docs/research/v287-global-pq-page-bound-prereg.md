# V287 global PQ64 page-ranking diagnostic

Status: frozen before computing V287 query outcomes. The completed V283
generation already contains authenticated PQ64 books and physical row codes.
V282/V283 truncate row nomination and V284/V286 unit-mean page scores fail,
while V285 exact-row page ranking fetches 100/100 CoHere GT rows under the
same 84-page/32-GET cap. This **offline upper diagnostic** scans all 100,000
PQ codes per query, takes the best approximate row score in each physical
page, and runs the unchanged V284 greedy page scheduler. It isolates compact
code quality from source-region/shortlist truncation. The full code scan is
not a 1M/100M product route.

Two paired scores use exactly the same books, codes, page layout, CoHere
first100k D768 cosine k100 development ordinals0–63 and physical scheduler:
(1) V282's squared Euclidean ADC on the PQ reconstruction as control;
(2) cosine ADC on the same reconstruction, changing only normalization of
row and query scores. The generation archive, root, router manifest and all
section hashes are authenticated before reading queries/truth; the existing
V283 development request and truth hashes are fixed. Sort pages by the best
row score, stable page-ID ties. Use at most 84 physical pages, 32 GETs and
16,777,216 planned bytes; count GT100 only after page selection.

The necessary gate is fetched mean >=98.9 GT100 and p05 >=96 for either arm,
allowing roughly one hit for SQ8 scoring loss before a 98%/p05 95 returned
gate. If both arms fail, the PQ64 representation itself is too lossy on this
layout and the next design must encode more row geometry. If Euclidean passes,
the region/shortlist truncation is the first target; if only cosine passes,
the score metric is. A passing arm merely permits one bounded, sublinear Rust
route and same-query SQ8 returned check; no 1M, S3 latency or vendor claim.
Numerical outcomes close to threshold require exact Rust score replay.
One local run only, <=2 minutes, <=2 GiB RSS; no training, cloud or full panel.
