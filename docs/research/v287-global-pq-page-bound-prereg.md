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

## Completed development decision (2026-09-27 UTC)

**KILL global scoring of this PQ64 code plane as a direct page router.** The
frozen `17a7eb06` NumPy diagnostic completed in 2.21 s with 47,692 KiB peak
process RSS. Its [result](v287-global-pq-page-bound-result.json) SHA-256 is
`c4915733af9c07f5ac830368a06419a233e85b6b6c1d2fcd1e8a7805625766f3`.
Because the Euclidean arm was only 0.30625 hits/query below the mean gate,
the exact `Pq64Router`/`Pq64CosineView` arithmetic was independently replayed
in Rust on the same authenticated router, SQ8 IDs, requests and truth.
The [Rust result](v287-rust-pq-page-bound-result.json) SHA-256 is
`6ce75aab64d057e834d0baadb724ca3e9c2738a985549dddb55eee1be208f28c`;
its mean, p05, GET and byte results match the NumPy diagnostic exactly.
The focused Rust binary built and completed exit 0 in 25.76 s with 91,804
KiB peak process RSS; this is diagnostic wall time, not serving latency.

| CoHere first100k D768 cosine k100, development0–63 | Fetched GT100 mean | p05 hits | Max planned GET | Max planned bytes | Frozen gate |
| --- | ---: | ---: | ---: | ---: | --- |
| Global PQ64 Euclidean page minimum | 98.59375 | 94 | 32 | 16,773,120 | KILL |
| Global PQ64 cosine page maximum | 98.390625 | 93 | 32 | 16,773,120 | KILL |
| V285 exact-row cosine page maximum, upper diagnostic | 100.000 | 100 | 32 | 16,773,120 | Bound only |

The PQ64 code plane loses at least 1.40625 fetched GT hits/query to exact-row
page ordering even without source-region or shortlist truncation. Cosine
normalization does not repair this. No sublinear router over these same codes,
larger panel or 1M run is promoted; the next candidate must materially improve
the query-to-page representation and pass a development-only quality/I/O gate
before changing the production format. This used-query coverage screen is not
returned SQ8 recall, live S3 latency or a competitor result.
