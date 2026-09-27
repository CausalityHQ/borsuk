# V285 exact page-rank diagnostic

Status: frozen after V284 KILL and before computing V285 scores. This is a
query-side **upper diagnostic**, not a source-only ANN candidate. On the same
CoHere first100k D768 cosine k100 development ordinals0–63 and immutable V283
physical layout, score each page by the cosine similarity of its single
closest exact source vector to the query. Rank pages by that score, stable page
ID tie break, then apply V284's unchanged greedy 32-range/84-page scheduler.
Truth is read only after page selection to count fetched GT100 rows. Input
SHA-256 values: V248 raw source
`0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e`,
V283 layout
`303f31ab8a182a0aaa304c4ef551a046be41071ac24e67a793882eb74c5b532e`,
development requests
`1de0122f73d1b72e54498640b9701ce6d156b513629596447580c85fac302ba4`,
and truth
`f6630d0edf06539752c3fbf129ae01e58d3a3cf7b6aefa4decaa9c979e8ba355`.

This isolates V283/V284 **page scheduling and physical layout** from compact
router approximation. If even exact nearest-row page ordering fetches below
98.7 mean GT100 or p05 below 95, KILL the 84-page/32-range scheduler on this
layout before training another descriptor. If it passes, compact page
descriptors remain plausible but are unproven; one distinct source-only
descriptor must pass the same fetched threshold on development queries before
returned SQ8 or any cloud gate. The prior GT-aware oracle100 may choose pages
with truth and therefore does not answer this question. This exact scan is
O(ND) per query and cannot be a 1M/100M serving route. One local run only,
<=2 minutes and <=2 GiB process RSS; no full panel, paid machine or vendor
claim. Compute p05, mean, max planned GET and bytes, and the gap to V284.
