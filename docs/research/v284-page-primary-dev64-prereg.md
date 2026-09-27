# V284 page-primary development falsifier

Status: frozen before reading V284 development coverage. V283 a0001 is a
terminal KILL: its unchanged row-PQ shortlist contains only 82.922 of 100
CoHere GT rows/query and returned recall is 95.984% on dev0–63. The V283
GT-aware page oracle is 100/100, so this experiment changes **only the
query-side page nomination representation**. It reuses V283's authenticated
semantic physical order, SQ8 plane, eight f16 unit centroids per page, and
CoHere first100k D768 cosine k100 development ordinals0–63. No vector or page
format, query split, training data, page budget, or scorer is tuned here.
The completed V283 generation archive SHA-256 is
`42b0ddef9488f2ad30ba6aa22b40f2472e8a37c36427db3d27bd618073d70510`;
the root-digest file SHA-256 is
`7158e40299caba8a520caf79866262982e6a932b593d8a389caeb494ad9545d7`.
Development request and truth SHA-256 values are
`1de0122f73d1b72e54498640b9701ce6d156b513629596447580c85fac302ba4`
and `f6630d0edf06539752c3fbf129ae01e58d3a3cf7b6aefa4decaa9c979e8ba355`.

Rank all 391 physical pages by their minimum squared Euclidean distance to
the query among the eight source-only f16 unit centroids. This is the V146
flat page score on V283's different physical layout, used **only as a cheap
quality upper screen**; V146's full scan failed the 1M CPU gate and is not a
production route. Greedily add pages in ascending score, tying by page ID.
For each tentative selection, bridge the smallest physical gaps, tying by
lower page ID, until at most 32 contiguous GET ranges remain. Reject a page
if that cover would exceed 84 physical pages (at most 16,773,120 planned SQ8
bytes). The selected physical ranges, including bridges, determine fetched
GT100 coverage. Query or GT data cannot change centroids, ranks or budget.

**KILL this page-primary representation** if fetched mean GT100 is below 98.7
or p05 below 95 on the 64 queries. A pass only permits one source-only,
sublinear page-router implementation and same-query returned-SQ8 check; it
does not validate V283, certify 98% returned recall, 1M CPU, S3 latency or a
vendor comparison. Read only the already authenticated V283 generation
artifact, CoHere development requests/truth and physical layout. No cloud
job, full panel or new training. Record mean/p05 fetched GT, selected and
bridged pages, GETs and bytes. Stop local computation at 2 minutes or 2 GiB
process RSS. Any numerical ranking close to the gate requires replay in the
Rust scorer before a decision.

## Completed development decision (2026-09-27 UTC)

**KILL this page-primary representation.** The frozen local script at source
`f99f35d8` completed exit 0 in 0.64 s, peak process RSS 53,156 KiB. Its
[machine-readable result](v284-page-primary-dev64-result.json) has SHA-256
`40318cc014a56ad8eeb837d21e3f649c9ff87198e32facc4ad214e4c56f85f2d`.
The archive, root, layout, request and truth digests matched the preregistration;
the trusted root authenticated the f16 centroid plane. With 84 selected and
fetched pages/query, at most 32 planned GETs and 16,773,120 planned bytes,
fetched GT100 coverage was **97.0625 mean, p05 92** on CoHere first100k D768
cosine k100 development0–63. It misses the 98.7/p05 95 necessary gate by
1.6375 mean hits and 3 p05 hits. The result is far from the threshold, so
the preregistered Rust ranking tie check is unnecessary for this KILL.

The V283 physical layout's 100/100 GT-aware oracle is attainable only with
query-informed page choice; direct source-only f16 centroid page scores do not
find those pages. The V283 PQ-seeded flat diagnostic fetched 97.625, also
below 98.7. Neither row-PQ nomination nor page-centroid nomination on this
layout merits a sublinear implementation or a larger run. This result is a
development-only fetched-coverage diagnostic, not returned recall, live S3
latency or a competitor comparison. The next candidate must materially change
the source-only representation/layout relationship, with a cheap quality/I/O
falsifier before any cloud or full-panel work.
