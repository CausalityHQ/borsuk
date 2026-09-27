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
