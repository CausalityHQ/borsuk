# V290 sublinear precursor coverage bound

Status: frozen before reading V290 query outcomes. V289's global 200-byte
rotated two-bit row scores fetched 99.953125 CoHere GT100 mean/p05 100 under
the V283 84-page/32-GET/16-MiB plan, but scan all rows. This cheapest
necessary screen asks whether the existing **eight V283 source-only unit
centroids per page** could first restrict expensive row-code scoring to
`ceil(8 sqrt(page_count))` candidate pages. At 100k this is 159 of 391 pages;
at a projected 100M/256-page geometry it is 5,000 of 390,625 pages, or
1.28% of rows. That is a work-count projection, not a 100M performance or
quality result. V284's final 84-page centroid plan and V286 cosine variant
failed; this tests a materially larger *first-stage candidate set*, not a
retuned final fetch or a rerun of those arms.

Use authenticated V283 f16 centroids, layout, CoHere first100k D768 cosine
k100 development0–63 requests/truth. Rank pages by the minimum squared
Euclidean distance to their eight unit centroids with stable page ties,
take exactly 159, and count GT100 rows whose physical page is retained.
No two-bit row scoring, SQ8 plan, S3 read, or validation query occurs here.
KILL this precursor if mean candidate GT100 <98.9 or p05 <96; otherwise
allow exactly one same-candidate V289 two-bit refinement and returned-SQ8
development check. A pass cannot certify sublinear *first-stage search*:
the prototype script scans all 391 pages, and any product implementation
must bound that search independently. One local run <=2 minutes/2 GiB RSS;
no cloud or full panel.
