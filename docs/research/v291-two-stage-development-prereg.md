# V291 centroid shortlist, two-bit page rank, SQ8 return

Status: frozen before reading V291 outcomes. V289 globally scored all 100k
rotated two-bit rows and fetched 99.953125 GT100 mean/p05 100 on CoHere
first100k D768 cosine k100 development0–63. V290's fixed top-159 of 391
physical pages by existing eight source unit centroids retained 99.8125
GT100 mean/p05 99. This is the sole combined follow-up authorized by those
necessary bounds.

Keep V283 layout, SQ8 payload, authenticated source and query/truth; V289's
200-byte encoder and reconstructed cosine score; V290's 159-page Euclidean
centroid precursor; V284's final 84-page/32-GET/16,777,216-byte scheduler.
For each query score *only* records in the 159 candidate pages, select final
pages by V289 page maximum, and rank all rows in the covered SQ8 pages using
V282's stored norm, `low`/`step`, and squared-L2 formula. No GT affects the
route. Report candidate, fetched and returned GT100, p05, planned GET/bytes,
maximum coded rows scored, offline wall/RSS and paired V283 flat development
control (96.703% returned). The returned rank and planner are diagnostic
NumPy arithmetic; a near-gate result requires bit-exact Rust replay.

KILL if fetched mean <98.9/p05 <96, returned mean <98.0%/p05 <95, or any
plan >32 GET/16 MiB. A pass permits one Rust implementation and source-only
ReLAION+CoHere paired validation, followed by a cold 1M HTTP gate only after
that passes. This 100k route still flat-scores all 3,125 centroids, so a pass
cannot certify 100M work, latency or cost; hierarchical first-stage search
must meet a separate visit and recall gate. One local run <=300 seconds and
2 GiB RSS; no new cloud or full panel.

## Coefficient correction before a valid SQ8 decision

The first local command exited 0 but its returned score is invalid: the script
derived SQ8 `low` and `step` from the authenticated *raw* CoHere input, whose
sample vector norms are 12.53–15.81. V282 normalized that input before
encoding SQ8; sample stored SQ8 norms are 0.999–1.001. The first output SHA
is `ceec9e3a1bb04723444f9787733d800f6bc2df2291f74777b0effcc0afbee80c`;
it reported 99.765625 fetched GT100 but only 91.375 returned, and must not
be used as a quality measurement or KILL decision. The frozen route and
thresholds remain unchanged. The correction loads `low`/`step` from sections
whose hashes are bound by the authenticated V283 router manifest, exactly as
the Rust scorer does, before one replacement local replay. The first command
took 14.43 s/874,832 KiB RSS, exit 0; its flaw is semantic, not a crash.
