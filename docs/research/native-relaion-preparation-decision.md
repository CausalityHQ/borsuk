# ReLAION preparation and builder checkpoint

ReLAION first100k D768 source was prepared on Spark with the unchanged
V283 source-only layout rule. Normalized source/provenance and reproduced
GT100 match authenticated V282 identities exactly. No query/GT entered layout
or the Rust generation builder. Original source preparation exit0:9.99s,
2,044,768KiB peak RSS. GT plus development0–63 page oracle exit0:4.33s,
2,976,880KiB. Zero swaps; no new paid/cloud instance.

The optimal page-coverage **upper bound** is100mean/p05 GT hits under
32GET/84pages. This diagnostic does not measure recall and does not direct
production queries. **GO to actual quality testing**, not release or scale.

The new thin Rust CLI exposes the already-tested public generation builder.
Its focused parser test passes (wrong digest/cap/unknown-field rejection),
and both builder and plan binaries compile on Spark Rust1.98.1.
Full100k generation assembly completed exit0 in107.14s with27,120KiB peak RSS,
zero swaps and256MiB payload allowance. This is a debug build cost sample,
not a scalable build estimate or query/service performance measurement.
RootSHA c4c2a37be1fd2065980f2e23f21b167384786a3f2317e848ca6f8a04cbec0506
binds the source/plane/SQ8/centroids/graph. Its placeholder ETag is offline-only;
this root has not been remotely published or authorized to serve S3 data.

Next single gate: actual corrected cosine library plans and sequential-f32
SQ8/paired-flat recall on development0–63 for this exact root (mean>=98,
p05>=95,deficit<=0.5pp,32GET/16MiB). KILL on failure; pass allows short
validation then full744-query qualification. Full CoHere qualification already
passed on its separately pinned root; neither proves a vendor win.
