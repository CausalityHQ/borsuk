# V122 D96 development screen before untouched cross-corpus quality

Status: preregistered before reading any deep-image query values. V120's
already active 10M cell constructs a source-only index; it is not a quality
promotion. Start V122 only after V120 reaches a terminal and its Spot is
terminated. Do not launch V121's first-1,000-query untouched evaluation
unless V122 passes this cheaper gate.

Use V119's authenticated 9,990,000-row, D96 unit-normalized source
(`source.parquet` SHA-256
`8f88122f412554107d97c07f440352f9043b8cb4b58fe08434ac75f4b90776ee`).
Choose 100,000 distinct training ordinals by
`numpy.random.default_rng(122001).choice(9990000,100000,replace=False)`;
sort them and remap to contiguous subset IDs 0..99,999. Seal the original
ordinal map and subset source SHA before any query download. Build that
subset with **the same V120 layout, SQ8, balanced PQ64, page geometry and
router rule**, including `min(N,ceil(8192*(N/1000000)^(1/3)))` clusters,
the same seeds and 12 Lloyd iterations. Fit no parameter from queries/GT.

For development queries, use the publication-v3 deep-image query object
`test.parquet`, SHA-256
`296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4`,
and **only** ordinals 9,000..9,999. Reject nonfinite/zero vectors and
normalize to unit length. Recompute exact cosine GT100 within the sealed
100k subset, using float32 dot products and stable `(descending score,
ascending subset ID)` ties. The published whole-corpus `neighbors.parquet`
is not valid GT for this subset and must not be read. None of V121's first
1,000 test query values or GT rows enter this screen.

Use 256-row SQ8 pages, two PQ64 block summaries/page, region count
`ceil(page_count*1024/3907)` (103 at 100k), 512 PQ64 nominees, exact local
SQ8 top-100 primary, 513/1 votes and the exact weighted interval planner.
The paired control uses V109-style minimum-PQ-score ranked-page admission
on the same nominee roster. Score returned SQ8 rows for both arms; allow at
most 32 GETs and 16,777,216 bytes. Record GT100 returned hits, p05,
sub-90 queries, paired wins/ties/losses, physical coverage, actual planned
GETs/bytes and source/build/query resource use. The screen passes only if
candidate returned Recall@100 >=99.0%, candidate total and p05 no worse
than its same-run control, p05 >=90, sub-90 count no worse than control,
and zero cap violations. A failure rejects this method before V121's
untouched split; diagnose representation, nomination, layout or planning
from the paired records and change the responsible generic layer. A pass
permits V121 but does not establish 10M quality or latency.

Run one immutable Causality Spot cell with a 7,200-second science cap,
interruption handling, terminal marker, resource logs and immediate Spot
termination. Monitor incomplete work by terminal and infrastructure only.
The V120 10M index is a separate construction artifact and cannot be used
as this 100k screen's index.
