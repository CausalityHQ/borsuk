# V207 resident FP16 nominee diagnostic preregistration

## Decision and scope

Test a corpus-generic memory/recall tier: keep one source-only,
ID-addressed FP16 vector plane resident and score the complete 512-row
source-only V121 nominee set before choosing returned top-100. This
removes the V120 physical layout and 32-GET/16-MiB cap from final vector
scoring. The S3 generation remains the authenticated source of truth.
The user's RAM policy permits memory to scale with corpus size and target
recall, without a vector-count knee; this cell measures one operating
point and does not set a default or 100M policy.

Use the **already-used** Deep-Image-96-angular publication test-first-1000
queries on 9,990,000 source vectors. This is a layer diagnostic, not
untouched validation or a same-method cross-corpus claim. Compare with
the closed V206 same-range FP16 99,547/100,000 GT100 hits, p05 99,
12 below90, and V121 SQ8 98,034/100,000, p05 96, 12 below90. V121's
rosters contain 99,994/100,000 GT IDs; that is a nomination ceiling,
not a predicted returned score.

## Frozen method

Authenticate V119 source Parquet (3,566,768,562 bytes; SHA-256
`8f88122f412554107d97c07f440352f9043b8cb4b58fe08434ac75f4b90776ee`),
V120 layout (79,920,128 bytes; SHA-256
`419f9280d2e85f6fa275c115dd342c249ac31ec6af2d19a42f5b96c38ed247c1`),
V121 queries (2,013,436 bytes; SHA-256
`331310ae7abc3f0b73ea20010c7ee5a1de5ff04f5ef1d3e45b90dc513a48962d`),
V121 nominee rosters (SHA-256
`eb24f9a22d237db7416696415220e3fba559eadf7c7165ac6c8fd230a1d91700`),
and V206 pretruth returned IDs (SHA-256
`3fb5ef49833d672b82346612bbd80d0d50cceaed72d5de1d6b5da797c5762dd0`).
V121 replay SHA-256 is
`ddc9af991bdc6d3ef77d34a156994daa43aeb78f67f18de2cd0dc5ebb93abe91`.

Build a contiguous source-ID-addressed FP16 plane by deterministic
float32→float16 rounding from authenticated train embeddings. Expected
payload is 9,990,000 × 96 × 2 = 1,918,080,000 bytes. Authenticate
the resulting plane before resident scoring. For each query, map the 512
V121 physical nominee ordinals through the authenticated V120 layout to
source train IDs. Rank all 512 from the resident plane by float64 cosine
and stable train-ID ties. Also rank the same IDs from source float32 as a
precision ceiling. Emit both returned top-100 lists, the V206 same-range
FP16 list, and per-query scoring time before opening GT. No query or GT
enters plane construction or routing; no dataset-name branch, query
exception, or fitted parameter is allowed.

Upload raw IDs and a SHA-256 pretruth seal to S3 before downloading the
publication GT Parquet (4,003,585 bytes; SHA-256
`d305fcea7387988941defd2942cca1673693271329f977ba073da888cac3de8d`).
Then count GT100 intersections, p05, below90, paired wins/ties/losses,
and FP16 versus float32 losses. Independently re-read all closed IDs and
GT to verify the reduction. Report process RSS and phase timing, with
the FP16 payload, loaded process RSS and temporary build peak separated.
The query-stage time excludes nomination, routing, S3 generation load,
concurrency and mutations; do not label it end-to-end latency.

The representation/architecture screen passes only if resident FP16
returns at least 99,900/100,000 GT100 hits, p05 at least 99, zero
below90, and beats V206 same-range FP16 on paired hits, with all 512
nominees and zero invalid IDs for every query. Failure must be decomposed
into nominee absence, FP16 score displacement or implementation error.
Passing authorizes a production Rust resident-plane path and untouched
Deep-Image ordinals, plus a matched ReLAION source-only build. It does
not authorize a 100M claim, a release default, or a comparison to S3
Vectors/Turbopuffer without paired conditions.

Run one Causality Spot attempt from a pushed source with a 7,200-second
wall cap, interruption discard/restart, terminal artifact readback and
immediate termination. Monitor only terminal/infrastructure while the
cell is incomplete. Do not start an overlapping build or full suite on
the local devbox.
