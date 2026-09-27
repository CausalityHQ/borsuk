# V293 bounded centroid candidates with fixed two-bit/SQ8 score

Status: frozen before V293 outcomes. V292 candidate pages are sealed at SHA
`c24e53f8e740ad569331c774f46ef3161873e3a765d20a345290af50a1b974d0`:
CoHere first100k D768 cosine k100 development0–63, 159 pages/query, <=1,400
centroid evaluations, mean candidate GT100 99.578125/p05 98. V291's flat
candidate route returned 98.1875/p05 96 with exact Rust SQ8 score and the
same source/layout/payload.

Replace only V291's flat centroid candidate sets with those exact V292
sets. Reuse its source-only 200-byte rotated two-bit codes, reconstructed
cosine page maximum, stable page ties, 84-page/32-range/16-MiB greedy
physical plan, and production sequential-f32 SQ8 score with authenticated
coefficients. Preserve source/query/truth hashes. No parameter tuning.

KILL if fetched mean GT100 <98.9 or p05 <96, returned mean <98.0 or p05 <95,
any plan >32 GET/16,777,216 bytes, local process RSS >2 GiB, or the single
run exceeds 300 seconds. A pass freezes this one route for a self-contained
Rust code increment and paired ReLAION/CoHere 100k validation. A fail identifies
whether bounded page discovery, final selection or SQ8 ranking is the first
causal loss; no paid or scale run. Reuse V291's verified Rust arithmetic
relation; near-gate new plans require an exact Rust score replay. These are
local quality/planned-I/O data, not HTTP service latency or vendor claims.

## Near-gate scorer replay

The frozen `bbdae616` local combined screen completed exit 0 in 37.27 s
with 861,224 KiB peak process RSS. Its result SHA is
`f9fc36ce0c1a8b13aa9c03d568c41436a27545f6d0df65930a21fd40778c91c0`;
selected physical plan receipt SHA is
`00190ff7ed017b2150d1415f36077be774c0e59075ec0eb0e691f81511ae3aa4`.
Fetched GT100 mean/p05 is 99.546875/98; returned is 98.015625/96, within
32 GET/16,773,120 bytes. The margin is one returned GT hit across64 queries.
Do not promote until the existing authenticated Rust SQ8 replayer agrees
on these exact page sets and returned counts. It now accepts a caller-pinned
plan digest so the same scorer can check this receipt without a copied binary.
No score, route, query or threshold changes are permitted by that adaptation.
