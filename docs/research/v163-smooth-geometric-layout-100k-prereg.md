# V163 smooth k-means layout 100k screen

## Decision

V161/V162 rejected balanced-two-means/512-row physical admission at 1M.
Test a **different source-only physical order**: the V120 k-means plus
centroid-chain rule, generalized to L2 and arbitrary unique stable IDs.
The cluster count is the smooth function
`K(N)=min(N,ceil(8192*(N/1,000,000)^(1/3)))`; it yields 3,803 at 100k
and 8,192 at 1M, with no corpus-name branch or vector-count knee.
The seed, 12 Lloyd iterations and centroid-chain ordering are fixed by
V120 before this gate. For cosine, normalize source vectors before fitting;
for L2, use raw vectors. This gate uses the frozen ReLAION-100k L2 source.
No query or truth enters construction.

## Frozen paired method

Use the same authenticated ReLAION-100k D768 development-1000 source,
V114 source-order SQ8 rows, V114 PQ64 nominee and exact-SQ8 primary
rosters, GT100 and V160 terminal/raw control. Authenticate every input
length/SHA-256 and the V160 complete terminal. Fit V120's `_fit_order`
on the source vectors with K=3,803; seal its source-ordinal permutation,
old SQ8/manifest, relaid SQ8 bytes, metric, K rule and seed before any GT
is read. Preserve every old SQ8 row byte. The generic page-width rule
from V160 stays 512 rows at 32 GETs/16,777,216 bytes/D768. The only
causal change against V160 is the source-only physical ordering.

Map the identical frozen 512 nominees and exact-primary-100 source
ordinals through the sealed permutation. Use the unchanged 513/1
weighted interval planner, 32-GET/16-MiB cap, SQ8 returned scorer and
stable-ID tie rule. Plan all 1,000 queries GT-blind and hash a plan seal;
only then download truth and the V160 raw control. Independently replay
every relaid row, plan, returned ID, GT hit, resource count and gate.
Publish mean/p05 Recall@100, mean Recall@10, physical GT coverage,
distinct exact-primary pages, bytes/GETs and per-query paired wins.

The direct control is V160's **99.170%** returned Recall@100, p05 97 and
99.45% mean Recall@10 on this same used cohort. Advance the smooth
layout as a transferable candidate only if it clears the standing 100k
floor of 97.5% mean Recall@100, p05 90 and mean Recall@10 96, with no
cap or independent-check failure. Report whether it also equals/exceeds
V160's quality and resources; a floor pass alone is not a 100k winner.
Only a pass permits a separately frozen 1M transfer against V155's
99.567% exact-source baseline. Neither used split is a release claim.

One immutable Causality Spot attempt is permitted, with a hard wall cap,
interruption as discard/restart under a new attempt, terminal even on
failure, postterminal S3 length/SHA readback and immediate termination.
Do not use local full builds/tests under devbox swap pressure. A higher
recall profile may explicitly allow more RAM, but no dataset-specific
switch or unsupported empirical recall guarantee is allowed. Lean may
prove the conditional page/GET/byte and memory arithmetic; quality and
latency require measurements.
