# V283 source-only semantic-cell page oracle

Status: preregistered after the complete V282 a0002 KILL, before any V283
source layout or query oracle. This is one physical-layout hypothesis, not a
parameter sweep or an S3/vendor benchmark.

**Hypothesis.** V120's 3,803 source-only clusters across 100,000 rows mix many
small clusters per 256-row page. Fit `ceil(N/256)` source-only k-means cells
using V120's unchanged seeds/iterations and stable physical ordering, then
pack exactly 256 physical rows per page. A page may straddle a cell boundary;
this probe measures whether that simple locality change alone can raise
CoHere GT100 page coverage under the same V282 fetch caps. It does not repeat
V139's unbudgeted threshold, V146's flat 1M serving scan, or V149/V150's
page-plan capture proxy. ReLAION and CoHere use the same rule.

**First cheap falsifier.** Use the sealed V248 CoHere first100k D768 cosine
raw source (SHA-256
`0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e`)
and its GT100 (SHA-256
`06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8`).
The source-only layout cannot read queries or truth. On development ordinals
0–63, an exact dynamic-programming diagnostic chooses the GT-aware best set
of at most 32 contiguous page ranges and 84 pages, which is within 16,777,216
SQ8 bytes at 256×780 bytes/page. This is an *optimistic upper bound*, not a
serving route. KILL before PQ training if mean fetched GT100 upper bound is
<98.7 or p05 <95. Even a pass cannot certify recall: it only permits one
paired V282-router/replay check on both 100k datasets with unchanged PQ policy,
32 GET/16 MiB and 98% mean/p05 95/flat-loss <=0.5 pp gates. Stop the local
probe if it takes more than 15 minutes or exceeds 4 GiB process RSS; no new
paid instance. A failed bound changes the physical cell/router representation,
not the page cap or PQ-region parameter. A passing full two-dataset replay
would permit one cold1M HTTP gate from the same frozen revision.

## Completed local development oracle (2026-09-27 UTC)

The source adapter reproduced V282's sealed CoHere source and provenance
SHA-256 values exactly (`f779b8b64722277273d41f684acbb66d82d9613745162af534dac478c1cfed00`
and `826945ac71ee9496f512e70a48dfd14b1f8c15db4669a6d5bff193ccec0aa730`).
The candidate layout SHA-256 is
`303f31ab8a182a0aaa304c4ef551a046be41071ac24e67a793882eb74c5b532e`;
its manifest is [v283-cohere-100k-layout.json](v283-cohere-100k-layout.json).
On the preregistered CoHere development ordinals0–63, the GT-aware oracle
found all 100 GT rows/query within the 32-range/84-page ceiling: mean and
p05 upper bounds are both 100. Its receipt is
[v283-cohere-dev64-oracle.json](v283-cohere-dev64-oracle.json), SHA-256
`9e2edaa98d18df78045e24e4d8a3ed2fdacf614e97a4a2213d9310c3abe69d24`.
Source preparation, layout, and oracle took 3.10, 10.68, and 0.62 seconds;
their peak process RSS values were 876,216, 1,613,768, and 37,316 KiB.
No paid compute was started. This optimistic GT-aware bound passes its
necessary threshold, but it cannot certify PQ nomination, actual page
selection, SQ8 recall, latency, or any vendor comparison. The next
discriminating check is an actual source-only PQ-primary replay on development
queries with this frozen layout and unchanged V282 policy; no full panel or
cloud gate has been launched.

## Frozen next development replay

The completed V282 a0002 CoHere ordinals0–63 graph control has mean
R@100 96.25%, p05 92, and mean fetched GT100 97.046875; paired flat mean
R@100 is 96.046875%. Build the source-only V115 PQ router and V282 routing
metadata from the already sealed V283 CoHere layout, without changing their
training, seeds, query parameters, SQ8 scorer, or 32-GET/16 MiB limits.
Replay **only** CoHere development ordinals0–63 and their sealed GT100.
KILL this layout before any ReLAION or full-panel build if graph mean
R@100 is below 97.25% (less than a one-point gain over the same-query V282
control), p05 is below 92, or any query breaches either physical cap.
Report PQ-shortlist, fetched, and returned GT100 hits so the first failing
layer is visible. A pass is only permission for development0–255 on both
datasets; it cannot certify the 98%/p05 95 full gate, HTTP latency, or vendor
superiority. Local execution only, <=4 GiB process RSS and <=15 minutes;
no cloud instance or replacement run.

The exact local V115 trainer did not finish: one attempt was stopped after
1:32 because this devbox cgroup was under reclaim pressure (exit 143, peak
2,042,876 KiB RSS); after reclaiming only this cgroup's file cache, the same
source/layout/training rule hit a 12:00 timeout (exit 124, peak 2,033,360 KiB
RSS). Neither produced a sealed router or quality measurement. The 64-query
requests/truth slices are SHA-256
`1de0122f73d1b72e54498640b9701ce6d156b513629596447580c85fac302ba4`
and `f6630d0edf06539752c3fbf129ae01e58d3a3cf7b6aefa4decaa9c979e8ba355`.
No local retry is planned. A single c7i.8xlarge Spot development cell is
prepared as a conditional compute-only exception: same frozen V283 physical
layout, V115 PQ rule, V282 planner/scorer, and dev0–63 gate; 30-minute
launch-to-terminal limit 30 minutes, remote-run TERM at 25 minutes,
hard shutdown after 27 minutes of user data, quoted compute <=$0.40,
terminal artifact hashes,
original-launcher closeout and EC2 termination. It must not launch until the
operator answers the explicit compute request. A non-paid remote host can run
the same cell instead. Neither path permits a full panel or vendor claim.

The pending cell seals `borsuk-object-native-generation-v2`: its trusted root
binds the full S3 object key under the attempt prefix. The V282 a0002 v1 root
remains immutable historical evidence and is deliberately incompatible with
the v2 loader; V282 query and quality receipts remain the paired control.
