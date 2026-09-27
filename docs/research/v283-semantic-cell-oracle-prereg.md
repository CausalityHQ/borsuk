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

## a0001 terminal decision (2026-09-27 UTC)

**KILL V283 semantic-cell layout for further panels and 1M.** The sole frozen
CoHere first100k D768 cosine k100 development0–63 cell completed with exit 0.
Source commit `9f8007a507e06970639a71432544d7fe0a22bf63`, archive SHA-256
`f6f748fa76f0b26d618b38da8d3391dd916bfcb0c5cf89075012fe06ba384ea6`,
layout SHA-256 `303f31ab8a182a0aaa304c4ef551a046be41071ac24e67a793882eb74c5b532e`,
and SQ8 SHA-256 `301696df05ca03122951b66ad8a9bedb5d5f1e675c6fc66f6019abbce3fcda58`
match the reservation. The runner authenticated the V248 raw vectors, requests,
truth, layout and SQ8 input before replay. The original launcher independently
replayed all 11 terminal artifact sizes and hashes; the terminal SHA-256 is
`2069defa6ff7dfacdca20e09e62f4bb01d65aeb9625aa30cd253313bb177c58f`,
closeout SHA-256 `fde4f369d2cace30729f1ea7302fa9491125f4b426e25e295a258066c122d057`,
and candidate raw SHA-256 `2d5e5c03fd333c3475c771ddaf45e2b47e30dbef6013460c521647ca953394b1`.
The [exact summary](v283-cohere-dev64-a0001-summary.json) is copied from the
authenticated terminal artifact (SHA-256
`a50c022e483c60f16270e62fb21c7d7dc7dd4cd9eb77a5dbde17b198def163f5`).
Raw receipts and terminal are preserved under
`s3://borsuk-bench-453182569524-euc1/research/v283-cohere-dev64/9f8007a507e06970639a71432544d7fe0a22bf63/runs/a0001/`.
Spot `c7i.8xlarge` instance `i-026d77be53dc1fda8` is confirmed terminated;
compute through terminal is estimated at $0.07766 at the $0.7262/hour quote,
excluding storage and request charges.

| Same 64 CoHere development queries | PQ shortlist GT hits/query | Fetched GT hits/query | Returned R@100 | p05 hits | Max planned GET / bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| V282 graph paired control | 84.328 | 97.047 | 96.250% | 92 | 32 / 16,773,120 |
| V283 graph candidate | 82.922 | 96.672 | 95.984% | 93 | 32 / 16,773,120 |
| V283 flat page-discovery diagnostic | 82.922 | 97.625 | 96.703% | 93 | 32 / 16,773,120 |

The preregistered one-point improvement gate required at least 97.25% mean
returned recall and p05 at least 92. The graph candidate misses the mean gate
by 1.266 percentage points and is 0.266 points below the paired V282 control.
The flat diagnostic is also below 97.25%. The optimistic GT-aware page oracle
of 100/100 was therefore insufficient: unchanged PQ nomination on the new
layout loses 1.406 GT hits/query versus V282, and graph page discovery then
loses 0.719 returned hits/query versus V283 flat. SQ8 scoring loses another
0.688 hit/query after V283 graph fetch. The first causal deficit is PQ-primary
nomination/physical-page coverage, with an additional graph-discovery deficit;
neither widening a reused panel nor a 1M run is justified. The 64-query replay
took 3.26 s with 102,600 KiB peak process RSS; it is **local replay, not live
S3 or end-to-end latency**, and cannot support a vendor claim.
