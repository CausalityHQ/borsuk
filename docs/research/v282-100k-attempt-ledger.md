# V282 paired 100k attempt ledger

## a0001: input-preparation failure, no quality observation

Frozen source `f1252ec7e65dedb9eff60320663322e09d3b725b`, source archive
SHA-256 `8cd04be022782a4c45d5ffe4e869508d0bb7d35b33935bb569a01b508883b69b`.
One `causality` c7i.8xlarge Spot worker in eu-central-1c,
`i-0dbdf31ec468ea996`, is terminated. The authenticated
[terminal](s3://borsuk-bench-453182569524-euc1/research/v282-paired-100k/f1252ec7e65dedb9eff60320663322e09d3b725b/runs/a0001/terminal.json)
SHA-256 is `1e3b8150e7a427a00caade1ed28d4f6d54da699f866ba4343f880e6b33ecc02d`;
closeout SHA-256 is `f1f26447f52eb20cedb9d4b1f6a68e4a7b1f8f7e0eebdcaa7c981a568f1576b9`.
The original launcher verified its terminal artifacts and exited zero after
closing the failed worker. Spot quote was $0.7216/hour; compute through the
terminal marker is estimated at $0.039287, excluding storage and billing tail.

The worker stopped in `relaion-source`: the adapter required ordinal source IDs,
but the downloaded V36 1M source had nonordinal IDs. It produced no layout,
generation, GT, query replay or recall/latency samples. Its result is **invalid
input preparation**, not a V282 quality failure.

The next attempt changes only input identity and the source adapter. Use the
historical V85 ReLAION-100k Parquet SHA-256
`a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d`
(100,000 unique random stable IDs) and V114 request panel SHA-256
`b2485629b919614bf46877a779b16d678cd1690d1872b7d4f9c9cbe6ddd94eb0`.
Preserve original IDs in an authenticated map, assign ordinal IDs to the
V120/V115 physical builder, and compute fresh exact GT100 on that exact
corpus. The graph/flat comparison, split, policy, physical caps and pass rule
are unchanged. The focused random-ID conversion regression passes. Launch a
new Spot attempt only after source commit and dry run; do not restart a0001.
# a0002 terminal decision (2026-09-27 UTC)

**KILL V282 for scale/HTTP.** Source `e74d75acb40647f606b498023b11dec26701e4aa`,
archive SHA-256 `3facfaf935a3e4921466281e79afe15f8cc945e30870a97806f814845972998d`,
Spot instance `i-06c4c766fcf79f221`, terminal SHA-256
`23cf19cdc03c5ce2df116b3eeb70e57c57f9aeaf84ea95ab110dbe598f78b3bc`,
closeout SHA-256 `76697fa1aa5b8a3fb1aeac82364683874e628b692cf57c351613a5a44908354a`.
Terminal status `complete`, exit 0; original launcher replayed all 42 artifact
sizes/digests, published closeout, and terminated EC2. Estimated Spot compute
through terminal: $0.16617, excluding storage and requests. Prefix:
`s3://borsuk-bench-453182569524-euc1/research/v282-paired-100k/e74d75acb40647f606b498023b11dec26701e4aa/runs/a0002/`.

The sealed ReLAION first100k and CoHere first100k sources are D768 cosine,
k100. ReLAION V85 input SHA-256 `a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d`,
V114 requests `b2485629b919614bf46877a779b16d678cd1690d1872b7d4f9c9cbe6ddd94eb0`,
new exact truth `4bd3ac79fce3919f85359ce3e305491663991f6cc0890ab91682cda34247a24e`.
CoHere V248 raw input SHA-256 `0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e`,
requests `86d9406486a2bb27aa2e603f019e078dd3ecaed47f79ec685558ba3536433812`,
exact truth `06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8`.
Those identities were checked by the frozen runner; the completed evidence
manifests and raw query receipts match their terminal digests.

| Dataset / split | Graph mean R@100 | Graph p05 hits | Flat mean R@100 | PQ shortlist GT hits | Graph fetched GT hits |
| --- | ---: | ---: | ---: | ---: | ---: |
| ReLAION dev0–255 | 98.301% | 94 | 98.617% | 96.508 | 98.559 |
| ReLAION val256–999 | 98.250% | 93 | 98.523% | 96.954 | 98.499 |
| CoHere dev0–255 | 96.055% | 90 | 96.055% | 84.508 | 96.828 |
| CoHere val256–999 | 96.620% | 92 | 96.630% | 85.749 | 97.382 |

Frozen gate: graph mean R@100 >=98%, p05 >=95, <=0.5 percentage-point
deficit against paired flat, and every query <=32 planned GET/16,777,216
planned bytes. The physical caps passed (observed maxima 32 GET and 16,773,120
bytes); quality failed on both datasets. On CoHere, flat and graph are
essentially equal. The first decisive deficit is PQ-primary/physical-page
coverage: the 512-row shortlist contains only ~85 GT100 rows, and page
expansion reaches only ~97; SQ8 ranking then loses another ~0.75 hit.
Graph traversal is not the causal fix. Local planner/rank CPU and ~106 MB
process RSS are diagnostic only; this run has **no live S3 latency or vendor
comparison**. Next: one preregistered source-only semantic-cell page oracle,
then router replay only if its necessary coverage bound passes. No V282 1M/10M run.

Digest transcription corrected2026-09-28: the terminal reference omitted `c57`. Its64-character SHA was independently checked against the original S3 terminal and the unchanged closeout SHA recorded above. Historical artifacts and decisions are unchanged.
