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
