# V216 cosine PQ graph method-held-out 100k closeout

**Decision: pass the frozen held-out gate.** The V215 selected
ef_search=2048, FP16 shortlist=2048 method is qualified for one
frozen ReLAION-1M serving/recall/resource comparison. Query ordinals
256–999 were held out from this method's arm selection, but appeared
in older BORSUK campaigns and are not globally pristine.

The sole Causality c7i.4xlarge Spot attempt `a0001` ran on
`i-0c46694312a110516` and is terminated. Source commit
`bf5db61a8f0f84543e57338be224e8636a41a170`, archive SHA-256
`1b55bbf4def489bc2a77e42b0fdd34ebd42e8b96edf96a6d5393f9ef87e7a318`.
Terminal SHA-256
`2bbba15b52ee361ba5b5c4fdb9cc73637344b3ed684d8ee157f6513a924c92c6`
is at `s3://borsuk-bench-453182569524-euc1/research/v216-pq-cosine-heldout-100k/bf5db61a8f0f84543e57338be224e8636a41a170/runs/a0001/terminal.json`.
It reports complete, exit 0; all seven terminal artifact lengths and
SHA-256 hashes passed readback. V214 parent artifacts and V215's
selected-arm terminal were verified before launch; V216 raw returned
IDs were sealed before truth and V193 baseline download.

Dataset/split: **ReLAION-100k D768, method-held-out queries
256–999 (744 queries)**, GT100. The frozen graph/PQ-cosine/FP16
method returned **74,048/74,400 GT100 hits**, versus paired V193
full-rank SQ8 **73,979/74,400**. Both have p05 **98 hits/query**.
V216 has 19 queries below 98, and paired wins/ties/losses
**212/404/128**. The hit advantage is 69 on this panel.

Whole in-process sequential Rust p50/p95/p99 was
**6.719/8.111/8.529 ms**; throughput was **147.93 queries/s** by
summed request time (147.47 queries/s over the full 744-request loop).
The p95 graph base-layer work was **22,580 visited rows**. Peak
serving RSS was **204,488,704 bytes**, including **30,889,640 bytes**
loaded graph heap, **154,400,000 bytes** FP16 plane,
**6,400,000 bytes** PQ codes and **400,000 bytes** PQ cosine-norm
sidecar. Cold hydration took **0.357 s**; vector-body GETs were zero.

The frozen gate required at least paired V193 aggregate GT hits,
p05≥98, whole p95≤10 ms, peak RSS≤320,000,000 bytes and zero
vector-body GETs. Every condition passed. These timings exclude
network front end, concurrent admission, mutation, generation swap,
S3 transport and index build. They are not end-to-end product or
competitor latency.

The Spot instance launched 2026-09-25 09:43:21 UTC; terminal landed
09:47:45 UTC, 264 s later. EC2 Spot history for eu-central-1c gave
**$0.3644/hour** effective at launch; launch-to-terminal compute-only
cost is **estimated $0.0267**, excluding termination tail, EBS, S3
and taxes. This is not a billing measurement.

Next: build the same source-trained graph and PQ-cosine/FP16 path on
ReLAION-1M D768 validation-1000, freeze the corpus/recall work
budget before truth, and compare paired GT100 quality and full
in-process request p50/p95/p99, QPS, RSS and build cost against the
strongest V199/V155 BORSUK evidence. A 1M pass would still require
an actual network service and matched Turbopuffer/S3 Vectors runs.
