# V210 resident neighborhood 100k falsifier

V209's ReLAION-1M 512-nominee resident path missed 2,792 GT100
positions versus the same-query V199 result. The historical 512
nominees themselves cover only 96,849/100,000 GT positions. Test one
material change: use each source-only nominee as a seed in the
authenticated relaid physical order and score FP16 vectors from the
nearest physical positions, with a fixed candidate budget. This
removes S3 range planning and tests whether locality can supply the
missing candidates at bounded resident scoring cost.

Use the already-used ReLAION-100k D768 development queries 0–255,
the frozen V114 512-nominee requests, V163 relaid physical order,
and corpus FP16 rounding from the authenticated source. For each
query, sort physical positions by distance to the nearest of its 512
mapped nominees, breaking ties by physical ordinal. Score fixed
prefixes of 512, 2,048, 8,192 and 32,768 positions by resident FP16
cosine, then return 100 IDs with stable ID ties. The method uses no
dataset-specific fitted coefficient, query exception or GT signal.
All four arms must be computed and sealed before downloading GT or
the V193 baseline rows. The 512 arm isolates FP16 from neighborhood
growth; all arms share the same source, request and order.

The first 256 V193 full-rank SQ8 returned results on those exact
queries are the paired internal quality baseline. A budget survives
only if it reaches at least the baseline's aggregate GT100 hits,
p05 at least 98, and FP16 score-only p95 at most 10 ms on one Spot
worker. Choose the smallest surviving budget without per-query
adaptation. This is a **falsifier**, not production latency, a
fresh-query qualification, or permission to tune on the 1M panel.
If no arm survives, reject the simple physical-neighborhood route
and choose a different candidate generator before another 1M run.
If an arm survives, implement it in Rust with authenticated generation
binding and test it on a fresh split before a new 1M product gate.

Run one bounded Causality Spot cell, preregister immutable inputs,
discard/restart interruptions, verify all terminal artifact digests,
and terminate compute immediately. No other BORSUK worker should
overlap. RAM at 100M remains a function of recall, corpus size and
concurrent generations, with no vector-count cutoff.
