# Semantic router: coverage gate survives both datasets

Verified FIRST100k D768 cosine, consumed development ordinals0–63,64 queries per
dataset. Exact pinned truth is the evaluator reference; this is not a fresh
matched ANN control, returned recall, serving latency or vendor comparison.

| Dataset | Selected-unit truth@10 | Page-closure truth@10 | Selected-unit truth@100 | Page-closure truth@100 | Unit truth@10 p05 | Closure truth@10 p05 |
|---|---:|---:|---:|---:|---:|---:|
| ReLAION |97.65625%|98.4375%|95.6875%|97.34375%|80%|90%|
| CoHere |95.78125%|96.875%|94.46875%|96.109375%|70%|80%|

The frozen mean closure@10>=95% ceiling passes both. This earns the unchanged
source/scorer test; it does not establish actual returned-quality survival.
The weaker tails and R100 loss remain visible. Do not turn the old98%R100
stretch diagnostic into a universal product KILL or retroactively alter any
historical FAIL.

| Dataset | Root bytes | Leaves | Max selected leaf payload bytes | Max modeled leaf reads | Mean closure rows | Max closure pages |
|---|---:|---:|---:|---:|---:|---:|
| ReLAION |752232|75|1359820|16|27327|154|
| CoHere |736275|75|1255100|16|26618|120|

Router root/leaf caps pass. These are modeled router reads; physical S3 requests,
retries, bytes, source/SQ8 interval schedules and returned IDs remain unmeasured.
Do not treat154 source pages as154 physical GETs; the exact bounded cover must
be planned and charged. Membership preload is12500 bytes; truth-order mapping
is800000 bytes and is evaluator-only. No full router hydration is earned for a
candidate serving path.

Synthetic maximum construction used32.36s,36784KiB maximum RSS and49606656B
cgroup peak. ReLAION/CoHere source-only construction used32.25s/32.11s in the
same dev-profile local binary under256MiB/zero-swap/two-CPU/900s limits.
These are construction measurements, not cold query latency or100M cost.
All four original local service/session handles are terminal. Original full
workspace2714passes/26ignored and final builder6tests, Clippy and full test
compilation are separately scoped and authenticated.

## Next decisive gate

Reuse the unchanged two-bit/SQ8 source scoring, logical-ID tie order and bounded
budget selection on the whole-leaf nomination. Test ReLAION first, then CoHere
with contemporaneous unchanged control. Decompose closure→source selection→SQ8
selection/quantization→returned truth losses. Require actual mean returned
R10>=95%, all calls successful, authenticated identities and frozen GET/byte
caps; report R100 and quality tails. If it fails, end the arm and name the layer.
Only a survivor earns object-native cold HTTP,8-QPS offered and total-cost
measurement. Current accepted FIRST1M graph-decode references remain stale for
this new100k comparison: R/C p90=1242.809/1289.024ms, not a matched control.

The flat100k research root is not scalable to1M/100M. A scale curve, bounded
incremental publication/GC/recovery and both-vendor matched outcomes remain open.
The floating-threshold concern was tested on Python3.14.4 and did not reproduce;
its speculative edit was reverted before CoHere evaluation. No result was rerun
or reclassified.
