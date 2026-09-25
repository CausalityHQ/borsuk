# V217 ReLAION-1M PQ-cosine graph closeout

**Decision: reject this 1M candidate-generation method.** The frozen
ReLAION-1M D768 validation-1000 cell completed, but none of its four
preregistered arms met the paired quality gate. Do not tune another
ef/shortlist ladder on this representation or claim a product latency win.

Source commit `47cbeeeceb49bf52b235d2017ee5a55297d080bf`, source
archive SHA-256 `4b4d7ff2bd2963172579f99cf95596b85e5598c360dcc856e815974ec552ce53`,
Spot attempt `a0001` on `i-0f6b246cd6109d5ed` in `eu-central-1c`, and
terminal SHA-256 `911cf3a55cf2b016f8df209dbd7ef52c56277b02532cfe80138c58f60912fb3a`.
The worker ended with `status=complete`, the launcher independently replayed
all 13 terminal artifact sizes and SHA-256s, and the instance was terminated.
Immutable evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v217-pq-cosine-graph-1m/47cbeeeceb49bf52b235d2017ee5a55297d080bf/runs/a0001/`.
Raw returned IDs were sealed at `sealed/raw.jsonl` before the V198 closed
GT100/V199/V155 witness was downloaded. The sealed raw SHA-256 is
`00ba8028e58c7349c7d65a8f1e2263fb07fd0efb02b4e67cfe8e8a16e891a460`.

| Frozen ef/FP16 shortlist | GT100 hits / 100,000 | Per-query p05 hits | Loaded p50/p95/p99 (ms) | Eight-worker completed QPS | p95 base visits | Gate |
|---|---:|---:|---:|---:|---:|---|
| 2,048/2,048 | 98,472 | 93 | 6.556/9.422/10.438 | 1,125.9 | 33,379 | Fail quality |
| 4,096/4,096 | 98,545 | 94 | 12.938/17.973/19.899 | 580.9 | 55,491 | Fail quality |
| 8,192/8,192 | 98,581 | 94 | 25.827/34.520/37.580 | 296.6 | 92,471 | Fail quality |
| 16,384/16,384 | 98,609 | 94 | 51.755/66.727/71.552 | 149.7 | 150,948 | Fail quality |

The same validation-1000 witness has V199's strongest BORSUK baseline at
**99,605/100,000 hits and p05 98**, and V155 at 99,567 hits. Even the
largest V217 arm loses 996 hits to V199, with 94 wins, 594 ties and 312
losses by query. Additional work yielded only +73, +36 and +28 hits as the
beam doubled. This supports a candidate-generation/score or graph-topology
bottleneck; these results do not distinguish those causes. V217 uses
PQ64-reconstructed cosine scores to traverse a source-cosine graph, then
FP16 cosine reranking. Its final FP16 reranker and V199's FP16 reranker
share the authenticated plane, so the observed gap is unlikely to be
explained by changing the final rerank precision alone.

The 2,048 arm's sequential whole in-process p50/p95/p99 was
5.862/8.726/10.579 ms at 162.2 QPS. The 16,384 arm's was
48.580/61.583/67.432 ms at 20.3 QPS. All four arms used zero vector-body
GETs. Peak serving RSS was 2,020,347,904 bytes; loaded graph heap
308,911,088 bytes, FP16 plane 1,544,000,000 bytes, PQ64 codes 64,000,000
bytes, PQ cosine norms 4,000,000 bytes, physical map 8,000,000 bytes, and
eight visit workspaces 32,000,000 bytes. Cold hydration was 3.149 s.
These are **in-process** timings, not networked product latency.

## Closed graph attribution

After terminal closure, the authenticated graph artifact was streamed and
SHA-256-checked again. Its directed base layer contains exactly 64,000,000
edges, but **37,362/1,000,000 nodes have zero in-degree, 70,473 have fewer
than four in-edges, and 37,780 are unreachable from its entry node**.
The independently authenticated V214 100k graph, built by the same code,
has 6,400,000 base edges, 1,760 zero-in-degree nodes, 4,231 with fewer
than four in-edges, and 1,791 unreachable from its entry. This growth
from 1.791% to 3.778% unreachable rows is a direct structural failure.
`centroid_hnsw.rs` selects the nearest M insert candidates and truncates
overflowing reciprocal lists to their nearest width; its comment about
robust pruning does not match the code. SQ8 scoring cannot recover a node
that graph traversal cannot reach. PQ64 score distortion may still be a
secondary loss, but topology must be repaired first. The proposed SQ8-only
falsifier was canceled before any remote run.

The separate graph build took 2,708.716 s (45m 8.7s by the Rust timer;
45m 12.2s by `/usr/bin/time`) and peaked at 8,359,153,664 bytes RSS.
The graph artifact was 263,200,298 bytes. Source preparation took 18.14 s
and peaked at 7.94 GiB RSS. The instance launched at 10:03:26 UTC and its
terminal was uploaded at 10:54:53 UTC on 2026-09-25. At the launch-time
Spot quote of **$0.3631/instance-hour**, this 51m27s interval implies about
**$0.311 compute**, an estimate excluding EBS, S3, and billing rounding.

Next, build one generic reachable graph variant, require all
100k nodes reachable and incoming-edge coverage, then falsify its exact
and PQ64 returned quality and latency against the strongest paired 100k
BORSUK baseline. Only a qualified graph advances to one frozen 1M rerun.
The existing S3 Vectors ReLAION-1M result uses the development split,
whereas V217 uses validation, and no matched Turbopuffer result exists. No
competitor win follows from V217.
