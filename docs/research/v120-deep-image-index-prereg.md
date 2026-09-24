# V120 deep-image source-only index construction preregistration

Status: construction cell, frozen before downloading any deep-image query or
GT object. This cell does not measure returned recall or live S3 latency.

The input is V119's sealed `source.parquet` at
`s3://borsuk-bench-453182569524-euc1/research/v119-deep-image-source/dc242e761ac2d60068c7e8e65812615a6ed6cfd7/runs/v119-deep-image-source-20260924T002421Z/a0001/artifacts/source.parquet`.
Require 3,566,768,562 bytes and SHA-256
`8f88122f412554107d97c07f440352f9043b8cb4b58fe08434ac75f4b90776ee`.
Require the adjacent `source.json` SHA-256
`a643e6e3342429fe2047b5d69c756a3b6adcb37bdaffe2274f4067d344160fa8`.
Download and independently hash both **before** construction. Require 9,990,000
ordered training IDs, D=96 fixed-list float32, finite nonzero unit vectors,
cosine metric, and `query_or_truth_used=false`. Do not download query/GT in
this attempt.

Fit one physical layout by corpus-only Lloyd centroids (12 iterations,
sample up to 64 rows per centroid, seeds 8201/8202), greedy centroid chaining,
then stable within-cluster radius order. The generic cluster count is
`min(N, ceil(8192 * (N / 1,000,000)^(1/3)))`; it equals the current 1M
layout's 8,192 clusters and grows smoothly with N. It is fixed now as a
method hypothesis, not selected from deep-image results. Emit original train
ordinals as SQ8 IDs. Fit per-coordinate SQ8 min/max from all corpus rows;
write `id:i64 | reconstructed_norm:f32 | D u8` records in physical order.
Seal the layout and SQ8 hashes and geometry. From the same authenticated
source, construct the version-2 balanced PQ64 router, SQ8 mirror and page
authority; no query or GT data may enter any artifact. Run the targeted Rust
router tests on the frozen source snapshot. A format or compile failure closes
this attempt; do not keep a partial index.

Run once on Causality Spot with a 7,200-second science wall cap, one immutable
source archive and attempt prefix, interruption detection, resource logs and
terminal marker. Sync terminal artifacts to S3, discard interrupted cells,
and terminate immediately after terminal. Before terminal, inspect only
infrastructure and terminal state. Verify all small artifacts independently;
the later quality consumer must download and independently hash large SQ8
and router planes before use.

The next gate, after a sealed V120 build, evaluates the frozen V116 candidate
rule and a same-run V109-style capped control on deep-image-96 query/GT data.
Use 100 primary/412 secondary votes, at most 32 GETs and 16,777,216 bytes,
and the preregistered proportional region count
`ceil(page_count * 1024 / 3907)`. Apply the same 99.0% Recall@100, p05 >=90,
zero-cap-violation acceptance rule; report paired totals and tails. Stop
early if construction or a short query runtime screen establishes the flat
router cannot meet a competitive latency objective. A cross-corpus miss is a
method failure to diagnose by layer, not a reason for corpus-specific tuning.
