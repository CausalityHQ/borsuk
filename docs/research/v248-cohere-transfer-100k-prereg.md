# V248 source-only graph transfer to CoHere, 100k falsifier

**Question:** does the V245 owner-partitioned graph and PQ64/FP16
method retain high recall on a second real D768 cosine corpus before
a 10M scale run? This is a dataset-transfer gate, not another
ReLAION parameter sweep or a vendor product comparison.

Use the authenticated CoHere-large-10M publication materialization
receipt SHA-256
`0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87`.
Its dataset content SHA-256 is
`fa8ccb38e5c761388e0c2ac211cc219438cd79e802e69debd6197d74f83f11ad`;
458 immutable train shards contain exactly 10,000,000 rows. The
query object has 1,000 rows, SHA-256
`5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e`.
The published 10M neighbor object SHA-256 is
`ae5a3a9de54099466d93609a3d6c4571e4bc69fff2e5e9003171d361e8621608`;
it is **not** ground truth for the 100k subset and must not score
this gate. Authority prefix:
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/datasets/cohere-large-10m-768/attempts/0001/materialized/`.

Take the first 100,000 canonical training rows in physical source
order, with implicit row IDs 0–99,999. The staging contract already
validated source IDs as row positions. Authenticate every consumed
shard by the receipt's length and SHA-256. Prepare the FP16 plane and
train/encode PQ64 from source rows only, using the existing 64
subspace, 256-codeword, deterministic Lloyd method. Queries and truth
are unavailable to construction. Use the unchanged M=32, M0=64,
ef_construction=128, eight-worker owner-partitioned graph policy and
the same exact FP16 rerank. Freeze ef/shortlist arms
2048/2048, 4096/4096 and 8192/8192; do not adjust them after
reading the 256–999 panel.

For the 1,000 frozen CoHere test queries, independently compute
exact top-100 over **this 100k subset** using float64 cosine scores
and a deterministic `(score, row ID)` tie break. Record the source
and query byte hashes, FP16/PQ/graph artifact hashes, and seal all
returned IDs before opening exact truth. Queries 0–255 are the
configuration development split; 256–999 are the method validation
split, though this dataset has appeared in older repository
publication work and is not a pristine external holdout. Choose the
smallest arm with development mean R@100≥0.995 and p05 hits≥98;
then require validation mean R@100≥0.995 and p05 hits≥98, without
retuning. Report all three arms and splits, per-query returned IDs,
p50/p90/p95/p99, QPS, build and preparation time, peak RSS and
GETs/bytes.

The structure gate requires 100,000/100,000 reachable rows, minimum
in-degree four and maximum degree≤256. The builder gate is≤120 s
and≤1.5 GB peak RSS; preparation and PQ training memory/time are
reported separately. These limits are falsification thresholds,
not extrapolated 10M measurements. The strongest same-method
ReLAION-100k baseline V244 built in 20.725 s at 537,739,264 B and
returned 99,767/100,000 exact GT100 hits on its **different** corpus;
it is a resource/topology reference, not a paired quality control.

Run one `causality` c7i.4xlarge Spot attempt with source archive,
immutable reservation, instance ID, terminal artifact hashes and
interruption handling. Discard any interrupted cell; terminate compute
at terminal. A pass promotes the unchanged source-only method to
one CoHere-large-10M build/serving gate with machine memory selected
from measured per-vector components and recall work, never a hard
vector-count knee. A fail requires one material representation,
navigation or training decision at 100k before any 10M run.
