# Rank16 ReLAION 1M exact-GT construction: GO for fresh development test

2026-09-29. One frozen AWS Spot attempt `fresh-rank16-seal-a0001` completed.
The original terminal and an independent closed-artifact verifier passed;
`i-0b38b829e7709b803` is terminated. The source archive was commit
`165aa6d0`, SHA256 `2500a6c97a219c4e91576f32f77079268184946eb901ef38f8da2b81a5cb5afc`.
The verifier checked the terminal, reservation, source archive, all small
artifacts, all three sealed S3 bodies, their metadata, and EC2 state.

The indexed FIRST1M D768 cosine source matched Parquet SHA256 `2796b579...`
and the current candidate/control raw source SHA256 `a3eac4de...` exactly.
All16 selected rank16–31 physical shards matched the pinned registry SHA,
and all1,000 frozen feature IDs matched their rank/row locators. The existing
exhaustive f64 cosine oracle produced GT100 with source-ordinal tie order and
passed its scalar/matrix self-check. Sealed query vectors are3,072,000B,
requests14,672,490B, and GT100400,000B; exact keys/SHA256 are in
[`verification.json`](fresh-rank16-seal/a0001/verification.json).

Construction took249.03s inside333s instance lifetime. Cgroup peak was
5,865,422,848B of24GiB, zero swap/OOM. Compute cost **estimated** $0.0264
from Spot quote and observed lifetime; EBS/S3 and invoice cost remain unknown.
No ANN recall, HTTP latency/QPS or vendor comparison was measured.

**Next gate:** one frozen ReLAION FIRST1M development0–63 candidate run against
these sealed requests/GT, with exact current root, SQ8, native scorer and
four-slot HTTP binary. Measure native R@10/R@100, then incoming cold HTTP at
offered8QPS if native R@10 >=95%; report hydration and physical counts
separately. Stop this arm on scientific failure; do not infer a matched
control or vendor win from the consumed historical reference. Prospective
ordinals64–999 remain sealed and may not guide tuning.
