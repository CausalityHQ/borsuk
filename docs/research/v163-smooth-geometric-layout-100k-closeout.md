# V163 smooth geometric layout 100k: closeout

## Decision

**Advance to a separate 1M transfer screen, without selecting a production
default.** On used ReLAION-100k D768 development-1000, the source-only
k-means/centroid-chain physical order passed the preregistered 97.5% mean
Recall@100, p05 90, and 96% mean Recall@10 gates. It exceeded the paired
V160 balanced-two-means control on returned quality, but cost more planned
GETs. The frozen 512-row page and 32-GET/16,777,216-byte caps were obeyed.

| Used ReLAION-100k D768 development-1000 | V160 balanced-two-means | V163 smooth k-means |
| --- | ---: | ---: |
| Returned GT100 hits / 100,000 | 99,170 (99.170%) | **99,322 (99.322%)** |
| p05 returned GT100 hits/query | 97 | **98** |
| Returned GT10 hits / 10,000 | 9,945 (99.45%) | **9,949 (99.49%)** |
| GT100 IDs in fetched SQ8 ranges / 100,000 | 99,545 | **99,704** |
| Distinct primary pages, median / p95 | 20 / not recorded here | **13 / 28** |
| Planned GETs, total / 1,000 queries | **9,894** | 15,275 |
| Planned GETs/query, median / p95 / max | **10 / 19 / 27** | 16 / 27 / 32 |
| Planned SQ8 bytes, total / 1,000 queries | **15,562,734,720** | 15,657,408,000 |
| Planned SQ8 bytes/query, median / p95 / max | 16,773,120 / 16,773,120 / 16,773,120 | **16,498,560 / 16,773,120 / 16,773,120** |

The new order gained 152 GT100 hits over 1,000 paired queries (95 wins,
849 ties, 56 losses), while GETs increased 54.38% and total planned SQ8
bytes increased 0.61%. The candidate is a 100k quality improvement, not a
resource winner. No live S3 GET, latency, throughput, retry cost, or charged
serving RAM was measured. Construction peak RSS was 2,485,928 KiB on the
Spot worker; this is offline build memory, not a serving bound.

## Provenance and validation

One Causality Spot `c7i.8xlarge` instance `i-03f78350da57478a7` ran from
pushed source `134613f2d6ac206eed2af5dce94544901eb51712` and was
confirmed terminated after its complete terminal. Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v163-smooth-layout/134613f2d6ac206eed2af5dce94544901eb51712/runs/a0001/`.
The source archive SHA-256 was
`465e7b48e2f4024ee1e7c89769fa40b1ada55da3998dfeb947113dd26f0fa902`;
terminal SHA-256 was
`f4d2bd82d1ac4a7c7c52e03f1496c6a44ce77f619e0878d0e73577aaa6f4828f`.
The controller independently streamed all 13 terminal-listed artifacts
from S3 and checked their byte lengths and SHA-256. The 78,000,000-byte
relaid SQ8 SHA-256 was
`76d325d20dd38063bb050f83cfa693f7748921c75280cb1bf1f988041329dd38`;
the GT-blind plan seal SHA-256 was
`f9776a193f3b9291a88183f089ca14394309afa862dbfc3763de235bc7e1e378`.
An independent checker reconstructed the layout and recounted all 1,000
plans, returned IDs, GT hits, budgets, aggregates, and gate decision; its
result was `pass` / `candidate-advance`.

## Next gate and limits

Freeze this same source-only smooth cluster-count rule, cosine normalization,
seeds, 12 Lloyd iterations, centroid chain, 512-row pages and identical
frozen routing at ReLAION-1M D768 validation-1000. Compare paired
exact-source returned quality and planned resources against the closed V155
cached-sparse baseline (99.567% Recall@100, p05 98, 11,134,007,040 bytes,
22,126 GETs). Apply V161's 99.400%/p05 97 transfer floor and separate
baseline-competitive gate. A failure requires causal decomposition before
changing another layer. The used 100k cohort neither validates a fresh
holdout nor proves any 1M, 10M, 100M, latency or RAM claim. Conditional
Lean bounds on pages, bytes and memory remain distinct from measured recall.
