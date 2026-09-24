# V164 smooth layout 1M transfer: closeout

## Decision

**Transfer pass; not baseline-competitive.** On used ReLAION-1M D768
validation-1000, the V163 source-only smooth k-means/centroid-chain order
returned **99,553/100,000 GT100 hits (99.553%)** after exact-source rerank,
with p05 98. It passed the predeclared 99,400/p05 97 transfer floor,
all query caps, and the 1,000-query checker. It missed the separate
baseline-competitive gate: V155 cached sparse returned 99,567/100,000
(99.567%), p05 98, with substantially fewer planned bytes. Retain V155 as
the strongest measured 1M operating point. Do not freeze V164 as the
production default or open 10M/100M quality gates from this result.

| Used ReLAION-1M D768 validation-1000 | V155 cached sparse baseline | V161 balanced-two-means context | V164 smooth k-means |
| --- | ---: | ---: | ---: |
| Exact-source returned GT100 / 100,000 | **99,567 (99.567%)** | 98,859 (98.859%) | 99,553 (99.553%) |
| Exact-source p05 hits/query | **98** | 94 | **98** |
| SQ8-only returned GT100 / 100,000 | **99,222** | 96,594 | 99,199 |
| GT100 in fetched SQ8 ranges / 100,000 | **99,647** | 96,881 | 99,633 |
| Planned SQ8 bytes / 1,000 queries | **11,134,007,040** | 16,493,667,840 | 14,568,253,440 |
| Planned GETs / 1,000 queries | 22,126 | 19,012 | **16,776** |
| Planned GETs/query median / p95 / max | not repeated here | not repeated here | **18 / 31 / 32** |
| Distinct exact-primary pages median / p95 | not repeated here | 30 / 56 | **12 / 32** |

V164 planned **30.84% more SQ8 bytes** and **24.18% fewer GETs** than
V155. Against the weaker V161 layout with the same page and routing policy,
it gained 694 exact-source GT100 hits, reduced planned bytes 11.67%, and
reduced planned GETs 11.76%. Paired with V155 per query, V164 won 75,
tied 846, and lost 79. V164's aggregate physical coverage is 14 GT100
positions below V155, exactly the aggregate exact-source return deficit;
both arms lose 80 positions between physical coverage and exact-source
return. This points to physical admission as the next resource/quality
question, but aggregate equality does not identify each missed query's
cause. V164's median planned bytes were 16,773,120 per query, only 4,096
below the 16,777,216 cap, so this format commonly saturates the byte budget.

The frozen 100k and 1M cohorts use different metrics and splits; the
100k result is not a paired cross-scale comparison. V164 used frozen
exact-primary rosters and a local SQ8 mirror. No live S3 query GETs,
latency, throughput, retry cost, charged serving RAM, fresh holdout, or
100M behavior was measured. Construction peaked at 10,152,300 KiB process
RSS and took 4:44.13 wall on the Spot worker; the checker independently
repeated the fit in 4:59.42. These are offline build/check resources.

## Provenance and verification

One Causality Spot `c7i.12xlarge` instance `i-0d4397abef19b0d2b` ran
from pushed source `488fc4702f6fd408385e532f67d0a110daf1ba33` at
`s3://borsuk-bench-453182569524-euc1/research/v164-smooth-layout/488fc4702f6fd408385e532f67d0a110daf1ba33/runs/a0001/`.
Source archive SHA-256:
`b3cda5eaaf7a813ca1034b3af0ec8781f0e751b116ee5b3fd15b007b1f0c4d9c`.
Complete terminal SHA-256:
`daa4025093ddef883358a200751972b9d953cd53be80681b9055677c3c7793c7`.
The original launcher exited zero, terminated the instance (EC2 confirmed
`terminated`), then streamed and matched the byte length and SHA-256 of
all **16** terminal-listed S3 artifacts. The relaid 780,000,000-byte SQ8
SHA-256 was
`aecf0f2704f44906f411a74ab81b36e5e05f81bab35f4c70558e88acbc4d05c9`;
the source-only order file SHA-256 was
`5b5ef48d86570e5ca68fdaaac9aef231ec7368dd526baef00474cd0a2f59a06f`.
The GT-blind plan and score seals were
`f456f9eb24601427c7afc9f94efd1aba7f63b7be186ea0e3428115a139a3826e`
and
`a3eb7f84ad60b3f41c173b101c6cdbfccc931fca9566d357e7f1eef55092b7f9`.
The checker reported `pass` / `transfer-pass` after reconstructing the
source-only order and recounting all 1,000 plans, scored IDs, hits and
aggregates. It shares the frozen V120 fitter and V161 planner/scorer
implementations, so this is a deterministic replay and aggregate audit.

## Next gate

Keep V155 as the 1M baseline. The next candidate must materially revise
physical scheduling to reduce charged bytes while preserving the smooth
source-only locality and the exact-primary tier. Preregister a bounded
**32-row-unit interval admission** screen on these same closed 1M rosters:
let primary rows nominate small units, then coalesce contiguous units into
at most 32 GETs under the same 16-MiB cap, with explicit source-only
layout and GT-blind plan seals. Compare its exact-source return, bytes and
GETs against V155 and V164, and reject it if it fails the same transfer
floor or merely trades the byte excess for GETs above V155. This changes
the physical retrieval unit and admission objective, not K or a dataset
switch. A win still needs fresh cohort and live-S3 qualification before
production defaults or scale promotion. `formal/SmoothPageBudget.lean`
proves only conditional byte/GET accounting for the current 512-row
format; empirical recall and latency remain open.
