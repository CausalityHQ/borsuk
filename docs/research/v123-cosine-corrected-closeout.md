# V123 a0002 corrected cosine diagnostic closeout

V123 a0002 is a development postmortem on the **already used**
deep-image-96-angular publication test-first-1000 cohort at 9,990,000 rows.
The immutable source commit was `874a0adf2ce175e228a9dbbcf083c0e0bcd1f5fe`.
Its Causality Spot terminal is
`s3://borsuk-bench-453182569524-euc1/research/v123-rerank-postmortem/874a0adf2ce175e228a9dbbcf083c0e0bcd1f5fe/runs/v123-20260924T0235Z/a0002/terminal.json`,
SHA-256 `f170c8805cd29a9ff4a2bc9a06f9ceb1d10908e350c059dcb0c600271888b150`.
The worker completed in 166 seconds, and Causality Spot
`i-0bd49edada059ff90` was terminated. All 24 terminal-listed artifacts were
independently streamed from S3 and checked by length and SHA-256. The capture
summary SHA-256 is
`594b7ef9eb675ec500a5f4d1ab6bfc0c3e83b8261c6200c81b8bcf4751282447`;
the corrected rerank summary SHA-256 is
`022489875d8d84565fd9a47971a2302d37fe2c0c91a3adb8ed2b7f4da7ec42a1`.
Six targeted Python tests and one Rust test passed on the worker. An
independent standard-library recount matched all 1,000 capture/rerank rows,
six metrics at six K widths for both arms, p05, sub-90 counts and paired
outcomes. The source and publication GT hashes matched their frozen inputs.

| Deep-image-96-angular, test-first-1000, GT100 positions | K=0 nominees only | K=512 candidate union | V121 served SQ8 candidate |
| --- | ---: | ---: | ---: |
| Candidate-set capture | 99,994 / 100,000 | 99,995 / 100,000 | 99,580 physical range coverage |
| Float32 source cosine returned | 99,993 / 100,000 | 99,994 / 100,000 | not run |
| Simulated FP16 source cosine returned | 99,959 / 100,000 | 99,959 / 100,000 | not run |
| Simulated FP16 p05 hits/query | 100 | 100 | not run |
| Simulated FP16 queries below 90 hits | 0 | 0 | not run |
| Served SQ8 returned | not run | not run | 98,034 / 100,000 |

At K=512, corrected FP16 cosine Recall@100 was **99.959%**, and float32
cosine was **99.994%**. The 35-hit gap is within the preregistered 50-hit
limit; FP16 exceeds 99%, p05≥90, and has no sub-90 queries. The
shared-nominee hybrid control had equal hit counts and p05; all 1,000 paired
queries tied in both precision arms. Thus the preregistered development
`supports_followup` flag is **true**. K≥100 did not improve corrected FP16
returned hits beyond the nominee-only K=0 path, while float32 gained one hit.
The 512 nominees supplied almost all candidate quality in this frozen router.
This does not prove a future faster router can omit expansion. The control is
the shared-nominee hybrid, not V121's served V109-style control, which returned
97,791/100,000 SQ8 hits under the paired 32-GET/16-MiB cap.

The a0001 historical dot-product scorer returned 99,890 FP16 hits at K=512;
the corrected cosine scorer returned 99,959. A0001's 104-hit FP16 gap is
valid only for its unnormalized implementation. Its artifacts remain sealed
under the separate source commit and terminal. The corrected scorer divides
by each decoded vector norm. It is still an offline simulation over a complete
local FP32 Parquet source, **not** a resident FP16 serving tier. The source
lookup was outside V121's S3 GET/byte accounting, and nominees may lie
outside V121's fetched SQ8 ranges. The diagnostic Python phase took 22.35
seconds for 1,000 queries and peaked at 8,991,004 KiB RSS; wide SQ8 replay
took 67.13 seconds and peaked at 2,142,976 KiB. These batch observations do
not measure live latency, throughput, charged serving RAM or 100M capacity.

**Decision and next gate:** the corrected cosine score representation is a
candidate for a generic accurate rerank tier, but V121/V123 are not qualified
for production. Freeze the exact score semantics and test a matched
ReLAION-1M build plus fresh deep-image ordinals outside 0..999. A serving
experiment must account for where every nominee vector resides, local SSD/RAM
cost, generation overlap, S3 data waves, cold and warm p95/p99 latency,
throughput and charged cgroup memory. A sublinear router must be paired with
the same scorer and remeasure nominee capture and any value from expansion;
V121's 184.76-ms/query offline flat nomination batch average at 9.99M rows
does not meet a latency claim. RAM policy should depend on `(N,D,R,C,G,L)`;
there is no data-supported vector-count quality knee. Formal proofs can
certify capture ceilings, quantized score-error margin implications, bytes
and conditional work/latency from authenticated premises. The present cohort
cannot prove unseen recall or deployed S3 latency.
