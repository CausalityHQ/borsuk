# V130 strict ordered-parity gate: stopped without quality measurement

V130 used sealed V122 deep-image-96 random-100k development inputs and the
production source writer, source-ID map and SQ8/scorer modules. Its three
immutable Causality Spot attempts stopped before a complete 1,000-query
cell. **There is no V130 recall, latency, or scale result.** Every attempted
instance was observed terminated.

| Attempt | Source archive SHA-256 | Terminal SHA-256 | Last phase and result |
| --- | --- | --- | --- |
| `v130-20260924T051018Z/a0001` | `1eddaf458f2a6278744a1c743d5a1c056eb4a546e28495cab0f3c82e5d755a0a` | `c1a32204e7bf0bc808c00bce1f45dfab2a63135eb6fdcaced7fb5016e6d0dc5d` | Compile failed: malformed new JSON summary macro. |
| `v130-20260924T051504Z/a0002` | `5b6441bd8c3701b96560e434f2f261e3f9c7a71b34e4aef6515addc6279a51a4` | `6223fe1aef015683227a704e9447988551bd2e0b0bfbfcbaf994a1653a6fdc1b` | Compile and frozen-query preflight passed; source preparation found a copied, truncated source SHA constant. |
| `v130-20260924T052116Z/a0003` | `1ff0889c19283b2c9adcfa5443fef592a11ee0eab62c3b7b42ca67e192ecd997` | `1c10446aca6b748cd60f6d4767a44fc57a72f055058858ffec99fa98754c9415` | Source/map build and open passed; strict ordered SQ8 top-100 parity failed at query 1, capped-control arm. |

All attempt receipts and checked logs are under
`s3://borsuk-bench-453182569524-euc1/research/v130-source-replay/56f7071716681fde524432d29a436015584df773/runs/`.
The source archive for each attempt is bound to the above SHA and the
terminal lists its available artifact hashes. Attempt `a0003` verified the
complete V122 input hashes and 1,000 sealed query/truth identities; it built
a 39,200,064-byte source plane and a 1,600,096-byte ID map. Those artifact
sizes are build evidence, not serving RAM or quality.

For the failed query-1 capped-control comparison, an independent narrow
replay of the **complete** V122 SQ8 object with the same production Rust
module found that Rust and V122 NumPy produced identical top-100 ID sets.
Only positions 56 and 57 were exchanged. Rust assigned those rows scores
0.91336000 and 0.91336024; the 2.4e-7 gap makes float32 reduction order a
plausible explanation. This is an inference from the scores, not a proof of
NumPy's internal reduction sequence. A strict ordering match across the
two scorer implementations was an unsuitable gate for set-based Recall@100.

Decision: do not weaken V130 or interpret its partial output as a result.
V131 is separately preregistered to use the same production Rust SQ8 scorer
for both paired arms, record all per-query differences from sealed V122,
and compare exact source results to that same-run baseline. This resolves
the measurement contract without changing the candidate algorithm or
selecting dataset-specific settings.
