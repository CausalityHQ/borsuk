# V223 authenticated library graph HTTP closeout

**Decision:** retain the source-only reachable PQ64 cosine graph and resident
FP16 rerank plane. The new single-root library loader served exactly the
same 1,000 ID lists as V219 and V222 in both passes, while meeting the
frozen HTTP latency, throughput and RSS gates. This qualifies the loader
as a product increment. Publication, mutation, compaction and scale remain
separate required work; this is not a production-ready release claim.

Frozen source commit `c38e4a0da557a9e41e5ce86020c5cb83609fe9d0`,
source archive SHA-256
`126199036de62e54a30775d0df272f33b404c0a63796bee2fd68abd4bcc28bc5`,
trusted generation root SHA-256
`c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf`.
The root binds generation 196, all five artifact lengths and SHA-256 hashes,
source identity, N=1,000,000 and D=768. The separately pinned root digest
was passed to the library after S3 hydration. The service used the library
loader and graph binding; no production default memory ceiling was frozen.

The single `causality` Spot attempt was `a0001` under
`s3://borsuk-bench-453182569524-euc1/research/v223-authenticated-graph-http-1m/c38e4a0da557a9e41e5ce86020c5cb83609fe9d0/runs/a0001/`.
Server `i-0b070a63b1d19a90a` and client `i-0acdb9018687b60e0`
were separate c7i.4xlarge hosts in `eu-central-1c`; both are independently
confirmed terminated. Their terminal SHA-256 values are respectively
`0162375eae0354068f03587a4a7c0e11ae6f8858b8c2e396c05ea4123470048e`
and `42a29a28a941c99a382c62f76a4d21c9808c040c057d032f4a62c431364f9d2c`.
The controller replayed every terminal artifact and sealed raw file, and
recorded `gate_pass=true`. Closeout SHA-256 is
`8c1ab3fabe43bbae341ebf7623d0675182f6d63f1918c1917fbd30fa9d2e442f`.

Dataset: ReLAION-1M, 768 dimensions, **validation ordinals 0–999 already
used**, k=100, eight persistent VPC-peer HTTP/1.1 connections. Timing
includes client JSON encode, HTTP exchange and decode. The server was
fully resident after authenticated hydration, had no response cache, and
issued zero vector-body GETs by construction. Each pass used 1,000 queries.
The V222 scorer and its schema were intentionally reused unchanged.

| Measured cell | GT100 hits / 100,000 | p50 / p90 / p95 / p99 (ms) | QPS |
| --- | ---: | ---: | ---: |
| V223 library loader, first | 99,664 | 14.828 / 19.302 / 20.820 / 22.564 | 497.0 |
| V223 library loader, repeat | 99,664 | 14.765 / 19.269 / 20.713 / 22.573 | 502.2 |
| V222 example loader, first | 99,664 | 14.723 / 19.267 / 20.424 / 22.402 | 506.3 |
| V222 example loader, repeat | 99,664 | 15.064 / 20.603 / 21.987 / 25.699 | 481.5 |

Independent closed-raw replay recomputed all percentiles and QPS from each
pass's same 1,000 samples; the V223 raw SHA-256 values are
`313b4b229a5e26a6db6c3a3ed0e62c243a1560d9a9b17813d6c8b88f9c32f3d0`
and `c3f1d2473b4847e7470555beff0a38f04d88b1b7813aae3564239e6872ecfb59`.
All 1,000 ID lists in each V223 pass exactly matched V222. Independent
GT100 replay found 99,664 hits, p05 98 and minimum 88 in each pass. Each
pass had peak eight simultaneous requests, 14,658,893 logical request
bytes and 1,026,973 logical response bytes. Server peak RSS was
2,026,614,784 bytes. V222 remains a separate run from a different source
revision, so differences in timing are descriptive, not a measured loader
speedup or regression. V221 S3 Vectors had lower recall and different
cache/transport semantics; [V222 closeout](v222-external-graph-http-1m-closeout.md)
details that comparison. No authenticated Turbopuffer comparison exists.

The launch-time Spot quote was $0.3631/hour per host; compute to both
terminal markers is estimated at **$0.034463**, excluding EBS, S3,
hydration, index build, network and billing adjustments.

**Next product gate:** publish one immutable, content-addressed generation
and conditionally update its trusted pointer, with failure-injection tests
for torn uploads and recovery. Then add mutation and compaction without
breaking pinned readers. The next scale decision is a 10M build/serve
resource gate from a frozen revision, comparing quality and latency with
the 1M selected graph under an operator-selected RAM/recall envelope.
For 100M, increase RAM when the required recall warrants it; no fixed
vector-count knee or extrapolated latency is treated as measurement.
