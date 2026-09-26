# V263 direct S3 Vectors comparator on CoHere first1M

Decision: characterize S3 Vectors on the exact CoHere corpus and prior-used
query/GT100 panel that passed BORSUK V262. Do not change BORSUK parameters or
infer a transport/cache-matched service win. One fresh float32 cosine
S3 Vectors index, one c7i.4xlarge `causality` Spot client in
eu-central-1c, five parallel 500-vector uploads, 60-second settle,
then eight concurrent ordinal SDK HTTPS queries in a fresh-index first pass
and immediate repeat. No client response cache. S3 Vectors managed cache is
opaque. Delete the temporary index and vector bucket before terminal;
discard an interrupted attempt and never combine partial samples.

Dataset is CoHere-large-10M canonical first1,000,000 source rows, D768,
cosine k100. Frozen V261 input terminal SHA-256 is
`00c7d4803354f15d5f62bea6e53fd0672a0f5fc91cc482e1fa4b20b8951a9f82`.
Source float32 bytes/SHA-256 are 3,072,000,000 /
`6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005`;
1,000 query JSONL bytes/SHA-256 are 15,369,495 /
`86d9406486a2bb27aa2e603f019e078dd3ecaed47f79ec685558ba3536433812`;
exact cosine GT100 bytes/SHA-256 are 400,000 /
`62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39`.
Development ordinals0–255 and validation256–999 are prior-used.

Frozen BORSUK V262 authenticated resident-after-S3-hydration VPC-peer HTTP
baseline on the same panel: both passes 99,717/100,000 GT100 hits;
first p50/p90/p95/p99 116.236/142.162/149.749/158.941 ms, 67.13 QPS;
repeat 116.049/141.497/147.844/157.258 ms, 67.37 QPS. Peak serving
RSS 2,050,895,872 B and zero query vector-body GETs. V262 closeout SHA-256
`eca88b883d56387ced98632a1df541ef9134b204b635b2b42e6732bda45215a9`.

Seal every returned ID list and raw client SDK latency before scoring.
Report first/repeat same-sample p50/p90/p95/p99, GT100 and GT10 hits,
development/validation means and p05, QPS, retries, response bytes,
ingestion time, PUTs and logical bytes, client RSS, Spot elapsed cost, and
separate S3 Vectors service pricing estimate. Compare first to first and
repeat to repeat, naming BORSUK's private HTTP/resident cache and S3
Vectors' regional HTTPS/opaque cache. If S3 Vectors wins a material latency
or throughput metric, choose one serving or architecture decision before
the 10M scale gate; if BORSUK leads at matched quality and disclosed cost,
proceed to the 10M gate. No direct Turbopuffer tenant measurement exists.
