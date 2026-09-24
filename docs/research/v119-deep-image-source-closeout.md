# V119 deep-image source materialization closeout

Status: **corpus-only source artifact sealed**. This is construction evidence,
not a returned-recall, live-S3 latency, or scale result.

- Source commit: `dc242e761ac2d60068c7e8e65812615a6ed6cfd7`.
- Immutable attempt: `s3://borsuk-bench-453182569524-euc1/research/v119-deep-image-source/dc242e761ac2d60068c7e8e65812615a6ed6cfd7/runs/v119-deep-image-source-20260924T002421Z/a0001`.
- Causality Spot `c7i.4xlarge` `i-05d8a3e2a4be08249` in
  `eu-central-1c` reached a `complete` terminal with exit code zero after
  73 seconds. EC2 subsequently reported `terminated`.
- The sealed `source.json` reports deep-image-96, cosine, 9,990,000 rows,
  dimension 96, and `query_or_truth_used: false`. It binds the publication-v3
  staging receipt SHA-256
  `eb10a317cc778b30e2ee88eee8614b760e36f31d2d7af1a62a60f5f32c163230`.
- The output `artifacts/source.parquet` is 3,566,768,562 bytes with worker
  SHA-256 `8f88122f412554107d97c07f440352f9043b8cb4b58fe08434ac75f4b90776ee`.
  Its S3 object length independently matches the terminal. Its full payload
  SHA-256 must be independently verified by the later source consumer before
  an index build; no local copy or independent full-payload hash is claimed
  here.
- All ten terminal-listed objects were checked against S3 object lengths.
  The nine small objects were downloaded and independently matched to their
  terminal SHA-256 values. The `source.json` SHA-256 is
  `a643e6e3342429fe2047b5d69c756a3b6adcb37bdaffe2274f4067d344160fa8`.

The next gate builds a new deep-image index from this source only. Query and
GT values enter only after the build is sealed. Compare returned results with
the paired capped control under one preregistered, corpus-agnostic method;
then measure live S3 performance and charged resources separately. Do not
reuse the old V82 index as a same-run baseline.
